// The durable-steps engine: Cloudflare Workflows' `step.do` / `step.sleep` /
// `step.waitForEvent`, replayed over stored step results.
//
// One job runs one instance for a while: claim the instance (the run lease),
// load every stored step, run the class's `run()` from the top. A step with a
// stored result returns it; the first step without one executes, its result is
// persisted, and the run goes on until it finishes, fails, has to wait (a
// sleep, a retry backoff, an event) or spends its time budget. Then it hands
// the instance to a new outbox job, due when the wait ends. See
// docs/durable-steps.md for the whole design.
//
// Everything that touches the database goes through the `Store` port, so the
// replay rules are tested against an in-memory store with the same
// compare-and-swap semantics as the SurrealDB one (`surreal.ts`).

// =============================================================
// Public types (Cloudflare's shapes)
// =============================================================

export type WorkflowDurationLabel =
  | 'millisecond'
  | 'second'
  | 'minute'
  | 'hour'
  | 'day'
  | 'week'
  | 'month'
  | 'year';
export type WorkflowSleepDuration = number | `${number} ${WorkflowDurationLabel}${'s' | ''}`;
export type WorkflowBackoff = 'constant' | 'linear' | 'exponential';

export interface WorkflowStepConfig {
  retries?: { limit: number; delay: WorkflowSleepDuration; backoff?: WorkflowBackoff };
  timeout?: WorkflowSleepDuration;
}

export interface WorkflowEvent<T = unknown> {
  payload: Readonly<T>;
  timestamp: Date;
  instanceId: string;
}

export interface WorkflowStepEvent<T = unknown> {
  payload: Readonly<T>;
  timestamp: Date;
  type: string;
}

export interface WorkflowStepContext {
  /** 1 on the first attempt. */
  attempt: number;
}

export interface WorkflowStep {
  do<T>(name: string, callback: (ctx: WorkflowStepContext) => Promise<T> | T): Promise<T>;
  do<T>(
    name: string,
    config: WorkflowStepConfig,
    callback: (ctx: WorkflowStepContext) => Promise<T> | T,
  ): Promise<T>;
  sleep(name: string, duration: WorkflowSleepDuration): Promise<void>;
  sleepUntil(name: string, timestamp: Date | number): Promise<void>;
  waitForEvent<T = unknown>(
    name: string,
    options: { type: string; timeout?: WorkflowSleepDuration },
  ): Promise<WorkflowStepEvent<T>>;
}

export type InstanceStatusName =
  | 'queued'
  | 'running'
  | 'paused'
  | 'errored'
  | 'terminated'
  | 'complete'
  | 'waiting'
  | 'waitingForPause'
  | 'unknown';

export interface InstanceStatus {
  status: InstanceStatusName;
  error?: { name: string; message: string };
  output?: unknown;
}

/** Fails the step at once, whatever its retry budget, and reaches `run()`. */
export class NonRetryableError extends Error {
  constructor(message: string, name = 'NonRetryableError') {
    super(message);
    this.name = name;
  }
}

/** The base class of a durable workflow, as in `cloudflare:workers`. */
export abstract class WorkflowEntrypoint<Env = unknown, Params = unknown> {
  protected ctx: ExecutionContextLike;
  protected env: Env;
  constructor(ctx: ExecutionContextLike, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }
  abstract run(event: Readonly<WorkflowEvent<Params>>, step: WorkflowStep): Promise<unknown>;
}

export interface ExecutionContextLike {
  waitUntil(promise: Promise<unknown>): void;
  passThroughOnException?(): void;
}

export type WorkflowClass = new (ctx: ExecutionContextLike, env: any) => {
  run(event: WorkflowEvent<any>, step: WorkflowStep): Promise<unknown>;
};

// =============================================================
// Limits and defaults
// =============================================================

/** Cloudflare's defaults for `step.do`. */
export const DEFAULT_RETRIES = { limit: 5, delay: 10_000, backoff: 'exponential' as WorkflowBackoff };
export const DEFAULT_STEP_TIMEOUT_MS = 10 * 60_000;
export const DEFAULT_EVENT_TIMEOUT_MS = 24 * 60 * 60_000;
/** A step result, the params and the output: Cloudflare's step limit. */
export const MAX_VALUE_BYTES = 1024 * 1024;
export const MAX_STEPS = 10_000;
export const MAX_STEP_NAME = 256;
/** How long a claim holds the instance without a renewal. */
export const LEASE_MS = 30_000;
export const RENEW_MS = 10_000;
/** A retry or sleep at most this long waits inside the run instead of a new job. */
export const INLINE_WAIT_MS = 2_000;
/** Budget when the job runner sent no deadline (its own default is 10 s). */
export const DEFAULT_BUDGET_MS = 8_000;
export const MAX_BUDGET_MS = 60_000;

export const ROUTE_PREFIX = '/__spky/workflow/';
export const ACTIVE_STATUSES = ['queued', 'running', 'waiting'] as const;
const INSTANCE_ID = /^[a-zA-Z0-9_][a-zA-Z0-9-_]*$/;

// =============================================================
// Pure helpers
// =============================================================

const UNIT_MS: Record<string, number> = {
  millisecond: 1,
  ms: 1,
  second: 1_000,
  sec: 1_000,
  s: 1_000,
  minute: 60_000,
  min: 60_000,
  m: 60_000,
  hour: 3_600_000,
  h: 3_600_000,
  day: 86_400_000,
  d: 86_400_000,
  week: 604_800_000,
  w: 604_800_000,
  month: 2_592_000_000,
  year: 31_536_000_000,
  y: 31_536_000_000,
};

/** A Workflows duration (`5000`, `'10 seconds'`, `'1 day'`) in milliseconds. */
export function toMs(duration: WorkflowSleepDuration | string): number {
  if (typeof duration === 'number') {
    if (!Number.isFinite(duration) || duration < 0) throw new TypeError(`invalid duration ${duration}`);
    return duration;
  }
  const m = /^\s*(\d+(?:\.\d+)?)\s*([a-z]+)\s*$/i.exec(duration);
  const word = m ? m[2].toLowerCase() : '';
  const unit = UNIT_MS[word] ?? (word.endsWith('s') ? UNIT_MS[word.slice(0, -1)] : undefined);
  if (!m || unit === undefined) throw new TypeError(`invalid duration "${duration}"`);
  return Math.round(Number(m[1]) * unit);
}

/** The wait before attempt `failed + 1`, after `failed` failed attempts. */
export function retryDelayMs(retries: { delay: WorkflowSleepDuration; backoff?: WorkflowBackoff }, failed: number): number {
  const base = toMs(retries.delay);
  switch (retries.backoff ?? 'exponential') {
    case 'constant':
      return base;
    case 'linear':
      return base * failed;
    default:
      return base * 2 ** (failed - 1);
  }
}

/**
 * How long a run may keep starting steps: the job's deadline minus a margin
 * (20%, at least 1 s), capped, or the default when the runner sent none.
 */
export function budgetMs(jobTimeoutMs: number | undefined): number {
  if (!jobTimeoutMs || !Number.isFinite(jobTimeoutMs) || jobTimeoutMs <= 0) return DEFAULT_BUDGET_MS;
  const margin = Math.max(1_000, jobTimeoutMs * 0.2);
  return Math.max(500, Math.min(MAX_BUDGET_MS, jobTimeoutMs - margin));
}

/** Cloudflare's instance id rule. */
export function validInstanceId(id: string): boolean {
  return id.length > 0 && id.length <= 100 && INSTANCE_ID.test(id);
}

/** The stored key of the `n`th step called `name` in one run. */
export function stepKey(name: string, n: number): string {
  return `${name}#${n}`;
}

export interface StoredError {
  name: string;
  message: string;
}

export function toStoredError(e: unknown): StoredError {
  if (e instanceof Error) return { name: e.name || 'Error', message: e.message };
  return { name: 'Error', message: typeof e === 'string' ? e : safeJson(e) ?? String(e) };
}

/**
 * The error a failed step throws, on its first run and on every replay alike:
 * an `Error` with the stored name and message (a `NonRetryableError` stays
 * one). Never the original object, so `run()` cannot behave differently on a
 * replay, which is also what Cloudflare does (errors cross an RPC boundary).
 */
export function fromStoredError(e: StoredError): Error {
  if (e.name === 'NonRetryableError') return new NonRetryableError(e.message);
  const err = new Error(e.message);
  err.name = e.name;
  return err;
}

function safeJson(v: unknown): string | undefined {
  try {
    return JSON.stringify(v);
  } catch {
    return undefined;
  }
}

/**
 * A value as stored: JSON text, or `undefined` for `undefined`. Throws a
 * `NonRetryableError` for a value JSON cannot hold or one over the limit,
 * because retrying would fail the same way.
 */
export function encodeValue(value: unknown, what: string): string | undefined {
  if (value === undefined) return undefined;
  let text: string | undefined;
  try {
    text = JSON.stringify(value);
  } catch (e) {
    throw new NonRetryableError(`${what} is not JSON serializable: ${(e as Error).message}`);
  }
  if (text === undefined) return undefined;
  const bytes = byteLength(text);
  if (bytes > MAX_VALUE_BYTES) {
    throw new NonRetryableError(
      `${what} is ${bytes} bytes of JSON, over the ${MAX_VALUE_BYTES}-byte limit: store it in a bucket and return its key`,
    );
  }
  return text;
}

export function decodeValue(text: string | null | undefined): unknown {
  return text === null || text === undefined ? undefined : JSON.parse(text);
}

function byteLength(s: string): number {
  return new TextEncoder().encode(s).length;
}

/** 128 random bits as hex: a wake token, and the key of the job carrying it. */
export function newToken(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

// =============================================================
// The store port
// =============================================================

export type StepKind = 'do' | 'sleep' | 'event';
export type StepStatus = 'success' | 'failed' | 'retrying' | 'sleeping' | 'waiting';

export interface StepRow {
  name: string;
  n: number;
  kind: StepKind;
  status: StepStatus;
  attempts: number;
  /** JSON text. */
  output?: string;
  error?: StoredError;
  /** Retry, wake or timeout time, epoch ms in the database's clock. */
  nextAt?: number;
  eventType?: string;
  startedAt?: number;
  finishedAt?: number;
}

export interface InstanceRow {
  id: string;
  binding: string;
  outbox: string;
  path: string;
  /** JSON text. */
  params?: string;
  status: InstanceStatusName;
  createdAt: number;
}

export type Claim =
  | { kind: 'claimed'; instance: InstanceRow; epoch: number; dbNow: number }
  | { kind: 'gone' | 'done' | 'stale' | 'busy' };

export interface StoredEvent {
  id: string;
  type: string;
  /** JSON text. */
  payload?: string;
  createdAt: number;
}

export interface Progress {
  stepsDone: number;
  currentStep?: string;
}

export type Finish =
  | { status: 'complete'; output?: string }
  | { status: 'errored'; error: StoredError }
  | {
      status: 'suspend';
      /** When the next run is due, database epoch ms. */
      dueAt: number;
      /** The token this run claimed with; a different one now means a nudge. */
      claimedWake: string;
      newWake: string;
    };

export interface NewInstance {
  id: string;
  binding: string;
  className: string;
  outbox: string;
  path: string;
  params?: string;
  owner?: string;
  wake: string;
}

/** Thrown by a store write whose fencing token no longer matches. */
export class FencedError extends Error {
  constructor() {
    super('spky:fenced: this run lost the instance (lease taken over, terminated or restarted)');
    this.name = 'FencedError';
  }
}

export interface Store {
  claim(id: string, wake: string, leaseMs: number): Promise<Claim>;
  loadSteps(id: string): Promise<StepRow[]>;
  /** `false` once the epoch moved (the run must stop). */
  renewLease(id: string, epoch: number, leaseMs: number): Promise<boolean>;
  /** Fenced; `consumeEvent` marks that event consumed in the same transaction. */
  saveStep(id: string, epoch: number, step: StepRow, progress: Progress, consumeEvent?: string): Promise<void>;
  /** The oldest unconsumed event of `type`. */
  findEvent(id: string, type: string): Promise<StoredEvent | null>;
  /** Fenced. A suspend also creates the next job in the instance's outbox. */
  finish(id: string, epoch: number, finish: Finish, progress: Progress, nowMs: number): Promise<void>;

  create(instance: NewInstance): Promise<void>;
  status(id: string): Promise<{ status: InstanceStatusName; output?: string; error?: StoredError } | null>;
  terminate(id: string, newWake: string): Promise<boolean>;
  restart(id: string, newWake: string): Promise<boolean>;
  /** Stores the event and, for an active instance, a job due now with a new token. */
  sendEvent(id: string, type: string, payload: string | undefined, newWake: string): Promise<boolean>;
}

// =============================================================
// One run
// =============================================================

export interface RunOptions {
  /** The job runner's deadline for this request (`x-spky-job-timeout-ms`). */
  jobTimeoutMs?: number;
  leaseMs?: number;
  renewMs?: number;
  inlineWaitMs?: number;
  /** Test seams. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  token?: () => string;
  setInterval?: (fn: () => void, ms: number) => unknown;
  clearInterval?: (handle: unknown) => void;
}

export type RunOutcome =
  | 'complete'
  | 'errored'
  | 'suspended'
  | 'fenced'
  | 'gone'
  | 'done'
  | 'stale'
  | 'busy';

export interface RunResult {
  /** HTTP status for the job runner: 503 makes it retry the same job later. */
  httpStatus: number;
  outcome: RunOutcome;
  /** For `suspended`: when the next run is due (database epoch ms). */
  dueAt?: number;
  stepsExecuted: number;
}


const HANDOVER = Symbol('handover');
/** Thrown inside the engine once a store call fenced or failed; never reaches user code. */
const STOPPED = Symbol('stopped');
const never = <T>(): Promise<T> => new Promise<T>(() => {});
const realSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Run instance `instanceId` for job `wake` until it finishes or has to hand
 * over. A workflow's own failure is the `errored` outcome, never a throw; a
 * store failure (the database is unreachable) throws, which the caller answers
 * with a 5xx so the job runner retries the same job.
 *
 * What user code can observe from a step is only ever its value, its own
 * (reconstructed) error, or a promise that never resolves: a hand-over, a
 * fenced run and a store failure all look like "this run stopped here", so a
 * `try/catch` around steps cannot turn them into application logic.
 */
export async function runInstance(
  store: Store,
  cls: WorkflowClass,
  env: unknown,
  ctx: ExecutionContextLike,
  instanceId: string,
  wake: string,
  options: RunOptions = {},
): Promise<RunResult> {
  const localNow = options.now ?? Date.now;
  const sleep = options.sleep ?? realSleep;
  const token = options.token ?? newToken;
  const leaseMs = options.leaseMs ?? LEASE_MS;
  const inlineWaitMs = options.inlineWaitMs ?? INLINE_WAIT_MS;

  const claim = await store.claim(instanceId, wake, leaseMs);
  if (claim.kind !== 'claimed') {
    return { httpStatus: claim.kind === 'busy' ? 503 : 200, outcome: claim.kind, stepsExecuted: 0 };
  }
  const { instance, epoch } = claim;
  // Every due time is in the database's clock: the job runner decides "due"
  // with `created_at + delay <= time::now()` there.
  const offset = claim.dbNow - localNow();
  const now = () => localNow() + offset;
  const deadline = now() + budgetMs(options.jobTimeoutMs);

  const stored = new Map<string, StepRow>();
  let finalCount = 0;
  for (const row of await store.loadSteps(instanceId)) {
    stored.set(stepKey(row.name, row.n), row);
    if (isFinal(row)) finalCount++;
  }

  // Why the run stops early: handing over (a wait, the budget), fenced (the
  // instance moved on without us) or a store failure. The first one wins the
  // race against `run()`.
  let handingOver = false;
  let fenced = false;
  let storeFailure: unknown;
  let dueAt = Number.POSITIVE_INFINITY;
  let executed = 0;
  let currentStep: string | undefined;
  const counts = new Map<string, number>();
  const executing = new Set<Promise<unknown>>();
  let stop!: (v: typeof HANDOVER) => void;
  const stopped = new Promise<typeof HANDOVER>((r) => (stop = r));

  /** Hand the instance over at `at` (a new job, due then) and never resolve. */
  const handOver = <T>(at: number): Promise<T> => {
    handingOver = true;
    dueAt = Math.min(dueAt, at);
    stop(HANDOVER);
    return never<T>();
  };

  /** A store call: a fence or a failure stops the run (the step then never resolves). */
  const guarded = async <T>(call: () => Promise<T>): Promise<T> => {
    try {
      return await call();
    } catch (e) {
      if (e instanceof FencedError) fenced = true;
      else if (storeFailure === undefined) storeFailure = e;
      stop(HANDOVER);
      throw STOPPED;
    }
  };
  /** A step's promise once its work rejected: user errors pass, a stop never resolves. */
  const unlessStopped = (e: unknown): Promise<never> => (e === STOPPED ? never() : Promise.reject(e));

  const persist = (row: StepRow, consumeEvent?: string) =>
    guarded(async () => {
      const key = stepKey(row.name, row.n);
      const gained = isFinal(row) && !isFinal(stored.get(key)) ? 1 : 0;
      await store.saveStep(instanceId, epoch, row, { stepsDone: finalCount + gained, currentStep }, consumeEvent);
      stored.set(key, row);
      finalCount += gained;
    });

  const claimKey = (name: string): { key: string; n: number } => {
    if (typeof name !== 'string' || name.length === 0 || name.length > MAX_STEP_NAME) {
      throw new NonRetryableError(`a step name must be 1 to ${MAX_STEP_NAME} characters`);
    }
    const n = counts.get(name) ?? 0;
    counts.set(name, n + 1);
    const key = stepKey(name, n);
    if (stored.size >= MAX_STEPS && !stored.has(key)) {
      throw new NonRetryableError(`an instance may have at most ${MAX_STEPS} steps`);
    }
    return { key, n };
  };

  const stopping = () => fenced || storeFailure !== undefined;
  /** No new execution: the run is stopping, handing over, or out of budget. */
  const mustYield = () => stopping() || handingOver || (executed > 0 && now() >= deadline);
  /** Whether waiting `ms` inside this run is allowed. */
  const canWaitInline = (ms: number) => ms <= inlineWaitMs && now() + ms < deadline && !handingOver;

  /** Work that must settle before the run ends (it persists). Never rejects. */
  const execute = <T>(work: () => Promise<T>): Promise<T> => {
    const p = work();
    const settled = p.then(
      () => undefined,
      () => undefined,
    );
    executing.add(settled);
    settled.then(() => executing.delete(settled));
    return p;
  };

  const runAttempts = async <T>(
    name: string,
    n: number,
    row: StepRow | undefined,
    config: WorkflowStepConfig,
    fn: (ctx: WorkflowStepContext) => Promise<T> | T,
  ): Promise<{ value: T } | { error: StoredError } | { at: number }> => {
    const retries = { ...DEFAULT_RETRIES, ...(config.retries ?? {}) };
    const timeoutMs = config.timeout === undefined ? DEFAULT_STEP_TIMEOUT_MS : toMs(config.timeout);
    let attempts = row?.attempts ?? 0;
    const startedAt = row?.startedAt ?? now();
    for (;;) {
      attempts++;
      executed++;
      currentStep = name;
      let error: StoredError;
      let retryable: boolean;
      try {
        const value = await withTimeout(Promise.resolve().then(() => fn({ attempt: attempts })), timeoutMs, name);
        const output = encodeValue(value, `the result of step "${name}"`);
        await persist({ name, n, kind: 'do', status: 'success', attempts, output, startedAt, finishedAt: now() });
        // The JSON round trip, so this run and every replay see the same value.
        return { value: decodeValue(output) as T };
      } catch (e) {
        if (e === STOPPED) throw e;
        error = toStoredError(e);
        retryable = !(e instanceof NonRetryableError) && attempts <= retries.limit;
      }
      if (!retryable) {
        await persist({ name, n, kind: 'do', status: 'failed', attempts, error, startedAt, finishedAt: now() });
        return { error };
      }
      const nextAt = now() + retryDelayMs(retries, attempts);
      await persist({ name, n, kind: 'do', status: 'retrying', attempts, error, nextAt, startedAt });
      const wait = nextAt - now();
      if (!canWaitInline(wait)) return { at: nextAt };
      if (wait > 0) await sleep(wait);
      if (mustYield()) return { at: nextAt };
    }
  };

  const doStep = <T>(name: string, config: WorkflowStepConfig, fn: (ctx: WorkflowStepContext) => Promise<T> | T): Promise<T> => {
    const { n, key } = claimKey(name);
    const row = stored.get(key);
    if (row?.status === 'success') return Promise.resolve(decodeValue(row.output) as T);
    if (row?.status === 'failed') return Promise.reject(fromStoredError(row.error ?? { name: 'Error', message: 'step failed' }));
    if (stopping()) return never<T>();
    if (row?.status === 'retrying' && row.nextAt !== undefined && row.nextAt > now()) return handOver<T>(row.nextAt);
    if (mustYield()) return handOver<T>(now());
    return execute(() => runAttempts(name, n, row, config, fn)).then((r) => {
      if ('value' in r) return r.value;
      if ('error' in r) throw fromStoredError(r.error);
      return handOver<T>(r.at);
    }, unlessStopped);
  };

  const sleepStep = (name: string, until: (startedAt: number) => number): Promise<void> => {
    const { n, key } = claimKey(name);
    const row = stored.get(key);
    if (row?.status === 'success') return Promise.resolve();
    if (stopping()) return never();
    const startedAt = row?.startedAt ?? now();
    const wakeAt = row?.nextAt ?? until(startedAt);
    return execute(async () => {
      currentStep = name;
      const wait = wakeAt - now();
      if (wait > 0 && !canWaitInline(wait)) {
        if (!row) await persist({ name, n, kind: 'sleep', status: 'sleeping', attempts: 0, nextAt: wakeAt, startedAt });
        return wakeAt;
      }
      if (wait > 0) await sleep(wait);
      if (stopping()) return wakeAt;
      await persist({ name, n, kind: 'sleep', status: 'success', attempts: 0, nextAt: wakeAt, startedAt, finishedAt: now() });
      return null;
    }).then((at) => (at === null ? undefined : handOver<void>(at)), unlessStopped);
  };

  const eventStep = <T>(name: string, options: { type: string; timeout?: WorkflowSleepDuration }): Promise<WorkflowStepEvent<T>> => {
    const { n, key } = claimKey(name);
    const row = stored.get(key);
    if (row?.status === 'success') return Promise.resolve(reviveEvent(decodeValue(row.output)) as WorkflowStepEvent<T>);
    if (row?.status === 'failed') return Promise.reject(fromStoredError(row.error ?? { name: 'Error', message: 'step failed' }));
    if (stopping()) return never();
    if (typeof options?.type !== 'string' || !options.type) return Promise.reject(new NonRetryableError('waitForEvent needs a `type`'));
    const startedAt = row?.startedAt ?? now();
    const timeoutAt =
      row?.nextAt ?? startedAt + (options.timeout === undefined ? DEFAULT_EVENT_TIMEOUT_MS : toMs(options.timeout));
    const base = { name, n, kind: 'event' as const, attempts: 0, eventType: options.type, nextAt: timeoutAt, startedAt };
    return execute(async (): Promise<{ value: unknown } | { error: StoredError } | { at: number }> => {
      currentStep = name;
      const event = await guarded(() => store.findEvent(instanceId, options.type));
      if (event) {
        const value = { payload: decodeValue(event.payload), timestamp: new Date(event.createdAt).toISOString(), type: event.type };
        const output = JSON.stringify(value);
        await persist({ ...base, status: 'success', output, finishedAt: now() }, event.id);
        return { value };
      }
      if (now() >= timeoutAt) {
        const error = { name: 'WorkflowTimeoutError', message: `waitForEvent "${name}" timed out waiting for "${options.type}"` };
        await persist({ ...base, status: 'failed', error, finishedAt: now() });
        return { error };
      }
      if (!row) await persist({ ...base, status: 'waiting' });
      return { at: timeoutAt };
    }).then((r) => {
      if ('value' in r) return reviveEvent(r.value) as WorkflowStepEvent<T>;
      if ('error' in r) throw fromStoredError(r.error);
      return handOver<WorkflowStepEvent<T>>(r.at);
    }, unlessStopped);
  };

  // A step name that breaks a rule is the workflow's error, so it rejects.
  const call = <T>(f: () => Promise<T>): Promise<T> => {
    try {
      return f();
    } catch (e) {
      return Promise.reject(e);
    }
  };

  const step = {
    do(name: string, a: any, b?: any) {
      const [config, fn] = typeof a === 'function' ? [{}, a] : [a ?? {}, b];
      if (typeof fn !== 'function') return Promise.reject(new TypeError('step.do needs a callback'));
      return call(() => doStep(name, config, fn));
    },
    sleep(name: string, duration: WorkflowSleepDuration) {
      return call(() => {
        const ms = toMs(duration);
        return sleepStep(name, (start) => start + ms);
      });
    },
    sleepUntil(name: string, timestamp: Date | number) {
      const at = timestamp instanceof Date ? timestamp.getTime() : Number(timestamp);
      return call(() => sleepStep(name, () => at));
    },
    waitForEvent(name: string, options: { type: string; timeout?: WorkflowSleepDuration }) {
      return call(() => eventStep(name, options));
    },
  } as WorkflowStep;

  // Renew the lease while the run is alive; a renewal that finds the epoch
  // moved fences the run (it was terminated, restarted or taken over).
  const setIv = options.setInterval ?? ((fn: () => void, ms: number) => setInterval(fn, ms));
  const clearIv = options.clearInterval ?? ((h: unknown) => clearInterval(h as ReturnType<typeof setInterval>));
  const renewal = setIv(() => {
    store.renewLease(instanceId, epoch, leaseMs).then(
      (ok) => {
        if (!ok) {
          fenced = true;
          stop(HANDOVER);
        }
      },
      () => {},
    );
  }, options.renewMs ?? RENEW_MS);

  let finish: Finish;
  try {
    const event: WorkflowEvent<any> = {
      payload: decodeValue(instance.params) as any,
      timestamp: new Date(instance.createdAt),
      instanceId,
    };
    let result: { ok: true; value: unknown } | { ok: false; error: unknown } | typeof HANDOVER;
    try {
      const workflow = new cls(ctx, env);
      result = await Promise.race([
        Promise.resolve()
          .then(() => workflow.run(event, step))
          .then(
            (value) => ({ ok: true as const, value }),
            (error) => ({ ok: false as const, error }),
          ),
        stopped,
      ]);
    } catch (error) {
      result = { ok: false, error };
    }
    // Steps still executing (a `Promise.all` sibling) settle and persist first.
    while (executing.size > 0) await Promise.all([...executing]);
    if (storeFailure !== undefined) throw storeFailure;
    if (fenced) return { httpStatus: 200, outcome: 'fenced', stepsExecuted: executed };

    if (result === HANDOVER || handingOver) {
      finish = { status: 'suspend', dueAt: Number.isFinite(dueAt) ? dueAt : now(), claimedWake: wake, newWake: token() };
    } else if (result.ok) {
      try {
        finish = { status: 'complete', output: encodeValue(result.value, 'the workflow output') };
      } catch (e) {
        finish = { status: 'errored', error: toStoredError(e) };
      }
    } else {
      finish = { status: 'errored', error: toStoredError(result.error) };
    }
  } finally {
    clearIv(renewal);
  }

  if (finish.status !== 'suspend') currentStep = undefined;
  try {
    await store.finish(instanceId, epoch, finish, { stepsDone: finalCount, currentStep }, now());
  } catch (e) {
    if (e instanceof FencedError) return { httpStatus: 200, outcome: 'fenced', stepsExecuted: executed };
    throw e;
  }
  if (finish.status === 'suspend') {
    return { httpStatus: 200, outcome: 'suspended', dueAt: finish.dueAt, stepsExecuted: executed };
  }
  return { httpStatus: 200, outcome: finish.status, stepsExecuted: executed };
}

function isFinal(row: StepRow | undefined): boolean {
  return row?.status === 'success' || row?.status === 'failed';
}

function reviveEvent(value: any): WorkflowStepEvent<unknown> {
  return { payload: value?.payload, timestamp: new Date(value?.timestamp), type: value?.type };
}

async function withTimeout<T>(p: Promise<T>, ms: number, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const e = new Error(`step "${name}" timed out after ${ms} ms`);
      e.name = 'WorkflowTimeoutError';
      reject(e);
    }, ms);
  });
  try {
    return await Promise.race([p, timeout]);
  } finally {
    clearTimeout(timer);
  }
}
