// An in-memory `Store` with the same compare-and-swap rules as the SurrealDB
// one, for testing workflows without a database. Every method is one atomic
// step (JavaScript is single threaded and nothing here awaits in the middle),
// which is what a SurrealDB transaction gives the real store.
//
// It also keeps the outbox: `jobs` holds the rows the engine created, so a test
// can play the job runner (see `drainJobs`).

import {
  ACTIVE_STATUSES,
  FencedError,
  type Claim,
  type Finish,
  type InstanceStatusName,
  type NewInstance,
  type Progress,
  type StepRow,
  type Store,
  type StoredError,
  type StoredEvent,
} from './engine';

export interface MemoryInstance extends NewInstance {
  status: InstanceStatusName;
  output?: string;
  error?: StoredError;
  leaseEpoch: number;
  leaseUntil?: number;
  wakeAt?: number;
  stepsDone: number;
  currentStep?: string;
  createdAt: number;
  startedAt?: number;
  finishedAt?: number;
}

export interface MemoryJob {
  id: string;
  outbox: string;
  path: string;
  payload: { instance: string; wake: string };
  delay: number;
  createdAt: number;
  status: 'pending' | 'processing' | 'success' | 'failed';
  retries: number;
  maxRetries: number;
}

export class MemoryStore implements Store {
  readonly instances = new Map<string, MemoryInstance>();
  readonly steps = new Map<string, Map<string, StepRow>>();
  readonly events: Array<StoredEvent & { instance: string; step?: string }> = [];
  readonly jobs: MemoryJob[] = [];
  /** Store calls to fail once, by method name: a database that is briefly unreachable. */
  readonly failNext = new Set<string>();
  private seq = 0;

  constructor(readonly now: () => number = Date.now) {}

  private trip(method: string) {
    if (this.failNext.delete(method)) throw new Error(`memory store: ${method} failed (injected)`);
  }

  private job(i: { outbox: string; path: string; id: string }, wake: string, delay: number) {
    this.jobs.push({
      id: `${i.outbox}:dwf_${wake}`,
      outbox: i.outbox,
      path: i.path,
      payload: { instance: i.id, wake },
      delay,
      createdAt: this.now(),
      status: 'pending',
      retries: 0,
      maxRetries: 10,
    });
  }

  private fence(id: string, epoch: number): MemoryInstance {
    const i = this.instances.get(id);
    if (!i || i.leaseEpoch !== epoch) throw new FencedError();
    return i;
  }

  async claim(id: string, wake: string, leaseMs: number): Promise<Claim> {
    this.trip('claim');
    const i = this.instances.get(id);
    const now = this.now();
    if (!i) return { kind: 'gone' };
    if (!(ACTIVE_STATUSES as readonly string[]).includes(i.status)) return { kind: 'done' };
    if (i.wake !== wake) return { kind: 'stale' };
    if (i.leaseUntil !== undefined && i.leaseUntil >= now) return { kind: 'busy' };
    i.leaseEpoch++;
    i.leaseUntil = now + leaseMs;
    if (i.status === 'queued' || i.status === 'waiting') i.status = 'running';
    i.startedAt ??= now;
    return {
      kind: 'claimed',
      epoch: i.leaseEpoch,
      dbNow: now,
      instance: { id, binding: i.binding, outbox: i.outbox, path: i.path, params: i.params, status: i.status, createdAt: i.createdAt },
    };
  }

  async loadSteps(id: string): Promise<StepRow[]> {
    this.trip('loadSteps');
    return [...(this.steps.get(id)?.values() ?? [])].map((s) => ({ ...s }));
  }

  async renewLease(id: string, epoch: number, leaseMs: number): Promise<boolean> {
    const i = this.instances.get(id);
    if (!i || i.leaseEpoch !== epoch) return false;
    i.leaseUntil = this.now() + leaseMs;
    return true;
  }

  async saveStep(id: string, epoch: number, step: StepRow, progress: Progress, consumeEvent?: string): Promise<void> {
    this.trip('saveStep');
    const i = this.fence(id, epoch);
    let rows = this.steps.get(id);
    if (!rows) this.steps.set(id, (rows = new Map()));
    rows.set(`${step.name}#${step.n}`, { ...step });
    i.stepsDone = progress.stepsDone;
    i.currentStep = progress.currentStep;
    if (consumeEvent) {
      const ev = this.events.find((e) => e.id === consumeEvent && e.step === undefined);
      if (ev) ev.step = `${step.name}#${step.n}`;
    }
  }

  async findEvent(id: string, type: string): Promise<StoredEvent | null> {
    this.trip('findEvent');
    const ev = this.events.find((e) => e.instance === id && e.type === type && e.step === undefined);
    return ev ? { id: ev.id, type: ev.type, payload: ev.payload, createdAt: ev.createdAt } : null;
  }

  async finish(id: string, epoch: number, finish: Finish, progress: Progress, nowMs: number): Promise<void> {
    this.trip('finish');
    const i = this.fence(id, epoch);
    i.stepsDone = progress.stepsDone;
    i.leaseUntil = undefined;
    if (finish.status === 'suspend') {
      const delay = i.wake !== finish.claimedWake ? 0 : Math.max(0, Math.ceil(finish.dueAt - nowMs));
      i.wake = finish.newWake;
      i.status = delay > 0 ? 'waiting' : 'running';
      i.wakeAt = this.now() + delay;
      i.currentStep = progress.currentStep;
      this.job(i, finish.newWake, delay);
      return;
    }
    i.status = finish.status;
    i.output = finish.status === 'complete' ? finish.output : undefined;
    i.error = finish.status === 'errored' ? finish.error : undefined;
    i.wakeAt = undefined;
    i.currentStep = undefined;
    i.finishedAt = this.now();
  }

  async create(n: NewInstance): Promise<void> {
    if (this.instances.has(n.id)) throw new Error(`workflow instance "${n.id}" already exists`);
    this.instances.set(n.id, { ...n, status: 'queued', leaseEpoch: 0, stepsDone: 0, createdAt: this.now(), wakeAt: this.now() });
    this.job(n, n.wake, 0);
  }

  async status(id: string) {
    const i = this.instances.get(id);
    return i ? { status: i.status, output: i.output, error: i.error } : null;
  }

  async terminate(id: string, newWake: string): Promise<boolean> {
    const i = this.instances.get(id);
    if (!i) throw new Error(`workflow instance "${id}" not found`);
    if (!(ACTIVE_STATUSES as readonly string[]).includes(i.status)) return false;
    Object.assign(i, { status: 'terminated', leaseEpoch: i.leaseEpoch + 1, leaseUntil: undefined, wake: newWake, wakeAt: undefined, currentStep: undefined, finishedAt: this.now() });
    return true;
  }

  async restart(id: string, newWake: string): Promise<boolean> {
    const i = this.instances.get(id);
    if (!i) throw new Error(`workflow instance "${id}" not found`);
    this.steps.delete(id);
    Object.assign(i, {
      status: 'queued',
      output: undefined,
      error: undefined,
      leaseEpoch: i.leaseEpoch + 1,
      leaseUntil: undefined,
      wake: newWake,
      wakeAt: this.now(),
      stepsDone: 0,
      currentStep: undefined,
      startedAt: undefined,
      finishedAt: undefined,
    });
    this.job(i, newWake, 0);
    return true;
  }

  async sendEvent(id: string, type: string, payload: string | undefined, newWake: string): Promise<boolean> {
    const i = this.instances.get(id);
    if (!i) throw new Error(`workflow instance "${id}" not found`);
    this.events.push({ id: `_00_wf_event:e${++this.seq}`, instance: id, type, payload, createdAt: this.now() });
    if (!(ACTIVE_STATUSES as readonly string[]).includes(i.status)) return false;
    i.wake = newWake;
    i.wakeAt = this.now();
    this.job(i, newWake, 0);
    return true;
  }
}
