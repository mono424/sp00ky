// @spooky-sync/backend: the sp00ky backend SDK.
//
// Durable workflows with Cloudflare Workflows' API that run on sp00ky's own
// job engine, on a VM, under `spky dev` and as a Cloudflare Worker backend:
//
//   import { WorkflowEntrypoint, withWorkflows } from '@spooky-sync/backend';
//   export class Ingest extends WorkflowEntrypoint<Env, Params> { async run(event, step) { ... } }
//   export default withWorkflows(app, { INGEST: Ingest });
//
// See docs/durable-steps.md.

import {
  ROUTE_PREFIX,
  decodeValue,
  encodeValue,
  newToken,
  runInstance,
  validInstanceId,
  type ExecutionContextLike,
  type InstanceStatus,
  type RunOptions,
  type Store,
  type WorkflowClass,
} from './engine';
import { SurrealStore, dbConfigFromEnv, httpQuery, type Query } from './surreal';

export {
  WorkflowEntrypoint,
  NonRetryableError,
  FencedError,
  runInstance,
  toMs,
  retryDelayMs,
  budgetMs,
  ROUTE_PREFIX,
  DEFAULT_RETRIES,
  MAX_VALUE_BYTES,
  type WorkflowEvent,
  type WorkflowStep,
  type WorkflowStepConfig,
  type WorkflowStepContext,
  type WorkflowStepEvent,
  type WorkflowSleepDuration,
  type WorkflowBackoff,
  type InstanceStatus,
  type InstanceStatusName,
  type ExecutionContextLike,
  type WorkflowClass,
  type Store,
  type StepRow,
  type InstanceRow,
  type Claim,
  type Finish,
  type NewInstance,
  type Progress,
  type StoredEvent,
  type StoredError,
  type RunOptions,
  type RunResult,
} from './engine';
export { SurrealStore, httpQuery, dbConfigFromEnv, isConflict, rpcUrl, QueryError, type Query, type DbConfig } from './surreal';
export { MemoryStore, type MemoryJob, type MemoryInstance } from './memory';

// =============================================================
// Bindings (Cloudflare's `Workflow` / `WorkflowInstance`)
// =============================================================

export interface WorkflowInstanceCreateOptions<Params = unknown> {
  /** Cloudflare's rule: `^[a-zA-Z0-9_][a-zA-Z0-9-_]*$`, at most 100 characters. A random UUID when omitted. */
  id?: string;
  params?: Params;
  /** sp00ky: the record (`user:abc`) allowed to read this instance and its steps. Root-only when omitted. */
  owner?: string;
}

export interface WorkflowInstance {
  readonly id: string;
  status(): Promise<InstanceStatus>;
  terminate(): Promise<void>;
  restart(): Promise<void>;
  sendEvent(event: { type: string; payload?: unknown }): Promise<void>;
  pause(): Promise<void>;
  resume(): Promise<void>;
}

export interface Workflow<Params = unknown> {
  create(options?: WorkflowInstanceCreateOptions<Params>): Promise<WorkflowInstance>;
  createBatch(batch: WorkflowInstanceCreateOptions<Params>[]): Promise<WorkflowInstance[]>;
  get(id: string): Promise<WorkflowInstance>;
}

class Instance implements WorkflowInstance {
  constructor(
    readonly id: string,
    private readonly store: Store,
  ) {}

  async status(): Promise<InstanceStatus> {
    const s = await this.store.status(this.id);
    if (!s) return { status: 'unknown' };
    const out: InstanceStatus = { status: s.status };
    if (s.error) out.error = s.error;
    if (s.output !== undefined) out.output = decodeValue(s.output);
    return out;
  }

  async terminate(): Promise<void> {
    await this.store.terminate(this.id, newToken());
  }

  async restart(): Promise<void> {
    await this.store.restart(this.id, newToken());
  }

  async sendEvent(event: { type: string; payload?: unknown }): Promise<void> {
    if (typeof event?.type !== 'string' || !event.type) throw new TypeError('sendEvent needs a `type`');
    await this.store.sendEvent(this.id, event.type, encodeValue(event.payload, 'the event payload'), newToken());
  }

  async pause(): Promise<void> {
    throw new Error('pause() is not supported by sp00ky durable workflows yet');
  }

  async resume(): Promise<void> {
    throw new Error('resume() is not supported by sp00ky durable workflows yet');
  }
}

/** The `env.<binding>` object for one workflow class. */
export function workflowBinding<Params = unknown>(
  store: Store,
  binding: string,
  cls: WorkflowClass,
  outbox: string,
): Workflow<Params> {
  const create = async (options: WorkflowInstanceCreateOptions<Params> = {}) => {
    const id = options.id ?? crypto.randomUUID();
    if (!validInstanceId(id)) {
      throw new TypeError(`invalid workflow instance id "${id}": use letters, digits, - and _ (at most 100)`);
    }
    await store.create({
      id,
      binding,
      className: cls.name,
      outbox,
      path: ROUTE_PREFIX + binding,
      params: encodeValue(options.params, 'the workflow params'),
      owner: options.owner,
      wake: newToken(),
    });
    return new Instance(id, store);
  };
  return {
    create,
    async createBatch(batch) {
      const out: WorkflowInstance[] = [];
      for (const options of batch) out.push(await create(options));
      return out;
    },
    async get(id) {
      if (!(await store.status(id))) throw new Error(`workflow instance "${id}" not found`);
      return new Instance(id, store);
    },
  };
}

// =============================================================
// The wrapper
// =============================================================

type Env = Record<string, any>;
type FetchFn = (request: Request, env?: any, ctx?: any) => Response | Promise<Response>;
type Handler = { fetch: FetchFn; [key: string]: any };

export interface WorkflowsOptions {
  /** The outbox table the jobs go to. Default `SPKY_OUTBOX_TABLE`, else `job`. */
  outbox?: string | ((env: Env) => string | undefined);
  /** Where instances and steps live. Default SurrealDB from the `SPKY_DB_*` variables. */
  store?: (env: Env) => Store;
  /** Or just the query function the SurrealDB store should use. */
  query?: (env: Env) => Query;
  /** A bearer token the workflow route requires. Default `SPKY_JOB_TOKEN`, else none (the wake token is the capability). */
  token?: string | ((env: Env) => string | undefined);
  /** Engine knobs and test seams. */
  run?: Omit<RunOptions, 'jobTimeoutMs'>;
}

/** The handler methods besides `fetch` a Worker may export; kept on the wrapped object. */
const HANDLER_METHODS = ['scheduled', 'queue', 'email', 'tail', 'trace', 'tailStream'];

const stores = new Map<string, Store>();

function lookup(env: Env | undefined): (name: string) => string | undefined {
  const processEnv = (globalThis as any).process?.env as Record<string, string | undefined> | undefined;
  return (name) => {
    const v = env?.[name];
    if (typeof v === 'string') return v;
    return processEnv?.[name];
  };
}

function defaultStore(env: Env | undefined): Store {
  const config = dbConfigFromEnv(lookup(env));
  if (!config) {
    throw new Error(
      'durable workflows need a database: set SPKY_DB_URL (sp00ky injects SPKY_DB_* into backends) or pass `store` to withWorkflows',
    );
  }
  const key = [config.url, config.ns, config.db, config.user ?? ''].join('|');
  let store = stores.get(key);
  if (!store) stores.set(key, (store = new SurrealStore(httpQuery(config))));
  return store;
}

const nodeCtx: ExecutionContextLike = {
  waitUntil(p) {
    Promise.resolve(p).catch(() => {});
  },
  passThroughOnException() {},
};

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
}

/**
 * Wrap a backend's fetch handler (a Hono app, an `{ fetch }` object or a plain
 * function) so that `env.<binding>` is the workflow binding in every request,
 * and `POST /__spky/workflow/<binding>` (the route the job runner calls) runs
 * the instance. Works the same on Node and on Workers.
 */
export function withWorkflows<H extends Handler | FetchFn>(
  handler: H,
  classes: Record<string, WorkflowClass>,
  options: WorkflowsOptions = {},
): { fetch: (request: Request, env?: any, ctx?: any) => Promise<Response>; env: (env?: any) => any } & Partial<Handler> {
  for (const binding of Object.keys(classes)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(binding)) throw new TypeError(`invalid workflow binding name "${binding}"`);
  }
  const inner: FetchFn = typeof handler === 'function' ? handler : handler.fetch.bind(handler);
  const storeFor = (env: Env | undefined): Store =>
    options.store?.(env ?? {}) ??
    (options.query ? new SurrealStore(options.query(env ?? {})) : defaultStore(env));
  const outboxFor = (env: Env | undefined): string =>
    (typeof options.outbox === 'function' ? options.outbox(env ?? {}) : options.outbox) ??
    lookup(env)('SPKY_OUTBOX_TABLE') ??
    'job';
  const tokenFor = (env: Env | undefined): string | undefined =>
    (typeof options.token === 'function' ? options.token(env ?? {}) : options.token) ?? lookup(env)('SPKY_JOB_TOKEN');

  /** `env` plus one binding per workflow; the store is resolved on first use. */
  const withBindings = (env: Env | undefined): Env => {
    const out: Env = { ...(env ?? {}) };
    for (const [binding, cls] of Object.entries(classes)) {
      let b: Workflow | undefined;
      const get = () => (b ??= workflowBinding(storeFor(env), binding, cls, outboxFor(env)));
      out[binding] = {
        create: (o?: any) => get().create(o),
        createBatch: (o: any[]) => get().createBatch(o),
        get: (id: string) => get().get(id),
      } satisfies Workflow;
    }
    return out;
  };

  const route = async (request: Request, env: Env | undefined, ctx: ExecutionContextLike | undefined, binding: string) => {
    const cls = classes[binding];
    if (!cls) return json(404, { error: `no workflow binding "${binding}"` });
    const required = tokenFor(env);
    if (required && request.headers.get('authorization') !== `Bearer ${required}`) {
      return json(401, { error: 'unauthorized' });
    }
    let body: any;
    try {
      body = await request.json();
    } catch {
      return json(400, { error: 'expected a JSON body { instance, wake }' });
    }
    if (typeof body?.instance !== 'string' || typeof body?.wake !== 'string') {
      return json(400, { error: 'expected a JSON body { instance, wake }' });
    }
    const timeout = Number(request.headers.get('x-spky-job-timeout-ms'));
    try {
      const r = await runInstance(storeFor(env), cls, withBindings(env), ctx ?? nodeCtx, body.instance, body.wake, {
        ...options.run,
        jobTimeoutMs: Number.isFinite(timeout) && timeout > 0 ? timeout : undefined,
      });
      return json(r.httpStatus, {
        outcome: r.outcome,
        steps: r.stepsExecuted,
        ...(r.dueAt !== undefined ? { due_at: new Date(r.dueAt).toISOString() } : {}),
      });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      console.error(JSON.stringify({ level: 'ERROR', message: 'durable workflow run failed', binding, instance: body.instance, error: message }));
      return json(500, { error: message });
    }
  };

  const wrapped: any = {
    async fetch(request: Request, env?: any, ctx?: any): Promise<Response> {
      if (request.method === 'POST') {
        const path = new URL(request.url).pathname;
        if (path.startsWith(ROUTE_PREFIX)) return route(request, env, ctx, decodeURIComponent(path.slice(ROUTE_PREFIX.length)));
      }
      return inner(request, withBindings(env), ctx);
    },
    /** The env with the workflow bindings, for code outside a request (a script, a test). */
    env: (env?: any) => withBindings(env),
  };
  if (typeof handler !== 'function') {
    for (const m of HANDLER_METHODS) {
      if (typeof (handler as Handler)[m] === 'function') {
        const f = (handler as Handler)[m];
        wrapped[m] = (event: unknown, env: any, ctx: any) => f.call(handler, event, withBindings(env), ctx);
      }
    }
  }
  return wrapped;
}
