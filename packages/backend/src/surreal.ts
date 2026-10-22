// The durable-steps store on SurrealDB, and the small HTTP client it uses.
//
// The client speaks SurrealDB's HTTP `/rpc` with a session token from one
// sign-in (SurrealDB runs Argon2 for every Basic-auth request), and sends binds
// as `LET` statements rather than RPC variables, exactly as the SSP's
// `HttpSqlDb` does: SurrealDB's JSON RPC parses a string variable shaped like a
// record id, datetime or uuid into one, while a quoted string in SurrealQL stays
// a string. JSON is valid SurrealQL for the values sent here.
//
// Every write a run makes is fenced on the instance's `lease_epoch` inside one
// transaction, so a run that lost the instance writes nothing.

import {
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

// =============================================================
// Client
// =============================================================

/** One statement's result per entry, the binds' `LET`s already dropped. */
export type Query = (sql: string, vars?: Record<string, unknown>) => Promise<unknown[]>;

export interface DbConfig {
  /** `http(s)://host[:port]`, a `ws(s)://` one, or either with `/rpc` or `/sql`. */
  url: string;
  ns: string;
  db: string;
  user?: string;
  pass?: string;
  /** `namespace` when the login is a namespace user (the shared free-plan DB). */
  level?: string;
}

/** The database a backend reaches through the `SPKY_DB_*` variables sp00ky injects. */
export function dbConfigFromEnv(get: (name: string) => string | undefined): DbConfig | null {
  const url = get('SPKY_DB_URL');
  if (!url) return null;
  return {
    url,
    ns: get('SPKY_DB_NS') ?? 'main',
    db: get('SPKY_DB_NAME') ?? 'main',
    user: get('SPKY_DB_USER'),
    pass: get('SPKY_DB_PASS'),
    level: get('SPKY_DB_LEVEL'),
  };
}

export function rpcUrl(url: string): string {
  let base = url.trim().replace(/\/+$/, '').replace(/\/(rpc|sql)$/, '');
  if (base.startsWith('wss://')) base = 'https://' + base.slice(6);
  else if (base.startsWith('ws://')) base = 'http://' + base.slice(5);
  return `${base}/rpc`;
}

/** A value as a SurrealQL literal: JSON, with `undefined` as `NONE`. */
export function literal(value: unknown): string {
  if (value === undefined) return 'NONE';
  const text = JSON.stringify(value);
  return text === undefined ? 'NONE' : text;
}

export class QueryError extends Error {
  constructor(message: string, readonly messages: string[] = [message]) {
    super(message);
    this.name = 'QueryError';
  }
}

/** The error SurrealDB reports for the failing statement, not the knock-on "not executed" ones. */
function primaryError(messages: string[]): string {
  return (
    messages.find((m) => !/not executed due to a (failed|cancelled) transaction|Cannot COMMIT/i.test(m)) ??
    messages[0] ??
    'query failed'
  );
}

const CONFLICT_RETRIES = 4;

/** SurrealDB's wording for a transaction that lost a write race (as `ssp-node`'s `is_write_conflict`). */
export function isConflict(message: string): boolean {
  return /write conflict|transaction conflict|can be retried|resource busy/i.test(message);
}

export function httpQuery(config: DbConfig, fetchImpl: typeof fetch = (...a) => fetch(...a)): Query {
  const url = rpcUrl(config.url);
  const headers = { Accept: 'application/json', 'Content-Type': 'application/json' };
  let token: Promise<string | null> | null = null;
  let id = 0;

  const rpc = async (method: string, params: unknown[], auth?: string) => {
    const res = await fetchImpl(url, {
      method: 'POST',
      headers: {
        ...headers,
        ...(auth ? { Authorization: auth } : {}),
        'surreal-ns': config.ns,
        'surreal-db': config.db,
      },
      body: JSON.stringify({ id: ++id, method, params }),
    });
    const text = await res.text();
    let body: any;
    try {
      body = JSON.parse(text);
    } catch {
      body = null;
    }
    return { status: res.status, body, text };
  };

  const signin = async (): Promise<string | null> => {
    if (!config.user || config.pass === undefined) return null;
    const user = { user: config.user, pass: config.pass };
    const levels = [user, { ns: config.ns, ...user }, { ns: config.ns, db: config.db, ...user }];
    if (config.level === 'namespace') levels.unshift(levels.splice(1, 1)[0]);
    for (const params of levels) {
      const r = await rpc('signin', [params]);
      if (typeof r.body?.result === 'string') return r.body.result;
    }
    return null;
  };

  const authorization = async (): Promise<string | undefined> => {
    token ??= signin().catch((e) => {
      token = null;
      throw e;
    });
    const t = await token;
    if (t) return `Bearer ${t}`;
    if (config.user && config.pass !== undefined) {
      return `Basic ${btoa(`${config.user}:${config.pass}`)}`;
    }
    return undefined;
  };

  const run = async (sql: string, vars: Record<string, unknown>, retried: boolean, conflicts = 0): Promise<unknown[]> => {
    const names = Object.keys(vars);
    const lets = names.map((k) => `LET $${k} = ${literal(vars[k])};\n`).join('');
    const r = await rpc('query', [lets + sql], await authorization());
    const authFailed =
      r.status === 401 ||
      r.status === 403 ||
      /authenticat|token has expired|expired token/i.test(r.body?.error?.message ?? '');
    if (authFailed && !retried) {
      token = null;
      return run(sql, vars, true);
    }
    if (r.body?.error) throw new QueryError(String(r.body.error.message ?? JSON.stringify(r.body.error)));
    if (r.status >= 400 || !Array.isArray(r.body?.result)) {
      throw new QueryError(`SurrealDB answered HTTP ${r.status}: ${r.text.slice(0, 300)}`);
    }
    const results = r.body.result.slice(names.length) as Array<{ status: string; result: unknown }>;
    const failed = results.filter((s) => s.status !== 'OK').map((s) => String(s.result));
    if (failed.length > 0) {
      if (failed.some((m) => m.includes('spky:fenced'))) throw new FencedError();
      const primary = primaryError(failed);
      // A lost optimistic-concurrency race (a `sendEvent` committing on the
      // instance row at the same moment) rolled the whole request back: run it
      // again. Every statement here is safe to repeat (fenced, keyed, CAS).
      if (isConflict(primary) && conflicts < CONFLICT_RETRIES) {
        await new Promise((r) => setTimeout(r, 20 * 2 ** conflicts + Math.random() * 20));
        return run(sql, vars, retried, conflicts + 1);
      }
      throw new QueryError(primary, failed);
    }
    return results.map((s) => s.result);
  };

  return (sql, vars = {}) => run(sql, vars, false);
}

// =============================================================
// Store
// =============================================================

const INSTANCE = "type::record('_00_wf_instance', $key)";
const FENCE = `IF (SELECT VALUE lease_epoch FROM ONLY ${INSTANCE}) != $epoch { THROW 'spky:fenced' };`;
const ACTIVE = "['queued', 'running', 'waiting']";
/** A job carrying the token `$new_wake` to `$outbox`, due after `$delay` ms. */
const CREATE_JOB = `CREATE type::record($outbox, string::concat('dwf_', $new_wake)) CONTENT {
  path: $path, payload: { instance: $key, wake: $new_wake }, delay: $delay,
  max_retries: 10, retry_strategy: 'exponential'
};`;

const ms = (v: unknown): number | undefined => {
  if (typeof v !== 'string' && typeof v !== 'number') return undefined;
  const t = typeof v === 'number' ? v : Date.parse(v);
  return Number.isFinite(t) ? t : undefined;
};
const opt = <T>(v: T | null | undefined): T | undefined => (v === null ? undefined : v);
/** `<datetime>$name` when the value is set, `NONE` otherwise. */
const dt = (name: string, value: number | undefined) => (value === undefined ? 'NONE' : `<datetime>$${name}`);
const iso = (v: number | undefined) => (v === undefined ? undefined : new Date(v).toISOString());
const last = (results: unknown[]) => results[results.length - 1] as any;

export class SurrealStore implements Store {
  constructor(private readonly q: Query) {}

  async claim(id: string, wake: string, leaseMs: number): Promise<Claim> {
    const r = last(
      await this.q(
        `LET $now = time::now();
LET $claimed = (UPDATE ${INSTANCE} SET
  lease_epoch = (lease_epoch ?? 0) + 1,
  lease_until = $now + <duration>(string::concat(<string>$lease_ms, 'ms')),
  status = IF status = 'queued' OR status = 'waiting' THEN 'running' ELSE status END,
  started_at = started_at ?? $now,
  updated_at = $now
  WHERE wake = $wake AND status IN ${ACTIVE} AND (lease_until = NONE OR lease_until < $now)
  RETURN AFTER);
LET $cur = (SELECT status, wake = $wake AS current FROM ONLY ${INSTANCE});
RETURN { now: <string>$now, claimed: $claimed[0], cur: $cur };`,
        { key: id, wake, lease_ms: leaseMs },
      ),
    );
    const row = r?.claimed;
    if (row) {
      return {
        kind: 'claimed',
        epoch: Number(row.lease_epoch),
        dbNow: ms(r.now) ?? Date.now(),
        instance: {
          id,
          binding: row.binding,
          outbox: row.outbox,
          path: row.path,
          params: opt(row.params),
          status: row.status,
          createdAt: ms(row.created_at) ?? Date.now(),
        },
      };
    }
    const cur = r?.cur;
    if (!cur) return { kind: 'gone' };
    if (!['queued', 'running', 'waiting'].includes(cur.status)) return { kind: 'done' };
    if (!cur.current) return { kind: 'stale' };
    return { kind: 'busy' };
  }

  async loadSteps(id: string): Promise<StepRow[]> {
    const rows = last(
      await this.q(
        `SELECT name, n, kind, status, attempts, output, error, next_at, event_type, started_at, finished_at
FROM _00_wf_step WHERE instance = ${INSTANCE};`,
        { key: id },
      ),
    ) as any[];
    return (rows ?? []).map((r) => ({
      name: r.name,
      n: Number(r.n),
      kind: r.kind,
      status: r.status,
      attempts: Number(r.attempts ?? 0),
      output: opt(r.output),
      error: opt(r.error),
      nextAt: ms(r.next_at),
      eventType: opt(r.event_type),
      startedAt: ms(r.started_at),
      finishedAt: ms(r.finished_at),
    }));
  }

  async renewLease(id: string, epoch: number, leaseMs: number): Promise<boolean> {
    const rows = last(
      await this.q(
        `UPDATE ${INSTANCE} SET lease_until = time::now() + <duration>(string::concat(<string>$lease_ms, 'ms'))
WHERE lease_epoch = $epoch RETURN VALUE lease_epoch;`,
        { key: id, epoch, lease_ms: leaseMs },
      ),
    );
    return Array.isArray(rows) && rows.length > 0;
  }

  async saveStep(id: string, epoch: number, step: StepRow, progress: Progress, consumeEvent?: string): Promise<void> {
    await this.q(
      `BEGIN TRANSACTION;
${FENCE}
UPSERT type::record('_00_wf_step', [$key, $skey]) CONTENT {
  instance: ${INSTANCE}, name: $name, n: $n, kind: $kind, status: $status, attempts: $attempts,
  output: $output, error: $error, event_type: $event_type,
  next_at: ${dt('next_at', step.nextAt)}, started_at: ${dt('started_at', step.startedAt)},
  finished_at: ${dt('finished_at', step.finishedAt)}, updated_at: time::now()
};
UPDATE ${INSTANCE} SET steps_done = $steps_done, current_step = $current_step, updated_at = time::now();
${consumeEvent ? 'UPDATE type::record($event) SET step = $skey WHERE step = NONE;' : ''}
COMMIT TRANSACTION;`,
      {
        key: id,
        epoch,
        skey: `${step.name}#${step.n}`,
        name: step.name,
        n: step.n,
        kind: step.kind,
        status: step.status,
        attempts: step.attempts,
        output: step.output,
        error: step.error,
        event_type: step.eventType,
        next_at: iso(step.nextAt),
        started_at: iso(step.startedAt),
        finished_at: iso(step.finishedAt),
        steps_done: progress.stepsDone,
        current_step: progress.currentStep,
        event: consumeEvent,
      },
    );
  }

  async findEvent(id: string, type: string): Promise<StoredEvent | null> {
    const rows = last(
      await this.q(
        `SELECT type::string(id) AS id, type, payload, created_at FROM _00_wf_event
WHERE instance = ${INSTANCE} AND type = $type AND step = NONE ORDER BY created_at ASC LIMIT 1;`,
        { key: id, type },
      ),
    ) as any[];
    const r = rows?.[0];
    return r ? { id: r.id, type: r.type, payload: opt(r.payload), createdAt: ms(r.created_at) ?? Date.now() } : null;
  }

  async finish(id: string, epoch: number, finish: Finish, progress: Progress, nowMs: number): Promise<void> {
    if (finish.status === 'suspend') {
      await this.q(
        `BEGIN TRANSACTION;
LET $cur = (SELECT lease_epoch, wake, outbox, path FROM ONLY ${INSTANCE});
IF $cur.lease_epoch != $epoch { THROW 'spky:fenced' };
LET $outbox = $cur.outbox;
LET $path = $cur.path;
LET $delay = IF $cur.wake != $claimed_wake THEN 0 ELSE $delay_ms END;
UPDATE ${INSTANCE} SET wake = $new_wake,
  status = IF $delay > 0 THEN 'waiting' ELSE 'running' END,
  wake_at = time::now() + <duration>(string::concat(<string>$delay, 'ms')),
  lease_until = NONE, steps_done = $steps_done, current_step = $current_step, updated_at = time::now();
${CREATE_JOB}
COMMIT TRANSACTION;`,
        {
          key: id,
          epoch,
          claimed_wake: finish.claimedWake,
          new_wake: finish.newWake,
          delay_ms: Math.max(0, Math.ceil(finish.dueAt - nowMs)),
          steps_done: progress.stepsDone,
          current_step: progress.currentStep,
        },
      );
      return;
    }
    await this.q(
      `BEGIN TRANSACTION;
${FENCE}
UPDATE ${INSTANCE} SET status = $status, output = $output, error = $error, lease_until = NONE, wake_at = NONE,
  steps_done = $steps_done, current_step = NONE, finished_at = time::now(), updated_at = time::now();
COMMIT TRANSACTION;`,
      {
        key: id,
        epoch,
        status: finish.status,
        output: finish.status === 'complete' ? finish.output : undefined,
        error: finish.status === 'errored' ? finish.error : undefined,
        steps_done: progress.stepsDone,
      },
    );
  }

  async create(i: NewInstance): Promise<void> {
    try {
      await this.q(
        `BEGIN TRANSACTION;
CREATE ${INSTANCE} CONTENT {
  binding: $binding, class: $class, outbox: $outbox, path: $path, params: $params,
  owner: ${i.owner ? 'type::record($owner)' : 'NONE'}, status: 'queued', wake: $new_wake, wake_at: time::now(),
  lease_epoch: 0, steps_done: 0, created_at: time::now(), updated_at: time::now()
};
LET $delay = 0;
${CREATE_JOB}
COMMIT TRANSACTION;`,
        {
          key: i.id,
          binding: i.binding,
          class: i.className,
          outbox: i.outbox,
          path: i.path,
          params: i.params,
          owner: i.owner,
          new_wake: i.wake,
        },
      );
    } catch (e) {
      if (e instanceof QueryError && /already exists/i.test(e.message) && e.message.includes('_00_wf_instance')) {
        throw new Error(`workflow instance "${i.id}" already exists`);
      }
      throw e;
    }
  }

  async status(id: string): Promise<{ status: InstanceStatusName; output?: string; error?: StoredError } | null> {
    const r = last(await this.q(`SELECT status, output, error FROM ONLY ${INSTANCE};`, { key: id }));
    return r ? { status: r.status, output: opt(r.output), error: opt(r.error) } : null;
  }

  async terminate(id: string, newWake: string): Promise<boolean> {
    const r = last(
      await this.q(
        `LET $done = (UPDATE ${INSTANCE} SET status = 'terminated', lease_epoch = (lease_epoch ?? 0) + 1,
  lease_until = NONE, wake = $new_wake, wake_at = NONE, current_step = NONE,
  finished_at = time::now(), updated_at = time::now()
  WHERE status IN ${ACTIVE} RETURN VALUE status);
RETURN { done: array::len($done) > 0, exists: (SELECT VALUE status FROM ONLY ${INSTANCE}) != NONE };`,
        { key: id, new_wake: newWake },
      ),
    );
    if (!r?.exists) throw new Error(`workflow instance "${id}" not found`);
    return Boolean(r.done);
  }

  async restart(id: string, newWake: string): Promise<boolean> {
    try {
      await this.q(
        `BEGIN TRANSACTION;
LET $cur = (SELECT outbox, path FROM ONLY ${INSTANCE});
IF $cur = NONE { THROW 'spky:not-found' };
LET $outbox = $cur.outbox;
LET $path = $cur.path;
LET $delay = 0;
DELETE _00_wf_step WHERE instance = ${INSTANCE};
UPDATE ${INSTANCE} SET status = 'queued', output = NONE, error = NONE, lease_epoch = (lease_epoch ?? 0) + 1,
  lease_until = NONE, wake = $new_wake, wake_at = time::now(), steps_done = 0, current_step = NONE,
  started_at = NONE, finished_at = NONE, updated_at = time::now();
${CREATE_JOB}
COMMIT TRANSACTION;`,
        { key: id, new_wake: newWake },
      );
      return true;
    } catch (e) {
      if (e instanceof QueryError && e.message.includes('spky:not-found')) {
        throw new Error(`workflow instance "${id}" not found`);
      }
      throw e;
    }
  }

  async sendEvent(id: string, type: string, payload: string | undefined, newWake: string): Promise<boolean> {
    try {
      const r = last(
        await this.q(
          `BEGIN TRANSACTION;
LET $cur = (SELECT status, outbox, path FROM ONLY ${INSTANCE});
IF $cur = NONE { THROW 'spky:not-found' };
LET $outbox = $cur.outbox;
LET $path = $cur.path;
LET $delay = 0;
LET $active = $cur.status IN ${ACTIVE};
CREATE _00_wf_event CONTENT { instance: ${INSTANCE}, type: $type, payload: $payload, created_at: time::now() };
IF $active {
  UPDATE ${INSTANCE} SET wake = $new_wake, wake_at = time::now(), updated_at = time::now();
  ${CREATE_JOB}
};
COMMIT TRANSACTION;
RETURN $active;`,
          { key: id, type, payload, new_wake: newWake },
        ),
      );
      return Boolean(r);
    } catch (e) {
      if (e instanceof QueryError && e.message.includes('spky:not-found')) {
        throw new Error(`workflow instance "${id}" not found`);
      }
      throw e;
    }
  }
}
