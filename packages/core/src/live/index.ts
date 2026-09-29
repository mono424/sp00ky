/**
 * `@spooky-sync/core/live`: a wasm-free live subscriber.
 *
 * Speaks the exact server contract the full client uses, over the pure-JS
 * `surrealdb` SDK and one WebSocket, with no local store, no circuit, no
 * logger and no `window`: it runs in a service worker, a dedicated worker and
 * Node.
 *
 * Per query:
 * - `fn::query::register($config)` plus the membership read-back in one
 *   request (`registerSelect` / `registerVars`, the same builders the full
 *   client's `registerRemote` uses; `$config = { id: _00_query:<key>, surql,
 *   params, ttl }`, key = sha256(JSON.stringify({ surql, params, sessionId })));
 * - bodies with `SELECT * FROM $ids`;
 * - one `LIVE SELECT * FROM <the user's _00_list_ref table> FETCH out` per
 *   feed, so a changed row usually arrives inline with its edge; an edge whose
 *   `out` is a bare id (a server that ignores FETCH) costs a body read, one
 *   whose `out` is null (unreadable or deleted row) a membership re-read;
 * - `fn::query::heartbeat` at half the ttl, re-registering a reclaimed view;
 * - `fn::query::unsubscribe` for every view on `unsubscribe()` / `close()`.
 *
 * Rows are the SDK's decoded values (`id` is a `RecordId`), unordered: apply
 * the query's order yourself if it matters. Related children (`.related()`)
 * are not joined in; subscribe to them separately when you need them.
 */
import { RecordId, Surreal } from 'surrealdb';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import { queryHashInput } from '../query/hash';
import {
  bodySelect,
  heartbeatBatch,
  heartbeatRowGone,
  registerSelect,
  registerVars,
  singleSnapshotSelect,
  type RegisterPayload,
} from '../query/sql';
import { metaFromRow, type QueryMetaRow } from '../query/membership';
import {
  ANON_USER_ID,
  DEFAULT_REF_MODE,
  listRefTableFor,
  type RefMode,
} from '../modules/ref-tables';
import { decodeTokenClaims } from '../modules/auth/impersonation';
import { hashOfEdge, rowOfEdge } from '../utils/edges';
import { sha256Hex } from '../utils/sha256';
import { parseQueryParams } from '../utils/parser';
import { encodeRecordId, parseDuration, parseRecordIdString, withTimeout } from '../utils/index';

export type LiveToken = string | null | undefined;
/** A token, or a function returning one (called on every connect). */
export type LiveTokenSource = string | (() => LiveToken | Promise<LiveToken>);

/** The subset of the `surrealdb` SDK's `Surreal` the feed drives. */
export interface LiveSurreal {
  connect(url: string, options?: Record<string, unknown>): Promise<unknown>;
  use(what: { namespace: string; database: string }): Promise<unknown>;
  authenticate(token: string): Promise<unknown>;
  query(sql: string, vars?: Record<string, unknown>): PromiseLike<unknown>;
  liveOf(id: never): PromiseLike<LiveStreamLike>;
  close(): Promise<unknown>;
  subscribe?(event: string, listener: (...args: unknown[]) => void): () => void;
}

export interface LiveStreamLike {
  subscribe(handler: (message: LiveMessageLike) => void): () => void;
  kill?(): Promise<void>;
}

export interface LiveMessageLike {
  action: string;
  value: unknown;
}

export interface LiveFeedOptions {
  /** SurrealDB endpoint, `wss://host/rpc` (http(s) is rewritten to ws(s)). */
  endpoint: string;
  namespace: string;
  database: string;
  /**
   * The session token (`db.auth.token` on the page, `getBridgedToken()` in the
   * service worker). None: the anonymous `_00_list_ref_anon` table, which
   * needs `anonymousLiveQueries` on the server.
   */
  token?: LiveTokenSource;
  /** `_00_list_ref` layout. Default `dedicated` (one table per user), as the SSP. */
  refMode?: RefMode;
  /** View ttl sent with every registration. Default `10m`, the full client's. */
  ttl?: string;
  /** Salt of the `_00_query` keys. Default: a fresh random id per feed. */
  sessionId?: string;
  /**
   * The generated schema. Used to type query params (record ids, datetimes)
   * exactly like the full client does before registering. Taken from a query
   * builder's own schema when omitted.
   */
  schema?: SchemaStructure;
  /** The server refused the token. Called once, before the connect rejects. */
  onUnauthorized?: (error: unknown) => void | Promise<void>;
  /** Background failures (LIVE, heartbeat, re-reads). Never thrown. */
  onError?: (error: unknown) => void;
  /** Deadline for every statement, ms. Default 15000. */
  timeoutMs?: number;
  /** Bring your own SDK client (tests, a custom WebSocket implementation). */
  surreal?: LiveSurreal | (() => LiveSurreal);
}

/** `{ surql, params }`, or anything the query builder builds (`FinalQuery`, `InnerQuery`). */
export type LiveQuery =
  | { surql: string; params?: Record<string, unknown>; table?: string }
  | BuiltQueryLike;

interface SelectInfoLike {
  query: string;
  vars?: Record<string, unknown>;
}
interface InnerQueryLike {
  selectQuery: SelectInfoLike;
  tableName: string;
}
/** A `FinalQuery` (via its `innerQuery`) or an `InnerQuery`. */
type BuiltQueryLike = { innerQuery: InnerQueryLike } | InnerQueryLike;

export interface LiveChange<T> {
  added: T[];
  updated: T[];
  /** The last known body of each row that left the set. */
  removed: T[];
}

export interface LiveHandlers<T> {
  /** The whole current set: once when it first loads, then after every change. */
  onSet(rows: T[]): void;
  /** What changed, after the first set. */
  onChange?(change: LiveChange<T>): void;
  /** The registration or a later re-read failed. */
  onError?(error: unknown): void;
}

export interface LiveFeedSubscription<T> {
  /** The first full set; rejects when the registration failed. */
  readonly ready: Promise<T[]>;
  /** The current rows, unordered. Empty until `ready`. */
  rows(): T[];
  /** The view's `_00_query` key, once computed. */
  readonly key: string | null;
  /** Stop listening; releases the server view when nobody else on this feed watches it. */
  unsubscribe(): Promise<void>;
}

export interface LiveIdleOptions {
  /** Give up after this long and resolve `false`. Default 10000. */
  timeoutMs?: number;
  /** How long nothing must have arrived. Default 300. */
  quietMs?: number;
}

export interface LiveFeed {
  subscribe<T = Record<string, unknown>>(
    query: LiveQuery,
    handlers: LiveHandlers<T>
  ): LiveFeedSubscription<T>;
  /** Run statements on the feed's connection (small writes, `fn::push::*`). The SDK's result array. */
  query<T extends unknown[] = unknown[]>(surql: string, vars?: Record<string, unknown>): Promise<T>;
  /** `true` once every subscription has its first set and nothing arrived for `quietMs`. */
  idle(options?: LiveIdleOptions): Promise<boolean>;
  /** Connect now (subscribe/query do it lazily). */
  connect(): Promise<void>;
  /** Release every view, kill LIVE, close the socket. Idempotent. */
  close(): Promise<void>;
  /** `$auth.id` from the token (`user:abc`), null when anonymous. Known after connect. */
  readonly userId: string | null;
  /** The `_00_list_ref` table LIVE watches. Known after connect. */
  readonly listRefTable: string | null;
  readonly closed: boolean;
}

export type LiveFeedErrorCode = 'closed' | 'unauthorized' | 'connect' | 'register' | 'query';

export class LiveFeedError extends Error {
  constructor(
    public readonly code: LiveFeedErrorCode,
    message: string,
    public readonly cause?: unknown
  ) {
    super(message);
    this.name = 'LiveFeedError';
  }
}

export function createLiveFeed(options: LiveFeedOptions): LiveFeed {
  return new Feed(options);
}

// ── internals ────────────────────────────────────────────────────────────

const MATERIALIZE_RETRIES = 20;
const MATERIALIZE_RETRY_MS = 150;
const FLUSH_MS = 10;
const FETCH_BATCH_MS = 15;
const REREAD_MS = 50;
const RELEASE_TIMEOUT_MS = 2_000;

interface Edge {
  rid: RecordId;
  version: number;
}

interface Held {
  version: number;
  row: Record<string, unknown>;
}

interface Listener {
  handlers: LiveHandlers<any>;
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // A subscription nobody awaits must not surface as an unhandled rejection.
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

class SubState {
  readonly rid: RecordId<string>;
  readonly payload: RegisterPayload;
  readonly rows = new Map<string, Held>();
  readonly listeners = new Set<Listener>();
  readonly ready = deferred<Record<string, unknown>[]>();
  loaded = false;
  failed = false;
  registered = false;
  /** A LIVE event hit this view before its first set landed. */
  dirtyDuringLoad = false;
  pending: {
    added: Map<string, Record<string, unknown>>;
    updated: Map<string, Record<string, unknown>>;
    removed: Map<string, Record<string, unknown>>;
  } = {
    added: new Map(),
    updated: new Map(),
    removed: new Map(),
  };
  flushTimer: ReturnType<typeof setTimeout> | null = null;
  rereadTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    readonly key: string,
    surql: string,
    params: Record<string, unknown>,
    ttl: string
  ) {
    this.rid = new RecordId('_00_query', key);
    this.payload = { id: this.rid, surql, params, ttl };
  }

  list(): Record<string, unknown>[] {
    return [...this.rows.values()].map((h) => h.row);
  }
}

function isBuilt(query: LiveQuery): query is BuiltQueryLike {
  const q = query as { innerQuery?: InnerQueryLike; selectQuery?: SelectInfoLike } | null;
  return !!(q && (q.innerQuery?.selectQuery || q.selectQuery));
}

/** The register input the full client derives from the same query. */
export function liveQueryInput(
  query: LiveQuery,
  schema?: SchemaStructure
): { table: string; surql: string; params: Record<string, unknown> } {
  if (isBuilt(query)) {
    const inner = ('innerQuery' in query ? query.innerQuery : query) as InnerQueryLike & {
      schema?: SchemaStructure;
    };
    const info = inner.selectQuery;
    const table = inner.tableName;
    // The builders keep their schema in a private field; reading it is how a
    // bare `FinalQuery` still gets the full client's param typing.
    const s = schema ?? inner.schema ?? (query as { schema?: SchemaStructure }).schema;
    const columns = s?.tables?.find((t) => t.name === table)?.columns;
    const vars = info.vars ?? {};
    return {
      table,
      surql: info.query,
      params: columns ? parseQueryParams(columns as never, vars) : { ...vars },
    };
  }
  const raw = query as { surql?: unknown; params?: Record<string, unknown>; table?: string };
  if (typeof raw?.surql !== 'string')
    throw new TypeError('live feed: expected a built query or { surql, params }');
  const table = raw.table ?? /\bFROM\s+([A-Za-z0-9_]+)/i.exec(raw.surql)?.[1] ?? '';
  return { table, surql: raw.surql, params: { ...raw.params } };
}

/** `http(s)://` becomes `ws(s)://`; a bare host gets `/rpc`. */
export function liveEndpoint(endpoint: string): string {
  try {
    const url = new URL(endpoint);
    if (url.protocol === 'http:') url.protocol = 'ws:';
    else if (url.protocol === 'https:') url.protocol = 'wss:';
    if (url.pathname === '' || url.pathname === '/') url.pathname = '/rpc';
    return url.toString();
  } catch {
    return endpoint;
  }
}

function asRecordId(value: unknown): RecordId | null {
  if (value instanceof RecordId) return value;
  if (typeof value === 'string' && value.includes(':')) return parseRecordIdString(value);
  return null;
}

function keyOf(rid: RecordId): string {
  return encodeRecordId(rid as RecordId<string>);
}

/** `SELECT out, version ...` rows into edges, one per id at its highest version. */
function parseEdges(result: unknown): Map<string, Edge> | null {
  if (!Array.isArray(result)) return null;
  const out = new Map<string, Edge>();
  for (const row of result as Array<{ out?: unknown; version?: unknown }>) {
    const rid = asRecordId(row?.out);
    if (!rid) continue;
    const version = typeof row.version === 'number' ? row.version : 0;
    const key = keyOf(rid);
    const held = out.get(key);
    if (!held || held.version < version) out.set(key, { rid, version });
  }
  return out;
}

interface Snapshot {
  edges: Map<string, Edge>;
  state: string | null;
}

/** The three membership statements starting at `offset`: edges, meta, children. */
function snapshotAt(results: unknown, offset: number): Snapshot | null {
  if (!Array.isArray(results)) return null;
  const edges = parseEdges(results[offset]);
  if (!edges) return null;
  const meta = metaFromRow(results[offset + 1] as QueryMetaRow | null);
  return { edges, state: meta.state };
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function randomId(): string {
  const c = (globalThis as { crypto?: Crypto }).crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `s${Date.now().toString(36)}${Math.random().toString(36).slice(2, 12)}`;
}

async function resolveToken(source: LiveTokenSource | undefined): Promise<string | null> {
  if (!source) return null;
  const value = typeof source === 'function' ? await source() : source;
  return typeof value === 'string' && value.length > 0 ? value : null;
}

class Feed implements LiveFeed {
  private db: LiveSurreal | null = null;
  private connecting: Promise<void> | null = null;
  private uid: string | null = null;
  private table: string | null = null;
  private live: { id: unknown; stream: LiveStreamLike; off: () => void } | null = null;
  private liveStarting: Promise<boolean> | null = null;
  private readonly subs = new Map<string, SubState>();
  private readonly sessionId: string;
  private readonly ttl: string;
  private readonly timeoutMs: number;
  private isClosed = false;
  private lastActivity = Date.now();
  private busy = 0;
  private starting = 0;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private fetchTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly fetchQueue = new Map<
    string,
    { rid: RecordId; waiters: Array<{ sub: SubState; version: number }> }
  >();
  private readonly offs: Array<() => void> = [];
  private dropped = false;

  constructor(private readonly options: LiveFeedOptions) {
    this.sessionId = options.sessionId ?? randomId();
    this.ttl = options.ttl ?? '10m';
    this.timeoutMs = options.timeoutMs ?? 15_000;
  }

  get userId(): string | null {
    return this.uid;
  }

  get listRefTable(): string | null {
    return this.table;
  }

  get closed(): boolean {
    return this.isClosed;
  }

  // ── connection ──

  connect(): Promise<void> {
    if (this.isClosed) return Promise.reject(new LiveFeedError('closed', 'live feed is closed'));
    if (this.db) return Promise.resolve();
    if (!this.connecting) {
      this.connecting = this.open().catch((error) => {
        this.connecting = null;
        throw error;
      });
    }
    return this.connecting;
  }

  private makeSurreal(): LiveSurreal {
    const given = this.options.surreal;
    if (typeof given === 'function') return given();
    if (given) return given;
    return new Surreal() as unknown as LiveSurreal;
  }

  private async open(): Promise<void> {
    const token = await resolveToken(this.options.token);
    this.uid = token ? decodeTokenClaims(token).userId : null;
    this.table = listRefTableFor(
      this.options.refMode ?? DEFAULT_REF_MODE,
      this.uid ?? ANON_USER_ID
    );
    const db = this.makeSurreal();
    try {
      await db.connect(liveEndpoint(this.options.endpoint), {
        reconnect: {
          enabled: true,
          attempts: 5,
          retryDelay: 500,
          retryDelayMax: 5_000,
          retryDelayMultiplier: 2,
          retryDelayJitter: 0.1,
        },
      });
      await db.use({ namespace: this.options.namespace, database: this.options.database });
    } catch (error) {
      await db.close().catch(() => undefined);
      throw new LiveFeedError(
        'connect',
        `live feed: could not connect to ${this.options.endpoint}`,
        error
      );
    }
    if (token) {
      try {
        await db.authenticate(token);
      } catch (error) {
        await db.close().catch(() => undefined);
        try {
          await this.options.onUnauthorized?.(error);
        } catch {
          /* the app's hook must not mask the refusal */
        }
        throw new LiveFeedError('unauthorized', 'live feed: the server refused the token', error);
      }
    }
    if (this.isClosed) {
      await db.close().catch(() => undefined);
      throw new LiveFeedError('closed', 'live feed is closed');
    }
    if (db.subscribe) {
      const drop = () => {
        // The server-side LIVE query died with the socket.
        this.dropped = true;
        this.live?.off();
        this.live = null;
      };
      this.offs.push(
        db.subscribe('disconnected', drop),
        db.subscribe('reconnecting', drop),
        db.subscribe('connected', () => {
          if (!this.dropped || this.isClosed) return;
          this.dropped = false;
          void this.recover();
        })
      );
    }
    this.db = db;
  }

  /** After a reconnect: LIVE again, then re-read every view (events in the gap are lost). */
  private async recover(): Promise<void> {
    try {
      await this.ensureLive();
      for (const sub of this.subs.values()) if (sub.loaded) this.scheduleReread(sub);
    } catch (error) {
      this.report(error);
    }
  }

  private async q(sql: string, vars?: Record<string, unknown>): Promise<unknown[]> {
    await this.connect();
    const db = this.db!;
    const result = await withTimeout(
      Promise.resolve(db.query(sql, vars)),
      this.timeoutMs,
      'live feed statement timed out'
    );
    return Array.isArray(result) ? result : [result];
  }

  async query<T extends unknown[] = unknown[]>(
    surql: string,
    vars?: Record<string, unknown>
  ): Promise<T> {
    try {
      return (await this.q(surql, vars)) as T;
    } catch (error) {
      if (error instanceof LiveFeedError) throw error;
      throw new LiveFeedError(
        'query',
        error instanceof Error ? error.message : String(error),
        error
      );
    }
  }

  private report(error: unknown): void {
    try {
      this.options.onError?.(error);
    } catch {
      /* never let a reporter break the feed */
    }
  }

  private touch(): void {
    this.lastActivity = Date.now();
  }

  // ── LIVE ──

  /** Start the feed's one LIVE query. `true` when this call started it. */
  private async ensureLive(): Promise<boolean> {
    if (this.live || this.isClosed) return false;
    if (!this.liveStarting) {
      this.liveStarting = (async () => {
        try {
          let id: unknown;
          try {
            [id] = await this.q(`LIVE SELECT * FROM ${this.table} FETCH out`);
          } catch (error) {
            // A server that refuses FETCH on LIVE still has the doorbell:
            // bare-id edges fall back to a body read in `onLive`.
            this.report(error);
            [id] = await this.q(`LIVE SELECT * FROM ${this.table}`);
          }
          const stream = await this.db!.liveOf(id as never);
          const off = stream.subscribe((message) => this.onLive(message));
          if (this.isClosed) {
            off();
            await (stream.kill?.() ?? Promise.resolve()).catch(() => undefined);
            return false;
          }
          this.live = { id, stream, off };
          return true;
        } catch (error) {
          // Membership still loads; only change delivery is missing.
          this.report(error);
          return false;
        } finally {
          this.liveStarting = null;
        }
      })();
    }
    return this.liveStarting;
  }

  private onLive(message: LiveMessageLike): void {
    if (message.action === 'KILLED' || this.isClosed) return;
    const edge = message.value as { out?: unknown; version?: unknown; parent?: unknown } | null;
    const hash = hashOfEdge(edge);
    const sub = hash ? this.subs.get(hash) : undefined;
    if (!sub || !edge) return;
    // Subquery children (`.related()`) are not part of the set this feed serves.
    if (edge.parent !== undefined && edge.parent !== null) return;
    this.touch();
    if (!sub.loaded) {
      sub.dirtyDuringLoad = true;
      return;
    }
    const out = edge.out;
    if (message.action === 'DELETE') {
      const rid = asRecordId(out) ?? asRecordId((out as { id?: unknown } | null)?.id);
      if (rid) this.remove(sub, keyOf(rid));
      else this.scheduleReread(sub);
      return;
    }
    const inline = rowOfEdge(edge);
    if (inline) {
      this.upsert(sub, inline.id, inline.version, inline.record);
      return;
    }
    const rid = asRecordId(out);
    if (rid) {
      this.enqueueFetch(sub, rid, typeof edge.version === 'number' ? edge.version : 0);
      return;
    }
    // `out: null`: a row this session may not read, or one that is gone.
    this.scheduleReread(sub);
  }

  // ── set bookkeeping ──

  private upsert(sub: SubState, key: string, version: number, row: Record<string, unknown>): void {
    const held = sub.rows.get(key);
    if (held && held.version >= version) return;
    sub.rows.set(key, { version, row });
    if (sub.pending.removed.has(key)) {
      sub.pending.removed.delete(key);
      sub.pending.updated.set(key, row);
    } else if (held || sub.pending.updated.has(key)) {
      if (sub.pending.added.has(key)) sub.pending.added.set(key, row);
      else sub.pending.updated.set(key, row);
    } else {
      sub.pending.added.set(key, row);
    }
    this.scheduleFlush(sub);
  }

  private remove(sub: SubState, key: string): void {
    const held = sub.rows.get(key);
    if (!held) return;
    sub.rows.delete(key);
    if (sub.pending.added.has(key)) sub.pending.added.delete(key);
    else {
      sub.pending.updated.delete(key);
      sub.pending.removed.set(key, held.row);
    }
    this.scheduleFlush(sub);
  }

  private scheduleFlush(sub: SubState): void {
    this.touch();
    if (sub.flushTimer) return;
    sub.flushTimer = setTimeout(() => {
      sub.flushTimer = null;
      this.flush(sub);
    }, FLUSH_MS);
  }

  private flush(sub: SubState): void {
    const { added, updated, removed } = sub.pending;
    if (added.size + updated.size + removed.size === 0) return;
    sub.pending = { added: new Map(), updated: new Map(), removed: new Map() };
    const change = {
      added: [...added.values()],
      updated: [...updated.values()],
      removed: [...removed.values()],
    };
    const rows = sub.list();
    this.touch();
    for (const l of [...sub.listeners]) {
      try {
        l.handlers.onChange?.(change);
        l.handlers.onSet(rows);
      } catch (error) {
        this.report(error);
      }
    }
  }

  // ── bodies ──

  private enqueueFetch(sub: SubState, rid: RecordId, version: number): void {
    const key = keyOf(rid);
    const entry = this.fetchQueue.get(key) ?? { rid, waiters: [] };
    entry.waiters.push({ sub, version });
    this.fetchQueue.set(key, entry);
    if (this.fetchTimer) return;
    this.busy++;
    this.fetchTimer = setTimeout(() => {
      this.fetchTimer = null;
      void this.drainFetches().finally(() => {
        this.busy--;
        this.touch();
      });
    }, FETCH_BATCH_MS);
  }

  private async drainFetches(): Promise<void> {
    const batch = [...this.fetchQueue.values()];
    this.fetchQueue.clear();
    if (batch.length === 0 || this.isClosed) return;
    try {
      const bodies = await this.fetchBodies(batch.map((e) => e.rid));
      for (const entry of batch) {
        const key = keyOf(entry.rid);
        const body = bodies.get(key);
        for (const w of entry.waiters) {
          if (!this.subs.has(w.sub.key)) continue;
          if (body) this.upsert(w.sub, key, w.version, body);
          else this.scheduleReread(w.sub);
        }
      }
    } catch (error) {
      this.report(error);
      for (const entry of batch) for (const w of entry.waiters) this.scheduleReread(w.sub);
    }
  }

  private async fetchBodies(rids: RecordId[]): Promise<Map<string, Record<string, unknown>>> {
    const out = new Map<string, Record<string, unknown>>();
    if (rids.length === 0) return out;
    const [rows] = await this.q(bodySelect(), { ids: rids });
    for (const row of Array.isArray(rows) ? (rows as Record<string, unknown>[]) : []) {
      const rid = asRecordId(row?.id);
      if (rid) out.set(keyOf(rid), row);
    }
    return out;
  }

  // ── membership ──

  private async readSnapshot(sub: SubState): Promise<Snapshot | null> {
    const results = await this.q(singleSnapshotSelect(this.table!), { in: sub.rid });
    return snapshotAt(results, 0);
  }

  /** Make `sub.rows` equal the snapshot: fetch what is new or newer, drop what left. */
  private async applySnapshot(sub: SubState, snap: Snapshot): Promise<void> {
    const need = [...snap.edges.entries()].filter(
      ([key, e]) => (sub.rows.get(key)?.version ?? -1) < e.version
    );
    const bodies = await this.fetchBodies(need.map(([, e]) => e.rid));
    if (!this.subs.has(sub.key)) return;
    for (const [key, e] of need) {
      const body = bodies.get(key);
      if (body) this.upsert(sub, key, e.version, body);
    }
    for (const key of [...sub.rows.keys()]) if (!snap.edges.has(key)) this.remove(sub, key);
  }

  private scheduleReread(sub: SubState): void {
    if (sub.rereadTimer || this.isClosed) return;
    this.busy++;
    sub.rereadTimer = setTimeout(() => {
      sub.rereadTimer = null;
      void this.reread(sub).finally(() => {
        this.busy--;
        this.touch();
      });
    }, REREAD_MS);
  }

  private async reread(sub: SubState): Promise<void> {
    if (!this.subs.has(sub.key) || this.isClosed) return;
    try {
      const snap = await this.readSnapshot(sub);
      if (!snap) throw new LiveFeedError('query', 'membership re-read did not answer');
      await this.applySnapshot(sub, snap);
    } catch (error) {
      this.report(error);
      for (const l of sub.listeners) l.handlers.onError?.(error);
    }
  }

  /** Register (or re-register) a view and read its membership back, in one request. */
  private async register(sub: SubState): Promise<Snapshot> {
    const results = await this.q(registerSelect(this.table!), registerVars(sub.payload));
    let snap = snapshotAt(results, 1);
    for (let i = 0; snap && snap.state === 'materializing' && i < MATERIALIZE_RETRIES; i++) {
      await sleep(MATERIALIZE_RETRY_MS);
      snap = await this.readSnapshot(sub);
    }
    if (!snap) throw new LiveFeedError('register', 'registration read-back did not answer');
    sub.registered = true;
    this.ensureHeartbeat();
    return snap;
  }

  private async load(sub: SubState): Promise<void> {
    this.busy++;
    try {
      await this.connect();
      let snap = await this.register(sub);
      // LIVE starts after the first registration (the SSP creates a per-user
      // table lazily), so whatever changed between that read-back and LIVE
      // coming up is only in a fresh read.
      if (await this.ensureLive()) snap = (await this.readSnapshot(sub)) ?? snap;
      if (!this.subs.has(sub.key)) return;
      const bodies = await this.fetchBodies([...snap.edges.values()].map((e) => e.rid));
      for (const [key, e] of snap.edges) {
        const body = bodies.get(key);
        if (body) sub.rows.set(key, { version: e.version, row: body });
      }
      sub.loaded = true;
      const rows = sub.list();
      sub.ready.resolve(rows);
      this.touch();
      for (const l of [...sub.listeners]) this.deliverInitial(l, rows);
      if (sub.dirtyDuringLoad) {
        sub.dirtyDuringLoad = false;
        this.scheduleReread(sub);
      }
    } catch (error) {
      sub.failed = true;
      const err =
        error instanceof LiveFeedError
          ? error
          : new LiveFeedError(
              'register',
              error instanceof Error ? error.message : String(error),
              error
            );
      sub.ready.reject(err);
      for (const l of [...sub.listeners]) l.handlers.onError?.(err);
      this.subs.delete(sub.key);
    } finally {
      this.busy--;
      this.touch();
    }
  }

  private deliverInitial(listener: Listener, rows: Record<string, unknown>[]): void {
    try {
      listener.handlers.onSet(rows);
    } catch (error) {
      this.report(error);
    }
  }

  // ── heartbeat ──

  private ensureHeartbeat(): void {
    if (this.heartbeatTimer || this.isClosed) return;
    const every = Math.max(5_000, Math.floor(parseDuration(this.ttl as never) / 2));
    this.heartbeatTimer = setInterval(() => void this.heartbeat(), every);
    (this.heartbeatTimer as { unref?: () => void }).unref?.();
  }

  private async heartbeat(): Promise<void> {
    const subs = [...this.subs.values()].filter((s) => s.registered);
    if (subs.length === 0 || this.isClosed) return;
    try {
      const beat = heartbeatBatch(subs.map((s) => s.rid));
      const results = await this.q(beat.sql, beat.vars);
      for (let i = 0; i < subs.length; i++) {
        if (!heartbeatRowGone(results[i])) continue;
        // The server reclaimed the view: register it again and diff.
        const sub = subs[i];
        try {
          const snap = await this.register(sub);
          await this.applySnapshot(sub, snap);
        } catch (error) {
          this.report(error);
        }
      }
    } catch (error) {
      this.report(error);
    }
  }

  // ── public ──

  subscribe<T = Record<string, unknown>>(
    query: LiveQuery,
    handlers: LiveHandlers<T>
  ): LiveFeedSubscription<T> {
    const listener: Listener = { handlers };
    let key: string | null = null;
    let state: SubState | null = null;
    let gone = false;
    this.starting++;
    const attached: Promise<SubState> = (async () => {
      try {
        if (this.isClosed) throw new LiveFeedError('closed', 'live feed is closed');
        const input = liveQueryInput(query, this.options.schema);
        key = await sha256Hex(
          queryHashInput({ surql: input.surql, params: input.params }, this.sessionId)
        );
        if (gone) throw new LiveFeedError('closed', 'unsubscribed before the view was registered');
        let sub = this.subs.get(key);
        const fresh = !sub;
        if (!sub) {
          sub = new SubState(key, input.surql, input.params, this.ttl);
          this.subs.set(key, sub);
        }
        state = sub;
        sub.listeners.add(listener);
        if (fresh) void this.load(sub);
        else if (sub.loaded) this.deliverInitial(listener, sub.list());
        return sub;
      } finally {
        this.starting--;
      }
    })();
    const ready = attached.then((sub) => sub.ready.promise) as Promise<T[]>;
    ready.catch((error) => {
      // Errors before a state existed (bad query, closed feed) still reach onError.
      if (!state) handlers.onError?.(error);
    });
    return {
      ready,
      rows: () => (state ? (state.list() as T[]) : []),
      get key() {
        return key;
      },
      unsubscribe: async () => {
        if (gone) return;
        gone = true;
        const sub = await attached.catch(() => null);
        if (!sub) return;
        sub.listeners.delete(listener);
        if (sub.listeners.size > 0 || this.subs.get(sub.key) !== sub) return;
        this.subs.delete(sub.key);
        if (sub.flushTimer) clearTimeout(sub.flushTimer);
        if (sub.rereadTimer) {
          clearTimeout(sub.rereadTimer);
          this.busy--;
        }
        if (sub.registered && !this.isClosed) {
          await this.q('fn::query::unsubscribe($id)', { id: sub.rid }).catch((error) =>
            this.report(error)
          );
        }
      },
    };
  }

  idle(options: LiveIdleOptions = {}): Promise<boolean> {
    const timeoutMs = options.timeoutMs ?? 10_000;
    const quietMs = options.quietMs ?? 300;
    const deadline = Date.now() + timeoutMs;
    return new Promise<boolean>((resolve) => {
      const check = () => {
        if (this.isClosed) return resolve(false);
        const loading =
          this.starting > 0 || [...this.subs.values()].some((s) => !s.loaded && !s.failed);
        const flushing = [...this.subs.values()].some((s) => s.flushTimer !== null);
        const quietFor = Date.now() - this.lastActivity;
        if (!loading && !flushing && this.busy === 0 && quietFor >= quietMs) return resolve(true);
        const left = deadline - Date.now();
        if (left <= 0) return resolve(false);
        setTimeout(
          check,
          Math.max(10, Math.min(50, left, quietMs - quietFor > 0 ? quietMs - quietFor : 50))
        );
      };
      check();
    });
  }

  async close(): Promise<void> {
    if (this.isClosed) return;
    const db = this.db;
    const views = [...this.subs.values()].filter((s) => s.registered).map((s) => s.rid);
    const live = this.live;
    // Release while the socket is still ours, then mark closed.
    if (db) {
      if (live) {
        live.off();
        await withTimeout(
          Promise.resolve(
            live.stream.kill ? live.stream.kill() : db.query('KILL $u', { u: live.id })
          ),
          RELEASE_TIMEOUT_MS,
          'kill timed out'
        ).catch(() => undefined);
      }
      if (views.length > 0) {
        const vars: Record<string, unknown> = {};
        const sql = views
          .map((id, i) => {
            vars[`id${i}`] = id;
            return `fn::query::unsubscribe($id${i})`;
          })
          .join(';\n');
        await withTimeout(
          Promise.resolve(db.query(sql, vars)),
          RELEASE_TIMEOUT_MS,
          'release timed out'
        ).catch(() => undefined);
      }
    }
    this.isClosed = true;
    this.live = null;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    if (this.fetchTimer) clearTimeout(this.fetchTimer);
    this.fetchTimer = null;
    for (const sub of this.subs.values()) {
      if (sub.flushTimer) clearTimeout(sub.flushTimer);
      if (sub.rereadTimer) clearTimeout(sub.rereadTimer);
      if (!sub.loaded) sub.ready.reject(new LiveFeedError('closed', 'live feed is closed'));
    }
    this.subs.clear();
    for (const off of this.offs.splice(0)) off();
    if (db) await db.close().catch(() => undefined);
    this.db = null;
  }
}
