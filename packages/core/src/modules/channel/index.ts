import type { ColumnSchema, QueryPlan, SchemaStructure, WhereComparison, WhereNode } from '@spooky-sync/query-builder';
import { RecordId } from 'surrealdb';
import type { Logger } from '../../services/logger/index';
import { encodeRecordId, parseRecordIdString } from '../../utils/index';

/**
 * Peer to peer channels (`-- @channel` tables, `client.channel(query, peers)`).
 *
 * A channel is a filter (the `where` of one or more queries on `@channel`
 * tables) plus the users it connects to. Writes to rows that match the
 * filter reach the users' tabs straight over the transport (WebRTC by
 * default), and their tabs fold them in before the database has seen them.
 * The filter is evaluated here, in plain JS, against the row; the query is
 * never registered. Optimistic writes, distributed:
 *
 * - Updates: persistent fields still go through the normal outbox. A peer
 *   shows the sent value until its own synced row catches up to it, or for
 *   `confirmTimeoutMs` at most, so a value the server rejected (or that a
 *   peer made up) falls back to the server's on its own.
 * - `-- @ephemeral` fields never reach the database. They live in memory,
 *   last writer wins, and a peer's values vanish when it disconnects.
 * - Creates go into the peer's store and circuit like its own optimistic
 *   create (minus the outbox); deletes hide the row. Both are undone unless
 *   the server confirms them (see the peer sagas in `mutation/write.saga.ts`).
 *
 * Nothing here enforces permissions beyond identity: the transport proves
 * which user a peer is (the database stamps the signaling rows), a message
 * is only taken from a user listed by an open channel whose filter the row
 * matches, and fields must exist and fit their column type. Everything else
 * is the app's call, through `channels.accept`.
 *
 * Update and ephemeral values are folded into subscription output only,
 * never into the local store.
 */

export type Row = Record<string, unknown>;

/** Hybrid logical clock: wall ms, counter, tab peer id. Orders every write. */
export type Hlc = readonly [ms: number, counter: number, peer: string];

export const compareHlc = (a: Hlc, b: Hlc): number =>
  a[0] - b[0] || a[1] - b[1] || (a[2] < b[2] ? -1 : a[2] > b[2] ? 1 : 0);

/** One tab of one user. */
export interface PeerRef {
  readonly user: string;
  readonly peer: string;
}

export const peerKey = (p: PeerRef): string => `${p.user}#${p.peer}`;

/** What a message does to its row. */
export type ChannelOpKind = 'update' | 'create' | 'delete';

/** The one message channels exchange: a write to one row. */
export interface ChannelMessage {
  readonly t: 'op';
  /** Absent means `update` (a field patch). */
  readonly kind?: ChannelOpKind;
  readonly table: string;
  readonly id: string;
  /** Wire-encoded values (see `encodeValue`): the patch, the whole row for a create, `{}` for a delete. */
  readonly patch: Record<string, unknown>;
  readonly hlc: Hlc;
}

/** How a channel is doing, as `useChannel(...).status()` shows it. */
export type ChannelStatus = 'idle' | 'connecting' | 'connected' | 'failed' | 'unsupported';

export interface ChannelState {
  readonly status: ChannelStatus;
  /** Users with at least one tab connected. */
  readonly peers: ReadonlyArray<string>;
}


/** What an `accept` rule decides on. */
export interface ChannelOp {
  readonly kind: ChannelOpKind;
  /** The sending user, as the database proved it. */
  readonly from: string;
  readonly table: string;
  readonly id: string;
  /** The row as this client currently shows it, overlay included; undefined for a create. */
  readonly row: Row | undefined;
  /** Decoded field values: the patch, the new row for a create, `{}` for a delete. */
  readonly patch: Readonly<Row>;
}

export type ChannelAcceptRule = (op: ChannelOp) => boolean;

// ---- transport -------------------------------------------------------------------

export interface TransportHooks {
  /** This tab, or null while signed out. */
  self(): PeerRef | null;
  onMessage(from: PeerRef, message: unknown): void;
  onPeer(peer: PeerRef, up: boolean): void;
  /** Per user: an attempt is running, a tab is connected, or attempts failed. */
  onStatus(user: string, status: 'connecting' | 'connected' | 'failed'): void;
}

/**
 * Moves messages between tabs. WebRTC by default; an app that already has a
 * room (LiveKit, its own relay) can plug that in. `send` is fire and forget.
 */
export interface ChannelTransport {
  readonly supported: boolean;
  /** Users this tab wants connections to: the union over open channels. */
  want(users: ReadonlySet<string>): void;
  send(to: PeerRef, message: ChannelMessage): void;
  close(): void;
}

export type ChannelTransportFactory = (hooks: TransportHooks) => ChannelTransport;

export interface ChannelsConfig {
  /**
   * Per table: decide whether to apply a peer's update. Runs after the
   * built-in checks (sender listed by an open channel carrying the table,
   * fields exist and fit).
   * Without a rule every such update is applied.
   */
  accept?: Record<string, ChannelAcceptRule>;
  /** STUN/TURN servers for the default WebRTC transport. */
  iceServers?: RTCIceServer[];
  /** Replace the WebRTC transport. */
  transport?: ChannelTransportFactory;
  /** How long a peer's persistent value shows without the server confirming it. Default 5000. */
  confirmTimeoutMs?: number;
}

// ---- pure helpers ----------------------------------------------------------------

const isRecordIdLike = (v: unknown): v is RecordId<string> =>
  v instanceof RecordId ||
  (v !== null && typeof v === 'object' && (v as { constructor?: { name?: string } }).constructor?.name === 'RecordId');

/** `table:id` for a record id in any spelling, or null. */
export function idKey(v: unknown): string | null {
  if (typeof v === 'string') return v.includes(':') ? v : null;
  if (isRecordIdLike(v)) return encodeRecordId(v);
  if (v !== null && typeof v === 'object' && 'table' in v && 'id' in v) {
    const t = (v as { table: unknown }).table;
    const name = typeof t === 'string' ? t : (t as { name?: unknown })?.name;
    if (typeof name === 'string') return `${name}:${String((v as { id: unknown }).id)}`;
  }
  return null;
}

const toBase64 = (bytes: Uint8Array): string => {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
};

const fromBase64 = (b64: string): Uint8Array => {
  const s = atob(b64);
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
};

/** JSON-safe form of a field value: record ids, dates and bytes are tagged. */
export function encodeValue(v: unknown): unknown {
  if (v === undefined) return null;
  if (isRecordIdLike(v)) return { $rid: encodeRecordId(v) };
  if (v instanceof Date) return { $date: v.toISOString() };
  if (v instanceof Uint8Array) return { $bytes: toBase64(v) };
  if (v instanceof ArrayBuffer) return { $bytes: toBase64(new Uint8Array(v)) };
  if (Array.isArray(v)) return v.map(encodeValue);
  if (v !== null && typeof v === 'object') {
    const out: Row = {};
    for (const [k, x] of Object.entries(v)) out[k] = encodeValue(x);
    return out;
  }
  return v;
}

export function decodeValue(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(decodeValue);
  if (v === null || typeof v !== 'object') return v;
  const o = v as Row;
  const keys = Object.keys(o);
  if (keys.length === 1) {
    if (typeof o.$rid === 'string') return parseRecordIdString(o.$rid);
    if (typeof o.$date === 'string') return new Date(o.$date);
    if (typeof o.$bytes === 'string') return fromBase64(o.$bytes);
  }
  const out: Row = {};
  for (const [k, x] of Object.entries(o)) out[k] = decodeValue(x);
  return out;
}

const sameValue = (a: unknown, b: unknown): boolean =>
  a === b || JSON.stringify(encodeValue(a)) === JSON.stringify(encodeValue(b));

/** A well-formed `op` message, or null. */
export function parseMessage(raw: unknown): ChannelMessage | null {
  if (raw === null || typeof raw !== 'object') return null;
  const m = raw as Record<string, unknown>;
  const hlc = m.hlc as unknown[];
  if (m.kind !== undefined && m.kind !== 'update' && m.kind !== 'create' && m.kind !== 'delete') return null;
  if (
    m.t !== 'op' ||
    typeof m.table !== 'string' ||
    typeof m.id !== 'string' ||
    m.patch === null ||
    typeof m.patch !== 'object' ||
    Array.isArray(m.patch) ||
    !Array.isArray(hlc) ||
    hlc.length !== 3 ||
    typeof hlc[0] !== 'number' ||
    typeof hlc[1] !== 'number' ||
    typeof hlc[2] !== 'string'
  ) {
    return null;
  }
  return m as unknown as ChannelMessage;
}

/** Whether a decoded value fits a column, loosely (the shapes the client itself writes). */
export function fitsColumn(col: ColumnSchema, v: unknown): boolean {
  if (v === null || v === undefined) return col.optional;
  if (col.array) return Array.isArray(v) && v.every((x) => fitsScalar(col, x));
  return fitsScalar(col, v);
}

function fitsScalar(col: ColumnSchema, v: unknown): boolean {
  if (col.recordId) return typeof v === 'string' || isRecordIdLike(v);
  if (col.dateTime) return v instanceof Date || typeof v === 'string';
  switch (col.type) {
    case 'string':
      return typeof v === 'string';
    case 'number':
      return typeof v === 'number' && Number.isFinite(v);
    case 'boolean':
      return typeof v === 'boolean';
    case 'Uint8Array':
      return v instanceof Uint8Array;
    case 'null':
      return v === null;
    default:
      return true;
  }
}

/** Why a peer patch is refused against the table's columns, or null when it fits. */
export function checkPatch(columns: Record<string, ColumnSchema>, patch: Row, allowEmpty = false): string | null {
  const fields = Object.keys(patch);
  if (fields.length === 0 && !allowEmpty) return 'empty patch';
  for (const field of fields) {
    if (field === 'id') return 'patches the id';
    const col = columns[field];
    if (!col) return `unknown field '${field}'`;
    if (col.crdt) return `'${field}' is a @crdt field`;
    if (!fitsColumn(col, patch[field])) return `'${field}' does not fit its column type`;
  }
  return null;
}

/** A patch split into the fields the database gets and the ones only peers get. */
export function splitPatch(
  columns: Record<string, ColumnSchema>,
  data: Row
): { persistent: Row; ephemeral: Row | null } {
  let ephemeral: Row | null = null;
  const persistent: Row = {};
  for (const [k, v] of Object.entries(data)) {
    if (columns[k]?.ephemeral) (ephemeral ??= {})[k] = v;
    else persistent[k] = v;
  }
  return { persistent, ephemeral };
}

/** One overlaid field: the value, who wrote it and when. */
export interface OverlayEntry {
  readonly value: unknown;
  readonly hlc: Hlc;
  /** `peerKey` of the author, or 'self'. */
  readonly from: string;
  readonly ephemeral: boolean;
  /** When a persistent peer value stops showing; null for ephemeral and own values. */
  readonly expiresAt: number | null;
}

export type Overlay = ReadonlyMap<string, ReadonlyMap<string, OverlayEntry>>;

const OVERLAY_DEPTH = 4;

/**
 * Rows with the overlay folded in, nested related rows included. Untouched
 * rows (and the array itself, when nothing changed) keep their identity.
 */
export function applyOverlay(rows: ReadonlyArray<Row>, overlay: Overlay): Row[] {
  if (overlay.size === 0) return rows as Row[];
  const out = foldArray(rows, overlay, 0);
  return out as Row[];
}

function foldArray(arr: ReadonlyArray<unknown>, overlay: Overlay, depth: number): ReadonlyArray<unknown> {
  let changed = false;
  const next = arr.map((x) => {
    const y = foldValue(x, overlay, depth);
    if (y !== x) changed = true;
    return y;
  });
  return changed ? next : arr;
}

function foldValue(v: unknown, overlay: Overlay, depth: number): unknown {
  if (depth > OVERLAY_DEPTH || v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return foldArray(v, overlay, depth + 1);
  if (isRecordIdLike(v) || v instanceof Date || v instanceof Uint8Array) return v;
  const row = v as Row;
  let next: Row | null = null;
  for (const [k, x] of Object.entries(row)) {
    if (k === 'id') continue;
    const y = foldValue(x, overlay, depth + 1);
    if (y !== x) (next ??= { ...row })[k] = y;
  }
  const key = 'id' in row ? idKey(row.id) : null;
  const fields = key ? overlay.get(key) : undefined;
  if (fields) {
    for (const [field, entry] of fields) {
      if (!sameValue(row[field], entry.value)) (next ??= { ...row })[field] = entry.value;
    }
  }
  return next ?? row;
}

/** Every record id in rows, nested related rows included. */
export function collectIds(rows: ReadonlyArray<unknown>, into = new Set<string>(), depth = 0): Set<string> {
  if (depth > OVERLAY_DEPTH) return into;
  for (const v of rows) {
    if (v === null || typeof v !== 'object') continue;
    if (Array.isArray(v)) {
      collectIds(v, into, depth + 1);
      continue;
    }
    if (isRecordIdLike(v) || v instanceof Date || v instanceof Uint8Array) continue;
    const row = v as Row;
    const key = 'id' in row ? idKey(row.id) : null;
    if (key) into.add(key);
    for (const [k, x] of Object.entries(row)) {
      if (k !== 'id' && x !== null && typeof x === 'object') collectIds([x], into, depth + 1);
    }
  }
  return into;
}

/** Normalized peer ids, without `self`. */
export function peerUsers(peers: Iterable<unknown>, self: string | null): Set<string> {
  const out = new Set<string>();
  for (const p of peers) {
    const key = idKey(p);
    if (key && key !== self) out.add(key);
  }
  return out;
}

/** What a channel carries: the rows of one `@channel` table its filter matches. */
export interface ChannelScope {
  readonly table: string;
  readonly where: ReadonlyArray<WhereNode>;
}

const OPS: Record<string, (a: unknown, b: unknown) => boolean> = {
  '=': (a, b) => sameKey(a, b),
  '==': (a, b) => sameKey(a, b),
  '!=': (a, b) => !sameKey(a, b),
  '>': (a, b) => order(a, b) > 0,
  '>=': (a, b) => order(a, b) >= 0,
  '<': (a, b) => order(a, b) < 0,
  '<=': (a, b) => order(a, b) <= 0,
  IN: (a, b) => Array.isArray(b) && b.some((x) => sameKey(a, x)),
  INSIDE: (a, b) => Array.isArray(b) && b.some((x) => sameKey(a, x)),
  '∈': (a, b) => Array.isArray(b) && b.some((x) => sameKey(a, x)),
  'NOT IN': (a, b) => !(Array.isArray(b) && b.some((x) => sameKey(a, x))),
  NOTINSIDE: (a, b) => !(Array.isArray(b) && b.some((x) => sameKey(a, x))),
  '∉': (a, b) => !(Array.isArray(b) && b.some((x) => sameKey(a, x))),
  CONTAINS: (a, b) => Array.isArray(a) && a.some((x) => sameKey(x, b)),
  '∋': (a, b) => Array.isArray(a) && a.some((x) => sameKey(x, b)),
  CONTAINSNOT: (a, b) => !(Array.isArray(a) && a.some((x) => sameKey(x, b))),
  '∌': (a, b) => !(Array.isArray(a) && a.some((x) => sameKey(x, b))),
};

/** A comparable form: record ids by `table:id` (in any spelling), dates by ms. */
function keyOf(v: unknown): unknown {
  if (v === undefined) return null;
  if (isRecordIdLike(v)) return idKey(v);
  if (v instanceof Date) return v.getTime();
  return v;
}

function sameKey(a: unknown, b: unknown): boolean {
  const x = isRecordIdLike(a) || isRecordIdLike(b) ? keyOfId(a) : keyOf(a);
  const y = isRecordIdLike(a) || isRecordIdLike(b) ? keyOfId(b) : keyOf(b);
  if (x === y) return true;
  return x !== null && y !== null && typeof x === 'object' && typeof y === 'object' && sameValue(x, y);
}

/** A record id compared against a string spelling of one. */
function keyOfId(v: unknown): unknown {
  return typeof v === 'string' ? v : keyOf(v);
}

function order(a: unknown, b: unknown): number {
  const x = keyOf(a) as number | string | null;
  const y = keyOf(b) as number | string | null;
  if (x === null || y === null || typeof x !== typeof y) return Number.NaN;
  return x < y ? -1 : x > y ? 1 : 0;
}

function pathValue(row: Row, path: string): unknown {
  let v: unknown = row;
  for (const part of path.split('.')) {
    if (v === null || typeof v !== 'object') return undefined;
    v = (v as Row)[part];
  }
  return v;
}

function matchesComparison(c: WhereComparison, row: Row): boolean {
  const field = pathValue(row, c.field);
  const op = OPS[c.op];
  return c.swap ? op(c.value, field) : op(field, c.value);
}

/** Whether `row` passes a query's `where` (a conjunction of comparisons and `_or` groups). */
export function matchesWhere(where: ReadonlyArray<WhereNode>, row: Row): boolean {
  return where.every((node) => ('or' in node ? node.or.some((c) => matchesComparison(c, row)) : matchesComparison(node, row)));
}

/**
 * The parts of a channel query that do nothing there (a channel only reads
 * the `where`). Not an error, worth a warning unless `.ignoreUnusedArgs()`.
 */
export function unusedArgs(plan: QueryPlan, isOne = false): string[] {
  const out: string[] = [];
  if (isOne) out.push('.one()');
  if (plan.orderBy && plan.orderBy.length > 0) out.push('orderBy');
  if (plan.limit !== undefined && !(isOne && plan.limit === 1)) out.push('limit');
  if (plan.offset !== undefined) out.push('offset');
  if (plan.select && plan.select.length > 0) out.push('select');
  return out;
}

/**
 * A query plan as a channel scope, or an error saying why it cannot be one:
 * the table must be `@channel`, and the filter must be decidable from the
 * row alone (its own fields, literal values, supported operators). Order,
 * limit and projection do not matter to a channel and are ignored.
 */
export function compileScope(schema: SchemaStructure, plan: QueryPlan): ChannelScope {
  const table = schema.tables.find((t) => t.name === plan.table);
  if (!table?.channel) {
    throw new Error(
      `Cannot open a channel on '${plan.table}': the table is not marked '-- @channel'. ` +
        `Add '-- @channel' above 'DEFINE TABLE ${plan.table}' and run 'spky generate'.`
    );
  }
  if (plan.relations && plan.relations.length > 0) {
    throw new Error(`A channel query on '${plan.table}' cannot use .related(): its filter must be decidable from the row alone.`);
  }
  const where = plan.where ?? [];
  for (const node of where) {
    for (const c of 'or' in node ? node.or : [node]) {
      if (!(c.op in OPS)) throw new Error(`A channel query on '${plan.table}' cannot use the operator '${c.op}'.`);
      if (c.value === undefined && c.paramRef) {
        throw new Error(`A channel query on '${plan.table}' cannot compare '${c.field}' with a parameter ($${c.paramRef}); use a value.`);
      }
      const root = c.field.split('.')[0];
      if (c.field.includes('.') && table.columns[root]?.recordId) {
        throw new Error(
          `A channel query on '${plan.table}' cannot filter on '${c.field}': it follows a record link, which the row alone does not hold.`
        );
      }
    }
  }
  return { table: plan.table, where };
}

// ---- manager ---------------------------------------------------------------------

interface Tap {
  readonly cb: (rows: Row[]) => void;
  raw: Row[] | null;
  /** Every record id in `raw`, computed on first need. */
  ids: Set<string> | null;
}

interface Channel {
  scopes: ReadonlyArray<ChannelScope>;
  /** Peers as given, kept so a sign-in can drop the new self from them. */
  listed: unknown[];
  users: Set<string>;
  readonly listeners: Set<(s: ChannelState) => void>;
  state: ChannelState;
  closed: boolean;
}

/** An open channel. `client.channel(...)` wraps it with scoped writes. */
export interface ChannelHandle {
  /** The tables its queries read. */
  tables(): ReadonlyArray<string>;
  state(): ChannelState;
  /** Called at once with the current state, then on every change. */
  subscribe(cb: (s: ChannelState) => void): () => void;
  /** Replace who it connects to (your own id is skipped). */
  setPeers(peers: Iterable<unknown>): void;
  /** Replace what it carries (the queries' filters changed). */
  setScopes(scopes: ReadonlyArray<ChannelScope>): void;
  close(): void;
}

/** How peers' creates and deletes reach the store (the runtime's peer sagas). */
export interface ChannelRows {
  /** Insert a peer's row, unconfirmed for `ttlMs`. False when refused. */
  create(table: string, id: string, row: Row, ttlMs: number): Promise<boolean>;
  /** Hide a row a peer deleted (or undo its unconfirmed create). False when there was nothing to do. */
  delete(table: string, id: string, ttlMs: number): Promise<boolean>;
  /** The row as the local store holds it, or null. What a filter is checked against. */
  read(id: string): Promise<Row | null>;
}

export interface ChannelManagerDeps {
  config?: ChannelsConfig;
  /** Without it, peers' creates and deletes are ignored. */
  rows?: ChannelRows;
  logger?: Pick<Logger, 'debug' | 'warn'>;
  /** Builds the transport the first time a channel has peers. */
  transport?: ChannelTransportFactory;
  now?: () => number;
}

const SWEEP_MS = 1000;

const randomPeerId = (): string => {
  const bytes = new Uint8Array(8);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
};

export class ChannelManager {
  private readonly tables: Map<string, { columns: Record<string, ColumnSchema>; channel: boolean }>;
  private readonly schema: SchemaStructure;
  private readonly overlay = new Map<string, Map<string, OverlayEntry>>();
  private readonly taps = new Set<Tap>();
  private readonly channels = new Set<Channel>();
  private readonly handles = new WeakMap<ChannelHandle, Channel>();
  private readonly connected = new Map<string, PeerRef>();
  private readonly userStatus = new Map<string, 'connecting' | 'connected' | 'failed'>();
  /** Unused-argument warnings already given, so a reactive query does not repeat them. */
  private readonly warnedUnused = new Set<string>();
  private readonly peerId = randomPeerId();
  private user: string | null = null;
  private clock: Hlc;
  private transport: ChannelTransport | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private closed = false;
  private readonly now: () => number;
  private readonly confirmMs: number;

  constructor(
    schema: SchemaStructure,
    private readonly deps: ChannelManagerDeps = {}
  ) {
    this.schema = schema;
    this.tables = new Map(schema.tables.map((t) => [t.name, { columns: t.columns, channel: t.channel === true }]));
    this.now = deps.now ?? Date.now;
    this.confirmMs = Math.max(0, deps.config?.confirmTimeoutMs ?? 5000);
    this.clock = [0, 0, this.peerId];
  }

  /** True when any table is a channel table; otherwise the client skips all of this. */
  get enabled(): boolean {
    for (const t of this.tables.values()) if (t.channel) return true;
    return false;
  }

  /** The signed-in user changed: peers, their values and the transport start over. */
  setUser(user: string | null): void {
    if (user === this.user) return;
    this.user = user;
    this.transport?.close();
    this.transport = null;
    this.connected.clear();
    this.userStatus.clear();
    const touched = new Set(this.overlay.keys());
    this.overlay.clear();
    for (const ch of this.channels) ch.users = peerUsers(ch.listed, user);
    this.syncWanted();
    this.emitFor(touched);
    this.notifyAll();
  }

  /**
   * Compile a query plan into a channel scope (throws when it cannot be
   * one). Parts the channel ignores draw one warning per distinct query
   * shape, unless the query says so itself (`ignoreUnused`).
   */
  scope(plan: QueryPlan, opts: { isOne?: boolean; ignoreUnused?: boolean } = {}): ChannelScope {
    const scope = compileScope(this.schema, plan);
    const unused = opts.ignoreUnused ? [] : unusedArgs(plan, opts.isOne);
    if (unused.length > 0) {
      const message =
        `channel query on '${plan.table}' has ${unused.join(', ')}, which a channel ignores (it only uses the where filter). ` +
        `Remove them, or add .ignoreUnusedArgs() to the query to say it is on purpose.`;
      if (!this.warnedUnused.has(message)) {
        this.warnedUnused.add(message);
        this.deps.logger?.warn({ table: plan.table, unused, Category: 'sp00ky-client::channel' }, message);
      }
    }
    return scope;
  }

  /** Open a channel carrying the rows `scopes` match to `peers`. */
  open(scopes: ReadonlyArray<ChannelScope>, peers: Iterable<unknown>): ChannelHandle {
    if (scopes.length === 0) throw new Error('A channel needs at least one query.');
    const listed = [...peers];
    const ch: Channel = {
      scopes,
      listed,
      users: peerUsers(listed, this.user),
      listeners: new Set(),
      state: { status: 'idle', peers: [] },
      closed: false,
    };
    this.channels.add(ch);
    this.syncWanted();
    ch.state = this.stateOf(ch);
    const handle: ChannelHandle = {
      tables: () => [...new Set(ch.scopes.map((sc) => sc.table))],
      state: () => ch.state,
      subscribe: (cb) => {
        cb(ch.state);
        ch.listeners.add(cb);
        return () => ch.listeners.delete(cb);
      },
      setPeers: (next) => {
        if (ch.closed) return;
        ch.listed = [...next];
        ch.users = peerUsers(ch.listed, this.user);
        this.syncWanted();
        this.notify(ch);
      },
      setScopes: (next) => {
        if (ch.closed) return;
        if (next.length === 0) throw new Error('A channel needs at least one query.');
        ch.scopes = next;
      },
      close: () => {
        if (ch.closed) return;
        ch.closed = true;
        this.channels.delete(ch);
        ch.listeners.clear();
        this.syncWanted();
      },
    };
    this.handles.set(handle, ch);
    return handle;
  }

  /** Throws unless `handle` is an open channel of this manager with a query on `table`. */
  assertCarries(handle: ChannelHandle, table: string): void {
    const ch = this.handles.get(handle);
    if (!ch || ch.closed) throw new Error('This channel is closed.');
    if (!ch.scopes.some((sc) => sc.table === table)) {
      throw new Error(`This channel has no query on '${table}' (it reads ${[...new Set(ch.scopes.map((sc) => sc.table))].join(', ')}).`);
    }
  }

  /**
   * Follow a subscription: `feed` it the raw rows, and `cb` gets them with
   * the overlay folded in, again whenever the overlay changes for its rows.
   */
  tap(cb: (rows: Row[]) => void): { feed: (rows: Row[]) => void; stop: () => void } {
    const tap: Tap = { cb, raw: null, ids: null };
    this.taps.add(tap);
    return {
      feed: (rows) => {
        tap.raw = rows;
        tap.ids = null;
        this.emit(tap);
      },
      stop: () => {
        this.taps.delete(tap);
      },
    };
  }

  /** Split an own write on `table` into database fields and peer-only fields. */
  split(table: string, data: Row): { persistent: Row; ephemeral: Row | null } {
    const t = this.tables.get(table);
    if (!t?.channel) return { persistent: data, ephemeral: null };
    return splitPatch(t.columns, data);
  }

  /**
   * An own update: ephemeral fields go into the overlay (they have no other
   * home), persistent fields drop any peer value they supersede, and the
   * whole patch goes to the peers (of `scope`, or of the channel carrying the
   * table, see `targets`).
   */
  local(table: string, id: string, persistent: Row, ephemeral: Row | null, scope?: ChannelHandle): void {
    const t = this.tables.get(table);
    if (!t?.channel || this.closed) return;
    const rid = normalizeId(table, id);
    const hlc = this.tick();
    const fields = this.overlay.get(rid);
    let touched = false;
    if (fields) {
      for (const f of Object.keys(persistent)) touched = fields.delete(f) || touched;
    }
    if (ephemeral) {
      const map = fields ?? new Map<string, OverlayEntry>();
      for (const [f, value] of Object.entries(ephemeral)) {
        map.set(f, { value, hlc, from: 'self', ephemeral: true, expiresAt: null });
      }
      this.overlay.set(rid, map);
      touched = true;
    }
    if (fields && fields.size === 0) this.overlay.delete(rid);
    if (touched) this.emitFor(new Set([rid]));
    const patch = { ...persistent, ...(ephemeral ?? {}) };
    if (Object.keys(patch).length === 0) return;
    // Sent if the row matches before or after the patch, so a row moving
    // out of a channel's filter still reaches its peers.
    void this.rowFor(rid).then((row) => {
      const after = { ...(row ?? { id: rid }), ...patch };
      this.send(this.targets(table, row ? [row, after] : [after], scope), table, rid, patch, hlc, 'update');
    });
  }

  /**
   * An own create: its ephemeral fields go into the overlay (the store never
   * holds them), and the whole row goes to the peers.
   */
  localCreate(table: string, id: string, row: Row, scope?: ChannelHandle): void {
    const t = this.tables.get(table);
    if (!t?.channel || this.closed) return;
    const rid = normalizeId(table, id);
    const full = pickColumns(t.columns, row);
    const hlc = this.tick();
    const ephemeral = Object.keys(full).filter((f) => t.columns[f]?.ephemeral);
    if (ephemeral.length > 0) {
      const map = this.overlay.get(rid) ?? new Map<string, OverlayEntry>();
      for (const f of ephemeral) map.set(f, { value: full[f], hlc, from: 'self', ephemeral: true, expiresAt: null });
      this.overlay.set(rid, map);
      this.emitFor(new Set([rid]));
    }
    this.send(this.targets(table, [{ ...full, id: rid }], scope), table, rid, full, hlc, 'create');
  }

  /**
   * An own delete: the peers hide the row at once. Resolves once it is sent;
   * await it before the local delete, which the filter check reads past.
   */
  async localDelete(table: string, id: string, scope?: ChannelHandle): Promise<void> {
    const t = this.tables.get(table);
    if (!t?.channel || this.closed) return;
    const rid = normalizeId(table, id);
    const hlc = this.tick();
    const row = await this.rowFor(rid);
    if (row) this.send(this.targets(table, [row], scope), table, rid, {}, hlc, 'delete');
    if (this.overlay.delete(rid)) this.emitFor(new Set([rid]));
  }

  /** A transport delivered a message from `from`. */
  receive(from: PeerRef, raw: unknown): void {
    if (this.closed) return;
    const msg = parseMessage(raw);
    if (!msg) return this.drop(from, 'malformed message');
    this.observe(msg.hlc);
    const t = this.tables.get(msg.table);
    if (!t?.channel) return this.drop(from, `'${msg.table}' is not a @channel table`);
    const rid = normalizeId(msg.table, msg.id);
    if (rid.slice(0, rid.indexOf(':')) !== msg.table) return this.drop(from, 'id is not a row of the table');
    if (!this.listedOn(msg.table, from.user)) return this.drop(from, `no channel on '${msg.table}' lists ${from.user}`);
    const kind = msg.kind ?? 'update';
    if (kind === 'create') return this.receiveCreate(from, msg, rid, t.columns);
    if (kind === 'delete') return void this.receiveDelete(from, msg.table, rid);
    const patch: Row = {};
    for (const [k, v] of Object.entries(msg.patch)) patch[k] = decodeValue(v);
    const problem = checkPatch(t.columns, patch);
    if (problem) return this.drop(from, problem);
    void this.receiveUpdate(from, msg, rid, patch, t.columns);
  }

  /** A peer updated a row we hold, inside a channel's filter (before or after the patch). */
  private async receiveUpdate(from: PeerRef, msg: ChannelMessage, rid: string, patch: Row, columns: Record<string, ColumnSchema>): Promise<void> {
    const row = await this.rowFor(rid);
    if (this.closed) return;
    if (!row) return this.drop(from, `${rid} is not held here, so no filter can be checked`);
    if (!this.matchesFor(msg.table, [row, { ...row, ...patch }], from.user)) return this.drop(from, `${rid} is outside every channel listing ${from.user}`);
    if (!this.accepts({ kind: 'update', from: from.user, table: msg.table, id: rid, row, patch })) {
      return this.drop(from, 'refused by the accept rule');
    }
    const map = this.overlay.get(rid) ?? new Map<string, OverlayEntry>();
    const key = peerKey(from);
    let touched = false;
    for (const [field, value] of Object.entries(patch)) {
      const prev = map.get(field);
      if (prev && compareHlc(prev.hlc, msg.hlc) >= 0) continue;
      const ephemeral = columns[field]?.ephemeral === true;
      map.set(field, { value, hlc: msg.hlc, from: key, ephemeral, expiresAt: ephemeral ? null : this.now() + this.confirmMs });
      touched = true;
    }
    if (map.size > 0) this.overlay.set(rid, map);
    if (!touched) return;
    this.ensureSweep();
    this.emitFor(new Set([rid]));
  }

  /** A peer created a row: into the store like an own optimistic create, unconfirmed. */
  private receiveCreate(from: PeerRef, msg: ChannelMessage, rid: string, columns: Record<string, ColumnSchema>): void {
    const rows = this.deps.rows;
    if (!rows) return this.drop(from, 'creates are not wired');
    const row: Row = {};
    for (const [k, v] of Object.entries(msg.patch)) if (k !== 'id') row[k] = decodeValue(v);
    const problem = checkPatch(columns, row, true);
    if (problem) return this.drop(from, problem);
    if (!this.matchesFor(msg.table, [{ ...row, id: parseRecordIdString(rid) }], from.user)) {
      return this.drop(from, `the new ${rid} is outside every channel listing ${from.user}`);
    }
    if (!this.accepts({ kind: 'create', from: from.user, table: msg.table, id: rid, row: undefined, patch: row })) {
      return this.drop(from, 'refused by the accept rule');
    }
    const persistent: Row = {};
    const ephemeral: Row = {};
    for (const [k, v] of Object.entries(row)) (columns[k]?.ephemeral ? ephemeral : persistent)[k] = v;
    void rows.create(msg.table, rid, persistent, this.confirmMs).then(
      (applied) => {
        if (!applied || this.closed || Object.keys(ephemeral).length === 0) return;
        const map = this.overlay.get(rid) ?? new Map<string, OverlayEntry>();
        for (const [f, value] of Object.entries(ephemeral)) {
          map.set(f, { value, hlc: msg.hlc, from: peerKey(from), ephemeral: true, expiresAt: null });
        }
        this.overlay.set(rid, map);
        this.emitFor(new Set([rid]));
      },
      (error) => this.deps.logger?.debug({ error, Category: 'sp00ky-client::channel' }, 'peer create failed')
    );
  }

  /** A peer deleted a row inside a channel's filter: hide it until the server confirms (or undo its own create). */
  private async receiveDelete(from: PeerRef, table: string, rid: string): Promise<void> {
    const rows = this.deps.rows;
    if (!rows) return this.drop(from, 'deletes are not wired');
    const row = await this.rowFor(rid);
    if (this.closed) return;
    if (!row) return this.drop(from, `${rid} is not held here`);
    if (!this.matchesFor(table, [row], from.user)) return this.drop(from, `${rid} is outside every channel listing ${from.user}`);
    if (!this.accepts({ kind: 'delete', from: from.user, table, id: rid, row, patch: {} })) {
      return this.drop(from, 'refused by the accept rule');
    }
    if (this.overlay.delete(rid)) this.emitFor(new Set([rid]));
    void rows.delete(table, rid, this.confirmMs).catch((error) =>
      this.deps.logger?.debug({ error, Category: 'sp00ky-client::channel' }, 'peer delete failed')
    );
  }

  private accepts(op: ChannelOp): boolean {
    const rule = this.deps.config?.accept?.[op.table];
    if (!rule) return true;
    try {
      return rule(op) === true;
    } catch (error) {
      this.deps.logger?.warn({ error, Category: 'sp00ky-client::channel' }, 'channel accept rule threw');
      return false;
    }
  }

  /** A peer tab came up (send it our ephemeral state) or went away (drop its). */
  peerChanged(peer: PeerRef, up: boolean): void {
    const key = peerKey(peer);
    if (up) {
      this.connected.set(key, peer);
      this.userStatus.set(peer.user, 'connected');
      this.sendSnapshot(peer);
    } else {
      this.connected.delete(key);
      const touched = new Set<string>();
      for (const [rid, fields] of this.overlay) {
        for (const [f, e] of fields) {
          if (e.from === key && e.ephemeral) {
            fields.delete(f);
            touched.add(rid);
          }
        }
        if (fields.size === 0) this.overlay.delete(rid);
      }
      this.emitFor(touched);
    }
    this.notifyAll();
  }

  statusChanged(user: string, status: 'connecting' | 'connected' | 'failed'): void {
    if (status !== 'connected' && [...this.connected.values()].some((p) => p.user === user)) return;
    this.userStatus.set(user, status);
    this.notifyAll();
  }

  close(): void {
    this.closed = true;
    this.transport?.close();
    this.transport = null;
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    this.sweepTimer = null;
    this.taps.clear();
    for (const ch of this.channels) ch.closed = true;
    this.channels.clear();
    this.overlay.clear();
  }

  // ---- internals -----------------------------------------------------------------

  private syncWanted(): void {
    if (this.closed) return;
    const wanted = new Set<string>();
    for (const ch of this.channels) for (const u of ch.users) wanted.add(u);
    if (!this.transport) {
      if (wanted.size === 0 || !this.user || !this.deps.transport) return;
      this.transport = this.deps.transport({
        self: () => (this.user ? { user: this.user, peer: this.peerId } : null),
        onMessage: (from, message) => this.receive(from, message),
        onPeer: (peer, up) => this.peerChanged(peer, up),
        onStatus: (user, status) => this.statusChanged(user, status),
      });
    }
    this.transport.want(wanted);
  }

  /** Some open channel lists `user` and has a query on `table` (the cheap check before any read). */
  private listedOn(table: string, user: string): boolean {
    for (const ch of this.channels) if (ch.users.has(user) && ch.scopes.some((sc) => sc.table === table)) return true;
    return false;
  }

  /** Some open channel listing `user` has a query on `table` one of `rows` matches. */
  private matchesFor(table: string, rows: ReadonlyArray<Row>, user: string): boolean {
    for (const ch of this.channels) if (ch.users.has(user) && carries(ch, table, rows)) return true;
    return false;
  }

  /**
   * Who an own write goes to: the peers of every open channel (or only
   * `scope`) whose filter one of `rows` matches. That is what keeps a
   * message of one conversation away from the participants of another.
   */
  private targets(table: string, rows: ReadonlyArray<Row>, scope?: ChannelHandle): ReadonlySet<string> {
    const users = new Set<string>();
    const pick = scope ? this.handles.get(scope) : undefined;
    for (const ch of pick ? [pick] : this.channels) {
      if (ch.closed || !carries(ch, table, rows)) continue;
      for (const u of ch.users) users.add(u);
    }
    return users;
  }

  /** The row as shown (overlay included): from a subscription, else the store. */
  private async rowFor(rid: string): Promise<Row | null> {
    const shown = this.rowOf(rid);
    if (shown) return shown;
    const stored = await this.deps.rows?.read(rid).catch(() => null);
    return stored ? applyOverlay([stored], this.overlay)[0] : null;
  }

  private send(users: ReadonlySet<string>, table: string, rid: string, patch: Row, hlc: Hlc, kind: ChannelOpKind): void {
    const transport = this.transport;
    if (!transport || users.size === 0) return;
    const encoded: Row = {};
    for (const [k, v] of Object.entries(patch)) encoded[k] = encodeValue(v);
    const message: ChannelMessage =
      kind === 'update' ? { t: 'op', table, id: rid, patch: encoded, hlc } : { t: 'op', kind, table, id: rid, patch: encoded, hlc };
    for (const peer of this.connected.values()) if (users.has(peer.user)) transport.send(peer, message);
  }

  /** Own ephemeral values of rows inside a channel a newly connected peer is listed on. */
  private sendSnapshot(peer: PeerRef): void {
    for (const [rid, fields] of this.overlay) {
      const own = [...fields].filter(([, e]) => e.from === 'self');
      if (own.length === 0) continue;
      const table = rid.slice(0, rid.indexOf(':'));
      if (!this.listedOn(table, peer.user)) continue;
      const patch: Row = {};
      let hlc = own[0][1].hlc;
      for (const [f, e] of own) {
        patch[f] = encodeValue(e.value);
        if (compareHlc(e.hlc, hlc) > 0) hlc = e.hlc;
      }
      void this.rowFor(rid).then((row) => {
        if (!row || !this.matchesFor(table, [row], peer.user)) return;
        this.transport?.send(peer, { t: 'op', table, id: rid, patch, hlc });
      });
    }
  }

  private rowOf(rid: string): Row | undefined {
    for (const tap of this.taps) {
      if (!tap.raw || !idsOf(tap).has(rid)) continue;
      const row = findRow(tap.raw, rid, 0);
      if (row) return applyOverlay([row], this.overlay)[0];
    }
    return undefined;
  }

  private emit(tap: Tap): void {
    if (!tap.raw) return;
    const rows = applyOverlay(tap.raw, this.overlay);
    this.confirm(tap.raw);
    tap.cb(rows);
  }

  private emitFor(rids: ReadonlySet<string>): void {
    if (rids.size === 0) return;
    for (const tap of this.taps) {
      if (!tap.raw) continue;
      const ids = idsOf(tap);
      for (const rid of rids) {
        if (ids.has(rid)) {
          this.emit(tap);
          break;
        }
      }
    }
  }

  /** Persistent peer values the synced rows already hold are done showing. */
  private confirm(raw: ReadonlyArray<Row>): void {
    if (this.overlay.size === 0) return;
    for (const rid of collectIds(raw)) {
      const fields = this.overlay.get(rid);
      if (!fields) continue;
      const row = findRow(raw, rid, 0);
      if (!row) continue;
      for (const [f, e] of fields) {
        if (!e.ephemeral && e.from !== 'self' && f in row && sameValue(row[f], e.value)) fields.delete(f);
      }
      if (fields.size === 0) this.overlay.delete(rid);
    }
  }

  private ensureSweep(): void {
    if (this.sweepTimer || this.closed) return;
    this.sweepTimer = setInterval(() => this.sweep(), SWEEP_MS);
  }

  private sweep(): void {
    const now = this.now();
    const touched = new Set<string>();
    let pending = false;
    for (const [rid, fields] of this.overlay) {
      for (const [f, e] of fields) {
        if (e.expiresAt === null) continue;
        if (e.expiresAt <= now) {
          fields.delete(f);
          touched.add(rid);
        } else {
          pending = true;
        }
      }
      if (fields.size === 0) this.overlay.delete(rid);
    }
    if (!pending && this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    this.emitFor(touched);
  }

  private tick(): Hlc {
    const now = this.now();
    const [ms, c] = this.clock;
    this.clock = now > ms ? [now, 0, this.peerId] : [ms, c + 1, this.peerId];
    return this.clock;
  }

  private observe(h: Hlc): void {
    if (h[0] > this.clock[0] || (h[0] === this.clock[0] && h[1] > this.clock[1])) this.clock = [h[0], h[1], this.peerId];
  }

  private stateOf(ch: Channel): ChannelState {
    const users = [...ch.users];
    if (users.length === 0) return { status: 'idle', peers: [] };
    if (this.transport && !this.transport.supported) return { status: 'unsupported', peers: [] };
    const up = users.filter((u) => [...this.connected.values()].some((p) => p.user === u));
    if (up.length > 0) return { status: 'connected', peers: up };
    if (users.every((u) => this.userStatus.get(u) === 'failed')) return { status: 'failed', peers: [] };
    return { status: 'connecting', peers: [] };
  }

  private notify(ch: Channel): void {
    const next = this.stateOf(ch);
    if (next.status === ch.state.status && next.peers.join(',') === ch.state.peers.join(',')) return;
    ch.state = next;
    for (const cb of ch.listeners) cb(next);
  }

  private notifyAll(): void {
    for (const ch of this.channels) this.notify(ch);
  }

  private drop(from: PeerRef, reason: string): void {
    this.deps.logger?.debug({ from: from.user, reason, Category: 'sp00ky-client::channel' }, 'channel message dropped');
  }
}

/** Whether a channel has a query on `table` that one of `rows` matches. */
function carries(ch: Channel, table: string, rows: ReadonlyArray<Row>): boolean {
  return ch.scopes.some((sc) => sc.table === table && rows.some((r) => matchesWhere(sc.where, r)));
}

/** The fields of `row` the table has as plain columns (no id, no `@crdt`). */
function pickColumns(columns: Record<string, ColumnSchema>, row: Row): Row {
  const out: Row = {};
  for (const [k, v] of Object.entries(row)) {
    if (k === 'id' || !columns[k] || columns[k].crdt || v === undefined) continue;
    out[k] = v;
  }
  return out;
}

function idsOf(tap: Tap): Set<string> {
  return (tap.ids ??= tap.raw ? collectIds(tap.raw) : new Set());
}

/** `table:id` for an id given with or without its table. */
function normalizeId(table: string, id: string): string {
  return id.includes(':') ? id : `${table}:${id}`;
}

function findRow(rows: ReadonlyArray<unknown>, rid: string, depth: number): Row | null {
  if (depth > OVERLAY_DEPTH) return null;
  for (const v of rows) {
    if (v === null || typeof v !== 'object') continue;
    if (Array.isArray(v)) {
      const hit = findRow(v, rid, depth + 1);
      if (hit) return hit;
      continue;
    }
    if (isRecordIdLike(v) || v instanceof Date || v instanceof Uint8Array) continue;
    const row = v as Row;
    if ('id' in row && idKey(row.id) === rid) return row;
    for (const [k, x] of Object.entries(row)) {
      if (k === 'id' || x === null || typeof x !== 'object') continue;
      const hit = findRow([x], rid, depth + 1);
      if (hit) return hit;
    }
  }
  return null;
}
