import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { LoroDoc } from 'loro-crdt';
import { RecordId } from 'surrealdb';
import type { LocalStore, RemoteDatabaseService } from '../../services/database/index';
import type { Logger } from '../../services/logger/index';
import { parseRecordIdString } from '../../utils/index';
import { check } from './acceptance';
import { asBytes, randomPeer } from './bytes';
import { CrdtField } from './crdt-field';
import { loadLoro } from './loro-loader';
import type { CrdtKind } from './mirror';

export { CrdtField, cursorColorFromName, CURSOR_COLORS } from './crdt-field';

/**
 * Collaborative (`@crdt`) fields.
 *
 * Each open field is a Loro document. The client never sends the document,
 * only its own edits: every batch of local edits becomes one row of the
 * generated `<table>__crdt_log` table, written through the ordinary outbox
 * (so it survives offline, reloads and retries), and every client receives
 * the other rows through an ordinary query and merges them. Nobody writes the
 * canonical state but the SSP, which folds the log into `_00_crdt_doc` and the
 * parent field's plain text (see `packages/ssp-node/src/crdt`).
 *
 * A row is imported only if it passes the acceptance rule the fold applies
 * (`./acceptance`), so this tab and the server always hold the same ops.
 *
 * Edits carry this tab's Loro peer id, claimed for the signed-in user in
 * `_00_crdt_peer` before the first row is sent; the database refuses a row
 * whose peer its author does not own. A tab picks a fresh peer per session.
 * Offline, edits stay local (and in the local snapshot) until the claim and
 * the outbox can go through.
 */

/** The runtime pieces the manager needs from the client facade. */
export interface CrdtPorts {
  /** Register a query on `table` filtered by `where` and follow its rows. */
  watch(table: string, where: Record<string, unknown>, onRows: (rows: Record<string, unknown>[]) => void): Promise<() => void>;
  /** Optimistic create through the outbox. */
  create(id: string, data: Record<string, unknown>): Promise<unknown>;
}

/** Local-only state of one field: the merged document and own unsent edits. */
interface LocalState {
  snapshot?: Uint8Array;
  /** Server fold this snapshot includes (for the canonical refetch). */
  rev: number;
  /** Every peer this device edited the field under, and how far its ops
   *  are already in the outbox. */
  peers: Record<string, { enqueued: number }>;
}

interface Session {
  key: string;
  table: string;
  recordId: string;
  field: string;
  kind: CrdtKind;
  cursor: boolean;
  doc: LoroDoc;
  handle: CrdtField;
  refs: number;
  local: LocalState;
  /** Log rows already looked at (imported, refused or own). */
  seen: Set<string>;
  disposers: Array<() => void>;
  flushTimer: ReturnType<typeof setTimeout> | null;
  firstDirtyAt: number;
  persistTimer: ReturnType<typeof setTimeout> | null;
  flushing: Promise<void> | null;
  refetching: boolean;
  presenceTimer: ReturnType<typeof setTimeout> | null;
  presencePending: Uint8Array | null;
}

const LOCAL_TABLE = '_00_crdt_local';
const LOG_SUFFIX = '__crdt_log';
const PRESENCE_SUFFIX = '__crdt_presence';
/** Longest a burst of typing waits before its edits go out. */
const MAX_WAIT_MS = 2000;
const PERSIST_DEBOUNCE_MS = 400;
const PRESENCE_THROTTLE_MS = 250;
const CLAIM_RETRY_MS = 5000;

export class CrdtManager {
  private sessions = new Map<string, Session>();
  private opening = new Map<string, Promise<Session>>();
  private ports: CrdtPorts | null = null;
  private logger: Logger;
  private peer: `${number}` = randomPeer();
  /** The current peer's claim: settled once the database holds it for us. */
  private claim: Promise<boolean> | null = null;
  private claimRetry: ReturnType<typeof setTimeout> | null = null;
  private connectionUnsubscribes: Array<() => void> = [];

  constructor(
    private schema: SchemaStructure,
    private local: LocalStore,
    private remote: RemoteDatabaseService,
    logger: Logger,
    private debounceMs: number = 300
  ) {
    this.logger = logger.child({ service: 'CrdtManager' });
    this.connectionUnsubscribes.push(
      this.remote.subscribeConnection('connected', () => {
        for (const s of this.sessions.values()) this.scheduleFlush(s, 0);
      })
    );
  }

  /** Hand over the runtime ports. Called once by the client facade. */
  attach(ports: CrdtPorts): void {
    this.ports = ports;
  }

  /**
   * A new signed-in identity (boot, sign-in, sign-out): edits from here on
   * belong to it, under a peer it claims. Edits already made stay with the
   * peer they were made under.
   */
  setSessionId(_salt: string): void {
    this.peer = randomPeer();
    this.claim = null;
    for (const s of this.sessions.values()) s.doc.setPeerId(this.peer);
  }

  dispose(): void {
    for (const off of this.connectionUnsubscribes.splice(0)) {
      try {
        off();
      } catch {
        /* ignore */
      }
    }
    if (this.claimRetry) clearTimeout(this.claimRetry);
  }

  /**
   * Open a collaborative field. Every open of the same field shares one
   * document; close each open with {@link close}.
   */
  async open(table: string, recordId: string, field: string, _fallbackText?: string): Promise<CrdtField> {
    const { kind, cursor } = this.fieldSpec(table, field);
    const key = `${table}|${recordId}|${field}`;
    const existing = this.sessions.get(key);
    if (existing) {
      existing.refs++;
      return existing.handle;
    }
    const inFlight = this.opening.get(key);
    if (inFlight) {
      const s = await inFlight;
      s.refs++;
      return s.handle;
    }
    const pending = this.start(key, table, recordId, field, kind, cursor).finally(() => this.opening.delete(key));
    this.opening.set(key, pending);
    return (await pending).handle;
  }

  close(table: string, recordId: string, field: string): void {
    const s = this.sessions.get(`${table}|${recordId}|${field}`);
    if (!s || --s.refs > 0) return;
    void this.stop(s, true);
  }

  /** Close every field. `flush: false` (a bucket switch) drops unsent edits
   *  from this session's memory; they stay in the local snapshot of the
   *  bucket they belong to. */
  closeAll(options: { flush?: boolean } = {}): void {
    for (const s of [...this.sessions.values()]) void this.stop(s, options.flush ?? true);
  }

  // ------------------------------------------------------------------

  private fieldSpec(table: string, field: string): { kind: CrdtKind; cursor: boolean } {
    const t = this.schema.tables.find((x) => x.name === table);
    if (!t) throw new Error(`CrdtManager: unknown table '${table}'`);
    const col = (t.columns as Record<string, any>)[field];
    if (!col) throw new Error(`CrdtManager: unknown field '${table}.${field}'`);
    if (!col.crdt) {
      throw new Error(
        `CrdtManager: '${table}.${field}' is not a CRDT field. Add \`-- @crdt text\` or \`-- @crdt prosemirror\` above its DEFINE FIELD and re-run \`spky generate\`.`
      );
    }
    const kind: CrdtKind = col.crdt === 'prosemirror' ? 'prosemirror' : 'text';
    return { kind, cursor: !!col.cursor };
  }

  private async start(
    key: string,
    table: string,
    recordId: string,
    field: string,
    kind: CrdtKind,
    cursor: boolean
  ): Promise<Session> {
    if (!this.ports) throw new Error('CrdtManager: not attached to a client');
    const loro = await loadLoro();
    const doc = new loro.LoroDoc();
    doc.setPeerId(this.peer);

    const local = await this.readLocal(recordId, field);
    if (local.snapshot) this.tryImport(doc, local.snapshot, key);

    const session: Session = {
      key,
      table,
      recordId,
      field,
      kind,
      cursor,
      doc,
      handle: undefined as unknown as CrdtField,
      refs: 1,
      local,
      seen: new Set(),
      disposers: [],
      flushTimer: null,
      firstDirtyAt: 0,
      persistTimer: null,
      flushing: null,
      refetching: false,
      presenceTimer: null,
      presencePending: null,
    };
    session.handle = new CrdtField(table, recordId, field, kind, cursor, doc, {
      pushPresence: (state) => this.pushPresence(session, state),
    });
    this.sessions.set(key, session);

    // First open on this device: start from the published document.
    if (!local.snapshot) await this.refetchCanonical(session);

    session.disposers.push(
      doc.subscribeLocalUpdates(() => {
        // Remember every peer that made edits here: a later identity change
        // swaps the peer, and these ops must still go out under this one.
        session.local.peers[this.peer] ??= { enqueued: 0 };
        this.scheduleFlush(session);
        this.schedulePersist(session);
      })
    );
    const ports = this.ports;
    session.disposers.push(
      await ports.watch(`${table}${LOG_SUFFIX}`, { doc: recordId, field }, (rows) => this.ingest(session, rows))
    );
    if (cursor) {
      session.disposers.push(
        await ports.watch(`${table}${PRESENCE_SUFFIX}`, { doc: recordId, field }, (rows) => this.ingestPresence(session, rows))
      );
    }
    // Edits a previous session made but never sent (closed tab, offline).
    this.scheduleFlush(session, 0);
    this.logger.debug({ key, peer: this.peer, Category: 'sp00ky-client::CrdtManager::open' }, 'CRDT field opened');
    return session;
  }

  private async stop(s: Session, flush: boolean): Promise<void> {
    this.sessions.delete(s.key);
    for (const off of s.disposers.splice(0)) {
      try {
        off();
      } catch {
        /* ignore */
      }
    }
    if (s.flushTimer) clearTimeout(s.flushTimer);
    if (s.persistTimer) clearTimeout(s.persistTimer);
    if (s.presenceTimer) clearTimeout(s.presenceTimer);
    if (flush) {
      await this.persist(s);
      await this.flush(s);
    }
  }

  // --- receiving ---------------------------------------------------

  private async ingest(s: Session, rows: Record<string, unknown>[]): Promise<void> {
    const loro = await loadLoro();
    const blobs: Uint8Array[] = [];
    for (const row of rows) {
      const id = String(row.id ?? '');
      if (!id || s.seen.has(id)) continue;
      s.seen.add(id);
      const blob = asBytes(row.data);
      const verdict = check(loro, blob, row.peer == null ? undefined : String(row.peer));
      if (!verdict.ok) {
        this.logger.debug({ row: id, reason: verdict.reason, Category: 'sp00ky-client::CrdtManager::ingest' }, 'CRDT log row refused');
        continue;
      }
      blobs.push(blob!);
    }
    if (blobs.length === 0) return;
    let pending = false;
    for (const blob of blobs) {
      const status = this.tryImport(s.doc, blob, s.key);
      if (status?.pending) pending = true;
    }
    this.schedulePersist(s);
    // Ops depending on rows that were folded away before this device saw
    // them: the published snapshot holds those predecessors.
    if (pending) void this.refetchCanonical(s);
  }

  private tryImport(doc: LoroDoc, blob: Uint8Array, key: string): { pending: boolean } | null {
    try {
      const status = doc.import(blob);
      return { pending: !!status.pending && status.pending.size > 0 };
    } catch (e) {
      this.logger.warn({ key, error: String(e), Category: 'sp00ky-client::CrdtManager::import' }, 'CRDT import failed');
      return null;
    }
  }

  private async refetchCanonical(s: Session): Promise<void> {
    if (s.refetching) return;
    s.refetching = true;
    try {
      const [row] = await this.remote.query<[{ rev?: number; snapshot?: unknown } | null]>(
        "SELECT rev, snapshot FROM ONLY type::record('_00_crdt_doc', [type::record($doc), $field])",
        { doc: s.recordId, field: s.field }
      );
      const snapshot = asBytes(row?.snapshot);
      if (snapshot) {
        this.tryImport(s.doc, snapshot, s.key);
        s.local.rev = Math.max(s.local.rev, row?.rev ?? 0);
        this.schedulePersist(s);
      }
    } catch (e) {
      // Offline or no published document yet: the log rows still arrive.
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::refetch' }, 'No canonical CRDT document fetched');
    } finally {
      s.refetching = false;
    }
  }

  // --- sending -----------------------------------------------------

  private scheduleFlush(s: Session, delay = this.debounceMs): void {
    const now = Date.now();
    if (!s.flushTimer) s.firstDirtyAt = now;
    else clearTimeout(s.flushTimer);
    const wait = Math.max(0, Math.min(delay, s.firstDirtyAt + MAX_WAIT_MS - now));
    s.flushTimer = setTimeout(() => {
      s.flushTimer = null;
      void this.flush(s);
    }, wait);
  }

  private async flush(s: Session): Promise<void> {
    if (s.flushing) return s.flushing;
    s.flushing = this.sendOwnEdits(s).finally(() => {
      s.flushing = null;
    });
    return s.flushing;
  }

  /** Send every own peer's ops that are not in the outbox yet, one row per
   *  peer. Only the peer's own ops travel (`updates-in-range`): a plain
   *  `from: version` export would carry ops imported from others too, and
   *  the acceptance rule refuses such a row. */
  private async sendOwnEdits(s: Session): Promise<void> {
    const vv = s.doc.oplogVersion();
    const own = new Set([...Object.keys(s.local.peers), this.peer]);
    const due = [...own]
      .map((peer) => ({ peer, end: vv.get(peer as `${number}`) ?? 0, from: s.local.peers[peer]?.enqueued ?? 0 }))
      .filter((p) => p.end > p.from);
    if (due.length === 0 || !this.ports) return;
    if (!(await this.ensureClaimed())) {
      this.retryLater(s);
      return;
    }
    for (const { peer, end, from } of due) {
      if (peer !== this.peer && !(await this.claimPeer(peer as `${number}`))) continue;
      const data = s.doc.export({
        mode: 'updates-in-range',
        spans: [{ id: { peer: peer as `${number}`, counter: from }, len: end - from }],
      });
      try {
        await this.ports.create(`${s.table}${LOG_SUFFIX}:${rowKey()}`, {
          doc: s.recordId,
          field: s.field,
          peer,
          data,
        });
      } catch (e) {
        this.logger.warn({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::flush' }, 'Queueing CRDT edits failed');
        this.retryLater(s);
        return;
      }
      s.local.peers[peer] = { enqueued: end };
    }
    await this.persist(s);
  }

  private retryLater(s: Session): void {
    if (this.sessions.get(s.key) === s) this.scheduleFlush(s, CLAIM_RETRY_MS);
  }

  /** Claim this session's peer once; a failure (offline, signed out) is retried. */
  private ensureClaimed(): Promise<boolean> {
    if (!this.claim) {
      const peer = this.peer;
      this.claim = this.claimPeer(peer).then((ok) => {
        if (!ok && this.peer === peer) this.claim = null;
        return ok;
      });
    }
    return this.claim;
  }

  private async claimPeer(peer: `${number}`): Promise<boolean> {
    try {
      // A refused CREATE (signed out, impersonating) answers with no row.
      const [created] = await this.remote.query<[unknown[]]>("CREATE type::record('_00_crdt_peer', $peer) RETURN id", { peer });
      return Array.isArray(created) && created.length > 0;
    } catch (e) {
      if (!/already exists/i.test(String(e))) return false;
      // Ours from an earlier claim (only its owner can read it), or taken.
      try {
        const [owner] = await this.remote.query<[unknown]>(
          "SELECT VALUE author FROM ONLY type::record('_00_crdt_peer', $peer)",
          { peer }
        );
        if (owner != null) return true;
      } catch {
        return false;
      }
      this.logger.error({ peer, Category: 'sp00ky-client::CrdtManager::claim' }, 'CRDT peer id taken by another user');
      if (peer === this.peer) this.setSessionId('');
      return false;
    }
  }

  // --- local state -------------------------------------------------

  private localId(recordId: string, field: string): RecordId {
    return new RecordId(LOCAL_TABLE, `${recordId}|${field}`);
  }

  private async readLocal(recordId: string, field: string): Promise<LocalState> {
    try {
      const row = await this.local.getById(LOCAL_TABLE, this.localId(recordId, field));
      if (row) {
        return {
          snapshot: asBytes(row.snapshot),
          rev: Number(row.rev ?? 0),
          peers: (row.peers as LocalState['peers']) ?? {},
        };
      }
    } catch (e) {
      this.logger.debug({ error: String(e), Category: 'sp00ky-client::CrdtManager::readLocal' }, 'No local CRDT state');
    }
    return { rev: 0, peers: {} };
  }

  private schedulePersist(s: Session): void {
    if (s.persistTimer) return;
    s.persistTimer = setTimeout(() => {
      s.persistTimer = null;
      void this.persist(s);
    }, PERSIST_DEBOUNCE_MS);
  }

  /** Save the merged document. Another tab may have saved too: merge with
   *  its snapshot rather than overwrite it, and keep the further of the two
   *  outbox marks per peer. */
  private async persist(s: Session): Promise<void> {
    try {
      const stored = await this.readLocal(s.recordId, s.field);
      if (stored.snapshot) this.tryImport(s.doc, stored.snapshot, s.key);
      for (const [peer, mark] of Object.entries(stored.peers)) {
        const mine = s.local.peers[peer]?.enqueued ?? 0;
        if (mark.enqueued > mine) s.local.peers[peer] = { enqueued: mark.enqueued };
      }
      s.local.rev = Math.max(s.local.rev, stored.rev);
      s.local.snapshot = s.doc.export({ mode: 'snapshot' });
      await this.local.upsert(
        LOCAL_TABLE,
        this.localId(s.recordId, s.field),
        { doc: s.recordId, field: s.field, snapshot: s.local.snapshot, rev: s.local.rev, peers: s.local.peers },
        'replace'
      );
    } catch (e) {
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::persist' }, 'Saving local CRDT state failed');
    }
  }

  // --- presence ----------------------------------------------------

  private pushPresence(s: Session, state: Uint8Array): void {
    s.presencePending = state;
    if (s.presenceTimer) return;
    s.presenceTimer = setTimeout(() => {
      s.presenceTimer = null;
      const data = s.presencePending;
      s.presencePending = null;
      if (data) void this.sendPresence(s, data);
    }, PRESENCE_THROTTLE_MS);
  }

  /** Presence is the latest state only: no outbox, no offline queue. */
  private async sendPresence(s: Session, state: Uint8Array): Promise<void> {
    if (!(await this.ensureClaimed())) return;
    try {
      await this.remote.query(
        `UPSERT type::record($table, [$doc, $field, $session]) SET doc = type::record($doc), field = $field, session = $session, state = $state RETURN NONE`,
        { table: `${s.table}${PRESENCE_SUFFIX}`, doc: s.recordId, field: s.field, session: this.peer, state }
      );
    } catch (e) {
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::presence' }, 'Presence update failed');
    }
  }

  private ingestPresence(s: Session, rows: Record<string, unknown>[]): void {
    for (const row of rows) {
      if (String(row.session ?? '') === this.peer) continue;
      const state = asBytes(row.state);
      if (state) s.handle.deliverPresence(state);
    }
  }
}

function rowKey(): string {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

// Kept for callers that parse record ids next to CRDT fields.
export { parseRecordIdString };
