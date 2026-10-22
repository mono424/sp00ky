import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { LoroDoc } from 'loro-crdt';
import { RecordId } from 'surrealdb';
import type { LocalStore, RemoteDatabaseService } from '../../services/database/index';
import type { Logger } from '../../services/logger/index';
import { parseRecordIdString } from '../../utils/index';
import { check } from './acceptance';
import { asBytes, randomPeer } from './bytes';
import { CrdtField } from './crdt-field';
import { stateHash } from './hash';
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

/** A followed query: stop following it, and know when the server answered. */
export interface Watch {
  stop: () => void;
  /** Resolves once the server's membership for the query is known and its
   *  rows have landed locally (never rejects; may never resolve offline). */
  settled: Promise<void>;
}

/** The runtime pieces the manager needs from the client facade. */
export interface CrdtPorts {
  /** Register a query on `table` filtered by `where` and follow its rows. */
  watch(table: string, where: Record<string, unknown>, onRows: (rows: Record<string, unknown>[]) => void): Promise<Watch>;
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
  /** Log rows in the latest delivery: when some vanish, the server folded. */
  present: Set<string>;
  /** Highest published rev this device's document was checked against. */
  verifiedRev: number;
  verifyTimer: ReturnType<typeof setTimeout> | null;
  disposers: Array<() => void>;
  flushTimer: ReturnType<typeof setTimeout> | null;
  /** When the armed flush timer fires. */
  flushDueAt: number;
  /** When this session last sent edits (0: not yet). */
  lastSentAt: number;
  persistTimer: ReturnType<typeof setTimeout> | null;
  flushing: Promise<void> | null;
  /** A flush asked for while one was running: it runs once that one is done. */
  flushQueued: Promise<void> | null;
  refetching: boolean;
  presenceTimer: ReturnType<typeof setTimeout> | null;
  presencePending: Uint8Array | null;
  /** This session wrote a presence row (removed again on close). */
  presenceSent: boolean;
  /** The latest import of delivered rows (chained, so awaiting it awaits all). */
  ingesting: Promise<void>;
  /** The server answered the published-document read (with a row or none). */
  canonicalAnswered: boolean;
  /** The log watch delivered the server's first answer. */
  logSettled: boolean;
  /** The second published-document read of a first open (see `recheckCanonical`). */
  recheck: Promise<void> | null;
}

const LOCAL_TABLE = '_00_crdt_local';
const LOG_SUFFIX = '__crdt_log';
const PRESENCE_SUFFIX = '__crdt_presence';
const PERSIST_DEBOUNCE_MS = 400;
const PRESENCE_THROTTLE_MS = 250;
const CLAIM_RETRY_MS = 5000;
/** Longest an open or close waits on the server (the first answer for a
 *  field with nothing local, the last edits going out) before going on
 *  without it. Offline it does not wait at all. */
const SERVER_WAIT_MS = 4000;
/** Pause after a fold is noticed before checking the published document. */
const VERIFY_DELAY_MS = 1000;

export class CrdtManager {
  private sessions = new Map<string, Session>();
  private opening = new Map<string, Promise<Session>>();
  /** Closes still saving their field's local state, by session key. */
  private saving = new Map<string, Promise<void>>();
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
    /** First edit after a quiet spell: wait this long to batch the burst. */
    private debounceMs: number = 50,
    /** Shortest gap between two sends of one field, while edits keep coming. */
    private sendIntervalMs: number = 200
  ) {
    this.logger = logger.child({ service: 'CrdtManager' });
    this.connectionUnsubscribes.push(
      this.remote.subscribeConnection('connected', () => {
        for (const s of this.sessions.values()) {
          this.scheduleFlush(s, 0);
          // Opened while the server could not answer: read it now.
          if (!s.handle.isLoaded() && !s.canonicalAnswered) void this.refetchCanonical(s);
        }
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
   *
   * `draft` (from {@link createDraft}) hands over edits made before the record
   * existed: they are merged in and sent like this tab's own.
   */
  async open(table: string, recordId: string, field: string, draft?: CrdtField): Promise<CrdtField> {
    const { kind, cursor } = this.fieldSpec(table, field);
    const key = `${table}|${recordId}|${field}`;
    let session = this.sessions.get(key);
    if (session) {
      session.refs++;
    } else {
      const inFlight = this.opening.get(key);
      if (inFlight) {
        session = await inFlight;
        session.refs++;
      } else {
        const pending = this.start(key, table, recordId, field, kind, cursor, !!draft).finally(() => this.opening.delete(key));
        this.opening.set(key, pending);
        session = await pending;
      }
    }
    if (draft) this.adopt(session, draft.getDoc());
    return session.handle;
  }

  /**
   * A detached document for a field of a record that does not exist yet (a
   * "new thread" form). Nothing is sent; pass it to {@link open} once the
   * record is created and its edits become the record's first ones.
   */
  async createDraft(table: string, field: string): Promise<CrdtField> {
    const { kind, cursor } = this.fieldSpec(table, field);
    const loro = await loadLoro();
    return new CrdtField(table, '', field, kind, cursor, new loro.LoroDoc(), { pushPresence: () => {} });
  }

  private adopt(s: Session, draft: LoroDoc): void {
    const peers = [...draft.oplogVersion().toJSON().keys()];
    if (peers.length === 0) return;
    this.tryImport(s.doc, draft.export({ mode: 'update' }), s.key);
    // The draft's ops carry its own peer: claim and send them under it.
    for (const peer of peers) s.local.peers[String(peer)] ??= { enqueued: 0 };
    this.schedulePersist(s);
    this.scheduleFlush(s, 0);
  }

  /** Close one open. The last close of a field resolves once its unsent
   *  edits are in the outbox, or once they are saved on the device when the
   *  server does not answer in time (offline: at once). */
  close(table: string, recordId: string, field: string): Promise<void> {
    const s = this.sessions.get(`${table}|${recordId}|${field}`);
    if (!s || --s.refs > 0) return Promise.resolve();
    return this.stop(s, true);
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
    cursor: boolean,
    /** Opened with a draft: the record was just created here, so the server
     *  has nothing for it yet and the open does not wait on it. */
    fresh = false
  ): Promise<Session> {
    if (!this.ports) throw new Error('CrdtManager: not attached to a client');
    const loro = await loadLoro();
    const doc = new loro.LoroDoc();
    doc.setPeerId(this.peer);

    // A close of this field may still be saving: read what it saves.
    await this.saving.get(key);
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
      present: new Set(),
      verifiedRev: 0,
      verifyTimer: null,
      disposers: [],
      flushTimer: null,
      flushDueAt: 0,
      lastSentAt: 0,
      persistTimer: null,
      flushing: null,
      flushQueued: null,
      refetching: false,
      presenceTimer: null,
      presencePending: null,
      presenceSent: false,
      ingesting: Promise.resolve(),
      canonicalAnswered: false,
      logSettled: false,
      recheck: null,
    };
    session.handle = new CrdtField(table, recordId, field, kind, cursor, doc, {
      pushPresence: (state) => this.pushPresence(session, state),
    });
    // Known from the start: this device's copy, or a record just created here.
    if (local.snapshot || fresh) session.handle.markLoaded();
    this.sessions.set(key, session);

    // Claimed now, so the first edits do not wait on the round trip.
    void this.ensureClaimed();
    // First open on this device: start from the published document. Read at
    // the same time as the log query below registers, not before it: each is
    // a round trip, and the import order does not matter (ops whose
    // predecessors are still missing wait inside Loro until they arrive).
    const canonical = !local.snapshot && !fresh ? this.refetchCanonical(session) : null;

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
    const logWatch = ports.watch(`${table}${LOG_SUFFIX}`, { doc: recordId, field }, (rows) => {
      session.ingesting = session.ingesting.then(() => this.ingest(session, rows));
    });
    const presenceWatch = cursor
      ? ports.watch(`${table}${PRESENCE_SUFFIX}`, { doc: recordId, field }, (rows) => this.ingestPresence(session, rows))
      : null;
    const [log, presence] = await Promise.all([logWatch, presenceWatch]);
    session.disposers.push(log.stop);
    if (presence) session.disposers.push(presence.stop);
    void log.settled.then(async () => {
      session.logSettled = true;
      await session.ingesting;
      await this.recheckCanonical(session, canonical);
      this.checkLoaded(session);
    });
    // Offline the published document is read once the connection is back
    // instead of holding the open.
    if (canonical) await this.serverWait(canonical);
    // An editor binding an empty document creates its root structure, and
    // two clients doing that at once keep only one of them (with whatever was
    // typed into the other). With nothing local, hand the document out only
    // once the server's rows for it are in.
    if (!session.handle.hasContent() && !fresh) await this.serverWait(log.settled);
    await session.ingesting;
    if (session.logSettled) await this.serverWait(this.recheckCanonical(session, canonical));
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
    if (s.verifyTimer) clearTimeout(s.verifyTimer);
    if (s.presenceSent) void this.dropPresence(s);
    if (flush) {
      const saved = this.persist(s);
      this.saving.set(s.key, saved);
      await saved;
      if (this.saving.get(s.key) === saved) this.saving.delete(s.key);
      await this.serverWait(this.flush(s));
    }
  }

  /**
   * Wait for server-bound `work`, at most {@link SERVER_WAIT_MS}, and not at
   * all while disconnected: a remote call made then queues behind the
   * reconnect for up to the query timeout, and an offline "new thread" form
   * must not hang on it. The work itself goes on either way.
   */
  private serverWait(work: Promise<unknown>): Promise<unknown> {
    if (this.remote.getStatus() !== 'connected') return Promise.resolve();
    return Promise.race([work, delay(SERVER_WAIT_MS)]);
  }

  // --- receiving ---------------------------------------------------

  private async ingest(s: Session, rows: Record<string, unknown>[]): Promise<void> {
    const loro = await loadLoro();
    const ids = new Set(rows.map((row) => String(row.id ?? '')));
    for (const id of s.present) {
      if (!ids.has(id)) {
        this.scheduleVerify(s);
        break;
      }
    }
    s.present = ids;
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
    this.checkLoaded(s);
    this.schedulePersist(s);
    // Ops depending on rows that were folded away before this device saw
    // them: the published snapshot holds those predecessors.
    if (pending) void this.refetchCanonical(s);
  }

  /** Mark the field loaded once its content arrived, or the server answered
   *  both reads (the published document and the log) with nothing. */
  private checkLoaded(s: Session): void {
    if (s.handle.hasContent() || (s.canonicalAnswered && s.logSettled)) s.handle.markLoaded();
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
      s.canonicalAnswered = true;
      this.checkLoaded(s);
    } catch (e) {
      // Offline or no published document yet: the log rows still arrive.
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::refetch' }, 'No canonical CRDT document fetched');
    } finally {
      s.refetching = false;
    }
  }

  /**
   * A first open reads the published document and the log at the same time,
   * and a fold landing between the two hides the field from both: it
   * publishes its snapshot, then deletes the rows the log query was about to
   * return, after the published-document read came back empty. Nothing seen
   * means nothing to verify, so the editor stayed empty until the next edit
   * (a second device opening a just-created thread, under e2e load). Once the
   * log has answered, a field that is still empty reads the published
   * document again: the fold that took its rows committed before deleting
   * them. Once per open; `first` is the first read (null with a local copy or
   * a draft, which need none).
   */
  private recheckCanonical(s: Session, first: Promise<void> | null): Promise<void> {
    if (!first) return Promise.resolve();
    s.recheck ??= (async () => {
      await first;
      await s.ingesting;
      if (!s.handle.hasContent()) await this.refetchCanonical(s);
    })();
    return s.recheck;
  }

  // --- checking against the published document ---------------------

  private scheduleVerify(s: Session): void {
    if (s.verifyTimer) clearTimeout(s.verifyTimer);
    s.verifyTimer = setTimeout(() => {
      s.verifyTimer = null;
      void this.verify(s);
    }, VERIFY_DELAY_MS);
  }

  /**
   * After a fold: compare this device's document with the one the server
   * published, at the version the server published. Missing ops (rows this
   * device never got before they were folded away) are repaired by merging
   * the published snapshot in. The same ops giving a different state cannot
   * be merged away; that is logged.
   */
  private async verify(s: Session): Promise<void> {
    type Published = {
      rev?: number;
      state_hash?: string;
      upto?: { vv?: Record<string, number>; frontiers?: { peer: `${number}`; counter: number }[] };
    };
    let row: Published | null;
    try {
      [row] = await this.remote.query<[Published | null]>(
        "SELECT rev, upto, state_hash FROM ONLY type::record('_00_crdt_doc', [type::record($doc), $field])",
        { doc: s.recordId, field: s.field }
      );
    } catch {
      return; // offline: the next fold checks again
    }
    const rev = row?.rev ?? 0;
    const frontiers = row?.upto?.frontiers;
    if (!row?.state_hash || !frontiers || !row.upto?.vv || rev <= s.verifiedRev || !this.sessions.has(s.key)) return;

    const have = s.doc.oplogVersion().toJSON();
    const behind = Object.entries(row.upto.vv).some(([peer, end]) => (have.get(peer as `${number}`) ?? 0) < end);
    if (behind) {
      this.logger.info({ key: s.key, rev, Category: 'sp00ky-client::CrdtManager::verify' }, 'CRDT document behind the published one; merging it in');
      await this.refetchCanonical(s);
      return;
    }
    s.verifiedRev = rev;
    // WebCrypto is missing on insecure origins: nothing to check with there.
    if (!globalThis.crypto?.subtle) return;
    const loro = await loadLoro();
    let fork: LoroDoc | undefined;
    try {
      fork = s.doc.forkAt(frontiers);
      const hash = await stateHash(loro, fork);
      if (hash !== row.state_hash) {
        this.logger.warn(
          { key: s.key, rev, hash, published: row.state_hash, Category: 'sp00ky-client::CrdtManager::verify' },
          'CRDT document differs from the published one at the same version'
        );
      }
    } catch (e) {
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::verify' }, 'CRDT document check failed');
    } finally {
      fork?.free();
    }
  }

  // --- sending -----------------------------------------------------

  /**
   * Arm the send. Edits go out on a steady cadence while they keep coming:
   * the first one after a quiet spell waits `debounceMs` (one row for the
   * burst), later ones at least `sendIntervalMs` after the previous send. An
   * armed timer is never pushed back, so continuous typing cannot hold the
   * edits; it is only brought forward (`delay`, e.g. 0 on reconnect).
   */
  private scheduleFlush(s: Session, delay?: number): void {
    const now = Date.now();
    const due = delay !== undefined ? now + delay : Math.max(now + this.debounceMs, s.lastSentAt + this.sendIntervalMs);
    if (s.flushTimer) {
      if (s.flushDueAt <= due) return;
      clearTimeout(s.flushTimer);
    }
    s.flushDueAt = due;
    s.flushTimer = setTimeout(() => {
      s.flushTimer = null;
      void this.flush(s);
    }, due - now);
  }

  /** Send what is unsent. A call while a send runs queues one more after it,
   *  so edits made during that send are not left waiting for the next edit. */
  private flush(s: Session): Promise<void> {
    if (s.flushing) {
      s.flushQueued ??= s.flushing.then(() => {
        s.flushQueued = null;
        return this.flush(s);
      });
      return s.flushQueued;
    }
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
    s.lastSentAt = Date.now();
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
    // The outbox marks reach the device with the next persist. An open field
    // sends every few hundred ms while typing, and a persist exports the whole
    // document, so it rides the persist debounce; a crash in between only
    // re-sends rows, which import as no-ops. A closing field saves now.
    if (this.sessions.get(s.key) === s) this.schedulePersist(s);
    else await this.persist(s);
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
        // One row per session and field, keyed by a plain identifier (an array
        // key does not survive the client's record-id handling). `$session`
        // is a protected SurrealDB variable: the session id binds as `$sid`.
        `UPSERT type::record($table, $key) SET doc = type::record($doc), field = $field, session = $sid, state = $state RETURN NONE`,
        {
          table: `${s.table}${PRESENCE_SUFFIX}`,
          key: presenceKey(this.peer, s.field, s.recordId),
          doc: s.recordId,
          field: s.field,
          sid: this.peer,
          state,
        }
      );
      s.presenceSent = true;
    } catch (e) {
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::presence' }, 'Presence update failed');
    }
  }

  /** Remove this session's cursor when the field closes; the server's TTL
   *  sweep covers a tab that never gets here. */
  private async dropPresence(s: Session): Promise<void> {
    try {
      await this.remote.query(`DELETE type::record($table, $key) RETURN NONE`, {
        table: `${s.table}${PRESENCE_SUFFIX}`,
        key: presenceKey(this.peer, s.field, s.recordId),
      });
    } catch (e) {
      this.logger.debug({ key: s.key, error: String(e), Category: 'sp00ky-client::CrdtManager::presence' }, 'Presence removal failed');
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

/** `p<session>_<field>_<record key>`, letters, digits and `_` only. */
function presenceKey(session: string, field: string, recordId: string): string {
  const key = recordId.slice(recordId.indexOf(':') + 1).replace(/[^A-Za-z0-9_]/g, '_');
  return `p${session}_${field}_${key}`;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function rowKey(): string {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

// Kept for callers that parse record ids next to CRDT fields.
export { parseRecordIdString };
