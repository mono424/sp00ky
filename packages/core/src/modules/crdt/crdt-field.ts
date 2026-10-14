import type { LoroDoc } from 'loro-crdt';
import type { CrdtKind } from './mirror';

// ==================== CURSOR UTILITIES ====================

export const CURSOR_COLORS = [
  '#3b82f6', '#ef4444', '#22c55e', '#f59e0b',
  '#8b5cf6', '#ec4899', '#14b8a6', '#f97316',
];

export function cursorColorFromName(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = ((hash << 5) - hash + name.charCodeAt(i)) | 0;
  }
  return CURSOR_COLORS[Math.abs(hash) % CURSOR_COLORS.length];
}

export { decodeBase64, encodeBase64 } from './bytes';

// ==================== CRDT FIELD ====================

/** What a field handle needs from the manager that owns its session. */
export interface CrdtFieldHost {
  pushPresence(state: Uint8Array): void;
}

/**
 * An open collaborative field: the Loro document an editor binds to. Edits
 * made on it are sent as the client's own edit-log rows; other clients' rows
 * are imported into it as they arrive. Every open of the same field in one tab
 * shares this document.
 */
export class CrdtField {
  private _onCursorUpdate: ((data: Uint8Array) => void) | null = null;
  private pendingCursorUpdate: Uint8Array | null = null;
  private loaded = false;
  private loadedListeners = new Set<() => void>();

  constructor(
    readonly table: string,
    readonly recordId: string,
    readonly fieldName: string,
    readonly kind: CrdtKind,
    readonly cursorsEnabled: boolean,
    private readonly doc: LoroDoc,
    private readonly host: CrdtFieldHost
  ) {}

  getDoc(): LoroDoc {
    return this.doc;
  }

  /** Whether the document holds anything yet (local state or edits). */
  hasContent(): boolean {
    return this.doc.oplogVersion().length() > 0;
  }

  /**
   * Whether the document is known: this device had it, it was handed a draft,
   * or the server answered (with content, or that it has none yet). Until then
   * an empty document means "not loaded", not "empty": show the column's text
   * instead, and do not bind an editor (an editor binding an empty document
   * creates structure that competes with the real one).
   */
  isLoaded(): boolean {
    return this.loaded;
  }

  /** Runs `cb` once the field is loaded (at once if it already is). Returns
   *  an unsubscribe. */
  onLoaded(cb: () => void): () => void {
    if (this.loaded) {
      cb();
      return () => {};
    }
    this.loadedListeners.add(cb);
    return () => this.loadedListeners.delete(cb);
  }

  /** @internal The manager calls this when the document became known. */
  markLoaded(): void {
    if (this.loaded) return;
    this.loaded = true;
    for (const cb of [...this.loadedListeners]) {
      try {
        cb();
      } catch {
        /* a listener's failure is its own */
      }
    }
    this.loadedListeners.clear();
  }

  /** Publish this session's presence (an `EphemeralStore` encoding, e.g. the
   *  editor's cursor). No-op unless the field is `@cursor`. */
  pushCursorState(encoded: Uint8Array): void {
    if (this.cursorsEnabled) this.host.pushPresence(encoded);
  }

  /** Called with every other session's presence blob. A blob that arrives
   *  before the editor sets this is replayed when it does. */
  set onCursorUpdate(cb: ((data: Uint8Array) => void) | null) {
    this._onCursorUpdate = cb;
    if (cb && this.pendingCursorUpdate) {
      const data = this.pendingCursorUpdate;
      this.pendingCursorUpdate = null;
      try {
        cb(data);
      } catch {
        /* the editor logs its own apply failures */
      }
    }
  }

  get onCursorUpdate() {
    return this._onCursorUpdate;
  }

  /** @internal Deliver one remote presence blob. */
  deliverPresence(data: Uint8Array): void {
    if (this._onCursorUpdate) this._onCursorUpdate(data);
    else this.pendingCursorUpdate = data;
  }
}
