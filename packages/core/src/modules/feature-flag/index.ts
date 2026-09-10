import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { AuthService } from '../auth/index';
import type { Logger } from '../../services/logger/index';
import type { QueryTimeToLive } from '../../types';

// One shared LIVE query over ALL of the signed-in user's assignments — the
// `_00_user_feature` select permission scopes it to `user = $auth.id`, so no
// per-key `WHERE key = $key` param is needed. A single registration means every
// flag the user is (or becomes) assigned is observed at once: new assignments
// stream in live, and a handle for an unassigned key simply resolves to its
// fallback. Avoids one-registration-per-flag and the param-filtered live query.
const FEATURE_QUERY = 'SELECT key, variant, payload FROM _00_user_feature';

// Local overrides live on the PAGE origin, so they survive reloads and are
// shared across tabs of the app. Deliberately not the DevTools panel's own
// storage — the panel runs on a different origin and would get a separate
// bucket.
const OVERRIDE_STORAGE_KEY = 'sp00ky:feature-overrides';

interface FeatureRow {
  key?: string;
  variant?: string;
  payload?: unknown;
}

export interface FeatureFlagSnapshot {
  variant: string | undefined;
  payload: unknown | undefined;
}

export interface FeatureFlagOptions {
  fallback?: string;
  ttl?: QueryTimeToLive;
}

/**
 * A locally forced variant. Applies to THIS browser only and is never sent to
 * the server — the assignment in `_00_user_feature` is untouched, so clearing
 * the override restores whatever the server says.
 */
export interface FeatureFlagOverride {
  variant: string;
  payload?: unknown;
}

export class FeatureFlagHandle {
  private latest: FeatureFlagSnapshot = { variant: undefined, payload: undefined };
  private listeners = new Set<(s: FeatureFlagSnapshot) => void>();
  private unsubscribeFn: (() => void) | null = null;
  private onCloseFn: (() => void) | null = null;
  private closed = false;

  constructor(
    public readonly key: string,
    public readonly fallback: string | undefined,
  ) {}

  attach(unsubscribe: () => void): void {
    this.unsubscribeFn?.();
    this.unsubscribeFn = unsubscribe;
  }

  detach(): void {
    this.unsubscribeFn?.();
    this.unsubscribeFn = null;
  }

  set(snapshot: FeatureFlagSnapshot): void {
    if (this.closed) return;
    this.latest = snapshot;
    for (const cb of this.listeners) cb(snapshot);
  }

  variant(): string | undefined {
    return this.latest.variant ?? this.fallback;
  }

  payload<T = unknown>(): T | undefined {
    return this.latest.payload as T | undefined;
  }

  enabled(): boolean {
    const v = this.variant();
    return v !== undefined && v !== 'off';
  }

  subscribe(cb: (s: FeatureFlagSnapshot) => void): () => void {
    this.listeners.add(cb);
    cb({ variant: this.variant(), payload: this.latest.payload });
    return () => {
      this.listeners.delete(cb);
    };
  }

  onClose(cb: () => void): void {
    this.onCloseFn = cb;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.listeners.clear();
    this.detach();
    this.onCloseFn?.();
  }
}

/** The slice of the query engine this module drives: register a live query
 *  locally, ask for its remote registration, subscribe to its rows. */
export interface LiveQueryPort {
  query(table: string, surql: string, params: Record<string, unknown>, ttl: QueryTimeToLive): Promise<string>;
  subscribe(hash: string, cb: (records: Record<string, any>[]) => void, options?: { immediate?: boolean }): () => void;
}
export interface RemoteRegisterPort {
  enqueueDownEvent(event: { type: 'register'; payload: { hash: string } }): void;
}

export interface FeatureFlagModuleDeps<S extends SchemaStructure> {
  dataModule: LiveQueryPort;
  sync: RemoteRegisterPort;
  auth: Pick<AuthService<S>, 'subscribe'>;
  logger: Logger;
}

export class FeatureFlagModule<S extends SchemaStructure> {
  private logger: Logger;
  private handles = new Set<FeatureFlagHandle>();
  private authUnsubscribe: (() => void) | null = null;
  private lastUserId: string | null = null;

  // The single shared live query over the user's assignments.
  private querySubscription: (() => void) | null = null;
  private starting = false;
  // Longest TTL any caller asked for (the query is shared across all flags).
  private ttl: QueryTimeToLive = '10m';
  // Latest assignment per key, plus whether the query has resolved at least
  // once (so a handle created before the first result knows to wait vs. fall
  // back). `snapshots` only holds ASSIGNED keys; an absent key → fallback.
  private snapshots = new Map<string, FeatureFlagSnapshot>();
  private loaded = false;
  // Developer-forced variants, this browser only. Take precedence over the
  // server assignment for every read path.
  private overrides = new Map<string, FeatureFlagOverride>();

  constructor(private deps: FeatureFlagModuleDeps<S>) {
    this.logger = deps.logger.child({ service: 'FeatureFlagModule' });
    // Loaded in the constructor rather than `init()` so an override applies
    // even when `client.feature()` is called before auth resolves.
    this.loadOverrides();
  }

  init(): void {
    if (this.authUnsubscribe) return;
    this.authUnsubscribe = this.deps.auth.subscribe((userId) => {
      if (userId === this.lastUserId) return;
      this.lastUserId = userId;
      void this.refresh();
    });
  }

  feature(key: string, options: FeatureFlagOptions = {}): FeatureFlagHandle {
    const handle = new FeatureFlagHandle(key, options.fallback);
    this.handles.add(handle);
    handle.onClose(() => this.handles.delete(handle));
    if (options.ttl) this.ttl = options.ttl;
    // If the shared query already resolved, seed this handle immediately so a
    // late `feature()` call doesn't flash the fallback for an assigned key.
    // An override seeds it too, even before the first result: forcing a
    // variant should take effect instantly, not one round trip later.
    if (this.loaded || this.overrides.has(key)) {
      handle.set(this.resolve(key));
    }
    void this.ensureStarted();
    return handle;
  }

  async closeAll(): Promise<void> {
    this.authUnsubscribe?.();
    this.authUnsubscribe = null;
    this.teardownQuery();
    for (const handle of [...this.handles]) handle.close();
  }

  /** Auth changed: drop the old user's query/snapshots and re-observe. */
  private async refresh(): Promise<void> {
    this.teardownQuery();
    this.loaded = false;
    this.snapshots.clear();
    // Clear handles immediately so a sign-out hides flag-gated UI without lag.
    // `resolve` keeps any local override in place across the switch — it is a
    // developer setting for this browser, not part of the session.
    for (const handle of this.handles) {
      handle.set(this.resolve(handle.key));
    }
    await this.ensureStarted();
  }

  private teardownQuery(): void {
    this.querySubscription?.();
    this.querySubscription = null;
  }

  /** Start the single shared live query (idempotent; no-op with no handles). */
  private async ensureStarted(): Promise<void> {
    if (this.querySubscription || this.starting || this.handles.size === 0) return;
    this.starting = true;
    try {
      const hash = await this.deps.dataModule.query(
        '_00_user_feature' as any,
        FEATURE_QUERY,
        {},
        this.ttl,
      );
      this.deps.sync.enqueueDownEvent({ type: 'register', payload: { hash } });
      this.querySubscription = this.deps.dataModule.subscribe(
        hash,
        (records) => this.applyRecords(records as FeatureRow[]),
        { immediate: true },
      );
    } catch (err) {
      this.logger.warn(
        { err, Category: 'sp00ky-client::FeatureFlagModule::register' },
        'Failed to register feature flag query',
      );
    } finally {
      this.starting = false;
    }
  }

  /** Live query result → per-key snapshots → push to every active handle. */
  private applyRecords(records: FeatureRow[]): void {
    this.snapshots.clear();
    for (const row of records ?? []) {
      if (row && typeof row.key === 'string') {
        this.snapshots.set(row.key, { variant: row.variant, payload: row.payload });
      }
    }
    this.loaded = true;
    this.pushAll();
  }

  // ===========================================================
  // Local overrides (this browser only)
  // ===========================================================

  /**
   * Force `key` to `variant` in THIS browser. Pass `null` to clear.
   *
   * Nothing is written to the server: the `_00_user_feature` assignment is
   * untouched, so clearing restores whatever the server says. Persisted to
   * localStorage on the page origin, so it survives a reload.
   */
  setLocalOverride(key: string, variant: string | null, payload?: unknown): void {
    if (variant === null) this.overrides.delete(key);
    else this.overrides.set(key, { variant, payload });
    this.persistOverrides();
    this.pushAll();
  }

  clearLocalOverrides(): void {
    this.overrides.clear();
    this.persistOverrides();
    this.pushAll();
  }

  getLocalOverrides(): Record<string, FeatureFlagOverride> {
    return Object.fromEntries(this.overrides);
  }

  /** The assignment for `key`, with any local override taking precedence. */
  private resolve(key: string): FeatureFlagSnapshot {
    const override = this.overrides.get(key);
    if (override) return { variant: override.variant, payload: override.payload };
    return this.snapshots.get(key) ?? { variant: undefined, payload: undefined };
  }

  private pushAll(): void {
    for (const handle of this.handles) handle.set(this.resolve(handle.key));
  }

  private loadOverrides(): void {
    try {
      const raw = globalThis.localStorage?.getItem(OVERRIDE_STORAGE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as Record<string, FeatureFlagOverride>;
      for (const [key, value] of Object.entries(parsed ?? {})) {
        if (value && typeof value.variant === 'string') this.overrides.set(key, value);
      }
    } catch (err) {
      // Best-effort: a corrupt or unavailable store must never stop the
      // client from booting. Same posture as the DevTools prefs helper.
      this.logger.warn(
        { err, Category: 'sp00ky-client::FeatureFlagModule::loadOverrides' },
        'Failed to read local feature flag overrides',
      );
    }
  }

  private persistOverrides(): void {
    try {
      if (this.overrides.size === 0) {
        globalThis.localStorage?.removeItem(OVERRIDE_STORAGE_KEY);
        return;
      }
      globalThis.localStorage?.setItem(
        OVERRIDE_STORAGE_KEY,
        JSON.stringify(this.getLocalOverrides()),
      );
    } catch (err) {
      this.logger.warn(
        { err, Category: 'sp00ky-client::FeatureFlagModule::persistOverrides' },
        'Failed to persist local feature flag overrides',
      );
    }
  }
}
