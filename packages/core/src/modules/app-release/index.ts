import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { AuthService } from '../auth/index';
import type { Logger } from '../../services/logger/index';
import type { QueryTimeToLive } from '../../types';
import { semverGt } from '../../utils/semver';

// One shared LIVE query over every app's release row. `_00_app_release` is
// world-readable (root-only writes), one row per app keyed by name — written
// by `spky deploy` / `spky release` / the git-linked builder. A single
// registration observes every app at once; a handle for an app with no row
// simply reports no update. Mirrors the FeatureFlagModule design.
const RELEASE_QUERY = 'SELECT * FROM _00_app_release';

interface ReleaseRow {
  app?: string;
  version?: string;
  cache_bust?: boolean | null;
  mandatory?: boolean | null;
  released_at?: string;
}

export interface AppReleaseSnapshot {
  /** Latest announced version for the app, or undefined when no row exists. */
  version: string | undefined;
  /** Clients should clear SW/caches when reloading onto this version. */
  cacheBust: boolean;
  /** Clients should reload/update immediately instead of asking. */
  mandatory: boolean;
  releasedAt: string | undefined;
}

const EMPTY_SNAPSHOT: AppReleaseSnapshot = {
  version: undefined,
  cacheBust: false,
  mandatory: false,
  releasedAt: undefined,
};

export interface AppReleaseOptions {
  ttl?: QueryTimeToLive;
}

export class AppReleaseHandle {
  private latest: AppReleaseSnapshot = EMPTY_SNAPSHOT;
  private listeners = new Set<(s: AppReleaseSnapshot) => void>();
  private onCloseFn: (() => void) | null = null;
  private closed = false;

  constructor(public readonly app: string) {}

  set(snapshot: AppReleaseSnapshot): void {
    if (this.closed) return;
    this.latest = snapshot;
    for (const cb of this.listeners) cb(snapshot);
  }

  snapshot(): AppReleaseSnapshot {
    return this.latest;
  }

  version(): string | undefined {
    return this.latest.version;
  }

  /** True when the announced version is semver-newer than `currentVersion`. */
  updateAvailable(currentVersion: string): boolean {
    return semverGt(this.latest.version, currentVersion);
  }

  subscribe(cb: (s: AppReleaseSnapshot) => void): () => void {
    this.listeners.add(cb);
    cb(this.latest);
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

export interface AppReleaseModuleDeps<S extends SchemaStructure> {
  dataModule: LiveQueryPort;
  sync: RemoteRegisterPort;
  auth: Pick<AuthService<S>, 'subscribe'>;
  logger: Logger;
}

export class AppReleaseModule<S extends SchemaStructure> {
  private logger: Logger;
  private handles = new Set<AppReleaseHandle>();
  private authUnsubscribe: (() => void) | null = null;
  private lastUserId: string | null = null;

  private querySubscription: (() => void) | null = null;
  private starting = false;
  private ttl: QueryTimeToLive = '10m';
  private snapshots = new Map<string, AppReleaseSnapshot>();
  private loaded = false;

  constructor(private deps: AppReleaseModuleDeps<S>) {
    this.logger = deps.logger.child({ service: 'AppReleaseModule' });
  }

  init(): void {
    if (this.authUnsubscribe) return;
    // Auth changes re-register the shared query (a new session invalidates the
    // old SSP plan). The table itself is world-readable, so the data is the
    // same for every user — this is purely plumbing hygiene.
    this.authUnsubscribe = this.deps.auth.subscribe((userId) => {
      if (userId === this.lastUserId) return;
      this.lastUserId = userId;
      void this.refresh();
    });
  }

  release(app: string, options: AppReleaseOptions = {}): AppReleaseHandle {
    const handle = new AppReleaseHandle(app);
    this.handles.add(handle);
    handle.onClose(() => this.handles.delete(handle));
    if (options.ttl) this.ttl = options.ttl;
    if (this.loaded) {
      handle.set(this.snapshots.get(app) ?? EMPTY_SNAPSHOT);
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

  private async refresh(): Promise<void> {
    this.teardownQuery();
    this.loaded = false;
    this.snapshots.clear();
    await this.ensureStarted();
  }

  private teardownQuery(): void {
    this.querySubscription?.();
    this.querySubscription = null;
  }

  private async ensureStarted(): Promise<void> {
    if (this.querySubscription || this.starting || this.handles.size === 0) return;
    this.starting = true;
    try {
      const hash = await this.deps.dataModule.query(
        '_00_app_release' as any,
        RELEASE_QUERY,
        {},
        this.ttl,
      );
      this.deps.sync.enqueueDownEvent({ type: 'register', payload: { hash } });
      this.querySubscription = this.deps.dataModule.subscribe(
        hash,
        (records) => this.applyRecords(records as ReleaseRow[]),
        { immediate: true },
      );
    } catch (err) {
      this.logger.warn(
        { err, Category: 'sp00ky-client::AppReleaseModule::register' },
        'Failed to register app release query',
      );
    } finally {
      this.starting = false;
    }
  }

  private applyRecords(records: ReleaseRow[]): void {
    this.snapshots.clear();
    for (const row of records ?? []) {
      if (row && typeof row.app === 'string' && typeof row.version === 'string') {
        this.snapshots.set(row.app, {
          version: row.version,
          cacheBust: row.cache_bust === true,
          mandatory: row.mandatory === true,
          releasedAt: row.released_at,
        });
      }
    }
    this.loaded = true;
    for (const handle of this.handles) {
      handle.set(this.snapshots.get(handle.app) ?? EMPTY_SNAPSHOT);
    }
  }
}
