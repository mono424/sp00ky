import { RecordId } from 'surrealdb';
import type { Logger } from '../../services/logger/index';
import type { SignOutContext } from '../auth/index';
import type { PersistenceClient } from '../../types';
import { encodeRecordId, withTimeout } from '../../utils/index';
import {
  base64UrlToBytes,
  sameBytes,
  type BridgeTokenMessage,
  type PushNotification,
  type PushUrgency,
  type WebPushClientMessage,
} from '../../push/types';

/**
 * `db.webPush`: this browser's Web Push subscription, on the page.
 *
 * Every server call is a `fn::push::*` function over the client's remote
 * connection (see `apps/cli/src/push_tables.surql`). Nothing here touches a
 * browser API at import time, and every method answers "unsupported" instead
 * of throwing where there is no window, service worker or push manager
 * (SSR, Node, workers, iOS Safari outside a home-screen app).
 */

// ── config & public types ───────────────────────────────────────────────

export interface WebPushConfig {
  /**
   * Registered with `navigator.serviceWorker.register(url)` when the page has
   * no service worker yet. Leave unset when the app (or vite-plugin-pwa)
   * registers its own.
   */
  serviceWorkerUrl?: string;
  /**
   * Reconcile this browser's subscription after sign-in (see `sync()`): a
   * rotated key, a browser that dropped the subscription or a server row that
   * went away are repaired without a prompt. Default `true`.
   */
  autoSync?: boolean;
  /**
   * Let `sync()` subscribe a signed-in user whenever notification permission
   * is already granted, even if they never subscribed on this browser.
   * Default `false`: only a user who called `subscribe()` here is restored.
   */
  autoResubscribe?: boolean;
  /**
   * Remove this device's subscription for the user on `signOut()` (before the
   * token is dropped, bounded by a short timeout). Default `true`.
   */
  unsubscribeOnSignOut?: boolean;
  /**
   * Post the session token to the app's service worker whenever it changes,
   * while notifications are allowed, so `@spooky-sync/core/sw` can render from
   * live data. Never posts an impersonation token. Default `true`.
   */
  bridge?: boolean;
}

export type WebPushUnsupportedReason =
  | 'no-window'
  | 'insecure-context'
  | 'no-service-worker'
  | 'no-push-manager'
  | 'no-notification'
  | 'ios-not-installed';

export interface WebPushSupport {
  supported: boolean;
  permission: NotificationPermission | 'unsupported';
  reason?: WebPushUnsupportedReason;
}

export interface WebPushInfo {
  /** A host with push on has published its key. */
  enabled: boolean;
  /** VAPID public key (base64url), the browser's `applicationServerKey`. */
  publicKey?: string;
  /** Id of that key. */
  kid?: string;
}

export interface WebPushDevice {
  /** `_00_push_subscription:<id>` */
  id: string;
  endpoint: string;
  /** Key id the device subscribed under. */
  kid: string;
  label?: string;
  userAgent?: string;
  /** Rule names this device receives; undefined = every rule. */
  rules?: string[];
  meta?: Record<string, unknown>;
  createdAt?: Date;
  updatedAt?: Date;
  lastOkAt?: Date;
  lastError?: string;
  failures: number;
  disabledAt?: Date;
  disabledReason?: string;
  /** Subscribed under the current key (pushes reach it). Undefined when unknown. */
  current?: boolean;
  /** This browser. */
  thisDevice: boolean;
}

export interface WebPushSubscribeOptions {
  /** Defaults to the page's service worker registration (`serviceWorker.ready`). */
  registration?: ServiceWorkerRegistration;
  /** Shown in the device list ("Work laptop"). */
  label?: string;
  /** Rule names this device wants. Omitted or empty: every rule. */
  rules?: string[];
  /** Free-form app data about the device. */
  meta?: Record<string, unknown>;
  /** Ask for notification permission when it is still `default`. Default `true`. Call from a user gesture. */
  requestPermission?: boolean;
}

export interface WebPushUnsubscribeOptions {
  /** Remove every device of the user, not only this one. */
  all?: boolean;
  /** Keep the browser's PushSubscription (only the server row goes). */
  keepBrowserSubscription?: boolean;
}

export interface WebPushSyncOptions {
  registration?: ServiceWorkerRegistration;
  /** Overrides `webPush.autoResubscribe` for this call. */
  autoResubscribe?: boolean;
}

/**
 * What `sync()` did. `ok`: nothing to repair. `resubscribed`: the browser
 * subscription was (re)created. `registered`: the server row was re-created.
 * The rest say why nothing happened.
 */
export type WebPushSyncStatus =
  | 'ok'
  | 'resubscribed'
  | 'registered'
  | 'unsupported'
  | 'signed-out'
  | 'impersonating'
  | 'permission-default'
  | 'permission-denied'
  | 'not-subscribed'
  | 'disabled'
  | 'no-registration'
  | 'error';

export interface WebPushUpdateOptions {
  label?: string;
  /** Rule names; `null` or `[]` resets the device to every rule. */
  rules?: string[] | null;
  meta?: Record<string, unknown>;
}

/** A push to yourself (`fn::push::notify`). */
export interface WebPushMessageInput {
  /** Omit for a content-free nudge. */
  notification?: PushNotification;
  data?: Record<string, unknown>;
  topic?: string;
  urgency?: PushUrgency;
  /** Push-service TTL, seconds. */
  ttl?: number;
  /** In the future: scheduled. */
  sendAt?: Date | string | number;
}

export interface WebPushMessage {
  /** `_00_push_message:<id>` */
  id: string;
  status: string;
  sendAt?: Date;
  createdAt?: Date;
}

export type WebPushErrorCode =
  | 'unsupported'
  | 'signed-out'
  | 'impersonating'
  | 'disabled'
  | 'permission-denied'
  | 'permission-default'
  | 'no-registration'
  | 'not-subscribed'
  | 'subscribe-failed'
  | 'server';

export class WebPushError extends Error {
  constructor(
    public readonly code: WebPushErrorCode,
    message: string,
    public readonly cause?: unknown
  ) {
    super(message);
    this.name = 'WebPushError';
  }
}

// ── ports ────────────────────────────────────────────────────────────────

export interface WebPushAuthPort {
  readonly token: string | null;
  readonly currentUser: { id?: unknown } | null;
  readonly isAuthenticated: boolean;
  readonly impersonation: unknown;
  subscribe(cb: (userId: unknown) => void): () => void;
  onBeforeSignOut(hook: (ctx: SignOutContext) => Promise<void> | void): () => void;
}

export interface WebPushRemotePort {
  query<T extends unknown[]>(sql: string, vars?: Record<string, unknown>): Promise<T>;
}

/** Browser globals, read at call time. Injectable for tests. */
export interface WebPushGlobals {
  window?: unknown;
  navigator?: Navigator & { standalone?: boolean };
  Notification?: {
    permission: NotificationPermission;
    requestPermission(
      cb?: (p: NotificationPermission) => void
    ): Promise<NotificationPermission> | void;
  };
  isSecureContext?: boolean;
  matchMedia?: (query: string) => { matches: boolean };
  PushManager?: unknown;
}

export interface WebPushModuleDeps {
  remote: WebPushRemotePort;
  auth: WebPushAuthPort;
  persistence: PersistenceClient;
  logger: Logger;
  database: { endpoint?: string; namespace: string; database: string };
  /** Transport state; auto-sync waits for `connected`. */
  connection?: { subscribe(cb: (state: string) => void): () => void };
  config?: WebPushConfig;
  globals?: () => WebPushGlobals;
}

// ── helpers ──────────────────────────────────────────────────────────────

export const PUSH_ENDPOINT_KEY = 'sp00ky_push_endpoint';
const SIGN_OUT_UNSUBSCRIBE_TIMEOUT_MS = 1_500;
const SUBSCRIBE_READY_TIMEOUT_MS = 10_000;
const SYNC_READY_TIMEOUT_MS = 5_000;
const AUTO_SYNC_DELAY_MS = 1_500;

interface StoredEndpoint {
  endpoint: string;
  userId: string;
  kid?: string;
  publicKey?: string;
  at: number;
}

interface DeviceRow {
  id?: unknown;
  endpoint?: string;
  kid?: string;
  label?: string | null;
  user_agent?: string | null;
  rules?: string[] | null;
  meta?: Record<string, unknown> | null;
  created_at?: unknown;
  updated_at?: unknown;
  last_ok_at?: unknown;
  last_error?: string | null;
  failures?: number;
  disabled_at?: unknown;
  disabled_reason?: string | null;
  current?: boolean;
}

const noop = () => {};

/** `table:id` of a record id value, without the SDK's escaping. */
export function recordIdString(value: unknown): string {
  if (value instanceof RecordId) return encodeRecordId(value as RecordId<string>);
  const v = value as { table?: unknown; id?: unknown } | null;
  if (v && typeof v === 'object' && v.table !== undefined && v.id !== undefined) {
    const table =
      typeof v.table === 'string'
        ? v.table
        : String((v.table as { name?: unknown }).name ?? v.table);
    return `${table}:${String(v.id)}`;
  }
  return String(value ?? '');
}

function toDate(value: unknown): Date | undefined {
  if (value === null || value === undefined) return undefined;
  if (value instanceof Date) return value;
  const d = value as { toDate?: () => Date };
  if (typeof d.toDate === 'function') return d.toDate();
  const parsed = new Date(value as string | number);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

function dropUndefined<T extends Record<string, unknown>>(obj: T): T {
  for (const key of Object.keys(obj)) if (obj[key] === undefined) delete obj[key];
  return obj;
}

function toDevice(row: DeviceRow, thisEndpoint: string | null): WebPushDevice {
  return {
    id: recordIdString(row.id),
    endpoint: String(row.endpoint ?? ''),
    kid: typeof row.kid === 'string' ? row.kid : '',
    label: row.label ?? undefined,
    userAgent: row.user_agent ?? undefined,
    rules: Array.isArray(row.rules) ? row.rules : undefined,
    meta: row.meta ?? undefined,
    createdAt: toDate(row.created_at),
    updatedAt: toDate(row.updated_at),
    lastOkAt: toDate(row.last_ok_at),
    lastError: row.last_error ?? undefined,
    failures: typeof row.failures === 'number' ? row.failures : 0,
    disabledAt: toDate(row.disabled_at),
    disabledReason: row.disabled_reason ?? undefined,
    current: typeof row.current === 'boolean' ? row.current : undefined,
    thisDevice: !!thisEndpoint && row.endpoint === thisEndpoint,
  };
}

function toMessage(
  row:
    | { id?: unknown; status?: unknown; send_at?: unknown; created_at?: unknown }
    | null
    | undefined
): WebPushMessage {
  return {
    id: recordIdString(row?.id),
    status: typeof row?.status === 'string' ? row.status : 'pending',
    sendAt: toDate(row?.send_at),
    createdAt: toDate(row?.created_at),
  };
}

/** Where Web Push can work, from the given globals. */
export function detectWebPushSupport(g: WebPushGlobals): WebPushSupport {
  const nav = g.navigator;
  const permission: WebPushSupport['permission'] = g.Notification?.permission ?? 'unsupported';
  if (!g.window || !nav)
    return { supported: false, permission: 'unsupported', reason: 'no-window' };
  if (g.isSecureContext === false)
    return { supported: false, permission, reason: 'insecure-context' };
  const ua = nav.userAgent ?? '';
  const ios =
    /iPad|iPhone|iPod/.test(ua) || (nav.platform === 'MacIntel' && (nav.maxTouchPoints ?? 0) > 1);
  if (ios) {
    const standalone =
      nav.standalone === true || g.matchMedia?.('(display-mode: standalone)').matches === true;
    if (!standalone) return { supported: false, permission, reason: 'ios-not-installed' };
  }
  if (!('serviceWorker' in nav) || !nav.serviceWorker)
    return { supported: false, permission, reason: 'no-service-worker' };
  if (!g.PushManager) return { supported: false, permission, reason: 'no-push-manager' };
  if (!g.Notification)
    return { supported: false, permission: 'unsupported', reason: 'no-notification' };
  return { supported: true, permission };
}

function browserGlobals(): WebPushGlobals {
  const g = globalThis as Record<string, any>;
  return {
    window: typeof g.window !== 'undefined' ? g.window : undefined,
    navigator: g.navigator,
    Notification: g.Notification,
    isSecureContext: g.isSecureContext,
    matchMedia: typeof g.matchMedia === 'function' ? (q: string) => g.matchMedia(q) : undefined,
    PushManager: g.PushManager,
  };
}

function requestPermission(
  N: NonNullable<WebPushGlobals['Notification']>
): Promise<NotificationPermission> {
  return new Promise((resolve) => {
    // Old Safari only has the callback form; current browsers return a promise.
    const ret = N.requestPermission((p) => resolve(p));
    if (ret && typeof (ret as Promise<NotificationPermission>).then === 'function') {
      (ret as Promise<NotificationPermission>).then(resolve, () => resolve(N.permission));
    }
  });
}

/** `true`/`false` when the subscription says which key it was made with, `null` when it does not. */
function keyMatches(sub: PushSubscription, key: Uint8Array): boolean | null {
  const k = sub.options?.applicationServerKey;
  if (!k) return null;
  return sameBytes(k, key);
}

// ── module ───────────────────────────────────────────────────────────────

export class WebPushModule {
  private readonly logger: Logger;
  private readonly config: WebPushConfig;
  private readonly globals: () => WebPushGlobals;
  private infoCache: Promise<WebPushInfo> | null = null;
  private lastInfo: WebPushInfo | null = null;
  private readonly bridges = new Set<{ reset(): void }>();
  private readonly disposers: Array<() => void> = [];
  private syncedFor: string | null = null;
  private syncTimer: ReturnType<typeof setTimeout> | null = null;
  private attached = false;

  constructor(private readonly deps: WebPushModuleDeps) {
    this.logger = deps.logger.child({ service: 'WebPush' });
    this.config = deps.config ?? {};
    this.globals = deps.globals ?? browserGlobals;
  }

  /**
   * Wire sign-out, auto-sync and the token bridge. Called by the client once;
   * a no-op beyond the sign-out hook where push is unsupported.
   */
  attach(): () => void {
    if (this.attached) return () => this.dispose();
    this.attached = true;
    this.disposers.push(this.deps.auth.onBeforeSignOut((ctx) => this.beforeSignOut(ctx)));
    const g = this.globals();
    if (!g.window || !g.navigator?.serviceWorker) return () => this.dispose();
    if (this.config.bridge !== false) this.disposers.push(this.startBridge(undefined, true));
    if (this.config.autoSync !== false) {
      let connected = !this.deps.connection;
      let user: string | null = null;
      const maybeSync = () => {
        if (!connected || !user || this.syncedFor === user) return;
        if (!this.support().supported) return;
        this.syncedFor = user;
        if (this.syncTimer) clearTimeout(this.syncTimer);
        this.syncTimer = setTimeout(() => {
          this.syncTimer = null;
          void this.sync().then((status) => {
            if (status === 'error') this.syncedFor = null;
          });
        }, AUTO_SYNC_DELAY_MS);
      };
      if (this.deps.connection) {
        this.disposers.push(
          this.deps.connection.subscribe((state) => {
            connected = state === 'connected';
            maybeSync();
          })
        );
      }
      this.disposers.push(
        this.deps.auth.subscribe((uid) => {
          user = uid ? recordIdString(uid) : null;
          if (!user) this.syncedFor = null;
          maybeSync();
        }),
        this.onMessage((msg) => {
          if (msg.type !== 'sp00ky:subscriptionchange') return;
          void this.sync();
        })
      );
    }
    return () => this.dispose();
  }

  dispose(): void {
    if (this.syncTimer) clearTimeout(this.syncTimer);
    this.syncTimer = null;
    for (const off of this.disposers.splice(0)) off();
    this.bridges.clear();
    this.attached = false;
  }

  // ── reads ──

  support(): WebPushSupport {
    try {
      return detectWebPushSupport(this.globals());
    } catch {
      return { supported: false, permission: 'unsupported', reason: 'no-window' };
    }
  }

  /** Is push on for this deployment, and with which key. Cached; `refresh` re-reads. */
  info(options: { refresh?: boolean } = {}): Promise<WebPushInfo> {
    if (!this.infoCache || options.refresh) {
      const pending = this.call<{
        enabled?: boolean;
        publicKey?: string | null;
        kid?: string | null;
      } | null>('RETURN fn::push::info()').then((r) => {
        const info: WebPushInfo = dropUndefined({
          enabled: r?.enabled === true && typeof r.publicKey === 'string',
          publicKey: typeof r?.publicKey === 'string' ? r.publicKey : undefined,
          kid: typeof r?.kid === 'string' ? r.kid : undefined,
        });
        this.lastInfo = info;
        return info;
      });
      this.infoCache = pending;
      pending.catch(() => {
        if (this.infoCache === pending) this.infoCache = null;
      });
    }
    return this.infoCache;
  }

  /** This browser holds a push subscription registered for the signed-in user. No server round trip. */
  async isSubscribed(): Promise<boolean> {
    const support = this.support();
    if (!support.supported || support.permission !== 'granted') return false;
    const userId = this.userId();
    if (!userId) return false;
    const stored = await this.readStored();
    if (!stored || stored.userId !== userId) return false;
    const sub = await this.currentSubscription();
    return !!sub && sub.endpoint === stored.endpoint;
  }

  /** The user's devices, this one marked. */
  async devices(): Promise<WebPushDevice[]> {
    this.assertSignedIn();
    const rows = await this.listRaw();
    const mine = await this.thisEndpoint();
    return rows.map((r) => toDevice(r, mine));
  }

  // ── subscription ──

  /**
   * Ask for permission (when still `default`), subscribe this browser with the
   * deployment's key and register it for the signed-in user. Call it from a
   * click handler: permission prompts need a user gesture.
   */
  async subscribe(options: WebPushSubscribeOptions = {}): Promise<WebPushDevice> {
    const support = this.support();
    if (!support.supported)
      throw new WebPushError('unsupported', `web push is not available here (${support.reason})`);
    const userId = this.userId();
    if (!userId)
      throw new WebPushError('signed-out', 'sign in before subscribing to push notifications');
    if (this.deps.auth.impersonation)
      throw new WebPushError(
        'impersonating',
        'push notifications are not available while impersonating'
      );
    const N = this.globals().Notification!;
    // First await: the prompt must stay inside the caller's user gesture.
    let permission = support.permission as NotificationPermission;
    if (permission === 'default' && options.requestPermission !== false)
      permission = await requestPermission(N);
    if (permission === 'denied')
      throw new WebPushError('permission-denied', 'notification permission was denied');
    if (permission !== 'granted')
      throw new WebPushError('permission-default', 'notification permission was not granted');

    const info = await this.info();
    if (!info.enabled || !info.publicKey)
      throw new WebPushError('disabled', 'push is not enabled on this deployment yet');
    const registration = await this.registration(options.registration, SUBSCRIBE_READY_TIMEOUT_MS);
    if (!registration?.pushManager)
      throw new WebPushError('no-registration', 'no active service worker to subscribe with');

    const key = base64UrlToBytes(info.publicKey);
    let sub: PushSubscription | null;
    try {
      sub = await registration.pushManager.getSubscription();
      // A subscription made with another key would be rejected by the push service.
      if (sub && keyMatches(sub, key) === false) {
        await sub.unsubscribe().catch(() => false);
        sub = null;
      }
      sub ??= await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: key as BufferSource,
      });
    } catch (error) {
      throw new WebPushError(
        'subscribe-failed',
        error instanceof Error ? error.message : String(error),
        error
      );
    }
    const row = await this.register(sub, options);
    await this.writeStored({
      endpoint: sub.endpoint,
      userId,
      kid: row.kid || info.kid,
      publicKey: info.publicKey,
      at: Date.now(),
    });
    this.refreshBridges();
    return toDevice(row, sub.endpoint);
  }

  /** Remove this device (or every device, `all`) for the signed-in user. Returns how many rows went. */
  async unsubscribe(options: WebPushUnsubscribeOptions = {}): Promise<number> {
    const userId = this.userId();
    let removed = 0;
    const sub = await this.currentSubscription();
    if (userId && !this.deps.auth.impersonation) {
      if (options.all) {
        removed = Number(await this.call<number>('RETURN fn::push::unsubscribe(NONE)')) || 0;
      } else {
        const stored = await this.readStored();
        const endpoint = sub?.endpoint ?? (stored?.userId === userId ? stored.endpoint : null);
        if (endpoint)
          removed =
            Number(
              await this.call<number>('RETURN fn::push::unsubscribe($endpoint)', { endpoint })
            ) || 0;
      }
    }
    if (sub && !options.keepBrowserSubscription) await sub.unsubscribe().catch(() => false);
    await this.deps.persistence.remove(PUSH_ENDPOINT_KEY).catch(noop);
    this.refreshBridges();
    return removed;
  }

  /**
   * Reconcile after sign-in (runs by itself with `autoSync`). Needs granted
   * permission and a user who subscribed on this browser (or
   * `autoResubscribe`). Repairs a missing browser subscription, one made with
   * a rotated key, and a missing, disabled or stale server row. Never throws.
   */
  async sync(options: WebPushSyncOptions = {}): Promise<WebPushSyncStatus> {
    try {
      const support = this.support();
      if (!support.supported) return 'unsupported';
      const userId = this.userId();
      if (!userId) return 'signed-out';
      if (this.deps.auth.impersonation) return 'impersonating';
      if (support.permission === 'denied') return 'permission-denied';
      if (support.permission !== 'granted') return 'permission-default';
      const stored = await this.readStored();
      const mine = stored && stored.userId === userId ? stored : null;
      if (!mine && !(options.autoResubscribe ?? this.config.autoResubscribe ?? false))
        return 'not-subscribed';
      const info = await this.info({ refresh: true });
      if (!info.enabled || !info.publicKey) return 'disabled';
      const registration = await this.registration(options.registration, SYNC_READY_TIMEOUT_MS);
      if (!registration?.pushManager) return 'no-registration';

      const key = base64UrlToBytes(info.publicKey);
      const devices = await this.listRaw();
      let sub = await registration.pushManager.getSubscription();
      const previousEndpoint = sub?.endpoint ?? mine?.endpoint;
      const previous =
        devices.find((d) => d.endpoint === previousEndpoint) ??
        devices.find((d) => d.endpoint === mine?.endpoint);
      let status: WebPushSyncStatus = 'ok';
      if (sub) {
        const match = keyMatches(sub, key);
        const rotated =
          match === false || (match === null && !!mine?.kid && !!info.kid && mine.kid !== info.kid);
        const broken = previous?.endpoint === sub.endpoint && previous.disabled_at != null;
        if (rotated || broken) {
          await sub.unsubscribe().catch(() => false);
          sub = null;
        }
      }
      if (!sub) {
        sub = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: key as BufferSource,
        });
        status = 'resubscribed';
      }
      const row = devices.find((d) => d.endpoint === sub!.endpoint);
      if (status === 'resubscribed' || !row || row.current === false || row.disabled_at != null) {
        await this.register(sub, {
          label: previous?.label ?? undefined,
          rules: previous?.rules ?? undefined,
          meta: previous?.meta ?? undefined,
        });
        if (status === 'ok') status = 'registered';
        if (previous?.endpoint && previous.endpoint !== sub.endpoint) {
          await this.call('RETURN fn::push::unsubscribe($endpoint)', {
            endpoint: previous.endpoint,
          }).catch(noop);
        }
      }
      await this.writeStored({
        endpoint: sub.endpoint,
        userId,
        kid: info.kid,
        publicKey: info.publicKey,
        at: Date.now(),
      });
      this.refreshBridges();
      return status;
    } catch (error) {
      this.logger.warn({ error, Category: 'sp00ky-client::WebPush::sync' }, 'Web Push sync failed');
      return 'error';
    }
  }

  /** Change this device's label, rule filter or meta. */
  async update(options: WebPushUpdateOptions): Promise<WebPushDevice | null> {
    this.assertSignedIn();
    this.assertNotImpersonating();
    const endpoint = await this.thisEndpoint();
    if (!endpoint) throw new WebPushError('not-subscribed', 'this browser is not subscribed');
    const opts = dropUndefined({
      label: options.label,
      rules: options.rules === null ? [] : options.rules,
      meta: options.meta,
    });
    const rows = await this.call<DeviceRow[]>('RETURN fn::push::update($endpoint, $opts)', {
      endpoint,
      opts,
    });
    const row = Array.isArray(rows) ? rows[0] : null;
    return row ? toDevice(row, endpoint) : null;
  }

  // ── direct messages ──

  /** Push to yourself, now or at `sendAt` (reminders). Omit `notification` for a nudge. */
  async notify(message: WebPushMessageInput): Promise<WebPushMessage> {
    this.assertSignedIn();
    this.assertNotImpersonating();
    const sendAt =
      message.sendAt === undefined
        ? undefined
        : new Date(message.sendAt as string | number | Date).toISOString();
    const msg = dropUndefined({
      notification: message.notification,
      data: message.data,
      topic: message.topic,
      urgency: message.urgency,
      ttl: message.ttl,
      sendAt,
    });
    return toMessage(await this.call('RETURN fn::push::notify($msg)', { msg }));
  }

  /** Cancel a scheduled message. `true` when it was still pending. */
  async cancel(id: string | RecordId): Promise<boolean> {
    this.assertSignedIn();
    const rid =
      id instanceof RecordId
        ? id
        : new RecordId('_00_push_message', String(id).replace(/^_00_push_message:/, ''));
    return (await this.call<boolean>('RETURN fn::push::cancel($id)', { id: rid })) === true;
  }

  /** A visible test notification to every device of the user. */
  async test(options: { title?: string; body?: string } = {}): Promise<WebPushMessage> {
    this.assertSignedIn();
    this.assertNotImpersonating();
    return toMessage(
      await this.call('RETURN fn::push::test($opts)', { opts: dropUndefined({ ...options }) })
    );
  }

  // ── service worker bridge ──

  /**
   * Post the session to the service worker now and whenever the token
   * changes (`{ type: 'sp00ky:token', token, userId, endpoint, namespace,
   * database }`). Never posts an impersonation token. Returns a disposer.
   * With `webPush.bridge` (the default) this already runs while notification
   * permission is granted.
   */
  bridge(registration?: ServiceWorkerRegistration): () => void {
    return this.startBridge(registration, false);
  }

  /** Messages from the push service worker (`sp00ky:push` while visible, `sp00ky:navigate`, ...). */
  onMessage(cb: (message: WebPushClientMessage) => void): () => void {
    const sw = this.globals().navigator?.serviceWorker;
    if (!sw || typeof sw.addEventListener !== 'function') return noop;
    const handler = (event: MessageEvent) => {
      const data = event.data as { type?: unknown } | null;
      if (data && typeof data.type === 'string' && data.type.startsWith('sp00ky:'))
        cb(data as WebPushClientMessage);
    };
    sw.addEventListener('message', handler);
    return () => sw.removeEventListener('message', handler);
  }

  // ── internals ──

  private userId(): string | null {
    const auth = this.deps.auth;
    if (!auth.isAuthenticated || !auth.currentUser?.id) return null;
    return recordIdString(auth.currentUser.id);
  }

  private assertSignedIn(): void {
    if (!this.userId()) throw new WebPushError('signed-out', 'sign in first');
  }

  private assertNotImpersonating(): void {
    if (this.deps.auth.impersonation)
      throw new WebPushError(
        'impersonating',
        'push notifications are not available while impersonating'
      );
  }

  private async call<T>(sql: string, vars?: Record<string, unknown>): Promise<T> {
    try {
      const [result] = await this.deps.remote.query<[T]>(sql, vars);
      return result;
    } catch (error) {
      throw new WebPushError(
        'server',
        error instanceof Error ? error.message : String(error),
        error
      );
    }
  }

  private async listRaw(): Promise<DeviceRow[]> {
    const rows = await this.call<DeviceRow[]>('RETURN fn::push::list()');
    return Array.isArray(rows) ? rows : [];
  }

  private async register(
    sub: PushSubscription,
    options: { label?: string; rules?: string[]; meta?: Record<string, unknown> }
  ): Promise<DeviceRow> {
    const opts = dropUndefined({
      label: options.label,
      rules: options.rules && options.rules.length > 0 ? options.rules : undefined,
      meta: options.meta,
      userAgent: this.globals().navigator?.userAgent,
    });
    const row = await this.call<DeviceRow | null>('RETURN fn::push::subscribe($sub, $opts)', {
      sub: sub.toJSON(),
      opts,
    });
    return row ?? { endpoint: sub.endpoint };
  }

  private async readStored(): Promise<StoredEndpoint | null> {
    try {
      const value = await this.deps.persistence.get<StoredEndpoint>(PUSH_ENDPOINT_KEY);
      return value && typeof value.endpoint === 'string' && typeof value.userId === 'string'
        ? value
        : null;
    } catch {
      return null;
    }
  }

  private async writeStored(value: StoredEndpoint): Promise<void> {
    await this.deps.persistence
      .set(PUSH_ENDPOINT_KEY, dropUndefined({ ...value }))
      .catch((error) => {
        this.logger.debug(
          { error, Category: 'sp00ky-client::WebPush' },
          'Could not persist the push endpoint'
        );
      });
  }

  /** The page's registration: given, existing, registered from config, or `ready` within `waitMs`. */
  private async registration(
    given: ServiceWorkerRegistration | undefined,
    waitMs: number
  ): Promise<ServiceWorkerRegistration | null> {
    if (given) return given;
    const sw = this.globals().navigator?.serviceWorker;
    if (!sw) return null;
    let reg = (await sw.getRegistration().catch(() => undefined)) ?? null;
    if (!reg && this.config.serviceWorkerUrl)
      reg = await sw.register(this.config.serviceWorkerUrl).catch(() => null);
    if (reg?.active) return reg;
    const ready = await withTimeout(sw.ready, waitMs, 'service worker not ready').catch(() => null);
    return ready ?? reg;
  }

  /** This browser's subscription, without waiting for a worker. */
  private async currentSubscription(): Promise<PushSubscription | null> {
    try {
      const sw = this.globals().navigator?.serviceWorker;
      const reg = sw ? await sw.getRegistration() : undefined;
      return (await reg?.pushManager?.getSubscription()) ?? null;
    } catch {
      return null;
    }
  }

  private async thisEndpoint(): Promise<string | null> {
    const sub = this.support().supported ? await this.currentSubscription() : null;
    if (sub) return sub.endpoint;
    const stored = await this.readStored();
    return stored && stored.userId === this.userId() ? stored.endpoint : null;
  }

  private async postToWorker(
    message: unknown,
    registration?: ServiceWorkerRegistration,
    wait = false
  ): Promise<void> {
    const sw = this.globals().navigator?.serviceWorker;
    if (!sw) return;
    let reg: ServiceWorkerRegistration | undefined | null = registration;
    if (!reg) reg = wait ? await sw.ready : await sw.getRegistration().catch(() => undefined);
    const target = reg?.active ?? sw.controller;
    target?.postMessage(message);
  }

  private startBridge(
    registration: ServiceWorkerRegistration | undefined,
    requirePermission: boolean
  ): () => void {
    const g = this.globals();
    if (!g.navigator?.serviceWorker || !this.deps.database.endpoint) return noop;
    let last: string | null = null;
    let disposed = false;
    const post = () => {
      if (disposed) return;
      const auth = this.deps.auth;
      const token = auth.token;
      const userId = this.userId();
      if (!token || !userId || auth.impersonation) return;
      if (requirePermission && this.support().permission !== 'granted') return;
      if (token === last) return;
      last = token;
      void this.tokenMessage(token, userId)
        .then((message) => this.postToWorker(message, registration, true))
        .catch((error) => {
          last = null;
          this.logger.debug(
            { error, Category: 'sp00ky-client::WebPush::bridge' },
            'Could not post the session to the service worker'
          );
        });
    };
    const entry = {
      reset: () => {
        last = null;
        post();
      },
    };
    this.bridges.add(entry);
    const off = this.deps.auth.subscribe(() => post());
    return () => {
      disposed = true;
      off();
      this.bridges.delete(entry);
    };
  }

  private refreshBridges(): void {
    for (const b of this.bridges) b.reset();
  }

  private async tokenMessage(token: string, userId: string): Promise<BridgeTokenMessage> {
    const stored = await this.readStored();
    const { endpoint, namespace, database } = this.deps.database;
    return dropUndefined({
      type: 'sp00ky:token' as const,
      token,
      userId,
      endpoint: endpoint ?? '',
      namespace,
      database,
      publicKey: this.lastInfo?.publicKey ?? stored?.publicKey,
      pushEndpoint: stored?.userId === userId ? stored.endpoint : undefined,
    });
  }

  /** Sign-out hook: tell the worker, and drop this device's row for the user. Bounded. */
  private async beforeSignOut(ctx: SignOutContext): Promise<void> {
    const tasks: Promise<unknown>[] = [this.postToWorker({ type: 'sp00ky:signout' }).catch(noop)];
    if (this.config.unsubscribeOnSignOut !== false && !ctx.impersonating) {
      const userId = this.userId();
      const stored = await this.readStored();
      if (userId && stored && stored.userId === userId) {
        tasks.push(
          withTimeout(
            this.deps.remote.query('RETURN fn::push::unsubscribe($endpoint)', {
              endpoint: stored.endpoint,
            }),
            SIGN_OUT_UNSUBSCRIBE_TIMEOUT_MS,
            'push unsubscribe timed out'
          )
            .catch((error) =>
              this.logger.debug(
                { error, Category: 'sp00ky-client::WebPush::signOut' },
                'Could not remove the push subscription'
              )
            )
            .finally(() => this.deps.persistence.remove(PUSH_ENDPOINT_KEY).catch(noop))
        );
      }
    }
    this.syncedFor = null;
    await Promise.allSettled(tasks);
  }
}
