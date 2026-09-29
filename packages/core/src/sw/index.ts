/**
 * `@spooky-sync/core/sw`: Web Push for the app's service worker.
 *
 * ```js
 * // sw.js
 * import { installPushHandlers } from '@spooky-sync/core/sw';
 * installPushHandlers({ placeholder: { title: 'My app', body: 'Something new' } });
 * ```
 *
 * Shows content pushes as sent, hands content-free nudges to the app (or
 * renders them from live data), suppresses notifications while a page of the
 * app is visible, routes clicks, and keeps the session the page bridges over
 * (`db.webPush.bridge()`) in IndexedDB.
 *
 * No wasm, no `surrealdb`, no `window`: it only imports the live feed's TYPES.
 * Pass a feed factory (`createLiveFeed` from `@spooky-sync/core/live`) when you
 * want live rendering.
 */
import type { LiveFeed, LiveQuery } from '../live/index';
import {
  base64UrlToBytes,
  isPushPayload,
  type BridgeTokenMessage,
  type PushNotification,
  type PushPayload,
  type WebPushClientMessage,
} from '../push/types';
import { decodeTokenClaims } from '../modules/auth/impersonation';
import { defaultStore, type KvStore } from './store';

export {
  idbStore,
  memoryStore,
  defaultStore,
  PUSH_DB_NAME,
  PUSH_STORE_NAME,
  type KvStore,
} from './store';
export * from '../push/types';

// ── structural service worker types (no WebWorker lib needed) ─────────────

export interface SwNotification {
  title: string;
  tag: string;
  body?: string;
  data?: any;
  close(): void;
}

export interface SwPushSubscription {
  endpoint: string;
  options?: { applicationServerKey?: ArrayBuffer | null };
  toJSON(): unknown;
}

export interface SwRegistration {
  scope: string;
  showNotification(title: string, options?: Record<string, unknown>): Promise<void>;
  getNotifications(filter?: { tag?: string }): Promise<SwNotification[]>;
  pushManager?: {
    getSubscription(): Promise<SwPushSubscription | null>;
    subscribe(options: {
      userVisibleOnly: boolean;
      applicationServerKey: unknown;
    }): Promise<SwPushSubscription>;
  };
}

export interface SwWindowClient {
  url: string;
  visibilityState?: string;
  focused?: boolean;
  focus(): Promise<unknown>;
  navigate?(url: string): Promise<unknown>;
  postMessage(message: unknown): void;
}

export interface SwClients {
  matchAll(options?: { type?: string; includeUncontrolled?: boolean }): Promise<SwWindowClient[]>;
  openWindow(url: string): Promise<unknown>;
}

export interface SwScope {
  registration: SwRegistration;
  clients: SwClients;
  location: { origin: string };
  navigator?: { userAgent?: string };
  addEventListener(type: string, listener: (event: any) => void): void;
  removeEventListener(type: string, listener: (event: any) => void): void;
}

export interface SwExtendableEvent {
  waitUntil(promise: Promise<unknown>): void;
}

export interface SwPushEvent extends SwExtendableEvent {
  data: { json(): unknown; text(): string } | null;
}

export interface SwNotificationEvent extends SwExtendableEvent {
  notification: SwNotification;
  action: string;
}

export interface SwPushSubscriptionChangeEvent extends SwExtendableEvent {
  oldSubscription?: SwPushSubscription | null;
  newSubscription?: SwPushSubscription | null;
}

// ── public types ──────────────────────────────────────────────────────────

/** What `showNotification` gets: a title plus any notification option. */
export interface NotificationSpec extends PushNotification {
  title: string;
}

/** The session a page bridged over (`db.webPush.bridge()`). Fits `createLiveFeed` as is. */
export interface BridgedToken {
  token: string;
  userId: string;
  endpoint: string;
  namespace: string;
  database: string;
  publicKey?: string;
  pushEndpoint?: string;
  /** Epoch ms it was stored. */
  at: number;
}

export interface PushHandlerContext {
  event: SwPushEvent;
  registration: SwRegistration;
  /** Show a notification the way the bridge would (tag, url and payload in `data`). */
  show(spec: NotificationSpec): Promise<void>;
  showPlaceholder(): Promise<void>;
  /** Visible windows of this app, right now. */
  visibleClients(): Promise<SwWindowClient[]>;
  /** The bridged session, or null (signed out, never bridged, expired). */
  bridged(): Promise<BridgedToken | null>;
}

export interface LiveRenderOptions<Row = Record<string, any>> {
  /** Open a feed for the bridged session: `(t) => createLiveFeed(t)`. */
  feed(bridged: BridgedToken): LiveFeed | Promise<LiveFeed>;
  /** What to read for this user (a built query, or `{ surql, params }`). */
  query(userId: string, payload: PushPayload): LiveQuery;
  /** Should this row be on screen as a notification? */
  visible(row: Row): boolean;
  /** The notification for one row; `tag` identifies it (one notification per row). */
  render(row: Row): NotificationSpec & { tag: string };
  /** Budget for loading the rows, ms. Default 8000 (the push event lives ~30 s). */
  timeoutMs?: number;
  /** Quiet period `idle()` waits for, ms. Default 300. */
  quietMs?: number;
  /**
   * A push whose only effect was closing notifications (their rows stopped
   * being visible, e.g. read on another device) shows nothing. Default true.
   * `false` shows the placeholder instead, for origins that hit Chrome's
   * silent-push allowance.
   */
  silentClose?: boolean;
}

export interface InstallPushHandlersOptions<Row = Record<string, any>> {
  /** Turn a notification url into the one to open. Default: resolved against the worker's scope. */
  resolveUrl?(url: string, payload: PushPayload | undefined): string;
  /** Sees every valid push first. Return `true` when you handled it (and showed something). */
  onPush?(payload: PushPayload, ctx: PushHandlerContext): Promise<boolean | void> | boolean | void;
  /** Handles content-free nudges (you must show a notification). */
  onNudge?(payload: PushPayload, ctx: PushHandlerContext): Promise<void> | void;
  /**
   * Custom rendering. For a content push, replaces the default mapping of
   * `payload.notification`; for a nudge, a returned spec is shown instead of
   * the live renderer. `null`/`undefined` keeps the default.
   */
  render?(payload: PushPayload): NotificationSpec | null | undefined;
  /** Post `{ type: 'sp00ky:push', payload }` to visible windows instead of showing. Default true. */
  suppressWhenVisible?: boolean;
  /** Shown when a push cannot be rendered (browsers require a notification per push). */
  placeholder?: { title: string; body?: string; icon?: string; [key: string]: unknown };
  /** Return `true` when you handled the click yourself. */
  onClick?(
    event: SwNotificationEvent,
    payload: PushPayload | undefined,
    action: string | undefined
  ): Promise<boolean | void> | boolean | void;
  onClose?(event: SwNotificationEvent, payload: PushPayload | undefined): Promise<void> | void;
  /**
   * How a click reaches an already open window: `navigate` (default) loads the
   * url in it, `message` posts `{ type: 'sp00ky:navigate', url }` for in-app
   * routing (`db.webPush.onMessage`).
   */
  clickMode?: 'navigate' | 'message';
  /** Render nudges from live data. */
  live?: LiveRenderOptions<Row>;
  /** Feed factory for `pushsubscriptionchange` when `live` is not configured. */
  feed?(bridged: BridgedToken): LiveFeed | Promise<LiveFeed>;
  /** Re-subscribe on `pushsubscriptionchange`. Default true. */
  resubscribe?: boolean;
  /** Defaults to `self`. */
  scope?: SwScope;
  /** Defaults to IndexedDB `sp00ky-push` / `kv`. */
  store?: KvStore;
}

export const DEFAULT_PLACEHOLDER = {
  title: 'New notification',
  body: 'Open the app to see what is new.',
};
const PLACEHOLDER_TAG = 'sp00ky-placeholder';
const TOKEN_KEY = 'token';

/** The session the page bridged, or null (none, signed out, or its JWT expired). */
export async function getBridgedToken(
  store: KvStore = defaultStore()
): Promise<BridgedToken | null> {
  let value: BridgedToken | undefined;
  try {
    value = await store.get<BridgedToken>(TOKEN_KEY);
  } catch {
    return null;
  }
  if (!value || typeof value.token !== 'string') return null;
  const { exp } = decodeTokenClaims(value.token);
  if (exp !== null && exp * 1000 <= Date.now()) return null;
  return value;
}

function isTokenMessage(data: unknown): data is BridgeTokenMessage {
  const m = data as Partial<BridgeTokenMessage> | null;
  return (
    !!m &&
    m.type === 'sp00ky:token' &&
    typeof m.token === 'string' &&
    typeof m.userId === 'string' &&
    typeof m.endpoint === 'string' &&
    typeof m.namespace === 'string' &&
    typeof m.database === 'string'
  );
}

function dropUndefined(obj: Record<string, unknown>): Record<string, unknown> {
  for (const key of Object.keys(obj)) if (obj[key] === undefined) delete obj[key];
  return obj;
}

/**
 * `showNotification(title, options)` arguments for a spec: every key passes
 * through except `title` and `url`; `tag` defaults to the topic; `data` gets
 * `url`, the payload and per-action urls next to the spec's own data.
 */
export function notificationOptions(
  spec: NotificationSpec,
  payload?: PushPayload,
  extraData?: Record<string, unknown>
): [string, Record<string, unknown>] {
  const { title, url, data, tag, actions, renotify, ...rest } = spec;
  const actionUrls: Record<string, string> = {};
  for (const a of actions ?? []) if (a && typeof a.url === 'string') actionUrls[a.action] = a.url;
  const finalTag = tag ?? payload?.topic;
  const options: Record<string, unknown> = {
    ...rest,
    tag: finalTag,
    actions: actions?.map(({ url: _url, ...a }) => a),
    // `renotify` without a tag is a TypeError in Chrome.
    renotify: finalTag ? renotify : undefined,
    data: dropUndefined({
      ...data,
      url,
      payload,
      actionUrls: Object.keys(actionUrls).length > 0 ? actionUrls : undefined,
      ...extraData,
    }),
  };
  return [title, dropUndefined(options)];
}

/** Wire the push, click, close, message and subscription-change handlers. Returns an uninstall. */
export function installPushHandlers<Row = Record<string, any>>(
  options: InstallPushHandlersOptions<Row> = {}
): () => void {
  const scope = options.scope ?? (globalThis as unknown as SwScope);
  const store = options.store ?? defaultStore();
  const suppress = options.suppressWhenVisible !== false;
  const placeholder = options.placeholder ?? DEFAULT_PLACEHOLDER;

  const resolveUrl = (url: string, payload: PushPayload | undefined): string => {
    if (options.resolveUrl) return options.resolveUrl(url, payload);
    try {
      return new URL(url, scope.registration?.scope || scope.location.origin).href;
    } catch {
      return url;
    }
  };

  const windows = async (): Promise<SwWindowClient[]> => {
    const all = await scope.clients.matchAll({ type: 'window', includeUncontrolled: true });
    return all.filter((c) => {
      try {
        return new URL(c.url).origin === scope.location.origin;
      } catch {
        return false;
      }
    });
  };
  const visibleClients = async () =>
    (await windows()).filter((c) => c.visibilityState === 'visible');

  const show = async (
    spec: NotificationSpec,
    payload?: PushPayload,
    extraData?: Record<string, unknown>
  ) => {
    const [title, opts] = notificationOptions(spec, payload, extraData);
    await scope.registration.showNotification(title, opts);
  };
  const showPlaceholder = () => show({ ...placeholder, tag: PLACEHOLDER_TAG, url: '/' });

  const ctxFor = (event: SwPushEvent, flag: { shown: boolean }): PushHandlerContext => ({
    event,
    registration: scope.registration,
    show: async (spec) => {
      flag.shown = true;
      await show(spec);
    },
    showPlaceholder: async () => {
      flag.shown = true;
      await showPlaceholder();
    },
    visibleClients,
    bridged: () => getBridgedToken(store),
  });

  const renderLive = async (live: LiveRenderOptions<Row>, payload: PushPayload): Promise<void> => {
    const bridged = await getBridgedToken(store);
    if (!bridged) throw new Error('sp00ky push: no bridged session for live rendering');
    const budget = live.timeoutMs ?? 8_000;
    const started = Date.now();
    const feed = await live.feed(bridged);
    let rows: Row[] = [];
    try {
      const sub = feed.subscribe<Row>(live.query(bridged.userId, payload), {
        onSet: (r) => void (rows = r),
      });
      let timer: ReturnType<typeof setTimeout> | undefined;
      const deadline = new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('sp00ky push: live rows did not load in time')),
          budget
        );
      });
      try {
        rows = await Promise.race([sub.ready, deadline]);
      } finally {
        clearTimeout(timer);
      }
      await feed.idle({
        timeoutMs: Math.max(0, budget - (Date.now() - started)),
        quietMs: live.quietMs,
      });
      rows = sub.rows();
    } finally {
      await feed.close().catch(() => undefined);
    }
    const specs = rows.filter((row) => live.visible(row)).map((row) => live.render(row));
    const wanted = new Set(specs.map((s) => s.tag));
    const shown = await scope.registration.getNotifications();
    let closed = 0;
    for (const n of shown) {
      if (n.data?.sp00kyLive && !wanted.has(n.tag)) {
        n.close();
        closed += 1;
      }
    }
    for (const spec of specs) {
      const existing = shown.find((n) => n.tag === spec.tag && n.data?.sp00kyLive);
      if (existing && existing.title === spec.title && (existing.body ?? '') === (spec.body ?? ''))
        continue;
      await show(spec, undefined, { sp00kyLive: true });
    }
    // A push must leave something on screen (Chrome shows its own "updated in
    // the background" notice otherwise). Closing a notification because its
    // row was read elsewhere is the point of the push, so by default that
    // case stays silent: Chrome tolerates occasional silent pushes, and a
    // placeholder there would replace "read on the phone" with "something new".
    const after = await scope.registration.getNotifications();
    if (after.length === 0 && (closed === 0 || live.silentClose === false)) await showPlaceholder();
  };

  const handlePush = async (event: SwPushEvent): Promise<void> => {
    let payload: unknown = null;
    try {
      payload = event.data ? event.data.json() : null;
    } catch {
      payload = null;
    }
    if (!isPushPayload(payload)) {
      await showPlaceholder();
      return;
    }
    const flag = { shown: false };
    const ctx = ctxFor(event, flag);
    try {
      if (options.onPush && (await options.onPush(payload, ctx)) === true) return;
    } catch {
      if (!flag.shown) await showPlaceholder();
      return;
    }
    if (suppress) {
      const visible = await visibleClients().catch(() => [] as SwWindowClient[]);
      if (visible.length > 0) {
        const message: WebPushClientMessage = { type: 'sp00ky:push', payload };
        for (const client of visible) client.postMessage(message);
        return;
      }
    }
    if (payload.notification) {
      const custom = options.render?.(payload);
      const spec: NotificationSpec = custom ?? {
        ...payload.notification,
        title: payload.notification.title ?? placeholder.title,
      };
      await show(spec, payload);
      return;
    }
    try {
      if (options.onNudge) {
        await options.onNudge(payload, ctx);
        return;
      }
      const custom = options.render?.(payload);
      if (custom) {
        await show(custom, payload);
        return;
      }
      if (options.live) {
        await renderLive(options.live, payload);
        return;
      }
    } catch {
      if (!flag.shown) await showPlaceholder();
      return;
    }
    await showPlaceholder();
  };

  const handleClick = async (event: SwNotificationEvent): Promise<void> => {
    const notification = event.notification;
    const data = (notification.data ?? {}) as {
      url?: string;
      payload?: PushPayload;
      actionUrls?: Record<string, string>;
    };
    const payload = isPushPayload(data.payload) ? data.payload : undefined;
    const action = event.action || undefined;
    if (options.onClick && (await options.onClick(event, payload, action)) === true) return;
    notification.close();
    const raw = (action && data.actionUrls?.[action]) || data.url || '/';
    const url = resolveUrl(raw, payload);
    const open = await windows();
    const target =
      open.find((c) => c.focused) ?? open.find((c) => c.visibilityState === 'visible') ?? open[0];
    if (!target) {
      await scope.clients.openWindow(url);
      return;
    }
    await target.focus().catch(() => undefined);
    const message: WebPushClientMessage = { type: 'sp00ky:navigate', url, payload, action };
    if (options.clickMode === 'message') {
      target.postMessage(message);
      return;
    }
    if (target.url === url) return;
    if (target.navigate) {
      try {
        await target.navigate(url);
        return;
      } catch {
        /* an uncontrolled window cannot be navigated from here; tell it instead */
      }
    }
    target.postMessage(message);
  };

  const handleClose = async (event: SwNotificationEvent): Promise<void> => {
    const data = (event.notification.data ?? {}) as { payload?: PushPayload };
    await options.onClose?.(event, isPushPayload(data.payload) ? data.payload : undefined);
  };

  const signOut = async (): Promise<void> => {
    await store.delete(TOKEN_KEY).catch(() => undefined);
    const shown = await scope.registration.getNotifications().catch(() => [] as SwNotification[]);
    for (const n of shown) n.close();
  };

  const handleMessage = (event: {
    data?: unknown;
    origin?: string;
    waitUntil?: (p: Promise<unknown>) => void;
  }) => {
    const data = event.data as { type?: unknown } | null | undefined;
    if (!data || typeof data !== 'object') return;
    if (event.origin && event.origin !== scope.location.origin) return;
    let work: Promise<unknown> | null = null;
    if (isTokenMessage(data)) {
      const value: BridgedToken = {
        token: data.token,
        userId: data.userId,
        endpoint: data.endpoint,
        namespace: data.namespace,
        database: data.database,
        publicKey: typeof data.publicKey === 'string' ? data.publicKey : undefined,
        pushEndpoint: typeof data.pushEndpoint === 'string' ? data.pushEndpoint : undefined,
        at: Date.now(),
      };
      work = store.set(TOKEN_KEY, dropUndefined({ ...value })).catch(() => undefined);
    } else if (data.type === 'sp00ky:signout') {
      work = signOut();
    }
    if (work) event.waitUntil?.(work);
  };

  const handleSubscriptionChange = async (event: SwPushSubscriptionChangeEvent): Promise<void> => {
    if (options.resubscribe === false) return;
    const bridged = await getBridgedToken(store);
    let sub = event.newSubscription ?? null;
    if (!sub) {
      const key =
        event.oldSubscription?.options?.applicationServerKey ??
        (bridged?.publicKey ? base64UrlToBytes(bridged.publicKey) : null);
      const manager = scope.registration.pushManager;
      if (key && manager) {
        try {
          sub = await manager.subscribe({ userVisibleOnly: true, applicationServerKey: key });
        } catch {
          sub = null;
        }
      }
    }
    const factory = options.live?.feed ?? options.feed;
    if (sub && bridged && factory) {
      let feed: LiveFeed | null = null;
      try {
        feed = await factory(bridged);
        const oldEndpoint = event.oldSubscription?.endpoint ?? bridged.pushEndpoint;
        // Carry the device's label and rule filter over to the new endpoint.
        const [devices] =
          await feed.query<
            [Array<{ endpoint?: string; label?: unknown; rules?: unknown; meta?: unknown }>]
          >('RETURN fn::push::list()');
        const old = Array.isArray(devices)
          ? devices.find((d) => d.endpoint === oldEndpoint)
          : undefined;
        await feed.query('RETURN fn::push::subscribe($sub, $opts)', {
          sub: sub.toJSON(),
          opts: dropUndefined({
            label: old?.label ?? undefined,
            rules: old?.rules ?? undefined,
            meta: old?.meta ?? undefined,
            userAgent: scope.navigator?.userAgent,
          }),
        });
        if (oldEndpoint && oldEndpoint !== sub.endpoint) {
          await feed.query('RETURN fn::push::unsubscribe($endpoint)', { endpoint: oldEndpoint });
        }
        await store.set(TOKEN_KEY, { ...bridged, pushEndpoint: sub.endpoint });
      } catch {
        /* the page's next sync() registers the new endpoint */
      } finally {
        await feed?.close().catch(() => undefined);
      }
    }
    const message: WebPushClientMessage = {
      type: 'sp00ky:subscriptionchange',
      endpoint: sub?.endpoint,
    };
    for (const client of await windows().catch(() => [] as SwWindowClient[]))
      client.postMessage(message);
  };

  const listeners: Array<[string, (event: any) => void]> = [
    [
      'push',
      (event: SwPushEvent) =>
        event.waitUntil(handlePush(event).catch(() => showPlaceholder().catch(() => undefined))),
    ],
    [
      'notificationclick',
      (event: SwNotificationEvent) => event.waitUntil(handleClick(event).catch(() => undefined)),
    ],
    [
      'notificationclose',
      (event: SwNotificationEvent) => event.waitUntil?.(handleClose(event).catch(() => undefined)),
    ],
    ['message', handleMessage],
    [
      'pushsubscriptionchange',
      (event: SwPushSubscriptionChangeEvent) =>
        event.waitUntil(handleSubscriptionChange(event).catch(() => undefined)),
    ],
  ];
  for (const [type, listener] of listeners) scope.addEventListener(type, listener);
  return () => {
    for (const [type, listener] of listeners) scope.removeEventListener(type, listener);
  };
}
