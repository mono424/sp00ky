import { afterEach, describe, expect, it, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import {
  WebPushModule,
  WebPushError,
  detectWebPushSupport,
  recordIdString,
  PUSH_ENDPOINT_KEY,
  type WebPushGlobals,
} from './index';
import { AuthService } from '../auth/index';
import { base64UrlToBytes, bytesToBase64Url } from '../../push/types';

const noop = () => {};
const logger: any = {
  debug: noop,
  info: noop,
  warn: noop,
  error: noop,
  trace: noop,
  child: () => logger,
};
const KEY = bytesToBase64Url(new Uint8Array([4, 10, 20, 30, 40]));
const NEW_KEY = bytesToBase64Url(new Uint8Array([4, 99, 98, 97, 96]));
const flush = async (n = 8) => {
  for (let i = 0; i < n; i++) await Promise.resolve();
};

interface Row {
  id: RecordId;
  endpoint: string;
  kid: string;
  label?: string;
  rules?: string[];
  user_agent?: string;
  created_at?: Date;
  failures?: number;
  current?: boolean;
  disabled_at?: Date;
  kind?: string;
  platform?: string;
  app_id?: string;
  environment?: string;
}

function harness(
  opts: {
    permission?: NotificationPermission;
    config?: Record<string, unknown>;
    globals?: Partial<WebPushGlobals>;
  } = {}
) {
  const order: string[] = [];
  const server = {
    info: { enabled: true, publicKey: KEY, kid: 'kid1' } as Record<string, unknown>,
    rows: [] as Row[],
    fail: false,
  };
  const remote = {
    query: vi.fn(async (sql: string, vars?: Record<string, any>) => {
      order.push(sql);
      if (server.fail) throw new Error('socket gone');
      switch (sql) {
        case 'RETURN fn::push::info()':
          return [server.info];
        case 'RETURN fn::push::subscribe($sub, $opts)': {
          const row: Row = {
            id: new RecordId('_00_push_subscription', `s${server.rows.length + 1}`),
            endpoint: vars!.sub.endpoint,
            kid: String(server.info.kid),
            label: vars!.opts.label,
            rules: vars!.opts.rules,
            user_agent: vars!.opts.userAgent,
            created_at: new Date(1_700_000_000_000),
            current: true,
          };
          server.rows = [...server.rows.filter((r) => r.endpoint !== row.endpoint), row];
          return [row];
        }
        case 'RETURN fn::push::list()':
          return [server.rows];
        case 'RETURN fn::push::unsubscribe($endpoint)': {
          const before = server.rows.length;
          server.rows = server.rows.filter((r) => r.endpoint !== vars!.endpoint);
          return [before - server.rows.length];
        }
        case 'RETURN fn::push::unsubscribe(NONE)': {
          const n = server.rows.length;
          server.rows = [];
          return [n];
        }
        case 'RETURN fn::push::update($endpoint, $opts)':
          return [
            server.rows
              .filter((r) => r.endpoint === vars!.endpoint)
              .map((r) => Object.assign(r, { label: vars!.opts.label ?? r.label })),
          ];
        case 'RETURN fn::push::notify($msg)':
          return [
            {
              id: new RecordId('_00_push_message', 'm1'),
              status: 'pending',
              send_at: vars!.msg.sendAt ? new Date(vars!.msg.sendAt) : undefined,
              created_at: new Date(1),
            },
          ];
        case 'RETURN fn::push::test($opts)':
          return [{ id: new RecordId('_00_push_message', 't1'), status: 'pending' }];
        case 'RETURN fn::push::cancel($id)':
          return [true];
        default:
          throw new Error(`unscripted ${sql}`);
      }
    }),
  };
  const persisted = new Map<string, unknown>();
  const persistence = {
    get: async <T>(k: string) => (persisted.get(k) as T) ?? null,
    set: async (k: string, v: unknown) => void persisted.set(k, v),
    remove: async (k: string) => void persisted.delete(k),
  };
  const authCbs: Array<(u: unknown) => void> = [];
  const hooks: Array<(ctx: any) => Promise<void> | void> = [];
  const auth = {
    token: 'tok-1' as string | null,
    currentUser: { id: new RecordId('user', 'alice') } as { id?: unknown } | null,
    isAuthenticated: true,
    impersonation: null as unknown,
    subscribe: (cb: (u: unknown) => void) => {
      authCbs.push(cb);
      cb(auth.currentUser?.id ?? null);
      return () => void authCbs.splice(authCbs.indexOf(cb), 1);
    },
    onBeforeSignOut: (h: (ctx: any) => Promise<void> | void) => {
      hooks.push(h);
      return () => void hooks.splice(hooks.indexOf(h), 1);
    },
    emit: () => authCbs.forEach((cb) => cb(auth.currentUser?.id ?? null)),
  };
  let n = 0;
  const browser = { sub: null as any };
  const makeSub = (endpoint: string, key: Uint8Array | null) => ({
    endpoint,
    options: { applicationServerKey: key ? key.slice().buffer : null },
    toJSON: () => ({ endpoint, expirationTime: null, keys: { p256dh: 'p', auth: 'a' } }),
    unsubscribe: vi.fn(async () => {
      order.push(`unsubscribe ${endpoint}`);
      if (browser.sub?.endpoint === endpoint) browser.sub = null;
      return true;
    }),
  });
  const pushManager = {
    getSubscription: vi.fn(async () => browser.sub),
    subscribe: vi.fn(async (o: { userVisibleOnly: boolean; applicationServerKey: Uint8Array }) => {
      order.push('pushManager.subscribe');
      browser.sub = makeSub(`https://push.example/${++n}`, new Uint8Array(o.applicationServerKey));
      return browser.sub;
    }),
  };
  const active = { postMessage: vi.fn() };
  const registration = { active, pushManager };
  const messageListeners: Array<(e: { data: unknown }) => void> = [];
  const serviceWorker = {
    getRegistration: vi.fn(async () => registration),
    ready: Promise.resolve(registration),
    register: vi.fn(),
    controller: null,
    addEventListener: vi.fn((_t: string, l: (e: { data: unknown }) => void) =>
      messageListeners.push(l)
    ),
    removeEventListener: vi.fn(
      (_t: string, l: (e: { data: unknown }) => void) =>
        void messageListeners.splice(messageListeners.indexOf(l), 1)
    ),
  };
  const Notification = {
    permission: opts.permission ?? ('default' as NotificationPermission),
    requestPermission: vi.fn(async () => {
      order.push('requestPermission');
      Notification.permission = 'granted';
      return 'granted' as NotificationPermission;
    }),
  };
  const globals: WebPushGlobals = {
    window: {},
    navigator: {
      userAgent: 'Mozilla/5.0 (X11; Linux x86_64) Chrome/130',
      platform: 'Linux x86_64',
      maxTouchPoints: 0,
      serviceWorker,
    } as any,
    Notification,
    isSecureContext: true,
    PushManager: function PushManager() {},
    ...opts.globals,
  };
  const connection = {
    cbs: [] as Array<(s: string) => void>,
    subscribe(cb: (s: string) => void) {
      this.cbs.push(cb);
      cb('connecting');
      return noop;
    },
  };
  const mod = new WebPushModule({
    remote: remote as any,
    auth,
    persistence,
    logger,
    database: { endpoint: 'wss://db.example/rpc', namespace: 'ns', database: 'db' },
    connection,
    config: opts.config as never,
    globals: () => globals,
  });
  return {
    mod,
    remote,
    server,
    persisted,
    auth,
    hooks,
    browser,
    makeSub,
    pushManager,
    active,
    serviceWorker,
    Notification,
    globals,
    order,
    connection,
    messageListeners,
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('detectWebPushSupport', () => {
  const base = (): WebPushGlobals => ({
    window: {},
    navigator: { userAgent: 'Chrome', serviceWorker: {} } as any,
    Notification: {
      permission: 'granted',
      requestPermission: async () => 'granted' as NotificationPermission,
    },
    isSecureContext: true,
    PushManager: {},
  });

  it('names what is missing', () => {
    expect(detectWebPushSupport({})).toEqual({
      supported: false,
      permission: 'unsupported',
      reason: 'no-window',
    });
    expect(detectWebPushSupport({ ...base(), isSecureContext: false }).reason).toBe(
      'insecure-context'
    );
    expect(detectWebPushSupport({ ...base(), navigator: { userAgent: 'x' } as any }).reason).toBe(
      'no-service-worker'
    );
    expect(detectWebPushSupport({ ...base(), PushManager: undefined }).reason).toBe(
      'no-push-manager'
    );
    expect(detectWebPushSupport({ ...base(), Notification: undefined })).toEqual({
      supported: false,
      permission: 'unsupported',
      reason: 'no-notification',
    });
    expect(detectWebPushSupport(base())).toEqual({ supported: true, permission: 'granted' });
  });

  it('iOS and iPadOS Safari need the home-screen app', () => {
    const iphone = {
      userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)',
      serviceWorker: {},
    } as any;
    expect(
      detectWebPushSupport({
        ...base(),
        navigator: iphone,
        PushManager: undefined,
        Notification: undefined,
      }).reason
    ).toBe('ios-not-installed');
    const ipad = {
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
      platform: 'MacIntel',
      maxTouchPoints: 5,
      serviceWorker: {},
    } as any;
    expect(detectWebPushSupport({ ...base(), navigator: ipad }).reason).toBe('ios-not-installed');
    expect(
      detectWebPushSupport({ ...base(), navigator: { ...iphone, standalone: true } }).supported
    ).toBe(true);
    expect(
      detectWebPushSupport({ ...base(), navigator: iphone, matchMedia: () => ({ matches: true }) })
        .supported
    ).toBe(true);
  });

  it('is unsupported in Node without throwing', () => {
    const h = harness({ globals: { window: undefined } });
    expect(h.mod.support()).toEqual({
      supported: false,
      permission: 'unsupported',
      reason: 'no-window',
    });
    const real = new WebPushModule({
      remote: {} as any,
      auth: {} as any,
      persistence: {} as any,
      logger,
      database: { namespace: 'n', database: 'd' },
    });
    expect(real.support().supported).toBe(false);
  });
});

describe('WebPushModule.subscribe', () => {
  it('asks for permission first, subscribes with the deployment key, registers and persists the device', async () => {
    const h = harness();
    const device = await h.mod.subscribe({ label: 'Laptop', rules: ['dm'], meta: { app: '1.2' } });
    expect(h.order[0]).toBe('requestPermission');
    expect(h.order).toEqual([
      'requestPermission',
      'RETURN fn::push::info()',
      'pushManager.subscribe',
      'RETURN fn::push::subscribe($sub, $opts)',
    ]);
    const call = h.pushManager.subscribe.mock.calls[0][0];
    expect(call.userVisibleOnly).toBe(true);
    expect(new Uint8Array(call.applicationServerKey)).toEqual(base64UrlToBytes(KEY));
    const vars = h.remote.query.mock.calls.find(
      (c) => c[0] === 'RETURN fn::push::subscribe($sub, $opts)'
    )![1]!;
    expect(vars).toEqual({
      sub: {
        endpoint: 'https://push.example/1',
        expirationTime: null,
        keys: { p256dh: 'p', auth: 'a' },
      },
      opts: {
        label: 'Laptop',
        rules: ['dm'],
        meta: { app: '1.2' },
        userAgent: 'Mozilla/5.0 (X11; Linux x86_64) Chrome/130',
      },
    });
    expect(device).toMatchObject({
      id: '_00_push_subscription:s1',
      endpoint: 'https://push.example/1',
      kid: 'kid1',
      label: 'Laptop',
      rules: ['dm'],
      thisDevice: true,
      failures: 0,
    });
    expect(device.createdAt).toEqual(new Date(1_700_000_000_000));
    expect(h.persisted.get(PUSH_ENDPOINT_KEY)).toMatchObject({
      endpoint: 'https://push.example/1',
      userId: 'user:alice',
      kid: 'kid1',
      publicKey: KEY,
    });
    expect(await h.mod.isSubscribed()).toBe(true);
  });

  it('reuses a subscription made with the same key, replaces one made with another', async () => {
    const same = harness({ permission: 'granted' });
    same.browser.sub = same.makeSub('https://push.example/kept', base64UrlToBytes(KEY));
    await same.mod.subscribe({ rules: [] });
    expect(same.pushManager.subscribe).not.toHaveBeenCalled();
    expect(same.remote.query.mock.calls.at(-1)![1]!.opts).toEqual({
      userAgent: expect.any(String),
    });

    const other = harness({ permission: 'granted' });
    const stale = other.makeSub('https://push.example/stale', base64UrlToBytes(NEW_KEY));
    other.browser.sub = stale;
    const device = await other.mod.subscribe();
    expect(stale.unsubscribe).toHaveBeenCalled();
    expect(other.pushManager.subscribe).toHaveBeenCalledTimes(1);
    expect(device.endpoint).toBe('https://push.example/1');
  });

  it('refuses with typed errors: impersonating, signed out, unsupported, denied, disabled', async () => {
    const imp = harness();
    imp.auth.impersonation = { target: 'user:alice' };
    await expect(imp.mod.subscribe()).rejects.toMatchObject({ code: 'impersonating' });
    await expect(imp.mod.notify({ notification: { title: 'x' } })).rejects.toMatchObject({
      code: 'impersonating',
    });
    await expect(imp.mod.update({ label: 'x' })).rejects.toMatchObject({ code: 'impersonating' });
    expect(imp.Notification.requestPermission).not.toHaveBeenCalled();
    expect(imp.remote.query).not.toHaveBeenCalled();

    const out = harness();
    out.auth.isAuthenticated = false;
    out.auth.currentUser = null;
    const err = await out.mod.subscribe().catch((e) => e);
    expect(err).toBeInstanceOf(WebPushError);
    expect(err.code).toBe('signed-out');
    await expect(out.mod.devices()).rejects.toMatchObject({ code: 'signed-out' });

    const none = harness({ globals: { PushManager: undefined } });
    await expect(none.mod.subscribe()).rejects.toMatchObject({ code: 'unsupported' });

    const denied = harness({ permission: 'denied' });
    await expect(denied.mod.subscribe()).rejects.toMatchObject({ code: 'permission-denied' });
    const noPrompt = harness();
    await expect(noPrompt.mod.subscribe({ requestPermission: false })).rejects.toMatchObject({
      code: 'permission-default',
    });

    const off = harness({ permission: 'granted' });
    off.server.info = { enabled: false };
    await expect(off.mod.subscribe()).rejects.toMatchObject({ code: 'disabled' });

    const broken = harness({ permission: 'granted' });
    broken.pushManager.subscribe.mockRejectedValueOnce(new Error('AbortError: push service error'));
    await expect(broken.mod.subscribe()).rejects.toMatchObject({ code: 'subscribe-failed' });
  });
});

describe('WebPushModule.sync', () => {
  function subscribed() {
    const h = harness({ permission: 'granted' });
    return h;
  }

  it('says why it did nothing', async () => {
    const h = subscribed();
    expect(await h.mod.sync()).toBe('not-subscribed');
    h.Notification.permission = 'default';
    expect(await h.mod.sync()).toBe('permission-default');
    h.Notification.permission = 'denied';
    expect(await h.mod.sync()).toBe('permission-denied');
    h.auth.impersonation = {};
    expect(await h.mod.sync()).toBe('impersonating');
    h.auth.isAuthenticated = false;
    expect(await h.mod.sync()).toBe('signed-out');
    expect(await harness({ globals: { window: undefined } }).mod.sync()).toBe('unsupported');
    const off = subscribed();
    off.server.info = { enabled: false };
    expect(await off.mod.sync({ autoResubscribe: true })).toBe('disabled');
  });

  it('ok when browser, key and server row agree; autoResubscribe subscribes a granted browser', async () => {
    const h = subscribed();
    await h.mod.subscribe();
    h.pushManager.subscribe.mockClear();
    expect(await h.mod.sync()).toBe('ok');
    expect(h.pushManager.subscribe).not.toHaveBeenCalled();

    const auto = subscribed();
    expect(await auto.mod.sync({ autoResubscribe: true })).toBe('resubscribed');
    expect(auto.server.rows).toHaveLength(1);
  });

  it('re-creates a browser subscription that went away, keeping label and rules, dropping the dead endpoint', async () => {
    const h = subscribed();
    await h.mod.subscribe({ label: 'Phone', rules: ['dm'] });
    h.browser.sub = null;
    expect(await h.mod.sync()).toBe('resubscribed');
    expect(h.server.rows.map((r) => [r.endpoint, r.label, r.rules])).toEqual([
      ['https://push.example/2', 'Phone', ['dm']],
    ]);
    expect(h.persisted.get(PUSH_ENDPOINT_KEY)).toMatchObject({
      endpoint: 'https://push.example/2',
    });
  });

  it('resubscribes when the deployment key rotated', async () => {
    const h = subscribed();
    await h.mod.subscribe({ label: 'Desk' });
    const old = h.browser.sub;
    h.server.info = { enabled: true, publicKey: NEW_KEY, kid: 'kid2' };
    h.server.rows = h.server.rows.map((r) => ({ ...r, current: false }));
    expect(await h.mod.sync()).toBe('resubscribed');
    expect(old.unsubscribe).toHaveBeenCalled();
    expect(
      new Uint8Array(h.pushManager.subscribe.mock.calls.at(-1)![0].applicationServerKey)
    ).toEqual(base64UrlToBytes(NEW_KEY));
    expect(h.server.rows.map((r) => [r.endpoint, r.kid, r.label])).toEqual([
      ['https://push.example/2', 'kid2', 'Desk'],
    ]);
    expect(h.persisted.get(PUSH_ENDPOINT_KEY)).toMatchObject({ kid: 'kid2', publicKey: NEW_KEY });
  });

  it('a browser that cannot tell its key falls back to the stored kid', async () => {
    const h = subscribed();
    await h.mod.subscribe();
    h.browser.sub = h.makeSub(h.browser.sub.endpoint, null);
    expect(await h.mod.sync()).toBe('ok');
    h.server.info = { enabled: true, publicKey: NEW_KEY, kid: 'kid2' };
    expect(await h.mod.sync()).toBe('resubscribed');
  });

  it('re-registers a server row that went missing or was disabled; reports errors instead of throwing', async () => {
    const h = subscribed();
    await h.mod.subscribe();
    h.server.rows = [];
    expect(await h.mod.sync()).toBe('registered');
    expect(h.server.rows).toHaveLength(1);
    h.server.rows[0].disabled_at = new Date();
    expect(await h.mod.sync()).toBe('resubscribed');
    h.server.fail = true;
    expect(await h.mod.sync()).toBe('error');
  });

  it('auto-syncs once the socket is connected and a user is signed in', async () => {
    vi.useFakeTimers();
    const h = harness({ permission: 'granted' });
    const sync = vi.spyOn(h.mod, 'sync').mockResolvedValue('ok');
    h.mod.attach();
    await vi.advanceTimersByTimeAsync(2_000);
    expect(sync).not.toHaveBeenCalled();
    h.connection.cbs.forEach((cb) => cb('connected'));
    await vi.advanceTimersByTimeAsync(2_000);
    expect(sync).toHaveBeenCalledTimes(1);
    // Same user again: nothing new.
    h.auth.emit();
    await vi.advanceTimersByTimeAsync(2_000);
    expect(sync).toHaveBeenCalledTimes(1);
    // The worker saw the subscription rotate.
    h.messageListeners.forEach((l) =>
      l({ data: { type: 'sp00ky:subscriptionchange', endpoint: 'x' } })
    );
    expect(sync).toHaveBeenCalledTimes(2);
    h.mod.dispose();
  });
});

describe('WebPushModule sign-out', () => {
  it('drops this device for the user and tells the worker, before the token goes', async () => {
    const h = harness({ permission: 'granted' });
    h.mod.attach();
    await h.mod.subscribe();
    h.active.postMessage.mockClear();
    await h.hooks[0]({ userId: 'user:alice', token: 'tok-1', impersonating: false });
    expect(h.remote.query).toHaveBeenLastCalledWith('RETURN fn::push::unsubscribe($endpoint)', {
      endpoint: 'https://push.example/1',
    });
    expect(h.server.rows).toHaveLength(0);
    expect(h.persisted.has(PUSH_ENDPOINT_KEY)).toBe(false);
    expect(h.active.postMessage).toHaveBeenCalledWith({ type: 'sp00ky:signout' });
  });

  it('while impersonating (or with unsubscribeOnSignOut: false) only the worker is told', async () => {
    const h = harness({ permission: 'granted' });
    h.mod.attach();
    await h.mod.subscribe();
    h.remote.query.mockClear();
    await h.hooks[0]({ userId: 'user:alice', token: 't', impersonating: true });
    expect(h.remote.query).not.toHaveBeenCalled();
    expect(h.active.postMessage).toHaveBeenCalledWith({ type: 'sp00ky:signout' });

    const k = harness({ permission: 'granted', config: { unsubscribeOnSignOut: false } });
    k.mod.attach();
    await k.mod.subscribe();
    k.remote.query.mockClear();
    await k.hooks[0]({ userId: 'user:alice', token: 't', impersonating: false });
    expect(k.remote.query).not.toHaveBeenCalled();
  });

  it('runs inside AuthService.signOut, ahead of the token removal, and never holds it past the timeout', async () => {
    const calls: string[] = [];
    let hang = false;
    const remote = {
      setAuthToken: noop,
      getClient: () => ({ invalidate: async () => void calls.push('invalidate') }),
      query: vi.fn(async (sql: string) => {
        calls.push(sql);
        if (hang) return new Promise(() => {});
        return [1];
      }),
    };
    const store = new Map<string, unknown>([
      [PUSH_ENDPOINT_KEY, { endpoint: 'https://push.example/e', userId: 'user:alice', at: 1 }],
    ]);
    const persistence = {
      get: async <T>(k: string) => (store.get(k) as T) ?? null,
      set: async (k: string, v: unknown) => void store.set(k, v),
      remove: async (k: string) => {
        calls.push(`remove ${k}`);
        store.delete(k);
      },
    };
    const auth = new AuthService(
      { tables: [], relationships: [] } as any,
      remote as any,
      persistence,
      logger
    );
    Object.assign(auth, {
      token: 'tok',
      isAuthenticated: true,
      currentUser: { id: new RecordId('user', 'alice') },
    });
    const h = harness({ permission: 'granted' });
    const mod = new WebPushModule({
      remote: remote as any,
      auth,
      persistence,
      logger,
      database: { endpoint: 'wss://db/rpc', namespace: 'n', database: 'd' },
      globals: () => h.globals,
    });
    mod.attach();
    await auth.signOut();
    const unsub = calls.indexOf('RETURN fn::push::unsubscribe($endpoint)');
    expect(unsub).toBeGreaterThanOrEqual(0);
    expect(unsub).toBeLessThan(calls.indexOf('remove sp00ky_auth_token'));
    expect(h.active.postMessage).toHaveBeenCalledWith({ type: 'sp00ky:signout' });
    expect(auth.token).toBeNull();

    // A socket that never answers costs at most the hook timeout.
    vi.useFakeTimers();
    hang = true;
    store.set(PUSH_ENDPOINT_KEY, {
      endpoint: 'https://push.example/e',
      userId: 'user:alice',
      at: 1,
    });
    Object.assign(auth, {
      token: 'tok2',
      isAuthenticated: true,
      currentUser: { id: 'user:alice' },
    });
    let done = false;
    const out = auth.signOut().then(() => void (done = true));
    await vi.advanceTimersByTimeAsync(1_000);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1_100);
    await out;
    expect(done).toBe(true);
    expect(auth.token).toBeNull();
  });
});

describe('WebPushModule bridge', () => {
  it('posts the session while permission is granted, again on token change, never an impersonation token', async () => {
    const h = harness({ permission: 'granted' });
    h.server.info = { enabled: true, publicKey: KEY, kid: 'kid1' };
    await h.mod.info();
    h.mod.attach();
    await flush();
    expect(h.active.postMessage).toHaveBeenCalledWith({
      type: 'sp00ky:token',
      token: 'tok-1',
      userId: 'user:alice',
      endpoint: 'wss://db.example/rpc',
      namespace: 'ns',
      database: 'db',
      publicKey: KEY,
    });
    h.active.postMessage.mockClear();
    h.auth.emit();
    await flush();
    expect(h.active.postMessage).not.toHaveBeenCalled();
    h.auth.token = 'tok-2';
    h.auth.emit();
    await flush();
    expect(h.active.postMessage).toHaveBeenCalledWith(expect.objectContaining({ token: 'tok-2' }));
    h.active.postMessage.mockClear();
    h.auth.token = 'imp-token';
    h.auth.impersonation = { target: 'user:bob' };
    h.auth.emit();
    await flush();
    expect(h.active.postMessage).not.toHaveBeenCalled();
    h.mod.dispose();
  });

  it('the automatic bridge waits for permission; an explicit bridge() does not; bridge: false opts out', async () => {
    const h = harness({ permission: 'default' });
    h.mod.attach();
    await flush();
    expect(h.active.postMessage).not.toHaveBeenCalled();
    const reg = { active: { postMessage: vi.fn() } };
    const off = h.mod.bridge(reg as any);
    await flush();
    expect(reg.active.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'sp00ky:token', token: 'tok-1' })
    );
    off();
    h.auth.token = 'tok-3';
    h.auth.emit();
    await flush();
    expect(reg.active.postMessage).toHaveBeenCalledTimes(1);

    const k = harness({ permission: 'granted', config: { bridge: false } });
    k.mod.attach();
    await flush();
    expect(k.active.postMessage).not.toHaveBeenCalled();
  });

  it('onMessage delivers only sp00ky messages', () => {
    const h = harness();
    const seen: unknown[] = [];
    const off = h.mod.onMessage((m) => seen.push(m));
    h.messageListeners.forEach((l) => l({ data: { type: 'sp00ky:push', payload: { v: 1 } } }));
    h.messageListeners.forEach((l) => l({ data: { type: 'other' } }));
    h.messageListeners.forEach((l) => l({ data: null }));
    expect(seen).toEqual([{ type: 'sp00ky:push', payload: { v: 1 } }]);
    off();
    expect(h.messageListeners).toHaveLength(0);
  });
});

describe('WebPushModule devices and direct messages', () => {
  it('lists devices with this one marked, updates and unsubscribes', async () => {
    const h = harness({ permission: 'granted' });
    h.server.rows.push({
      id: new RecordId('_00_push_subscription', 'other'),
      endpoint: 'https://push.example/other',
      kid: 'kid1',
      user_agent: 'Firefox',
      failures: 2,
      current: true,
    });
    await h.mod.subscribe({ label: 'Laptop' });
    const devices = await h.mod.devices();
    expect(devices.map((d) => [d.id, d.thisDevice, d.userAgent ?? null, d.failures])).toEqual([
      ['_00_push_subscription:other', false, 'Firefox', 2],
      ['_00_push_subscription:s2', true, 'Mozilla/5.0 (X11; Linux x86_64) Chrome/130', 0],
    ]);
    const updated = await h.mod.update({ label: 'Work', rules: null });
    expect(h.remote.query).toHaveBeenLastCalledWith('RETURN fn::push::update($endpoint, $opts)', {
      endpoint: 'https://push.example/1',
      opts: { label: 'Work', rules: [] },
    });
    expect(updated?.label).toBe('Work');

    expect(await h.mod.unsubscribe({ keepBrowserSubscription: true })).toBe(1);
    expect(h.browser.sub).not.toBeNull();
    expect(h.persisted.has(PUSH_ENDPOINT_KEY)).toBe(false);
    expect(await h.mod.isSubscribed()).toBe(false);
    await expect(
      harness({ permission: 'granted' }).mod.update({ label: 'x' })
    ).rejects.toMatchObject({ code: 'not-subscribed' });

    expect(await h.mod.unsubscribe({ all: true })).toBe(1);
    expect(h.browser.sub).toBeNull();
  });

  it('notify, test and cancel call the fn::push API with the right shapes', async () => {
    const h = harness({ permission: 'granted' });
    const at = new Date('2026-10-01T09:00:00.000Z');
    const msg = await h.mod.notify({
      notification: { title: 'Stand up', url: '/break' },
      topic: 'reminder',
      sendAt: at,
      ttl: 600,
    });
    expect(h.remote.query).toHaveBeenLastCalledWith('RETURN fn::push::notify($msg)', {
      msg: {
        notification: { title: 'Stand up', url: '/break' },
        topic: 'reminder',
        ttl: 600,
        sendAt: '2026-10-01T09:00:00.000Z',
      },
    });
    expect(msg).toEqual({
      id: '_00_push_message:m1',
      status: 'pending',
      sendAt: at,
      createdAt: new Date(1),
    });
    expect(await h.mod.test({ title: 'Hi' })).toMatchObject({ id: '_00_push_message:t1' });
    expect(h.remote.query).toHaveBeenLastCalledWith('RETURN fn::push::test($opts)', {
      opts: { title: 'Hi' },
    });
    expect(await h.mod.cancel('_00_push_message:m1')).toBe(true);
    expect(h.remote.query.mock.calls.at(-1)![1]).toEqual({
      id: new RecordId('_00_push_message', 'm1'),
    });
    await h.mod.cancel(new RecordId('_00_push_message', 'm2'));
    expect(h.remote.query.mock.calls.at(-1)![1]).toEqual({
      id: new RecordId('_00_push_message', 'm2'),
    });
    h.server.fail = true;
    await expect(h.mod.test()).rejects.toMatchObject({ code: 'server' });
  });

  it('lists native devices (APNs / FCM) with their kind, never as this browser', async () => {
    const h = harness({ permission: 'granted' });
    h.server.info = { enabled: true, publicKey: KEY, kid: 'kid1', providers: ['web', 'apns', 'bogus'] };
    h.server.rows.push({
      id: new RecordId('_00_push_subscription', 'phone'),
      endpoint: 'apns:ab12',
      kind: 'apns',
      platform: 'ios',
      app_id: 'im.app',
      environment: 'sandbox',
      kid: '',
      current: true,
    });
    expect((await h.mod.info({ refresh: true })).providers).toEqual(['web', 'apns']);
    await h.mod.subscribe({ label: 'Laptop' });
    const devices = await h.mod.devices();
    const phone = devices.find((d) => d.endpoint === 'apns:ab12')!;
    expect(phone).toMatchObject({ kind: 'apns', platform: 'ios', appId: 'im.app', environment: 'sandbox', thisDevice: false });
    expect(devices.find((d) => d.thisDevice)?.kind).toBe('web');
    // A native row never makes the browser resubscribe.
    expect(await h.mod.sync()).toBe('ok');
  });

  it('caches info until refreshed; a failed read is not cached', async () => {
    const h = harness();
    expect(await h.mod.info()).toEqual({ enabled: true, publicKey: KEY, kid: 'kid1' });
    await h.mod.info();
    expect(h.remote.query).toHaveBeenCalledTimes(1);
    h.server.info = { enabled: true, publicKey: null, kid: null };
    expect(await h.mod.info({ refresh: true })).toEqual({ enabled: false });
    h.server.fail = true;
    await expect(h.mod.info({ refresh: true })).rejects.toMatchObject({ code: 'server' });
    h.server.fail = false;
    expect((await h.mod.info()).enabled).toBe(false);
    expect(recordIdString({ table: { name: 't' }, id: 'x' })).toBe('t:x');
    expect(recordIdString('a:b')).toBe('a:b');
  });
});
