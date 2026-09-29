import { describe, expect, it, vi } from 'vitest';
import {
  installPushHandlers,
  getBridgedToken,
  memoryStore,
  notificationOptions,
  DEFAULT_PLACEHOLDER,
  type SwScope,
} from './index';
import type { PushPayload } from '../push/types';
import { base64UrlToBytes, bytesToBase64Url } from '../push/types';

function jwt(claims: Record<string, unknown>): string {
  const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
  return `${b64({ alg: 'HS512' })}.${b64(claims)}.sig`;
}
const token = jwt({ ID: 'user:alice', exp: Math.floor(Date.now() / 1000) + 3600 });
const KEY = bytesToBase64Url(new Uint8Array([4, 1, 2, 3, 4, 5]));

interface FakeWindow {
  url: string;
  visibilityState: string;
  focused?: boolean;
  focus: ReturnType<typeof vi.fn>;
  navigate: ReturnType<typeof vi.fn>;
  postMessage: ReturnType<typeof vi.fn>;
}

function win(url: string, visibilityState = 'hidden', navigateFails = false): FakeWindow {
  return {
    url,
    visibilityState,
    focus: vi.fn(async () => undefined),
    navigate: vi.fn(async () => {
      if (navigateFails) throw new TypeError('not controlled');
    }),
    postMessage: vi.fn(),
  };
}

function fakeScope() {
  const listeners = new Map<string, (e: any) => void>();
  const shown: Array<{ title: string; options: any; closed: boolean }> = [];
  const windows: FakeWindow[] = [];
  const subscribe = vi.fn(async (_o: unknown) => ({
    endpoint: 'https://push.example/new',
    toJSON: () => ({ endpoint: 'https://push.example/new', keys: { p256dh: 'p', auth: 'a' } }),
  }));
  const scope = {
    registration: {
      scope: 'https://app.example/',
      showNotification: vi.fn(async (title: string, options: any = {}) => {
        for (const n of shown)
          if (!n.closed && options.tag && n.options.tag === options.tag) n.closed = true;
        shown.push({ title, options, closed: false });
      }),
      getNotifications: async () =>
        shown
          .filter((n) => !n.closed)
          .map((n) => ({
            title: n.title,
            tag: n.options.tag ?? '',
            body: n.options.body,
            data: n.options.data,
            close: () => void (n.closed = true),
          })),
      pushManager: { getSubscription: async () => null, subscribe },
    },
    clients: { matchAll: vi.fn(async () => windows), openWindow: vi.fn(async () => null) },
    location: { origin: 'https://app.example' },
    navigator: { userAgent: 'sw-ua' },
    addEventListener: (type: string, l: (e: any) => void) => void listeners.set(type, l),
    removeEventListener: (type: string) => void listeners.delete(type),
  };
  const dispatch = async (type: string, event: Record<string, unknown> = {}) => {
    let work: Promise<unknown> | undefined;
    listeners.get(type)!({ ...event, waitUntil: (p: Promise<unknown>) => void (work = p) });
    await work;
  };
  const open = () => shown.filter((n) => !n.closed);
  return {
    scope: scope as unknown as SwScope,
    raw: scope,
    shown,
    open,
    windows,
    dispatch,
    listeners,
    subscribe,
  };
}

const pushEvent = (payload: unknown) => ({
  data: { json: () => payload, text: () => JSON.stringify(payload) },
});

const content: PushPayload = {
  v: 1,
  kind: 'rule',
  rule: 'new-message',
  table: 'message',
  id: 'message:1',
  op: 'create',
  topic: 'dm:1',
  notification: {
    title: 'Ada',
    body: 'hi there',
    url: '/m/1',
    icon: '/i.png',
    vibrate: [100, 50],
    actions: [{ action: 'reply', title: 'Reply', url: '/m/1/reply' }],
    data: { k: 1 },
  },
  data: { conversation: 'conversation:1' },
  ts: 1,
};
const nudge: PushPayload = {
  v: 1,
  kind: 'rule',
  rule: 'feed',
  table: 'notification',
  id: 'notification:9',
  op: 'create',
  ts: 1,
};

function fakeFeed(rows: Record<string, any>[], opts: { fail?: boolean } = {}) {
  const feed = {
    closed: false,
    subscribe: vi.fn((_q: unknown, h: { onSet(r: unknown[]): void }) => {
      if (!opts.fail) h.onSet(rows);
      return {
        ready: opts.fail ? Promise.reject(new Error('register failed')) : Promise.resolve(rows),
        rows: () => rows,
        key: 'k',
        unsubscribe: async () => {},
      };
    }),
    idle: vi.fn(async () => true),
    close: vi.fn(async () => void (feed.closed = true)),
    query: vi.fn(async (sql: string, _vars?: unknown) =>
      sql.includes('fn::push::list')
        ? [[{ endpoint: 'https://push.example/old', label: 'Laptop', rules: ['dm'] }]]
        : ['ok']
    ),
    connect: async () => {},
    userId: 'user:alice',
    listRefTable: '_00_list_ref_user_alice',
  };
  return feed;
}

describe('installPushHandlers: push', () => {
  it('shows a content push with every option, tag defaulting to the topic, url and payload in data', async () => {
    const f = fakeScope();
    installPushHandlers({ scope: f.scope, store: memoryStore() });
    await f.dispatch('push', pushEvent(content));
    expect(f.raw.registration.showNotification).toHaveBeenCalledTimes(1);
    const [title, options] = f.raw.registration.showNotification.mock.calls[0];
    expect(title).toBe('Ada');
    expect(options).toEqual({
      body: 'hi there',
      icon: '/i.png',
      vibrate: [100, 50],
      tag: 'dm:1',
      actions: [{ action: 'reply', title: 'Reply' }],
      data: { k: 1, url: '/m/1', payload: content, actionUrls: { reply: '/m/1/reply' } },
    });
  });

  it('posts to a visible window instead of showing; shows anyway with suppressWhenVisible: false', async () => {
    const f = fakeScope();
    const visible = win('https://app.example/inbox', 'visible');
    const foreign = win('https://evil.example/', 'visible');
    f.windows.push(visible, foreign, win('https://app.example/other'));
    installPushHandlers({ scope: f.scope, store: memoryStore() });
    await f.dispatch('push', pushEvent(content));
    expect(visible.postMessage).toHaveBeenCalledWith({ type: 'sp00ky:push', payload: content });
    expect(foreign.postMessage).not.toHaveBeenCalled();
    expect(f.raw.registration.showNotification).not.toHaveBeenCalled();

    const g = fakeScope();
    g.windows.push(win('https://app.example/inbox', 'visible'));
    installPushHandlers({ scope: g.scope, store: memoryStore(), suppressWhenVisible: false });
    await g.dispatch('push', pushEvent(content));
    expect(g.raw.registration.showNotification).toHaveBeenCalledTimes(1);
  });

  it('shows the placeholder for a nudge nobody renders, an unknown version and garbage', async () => {
    const f = fakeScope();
    installPushHandlers({
      scope: f.scope,
      store: memoryStore(),
      placeholder: { title: 'WhitePawn', body: 'New activity' },
    });
    await f.dispatch('push', pushEvent(nudge));
    await f.dispatch('push', pushEvent({ ...content, v: 2 }));
    await f.dispatch('push', { data: { json: () => JSON.parse('{') } });
    await f.dispatch('push', { data: null });
    expect(f.raw.registration.showNotification).toHaveBeenCalledTimes(4);
    for (const [title, options] of f.raw.registration.showNotification.mock.calls) {
      expect(title).toBe('WhitePawn');
      expect(options.tag).toBe('sp00ky-placeholder');
    }
    const h = fakeScope();
    installPushHandlers({ scope: h.scope, store: memoryStore() });
    await h.dispatch('push', pushEvent(nudge));
    expect(h.raw.registration.showNotification.mock.calls[0][0]).toBe(DEFAULT_PLACEHOLDER.title);
  });

  it('onPush can take over; onNudge and render handle nudges; a throwing handler falls back to the placeholder', async () => {
    const f = fakeScope();
    const onPush = vi.fn(async () => true);
    installPushHandlers({ scope: f.scope, store: memoryStore(), onPush });
    await f.dispatch('push', pushEvent(content));
    expect(onPush).toHaveBeenCalledWith(
      content,
      expect.objectContaining({ registration: f.raw.registration })
    );
    expect(f.raw.registration.showNotification).not.toHaveBeenCalled();

    const g = fakeScope();
    installPushHandlers({
      scope: g.scope,
      store: memoryStore(),
      onNudge: async (p, ctx) => ctx.show({ title: `nudge ${p.rule}`, tag: 'n' }),
    });
    await g.dispatch('push', pushEvent(nudge));
    expect(g.raw.registration.showNotification.mock.calls[0][0]).toBe('nudge feed');

    const h = fakeScope();
    installPushHandlers({
      scope: h.scope,
      store: memoryStore(),
      render: (p) => (p.notification ? null : { title: `rendered ${p.id}` }),
    });
    await h.dispatch('push', pushEvent(nudge));
    await h.dispatch('push', pushEvent(content));
    expect(h.raw.registration.showNotification.mock.calls.map((c) => c[0])).toEqual([
      'rendered notification:9',
      'Ada',
    ]);

    const k = fakeScope();
    installPushHandlers({
      scope: k.scope,
      store: memoryStore(),
      onNudge: async () => {
        throw new Error('boom');
      },
    });
    await k.dispatch('push', pushEvent(nudge));
    expect(k.raw.registration.showNotification.mock.calls[0][1].tag).toBe('sp00ky-placeholder');
  });

  it('renders a nudge from live data: one notification per visible row, stale tags closed, feed closed', async () => {
    const f = fakeScope();
    const store = memoryStore();
    await store.set('token', {
      token,
      userId: 'user:alice',
      endpoint: 'wss://db/rpc',
      namespace: 'n',
      database: 'd',
      at: 1,
    });
    // A notification the live renderer showed earlier for a row that is now read.
    await f.raw.registration.showNotification('old', { tag: 'n:old', data: { sp00kyLive: true } });
    await f.raw.registration.showNotification('unrelated', { tag: 'other' });
    const rows = [
      { id: 'notification:1', title: 'A', read: false },
      { id: 'notification:2', title: 'B', read: true },
      { id: 'notification:3', title: 'C', read: false },
    ];
    const feed = fakeFeed(rows);
    const factory = vi.fn(() => feed);
    const query = vi.fn((userId: string) => ({
      surql: 'SELECT * FROM notification WHERE user = $u',
      params: { u: userId },
    }));
    installPushHandlers({
      scope: f.scope,
      store,
      live: {
        feed: factory as never,
        query,
        visible: (r) => !r.read,
        render: (r) => ({
          title: r.title,
          body: String(r.id),
          tag: `n:${r.id}`,
          url: `/n/${r.id}`,
        }),
      },
    });
    await f.dispatch('push', pushEvent(nudge));
    expect(factory).toHaveBeenCalledWith(
      expect.objectContaining({ token, userId: 'user:alice', endpoint: 'wss://db/rpc' })
    );
    expect(query).toHaveBeenCalledWith('user:alice', nudge);
    expect(feed.close).toHaveBeenCalled();
    expect(
      f
        .open()
        .map((n) => n.options.tag)
        .sort()
    ).toEqual(['n:notification:1', 'n:notification:3', 'other']);
    const shownLive = f.open().find((n) => n.options.tag === 'n:notification:1')!;
    expect(shownLive.options.data).toEqual({ url: '/n/notification:1', sp00kyLive: true });

    // Nothing visible any more: the stale one closes, and closing was the
    // push's effect, so nothing else is shown.
    const g = fakeScope();
    await g.raw.registration.showNotification('old', { tag: 'n:old', data: { sp00kyLive: true } });
    installPushHandlers({
      scope: g.scope,
      store,
      live: {
        feed: () => fakeFeed([]) as never,
        query,
        visible: () => true,
        render: (r: any) => ({ title: r.title, tag: 'x' }),
      },
    });
    await g.dispatch('push', pushEvent(nudge));
    expect(g.open()).toEqual([]);

    // `silentClose: false` keeps a notification on screen for every push.
    const h = fakeScope();
    await h.raw.registration.showNotification('old', { tag: 'n:old', data: { sp00kyLive: true } });
    installPushHandlers({
      scope: h.scope,
      store,
      live: {
        feed: () => fakeFeed([]) as never,
        query,
        visible: () => true,
        render: (r: any) => ({ title: r.title, tag: 'x' }),
        silentClose: false,
      },
    });
    await h.dispatch('push', pushEvent(nudge));
    expect(h.open().map((n) => n.options.tag)).toEqual(['sp00ky-placeholder']);

    // Nothing to show and nothing closed: the placeholder.
    const k = fakeScope();
    installPushHandlers({
      scope: k.scope,
      store,
      live: {
        feed: () => fakeFeed([]) as never,
        query,
        visible: () => true,
        render: (r: any) => ({ title: r.title, tag: 'x' }),
      },
    });
    await k.dispatch('push', pushEvent(nudge));
    expect(k.open().map((n) => n.options.tag)).toEqual(['sp00ky-placeholder']);
  });

  it('falls back to the placeholder when the feed fails or no session was bridged', async () => {
    const store = memoryStore();
    const f = fakeScope();
    const live = {
      feed: () => fakeFeed([], { fail: true }) as never,
      query: () => ({ surql: 'SELECT * FROM x' }),
      visible: () => true,
      render: () => ({ title: 't', tag: 't' }),
    };
    installPushHandlers({ scope: f.scope, store, live });
    await f.dispatch('push', pushEvent(nudge));
    expect(f.open().map((n) => n.options.tag)).toEqual(['sp00ky-placeholder']);
    await store.set('token', {
      token,
      userId: 'user:alice',
      endpoint: 'wss://db/rpc',
      namespace: 'n',
      database: 'd',
      at: 1,
    });
    const g = fakeScope();
    installPushHandlers({ scope: g.scope, store, live });
    await g.dispatch('push', pushEvent(nudge));
    expect(g.open().map((n) => n.options.tag)).toEqual(['sp00ky-placeholder']);
  });
});

describe('installPushHandlers: clicks', () => {
  const clickEvent = (data: unknown, action = '') => {
    const close = vi.fn();
    return { event: { notification: { title: 't', tag: 't', data, close }, action }, close };
  };

  it('opens the url when no window is open', async () => {
    const f = fakeScope();
    installPushHandlers({ scope: f.scope, store: memoryStore() });
    const { event, close } = clickEvent({ url: '/m/1', payload: content });
    await f.dispatch('notificationclick', event);
    expect(close).toHaveBeenCalled();
    expect(f.raw.clients.openWindow).toHaveBeenCalledWith('https://app.example/m/1');
  });

  it('focuses an open window and navigates it; action urls win; message mode posts instead', async () => {
    const f = fakeScope();
    const w = win('https://app.example/inbox');
    f.windows.push(w);
    installPushHandlers({ scope: f.scope, store: memoryStore() });
    await f.dispatch(
      'notificationclick',
      clickEvent({ url: '/m/1', actionUrls: { reply: '/m/1/reply' }, payload: content }, 'reply')
        .event
    );
    expect(w.focus).toHaveBeenCalled();
    expect(w.navigate).toHaveBeenCalledWith('https://app.example/m/1/reply');
    expect(f.raw.clients.openWindow).not.toHaveBeenCalled();

    const g = fakeScope();
    const uncontrolled = win('https://app.example/inbox', 'visible', true);
    g.windows.push(uncontrolled);
    installPushHandlers({ scope: g.scope, store: memoryStore() });
    await g.dispatch('notificationclick', clickEvent({ url: '/m/2', payload: content }).event);
    expect(uncontrolled.postMessage).toHaveBeenCalledWith({
      type: 'sp00ky:navigate',
      url: 'https://app.example/m/2',
      payload: content,
      action: undefined,
    });

    const h = fakeScope();
    const spa = win('https://app.example/inbox', 'visible');
    h.windows.push(spa);
    installPushHandlers({
      scope: h.scope,
      store: memoryStore(),
      clickMode: 'message',
      resolveUrl: (u) => `https://app.example/#${u}`,
    });
    await h.dispatch('notificationclick', clickEvent({ url: '/m/3' }).event);
    expect(spa.navigate).not.toHaveBeenCalled();
    expect(spa.postMessage).toHaveBeenCalledWith({
      type: 'sp00ky:navigate',
      url: 'https://app.example/#/m/3',
      payload: undefined,
      action: undefined,
    });
  });

  it('onClick can take over; onClose sees the payload', async () => {
    const f = fakeScope();
    const onClick = vi.fn(async () => true);
    const onClose = vi.fn();
    installPushHandlers({ scope: f.scope, store: memoryStore(), onClick, onClose });
    const { event, close } = clickEvent({ url: '/x', payload: content }, 'reply');
    await f.dispatch('notificationclick', event);
    expect(onClick).toHaveBeenCalledWith(expect.anything(), content, 'reply');
    expect(close).not.toHaveBeenCalled();
    expect(f.raw.clients.openWindow).not.toHaveBeenCalled();
    await f.dispatch('notificationclose', clickEvent({ payload: content }).event);
    expect(onClose).toHaveBeenCalledWith(expect.anything(), content);
  });
});

describe('installPushHandlers: token bridge', () => {
  const tokenMessage = {
    type: 'sp00ky:token',
    token,
    userId: 'user:alice',
    endpoint: 'wss://db/rpc',
    namespace: 'n',
    database: 'd',
    publicKey: KEY,
  };

  it('stores the bridged session, ignores foreign origins, wipes it and every notification on sign-out', async () => {
    const f = fakeScope();
    const store = memoryStore();
    installPushHandlers({ scope: f.scope, store });
    await f.dispatch('message', {
      data: { ...tokenMessage, token: 'foreign' },
      origin: 'https://evil.example',
    });
    expect(await getBridgedToken(store)).toBeNull();
    await f.dispatch('message', { data: tokenMessage, origin: 'https://app.example' });
    expect(await getBridgedToken(store)).toMatchObject({
      token,
      userId: 'user:alice',
      endpoint: 'wss://db/rpc',
      namespace: 'n',
      database: 'd',
      publicKey: KEY,
    });
    await f.dispatch('message', { data: { type: 'sp00ky:token', token: 'x' } });
    expect((await getBridgedToken(store))!.token).toBe(token);

    await f.dispatch('push', pushEvent(content));
    expect(f.open()).toHaveLength(1);
    await f.dispatch('message', {
      data: { type: 'sp00ky:signout' },
      origin: 'https://app.example',
    });
    expect(await getBridgedToken(store)).toBeNull();
    expect(f.open()).toHaveLength(0);
  });

  it('treats an expired bridged token as none', async () => {
    const store = memoryStore();
    await store.set('token', {
      token: jwt({ ID: 'user:a', exp: 1 }),
      userId: 'user:a',
      endpoint: 'e',
      namespace: 'n',
      database: 'd',
      at: 1,
    });
    expect(await getBridgedToken(store)).toBeNull();
    const broken = {
      get: async () => Promise.reject(new Error('idb gone')),
      set: async () => {},
      delete: async () => {},
    };
    expect(await getBridgedToken(broken as never)).toBeNull();
  });

  it('pushsubscriptionchange: re-subscribes with the stored key, moves the device on the server, tells the pages', async () => {
    const f = fakeScope();
    const store = memoryStore();
    await store.set('token', { ...tokenMessage, pushEndpoint: 'https://push.example/old', at: 1 });
    const w = win('https://app.example/');
    f.windows.push(w);
    const feed = fakeFeed([]);
    installPushHandlers({ scope: f.scope, store, feed: () => feed as never });
    await f.dispatch('pushsubscriptionchange', { oldSubscription: null, newSubscription: null });
    expect(f.subscribe).toHaveBeenCalledWith({
      userVisibleOnly: true,
      applicationServerKey: base64UrlToBytes(KEY),
    });
    const sqls = feed.query.mock.calls.map((c) => c[0]);
    expect(sqls).toEqual([
      'RETURN fn::push::list()',
      'RETURN fn::push::subscribe($sub, $opts)',
      'RETURN fn::push::unsubscribe($endpoint)',
    ]);
    expect(feed.query.mock.calls[1][1]).toEqual({
      sub: { endpoint: 'https://push.example/new', keys: { p256dh: 'p', auth: 'a' } },
      opts: { label: 'Laptop', rules: ['dm'], userAgent: 'sw-ua' },
    });
    expect(feed.query.mock.calls[2][1]).toEqual({ endpoint: 'https://push.example/old' });
    expect(feed.close).toHaveBeenCalled();
    expect((await getBridgedToken(store))!.pushEndpoint).toBe('https://push.example/new');
    expect(w.postMessage).toHaveBeenCalledWith({
      type: 'sp00ky:subscriptionchange',
      endpoint: 'https://push.example/new',
    });
  });

  it('uninstall removes every listener', () => {
    const f = fakeScope();
    const off = installPushHandlers({ scope: f.scope, store: memoryStore() });
    expect([...f.listeners.keys()].sort()).toEqual([
      'message',
      'notificationclick',
      'notificationclose',
      'push',
      'pushsubscriptionchange',
    ]);
    off();
    expect(f.listeners.size).toBe(0);
  });
});

describe('notificationOptions', () => {
  it('drops renotify without a tag and keeps unknown keys', () => {
    const [title, options] = notificationOptions({ title: 'x', renotify: true, timestamp: 5 });
    expect(title).toBe('x');
    expect(options).toEqual({ timestamp: 5, data: {} });
    const [, tagged] = notificationOptions(
      { title: 'x', renotify: true },
      { v: 1, kind: 'message', topic: 't', ts: 1 }
    );
    expect(tagged).toMatchObject({ renotify: true, tag: 't' });
  });
});
