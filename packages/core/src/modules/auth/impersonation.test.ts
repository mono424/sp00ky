import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AuthService } from './index';
import {
  decodeTokenClaims,
  impersonationFromToken,
  ImpersonationTimers,
  ORIGIN_TOKEN_KEY,
  renewDelayMs,
  SESSION_POLL_MS,
} from './impersonation';

function jwt(claims: Record<string, unknown>): string {
  const b64 = Buffer.from(JSON.stringify(claims)).toString('base64url');
  return `header.${b64}.signature`;
}

const ADMIN = jwt({ AC: 'account', ID: 'user:alice', exp: 9_999_999_999 });
const impToken = (n = 1, extra: Record<string, unknown> = {}) =>
  jwt({
    AC: '_00_impersonate',
    ID: 'user:bob',
    exp: Math.floor(Date.now() / 1000) + 900,
    spky_imp: '_00_impersonation:s1',
    spky_admin: 'user:alice',
    n,
    ...extra,
  });

const silentLogger = () =>
  ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn(), child: () => silentLogger() }) as any;

/**
 * A fake server: tracks which token the socket is authenticated with, and
 * answers the handful of statements the auth flow sends. `ended` simulates a
 * stop or revocation made elsewhere.
 */
function makeServer(opts: { refuse?: Set<string>; unreachable?: boolean } = {}) {
  const state = { socketToken: null as string | null, ended: false, stops: [] as unknown[], renews: 0 };
  const idFor = (token: string | null) => (token ? decodeTokenClaims(token).userId : null);
  const query = vi.fn(async (sql: string, vars?: Record<string, unknown>) => {
    if (opts.unreachable && sql.includes('fn::_00_impersonate::stop')) {
      throw new Error('There was a problem with the underlying connection');
    }
    if (sql.includes('fn::_00_impersonate::start')) return [{ token: impToken(), session: '_00_impersonation:s1' }];
    if (sql.includes('fn::_00_impersonate::renew')) {
      if (state.ended) throw new Error('Impersonation session has ended.');
      state.renews++;
      return [{ token: impToken(state.renews + 1) }];
    }
    if (sql.includes('fn::_00_impersonate::current')) return [state.ended ? null : { session: '_00_impersonation:s1' }];
    if (sql.includes('fn::_00_impersonate::stop')) {
      state.stops.push(vars?.session);
      state.ended = true;
      return [null];
    }
    if (sql.includes('fn::_00_impersonate::list_users')) return [[{ id: 'user:bob', is_admin: false }]];
    if (sql.includes('$auth.id')) return [[{ id: idFor(state.socketToken) }]];
    throw new Error(`unexpected statement ${sql}`);
  });
  const authenticate = vi.fn(async (token: string) => {
    if (opts.refuse?.has(token) || (state.ended && decodeTokenClaims(token).impersonation)) {
      throw new Error('Impersonation session is no longer valid.');
    }
    state.socketToken = token;
  });
  return { state, query, authenticate };
}

function makeAuth(server: ReturnType<typeof makeServer>, stored: Record<string, string> = {}) {
  const store = new Map<string, unknown>(Object.entries(stored));
  const persistence = {
    get: vi.fn(async (k: string) => store.get(k) ?? null),
    set: vi.fn(async (k: string, v: unknown) => void store.set(k, v)),
    remove: vi.fn(async (k: string) => void store.delete(k)),
  } as any;
  const remote = {
    query: server.query,
    setAuthToken: vi.fn(),
    getClient: () => ({ authenticate: server.authenticate, invalidate: vi.fn(async () => undefined) }),
  } as any;
  const auth = new AuthService({} as any, remote, persistence, silentLogger());
  return { auth, store, remote };
}

async function signedInAdmin(server = makeServer()) {
  const ctx = makeAuth(server);
  await ctx.auth.check(ADMIN);
  return { ...ctx, server };
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
});
afterEach(() => {
  vi.useRealTimers();
});

describe('token claims', () => {
  it('recognises only impersonation tokens', () => {
    expect(impersonationFromToken(ADMIN)).toBeNull();
    expect(impersonationFromToken(null)).toBeNull();
    expect(impersonationFromToken('garbage')).toBeNull();
    // A regular token cannot pose as one by carrying the claim.
    expect(impersonationFromToken(jwt({ AC: 'account', ID: 'user:x', spky_imp: 's', spky_admin: 'a' }))).toBeNull();
    const info = impersonationFromToken(impToken());
    expect(info).toMatchObject({ session: '_00_impersonation:s1', target: 'user:bob', admin: 'user:alice' });
    expect(info?.tokenExpiresAt).toBeInstanceOf(Date);
  });

  it('renews at 80% of the remaining life, never later than expiry', () => {
    expect(renewDelayMs(1_000, 0)).toBe(800_000);
    expect(renewDelayMs(10, 0)).toBe(8_000);
    expect(renewDelayMs(4, 0)).toBe(0);
    expect(renewDelayMs(1, 5_000)).toBe(0);
  });
});

describe('impersonate / stopImpersonating', () => {
  it('switches the session to the target and keeps the admin token aside', async () => {
    const { auth, store, server } = await signedInAdmin();
    const changes: (string | null)[] = [];
    auth.subscribeImpersonation((i) => changes.push(i?.target ?? null));

    const info = await auth.impersonate('user:bob', 'support ticket');

    expect(info.target).toBe('user:bob');
    expect(auth.currentUser.id).toBe('user:bob');
    expect(auth.access).toBe('_00_impersonate');
    expect(store.get(ORIGIN_TOKEN_KEY)).toBe(ADMIN);
    expect(server.state.socketToken).toBe(auth.token);
    expect(changes).toEqual([null, 'user:bob']);
  });

  it('refuses to nest', async () => {
    const { auth } = await signedInAdmin();
    await auth.impersonate('user:bob', 'support ticket');
    await expect(auth.impersonate('user:carol', 'again')).rejects.toThrow('Already impersonating');
  });

  it('stays the admin when the server refuses the issued token', async () => {
    const server = makeServer();
    const refused = impToken();
    server.query.mockImplementationOnce(async () => [{ token: refused }]);
    server.authenticate.mockImplementation(async (token: string) => {
      if (token === refused) throw new Error('Impersonation session is no longer valid.');
      server.state.socketToken = token;
    });
    const { auth, store } = await signedInAdmin(server);

    await expect(auth.impersonate('user:carol', 'support ticket')).rejects.toThrow('no longer valid');
    expect(auth.currentUser.id).toBe('user:alice');
    expect(auth.impersonation).toBeNull();
    expect(store.has(ORIGIN_TOKEN_KEY)).toBe(false);
    expect(server.state.socketToken).toBe(ADMIN);
  });

  it('stop ends the server session and returns to the admin', async () => {
    const { auth, store, server } = await signedInAdmin();
    await auth.impersonate('user:bob', 'support ticket');

    await auth.stopImpersonating();

    expect(server.state.stops).toEqual(['_00_impersonation:s1']);
    expect(auth.currentUser.id).toBe('user:alice');
    expect(auth.token).toBe(ADMIN);
    expect(auth.impersonation).toBeNull();
    expect(store.get('sp00ky_auth_token')).toBe(ADMIN);
    expect(store.has(ORIGIN_TOKEN_KEY)).toBe(false);
    expect(auth.consumeEndedImpersonation()).toBe('user:bob');
    expect(auth.consumeEndedImpersonation()).toBeNull();
  });

  it('stop still leaves the target when the server is unreachable', async () => {
    const { auth } = await signedInAdmin(makeServer({ unreachable: true }));
    await auth.impersonate('user:bob', 'support ticket');

    await auth.stopImpersonating();

    expect(auth.currentUser.id).toBe('user:alice');
    expect(auth.impersonation).toBeNull();
  });

  it('signs out entirely when the admin token is refused on the way back', async () => {
    const server = makeServer({ refuse: new Set([ADMIN]) });
    const { auth, store } = makeAuth(server, {
      sp00ky_auth_token: impToken(),
      [ORIGIN_TOKEN_KEY]: ADMIN,
    });
    await auth.check();
    expect(auth.impersonation?.target).toBe('user:bob');

    await auth.stopImpersonating();

    expect(auth.isAuthenticated).toBe(false);
    expect(auth.currentUser).toBeNull();
    expect(store.has('sp00ky_auth_token')).toBe(false);
  });

  it('a revoked impersonation token at boot returns to the admin instead of signing out', async () => {
    const server = makeServer();
    server.state.ended = true;
    const { auth } = makeAuth(server, { sp00ky_auth_token: impToken(), [ORIGIN_TOKEN_KEY]: ADMIN });

    await auth.check();

    expect(auth.isAuthenticated).toBe(true);
    expect(auth.currentUser.id).toBe('user:alice');
  });

  it('an impersonation token with no admin token behind it signs out', async () => {
    const server = makeServer();
    server.state.ended = true;
    const { auth } = makeAuth(server, { sp00ky_auth_token: impToken() });

    await auth.check();

    expect(auth.isAuthenticated).toBe(false);
  });

  it('signOut while impersonating ends the session and forgets the admin token', async () => {
    const { auth, store, server } = await signedInAdmin();
    await auth.impersonate('user:bob', 'support ticket');

    await auth.signOut();

    expect(server.state.stops).toEqual(['_00_impersonation:s1']);
    expect(auth.isAuthenticated).toBe(false);
    expect(store.has(ORIGIN_TOKEN_KEY)).toBe(false);
    expect(store.has('sp00ky_auth_token')).toBe(false);
  });

  it('search goes through the admin-only function', async () => {
    const { auth } = await signedInAdmin();
    expect(await auth.searchImpersonationTargets('bo')).toEqual([{ id: 'user:bob', is_admin: false }]);
  });
});

describe('timers', () => {
  it('renews the token before it expires', async () => {
    const { auth, server } = await signedInAdmin();
    await auth.impersonate('user:bob', 'support ticket');
    const first = auth.token;

    await vi.advanceTimersByTimeAsync(800 * 1000);

    expect(server.state.renews).toBe(1);
    expect(auth.token).not.toBe(first);
    expect(server.state.socketToken).toBe(auth.token);
    expect(auth.impersonation?.target).toBe('user:bob');
  });

  it('a session ended elsewhere drops the client back to the admin within a poll', async () => {
    const { auth, server } = await signedInAdmin();
    await auth.impersonate('user:bob', 'support ticket');
    server.state.ended = true;

    await vi.advanceTimersByTimeAsync(SESSION_POLL_MS);

    expect(auth.currentUser.id).toBe('user:alice');
    expect(auth.impersonation).toBeNull();
  });

  it('keeps going through network failures on the poll', async () => {
    let calls = 0;
    const host = {
      current: () => ({ session: 's', target: 't', admin: 'a', tokenExpiresAt: null }),
      renew: vi.fn(),
      stillActive: vi.fn(async () => {
        calls++;
        throw new Error('There was a problem with the underlying connection');
      }),
      endedRemotely: vi.fn(),
      isNetworkError: () => true,
    };
    const timers = new ImpersonationTimers(host);
    timers.sync();
    await vi.advanceTimersByTimeAsync(SESSION_POLL_MS * 2);
    expect(calls).toBe(2);
    expect(host.endedRemotely).not.toHaveBeenCalled();
    timers.clear();
  });
});

describe('banner configuration', () => {
  it('builds CSS from the theme, defaults included', async () => {
    const { bannerCss, BANNER_BACKGROUND } = await import('./impersonation-banner');
    // `bannerCss` takes a resolved theme; the client resolves it, so this
    // pins the rendered output for a fully specified one.
    const css = bannerCss({
      heightPx: 60,
      insetPx: 16,
      radiusPx: 0,
      background: 'var(--brand-warning)',
      text: '#222',
      accent: '#f00',
      accentText: '#fff',
      stopLabel: 'Leave',
      noPageShift: true,
      label: () => 'x',
    });
    expect(css).toContain('height: 60px');
    expect(css).toContain('background-image: var(--brand-warning)');
    expect(css).toContain('background: #f00; color: #fff');
    expect(css).not.toContain(BANNER_BACKGROUND);
  });

  it('scales the page to the frame without distorting it', async () => {
    const { frameMetrics } = await import('./impersonation-banner');
    const theme = { heightPx: 40, insetPx: 10 };

    const desktop = frameMetrics({ width: 1440, height: 900 }, theme);
    // 900 - 40 - 10 = 850 of 900: one factor for both axes, so the aspect
    // ratio is untouched and nothing is cropped.
    expect(desktop.scale).toBeCloseTo(850 / 900);
    expect(desktop.contentHeight).toBe(850);
    // A zoomed box occupies width * scale, so the layout box is widened to
    // still fill the frame exactly.
    expect(desktop.width * desktop.scale).toBeCloseTo(1440 - 20);

    const phone = frameMetrics({ width: 390, height: 844 }, theme);
    expect(phone.contentHeight).toBe(844 - 50);
    expect(phone.width * phone.scale).toBeCloseTo(370);

    // Degenerate viewports (a hidden tab reporting 0) must not divide by zero
    // or hand back a negative box.
    const zero = frameMetrics({ width: 0, height: 0 }, theme);
    expect(zero.scale).toBe(1);
    expect(zero.contentHeight).toBeGreaterThan(0);
    expect(zero.width).toBeGreaterThan(0);
  });

  it('emphasises the label between ** markers instead of parsing markup', async () => {
    const { __test } = await import('./impersonation-banner');
    // A label is a plain string an app supplies, so markup in it must stay text.
    expect(__test.labelSegments('Impersonating **user:bob** as user:alice')).toEqual([
      { text: 'Impersonating ', bold: false },
      { text: 'user:bob', bold: true },
      { text: ' as user:alice', bold: false },
    ]);
    expect(__test.labelSegments('<img src=x onerror=alert(1)>')).toEqual([
      { text: '<img src=x onerror=alert(1)>', bold: false },
    ]);
  });

  it('resolves a partial theme over the defaults', async () => {
    const { __test, BANNER_HEIGHT_PX } = await import('./impersonation-banner');
    const theme = __test.resolveTheme({ insetPx: 0, stopLabel: 'Exit' });
    expect(theme.insetPx).toBe(0);
    expect(theme.stopLabel).toBe('Exit');
    expect(theme.heightPx).toBe(BANNER_HEIGHT_PX);
    expect(theme.label({ target: 'user:b', admin: 'user:a', session: 's', tokenExpiresAt: null })).toBe(
      'Impersonating **user:b** as user:a'
    );
  });
});
