import { describe, it, expect, vi } from 'vitest';
import { AuthService, REFUSAL_RECHECK_MIN_MS } from './index';
import {
  createDatabaseEventSystem,
  DatabaseEventTypes,
} from '../../services/database/events/index';

// The session is re-checked after every reconnect and after a statement the
// server refused because of the session: a token refused on a socket's own
// handshake used to leave the page on an anonymous `connected` socket while
// it kept announcing the user.

function jwt(claims: Record<string, unknown>): string {
  return `header.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.signature`;
}

const silentLogger = () =>
  ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn(), child: () => silentLogger() }) as any;

const REFUSED = 'There was a problem with authentication: this session has ended, sign in again';

function makeAuth() {
  const token = jwt({ AC: 'account', ID: 'user:abc' });
  const store = new Map<string, unknown>([['sp00ky_auth_token', token]]);
  const persistence = {
    get: vi.fn(async (k: string) => store.get(k) ?? null),
    set: vi.fn(async (k: string, v: unknown) => void store.set(k, v)),
    remove: vi.fn(async (k: string) => void store.delete(k)),
  } as any;
  const connected = new Set<() => void>();
  const events = createDatabaseEventSystem();
  let refuse = false;
  const authenticate = vi.fn(async () => {
    if (refuse) throw new Error(REFUSED);
  });
  const remote = {
    query: vi.fn(async () => [{ id: 'user:abc' }]),
    setAuthToken: vi.fn(),
    whenConnected: vi.fn(async () => undefined),
    subscribeConnection: (_event: 'connected', cb: () => void) => {
      connected.add(cb);
      return () => connected.delete(cb);
    },
    getEvents: () => events,
    getClient: () => ({ authenticate, invalidate: vi.fn(async () => undefined) }),
  } as any;
  const auth = new AuthService({} as any, remote, persistence, silentLogger());
  const settle = () => new Promise((r) => setTimeout(r, 0));
  return {
    auth,
    authenticate,
    store,
    refuseFromNowOn: () => void (refuse = true),
    reconnect: async () => {
      for (const cb of connected) cb();
      await settle();
    },
    statementFails: async (error: string) => {
      events.emit(DatabaseEventTypes.RemoteQuery, {
        query: 'RETURN fn::query::register($config)',
        duration: 1,
        success: false,
        error,
        timestamp: Date.now(),
      });
      await settle();
    },
  };
}

describe('AuthService session re-check', () => {
  it('signs out when a reconnected socket can no longer use the token', async () => {
    const t = makeAuth();
    await t.auth.init();
    expect(t.auth.isAuthenticated).toBe(true);

    t.refuseFromNowOn();
    await t.reconnect();

    expect(t.authenticate).toHaveBeenCalledTimes(2);
    expect(t.auth.isAuthenticated).toBe(false);
    expect(t.store.get('sp00ky_auth_token')).toBeUndefined();
  });

  it('keeps a session the server still accepts after a reconnect', async () => {
    const t = makeAuth();
    await t.auth.init();
    await t.reconnect();

    expect(t.authenticate).toHaveBeenCalledTimes(2);
    expect(t.auth.isAuthenticated).toBe(true);
  });

  it('signs out when a statement on an open socket is refused for the session', async () => {
    const t = makeAuth();
    await t.auth.init();

    t.refuseFromNowOn();
    await t.statementFails(`An error occurred: ${REFUSED}`);

    expect(t.auth.isAuthenticated).toBe(false);
  });

  it('ignores statement errors that are not about the session', async () => {
    const t = makeAuth();
    await t.auth.init();
    await t.statementFails('Found 3 for field `n`, but expected a string');
    await t.statementFails('There was a problem with the underlying connection');

    expect(t.authenticate).toHaveBeenCalledTimes(1);
  });

  it('re-checks once per window however many statements are refused', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      const t = makeAuth();
      await t.auth.init();
      await t.statementFails(REFUSED);
      await t.statementFails(REFUSED);
      expect(t.authenticate).toHaveBeenCalledTimes(2);

      vi.setSystemTime(Date.now() + REFUSAL_RECHECK_MIN_MS);
      await t.statementFails(REFUSED);
      expect(t.authenticate).toHaveBeenCalledTimes(3);
      expect(t.auth.isAuthenticated).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does nothing for a signed-out page', async () => {
    const t = makeAuth();
    t.store.clear();
    await t.auth.init();
    await t.reconnect();
    await t.statementFails(REFUSED);

    expect(t.authenticate).not.toHaveBeenCalled();
  });
});
