import { describe, it, expect, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import { AuthService } from './index';

// A SurrealDB record-access JWT carries the access method as `AC` and the
// `$auth.id` record id as `ID`. Only the payload matters here - nothing in the
// client verifies the signature, the server does.
function jwt(claims: Record<string, unknown>): string {
  const b64 = Buffer.from(JSON.stringify(claims)).toString('base64url');
  return `header.${b64}.signature`;
}

const silentLogger = () =>
  ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn(), child: () => silentLogger() }) as any;

function makeAuth(opts: { token?: string | null; query?: any; authenticate?: any } = {}) {
  const store = new Map<string, unknown>();
  if (opts.token) store.set('sp00ky_auth_token', opts.token);
  const persistence = {
    get: vi.fn(async (k: string) => store.get(k) ?? null),
    set: vi.fn(async (k: string, v: unknown) => void store.set(k, v)),
    remove: vi.fn(async (k: string) => void store.delete(k)),
  } as any;
  const remote = {
    query: opts.query ?? vi.fn(async () => [[]]),
    setAuthToken: vi.fn(),
    getClient: () => ({
      authenticate: opts.authenticate ?? vi.fn(async () => undefined),
      invalidate: vi.fn(async () => undefined),
    }),
  } as any;
  return { auth: new AuthService({} as any, remote, persistence, silentLogger()), remote, persistence, store };
}

describe('restoreSessionFromToken', () => {
  it('restores the session from the cached token with NO network', async () => {
    const { auth, remote } = makeAuth({ token: jwt({ AC: 'account', ID: 'user:abc' }) });

    const userId = await auth.restoreSessionFromToken();

    expect(userId).toBe('user:abc');
    expect(auth.isAuthenticated).toBe(true);
    expect(auth.currentUser?.id).toBe('user:abc');
    expect(auth.access).toBe('account');
    // The whole point: this is what makes a warm/offline boot paint.
    expect(remote.query).not.toHaveBeenCalled();
  });

  it('notifies subscribers so query routing can be set before any registration', async () => {
    const { auth } = makeAuth({ token: jwt({ AC: 'account', ID: 'user:abc' }) });
    const seen: (string | null)[] = [];
    auth.subscribe((uid) => seen.push(uid));

    await auth.restoreSessionFromToken();

    expect(seen).toContain('user:abc');
  });

  it('returns null when there is no token, and when the token carries no id', async () => {
    expect(await makeAuth().auth.restoreSessionFromToken()).toBeNull();
    const noId = makeAuth({ token: jwt({ AC: 'account' }) });
    expect(await noId.auth.restoreSessionFromToken()).toBeNull();
    expect(noId.auth.isAuthenticated).toBe(false);
  });

  it('survives a malformed token rather than throwing', async () => {
    const { auth } = makeAuth({ token: 'not-a-jwt' });
    expect(await auth.restoreSessionFromToken()).toBeNull();
  });
});

describe('check() error handling', () => {
  const token = jwt({ AC: 'account', ID: 'user:abc' });

  it('KEEPS the cached session when the server is unreachable', async () => {
    const { auth, persistence, store } = makeAuth({
      token,
      authenticate: vi.fn(async () => {
        throw new Error('There was a problem with the underlying connection');
      }),
    });
    await auth.restoreSessionFromToken();

    await auth.check();

    // A blip must not log the user out - that is what makes offline possible.
    expect(auth.isAuthenticated).toBe(true);
    expect(store.get('sp00ky_auth_token')).toBe(token);
    expect(persistence.remove).not.toHaveBeenCalled();
  });

  it('signs out for real when the server REJECTS the token', async () => {
    const { auth, store } = makeAuth({
      token,
      authenticate: vi.fn(async () => {
        throw new Error('There was a problem with the database: Invalid token');
      }),
    });
    await auth.restoreSessionFromToken();

    await auth.check();

    expect(auth.isAuthenticated).toBe(false);
    expect(store.get('sp00ky_auth_token')).toBeUndefined();
  });
});

// The transport authenticates a NEW socket from the token it was handed, not
// from the one the app passed at construction (most apps pass none and sign in
// later). If sign-in never reaches it, the supervisor's revive loop rebuilds a
// socket that comes back anonymous and stays that way for the life of the page,
// while `currentUser` — restored from local storage — keeps the UI signed in.
// Every view registered after that carries `auth_id = ''`, so `$auth.id`
// predicates resolve false and the user's own rows silently vanish.
describe('auth token reaches the transport', () => {
  it('hands the token to the transport when restoring from cache', async () => {
    const token = jwt({ AC: 'account', ID: 'user:abc' });
    const { auth, remote } = makeAuth({ token });

    await auth.restoreSessionFromToken();

    expect(remote.setAuthToken).toHaveBeenCalledWith(token);
  });

  it('hands the token to the transport on check()', async () => {
    const token = jwt({ AC: 'account', ID: 'user:abc' });
    const { auth, remote } = makeAuth({ token });

    await auth.check();

    expect(remote.setAuthToken).toHaveBeenCalledWith(token);
  });

  it('clears it on sign-out so a revived socket does not come back as the old user', async () => {
    const { auth, remote } = makeAuth({ token: jwt({ AC: 'account', ID: 'user:abc' }) });
    await auth.restoreSessionFromToken();

    await auth.signOut();

    expect(remote.setAuthToken).toHaveBeenLastCalledWith(null);
  });
});

describe('the announced user id', () => {
  // The cached token names the user as a raw string; once check() answers,
  // currentUser holds the server's RecordId. Subscribers compare ids, so the
  // two must announce identically or every reload reads as a sign-in switch.
  it.each([['abc'], ['5onl3w8g9jhnymk3d10p']])(
    'is the same raw string before and after the server check (user:%s)',
    async (key) => {
      const claim = `user:${key}`;
      const { auth } = makeAuth({
        token: jwt({ AC: 'account', ID: claim }),
        query: vi.fn(async () => [{ id: new RecordId('user', key), email: 'x@y' }]),
      });
      const seen: (string | null)[] = [];
      auth.subscribe((uid) => seen.push(uid));

      await auth.restoreSessionFromToken();
      await auth.check();

      expect(auth.currentUser.id).toBeInstanceOf(RecordId);
      // A RecordId's toString escapes a key that starts with a digit.
      expect(seen.filter((v) => v !== null)).toEqual([claim, claim]);
      expect(seen.every((v) => v === null || typeof v === 'string')).toBe(true);
    }
  );
});

