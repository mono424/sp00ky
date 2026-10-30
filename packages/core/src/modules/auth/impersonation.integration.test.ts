import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createHmac } from 'node:crypto';
import { Surreal } from 'surrealdb';
import { AuthService } from './index';
import { ORIGIN_TOKEN_KEY } from './impersonation';

/**
 * Impersonation start and stop against a real SurrealDB (the WASM engine) and
 * the real SDK: record access for the admin, the `_00_impersonate` access with
 * the same AUTHENTICATE revocation block the CLI generates, and the session
 * functions minus the HTTP mint (the token is signed here, the way
 * `ssp_protocol::impersonation::mint` signs it).
 */

const KEY = 'test-impersonation-key';
const b64 = (v: string | Buffer) => Buffer.from(v).toString('base64url');

function mintImpersonation(session: string, target: string, admin: string, ttlSecs = 900): string {
  const now = Math.floor(Date.now() / 1000);
  const header = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const claims = b64(
    JSON.stringify({
      iss: 'SurrealDB',
      iat: now,
      nbf: now,
      exp: now + ttlSecs,
      NS: 'n',
      DB: 'd',
      AC: '_00_impersonate',
      ID: target,
      spky_imp: session,
      spky_admin: admin,
      spky_as_access: 'account',
    })
  );
  const sig = createHmac('sha256', KEY).update(`${header}.${claims}`).digest('base64url');
  return `${header}.${claims}.${sig}`;
}

const SCHEMA = `
DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE true FOR create, update, delete NONE;
DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNIN (SELECT * FROM user WHERE email = $email AND pass = $pass)
  DURATION FOR TOKEN 365d, FOR SESSION 365d;
DEFINE TABLE _00_admin SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE _00_impersonation SCHEMALESS PERMISSIONS FOR select, create, update WHERE true;
DEFINE ACCESS _00_impersonate ON DATABASE TYPE RECORD
  WITH JWT ALGORITHM HS256 KEY '${KEY}'
  AUTHENTICATE {
    LET $s = (SELECT * FROM ONLY type::record($token.spky_imp));
    IF $s = NONE OR $s.ended_at != NONE OR <string>$s.target != <string>$auth.id {
      THROW 'Impersonation session is no longer valid.';
    };
    IF array::len((SELECT VALUE id FROM _00_admin WHERE user = $s.admin LIMIT 1)) = 0 {
      THROW 'Impersonation session is no longer valid.';
    };
    RETURN $auth;
  }
  DURATION FOR TOKEN 15m, FOR SESSION 15m;
DEFINE FUNCTION fn::_00_impersonate::start($target: string, $reason: string) {
  IF $access = '_00_impersonate' { THROW 'Cannot start impersonation from an impersonated session.'; };
  UPSERT _00_impersonation:s1 SET target = type::record($target), admin = $auth.id, reason = $reason, ended_at = NONE;
  RETURN { token: $test_imp_token, session: '_00_impersonation:s1' };
} PERMISSIONS FULL;
DEFINE FUNCTION fn::_00_impersonate::stop($session_id: string) {
  UPDATE type::record($session_id) SET ended_at = time::now();
  RETURN NONE;
} PERMISSIONS FULL;
DEFINE FUNCTION fn::_00_impersonate::renew() {
  IF $access != '_00_impersonate' { THROW 'Not an impersonated session.'; };
  RETURN { token: $test_imp_token_renewed };
} PERMISSIONS FULL;
DEFINE FUNCTION fn::_00_impersonate::current() {
  IF $access != '_00_impersonate' { RETURN NONE; };
  LET $s = (SELECT * FROM ONLY type::record($token.spky_imp));
  RETURN IF $s = NONE OR $s.ended_at != NONE { NONE } ELSE { { session: <string>$s.id } };
} PERMISSIONS FULL;
CREATE user:alice SET email = 'alice@x', pass = 'pw';
CREATE user:bob SET email = 'bob@x', pass = 'pw';
CREATE _00_admin:alice SET user = user:alice;
`;

const silentLogger = (): any => {
  const l: any = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn(), trace: vi.fn() };
  l.child = () => l;
  return l;
};

describe('impersonation on the SurrealDB-WASM engine', () => {
  let db: Surreal;
  let adminToken: string;

  beforeAll(async () => {
    const realFetch = globalThis.fetch;
    vi.stubGlobal('fetch', async (input: unknown, init?: RequestInit) => {
      const url = input instanceof URL ? input : new URL(String(input));
      if (url.protocol === 'file:') return new Response(await readFile(fileURLToPath(url)));
      return realFetch(input as RequestInfo, init);
    });
    const { createWasmEngines } = await import('@surrealdb/wasm');
    db = new Surreal({ engines: createWasmEngines() });
    await db.connect('mem://');
    await db.use({ namespace: 'n', database: 'd' });
    await db.query(SCHEMA);
    await db.query(`DEFINE PARAM $test_imp_token VALUE '${mintImpersonation('_00_impersonation:s1', 'user:bob', 'user:alice')}' PERMISSIONS FULL;`);
    await db.query(`DEFINE PARAM $test_imp_token_renewed VALUE '${mintImpersonation('_00_impersonation:s1', 'user:bob', 'user:alice', 901)}' PERMISSIONS FULL;`);
    const tokens = await db.signin({ access: 'account', variables: { email: 'alice@x', pass: 'pw' } });
    adminToken = typeof tokens === 'string' ? tokens : (tokens as { access: string }).access;
  }, 60_000);

  afterAll(async () => {
    await db?.close();
    vi.unstubAllGlobals();
  });

  function makeAuth(opts: { failUserRead?: () => boolean; holdStop?: Promise<void> } = {}) {
    const store = new Map<string, unknown>();
    const persistence = {
      get: async (k: string) => store.get(k) ?? null,
      set: async (k: string, v: unknown) => void store.set(k, v),
      remove: async (k: string) => void store.delete(k),
    } as any;
    const remote = {
      query: async (sql: string, vars?: Record<string, unknown>) => {
        if (sql.includes('$auth.id') && opts.failUserRead?.()) {
          throw new Error('There was a problem with the database: the query was cancelled');
        }
        if (sql.includes('fn::_00_impersonate::stop') && opts.holdStop) await opts.holdStop;
        return db.query(sql, vars);
      },
      setAuthToken: () => undefined,
      getClient: () => db,
    } as any;
    return { auth: new AuthService({} as any, remote, persistence, silentLogger()), store };
  }

  it('stop returns to the admin', async () => {
    const { auth, store } = makeAuth();
    await auth.check(adminToken);
    expect(String(auth.currentUser.id)).toBe('user:alice');

    await auth.impersonate('user:bob', 'support');
    expect(String(auth.currentUser.id)).toBe('user:bob');

    await auth.stopImpersonating();
    expect(auth.isAuthenticated).toBe(true);
    expect(String(auth.currentUser.id)).toBe('user:alice');
    expect(store.get('sp00ky_auth_token')).toBe(adminToken);
    expect(store.has(ORIGIN_TOKEN_KEY)).toBe(false);
    // The server session ended too. Binding the id as `session` (a protected
    // variable) failed silently here, and only the token's expiry ended it.
    const [ended] = await db.query<[unknown]>('SELECT VALUE ended_at FROM ONLY _00_impersonation:s1');
    expect(ended).toBeTruthy();
  });

  it('an explicit stop racing a server-ended stop still returns to the admin', async () => {
    let release!: () => void;
    const holdStop = new Promise<void>((r) => (release = r));
    const { auth } = makeAuth({ holdStop });
    await auth.check(adminToken);
    await auth.impersonate('user:bob', 'support');

    // The Stop button's stop is waiting on the server when the poll notices
    // the end (or a renewal is refused). Run apart, the poll's stop returned
    // to the admin and the button's, finding the admin token taken, signed
    // the admin out. Now the second joins the first.
    const explicit = auth.stopImpersonating();
    const fromPoll = auth.stopImpersonating({ serverEnded: true });
    release();
    await Promise.all([explicit, fromPoll]);

    expect(auth.isAuthenticated).toBe(true);
    expect(String(auth.currentUser.id)).toBe('user:alice');
  });

  it('a failed read of the admin row on the way back keeps the admin signed in', async () => {
    let failNext = false;
    const { auth, store } = makeAuth({ failUserRead: () => (failNext ? ((failNext = false), true) : false) });
    await auth.check(adminToken);
    await auth.impersonate('user:bob', 'support');

    failNext = true;
    await auth.stopImpersonating();

    expect(auth.isAuthenticated).toBe(true);
    expect(String(auth.currentUser.id)).toBe('user:alice');
    expect(store.get('sp00ky_auth_token')).toBe(adminToken);
  });

  it('stop after half an hour of renewals and polls returns to the admin', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
    try {
      const { auth } = makeAuth();
      await auth.check(adminToken);
      await auth.impersonate('user:bob', 'support');
      await vi.advanceTimersByTimeAsync(30 * 60 * 1000);
      expect(String(auth.currentUser.id)).toBe('user:bob');

      await auth.stopImpersonating();

      expect(auth.isAuthenticated).toBe(true);
      expect(String(auth.currentUser.id)).toBe('user:alice');
    } finally {
      vi.useRealTimers();
    }
  });
});
