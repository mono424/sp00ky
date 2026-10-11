import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import { FeatureFlagModule } from './index';
import { AuthService } from '../auth/index';

// vitest runs in the `node` environment here, so there is no localStorage.
// The module guards every access with `globalThis.localStorage?.`, which keeps
// overrides in-memory-only — this shim lets the persistence path be tested.
function installLocalStorage() {
  const store = new Map<string, string>();
  const shim = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
  };
  (globalThis as any).localStorage = shim;
  return { store, uninstall: () => delete (globalThis as any).localStorage };
}

// Minimal mocks for the three deps the module touches. The DataModule mock
// captures the single subscribe callback so a test can push live results, and
// counts query() calls to assert the query is SHARED (one registration for all
// flags), not per-key.
function makeDeps() {
  let subCb: ((records: unknown[]) => void) | null = null;
  const calls: Array<{ sql: string; params: unknown }> = [];
  let authCb: ((userId: string | null) => void) | null = null;

  const dataModule = {
    query: async (_table: string, sql: string, params: unknown) => {
      calls.push({ sql, params });
      return `hash:${calls.length}`;
    },
    subscribe: (_hash: string, cb: (records: unknown[]) => void) => {
      subCb = cb;
      return () => {
        subCb = null;
      };
    },
  };
  const sync = { enqueueDownEvent: () => {} };
  const auth = {
    subscribe: (cb: (userId: string | null) => void) => {
      authCb = cb;
      return () => {
        authCb = null;
      };
    },
  };
  const logger = { child: () => ({ warn: () => {} }) };

  const deps = { dataModule, sync, auth, logger } as any;
  return {
    deps,
    calls,
    push: (records: unknown[]) => subCb?.(records),
    setUser: (id: string | null) => authCb?.(id),
    hasSub: () => subCb !== null,
  };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

describe('FeatureFlagModule', () => {
  let env: ReturnType<typeof makeDeps>;
  let mod: FeatureFlagModule<any>;

  beforeEach(() => {
    env = makeDeps();
    mod = new FeatureFlagModule(env.deps);
  });

  it('registers ONE shared, unfiltered query for many flags', async () => {
    mod.feature('alpha');
    mod.feature('beta');
    mod.feature('gamma');
    await tick();

    expect(env.calls.length).toBe(1);
    expect(env.calls[0].sql).not.toContain('WHERE');
    expect(env.calls[0].sql).toContain('FROM _00_user_feature');
    expect(env.calls[0].params).toEqual({});
  });

  it('fans the shared result out to each handle by key', async () => {
    const alpha = mod.feature('alpha', { fallback: 'off' });
    const beta = mod.feature('beta', { fallback: 'off' });
    const missing = mod.feature('missing', { fallback: 'off' });
    await tick();

    env.push([
      { key: 'alpha', variant: 'on' },
      { key: 'beta', variant: 'off' },
    ]);

    expect(alpha.enabled()).toBe(true);
    expect(alpha.variant()).toBe('on');
    expect(beta.enabled()).toBe(false); // assigned 'off'
    expect(missing.enabled()).toBe(false); // no row → fallback 'off'
  });

  it('observes NEW assignments live without re-registering', async () => {
    const flag = mod.feature('live', { fallback: 'off' });
    await tick();

    env.push([]); // user starts with no assignment
    expect(flag.enabled()).toBe(false);

    env.push([{ key: 'live', variant: 'on' }]); // assigned while observing
    expect(flag.enabled()).toBe(true);

    expect(env.calls.length).toBe(1); // still the same single query
  });

  it('seeds a late-created handle from the already-loaded snapshot', async () => {
    mod.feature('alpha', { fallback: 'off' });
    await tick();
    env.push([{ key: 'alpha', variant: 'on' }]);

    const late = mod.feature('alpha', { fallback: 'off' });
    expect(late.enabled()).toBe(true); // no fallback flash
  });

  it('clears flags and re-observes on user change', async () => {
    mod.init();
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    env.push([{ key: 'alpha', variant: 'on' }]);
    expect(flag.enabled()).toBe(true);

    env.setUser('user:other'); // sign-in as a different user
    await tick();
    expect(flag.enabled()).toBe(false); // cleared until the new query resolves
    expect(env.hasSub()).toBe(true); // re-registered for the new user
  });
});

// Local overrides force a variant in THIS browser only. They must win over the
// server assignment on every read path — `variant()`, `payload()`, `enabled()`
// and `subscribe()` all funnel through the same resolver, so a gap in any one
// of them is a gap in all of them.
describe('FeatureFlagModule local overrides', () => {
  let env: ReturnType<typeof makeDeps>;
  let mod: FeatureFlagModule<any>;
  let ls: ReturnType<typeof installLocalStorage>;

  beforeEach(() => {
    ls = installLocalStorage();
    env = makeDeps();
    mod = new FeatureFlagModule(env.deps);
  });

  afterEach(() => ls.uninstall());

  it('wins over the server assignment', async () => {
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    env.push([{ key: 'alpha', variant: 'off' }]);
    expect(flag.enabled()).toBe(false);

    mod.setLocalOverride('alpha', 'on');
    expect(flag.variant()).toBe('on');
    expect(flag.enabled()).toBe(true);
  });

  it('survives a later live result for the same key', async () => {
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    mod.setLocalOverride('alpha', 'on');

    env.push([{ key: 'alpha', variant: 'off' }]); // server disagrees
    expect(flag.variant()).toBe('on');
  });

  it('applies before the first result, without a fallback flash', () => {
    mod.setLocalOverride('alpha', 'on');
    const flag = mod.feature('alpha', { fallback: 'off' });
    expect(flag.variant()).toBe('on'); // seeded even though nothing loaded yet
  });

  it('carries its own payload', async () => {
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    env.push([{ key: 'alpha', variant: 'off', payload: { copy: 'server' } }]);

    mod.setLocalOverride('alpha', 'on', { copy: 'local' });
    expect(flag.payload()).toEqual({ copy: 'local' });
  });

  it('notifies subscribers when set and cleared', async () => {
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    env.push([{ key: 'alpha', variant: 'off' }]);

    const seen: (string | undefined)[] = [];
    flag.subscribe((s) => seen.push(s.variant));
    expect(seen).toEqual(['off']); // immediate call

    mod.setLocalOverride('alpha', 'on');
    mod.setLocalOverride('alpha', null);
    expect(seen).toEqual(['off', 'on', 'off']);
  });

  it('restores the server assignment when cleared', async () => {
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    env.push([{ key: 'alpha', variant: 'treatment', payload: { copy: 'server' } }]);

    mod.setLocalOverride('alpha', 'off');
    expect(flag.variant()).toBe('off');

    mod.clearLocalOverrides();
    expect(flag.variant()).toBe('treatment');
    expect(flag.payload()).toEqual({ copy: 'server' });
  });

  it('survives a user change — it is a browser setting, not a session one', async () => {
    mod.init();
    const flag = mod.feature('alpha', { fallback: 'off' });
    await tick();
    mod.setLocalOverride('alpha', 'on');

    env.setUser('user:other');
    await tick();
    expect(flag.variant()).toBe('on');
  });

  it('persists to localStorage and reloads into a fresh module', () => {
    mod.setLocalOverride('alpha', 'on', { copy: 'local' });
    expect(mod.getLocalOverrides()).toEqual({ alpha: { variant: 'on', payload: { copy: 'local' } } });

    // A new module reads the same page-origin store, as after a reload.
    const reloaded = new FeatureFlagModule(makeDeps().deps);
    expect(reloaded.getLocalOverrides()).toEqual({
      alpha: { variant: 'on', payload: { copy: 'local' } },
    });
    expect(reloaded.feature('alpha', { fallback: 'off' }).variant()).toBe('on');
  });

  it('drops the storage key entirely once the last override is cleared', () => {
    mod.setLocalOverride('alpha', 'on');
    expect(ls.store.size).toBe(1);

    mod.setLocalOverride('alpha', null);
    expect(ls.store.size).toBe(0);
    expect(new FeatureFlagModule(makeDeps().deps).getLocalOverrides()).toEqual({});
  });

  it('ignores a corrupt store rather than failing to construct', () => {
    ls.store.set('sp00ky:feature-overrides', '{not json');
    expect(() => new FeatureFlagModule(makeDeps().deps)).not.toThrow();
  });
});

describe('FeatureFlagModule across a reload', () => {
  // A reload restores the session from the cached token (the id as a string),
  // then the server check lands the user row (the id as a RecordId) a few
  // seconds later. Same user: an assigned flag must stay on throughout, not
  // blink off while the module re-registers its query.
  it('keeps flags through the server verification of the same user', async () => {
    const b64 = Buffer.from(JSON.stringify({ AC: 'account', ID: 'user:5abc' })).toString('base64url');
    const token = `h.${b64}.s`;
    const store = new Map<string, unknown>([['sp00ky_auth_token', token]]);
    const persistence = {
      get: async (k: string) => store.get(k) ?? null,
      set: async (k: string, v: unknown) => void store.set(k, v),
      remove: async (k: string) => void store.delete(k),
    } as any;
    const remote = {
      query: async () => [{ id: new RecordId('user', '5abc') }],
      setAuthToken: () => undefined,
      getClient: () => ({ authenticate: async () => undefined, invalidate: async () => undefined }),
    } as any;
    const silent: any = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    silent.child = () => silent;
    const auth = new AuthService({} as any, remote, persistence, silent);

    const env = makeDeps();
    const flags = new FeatureFlagModule({ ...env.deps, auth } as any);
    flags.init();
    await auth.restoreSessionFromToken();
    const clubs = flags.feature('clubs');
    const seen: (string | undefined)[] = [];
    clubs.subscribe((s) => seen.push(s.variant));
    await tick();
    env.push([{ key: 'clubs', variant: 'on' }]);
    expect(clubs.variant()).toBe('on');
    const registrations = env.calls.length;

    await auth.check();
    await tick();

    expect(clubs.variant()).toBe('on');
    expect(seen.slice(seen.indexOf('on'))).not.toContain(undefined);
    expect(env.calls.length).toBe(registrations);
  });
});

