import { describe, it, expect } from 'vitest';
import { isLocalStoreOpenError } from './local';

// `LocalDatabaseService.connect` recovers (drops the store + reconnects, falling
// back to in-memory) only when the failure is the SurrealDB-WASM IndexedDB
// open/transaction error — NOT for unrelated errors. This pins the message match
// against the real error the engine throws so a recovery path doesn't silently
// stop triggering (or start swallowing unrelated failures).
describe('isLocalStoreOpenError', () => {
  it('matches the real SurrealDB-WASM IndexedDB key-value store error', () => {
    const real = new Error(
      'There was a problem with the key-value store: There was a problem with a ' +
        'transaction: An IndexedDB error occured: idb error'
    );
    expect(isLocalStoreOpenError(real)).toBe(true);
  });

  it('matches on the individual signals (indexeddb / idb error / key-value store)', () => {
    expect(isLocalStoreOpenError(new Error('IndexedDB is not available'))).toBe(true);
    expect(isLocalStoreOpenError(new Error('idb error'))).toBe(true);
    expect(isLocalStoreOpenError(new Error('problem with the key-value store'))).toBe(true);
    // Non-Error inputs are stringified.
    expect(isLocalStoreOpenError('An IndexedDB error occured')).toBe(true);
  });

  it('does NOT match unrelated errors (so we never clear the store spuriously)', () => {
    expect(isLocalStoreOpenError(new Error('WebSocket connection refused'))).toBe(false);
    expect(isLocalStoreOpenError(new Error('Parse error: unexpected token'))).toBe(false);
    expect(isLocalStoreOpenError(new Error('permission denied'))).toBe(false);
    expect(isLocalStoreOpenError(null)).toBe(false);
    expect(isLocalStoreOpenError(undefined)).toBe(false);
  });
});

// Per-user local buckets: URL/name derivation and the SCOPE of the tier-2
// corruption drop. The drop must only ever match the failing bucket's store —
// a substring wipe would take every user's cache AND their un-pushed mutation
// outboxes down with one corrupt store.
import { bucketStoreName, bucketStoreUrl, matchesBucketStore } from './local';

describe('bucket store naming', () => {
  it('derives the store url/name from the bucket id', () => {
    expect(bucketStoreUrl('abc')).toBe('indxdb://sp00ky-abc');
    expect(bucketStoreName('abc')).toBe('sp00ky-abc');
    expect(bucketStoreUrl('anon')).toBe('indxdb://sp00ky-anon');
  });
});

describe('matchesBucketStore', () => {
  it('matches the exact store and derived names', () => {
    expect(matchesBucketStore('sp00ky-abc', 'sp00ky-abc')).toBe(true);
    expect(matchesBucketStore('sp00ky-abc-wal', 'sp00ky-abc')).toBe(true);
    expect(matchesBucketStore('surrealdb/sp00ky-abc', 'sp00ky-abc')).toBe(true);
    expect(matchesBucketStore('SP00KY-ABC', 'sp00ky-abc')).toBe(true);
  });

  it("never matches another bucket's store", () => {
    expect(matchesBucketStore('sp00ky-abcdef', 'sp00ky-abc')).toBe(false);
    expect(matchesBucketStore('sp00ky-anon', 'sp00ky-abc')).toBe(false);
    expect(matchesBucketStore('sp00ky', 'sp00ky-abc')).toBe(false);
    // The legacy shared store is not any bucket's store.
    expect(matchesBucketStore('sp00ky', 'sp00ky-anon')).toBe(false);
  });
});

// A bucket switch that lands while the boot open is still in flight (a reload
// of a signed-in tab, with IndexedDB slow to open) must wait for it. Run
// concurrently, the switch closed the client `connect` was opening and both
// provisioned a store at once, so boot never finished.
import { vi } from 'vitest';
import { Surreal } from 'surrealdb';
import { LocalDatabaseService } from './local';

vi.mock('@surrealdb/wasm', () => ({ createWasmWorkerEngines: () => ({}) }));

describe('LocalDatabaseService open and switch', () => {
  it('a switch waits for the in-flight connect instead of closing the client it opens', async () => {
    const opens: Array<{ url: string; done: () => void }> = [];
    const closed: string[] = [];
    const urlOf = new WeakMap<object, string>();
    vi.spyOn(Surreal.prototype, 'connect').mockImplementation(function (this: Surreal, url: string | URL) {
      urlOf.set(this, String(url));
      return new Promise<true>((resolve) => opens.push({ url: String(url), done: () => resolve(true) }));
    });
    vi.spyOn(Surreal.prototype, 'use').mockResolvedValue({});
    vi.spyOn(Surreal.prototype, 'close').mockImplementation(async function (this: Surreal) {
      closed.push(urlOf.get(this) ?? 'placeholder');
      return true as const;
    });
    const logger: any = { trace: vi.fn(), debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    logger.child = () => logger;
    const local = new LocalDatabaseService({ namespace: 'n', database: 'd', store: 'indexeddb' } as any, logger);

    const booting = local.connect('u1');
    await vi.waitFor(() => expect(opens).toHaveLength(1));
    const switching = local.switchStore('anon');
    await new Promise((r) => setTimeout(r, 10));
    expect(opens.map((o) => o.url)).toEqual(['indxdb://sp00ky-u1']);
    expect(closed).toEqual([]);

    opens[0].done();
    await booting;
    await vi.waitFor(() => expect(opens).toHaveLength(2));
    expect(opens[1].url).toBe('indxdb://sp00ky-anon');
    opens[1].done();
    await switching;
    expect(closed).toEqual(['indxdb://sp00ky-u1']);
    expect(local.currentBucketId).toBe('anon');
    vi.restoreAllMocks();
  });

  // The engine streams a statement's result from its worker; closing the
  // client under it left the statement pending until its 30 s deadline, and
  // every later local statement queued behind it (sign-in hung on "Loading").
  it.each(['memory', 'indexeddb'] as const)('%s: a switch lets a statement in flight finish before it closes the client', async (store) => {
    let answer!: (rows: unknown[]) => void;
    const events: string[] = [];
    vi.spyOn(Surreal.prototype, 'connect').mockResolvedValue(true);
    vi.spyOn(Surreal.prototype, 'use').mockResolvedValue({});
    vi.spyOn(Surreal.prototype, 'query').mockImplementation((() =>
      new Promise((r) => {
        answer = (rows) => {
          events.push('answered');
          r(rows);
        };
      })) as any);
    vi.spyOn(Surreal.prototype, 'close').mockImplementation(async () => {
      events.push('close');
      return true as const;
    });
    const logger: any = { trace: vi.fn(), debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    logger.child = () => logger;
    const local = new LocalDatabaseService({ namespace: 'n', database: 'd', store } as any, logger);
    await local.connect('anon');

    const reading = local.query('SELECT * FROM thing');
    await vi.waitFor(() => expect(answer).toBeDefined());
    const release = local.beginSwitch();
    const switching = local.switchStore('u1');
    await new Promise((r) => setTimeout(r, 10));
    expect(events).toEqual([]);

    answer([[]]);
    await reading;
    await switching;
    release();
    expect(events).toEqual(['answered', 'close']);
    vi.restoreAllMocks();
  });
});
