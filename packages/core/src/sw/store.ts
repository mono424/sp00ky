/**
 * The service worker's tiny key-value store: IndexedDB database `sp00ky-push`,
 * object store `kv`. Holds the bridged session (`token`) so a push that wakes
 * the worker with no page open can still read live data. Falls back to memory
 * where IndexedDB does not exist (Node, tests).
 */

export interface KvStore {
  get<T>(key: string): Promise<T | undefined>;
  set(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
}

export const PUSH_DB_NAME = 'sp00ky-push';
export const PUSH_STORE_NAME = 'kv';

export function memoryStore(): KvStore {
  const map = new Map<string, unknown>();
  return {
    get: async <T>(key: string) => map.get(key) as T | undefined,
    set: async (key, value) => void map.set(key, value),
    delete: async (key) => void map.delete(key),
  };
}

type IdbFactoryLike = {
  open(name: string, version?: number): IDBOpenDBRequest;
};

export function idbStore(
  dbName = PUSH_DB_NAME,
  storeName = PUSH_STORE_NAME,
  factory?: IdbFactoryLike
): KvStore {
  let opening: Promise<IDBDatabase> | null = null;
  const open = (): Promise<IDBDatabase> => {
    if (opening) return opening;
    const idb = factory ?? (globalThis as { indexedDB?: IdbFactoryLike }).indexedDB;
    if (!idb) return Promise.reject(new Error('indexedDB unavailable'));
    opening = new Promise<IDBDatabase>((resolve, reject) => {
      const req = idb.open(dbName, 1);
      req.onupgradeneeded = () => {
        if (!req.result.objectStoreNames.contains(storeName))
          req.result.createObjectStore(storeName);
      };
      req.onsuccess = () => {
        const db = req.result;
        // Another context upgrading the database closes ours; reopen next time.
        db.onversionchange = () => {
          db.close();
          opening = null;
        };
        resolve(db);
      };
      req.onerror = () => {
        opening = null;
        reject(req.error);
      };
    });
    return opening;
  };
  const run = async <T>(
    mode: IDBTransactionMode,
    op: (store: IDBObjectStore) => IDBRequest
  ): Promise<T> => {
    const db = await open();
    return new Promise<T>((resolve, reject) => {
      const tx = db.transaction(storeName, mode);
      const req = op(tx.objectStore(storeName));
      tx.oncomplete = () => resolve(req.result as T);
      tx.onerror = () => reject(tx.error ?? req.error);
      tx.onabort = () => reject(tx.error ?? req.error);
    });
  };
  return {
    get: <T>(key: string) => run<T | undefined>('readonly', (s) => s.get(key)),
    set: async (key, value) => {
      await run('readwrite', (s) => s.put(value, key));
    },
    delete: async (key) => {
      await run('readwrite', (s) => s.delete(key));
    },
  };
}

let shared: KvStore | null = null;

/** IndexedDB when it exists, else one process-wide memory store. */
export function defaultStore(): KvStore {
  if (shared) return shared;
  shared =
    typeof (globalThis as { indexedDB?: unknown }).indexedDB !== 'undefined'
      ? idbStore()
      : memoryStore();
  return shared;
}
