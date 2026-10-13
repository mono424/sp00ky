import type { SyncedDbConfig } from '@spooky-sync/client-solid';
import { createOtelTransmit } from '@spooky-sync/core/otel';
import { schema, SURQL_SCHEMA } from './schema.gen';

// Database configuration
export const dbConfig: SyncedDbConfig<typeof schema> = {
  logLevel: 'trace',
  otelTransmit: createOtelTransmit('/v1/logs'),
  schema: schema,
  schemaSurql: SURQL_SCHEMA,
  // Thread titles and bodies are `@crdt` fields: load Loro at startup so the
  // editors open at once, offline included.
  crdt: true,
  // The production setup (WhitePawn runs the same): SQLite-WASM in a Worker,
  // persisted in OPFS, with one store shared by every tab of the origin. The
  // leader tab owns the store and the sync loop; other tabs read and write it
  // over MessagePorts, and fall back to running alone where SharedWorker or
  // Web Locks are missing.
  localEngine: 'sqlite',
  sharedTabs: true,
  database: {
    namespace: import.meta.env.VITE_DB_NS || 'main',
    database: import.meta.env.VITE_DB_DB || 'example',
    endpoint: import.meta.env.VITE_DB_ENDPOINT || 'ws://localhost:8666/rpc',
    // Under the SQLite engine, anything but 'memory' means the OPFS store.
    store: 'indexeddb',
    persistenceClient: 'localstorage',
  },
};
