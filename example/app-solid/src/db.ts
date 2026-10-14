import type { SyncedDbConfig } from '@spooky-sync/client-solid';
import { createOtelTransmit } from '@spooky-sync/core/otel';
import { schema, SURQL_SCHEMA } from './schema.gen';

export const dbConfig: SyncedDbConfig<typeof schema> = {
  // Full client logs, shipped to the dev stack's collector, in development only.
  logLevel: import.meta.env.DEV ? 'trace' : 'warn',
  otelTransmit: import.meta.env.DEV ? createOtelTransmit('/v1/logs') : undefined,
  schema,
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
  // A changed row rides in on its LIVE notification instead of costing a
  // body fetch: one round trip less between another device's edit (a CRDT
  // log row, a comment) and this screen.
  liveInlineBodies: true,
  // Profile pictures are an r2 bucket: their files go through the SSP.
  files: {
    endpoint: import.meta.env.VITE_SPKY_FILES_ENDPOINT || 'http://localhost:8667',
  },
  database: {
    namespace: import.meta.env.VITE_DB_NS || 'main',
    database: import.meta.env.VITE_DB_DB || 'example',
    endpoint: import.meta.env.VITE_DB_ENDPOINT || 'ws://localhost:8666/rpc',
    // Under the SQLite engine, anything but 'memory' means the OPFS store.
    store: 'indexeddb',
  },
};
