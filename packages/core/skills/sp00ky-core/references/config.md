# Sp00kyConfig Reference

```typescript
interface Sp00kyConfig<S extends SchemaStructure> {
  database: {
    /** SurrealDB WebSocket endpoint URL */
    endpoint?: string;
    /** SurrealDB namespace */
    namespace: string;
    /** SurrealDB database name */
    database: string;
    /** Local store type: 'memory' (transient) or 'indexeddb' (persistent) */
    store?: 'memory' | 'indexeddb';
    /** Authentication token */
    token?: string;
  };
  /** Unique client identifier. Auto-generated if not provided. */
  clientId?: string;
  /** The schema object (must use `as const satisfies SchemaStructure`) */
  schema: S;
  /** Compiled SURQL schema string for local DB provisioning */
  schemaSurql: string;
  /** Logging level */
  logLevel: 'debug' | 'info' | 'warn' | 'error' | 'fatal' | 'silent' | 'trace';
  /**
   * Persistence client for local storage.
   * - 'surrealdb': Uses the local SurrealDB instance (default)
   * - 'localstorage': Uses browser localStorage
   * - Custom: Provide an object implementing PersistenceClient
   */
  persistenceClient?: PersistenceClient | 'surrealdb' | 'localstorage';
  /** A pino browser transmit object for forwarding logs (e.g. via @spooky-sync/core/otel) */
  otelTransmit?: PinoTransmit;
  /** Debounce time in ms for stream updates (default: 100) */
  streamDebounceTime?: number;
}
```

## PersistenceClient Interface

```typescript
interface PersistenceClient {
  set<T>(key: string, value: T): Promise<void>;
  get<T>(key: string): Promise<T | null>;
  remove(key: string): Promise<void>;
}
```

## QueryTimeToLive Values

Supported TTL values for `client.query()`:

`'1m'`, `'5m'`, `'10m'`, `'15m'`, `'20m'`, `'25m'`, `'30m'`, `'1h'`, `'2h'`, `'3h'`, `'4h'`, `'5h'`, `'6h'`, `'7h'`, `'8h'`, `'9h'`, `'10h'`, `'11h'`, `'12h'`, `'1d'`

## RunOptions

```typescript
interface RunOptions {
  assignedTo?: string;
  max_retries?: number;
  retry_strategy?: 'linear' | 'exponential';
}
```

## UpdateOptions

```typescript
interface UpdateOptions {
  debounced?: boolean | {
    /** 'recordId': latest write wins. 'recordId_x_fields': merge by field (recommended). */
    key?: 'recordId' | 'recordId_x_fields';
    /** Debounce delay in ms */
    delay?: number;
    /**
     * Default true. Keep the merged patch in the local `_00_pending_writes` table and
     * push it as soon as the page is hidden (tab switch, reload, close), so a reload
     * inside the delay does not lose it. false: memory only.
     */
    flushOnHide?: boolean;
  };
}
```
