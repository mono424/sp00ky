import type { Surreal, SurrealTransaction } from 'surrealdb';
import type { Logger } from '../logger/index';
import type {
  DatabaseEventSystem,
  DatabaseEventTypes} from './events/index';
import type { SealedQuery } from '../../utils/surql';
import { withTimeout } from '../../utils/index';

export abstract class AbstractDatabaseService {
  protected client: Surreal;
  protected logger: Logger;
  protected events: DatabaseEventSystem;
  /**
   * Per-query deadline in ms; `0` disables. The remote service sets it from
   * `queryTimeoutMs` (see `RemoteDatabaseService`), the local one from
   * `localOpTimeoutMs` (see `LocalDatabaseService`): a local query can be
   * legitimately slow, but it must never be endless - every query waits on the
   * previous link of {@link query}'s chain, and one that never settled wedged
   * every later local op behind it.
   */
  protected queryTimeoutMs = 0;

  /** The error a deadline expiry rejects with; the local service substitutes
   *  its typed `LocalOpTimeoutError`. "timed out" in the message is
   *  load-bearing either way: `classifySyncError` keys off it. */
  protected timeoutError(_query: string): Error {
    return new Error(`Remote query timed out after ${this.queryTimeoutMs}ms`);
  }
  protected abstract eventType:
    | typeof DatabaseEventTypes.LocalQuery
    | typeof DatabaseEventTypes.RemoteQuery;

  constructor(client: Surreal, logger: Logger, events: DatabaseEventSystem) {
    this.client = client;
    this.logger = logger.child({ service: 'Database' });
    this.events = events;
  }

  abstract connect(): Promise<void>;

  getClient(): Surreal {
    return this.client;
  }

  getEvents(): DatabaseEventSystem {
    return this.events;
  }

  tx(): Promise<SurrealTransaction> {
    return this.client.beginTransaction();
  }

  /**
   * How many statements may be in flight at once.
   *
   * `1` serializes every statement behind the previous one. The local engines
   * need that: overlapping ops trip the WASM / SQLite transaction layer. The
   * remote service raises it (`RemoteDatabaseService`): a SurrealDB socket
   * multiplexes requests by id, and serializing them capped the client at one
   * statement per round trip (~5/s at 170ms RTT), so the `_00_list_ref` poll
   * alone could hold every registration behind it and one slow one-shot read
   * stalled the whole client. The scheduler's `MAX_CONCURRENT_DOWN` only means
   * something when this is above 1.
   */
  protected maxConcurrentQueries = 1;
  private inFlightQueries = 0;
  private queryWaiters: Array<() => void> = [];

  /** FIFO slot acquisition, so `maxConcurrentQueries = 1` keeps strict order. */
  private acquireQuerySlot(): Promise<void> {
    if (this.inFlightQueries < this.maxConcurrentQueries) {
      this.inFlightQueries++;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.queryWaiters.push(() => {
        this.inFlightQueries++;
        resolve();
      });
    });
  }

  private releaseQuerySlot(): void {
    this.inFlightQueries--;
    const next = this.queryWaiters.shift();
    if (next) next();
  }

  /**
   * Hook run right before a statement is sent, inside its slot. The remote
   * service uses it to hold statements while a connect is in flight, so a
   * query never runs on a socket that is open but has not had its namespace
   * and token applied yet. The default does nothing.
   */
  protected async beforeQuery(): Promise<void> {}

  /**
   * Execute a statement under {@link maxConcurrentQueries}.
   *
   * A statement that never settles would hold its slot forever, and with a
   * limit of 1 that blocks every later statement behind it. {@link
   * queryTimeoutMs} bounds each one: on expiry this promise rejects and the
   * slot is released, even though the underlying RPC is still parked in the
   * SDK's pending map.
   */
  async query<T extends unknown[]>(query: string, vars?: Record<string, unknown>): Promise<T> {
    return this.runStatement<T>(query, vars, () => this.client.query(query, vars) as unknown as Promise<T>);
  }

  /**
   * Like {@link query} but never rejects on a statement error: every
   * statement answers on its own as `{ status: 'OK', result }` or
   * `{ status: 'ERR', error }`. This is what lets a batched outbox push judge
   * each mutation separately.
   */
  async queryResponses(
    query: string,
    vars?: Record<string, unknown>
  ): Promise<Array<{ status: 'OK'; result: unknown } | { status: 'ERR'; error: string }>> {
    return this.runStatement(query, vars, async () => {
      const responses = await this.client.query(query, vars).responses();
      return responses.map((r) =>
        r.success
          ? { status: 'OK' as const, result: r.result }
          : { status: 'ERR' as const, error: r.error instanceof Error ? r.error.message : String(r.error) }
      );
    });
  }

  private async runStatement<T>(query: string, vars: Record<string, unknown> | undefined, dispatch: () => Promise<T>): Promise<T> {
    await this.acquireQuerySlot();
    try {
      await this.beforeQuery();
      const startTime = performance.now();
      try {
        this.logger.debug(
          { query, vars, Category: 'sp00ky-client::Database::query' },
          'Executing query'
        );
        // "timed out" in the message is load-bearing: `classifySyncError`
        // keys off it to classify this as `network` so the sync queues
        // retry rather than rolling the mutation back.
        const result = await withTimeout(dispatch(), this.queryTimeoutMs, () => this.timeoutError(query));
        const duration = performance.now() - startTime;

        this.events.emit(this.eventType, {
          query,
          vars,
          duration,
          success: true,
          timestamp: Date.now(),
        });

        this.logger.trace(
          { query, result, Category: 'sp00ky-client::Database::query' },
          'Query executed successfully'
        );
        return result;
      } catch (err) {
        const duration = performance.now() - startTime;

        this.events.emit(this.eventType, {
          query,
          vars,
          duration,
          success: false,
          error: err instanceof Error ? err.message : String(err),
          timestamp: Date.now(),
        });

        this.logger.error(
          { query, vars, err, Category: 'sp00ky-client::Database::query' },
          'Query execution failed'
        );
        throw err;
      }
    } finally {
      this.releaseQuerySlot();
    }
  }

  async execute<T>(query: SealedQuery<T>, vars?: Record<string, unknown>): Promise<T> {
    const raw = await this.query<unknown[]>(query.sql, vars);
    return query.extract(raw);
  }

  async close(): Promise<void> {
    this.logger.info({ Category: 'sp00ky-client::Database::close' }, 'Closing database connection');
    await this.client.close();
  }
}
