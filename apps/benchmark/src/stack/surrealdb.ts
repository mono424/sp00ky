import path from 'node:path';
import fs from 'node:fs';
import type { Readable } from 'node:stream';
import {
  GenericContainer,
  type StartedTestContainer,
  Wait,
  type WaitStrategy,
} from 'testcontainers';
import { Surreal } from 'surrealdb';
import { withTimeout } from '../util/wait.js';
import { log } from '../util/log.js';

export interface SurrealHandle {
  wsUrl: string;
  httpUrl: string;
  namespace: string;
  database: string;
  username: string;
  password: string;
  containerId: string;
  /** Container start and initial schema application, excluding seeding. */
  readyMs: number;
  stop: () => Promise<void>;
}

export interface SurrealOptions {
  image?: string;
  /** False skips generated WASM module dependencies and requires an explicit schema. */
  modules?: boolean;
  modulesDir?: string;
  schemaPath?: string;
  namespace?: string;
  database?: string;
  startupTimeoutMs?: number;
  /** Absolute caller-owned host directory, bound at /data for RocksDB storage. */
  storagePath?: string;
  logFile?: string;
  /** Cancels readiness; an in-flight image pull must finish before observing cancellation. */
  signal?: AbortSignal;
  verbose?: boolean;
}

/** No handle could be returned; callers must preserve the mounted storage directory. */
export class SurrealStartupCleanupError extends AggregateError {
  readonly storagePath: string | undefined;

  constructor(startupError: unknown, cleanupError: unknown, storagePath?: string) {
    super(
      [startupError, cleanupError],
      'SurrealDB startup failed and container cleanup failed; preserve mounted storage',
      { cause: startupError }
    );
    this.name = 'SurrealStartupCleanupError';
    this.storagePath = storagePath;
  }
}

const DEFAULT_IMAGE = 'surrealdb/surrealdb:v3.0.0';

function quoteIdentifier(value: string): string {
  return `\`${value.replaceAll('`', '``')}\``;
}

class InterruptibleContainer extends GenericContainer {
  private readonly startupSignal: AbortSignal | undefined;

  constructor(image: string, signal?: AbortSignal) {
    super(image);
    this.startupSignal = signal;
  }

  protected override async beforeContainerCreated(): Promise<void> {
    // Testcontainers pulls first and exposes no cancellation API for that pull.
    // Prevent a completed pull from creating a container after an interrupt.
    this.startupSignal?.throwIfAborted();
  }
}

function interruptibleWait(strategy: WaitStrategy, signal?: AbortSignal): WaitStrategy {
  return {
    async waitUntilReady(container, boundPorts, startTime) {
      signal?.throwIfAborted();
      // Keep awaiting GenericContainer.start: its failure path owns stop/remove.
      await withTimeout(
        strategy.waitUntilReady(container, boundPorts, startTime),
        strategy.getStartupTimeout(),
        'SurrealDB container readiness',
        signal
      );
    },
    withStartupTimeout(timeoutMs) {
      strategy.withStartupTimeout(timeoutMs);
      return this;
    },
    isStartupTimeoutSet: () => strategy.isStartupTimeoutSet(),
    getStartupTimeout: () => strategy.getStartupTimeout(),
  };
}

export async function startSurrealDB(opts: SurrealOptions = {}): Promise<SurrealHandle> {
  opts.signal?.throwIfAborted();
  const startedAt = performance.now();
  const image = opts.image ?? DEFAULT_IMAGE;
  const modules = opts.modules !== false;
  const modulesDir =
    opts.modulesDir ?? path.resolve(import.meta.dirname, '../../../..', 'tests/.spooky');
  // The plain schema has no generated HTTP ingest calls; legacy suites drive /ingest themselves.
  const schemaPath = opts.schemaPath ?? path.resolve(modulesDir, '..', 'schema.surql');
  const namespace = opts.namespace ?? 'bench_ns';
  const database = opts.database ?? 'bench_db';
  const startupTimeoutMs = opts.startupTimeoutMs ?? 60_000;
  if (modules && !fs.existsSync(modulesDir)) {
    throw new Error(
      `SurrealDB modules dir not found: ${modulesDir}. Run \`pnpm -F @spooky-sync/tests run generate\` first.`
    );
  }
  if (!modules && (!opts.schemaPath || !fs.existsSync(schemaPath))) {
    throw new Error('SurrealDB modules:false requires an explicit existing schemaPath');
  }
  const bindMounts: { source: string; target: string }[] = [];
  if (modules) {
    for (const name of ['xor_module.surli', 'ssp_module.surli']) {
      const source = path.resolve(modulesDir, name);
      if (fs.existsSync(source)) bindMounts.push({ source, target: `/modules/${name}` });
    }
  }
  if (opts.storagePath) {
    if (!path.isAbsolute(opts.storagePath))
      throw new Error('SurrealDB storagePath must be an absolute host directory');
    fs.mkdirSync(opts.storagePath, { recursive: true });
    bindMounts.push({ source: opts.storagePath, target: '/data' });
  }
  let output: fs.WriteStream | undefined;
  if (opts.logFile) {
    const file = path.resolve(opts.logFile);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    output = fs.createWriteStream(file, { fd: fs.openSync(file, 'a') });
    output.on('error', (error) => log.warn(`SurrealDB log write failed: ${error.message}`));
  }
  let container: StartedTestContainer | undefined;
  let logs: Readable | undefined;
  let stopping: Promise<void> | undefined;
  const stop = (): Promise<void> =>
    (stopping ??= (async () => {
      logs?.destroy();
      try {
        if (container) await container.stop({ timeout: 10_000 });
      } finally {
        if (output)
          await new Promise<void>((resolve) => {
            if (output!.destroyed) resolve();
            else output!.end(resolve);
          });
      }
    })());
  const db = new Surreal();
  try {
    log.info(`Starting SurrealDB container (${image})`);
    const command = ['start', '--log', 'info', '--user', 'root', '--pass', 'root', '--allow-all'];
    if (modules) command.push('--allow-experimental');
    if (opts.storagePath) command.push('rocksdb:/data/db');
    let builder = new InterruptibleContainer(image, opts.signal)
      .withExposedPorts(8000)
      .withBindMounts(bindMounts)
      .withUser('root')
      .withCommand(command)
      .withStartupTimeout(startupTimeoutMs)
      .withWaitStrategy(
        interruptibleWait(Wait.forLogMessage('Started web server on 0.0.0.0:8000'), opts.signal)
      );
    if (modules)
      builder = builder.withEnvironment({
        SURREAL_BUCKET_FOLDER_ALLOWLIST: '/modules',
        SURREAL_CAPS_ALLOW_EXPERIMENTAL: 'surrealism,files,kv',
      });
    container = await builder.start();
    opts.signal?.throwIfAborted();
    if (output || opts.verbose) {
      logs = await withTimeout(container.logs(), 5_000, 'SurrealDB log stream', opts.signal);
      logs.on('data', (chunk: Buffer) => {
        output?.write(chunk);
        if (opts.verbose) process.stdout.write(chunk);
      });
    }
    const port = container.getMappedPort(8000);
    const rawHost = container.getHost();
    const host = rawHost === 'localhost' ? '127.0.0.1' : rawHost;
    const httpUrl = `http://${host}:${port}`;
    const wsUrl = `ws://${host}:${port}`;
    log.info(`SurrealDB listening at ${httpUrl}`);
    const remainingMs = Math.max(1, startupTimeoutMs - (performance.now() - startedAt));
    await withTimeout(
      (async () => {
        await db.connect(`${httpUrl}/rpc`);
        await db.signin({ username: 'root', password: 'root' });
        await db.query(`DEFINE NAMESPACE IF NOT EXISTS ${quoteIdentifier(namespace)};`);
        await db.use({ namespace });
        await db.query(`DEFINE DATABASE IF NOT EXISTS ${quoteIdentifier(database)};`);
        await db.use({ namespace, database });
        if (fs.existsSync(schemaPath)) {
          const schema = fs
            .readFileSync(schemaPath, 'utf8')
            .replace('file:/modules', 'file:///modules');
          log.info(`Applying schema from ${schemaPath}`);
          const results = (await db.query(schema)) as unknown;
          if (Array.isArray(results))
            for (const result of results as Array<{ status?: string; result?: unknown }>) {
              if (result?.status === 'ERR')
                throw new Error(`Schema apply failed: ${JSON.stringify(result)}`);
            }
        } else {
          log.warn(`Schema file not found at ${schemaPath}, proceeding without applying it.`);
        }
      })(),
      remainingMs,
      'SurrealDB initialization',
      opts.signal
    );
    return {
      wsUrl,
      httpUrl,
      namespace,
      database,
      username: 'root',
      password: 'root',
      containerId: container.getId(),
      readyMs: performance.now() - startedAt,
      stop,
    };
  } catch (error) {
    try {
      await stop();
    } catch (cleanupError) {
      throw new SurrealStartupCleanupError(error, cleanupError, opts.storagePath);
    }
    throw error;
  } finally {
    await withTimeout(db.close(), 1_000, 'SurrealDB setup connection close').catch(() => {});
  }
}

/** Connect a fresh client to the running SurrealDB. */
export async function connectSurreal(handle: SurrealHandle): Promise<Surreal> {
  const db = new Surreal();
  try {
    await withTimeout(
      (async () => {
        await db.connect(`${handle.httpUrl}/rpc`);
        await db.signin({ username: handle.username, password: handle.password });
        await db.use({ namespace: handle.namespace, database: handle.database });
      })(),
      30_000,
      'SurrealDB connection'
    );
    return db;
  } catch (error) {
    await withTimeout(db.close(), 1_000, 'SurrealDB connection close').catch(() => {});
    throw error;
  }
}
