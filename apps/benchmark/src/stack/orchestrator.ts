import { startSurrealDB, type SurrealHandle, type SurrealOptions } from './surrealdb.js';
import { startScheduler, type SchedulerHandle, type SchedulerOptions } from './scheduler.js';
import { startSsp, type SspHandle, type SspOptions } from './ssp.js';
import { waitFor } from '../util/wait.js';
import { log } from '../util/log.js';

export interface Stack {
  surreal: SurrealHandle;
  scheduler: SchedulerHandle;
  ssp: SspHandle;
  /** Bearer secret shared by scheduler and SSP for authenticated routes. */
  authSecret: string;
  stop: () => Promise<void>;
}

export interface StackOptions {
  surreal?: SurrealOptions;
  scheduler?: SchedulerOptions;
  ssp?: Omit<SspOptions, 'schedulerUrl'>;
  verbose?: boolean;
  isolateEnv?: boolean;
  /** Disable when a suite owns interrupt handling and must save its report first. */
  handleSignals?: boolean;
  signal?: AbortSignal;
}

export interface PreStack {
  surreal: SurrealHandle;
  authSecret: string;
  /** Start scheduler + SSP after seeding, so bootstrap loads the initial data. */
  startBackend(): Promise<Stack>;
  /** Idempotently stop every resource started by this pre-stack. */
  stop(): Promise<void>;
}

/** Start the database, seed it, then call startBackend to load that initial snapshot. */
export async function startSurrealOnly(opts: StackOptions = {}): Promise<PreStack> {
  const verbose = opts.verbose ?? false;
  const controller = new AbortController();
  const signal = opts.signal
    ? AbortSignal.any([opts.signal, controller.signal])
    : controller.signal;
  const surreal = await startSurrealDB({ ...opts.surreal, verbose, signal });
  const isolated = opts.isolateEnv || opts.scheduler?.isolateEnv || opts.ssp?.isolateEnv;
  const authSecret =
    opts.scheduler?.env?.SPKY_AUTH_SECRET ??
    opts.ssp?.env?.SPKY_AUTH_SECRET ??
    (isolated ? undefined : process.env.SPKY_AUTH_SECRET) ??
    'bench';
  let scheduler: SchedulerHandle | undefined;
  let ssp: SspHandle | undefined;
  let pendingScheduler: Promise<SchedulerHandle> | undefined;
  let pendingSsp: Promise<SspHandle> | undefined;
  let backendStarted = false;
  let stopping: Promise<void> | undefined;
  const onInterrupt = () => {
    void stop().finally(() => process.exit(130));
  };
  const onTerminate = () => {
    void stop().finally(() => process.exit(143));
  };
  const stop = (): Promise<void> =>
    (stopping ??= (async () => {
      controller.abort();
      try {
        // Launchers own their child from spawn, and clean it before rejecting on abort.
        await pendingScheduler?.catch(() => {});
        await pendingSsp?.catch(() => {});
        const errors: unknown[] = [];
        for (const resource of [ssp, scheduler, surreal]) {
          try {
            await resource?.stop();
          } catch (error) {
            errors.push(error);
          }
        }
        if (errors.length) throw new AggregateError(errors, 'Benchmark stack cleanup failed');
      } finally {
        // Keep listeners until cleanup finishes: signal-exit can re-raise a
        // signal when the earlier listener disappears during its own callback.
        process.removeListener('SIGINT', onInterrupt);
        process.removeListener('SIGTERM', onTerminate);
      }
    })());
  if (opts.handleSignals !== false) {
    process.on('SIGINT', onInterrupt);
    process.on('SIGTERM', onTerminate);
  }

  async function startBackend(): Promise<Stack> {
    signal.throwIfAborted();
    if (backendStarted) throw new Error('startBackend called twice');
    backendStarted = true;
    try {
      pendingScheduler = startScheduler(surreal, {
        ...opts.scheduler,
        isolateEnv: opts.scheduler?.isolateEnv ?? opts.isolateEnv,
        verbose,
        signal,
        env: { ...(opts.scheduler?.env ?? {}), SPKY_AUTH_SECRET: authSecret },
      });
      scheduler = await pendingScheduler;
      signal.throwIfAborted();
      pendingSsp = startSsp(surreal, {
        ...opts.ssp,
        isolateEnv: opts.ssp?.isolateEnv ?? opts.isolateEnv,
        schedulerUrl: scheduler.baseUrl,
        verbose,
        signal,
        env: { ...(opts.ssp?.env ?? {}), SPKY_AUTH_SECRET: authSecret },
      });
      ssp = await pendingSsp;
      log.info('Waiting for scheduler to mark SSP as ready');
      const configuredTimeouts = [
        opts.scheduler?.startupTimeoutMs,
        opts.ssp?.startupTimeoutMs,
      ].filter((value): value is number => value !== undefined);
      const readyTimeoutMs = configuredTimeouts.length ? Math.max(...configuredTimeouts) : 60_000;
      await waitFor(
        async () => {
          const requestSignal = AbortSignal.any([signal, AbortSignal.timeout(1000)]);
          const response = await fetch(`${scheduler!.baseUrl}/metrics`, { signal: requestSignal });
          if (!response.ok) {
            await response.body?.cancel();
            return false;
          }
          const metrics = (await response.json()) as { scheduler: { ready_ssps: number } };
          return metrics.scheduler.ready_ssps >= 1;
        },
        { timeoutMs: readyTimeoutMs, intervalMs: 500, label: 'scheduler.ready_ssps>=1', signal }
      );
      log.info('Stack ready');
      return { surreal, scheduler, ssp, authSecret, stop };
    } catch (error) {
      await stop().catch(() => {});
      throw error;
    }
  }
  return { surreal, authSecret, startBackend, stop };
}

/** Convenience for suites that do not need pre-existing rows. */
export async function startStack(opts: StackOptions = {}): Promise<Stack> {
  const pre = await startSurrealOnly(opts);
  return pre.startBackend();
}
