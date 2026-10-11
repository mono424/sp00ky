import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { randomBytes } from 'node:crypto';
import { execa } from 'execa';
import { RecordId, type Surreal } from 'surrealdb';
import { startSurrealOnly, type Stack, type PreStack } from '../stack/orchestrator.js';
import { connectSurreal, SurrealStartupCleanupError } from '../stack/surrealdb.js';
import { processEnvironment } from '../stack/process.js';
import {
  gameRecord,
  gameSql,
  gameWindowQuery,
  gameWindowParams,
  presetCases,
  scrollOffsets,
  seedWhitepawn,
  type WhitepawnProfile,
} from '../workload/whitepawn.js';
import {
  expectedFromRows,
  expectMutationPublication,
  readPublication,
  snapshotExpected,
  waitPublication,
  waitRetainedQueries,
  writeWhitePawnReport,
  type ExpectedPublication,
  type CaseResult,
  type PhaseResult,
  type RunReport,
} from '../metrics/whitepawn.js';
import {
  databaseStats,
  directoryBytes,
  estimateResources,
  ProcessSampler,
  readResources,
  resourceLimit,
  type ResourceEstimate,
} from '../metrics/whitepawn-resources.js';
import { envSnapshot, gitSha } from '../metrics/report.js';
import { log } from '../util/log.js';
import { sleep, waitFor, withTimeout } from '../util/wait.js';

export interface WhitepawnArgs {
  profile: WhitepawnProfile;
  outDir: string;
  startupTimeoutSecs: number;
  spkyBin?: string;
  schedulerBin?: string;
  sspBin?: string;
  surrealImage?: string;
  schemaPath?: string;
  verbose?: boolean;
}

interface ActiveView {
  id: string;
  surql: string;
  clientId: string;
}

class ResourceLimitedError extends Error {}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as net.AddressInfo).port;
      server.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

async function jsonRequest(
  url: string,
  secret: string,
  signal: AbortSignal,
  body?: unknown,
  timeoutMs = 30_000
): Promise<any> {
  const response = await fetch(url, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { authorization: `Bearer ${secret}`, 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]),
  });
  if (!response.ok) throw new Error(`${url}: ${response.status} ${await response.text()}`);
  const text = await response.text();
  return text ? JSON.parse(text) : undefined;
}

/** Every timed write is a durable database transaction; /ingest is never driven directly. */
export async function runWhitepawn(args: WhitepawnArgs): Promise<void> {
  if (!Number.isSafeInteger(args.startupTimeoutSecs) || args.startupTimeoutSecs < 1)
    throw new Error('startup timeout must be a positive integer');
  const root = path.resolve(import.meta.dirname, '../../../..');
  const outDir = path.resolve(args.outDir);
  fs.mkdirSync(outDir, { recursive: true });
  const report: RunReport = {
    suite: 'whitepawn',
    profile: args.profile,
    startedAt: new Date().toISOString(),
    status: 'running',
    config: {
      ...args,
      outDir,
      surrealImage: args.surrealImage ?? 'surrealdb/surrealdb:v3.1.5',
      windowRows: 50,
      retainedWindows: 20,
      scrollSteps: 100,
      mergeViews: true,
      transport: 'changefeed',
      pollMs: 10,
      mutationIterations: args.profile === 'smoke' ? 2 : 20,
      steady: { rowsPerSec: 50, seconds: args.profile === 'smoke' ? 2 : 60 },
      import: {
        rowsPerSec: 1000,
        seconds: args.profile === 'smoke' ? 2 : 30,
        batch: 100,
        concurrency: 4,
      },
    },
    env: { ...envSnapshot(), gitSha: await gitSha() },
    cases: presetCases(args.profile).map((entry) => ({ ...entry, status: 'pending', phases: {} })),
  };
  const controller = new AbortController();
  let current: CaseResult | undefined;
  const save = () => writeWhitePawnReport(outDir, report);
  const interrupt = () => {
    controller.abort(new Error('Benchmark interrupted'));
    report.status = 'interrupted';
    if (current?.status === 'running') current.status = 'interrupted';
    void save();
  };
  // Keep handlers installed until cleanup finishes; signal-exit can re-send
  // the signal when a once-handler has already removed itself.
  process.on('SIGINT', interrupt);
  process.on('SIGTERM', interrupt);
  const checkpoint = setInterval(() => {
    void save();
  }, 5000);
  const observed: ResourceEstimate[] = [];
  await save();

  try {
    for (const entry of report.cases) {
      if (controller.signal.aborted) break;
      current = entry;
      let workDir: string | undefined;
      let pre: PreStack | undefined;
      let stack: Stack | undefined;
      let db: Surreal | undefined;
      let pollTimer: ReturnType<typeof setTimeout> | undefined;
      let sampling: Promise<void> | undefined;
      let samplingStopped = false;
      let resourceFailure: string | undefined;
      const samplers: ProcessSampler[] = [];
      const databaseSamples: Array<{ t: number; memoryBytes: number; cpuPercent: number }> = [];
      const metrics: Array<{ t: number; metrics?: unknown; info?: unknown; error?: string }> = [];
      const local = new AbortController();
      const signal = AbortSignal.any([controller.signal, local.signal]);
      const caseStart = performance.now();
      entry.processSamples = [];
      entry.databaseSamples = databaseSamples;
      entry.schedulerSamples = metrics;
      const ensure = () => {
        if (resourceFailure) throw new ResourceLimitedError(resourceFailure);
        signal.throwIfAborted();
      };
      const phase = async (name: string): Promise<PhaseResult> => {
        ensure();
        entry.phase = name;
        const result = (entry.phases[name] ??= { samples: [] });
        log.info(`[${entry.name}] ${name}`);
        await save();
        return result;
      };

      try {
        entry.status = 'running';
        const resources = await readResources(outDir);
        const estimate = estimateResources(
          entry.rows,
          !!entry.pgnPayload,
          observed,
          args.profile === 'smoke' ? 2100 : 33000
        );
        entry.resources = { before: resources, estimate };
        const limited = resourceLimit(resources, estimate);
        if (limited) throw new ResourceLimitedError(limited);
        workDir = fs.mkdtempSync(path.join(outDir, '.bench-whitepawn-'));
        const schedulerPort = await freePort();
        const sspPort = await freePort();
        const secret = randomBytes(24).toString('hex');
        const logDir = path.join(outDir, entry.name);
        fs.mkdirSync(logDir, { recursive: true });
        const schemaPath = path.join(workDir, 'schema.gen.surql');
        const fixture = path.join(root, 'apps/benchmark/fixtures/whitepawn');
        const spky = path.resolve(args.spkyBin ?? path.join(root, 'target/release/spky'));
        if (!fs.existsSync(spky))
          throw new Error(
            `Missing spky binary: ${spky}. Build with cargo build --release -p sp00ky-cli -p scheduler -p ssp-server`
          );
        for (const binary of [
          args.schedulerBin ?? path.join(root, 'target/release/scheduler'),
          args.sspBin ?? path.join(root, 'target/release/ssp-server'),
        ]) {
          if (!fs.existsSync(binary))
            throw new Error(
              `Missing backend binary: ${binary}. Build release binaries before benchmarking so compilation is excluded from startup timings.`
            );
        }
        await phase('schema');
        const generated = await execa(
          spky,
          [
            'generate',
            '--input',
            path.resolve(args.schemaPath ?? path.join(fixture, 'schema.surql')),
            '--output',
            schemaPath,
            '--format',
            'surql',
            '--mode',
            'cluster',
            '--config',
            path.join(fixture, 'sp00ky.yml'),
            '--endpoint',
            `http://host.docker.internal:${schedulerPort}`,
            '--secret',
            secret,
            '-y',
          ],
          {
            cwd: workDir,
            timeout: 120_000,
            cancelSignal: signal,
            env: processEnvironment(true),
            extendEnv: false,
          }
        );
        fs.writeFileSync(path.join(logDir, 'schema.log'), generated.stdout + generated.stderr);
        const startSampler = (entity: 'scheduler' | 'ssp') => (pid: number) => {
          const sampler = new ProcessSampler(entity, pid);
          samplers.push(sampler);
          (entry.processSamples as unknown[]).push({ entity, samples: sampler.samples });
          sampler.start();
        };
        const backendEnv = {
          SPKY_AUTH_SECRET: secret,
          SPKY_INGEST_TRANSPORT: 'changefeed',
          SPKY_CHANGEFEED_RETENTION: '1d',
          SPKY_CLONE_TIMEOUT_SECS: String(args.startupTimeoutSecs),
          SPKY_BOOTSTRAP_TIMEOUT_SECS: String(args.startupTimeoutSecs),
          SPKY_SSP_MERGE_VIEWS: 'true',
        };
        pre = await startSurrealOnly({
          verbose: args.verbose,
          isolateEnv: true,
          handleSignals: false,
          signal,
          surreal: {
            image: args.surrealImage ?? 'surrealdb/surrealdb:v3.1.5',
            modules: false,
            startupTimeoutMs: args.startupTimeoutSecs * 1000,
            schemaPath,
            storagePath: path.join(workDir, 'database'),
            logFile: path.join(logDir, 'surrealdb.log'),
          },
          scheduler: {
            binPath: args.schedulerBin,
            port: schedulerPort,
            workDir: path.join(workDir, 'scheduler'),
            startupTimeoutMs: args.startupTimeoutSecs * 1000,
            env: backendEnv,
            onSpawn: startSampler('scheduler'),
            logFile: path.join(logDir, 'scheduler.log'),
          },
          ssp: {
            binPath: args.sspBin,
            port: sspPort,
            workDir: path.join(workDir, 'ssp'),
            startupTimeoutMs: args.startupTimeoutSecs * 1000,
            env: backendEnv,
            onSpawn: startSampler('ssp'),
            logFile: path.join(logDir, 'ssp.log'),
          },
        });
        entry.startup = { databaseReadyMs: pre.surreal.readyMs };
        const capture = async () => {
          const t = performance.now() - caseStart;
          if (stack) {
            const [m, info] = await Promise.allSettled([
              jsonRequest(`${stack.scheduler.baseUrl}/metrics`, secret, signal, undefined, 5000),
              metrics.length % 5 === 0
                ? jsonRequest(`${stack.scheduler.baseUrl}/info`, secret, signal, undefined, 5000)
                : Promise.resolve(undefined),
            ]);
            metrics.push({
              t,
              ...(m.status === 'fulfilled' ? { metrics: m.value } : { error: String(m.reason) }),
              ...(info.status === 'fulfilled' && info.value !== undefined
                ? { info: info.value }
                : {}),
            });
          }
          if (
            databaseSamples.length === 0 ||
            t - databaseSamples[databaseSamples.length - 1]!.t >= 5000
          ) {
            try {
              databaseSamples.push({ t, ...(await databaseStats(pre!.surreal.containerId)) });
            } catch {
              /* Persist other telemetry even when stats are unavailable. */
            }
          }
          const disk = fs.statfsSync(outDir);
          const rss = samplers.reduce(
            (total, sampler) => total + (sampler.samples.at(-1)?.rssBytes ?? 0),
            0
          );
          if (disk.bavail * disk.bsize < 5 * 1024 ** 3)
            resourceFailure = 'Free disk fell below the reserved 5 GiB';
          else if (
            rss + (databaseSamples.at(-1)?.memoryBytes ?? 0) >
            resources.hostMemoryBytes * 0.8
          )
            resourceFailure = 'Scheduler + SSP + database memory exceeded 80% of host RAM';
          else if ((databaseSamples.at(-1)?.memoryBytes ?? 0) > resources.dockerMemoryBytes * 0.8)
            resourceFailure = 'Database memory exceeded 80% of Docker RAM';
          if (resourceFailure) local.abort(new ResourceLimitedError(resourceFailure));
          if (!samplingStopped)
            pollTimer = setTimeout(() => {
              sampling = capture();
            }, 1000);
        };
        sampling = capture();
        await phase('seed');
        const seedStart = performance.now();
        entry.seed = await seedWhitepawn(
          pre.surreal,
          entry.rows,
          !!entry.pgnPayload,
          (seeded, total) => {
            ensure();
            entry.seedProgress = { seeded, total };
            const disk = fs.statfsSync(outDir);
            if (disk.bavail * disk.bsize < 5 * 1024 ** 3)
              throw new ResourceLimitedError(
                'Free disk fell below the reserved 5 GiB during seeding'
              );
          },
          signal
        );
        entry.seedMs = performance.now() - seedStart;
        await phase('bootstrap');
        stack = await pre.startBackend();
        entry.startup = {
          ...(entry.startup as object),
          schedulerReadyMs: stack.scheduler.readyMs,
          sspReadyMs: stack.ssp.readyMs,
          backendReadyMs: performance.now() - seedStart - (entry.seedMs as number),
        };
        db = await connectSurreal(stack.surreal);
        const query = <T extends unknown[] = unknown[]>(sql: string) =>
          withTimeout(
            Promise.resolve(db!.query<T>(sql)),
            60_000,
            'benchmark database query',
            signal
          );
        const queryParams = { ...gameWindowParams(), id: 'game:g000000005' };
        const sourceParams = {
          database: new RecordId('game_database', 'main'),
          id: new RecordId('game', 'g000000005'),
        };
        const source = (surql: string) =>
          withTimeout(
            snapshotExpected(db!, surql, sourceParams),
            60_000,
            'source window oracle',
            signal
          );
        const future = (rows: Array<Record<string, unknown>>, versions: Record<string, number>) =>
          withTimeout(
            expectedFromRows(db!, rows, versions),
            30_000,
            'future publication oracle',
            signal
          );
        const counts = await query(
          'SELECT count() AS count FROM game GROUP ALL; SELECT count() AS count FROM _00_version GROUP ALL;'
        );
        entry.databaseCounts = counts;
        const memory = async (label: string, expectedRegistrations?: number) => {
          ensure();
          let value: any;
          await waitFor(
            async () => {
              const health = await fetch(`${stack!.ssp.baseUrl}/health`, {
                signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]),
              });
              await health.body?.cancel();
              if (!health.ok) return false;
              value = await jsonRequest(
                `${stack!.ssp.baseUrl}/debug/memory`,
                secret,
                signal,
                undefined,
                args.startupTimeoutSecs * 1000
              );
              return (
                expectedRegistrations === undefined ||
                value.graphs + value.subscribers === expectedRegistrations
              );
            },
            {
              timeoutMs: args.startupTimeoutSecs * 1000,
              intervalMs: 1000,
              signal,
              label: 'SSP retained graph/subscriber checkpoint',
            }
          );
          const measurements = (entry.memoryMeasurements ??= []) as unknown[];
          measurements.push({ label, value });
          await save();
          return value;
        };
        await memory('bootstrapped');
        const views = new Map<string, ActiveView>();
        const retainedQueries = () =>
          waitRetainedQueries(db!, [...views.keys()], {
            timeoutMs: 30_000,
            pollMs: 100,
            signal,
          });
        let seq = 0;
        const idPrefix = `wp${randomBytes(8).toString('hex')}`;
        const unregister = async (view: ActiveView) => {
          // First detach the active graph using its original registration ID.
          await jsonRequest(`${stack!.scheduler.baseUrl}/view/unregister`, secret, signal, {
            id: view.id,
          });
          // Scheduler acknowledgement precedes SSP teardown. Wait for its
          // committed edge deletion before counting retained graphs/subscribers.
          await waitFor(async () => (await readPublication(db!, view.id)).edges.length === 0, {
            timeoutMs: args.startupTimeoutSecs * 1000,
            intervalMs: 10,
            signal,
            label: 'published view teardown',
          });
          // HTTP teardown retains metadata. Delete it only after SSP clears its
          // edges: record deletion itself cascades graph edges in SurrealDB.
          await withTimeout(
            db!.delete(new RecordId('_00_query', view.id)),
            30_000,
            'durable view eviction',
            signal
          );
          await waitFor(async () => (await readPublication(db!, view.id)).state === null, {
            timeoutMs: 30_000,
            intervalMs: 10,
            signal,
            label: 'durable view teardown',
          });
          views.delete(view.id);
        };
        const register = async (
          surql: string,
          result: PhaseResult,
          clientId = 'whitepawn-client-a'
        ): Promise<ActiveView> => {
          ensure();
          const expected = await source(surql);
          const view = { id: `${idPrefix}v${++seq}`, surql, clientId };
          const started = performance.now();
          const assignment = await jsonRequest(
            `${stack!.scheduler.baseUrl}/view/register`,
            secret,
            signal,
            {
              ...view,
              ttl: '2h',
              lastActiveAt: new Date().toISOString(),
              params: { ...queryParams, auth: { id: 'user:bench' } },
            },
            args.startupTimeoutSecs * 1000
          );
          const responseMs = performance.now() - started;
          const observedPublication = await waitPublication(db!, view.id, expected, {
            timeoutMs: args.startupTimeoutSecs * 1000,
            pollMs: 10,
            initial: true,
            signal,
          });
          result.samples.push({
            name: view.id,
            registerResponseMs: responseMs,
            publishedReadyMs: performance.now() - started,
            assignment,
            observation: observedPublication,
            rootCount: expected.rootIds.length,
            surql,
          });
          views.set(view.id, view);
          return view;
        };
        const warm = await phase('warmup');
        await unregister(await register(gameWindowQuery(0), warm));
        warm.retention = await retainedQueries();
        const scroll = await phase('scroll');
        for (const offset of scrollOffsets(entry.rows)) {
          ensure();
          const surql = gameWindowQuery(offset);
          const retained = [...views.values()].find((view) => view.surql === surql);
          if (retained) {
            views.delete(retained.id);
            views.set(retained.id, retained);
            scroll.samples.push({ name: 'retained-window-hit', offset, id: retained.id });
          } else {
            if (views.size >= 20) await unregister(views.values().next().value!);
            await register(surql, scroll);
          }
          scroll.activeWindows = views.size;
        }
        scroll.retention = await retainedQueries();
        await memory('retained-windows', views.size);
        const registration = await phase('filtered-and-shared');
        while (views.size > 16) await unregister(views.values().next().value!);
        const base =
          [...views.values()].find((view) => view.surql === gameWindowQuery(0)) ??
          (await register(gameWindowQuery(0), registration));
        const filtered = await register(gameWindowQuery(0, { filtered: true }), registration);
        const beforeSharing = await memory('before-shared-subscriber', views.size);
        const duplicate = await register(base.surql, registration, 'whitepawn-client-b');
        const afterSharing = await memory('shared-subscriber', views.size);
        registration.sharing = {
          graphsBefore: beforeSharing.graphs,
          graphsAfter: afterSharing.graphs,
          subscribersBefore: beforeSharing.subscribers,
          subscribersAfter: afterSharing.subscribers,
        };
        if (
          afterSharing.graphs !== beforeSharing.graphs ||
          afterSharing.subscribers !== beforeSharing.subscribers + 1
        )
          throw new Error('Identical subscriptions did not share one operator graph');
        const coldQuery = 'SELECT id, eco FROM game WHERE id = $id LIMIT 1;';
        const cold = await register(coldQuery, registration);
        registration.retention = await retainedQueries();
        const probes = await phase('mutations');
        const iterations = args.profile === 'smoke' ? 2 : 20;
        let newIndex = entry.rows;
        for (let iteration = 0; iteration < iterations; iteration++) {
          for (const category of [
            'insert',
            'projected-update',
            'sort-boundary',
            'filter-boundary',
            'delete',
            'joined-name',
            'unrelated-write',
          ] as const) {
            ensure();
            const observedView = category === 'unrelated-write' ? cold : base;
            const before = await source(observedView.surql);
            const head = before.rows![0]!;
            let sql: string;
            let expected: ExpectedPublication;
            if (category === 'insert') {
              const game = gameRecord(++newIndex, entry.rows, !!entry.pgnPayload);
              game.database = 'game_database:main';
              game.owner = 'owner-main';
              game.sort_index = -2_000_000_000 - newIndex;
              game.white = (head.white as { id: string }).id;
              game.black = (head.black as { id: string }).id;
              sql = `INSERT INTO game ${gameSql(game)};`;
              expected = await future(
                [{ ...game, white: head.white, black: head.black }, ...before.rows!.slice(0, 49)],
                { ...before.versions, [game.id]: 1 }
              );
            } else if (
              category === 'sort-boundary' ||
              category === 'filter-boundary' ||
              category === 'delete'
            ) {
              expected = await source(gameWindowQuery(1));
              sql =
                category === 'sort-boundary'
                  ? `UPDATE ${head.id} SET sort_index = 2147483647;`
                  : category === 'filter-boundary'
                    ? `UPDATE ${head.id} SET database = game_database:c01, owner = "owner-c01";`
                    : `DELETE ${head.id};`;
            } else {
              const id = category === 'joined-name' ? (head.white as { id: string }).id : head.id;
              sql =
                category === 'joined-name'
                  ? `UPDATE ${id} SET name = "Benchmark rename ${iteration}";`
                  : `UPDATE ${id} SET eco = "X${String(iteration).padStart(2, '0')}";`;
              // A previously ready edge may be ahead of the source baseline.
              // Require this mutation to advance its committed version again.
              const committedBefore = await withTimeout(
                readPublication(db!, observedView.id),
                10_000,
                'prewrite publication',
                signal
              );
              expected = expectMutationPublication(before, committedBefore, id);
            }
            const unchanged = category === 'unrelated-write' ? await source(base.surql) : undefined;
            const started = performance.now();
            try {
              await query(`BEGIN TRANSACTION; ${sql} COMMIT TRANSACTION;`);
              const writeAckMs = performance.now() - started;
              await waitPublication(db!, observedView.id, expected, {
                timeoutMs: args.startupTimeoutSecs * 1000,
                pollMs: 10,
                initial: false,
                signal,
              });
              const publishedReadyMs = performance.now() - started;
              if (unchanged)
                await waitPublication(db!, base.id, unchanged, {
                  timeoutMs: args.startupTimeoutSecs * 1000,
                  pollMs: 10,
                  initial: false,
                  signal,
                });
              probes.samples.push({
                name: category,
                iteration,
                writeAckMs,
                publishedReadyMs,
                observedView: observedView.id,
              });
            } catch (error) {
              probes.samples.push({
                name: category,
                iteration,
                failed: true,
                error: String(error),
              });
              throw error;
            }
          }
        }
        await memory('after-mutations', views.size);
        const load = async (name: string, rate: number, seconds: number, batch: number) => {
          const result = await phase(name);
          const pending = new Set<Promise<void>>();
          const started = performance.now();
          let completed = 0;
          let failed = 0;
          let failure: unknown;
          result.requestedRowsPerSec = rate;
          result.seconds = seconds;
          const updateTotals = () => {
            result.completedRows = completed;
            result.failedRows = failed;
            result.elapsedMs = performance.now() - started;
            result.achievedRowsPerSec =
              (completed / Math.max(1, result.elapsedMs as number)) * 1000;
          };
          const transactions = Math.ceil((rate * seconds) / batch);
          try {
            for (let transaction = 0; transaction < transactions; transaction++) {
              ensure();
              if (failure) throw failure;
              const due = started + ((transaction * batch) / rate) * 1000;
              if (due > performance.now()) await sleep(due - performance.now());
              if (pending.size >= 4) await Promise.race(pending);
              ensure();
              if (failure) throw failure;
              const records = Array.from({ length: batch }, () => {
                const game = gameRecord(++newIndex, entry.rows, !!entry.pgnPayload);
                game.database = 'game_database:main';
                game.owner = 'owner-main';
                game.sort_index = -2_000_000_000 - newIndex;
                return gameSql(game);
              });
              const writeStart = performance.now();
              const task = query(
                `BEGIN TRANSACTION; INSERT INTO game [${records.join(',')}]; COMMIT TRANSACTION;`
              )
                .then(() => {
                  completed += batch;
                  result.samples.push({
                    name: 'batch',
                    rows: batch,
                    writeAckMs: performance.now() - writeStart,
                  });
                })
                .catch((error) => {
                  failed += batch;
                  failure = error;
                  result.samples.push({
                    name: 'batch',
                    rows: batch,
                    failed: true,
                    error: String(error),
                  });
                })
                .finally(() => {
                  pending.delete(task);
                  updateTotals();
                });
              pending.add(task);
            }
            const remaining = started + seconds * 1000 - performance.now();
            if (remaining > 0) await sleep(remaining);
          } finally {
            // Drain acknowledged/failed transactions before teardown, and retain
            // achieved throughput even if this phase is interrupted or fails.
            await Promise.all(pending);
            updateTotals();
          }
          ensure();
          if (failure) throw failure;
          // Source queries are deliberately outside timed catch-up observations.
          const expectedViews = [];
          for (const view of views.values())
            expectedViews.push({ view, expected: await source(view.surql) });
          const catchupStart = performance.now();
          for (const { view, expected } of expectedViews)
            await waitPublication(db!, view.id, expected, {
              timeoutMs: args.startupTimeoutSecs * 1000,
              pollMs: 10,
              initial: false,
              signal,
            });
          result.catchupAfterOracleMs = performance.now() - catchupStart;
          result.sourceOracleMs = catchupStart - started - (result.elapsedMs as number);
          await save();
        };
        await load('steady', 50, args.profile === 'smoke' ? 2 : 60, 1);
        await load('import', 1000, args.profile === 'smoke' ? 2 : 30, 100);
        // Complete any recovery before validating the final publications.
        await memory('after-import', views.size);
        const verify = await phase('final-verification');
        for (const view of views.values()) {
          const expected = await source(view.surql);
          await waitPublication(db!, view.id, expected, {
            timeoutMs: args.startupTimeoutSecs * 1000,
            pollMs: 10,
            initial: false,
            signal,
          });
          verify.samples.push({ name: view.id, roots: expected.rootIds.length, matched: true });
        }
        // These references are intentionally included in final verification.
        verify.sharedSubscriber = duplicate.id;
        verify.filteredView = filtered.id;
        verify.retention = await retainedQueries();
        // The entire disposable stack is removed in finally. Per-view teardown
        // is enforced during scrolling; final cleanup does not depend on an
        // overloaded backend accepting more control-plane requests.
        entry.status = 'passed';
      } catch (error) {
        if (error instanceof SurrealStartupCleanupError) entry.cleanupError = String(error);
        entry.status = controller.signal.aborted
          ? 'interrupted'
          : error instanceof ResourceLimitedError || resourceFailure
            ? 'resource-limited'
            : 'failed';
        entry.error =
          resourceFailure ??
          (error instanceof Error ? (error.stack ?? error.message) : String(error));
        fs.writeFileSync(path.join(outDir, `${entry.name}-failure.log`), entry.error + '\n');
        log.warn(`[${entry.name}] ${entry.status}: ${entry.error.split('\n')[0]}`);
      } finally {
        samplingStopped = true;
        clearTimeout(pollTimer);
        await sampling?.catch(() => {});
        await Promise.all(samplers.map((sampler) => sampler.stop()));
        if (workDir) {
          const diskBytes = await directoryBytes(workDir).catch(() => undefined);
          const peak = (entity: 'scheduler' | 'ssp') =>
            samplers.reduce(
              (maximum, sampler) =>
                sampler.samples.reduce(
                  (value, sample) =>
                    sample.entity === entity ? Math.max(value, sample.rssBytes) : value,
                  maximum
                ),
              0
            );
          const observation = {
            rows: entry.rows,
            loadedRows:
              entry.rows +
              Number(entry.phases.steady?.completedRows ?? 0) +
              Number(entry.phases.import?.completedRows ?? 0),
            pgnPayload: !!entry.pgnPayload,
            diskBytes,
            hostPeakBytes: peak('scheduler') + peak('ssp'),
            dockerPeakBytes: databaseSamples.reduce(
              (maximum, sample) => Math.max(maximum, sample.memoryBytes),
              0
            ),
            basis: 'observed',
          };
          entry.observedResources = observation;
          if (
            entry.status === 'passed' &&
            diskBytes !== undefined &&
            observation.hostPeakBytes > 0 &&
            observation.dockerPeakBytes > 0
          )
            observed.push({ ...observation, diskBytes });
        }
        if (db) await withTimeout(db.close(), 1000, 'benchmark database close').catch(() => {});
        await (stack ? stack.stop() : pre?.stop())?.catch((error) => {
          entry.cleanupError = String(error);
          entry.status = 'failed';
        });
        if (workDir && !entry.cleanupError) fs.rmSync(workDir, { recursive: true, force: true });
        entry.durationMs = performance.now() - caseStart;
        await save();
      }
      // A surviving resource would invalidate the isolation of every later case.
      if (entry.cleanupError) break;
    }
  } finally {
    clearInterval(checkpoint);
    process.removeListener('SIGINT', interrupt);
    process.removeListener('SIGTERM', interrupt);
    report.status = controller.signal.aborted
      ? 'interrupted'
      : report.cases.every((entry) => entry.status === 'passed')
        ? 'passed'
        : 'failed';
    report.finishedAt = new Date().toISOString();
    await save();
    log.info(`WhitePawn benchmark report: ${path.join(outDir, 'summary.md')}`);
  }
  if (report.status !== 'passed')
    throw new Error(`WhitePawn benchmark ${report.status}; see ${path.join(outDir, 'summary.md')}`);
}
