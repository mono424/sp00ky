import fs from 'node:fs';
import path from 'node:path';
import { RecordId, type Surreal } from 'surrealdb';

export type BenchmarkStatus =
  | 'pending'
  | 'running'
  | 'passed'
  | 'failed'
  | 'resource-limited'
  | 'interrupted';

export interface PhaseSample {
  name: string;
  writeAckMs?: number;
  registerResponseMs?: number;
  publishedReadyMs?: number;
  [key: string]: unknown;
}

export interface PhaseResult {
  samples: PhaseSample[];
  [key: string]: unknown;
}

export interface CaseResult {
  name: string;
  rows: number;
  pgnPayload?: string | number | boolean;
  status: BenchmarkStatus;
  phase?: string;
  error?: string;
  phases: Record<string, PhaseResult>;
  [key: string]: unknown;
}

export interface RunReport {
  suite: string;
  startedAt: string;
  finishedAt?: string;
  profile: string;
  config: Record<string, unknown>;
  env: Record<string, unknown>;
  status?: BenchmarkStatus;
  validationIssue?: string;
  cases: CaseResult[];
  [key: string]: unknown;
}

export interface SampleSummary {
  n: number;
  invalid: number;
  min: number | null;
  max: number | null;
  mean: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
}

/** Summarize a copy: samples in reports retain their original measurement order. */
export function percentileSummary(samples: readonly number[]): SampleSummary {
  const sorted = samples.filter((n) => Number.isFinite(n) && n >= 0).sort((a, b) => a - b);
  const invalid = samples.length - sorted.length;
  if (!sorted.length)
    return { n: 0, invalid, min: null, max: null, mean: null, p50: null, p95: null, p99: null };
  const percentile = (p: number) => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]!;
  return {
    n: sorted.length,
    invalid,
    min: sorted[0]!,
    max: sorted[sorted.length - 1]!,
    mean: sorted.reduce((sum, n) => sum + n, 0) / sorted.length,
    p50: percentile(0.5),
    p95: percentile(0.95),
    p99: percentile(0.99),
  };
}

export function summarizePhase(phase: PhaseResult): Record<string, SampleSummary> {
  const metrics = new Set(
    phase.samples.flatMap((sample) => Object.keys(sample).filter((key) => key.endsWith('Ms')))
  );
  return Object.fromEntries(
    [...metrics].map((metric) => [
      metric,
      percentileSummary(
        phase.samples.flatMap((sample) =>
          sample.failed !== true && sample.status !== 'failed' && typeof sample[metric] === 'number'
            ? [sample[metric] as number]
            : []
        )
      ),
    ])
  );
}

/** Call after every phase/status update, including failure and interruption. */
export async function writeWhitePawnReport(outDir: string, report: RunReport): Promise<void> {
  fs.mkdirSync(outDir, { recursive: true });
  const summaries = report.cases.map((entry) => ({
    name: entry.name,
    status: entry.status,
    phases: Object.fromEntries(
      Object.entries(entry.phases).map(([name, phase]) => [name, summarizePhase(phase)])
    ),
  }));
  const { cases: _cases, ...meta } = report;
  // Each replacement is atomic. A killed run retains the last complete phase.
  const outputs = {
    'run.json': JSON.stringify(
      { ...meta, cases: report.cases.map(({ phases: _phases, ...entry }) => entry) },
      null,
      2
    ),
    'samples.json': JSON.stringify({ ...report, summaries }, null, 2),
    'summary.md': renderSummary(report, summaries),
  };
  for (const [name, contents] of Object.entries(outputs)) {
    const target = path.join(outDir, name);
    const temporary = `${target}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, `${contents}\n`);
    fs.renameSync(temporary, target);
  }
}

function renderSummary(
  report: RunReport,
  summaries: Array<{
    name: string;
    status: BenchmarkStatus;
    phases: Record<string, Record<string, SampleSummary>>;
  }>
): string {
  const escape = (value: unknown) =>
    String(value ?? '')
      .replace(/\|/g, '\\|')
      .replace(/\r?\n/g, ' ');
  const object = (value: unknown): Record<string, unknown> =>
    value !== null && typeof value === 'object' && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : {};
  const records = (value: unknown): Array<Record<string, unknown>> =>
    Array.isArray(value) ? value.map(object) : [];
  const numeric = (value: unknown): number | null =>
    typeof value === 'number' && Number.isFinite(value) ? value : null;
  const format = (value: unknown) => numeric(value)?.toFixed(2) ?? '—';
  const count = (value: unknown) => numeric(value)?.toLocaleString('en-US') ?? '—';
  const bytes = (value: unknown) => {
    const n = numeric(value);
    if (n === null) return '—';
    return n >= 1024 ** 3
      ? `${(n / 1024 ** 3).toFixed(2)} GiB`
      : `${(n / 1024 ** 2).toFixed(2)} MiB`;
  };
  const values = (rows: Array<Record<string, unknown>>, key: string) =>
    rows.flatMap((row) => (numeric(row[key]) === null ? [] : [row[key] as number]));
  const maximum = (rows: Array<Record<string, unknown>>, key: string) => {
    const numbers = values(rows, key);
    return numbers.length ? numbers.reduce((peak, n) => Math.max(peak, n), numbers[0]!) : null;
  };
  const latest = (rows: Array<Record<string, unknown>>, key: string) =>
    values(rows, key).at(-1) ?? null;
  const table = (title: string, headers: string[], rows: unknown[][]) => {
    lines.push(
      '',
      `## ${title}`,
      '',
      `| ${headers.join(' | ')} |`,
      `| ${headers.map(() => '---').join(' | ')} |`
    );
    for (const row of rows) lines.push(`| ${row.map(escape).join(' | ')} |`);
    if (!rows.length)
      lines.push(`| ${['Unavailable', ...headers.slice(1).map(() => '—')].join(' | ')} |`);
  };
  const lines = [
    `# ${report.suite}`,
    '',
    `Profile: ${report.profile}. Status: ${report.status ?? 'running'}. Started: ${report.startedAt}.`,
    ...(report.finishedAt ? [`Finished: ${report.finishedAt}.`] : []),
    ...(report.validationIssue
      ? ['', `**VALIDATION ISSUE: ${escape(report.validationIssue)}**`]
      : []),
    '',
    '| Case | Game rows | PGN payload | Status | Last phase | Error |',
    '| --- | ---: | --- | --- | --- | --- |',
    ...report.cases.map(
      (entry) =>
        `| ${escape(entry.name)} | ${entry.rows} | ${escape(entry.pgnPayload ?? 'metadata only')} | ${entry.status} | ${escape(entry.phase)} | ${escape(entry.error)} |`
    ),
    '',
    `Latency is in milliseconds. Published readiness verifies committed list-ref membership, child parents and durable source record versions. Publication polls run every ${format(report.config.pollMs ?? 10)} ms plus database request time, so readiness measurements include polling quantization. Raw samples preserve measurement order; percentiles use nearest rank. Failed samples are excluded from latency percentiles. Empty samples are unavailable, not zero latency.`,
    '',
    'Write acknowledgment measures the database transaction response; it does not measure sync propagation. Published readiness runs from before registration or mutation until the publication oracle matches. Load throughput measures committed source rows per elapsed write interval. Residual catch-up starts after the source window oracle finishes, so source oracle time is reported separately and the sync pipeline can drain during that oracle.',
    '',
    '| Case | Phase / category | Metric | n | Failed samples | Invalid | p50 | p95 | p99 | Min | Max |',
    '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
  ];
  for (const [index, entry] of summaries.entries()) {
    for (const [phase, metrics] of Object.entries(entry.phases)) {
      const rawPhase = report.cases[index]!.phases[phase]!;
      const failed = rawPhase.samples.filter(
        (sample) => sample.failed === true || sample.status === 'failed'
      ).length;
      for (const [metric, values] of Object.entries(metrics)) {
        lines.push(
          `| ${escape(entry.name)} | ${escape(phase)} | ${escape(metric)} | ${values.n} | ${failed} | ${values.invalid} | ${format(values.p50)} | ${format(values.p95)} | ${format(values.p99)} | ${format(values.min)} | ${format(values.max)} |`
        );
      }
      if (!Object.keys(metrics).length && failed)
        lines.push(
          `| ${escape(entry.name)} | ${escape(phase)} | unavailable | 0 | ${failed} | 0 | — | — | — | — | — |`
        );
      if (phase === 'mutations') {
        for (const category of new Set(rawPhase.samples.map((sample) => sample.name))) {
          const samples = rawPhase.samples.filter((sample) => sample.name === category);
          const categoryFailed = samples.filter(
            (sample) => sample.failed === true || sample.status === 'failed'
          ).length;
          for (const [metric, values] of Object.entries(summarizePhase({ samples }))) {
            lines.push(
              `| ${escape(entry.name)} | ${escape(`${phase} / ${category}`)} | ${escape(metric)} | ${values.n} | ${categoryFailed} | ${values.invalid} | ${format(values.p50)} | ${format(values.p95)} | ${format(values.p99)} | ${format(values.min)} | ${format(values.max)} |`
            );
          }
        }
      }
    }
  }

  table(
    'Dataset and startup',
    [
      'Case',
      'Games',
      'Players',
      'Collections',
      'Main games',
      'Serialized fixture',
      'Seed ms',
      'Database ready ms',
      'Scheduler ready ms',
      'SSP ready ms',
      'Backend ready ms',
    ],
    report.cases.map((entry) => {
      const seed = object(entry.seed);
      const startup = object(entry.startup);
      return [
        entry.name,
        count(seed.games),
        count(seed.players),
        count(seed.collections),
        count(seed.mainCount),
        bytes(seed.serializedBytes),
        format(entry.seedMs),
        format(startup.databaseReadyMs),
        format(startup.schedulerReadyMs),
        format(startup.sspReadyMs),
        format(startup.backendReadyMs),
      ];
    })
  );
  table(
    'Bootstrap database counts',
    ['Case', 'Source game records', '`_00_version` metadata records'],
    report.cases.map((entry) => {
      const statements = Array.isArray(entry.databaseCounts) ? entry.databaseCounts : [];
      return [
        entry.name,
        count(records(statements[0])[0]?.count),
        count(records(statements[1])[0]?.count),
      ];
    })
  );
  lines.push(
    '',
    'These counts are read after bootstrap, before timed mutations. Durable version metadata is counted separately from source games and also covers the player and collection records.'
  );
  table(
    'Write throughput and residual catch-up',
    [
      'Case',
      'Phase',
      'Target rows/s',
      'Achieved rows/s',
      'Committed rows',
      'Failed rows',
      'Write interval ms',
      'Residual catch-up ms',
      'Source oracle ms',
    ],
    report.cases.flatMap((entry) =>
      ['steady', 'import'].flatMap((phase) => {
        const value = entry.phases[phase];
        return value
          ? [
              [
                entry.name,
                phase,
                format(value.requestedRowsPerSec),
                format(value.achievedRowsPerSec),
                count(value.completedRows),
                count(value.failedRows),
                format(value.elapsedMs),
                format(value.catchupAfterOracleMs),
                format(value.sourceOracleMs),
              ],
            ]
          : [];
      })
    )
  );

  const processRows: unknown[][] = [];
  for (const entry of report.cases) {
    for (const process of records(entry.processSamples)) {
      const samples = records(process.samples);
      const cpu = percentileSummary(values(samples, 'cpuPercent'));
      processRows.push([
        entry.name,
        process.entity,
        samples.length,
        bytes(maximum(samples, 'rssBytes')),
        format(cpu.mean),
        format(cpu.max),
      ]);
    }
    const database = records(entry.databaseSamples);
    if (database.length) {
      const cpu = percentileSummary(values(database, 'cpuPercent'));
      processRows.push([
        entry.name,
        'database container',
        database.length,
        bytes(maximum(database, 'memoryBytes')),
        format(cpu.mean),
        format(cpu.max),
      ]);
    }
  }
  table(
    'Process memory and CPU',
    ['Case', 'Process', 'Samples', 'Peak RSS / container memory', 'Mean CPU %', 'Peak CPU %'],
    processRows
  );
  lines.push(
    '',
    'Scheduler and SSP RSS and CPU come from OS process samples; database memory and CPU come from Docker. These are sampled peaks, and CPU percentages are OS/container observations rather than per-request costs.'
  );
  table(
    'Disk and resource observations',
    [
      'Case',
      'Measurement',
      'Work directory disk',
      'Sum of process RSS peaks',
      'Database memory peak',
      'Basis',
    ],
    report.cases.flatMap((entry) => {
      const resource = object(entry.observedResources);
      const estimate = object(object(entry.resources).estimate);
      return [
        [
          entry.name,
          'estimate',
          bytes(estimate.diskBytes),
          bytes(estimate.hostPeakBytes),
          bytes(estimate.dockerPeakBytes),
          estimate.basis ?? '—',
        ],
        ...(Object.keys(resource).length
          ? [
              [
                entry.name,
                'observed',
                bytes(resource.diskBytes),
                bytes(resource.hostPeakBytes),
                bytes(resource.dockerPeakBytes),
                resource.basis ?? 'observed',
              ],
            ]
          : []),
      ];
    })
  );
  lines.push(
    '',
    'Disk covers the isolated database, scheduler and SSP work directory. The sum of individual process RSS peaks is conservative and does not imply both peaks occurred at the same instant. Missing observations remain unavailable on partial runs.'
  );

  const schedulerRows: unknown[][] = [];
  const publicationRows: unknown[][] = [];
  for (const entry of report.cases) {
    const samples = records(entry.schedulerSamples);
    const scheduler = samples.map((sample) => object(object(sample.metrics).scheduler));
    schedulerRows.push([
      entry.name,
      samples.length,
      count(maximum(scheduler, 'pending_events')),
      count(latest(scheduler, 'pending_events')),
      count(maximum(scheduler, 'lag')),
      count(latest(scheduler, 'lag')),
      count(maximum(scheduler, 'total_queries')),
      count(latest(scheduler, 'ready_ssps')),
      samples.filter((sample) => sample.error !== undefined).length,
    ]);
    const publication = samples.flatMap((sample) => {
      const info = Array.isArray(sample.info)
        ? records(sample.info)
        : records(object(sample.info).entities);
      const ssps = info.filter((entity) => entity.entity === 'ssp');
      const queues = ssps
        .map((entity) => object(entity.publication))
        .filter((queue) => Object.keys(queue).length);
      return queues.length
        ? [
            Object.fromEntries(
              [
                'pending_batches',
                'pending_operations',
                'pending_bytes',
                'parked_batches',
                'overload_total',
              ]
                .map((key) => [key, values(queues, key).reduce((sum, n) => sum + n, 0)])
                .concat([['oldest_age_ms', maximum(queues, 'oldest_age_ms') ?? 0]])
            ),
          ]
        : [];
    });
    publicationRows.push([
      entry.name,
      publication.length,
      count(maximum(publication, 'pending_batches')),
      count(maximum(publication, 'pending_operations')),
      bytes(maximum(publication, 'pending_bytes')),
      format(maximum(publication, 'oldest_age_ms')),
      count(maximum(publication, 'parked_batches')),
      count(maximum(publication, 'overload_total')),
    ]);
  }
  table(
    'Scheduler backlog',
    [
      'Case',
      'Samples',
      'Peak pending events',
      'Last pending events',
      'Peak sequence lag',
      'Last sequence lag',
      'Peak tracked views',
      'Last ready SSPs',
      'Sampling errors',
    ],
    schedulerRows
  );
  table(
    'SSP publication backlog',
    [
      'Case',
      'Info observations',
      'Peak pending batches',
      'Peak operations',
      'Peak pending bytes',
      'Oldest pending age ms',
      'Peak parked batches',
      'Overloads observed',
    ],
    publicationRows
  );
  lines.push(
    '',
    'Backlog values are sampled independently of phase completion; the last telemetry sample can precede final verification. Successful window verification does not assert that scheduler or publication queues are empty. Publication queue values sum SSP queues per observation, then take the peak; oldest age takes the maximum.'
  );

  table(
    'Durable retained-query verification',
    ['Case', 'Phase', 'Metadata queries', 'Maximum', 'Overflow', 'Polls', 'Oracle ms'],
    report.cases.flatMap((entry) =>
      Object.entries(entry.phases).flatMap(([phase, value]) => {
        const retention = object(value.retention);
        const snapshot = object(retention.snapshot);
        return Array.isArray(snapshot.queryIds)
          ? [
              [
                entry.name,
                phase,
                count(snapshot.queryIds.length),
                MAX_RETAINED_QUERIES,
                snapshot.overflow,
                count(retention.polls),
                format(retention.elapsedMs),
              ],
            ]
          : [];
      })
    )
  );
  lines.push(
    '',
    'Each completed retention oracle verifies the exact metadata ID set against retained driver registrations, with a bounded 21-row read and a maximum of 20 queries. This catches evicted metadata that could restore ghost views during bootstrap. Healthy physical graph and merged-follower checkpoints separately verify the active registration count; counts alone do not establish metadata membership.'
  );

  table(
    'SSP memory attribution',
    [
      'Case',
      'Checkpoint',
      'Shared store',
      'Query/operator state',
      'Total estimate',
      'Graphs',
      'Merged followers',
      'Merging',
    ],
    report.cases.flatMap((entry) =>
      records(entry.memoryMeasurements).map((measurement) => {
        const value = object(measurement.value);
        return [
          entry.name,
          measurement.label,
          bytes(value.store_bytes),
          bytes(value.query_bytes),
          bytes(value.total_bytes),
          count(value.graphs),
          count(value.subscribers),
          value.merging ?? '—',
        ];
      })
    )
  );
  lines.push(
    '',
    'SSP attribution estimates heap ownership; use checkpoint deltas to compare store and operator growth. It is separate from measured process RSS. Graphs count distinct operator computations. The raw subscribers field counts merged followers attached to another registration’s graph and excludes graph owners; zero followers can therefore accompany many active graphs.'
  );
  table(
    'Identical-query graph sharing',
    [
      'Case',
      'Graphs before',
      'Graphs after',
      'Merged followers before',
      'Merged followers after',
      'Graphs after import',
      'Merged followers after import',
    ],
    report.cases.flatMap((entry) => {
      const value = object(entry.phases['filtered-and-shared']?.sharing);
      const afterImport = object(
        records(entry.memoryMeasurements)
          .filter((row) => row.label === 'after-import')
          .at(-1)?.value
      );
      return Object.keys(value).length
        ? [
            [
              entry.name,
              count(value.graphsBefore),
              count(value.graphsAfter),
              count(value.subscribersBefore),
              count(value.subscribersAfter),
              count(afterImport.graphs),
              count(afterImport.subscribers),
            ],
          ]
        : [];
    })
  );
  for (const entry of report.cases) {
    const initial = object(entry.phases['filtered-and-shared']?.sharing);
    const afterImport = object(
      records(entry.memoryMeasurements)
        .filter((row) => row.label === 'after-import')
        .at(-1)?.value
    );
    const initialFollowers = numeric(initial.subscribersAfter);
    const laterFollowers = numeric(afterImport.subscribers);
    if (
      initialFollowers !== null &&
      initialFollowers > 0 &&
      laterFollowers !== null &&
      laterFollowers < initialFollowers
    ) {
      const initialPassed =
        numeric(initial.graphsBefore) !== null &&
        initial.graphsBefore === initial.graphsAfter &&
        numeric(initial.subscribersBefore) !== null &&
        initialFollowers === (initial.subscribersBefore as number) + 1;
      lines.push(
        '',
        `${escape(entry.name)}: ${initialPassed ? 'Initial graph sharing passed; merged' : 'Merged'} followers decreased after import from ${count(initialFollowers)} to ${count(laterFollowers)}. Later memory measurements include the observed ${count(afterImport.graphs)} graphs. The counters establish the change in sharing; they do not identify its cause.`
      );
    }
  }
  lines.push(
    '',
    'These measurements cover the local sync pipeline. Browser rendering and client download time are outside the measured interval.'
  );
  return lines.join('\n');
}

export const MAX_PUBLICATION_EDGES = 151;
export const MAX_RETAINED_QUERIES = 20;
const LIST_REF_TABLE = '_00_list_ref_user_bench';
type Database = Pick<Surreal, 'query'>;

export interface RetainedQueriesSnapshot {
  queryIds: string[];
  overflow: boolean;
}

export interface RetainedQueriesObservation {
  snapshot: RetainedQueriesSnapshot;
  polls: number;
  elapsedMs: number;
}

function queryRecordString(value: string | RecordId): string {
  const id = recordString(value);
  if (!id.includes(':')) return new RecordId('_00_query', id).toString();
  if (!id.startsWith('_00_query:') || id === '_00_query:') {
    throw new Error(`Invalid retained query record id: ${id}`);
  }
  return id;
}

/** The isolated case database must contain exactly the driver's retained registrations. */
export function retainedQueriesMatch(
  actual: RetainedQueriesSnapshot,
  expectedIds: readonly (string | RecordId)[]
): { matches: boolean; reasons: string[] } {
  if (expectedIds.length > MAX_RETAINED_QUERIES) {
    throw new Error(`Expected retained queries exceed the ${MAX_RETAINED_QUERIES}-query bound`);
  }
  const wanted = expectedIds.map(queryRecordString);
  if (new Set(wanted).size !== wanted.length) {
    throw new Error('Expected retained query ids contain duplicates');
  }
  const found = new Set(actual.queryIds);
  const expected = new Set(wanted);
  const reasons: string[] = [];
  if (actual.overflow || actual.queryIds.length > MAX_RETAINED_QUERIES) {
    reasons.push('Retained query metadata reached the 21-row overflow sentinel');
  }
  if (found.size !== actual.queryIds.length) reasons.push('Duplicate retained query metadata ids');
  if (found.size !== expected.size || [...found].some((id) => !expected.has(id))) {
    reasons.push('Retained query metadata differs from the driver registrations');
  }
  return { matches: reasons.length === 0, reasons };
}

/** Read at most 21 metadata ids; a sentinel detects historical ghost registrations. */
export async function readRetainedQueries(
  db: Database,
  opts: { timeoutMs?: number; signal?: AbortSignal } = {}
): Promise<RetainedQueriesSnapshot> {
  const timeoutMs = opts.timeoutMs ?? 5000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error('Retained query metadata timeout must be finite and positive');
  }
  opts.signal?.throwIfAborted();
  const [ids] = await bounded(
    db.query<[Array<string | RecordId>]>(
      'SELECT VALUE id FROM _00_query LIMIT $limit TIMEOUT 5s;',
      { limit: MAX_RETAINED_QUERIES + 1 }
    ),
    timeoutMs,
    opts.signal,
    'Retained query metadata request deadline exceeded'
  );
  if (!Array.isArray(ids)) throw new Error('Retained query metadata oracle did not return ids');
  return {
    queryIds: ids.map(queryRecordString),
    overflow: ids.length > MAX_RETAINED_QUERIES,
  };
}

export async function waitRetainedQueries(
  db: Database,
  expectedIds: readonly (string | RecordId)[],
  opts: { timeoutMs: number; pollMs: number; signal?: AbortSignal }
): Promise<RetainedQueriesObservation> {
  if (
    !Number.isFinite(opts.timeoutMs) ||
    opts.timeoutMs <= 0 ||
    !Number.isFinite(opts.pollMs) ||
    opts.pollMs <= 0
  ) {
    throw new Error(
      'Retained query metadata timeout and poll interval must be finite and positive'
    );
  }
  // Reject invalid expectations before entering the retry loop or issuing reads.
  retainedQueriesMatch({ queryIds: [], overflow: false }, expectedIds);
  const started = performance.now();
  const deadline = started + opts.timeoutMs;
  let polls = 0;
  let lastReason = 'No retained query metadata observed';
  while (performance.now() < deadline) {
    opts.signal?.throwIfAborted();
    try {
      const snapshot = await readRetainedQueries(db, {
        timeoutMs: deadline - performance.now(),
        signal: opts.signal,
      });
      polls++;
      const result = retainedQueriesMatch(snapshot, expectedIds);
      if (result.matches) return { snapshot, polls, elapsedMs: performance.now() - started };
      lastReason = result.reasons.join('; ');
    } catch (error) {
      opts.signal?.throwIfAborted();
      lastReason = error instanceof Error ? error.message : String(error);
    }
    const remaining = deadline - performance.now();
    if (remaining > 0) {
      try {
        await bounded(
          new Promise<void>((resolve) => setTimeout(resolve, Math.min(opts.pollMs, remaining))),
          remaining,
          opts.signal
        );
      } catch {
        opts.signal?.throwIfAborted();
      }
    }
  }
  throw new Error(`Retained query metadata timed out after ${opts.timeoutMs}ms: ${lastReason}`);
}

export interface ExpectedPublication {
  rootIds: string[];
  childIds: string[];
  versions: Record<string, number>;
  rows?: Array<{ id: string; [key: string]: unknown }>;
  /** A shared child has one physical edge; any correct referencing parent/alias is valid. */
  childLinks?: Record<string, Array<{ rootId: string; alias: string }>>;
}

export interface PublishedEdge {
  id: string;
  out: string;
  version: number;
  parent?: string | null;
  parentRel?: string | null;
}

export interface PublishedSnapshot {
  state: string | null;
  rowCount: number | null;
  edges: PublishedEdge[];
  overflow: boolean;
}

/** Require a new touched-record publication beyond both pre-write baselines. */
export function expectMutationPublication(
  before: ExpectedPublication,
  committedBefore: PublishedSnapshot,
  touchedId: string
): ExpectedPublication {
  const sourceVersion = before.versions[touchedId];
  if (!Number.isSafeInteger(sourceVersion) || sourceVersion! < 1) {
    throw new Error(`Missing source mutation baseline for ${touchedId}`);
  }
  const edges = committedBefore.edges.filter((edge) => edge.out === touchedId);
  if (edges.length !== 1) {
    throw new Error(
      `Expected one committed mutation baseline edge for ${touchedId}, found ${edges.length}`
    );
  }
  const committedVersion = edges[0]!.version;
  if (!Number.isSafeInteger(committedVersion) || committedVersion < 1) {
    throw new Error(`Invalid committed mutation baseline version for ${touchedId}`);
  }
  const targetVersion = Math.max(sourceVersion!, committedVersion) + 1;
  if (!Number.isSafeInteger(targetVersion)) {
    throw new Error(`Mutation baseline version cannot advance safely for ${touchedId}`);
  }
  return { ...before, versions: { ...before.versions, [touchedId]: targetVersion } };
}

function recordString(value: unknown): string {
  if (typeof value === 'string') return value;
  if (value instanceof RecordId) return value.toString();
  throw new Error(`Expected a record id, received ${String(value)}`);
}

function record(value: string): RecordId {
  const colon = value.indexOf(':');
  if (colon <= 0 || colon === value.length - 1)
    throw new Error(`Invalid benchmark record id: ${value}`);
  return new RecordId(value.slice(0, colon), value.slice(colon + 1));
}

/** Run outside timed measurement intervals: the source query itself is the oracle. */
export async function snapshotExpected(
  db: Database,
  surql: string,
  bindings?: Record<string, unknown>
): Promise<ExpectedPublication> {
  const response = await db.query<[Array<Record<string, unknown>>]>(surql, bindings);
  const rows = response[0];
  if (!Array.isArray(rows)) throw new Error('Window oracle did not return rows');
  return expectedFromRows(db, rows);
}

/** Build a future expectation before a timed write; supplied versions override source reads. */
export async function expectedFromRows(
  db: Database,
  sourceRows: Array<Record<string, unknown>>,
  knownVersions: Record<string, number> = {}
): Promise<ExpectedPublication> {
  if (sourceRows.length > 50)
    throw new Error('Window oracle exceeded the 50-row publication bound');
  const rows = sourceRows.map((row) => {
    const normalized: { id: string; [key: string]: unknown } = { ...row, id: recordString(row.id) };
    for (const alias of ['white', 'black', 'white_player', 'black_player']) {
      const child = row[alias];
      if (child && typeof child === 'object' && !(child instanceof RecordId) && 'id' in child) {
        normalized[alias] = { ...child, id: recordString(child.id) };
      }
    }
    return normalized;
  });
  const rootIds = rows.map((row) => row.id);
  if (new Set(rootIds).size !== rootIds.length)
    throw new Error('Window oracle returned duplicate game ids');
  const childLinks: NonNullable<ExpectedPublication['childLinks']> = {};
  for (const row of rows) {
    const rootId = recordString(row.id);
    for (const alias of ['white', 'black', 'white_player', 'black_player']) {
      const child = row[alias];
      if (!child || typeof child !== 'object' || child instanceof RecordId || !('id' in child))
        continue;
      const id = recordString(child.id);
      (childLinks[id] ??= []).push({ rootId, alias });
    }
  }
  const childIds = Object.keys(childLinks);
  const ids = [...new Set([...rootIds, ...childIds])];
  const versions: Record<string, number> = Object.fromEntries(
    ids.filter((id) => knownVersions[id] !== undefined).map((id) => [id, knownVersions[id]!])
  );
  const missing = ids.filter((id) => versions[id] === undefined);
  if (missing.length) {
    const [values] = await db.query<[Array<{ record_id: unknown; version: number }>]>(
      'SELECT record_id, version FROM _00_version WHERE record_id IN $records LIMIT $limit TIMEOUT 30s;',
      { records: missing.map(record), limit: missing.length + 1 }
    );
    for (const value of values) versions[recordString(value.record_id)] = value.version;
  }
  for (const id of ids) {
    if (!Number.isSafeInteger(versions[id]) || versions[id]! < 1)
      throw new Error(`Missing durable source version for ${id}`);
  }
  return { rootIds, childIds, versions, childLinks, rows };
}

/** Graph adjacency and a sentinel row bound the read even if publication is broken. */
export async function readPublication(
  db: Database,
  queryId: string,
  maxEdges = MAX_PUBLICATION_EDGES
): Promise<PublishedSnapshot> {
  if (!Number.isSafeInteger(maxEdges) || maxEdges < 1 || maxEdges > MAX_PUBLICATION_EDGES)
    throw new Error('Invalid publication edge bound');
  // Match ssp-node's format_incantation_id, which publishes the last id segment.
  const bareId = queryId.slice(queryId.lastIndexOf(':') + 1);
  const [metadata, rows] = await db.query<
    [Array<{ state?: string; rowCount?: number }>, Array<Record<string, unknown>>]
  >(
    `SELECT state, rowCount FROM $query TIMEOUT 5s; SELECT id, out, version, parent, parent_rel FROM $query->${LIST_REF_TABLE} LIMIT $limit TIMEOUT 5s;`,
    { query: new RecordId('_00_query', bareId), limit: maxEdges }
  );
  return {
    state: metadata[0]?.state ?? null,
    rowCount: metadata[0]?.rowCount ?? null,
    overflow: rows.length >= maxEdges,
    edges: rows.map((row) => ({
      id: recordString(row.id),
      out: recordString(row.out),
      version: Number(row.version),
      parent: row.parent == null ? null : recordString(row.parent),
      parentRel: typeof row.parent_rel === 'string' ? row.parent_rel : null,
    })),
  };
}

export function publicationMatches(
  actual: PublishedSnapshot,
  expected: ExpectedPublication,
  opts: { initial?: boolean } = {}
): { matches: boolean; reasons: string[] } {
  const reasons: string[] = [];
  if (actual.overflow) reasons.push('Published edges reached the overflow sentinel');
  if (actual.state === null) reasons.push('Published query metadata is missing');
  if (opts.initial && actual.state !== 'ready')
    reasons.push(`Initial publication state is ${actual.state ?? 'missing'}`);
  if (opts.initial && actual.rowCount !== expected.rootIds.length)
    reasons.push('Initial publication rowCount differs from source window');
  const roots = actual.edges.filter((edge) => !edge.parent && !edge.parentRel);
  const children = actual.edges.filter((edge) => !!edge.parent || !!edge.parentRel);
  const compareIds = (label: string, found: string[], wanted: string[]) => {
    const actualIds = new Set(found);
    const expectedIds = new Set(wanted);
    if (actualIds.size !== found.length) reasons.push(`Duplicate ${label} edges`);
    if (actualIds.size !== expectedIds.size || [...actualIds].some((id) => !expectedIds.has(id)))
      reasons.push(`Published ${label} membership differs from source window`);
  };
  compareIds(
    'root',
    roots.map((edge) => edge.out),
    expected.rootIds
  );
  compareIds(
    'child',
    children.map((edge) => edge.out),
    expected.childIds
  );
  const rootsByEdgeId = new Map(roots.map((edge) => [edge.id, edge.out]));
  for (const edge of actual.edges) {
    const wantedVersion = expected.versions[edge.out];
    if (
      !Number.isSafeInteger(wantedVersion) ||
      !Number.isSafeInteger(edge.version) ||
      edge.version < wantedVersion!
    )
      reasons.push(`Stale or missing published version for ${edge.out}`);
  }
  for (const edge of children) {
    const parentRoot = edge.parent ? rootsByEdgeId.get(edge.parent) : undefined;
    if (!parentRoot || !edge.parentRel) {
      reasons.push(`Invalid published parent for ${edge.out}`);
      continue;
    }
    const validLinks = expected.childLinks?.[edge.out];
    if (
      validLinks &&
      !validLinks.some((link) => link.rootId === parentRoot && link.alias === edge.parentRel)
    )
      reasons.push(`Incorrect published parent or alias for ${edge.out}`);
  }
  return { matches: reasons.length === 0, reasons };
}

export interface WaitPublicationOptions {
  timeoutMs: number;
  pollMs: number;
  initial?: boolean;
  signal?: AbortSignal;
}

export interface PublicationObservation {
  snapshot: PublishedSnapshot;
  polls: number;
  elapsedMs: number;
}

export async function waitPublication(
  db: Database,
  queryId: string,
  expected: ExpectedPublication,
  opts: WaitPublicationOptions
): Promise<PublicationObservation> {
  if (!(opts.timeoutMs > 0) || !(opts.pollMs > 0))
    throw new Error('Publication timeout and poll interval must be positive');
  const started = performance.now();
  const deadline = started + opts.timeoutMs;
  let polls = 0;
  let lastReason = 'No publication observed';
  while (performance.now() < deadline) {
    opts.signal?.throwIfAborted();
    try {
      const snapshot = await bounded(
        readPublication(db, queryId),
        deadline - performance.now(),
        opts.signal
      );
      polls++;
      const result = publicationMatches(snapshot, expected, opts);
      if (result.matches) return { snapshot, polls, elapsedMs: performance.now() - started };
      lastReason = result.reasons.join('; ');
    } catch (error) {
      opts.signal?.throwIfAborted();
      lastReason = error instanceof Error ? error.message : String(error);
    }
    const remaining = deadline - performance.now();
    if (remaining > 0) {
      try {
        await bounded(
          new Promise<void>((resolve) => setTimeout(resolve, Math.min(opts.pollMs, remaining))),
          remaining,
          opts.signal
        );
      } catch {
        opts.signal?.throwIfAborted();
      }
    }
  }
  throw new Error(`Publication timed out after ${opts.timeoutMs}ms for ${queryId}: ${lastReason}`);
}

function bounded<T>(
  operation: PromiseLike<T>,
  timeoutMs: number,
  signal?: AbortSignal,
  timeoutMessage = 'Publication request deadline exceeded'
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      signal?.removeEventListener('abort', onAbort);
      callback();
    };
    const onAbort = () =>
      finish(() => reject(signal?.reason ?? new Error('Benchmark interrupted')));
    const timeout = setTimeout(
      () => finish(() => reject(new Error(timeoutMessage))),
      Math.max(0, timeoutMs)
    );
    signal?.addEventListener('abort', onAbort, { once: true });
    Promise.resolve(operation).then(
      (value) => finish(() => resolve(value)),
      (error) => finish(() => reject(error))
    );
    if (signal?.aborted) onAbort();
  });
}
