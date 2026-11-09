// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  axisLabels,
  changePoints,
  defaultOpen,
  formatBuildTime,
  formatDelta,
  median,
  niceTicks,
  quantile,
  reuse,
  rollingMedian,
  tile,
  trendModel,
  visibleRows,
  waterfall,
  type BuildPhases,
  type BuildPoint,
  type RunStep,
} from '../src/console/buildStats.ts';

const MIN = 60_000;
const NONE: BuildPhases = { queue: 0, source: 0, build: 0, infra: 0, migrate: 0, rollout: 0 };

let seq = 0;
function point(total: number, over: Partial<BuildPoint> = {}): BuildPoint {
  seq += 1;
  return {
    id: `b${seq}`,
    kind: 'build',
    status: 'success',
    version: seq,
    source: 'git',
    commit_sha: `sha${seq}`,
    commit_message: `commit ${seq}`,
    commit_author: 'mira',
    error: null,
    created_at: new Date(Date.UTC(2026, 9, 1, 0, seq)).toISOString(),
    total_ms: total,
    phases: { ...NONE, build: total },
    detailed: false,
    apps: [],
    apps_built: 0,
    apps_reused: 0,
    layers: 0,
    layers_cached: 0,
    ...over,
  };
}

test('median and quantile interpolate and survive nothing', () => {
  assert.equal(median([3, 1, 2]), 2);
  assert.equal(median([1, 2, 3, 4]), 2.5);
  assert.equal(median([]), 0);
  assert.equal(quantile([0, 10], 0.9), 9);
});

test('the rolling median is each build and three on each side', () => {
  assert.deepEqual(rollingMedian([1, 100, 3, 4, 5, 6, 7]), [3.5, 4, 4.5, 5, 5.5, 5, 5.5]);
});

test('durations read like durations', () => {
  assert.equal(formatBuildTime(850), '850ms');
  assert.equal(formatBuildTime(3200), '3.2s');
  assert.equal(formatBuildTime(38_000), '38s');
  assert.equal(formatBuildTime(252_000), '4m 12s');
  assert.equal(formatBuildTime(240_000), '4m');
  assert.equal(formatBuildTime(4_320_000), '1h 12m');
  assert.equal(formatDelta(-64_000), '-1m 4s');
  assert.equal(formatDelta(38_000), '+38s');
});

test('ticks are round and cover the top', () => {
  assert.deepEqual(niceTicks(11 * MIN), [0, 5 * MIN, 10 * MIN, 15 * MIN]);
  assert.deepEqual(niceTicks(0), [0]);
  const big = niceTicks(9 * 3_600_000);
  assert.ok(big[big.length - 1] >= 9 * 3_600_000);
});

test('one wedged build cannot flatten the scale, and failures stay off the line', () => {
  const points = [...Array.from({ length: 40 }, () => point(5 * MIN)), point(120 * MIN), point(2 * MIN, { status: 'failed' })];
  const model = trendModel(points, 'total');
  assert.ok(model.yMax <= 15 * MIN, `yMax ${model.yMax}`);
  assert.equal(model.line[41], null);
  assert.equal(model.line[0], 5 * MIN);
});

test('a step change is found, on the build it happened, and explained', () => {
  const before = Array.from({ length: 12 }, () =>
    point(10 * MIN, { detailed: true, phases: { ...NONE, build: 8 * MIN, rollout: 2 * MIN }, apps: [{ name: 'web', ms: 5 * MIN, status: 'ok' }, { name: 'api', ms: 3 * MIN, status: 'ok' }] }),
  );
  const after = Array.from({ length: 12 }, () =>
    point(5 * MIN, { detailed: true, phases: { ...NONE, build: 3 * MIN, rollout: 2 * MIN }, apps: [{ name: 'web', ms: 30_000, status: 'ok' }, { name: 'api', ms: 3 * MIN, status: 'ok' }] }),
  );
  const found = changePoints([...before, ...after]);
  assert.equal(found.length, 1);
  assert.equal(found[0].id, after[0].id);
  assert.equal(found[0].beforeMs, 10 * MIN);
  assert.equal(found[0].afterMs, 5 * MIN);
  assert.equal(found[0].pct, -0.5);
  assert.deepEqual(found[0].because.map((b) => b.label), ['Build', 'web']);
});

test('noise and a single slow build are not steps', () => {
  const noisy = Array.from({ length: 30 }, (_, i) => point(10 * MIN + (i % 2 ? 20_000 : -20_000)));
  noisy[15] = point(40 * MIN);
  assert.deepEqual(changePoints(noisy), []);
});

test('a project that always swings by minutes needs a bigger swing to count', () => {
  const swings = [9, 11.5, 8, 12, 9.5, 11, 8.5, 12, 9, 11.5, 8.5, 12, 9, 11, 8, 12, 9.5, 11.5, 8, 12, 9, 11, 8.5, 12];
  assert.deepEqual(changePoints(swings.map((m) => point(m * MIN))), []);
  const real = [...swings, ...swings.map((m) => m + 14)];
  const found = changePoints(real.map((m) => point(m * MIN)));
  assert.equal(found.length, 1);
  assert.ok(found[0].deltaMs > 10 * MIN);
});

test('failed builds cannot cause or hide a step', () => {
  const points = [
    ...Array.from({ length: 10 }, () => point(10 * MIN)),
    ...Array.from({ length: 6 }, () => point(MIN, { status: 'failed' })),
    ...Array.from({ length: 10 }, () => point(10 * MIN)),
  ];
  assert.deepEqual(changePoints(points), []);
});

test('too few builds claim nothing', () => {
  assert.deepEqual(changePoints([point(MIN), point(9 * MIN)]), []);
});

test('a tile compares only successful builds with the window before', () => {
  const t = tile([point(4 * MIN), point(4 * MIN), point(50 * MIN, { status: 'failed' })], [point(8 * MIN)], 'total');
  assert.equal(t.medianMs, 4 * MIN);
  assert.equal(t.deltaPct, -0.5);
  const first = tile([point(4 * MIN)], [], 'total');
  assert.equal(first.deltaPct, null);
  assert.equal(first.priorMedianMs, null);
});

test('reuse only counts builds that recorded it', () => {
  const r = reuse([point(MIN, { detailed: true, apps_built: 1, apps_reused: 3, layers: 10, layers_cached: 5 }), point(MIN)]);
  assert.equal(r.apps, 0.75);
  assert.equal(r.layers, 0.5);
  assert.deepEqual(reuse([point(MIN)]), { apps: null, layers: null });
});

test('axis labels skip repeated days and crowding', () => {
  const day = (d: number, h: number) => new Date(Date.UTC(2026, 9, d, h)).toISOString();
  const points = [point(1, { created_at: day(1, 1) }), point(1, { created_at: day(1, 2) }), point(1, { created_at: day(2, 1) }), point(1, { created_at: day(3, 1) }), point(1, { created_at: day(4, 1) })];
  assert.deepEqual(axisLabels(points, 2), [0, 2, 4]);
});

function run(): { run: BuildPoint; steps: RunStep[] } {
  const created = Date.UTC(2026, 9, 1);
  const at = (s: number) => new Date(created + s * 1000).toISOString();
  const r = point(100_000, { created_at: at(0), detailed: true, phases: { ...NONE, queue: 10_000, source: 10_000, build: 60_000, rollout: 20_000 } });
  const step = (id: number, phase: string, name: string, s: number, d: number | null, over: Partial<RunStep> = {}): RunStep => ({
    id, parent_id: null, phase, step: name, app: null, name, status: 'ok', started_at: at(s), duration_ms: d, ...over,
  });
  return {
    run: r,
    steps: [
      step(1, 'source', 'clone', 10, 8000),
      step(2, 'build', 'app', 20, 50_000, { app: 'web', name: 'web' }),
      step(3, 'build', 'docker_build', 22, 45_000, { app: 'web', parent_id: 2 }),
      step(4, 'build', 'layer', 23, 40_000, { app: 'web', parent_id: 3, name: 'RUN pnpm build' }),
      step(5, 'build', 'layer', 23, 0, { app: 'web', parent_id: 3, name: 'COPY . .', status: 'cached' }),
      step(6, 'build', 'app', 70, 1000, { app: 'api', name: 'api', status: 'reused' }),
    ],
  };
}

test('the waterfall nests phases, images, builds and layers at real offsets', () => {
  const { run: r, steps } = run();
  const rows = waterfall(r, steps, [], { build: 30_000 });
  assert.deepEqual(rows.map((x) => [x.label, x.depth]), [
    ['Queue', 0], ['Source', 0], ['clone', 1], ['Build', 0], ['web', 1], ['docker_build', 2], ['RUN pnpm build', 3], ['COPY . .', 3], ['api', 1], ['Rollout', 0],
  ]);
  const build = rows.find((x) => x.label === 'Build')!;
  assert.equal(build.startMs, 20_000);
  assert.equal(build.typicalMs, 30_000);
  assert.equal(rows.find((x) => x.label === 'RUN pnpm build')!.startMs, 23_000);
  assert.equal(rows.find((x) => x.label === 'Rollout')!.startMs, 80_000);
});

test('a step that was skipped has no typical time to miss', () => {
  const { run: r, steps } = run();
  const stats = [
    { phase: 'build', step: 'layer', app: 'web', name: 'COPY . .', count: 3, skipped: 1, median_ms: 500, p90_ms: 900, total_ms: 1000, prior_median_ms: null, series: [] },
    { phase: 'build', step: 'layer', app: 'web', name: 'RUN pnpm build', count: 3, skipped: 0, median_ms: 30_000, p90_ms: 40_000, total_ms: 90_000, prior_median_ms: null, series: [] },
  ];
  const rows = waterfall(r, steps, stats, {});
  assert.equal(rows.find((x) => x.label === 'COPY . .')!.typicalMs, null);
  assert.equal(rows.find((x) => x.label === 'RUN pnpm build')!.typicalMs, 30_000);
});

test('a step still running is measured to now', () => {
  const { run: r, steps } = run();
  steps[0].duration_ms = null;
  const rows = waterfall(r, steps, [], {}, Date.parse(steps[0].started_at) + 2500);
  assert.equal(rows.find((x) => x.label === 'clone')!.durationMs, 2500);
});

test('it opens on the slowest image, down to its layers', () => {
  const { run: r, steps } = run();
  const rows = waterfall(r, steps, [], {});
  const open = defaultOpen(rows);
  assert.ok(open.has('s2') && open.has('s3'));
  assert.deepEqual(visibleRows(rows, open).map((x) => x.label), rows.map((x) => x.label));
  open.delete('s2');
  assert.deepEqual(visibleRows(rows, open).map((x) => x.label), ['Queue', 'Source', 'clone', 'Build', 'web', 'api', 'Rollout']);
});
