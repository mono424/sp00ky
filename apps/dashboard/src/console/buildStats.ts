/**
 * The arithmetic behind the build-time pages: medians, the rolling line, where
 * a build's time shifted and why, and one build's waterfall. Pure, so it is
 * tested without a browser (test/buildStats.test.ts).
 */

export const PHASES = ['queue', 'source', 'build', 'infra', 'migrate', 'rollout'] as const;
export type Phase = (typeof PHASES)[number];
export type Metric = 'total' | Phase;
export type BuildPhases = Record<Phase, number>;

export const PHASE_LABEL: Record<Phase, string> = {
  queue: 'Queue',
  source: 'Source',
  build: 'Build',
  infra: 'Infra',
  migrate: 'Migrate',
  rollout: 'Rollout',
};

export const PHASE_HELP: Record<Phase, string> = {
  queue: 'Waiting for a builder',
  source: 'Clone, secrets, sp00ky.yml',
  build: 'Images and the deployment row',
  infra: 'Database, scheduler and SSPs come up',
  migrate: 'Schema and machine pools',
  rollout: 'App containers until healthy',
};

export interface BuildApp {
  name: string;
  ms: number;
  status: string;
}

export interface BuildPoint {
  id: string;
  kind: 'build' | 'deploy';
  status: 'success' | 'failed' | 'cancelled' | 'running';
  version: number | null;
  source: string | null;
  commit_sha: string | null;
  commit_message: string | null;
  commit_author: string | null;
  error: string | null;
  created_at: string;
  total_ms: number;
  phases: BuildPhases;
  detailed: boolean;
  apps: BuildApp[];
  apps_built: number;
  apps_reused: number;
  layers: number;
  layers_cached: number;
}

export interface StepStat {
  phase: string;
  step: string;
  app: string;
  name: string;
  count: number;
  skipped: number;
  median_ms: number;
  p90_ms: number;
  total_ms: number;
  prior_median_ms: number | null;
  /** Newest first. */
  series: number[];
}

export interface RunStep {
  id: number;
  parent_id: number | null;
  phase: string;
  step: string;
  app: string | null;
  name: string;
  status: string;
  started_at: string;
  duration_ms: number | null;
}

export function median(xs: readonly number[]): number {
  return quantile(xs, 0.5);
}

export function quantile(xs: readonly number[], q: number): number {
  if (!xs.length) return 0;
  const sorted = [...xs].sort((a, b) => a - b);
  const at = (sorted.length - 1) * q;
  const lo = Math.floor(at);
  const hi = Math.ceil(at);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (at - lo);
}

export const metricValue = (p: BuildPoint, m: Metric): number => (m === 'total' ? p.total_ms : p.phases[m]);

/** Each value's median with `radius` neighbours on either side, fewer at the ends. */
export function rollingMedian(xs: readonly number[], radius = 3): number[] {
  return xs.map((_, i) => median(xs.slice(Math.max(0, i - radius), i + radius + 1)));
}

/** A build's duration in words: `850ms`, `38s`, `4m 12s`, `1h 12m`. */
export function formatBuildTime(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const s = Math.round(ms / 1000);
  if (s < 60) return ms < 10_000 ? `${(ms / 1000).toFixed(1)}s` : `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return s % 60 ? `${m}m ${s % 60}s` : `${m}m`;
  const h = Math.floor(m / 60);
  return m % 60 ? `${h}h ${m % 60}m` : `${h}h`;
}

/** A signed difference: `+38s`, `-1m 4s`. */
export function formatDelta(ms: number): string {
  return `${ms < 0 ? '-' : '+'}${formatBuildTime(Math.abs(ms))}`;
}

/** An axis tick: `0`, `30s`, `5m`, `1h`. */
export function formatTick(ms: number): string {
  if (ms === 0) return '0';
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)}m`;
  return `${Math.round(ms / 360_000) / 10}h`;
}

const TICK_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200].map((s) => s * 1000);

/** Round axis ticks from 0 that cover `max`, about `count` intervals of them. */
export function niceTicks(max: number, count = 4): number[] {
  if (max <= 0) return [0];
  const step = TICK_STEPS.find((s) => max / s <= count) ?? Math.ceil(max / count / 3_600_000) * 3_600_000;
  const out: number[] = [];
  for (let v = 0; v < max + step; v += step) out.push(v);
  return out;
}

export interface TrendModel {
  values: number[];
  /** The rolling median at each successful build, null at any other. */
  line: (number | null)[];
  yMax: number;
  ticks: number[];
}

/**
 * The trend chart's numbers. The scale follows the bulk of the builds, not the
 * worst one, so one wedged build cannot flatten everything else; what is off
 * the scale is pinned to its top.
 */
export function trendModel(points: readonly BuildPoint[], metric: Metric): TrendModel {
  const values = points.map((p) => metricValue(p, metric));
  const good = points.map((p, i) => (p.status === 'success' ? i : -1)).filter((i) => i >= 0);
  const rolled = rollingMedian(good.map((i) => values[i]));
  const line: (number | null)[] = points.map(() => null);
  good.forEach((idx, n) => {
    line[idx] = rolled[n];
  });
  const sample = good.length ? good.map((i) => values[i]) : values;
  const cap = Math.max(...sample, 0);
  const typical = quantile(sample, 0.97) * 1.2;
  const ticks = niceTicks(Math.min(cap, typical) || cap);
  return { values, line, yMax: ticks[ticks.length - 1] || 1, ticks };
}

export interface Explanation {
  label: string;
  deltaMs: number;
}

export interface ChangePoint {
  /** First build after the shift, by id so it survives re-slicing. */
  id: string;
  at: string;
  beforeMs: number;
  afterMs: number;
  deltaMs: number;
  pct: number;
  commit: { sha: string; message: string | null } | null;
  /** What moved most in the same direction, biggest first. */
  because: Explanation[];
}

const WINDOW = 5;
const MIN_SHIFT_PCT = 0.15;
const MIN_SHIFT_MS = 20_000;
const MAX_CHANGES = 6;
const PLATEAU = 0.98;
/** A step has to clear this many times the builds' ordinary wobble. */
const NOISE_FACTOR = 3;

function medianOf<T>(rows: T[], pick: (r: T) => number): number {
  return median(rows.map(pick));
}

function explain(before: BuildPoint[], after: BuildPoint[], direction: number, metric: Metric): Explanation[] {
  const out: Explanation[] = [];
  for (const phase of PHASES) {
    if (metric !== 'total' && metric !== phase) continue;
    const delta = medianOf(after, (p) => p.phases[phase]) - medianOf(before, (p) => p.phases[phase]);
    if (delta * direction > 0) out.push({ label: PHASE_LABEL[phase], deltaMs: delta });
  }
  const detailed = [...before, ...after].filter((p) => p.detailed);
  if (detailed.length && (metric === 'total' || metric === 'build')) {
    const names = new Set(detailed.flatMap((p) => p.apps.map((a) => a.name)));
    for (const name of names) {
      const time = (p: BuildPoint) => p.apps.find((a) => a.name === name)?.ms ?? 0;
      const delta = medianOf(after.filter((p) => p.detailed), time) - medianOf(before.filter((p) => p.detailed), time);
      if (delta * direction > 0) out.push({ label: name, deltaMs: delta });
    }
  }
  return out
    .filter((e) => Math.abs(e.deltaMs) >= 2000)
    .sort((a, b) => Math.abs(b.deltaMs) - Math.abs(a.deltaMs))
    .slice(0, 3);
}

/**
 * Where build time stepped up or down: the median of the builds after a point
 * against the median of those before it, keeping the sharpest shifts that are
 * at least a window apart. Medians, so one slow build is never a step.
 */
export function changePoints(points: readonly BuildPoint[], metric: Metric = 'total'): ChangePoint[] {
  const good = points.filter((p) => p.status === 'success');
  if (good.length < WINDOW * 2) return [];
  const values = good.map((p) => metricValue(p, metric));
  const local = rollingMedian(values);
  const wobble = median(values.map((v, i) => Math.abs(v - local[i])));
  const minShift = Math.max(MIN_SHIFT_MS, wobble * NOISE_FACTOR);
  const found: { i: number; beforeMs: number; afterMs: number; deltaMs: number; pct: number }[] = [];
  for (let i = WINDOW; i <= good.length - WINDOW; i++) {
    const beforeMs = median(values.slice(i - WINDOW, i));
    const afterMs = median(values.slice(i, i + WINDOW));
    const deltaMs = afterMs - beforeMs;
    const pct = beforeMs > 0 ? deltaMs / beforeMs : 0;
    if (Math.abs(deltaMs) >= minShift && Math.abs(pct) >= MIN_SHIFT_PCT) found.push({ i, beforeMs, afterMs, deltaMs, pct });
  }
  const byBuild = new Map(found.map((c) => [c.i, c]));
  const kept: typeof found = [];
  let left = [...found].sort((a, b) => Math.abs(b.deltaMs) - Math.abs(a.deltaMs));
  while (left.length && kept.length < MAX_CHANGES) {
    // A clean step reads the same from every point inside its window, so the
    // sharpest shift is a plateau; the step itself is at the plateau's middle.
    const top = left[0];
    const onPlateau = (i: number) => {
      const c = byBuild.get(i);
      return !!c && c.deltaMs * top.deltaMs > 0 && Math.abs(c.deltaMs) >= Math.abs(top.deltaMs) * PLATEAU;
    };
    let lo = top.i;
    let hi = top.i;
    while (onPlateau(lo - 1)) lo--;
    while (onPlateau(hi + 1)) hi++;
    const pick = byBuild.get(Math.floor((lo + hi) / 2))!;
    kept.push(pick);
    left = left.filter((c) => Math.abs(c.i - pick.i) >= WINDOW);
  }
  kept.sort((a, b) => a.i - b.i);
  return kept.map((c) => {
    const first = good[c.i];
    return {
      id: first.id,
      at: first.created_at,
      beforeMs: c.beforeMs,
      afterMs: c.afterMs,
      deltaMs: c.deltaMs,
      pct: c.pct,
      commit: first.commit_sha ? { sha: first.commit_sha, message: first.commit_message } : null,
      because: explain(good.slice(c.i - WINDOW, c.i), good.slice(c.i, c.i + WINDOW), Math.sign(c.deltaMs), metric),
    };
  });
}

export interface Tile {
  medianMs: number;
  p90Ms: number;
  priorMedianMs: number | null;
  /** -0.12 for 12% faster than the window before; null with nothing to compare. */
  deltaPct: number | null;
}

/** A metric over a window of builds, against the window before it. */
export function tile(current: readonly BuildPoint[], prior: readonly BuildPoint[], metric: Metric): Tile {
  const values = current.filter((p) => p.status === 'success').map((p) => metricValue(p, metric));
  const before = prior.filter((p) => p.status === 'success').map((p) => metricValue(p, metric));
  const medianMs = median(values);
  const priorMedianMs = before.length ? median(before) : null;
  return {
    medianMs,
    p90Ms: quantile(values, 0.9),
    priorMedianMs,
    deltaPct: priorMedianMs ? (medianMs - priorMedianMs) / priorMedianMs : null,
  };
}

/** Of the apps and layers these builds had, how many were reused or cached. */
export function reuse(points: readonly BuildPoint[]): { apps: number | null; layers: number | null } {
  const good = points.filter((p) => p.status === 'success' && p.detailed);
  const reused = good.reduce((n, p) => n + p.apps_reused, 0);
  const apps = good.reduce((n, p) => n + p.apps_reused + p.apps_built, 0);
  const cached = good.reduce((n, p) => n + p.layers_cached, 0);
  const layers = good.reduce((n, p) => n + p.layers, 0);
  return { apps: apps ? reused / apps : null, layers: layers ? cached / layers : null };
}

export const stepKey = (phase: string, step: string, app: string | null, name: string): string =>
  `${phase}\u0000${step}\u0000${app ?? ''}\u0000${name}`;

/** Which builds' dates to print under an evenly spaced axis, at least `minGap` builds apart. */
export function axisLabels(points: readonly BuildPoint[], minGap: number): number[] {
  const out: number[] = [];
  let last = -Infinity;
  let lastDay = '';
  points.forEach((p, i) => {
    const day = p.created_at.slice(0, 10);
    if (day !== lastDay && i - last >= minGap) {
      out.push(i);
      last = i;
      lastDay = day;
    }
  });
  return out;
}

export interface WaterfallRow {
  key: string;
  parent: string | null;
  depth: number;
  kind: 'phase' | 'step' | 'layer';
  phase: Phase;
  label: string;
  startMs: number;
  durationMs: number;
  status: string;
  typicalMs: number | null;
  children: number;
}

/**
 * One build as a tree of bars: its phases, the steps under each, and under an
 * image its `docker build` and that build's layers. A step's offset is real
 * time since the push, so parallel build stages show as parallel.
 */
export function waterfall(run: BuildPoint, steps: readonly RunStep[], stats: readonly StepStat[], phaseTypical: Partial<BuildPhases>, now = Date.now()): WaterfallRow[] {
  const zero = Date.parse(run.created_at);
  const typical = new Map(stats.map((s) => [stepKey(s.phase, s.step, s.app, s.name), s.median_ms]));
  const kids = new Map<string, RunStep[]>();
  for (const s of steps) {
    const key = s.parent_id != null ? `s${s.parent_id}` : `p${s.phase}`;
    kids.set(key, [...(kids.get(key) ?? []), s]);
  }

  const rows: WaterfallRow[] = [];
  const walk = (children: RunStep[], parent: string, depth: number, phase: Phase) => {
    for (const s of children) {
      const key = `s${s.id}`;
      const grand = kids.get(key) ?? [];
      const known = typical.get(stepKey(s.phase, s.step, s.app, s.name));
      rows.push({
        key,
        parent,
        depth,
        kind: s.step === 'layer' ? 'layer' : 'step',
        phase,
        label: s.name,
        startMs: Date.parse(s.started_at) - zero,
        durationMs: s.duration_ms ?? Math.max(0, now - Date.parse(s.started_at)),
        status: s.status,
        typicalMs: known && s.status !== 'cached' && s.status !== 'reused' ? known : null,
        children: grand.length,
      });
      walk(grand, key, depth + 1, phase);
    }
  };

  let at = 0;
  for (const phase of PHASES) {
    const ms = run.phases[phase];
    if (ms > 0 || kids.has(`p${phase}`)) {
      const key = `p${phase}`;
      const children = kids.get(key) ?? [];
      rows.push({
        key,
        parent: null,
        depth: 0,
        kind: 'phase',
        phase,
        label: PHASE_LABEL[phase],
        startMs: at,
        durationMs: ms,
        status: 'ok',
        typicalMs: phaseTypical[phase] ?? null,
        children: children.length,
      });
      walk(children, key, 1, phase);
    }
    at += ms;
  }
  return rows;
}

/** A row is on screen when all its ancestors are open. */
export function visibleRows(rows: readonly WaterfallRow[], open: ReadonlySet<string>): WaterfallRow[] {
  const shown = new Set<string>();
  return rows.filter((r) => {
    if (r.parent === null || (shown.has(r.parent) && open.has(r.parent))) {
      shown.add(r.key);
      return true;
    }
    return false;
  });
}

/** The waterfall's starting state: phases open, and the slowest image down to its layers. */
export function defaultOpen(rows: readonly WaterfallRow[]): Set<string> {
  const open = new Set(rows.filter((r) => r.kind === 'phase').map((r) => r.key));
  const images = rows.filter((r) => r.parent === 'pbuild' && r.children > 0).sort((a, b) => b.durationMs - a.durationMs);
  const slowest = images[0];
  if (slowest) {
    open.add(slowest.key);
    for (const r of rows) if (r.parent === slowest.key && r.children > 0) open.add(r.key);
  }
  return open;
}
