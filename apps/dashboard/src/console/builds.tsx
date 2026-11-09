/**
 * Build time: a box on the Deployments page and, behind it, the page for working
 * out where a project's build time goes. From the trend of whole builds, down
 * through phases and images to one build's waterfall and its Docker layers.
 */

import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup, type JSX } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { PageHead } from '../components/Chrome';
import { FxCell, Sq } from '../components/Frame';
import { formatStamp } from '../lib/format';
import { cloud, consoleApi, type Workspace, type WorkspaceProject } from './cloud';
import { ago } from './chrome';
import {
  PHASES,
  PHASE_HELP,
  PHASE_LABEL,
  axisLabels,
  changePoints,
  defaultOpen,
  formatBuildTime,
  formatDelta,
  formatTick,
  median,
  metricValue,
  niceTicks,
  reuse,
  tile,
  trendModel,
  visibleRows,
  waterfall,
  type BuildPhases,
  type BuildPoint,
  type ChangePoint,
  type Metric,
  type Phase,
  type StepStat,
  type WaterfallRow,
} from './buildStats';

interface Ctx {
  workspace: Workspace;
  project: WorkspaceProject;
}

const RANGES = [
  { days: 7, label: '7d' },
  { days: 30, label: '30d' },
  { days: 90, label: '90d' },
  { days: 365, label: '1y' },
] as const;
const RECENT_ROWS = 12;
const LAYER_ROWS = 12;
/** A typical-time mark on a bar too short to see would only be noise. */
const MIN_TICK_SHARE = 0.012;

const sha7 = (sha: string) => sha.slice(0, 7);
const firstLine = (s: string | null | undefined) => s?.split('\n')[0]?.trim() ?? '';
const shortDate = (iso: string) => new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });

const whatOf = (p: BuildPoint): string => {
  const commit = firstLine(p.commit_message);
  if (commit) return commit;
  if (p.commit_sha) return `Commit ${sha7(p.commit_sha)}`;
  if (p.source === 'rollback' || p.source === 'auto_rollback') return 'Rollback';
  if (p.source === 'cli') return 'Deployed from the CLI';
  return 'Deployment';
};

const toneOf = (p: BuildPoint) => (p.status === 'failed' ? 'bad' : p.status === 'running' ? 'run' : 'ok');
const seriesColor = (m: Metric) => (m === 'total' ? 'var(--accent-strong)' : `var(--ph-${m})`);
const metricLabel = (m: Metric) => (m === 'total' ? 'Push to live' : PHASE_LABEL[m]);

/** An element's width, kept current. */
function useWidth(initial = 800): [(el: HTMLElement) => void, () => number] {
  const [width, setWidth] = createSignal(initial);
  let observer: ResizeObserver | undefined;
  const measure = (el: HTMLElement) => {
    setWidth(el.clientWidth || initial);
    observer = new ResizeObserver(([entry]) => {
      if (entry.contentRect.width > 0) setWidth(entry.contentRect.width);
    });
    observer.observe(el);
  };
  onCleanup(() => observer?.disconnect());
  return [measure, width];
}

/** Better or worse than before, as a word and a direction, never only a color. */
function Delta(props: { pct: number | null | undefined; from?: number | null; label?: string }) {
  return (
    <Show when={props.pct != null} fallback={<span class="bt-delta none">no earlier builds to compare</span>}>
      <span class="bt-delta" classList={{ worse: props.pct! >= 0.02, better: props.pct! <= -0.02 }}>
        <b>{props.pct! > 0 ? '▲' : props.pct! < 0 ? '▼' : '■'} {Math.abs(Math.round(props.pct! * 100))}%</b>
        <Show when={props.from != null}> from {formatBuildTime(props.from!)}</Show>
        <Show when={props.label}> {props.label}</Show>
      </span>
    </Show>
  );
}

/** One build split into its phases. */
function PhaseBar(props: { phases: BuildPhases; scale?: number; tall?: boolean; total: number }) {
  const width = (ms: number) => `${(ms / (props.scale ?? props.total)) * 100}%`;
  return (
    <span class="bt-phasebar" classList={{ tall: props.tall }} role="img" aria-label={PHASES.filter((p) => props.phases[p] > 0).map((p) => `${PHASE_LABEL[p]} ${formatBuildTime(props.phases[p])}`).join(', ')}>
      <For each={PHASES.filter((p) => props.phases[p] > 0)}>
        {(p) => <i style={{ width: width(props.phases[p]), background: `var(--ph-${p})` }} title={`${PHASE_LABEL[p]} ${formatBuildTime(props.phases[p])}`} />}
      </For>
    </span>
  );
}

function useBuilds(project: () => WorkspaceProject, days: () => number, withSteps: boolean) {
  return createResource(
    () => ({ id: project().id, days: days() }),
    (k) => consoleApi.builds(k.id, k.days, withSteps),
  );
}

/* ------------------------------------------------------------------ */
/* The box on the Deployments page                                      */
/* ------------------------------------------------------------------ */

const BOX_DAYS = 30;

export function BuildTimeBox(props: { project: WorkspaceProject }) {
  const [data, { refetch }] = useBuilds(() => props.project, () => BOX_DAYS, false);
  const timer = setInterval(() => void refetch(), 60_000);
  onCleanup(() => clearInterval(timer));

  const split = createMemo(() => {
    const runs = data()?.runs ?? [];
    const start = Date.parse(data()?.window_start ?? '') || 0;
    const builds = runs.some((r) => r.kind === 'build') ? runs.filter((r) => r.kind === 'build') : runs;
    const chartable = builds.filter((r) => r.status === 'success' || r.status === 'failed');
    return {
      current: chartable.filter((r) => Date.parse(r.created_at) >= start),
      prior: chartable.filter((r) => Date.parse(r.created_at) < start),
    };
  });
  const last = createMemo(() => [...split().current].reverse().find((p) => p.status === 'success'));
  const stat = createMemo(() => tile(split().current, split().prior, 'total'));

  return (
    <Show when={split().current.length > 0}>
      <section class="fx-row">
        <A class="fx-cell bt-box" href="/deployments/builds">
          <div class="fx-head">
            <span class="cap">Build time</span>
            <span class="fx-meta cap">
              <span>{split().current.length} {split().current.length === 1 ? 'build' : 'builds'} in {BOX_DAYS} days</span>
              <b class="bt-go">Investigate <span aria-hidden="true">→</span></b>
            </span>
          </div>
          <div class="bt-box-grid">
            <div class="bt-box-stat">
              <span class="cap">Push to live, median</span>
              <span class="bt-figure">{formatBuildTime(stat().medianMs)}</span>
              <Delta pct={stat().deltaPct} from={stat().priorMedianMs} label="last period" />
            </div>
            <Show when={last()}>
              {(p) => (
                <div class="bt-box-last">
                  <span class="cap">Latest build<Show when={p().version != null}> · v{p().version}</Show> · {formatBuildTime(p().total_ms)}</span>
                  <PhaseBar phases={p().phases} total={p().total_ms} tall />
                  <div class="bt-mini-legend">
                    <For each={PHASES.filter((ph) => p().phases[ph] > 0)}>
                      {(ph) => (
                        <span><i style={{ background: `var(--ph-${ph})` }} />{PHASE_LABEL[ph]} <b>{formatBuildTime(p().phases[ph])}</b></span>
                      )}
                    </For>
                  </div>
                </div>
              )}
            </Show>
            <div class="bt-box-trend" aria-hidden="true">
              <MiniTrend points={split().current} />
            </div>
          </div>
        </A>
      </section>
    </Show>
  );
}

function MiniTrend(props: { points: BuildPoint[] }) {
  const [measure, width] = useWidth(320);
  const H = 72;
  const model = createMemo(() => trendModel(props.points, 'total'));
  const x = (i: number) => 4 + ((i + 0.5) / props.points.length) * (width() - 8);
  const y = (v: number) => 4 + (H - 8) * (1 - Math.min(v, model().yMax) / model().yMax);
  const path = createMemo(() => {
    const parts: string[] = [];
    model().line.forEach((v, i) => {
      if (v != null) parts.push(`${parts.length ? 'L' : 'M'}${x(i).toFixed(1)} ${y(v).toFixed(1)}`);
    });
    return parts.join('');
  });
  return (
    <div ref={measure} class="bt-mini">
      <svg width={width()} height={H} role="presentation">
        <line x1="0" x2={width()} y1={H - 4} y2={H - 4} class="bt-axis" />
        <For each={props.points}>
          {(p, i) => <circle cx={x(i())} cy={y(model().values[i()])} r="2.5" class={p.status === 'failed' ? 'bt-mini-fail' : 'bt-mini-dot'} />}
        </For>
        <path d={path()} class="bt-line" style={{ stroke: 'var(--accent-strong)' }} />
      </svg>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* The investigation page                                               */
/* ------------------------------------------------------------------ */

const MARGIN = { l: 48, r: 14 };

export function Builds(props: Ctx) {
  const navigate = useNavigate();
  const [days, setDays] = createSignal(30);
  const [metric, setMetric] = createSignal<Metric>('total');
  const [scope, setScope] = createSignal<'build' | 'all'>('build');
  const [data, { refetch }] = useBuilds(() => props.project, days, true);
  const timer = setInterval(() => void refetch(), 30_000);
  onCleanup(() => clearInterval(timer));
  const [link] = createResource(() =>
    cloud<{ repo_owner?: string; repo_name?: string }>(`/projects/${encodeURIComponent(props.project.id)}/link`, { tenant: props.workspace.id }).catch((): { repo_owner?: string; repo_name?: string } => ({})),
  );
  const commitUrl = (sha: string) => {
    const l = link();
    return l?.repo_owner && l.repo_name ? `https://github.com/${l.repo_owner}/${l.repo_name}/commit/${sha}` : undefined;
  };

  const runs = createMemo(() => data.latest?.runs ?? []);
  const hasBuilds = createMemo(() => runs().some((r) => r.kind === 'build'));
  const hasDeploys = createMemo(() => runs().some((r) => r.kind === 'deploy'));
  // With nothing built here there are only deployments to show.
  const inScope = createMemo(() => runs().filter((r) => (scope() === 'all' || !hasBuilds() ? true : r.kind === 'build')));
  const start = () => Date.parse(data.latest?.window_start ?? '') || 0;
  const charted = createMemo(() => inScope().filter((r) => r.status === 'success' || r.status === 'failed'));
  const current = createMemo(() => charted().filter((r) => Date.parse(r.created_at) >= start()));
  const prior = createMemo(() => charted().filter((r) => Date.parse(r.created_at) < start()));
  const marks = createMemo(() => changePoints(current(), metric()));
  const [hover, setHover] = createSignal<number | null>(null);
  const failed = createMemo(() => current().filter((r) => r.status === 'failed').length);

  const tiles = createMemo(() => ({
    total: tile(current(), prior(), 'total'),
    build: tile(current(), prior(), 'build'),
    rollout: tile(current(), prior(), 'rollout'),
    reused: reuse(current()),
  }));
  const phaseMedians = createMemo(() => {
    const good = current().filter((p) => p.status === 'success');
    return Object.fromEntries(PHASES.map((p) => [p, median(good.map((b) => b.phases[p]))])) as BuildPhases;
  });
  const typicalTotal = () => tiles().total.medianMs;
  const pick = (p: BuildPoint) => navigate(`/deployments/builds/${p.id}`);

  return (
    <>
      <PageHead
        crumb="Deployments"
        crumbHref="/deployments"
        title="Build time"
        actions={
          <>
            <Show when={hasBuilds() && hasDeploys()}>
              <span class="bt-seg" role="group" aria-label="What to include">
                <button aria-pressed={scope() === 'build'} onClick={() => setScope('build')}>Git builds</button>
                <button aria-pressed={scope() === 'all'} onClick={() => setScope('all')}>All deploys</button>
              </span>
            </Show>
            <span class="bt-seg" role="group" aria-label="Time range">
              <For each={RANGES}>
                {(r) => <button aria-pressed={days() === r.days} onClick={() => setDays(r.days)}>{r.label}</button>}
              </For>
            </span>
          </>
        }
      />
      <div class="fx-page" classList={{ 'bt-stale': data.loading && !!data.latest }}>
        <Show when={data.error}>
          <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{String(data.error?.message ?? data.error)}</span></div></FxCell></section>
        </Show>

        <section class="fx-row bt-tiles">
          <FxCell label="Push to live, median">
            <span class="bt-figure hero">{formatBuildTime(tiles().total.medianMs)}</span>
            <Delta pct={tiles().total.deltaPct} from={tiles().total.priorMedianMs} label={`prior ${days()}d`} />
          </FxCell>
          <FxCell label="Slowest 1 in 10">
            <span class="bt-figure">{formatBuildTime(tiles().total.p90Ms)}</span>
            <span class="bt-delta none">{current().length} builds, {failed()} failed</span>
          </FxCell>
          <FxCell label="Build, median">
            <span class="bt-figure">{formatBuildTime(tiles().build.medianMs)}</span>
            <Delta pct={tiles().build.deltaPct} from={tiles().build.priorMedianMs} />
          </FxCell>
          <FxCell label="Rollout, median">
            <span class="bt-figure">{formatBuildTime(tiles().rollout.medianMs)}</span>
            <Delta pct={tiles().rollout.deltaPct} from={tiles().rollout.priorMedianMs} />
          </FxCell>
          <FxCell label="Images reused">
            <span class="bt-figure">{tiles().reused.apps != null ? `${Math.round(tiles().reused.apps! * 100)}%` : '—'}</span>
            <span class="bt-delta none">{tiles().reused.layers != null ? `${Math.round(tiles().reused.layers! * 100)}% of layers cached` : 'from step timing'}</span>
          </FxCell>
        </section>

        <section class="fx-row">
          <div class="fx-cell">
            <div class="bt-chart-head">
              <div>
                <h2 class="bt-title">{metricLabel(metric())}, build by build</h2>
                <p class="bt-sub">
                  Each dot is one successful build{metric() === 'total' ? ', from the push until it was live' : ` (${PHASE_HELP[metric() as Phase].toLowerCase()})`}.
                  The line is the median of each build and the 3 on each side.
                </p>
              </div>
              <div class="bt-legend" aria-label="Legend">
                <span><i class="dot" style={{ background: seriesColor(metric()) }} />One build</span>
                <span><i class="stroke" style={{ background: seriesColor(metric()) }} />7-build median</span>
                <span><span class="cross" aria-hidden="true">×</span>Failed</span>
                <span><span class="tri" aria-hidden="true">▲</span>Off the scale</span>
              </div>
            </div>

            <div class="bt-metrics" role="group" aria-label="What to chart">
              <button aria-pressed={metric() === 'total'} onClick={() => setMetric('total')}>
                <span class="n">Push to live</span>
                <b>{formatBuildTime(typicalTotal())}</b>
              </button>
              <For each={PHASES}>
                {(p) => (
                  <button aria-pressed={metric() === p} onClick={() => setMetric(metric() === p ? 'total' : p)} title={PHASE_HELP[p]}>
                    <span class="n"><i style={{ background: `var(--ph-${p})` }} />{PHASE_LABEL[p]}</span>
                    <b>{formatBuildTime(phaseMedians()[p])}</b>
                  </button>
                )}
              </For>
            </div>

            <Show
              when={current().length > 0}
              fallback={<div class="bt-empty cap">{data.loading ? 'Loading' : `No builds in the last ${days()} days`}</div>}
            >
              <TrendChart points={current()} metric={metric()} marks={marks()} hover={hover()} setHover={setHover} onPick={pick} />
              <StackChart points={current()} metric={metric()} hover={hover()} setHover={setHover} onPick={pick} />
            </Show>

            <Show when={marks().length}>
              <ol class="bt-changes">
                <For each={marks()}>
                  {(c, i) => <Change change={c} n={i() + 1} commitUrl={commitUrl} onOpen={() => navigate(`/deployments/builds/${c.id}`)} />}
                </For>
              </ol>
            </Show>
          </div>
        </section>

        <Show when={(data.latest?.steps ?? []).length > 0}>
          <section class="fx-row">
            <FxCell
              label={metric() === 'total' ? 'Where the time goes, by step' : `Inside ${PHASE_LABEL[metric() as Phase]}`}
              meta={<span>Slowest first · median of the runs that did work</span>}
            >
              <StepsTable steps={data.latest?.steps ?? []} metric={metric()} />
            </FxCell>
          </section>
        </Show>

        <section class="fx-row">
          <FxCell label="Builds" meta={<span>{current().length} in {days()} days</span>}>
            <RecentBuilds points={current()} typical={typicalTotal()} />
          </FxCell>
        </section>
      </div>
    </>
  );
}

function Change(props: { change: ChangePoint; n: number; commitUrl: (sha: string) => string | undefined; onOpen: () => void }) {
  const c = () => props.change;
  const slower = () => c().deltaMs > 0;
  return (
    <li class="bt-change">
      <span class="bt-flag">{props.n}</span>
      <span class="bt-change-date">{shortDate(c().at)}</span>
      <span class="bt-change-shift" classList={{ worse: slower() }}>
        {formatBuildTime(c().beforeMs)} → {formatBuildTime(c().afterMs)}
        <b> {slower() ? '▲' : '▼'} {Math.abs(Math.round(c().pct * 100))}%</b>
      </span>
      <span class="bt-change-why">
        <Show when={c().because.length} fallback="spread across the build">
          <For each={c().because}>
            {(b, i) => <><Show when={i() > 0}>, </Show><b>{b.label}</b> {formatDelta(b.deltaMs)}</>}
          </For>
        </Show>
      </span>
      <span class="bt-change-commit">
        <Show when={c().commit}>
          {(m) => (
            <>
              <Show when={props.commitUrl(m().sha)} fallback={<span class="cx-sha">{sha7(m().sha)}</span>}>
                <a class="cx-sha" href={props.commitUrl(m().sha)} target="_blank" rel="noopener noreferrer">{sha7(m().sha)}</a>
              </Show>
              <span class="dim"> {firstLine(m().message)}</span>
            </>
          )}
        </Show>
      </span>
      <button class="bt-change-open" onClick={props.onOpen}>Open build →</button>
    </li>
  );
}

interface ChartProps {
  points: BuildPoint[];
  metric: Metric;
  hover: number | null;
  setHover: (i: number | null) => void;
  onPick: (p: BuildPoint) => void;
}

/** Pointer and keyboard over an evenly spaced row of builds. */
function hoverHandlers(props: ChartProps, slot: () => number) {
  const indexAt = (e: MouseEvent) => {
    const r = (e.currentTarget as Element).getBoundingClientRect();
    const i = Math.floor((e.clientX - r.left - MARGIN.l) / slot());
    return Math.max(0, Math.min(props.points.length - 1, i));
  };
  return {
    onPointerMove: (e: PointerEvent) => props.setHover(indexAt(e)),
    onPointerLeave: () => props.setHover(null),
    onClick: (e: MouseEvent) => {
      const p = props.points[indexAt(e)];
      if (p) props.onPick(p);
    },
    onKeyDown: (e: KeyboardEvent) => {
      const last = props.points.length - 1;
      const cur = props.hover ?? last;
      if (e.key === 'ArrowLeft') props.setHover(Math.max(0, cur - 1));
      else if (e.key === 'ArrowRight') props.setHover(Math.min(last, cur + 1));
      else if (e.key === 'Enter' && props.points[cur]) props.onPick(props.points[cur]);
      else return;
      e.preventDefault();
    },
    onFocus: () => props.setHover(props.points.length - 1),
    onBlur: () => props.setHover(null),
  };
}

function TrendChart(props: ChartProps & { marks: ChangePoint[] }) {
  const [measure, width] = useWidth();
  const TOP = 28;
  const PLOT = 288;
  const BOTTOM = 26;
  const H = TOP + PLOT + BOTTOM;
  const model = createMemo(() => trendModel(props.points, props.metric));
  const slot = () => (width() - MARGIN.l - MARGIN.r) / Math.max(1, props.points.length);
  const x = (i: number) => MARGIN.l + (i + 0.5) * slot();
  const y = (v: number) => TOP + PLOT * (1 - Math.min(v, model().yMax) / model().yMax);
  const r = () => (slot() < 4 ? 2.5 : slot() < 8 ? 3 : 4);
  const color = () => seriesColor(props.metric);
  const path = createMemo(() => {
    const parts: string[] = [];
    model().line.forEach((v, i) => {
      if (v != null) parts.push(`${parts.length ? 'L' : 'M'}${x(i).toFixed(1)} ${y(v).toFixed(1)}`);
    });
    return parts.join('');
  });
  const labels = createMemo(() => axisLabels(props.points, Math.ceil(72 / Math.max(slot(), 0.1))));
  const flags = createMemo(() => props.marks.map((m, n) => ({ n: n + 1, i: props.points.findIndex((p) => p.id === m.id) })).filter((f) => f.i >= 0));
  const off = (v: number) => v > model().yMax;
  const handlers = hoverHandlers(props, slot);
  // A range change can leave the pointer on a build that is no longer drawn.
  const at = () => (props.hover != null && props.hover < props.points.length ? props.hover : null);
  const hovered = () => (at() != null ? props.points[at()!] : undefined);

  return (
    <div class="bt-plot" ref={measure}>
      <svg
        width={width()}
        height={H}
        tabindex="0"
        role="img"
        aria-label={`${metricLabel(props.metric)} for ${props.points.length} builds. Use the arrow keys to step through them. The list below has every value.`}
        {...handlers}
      >
        <For each={model().ticks}>
          {(t) => (
            <>
              <line x1={MARGIN.l} x2={width() - MARGIN.r} y1={y(t)} y2={y(t)} class={t === 0 ? 'bt-axis' : 'bt-grid'} />
              <text x={MARGIN.l - 10} y={y(t) + 3.5} class="bt-tick" text-anchor="end">{formatTick(t)}</text>
            </>
          )}
        </For>
        <For each={labels()}>
          {(i) => <text x={x(i)} y={H - 8} class="bt-tick" text-anchor="middle">{shortDate(props.points[i].created_at)}</text>}
        </For>
        <For each={flags()}>
          {(f) => (
            <g>
              <line x1={x(f.i)} x2={x(f.i)} y1={TOP - 8} y2={TOP + PLOT} class="bt-flagline" />
              <rect x={x(f.i) - 8} y={2} width="16" height="16" class="bt-flagbox" />
              <text x={x(f.i)} y={13.5} text-anchor="middle" class="bt-flagtext">{f.n}</text>
            </g>
          )}
        </For>
        <For each={props.points}>
          {(p, i) => {
            const v = () => model().values[i()];
            return (
              <Show
                when={p.status === 'success'}
                fallback={
                  <g class="bt-failed" transform={`translate(${x(i())} ${y(v())})`}>
                    <path d="M-3.5 -3.5L3.5 3.5M3.5 -3.5L-3.5 3.5" />
                  </g>
                }
              >
                <Show
                  when={!off(v())}
                  fallback={<path d={`M${x(i()) - 4} ${TOP + 5}L${x(i())} ${TOP - 2}L${x(i()) + 4} ${TOP + 5}Z`} style={{ fill: color() }} class="bt-off" />}
                >
                  <circle cx={x(i())} cy={y(v())} r={r()} style={{ fill: color() }} class="bt-dot" />
                </Show>
              </Show>
            );
          }}
        </For>
        <path d={path()} class="bt-line" style={{ stroke: color() }} />
        <Show when={at() != null}>
          <line x1={x(at()!)} x2={x(at()!)} y1={TOP} y2={TOP + PLOT} class="bt-cross" />
          <circle cx={x(at()!)} cy={y(model().values[at()!])} r="5" class="bt-hot" style={{ fill: hovered()?.status === 'failed' ? 'var(--bad)' : color() }} />
        </Show>
      </svg>
      <Show when={hovered()}>
        {(p) => <BuildTip point={p()} metric={props.metric} left={x(at()!)} width={width()} top={Math.max(TOP, y(model().values[at()!]) - 40)} />}
      </Show>
    </div>
  );
}

function BuildTip(props: { point: BuildPoint; metric: Metric; left: number; width: number; top: number }) {
  const W = 276;
  const flipped = () => props.left + W + 24 > props.width;
  const p = () => props.point;
  return (
    <div class="bt-tip" style={{ left: `${flipped() ? props.left - W - 16 : props.left + 16}px`, top: `${props.top}px`, width: `${W}px` }}>
      <div class="bt-tip-head">
        <b>{formatBuildTime(metricValue(p(), props.metric))}</b>
        <span>{metricLabel(props.metric).toLowerCase()}<Show when={p().status === 'failed'}> · failed</Show></span>
      </div>
      <div class="bt-tip-rows">
        <For each={PHASES.filter((ph) => p().phases[ph] > 0)}>
          {(ph) => (
            <div classList={{ on: props.metric === ph }}>
              <i style={{ background: `var(--ph-${ph})` }} />
              <b>{formatBuildTime(p().phases[ph])}</b>
              <span>{PHASE_LABEL[ph]}</span>
            </div>
          )}
        </For>
      </div>
      <div class="bt-tip-foot">
        <span class="t">{whatOf(p())}</span>
        <span class="m"><Show when={p().version != null}>v{p().version} · </Show>{ago(p().created_at)}</span>
      </div>
    </div>
  );
}

/** The same builds on the same axis, each split into where its time went. */
function StackChart(props: ChartProps) {
  const [measure, width] = useWidth();
  const H = 96;
  const max = createMemo(() => {
    const sample = props.points.filter((p) => p.status === 'success').map((p) => p.total_ms).sort((a, b) => a - b);
    return niceTicks(sample[Math.floor(sample.length * 0.97)] ?? 0, 3).slice(-1)[0] || 1;
  });
  const slot = () => (width() - MARGIN.l - MARGIN.r) / Math.max(1, props.points.length);
  const gap = () => (slot() >= 4 ? 2 : slot() >= 2 ? 1 : 0);
  const barW = () => Math.min(14, Math.max(1, slot() - gap()));
  const barX = (i: number) => MARGIN.l + (i + 0.5) * slot() - barW() / 2;
  const scale = (ms: number) => (ms / max()) * (H - 6);
  const handlers = hoverHandlers(props, slot);
  const stacks = createMemo(() =>
    props.points.map((p) => {
      let y = H;
      const parts: { phase: Phase; y: number; h: number }[] = [];
      for (const phase of PHASES) {
        const h = Math.max(0, scale(p.phases[phase]));
        if (h < 0.5) continue;
        y -= h;
        parts.push({ phase, y: Math.max(0, y), h });
      }
      return parts;
    }),
  );
  return (
    <div class="bt-plot bt-stack" ref={measure}>
      <svg width={width()} height={H + 1} aria-hidden="true" {...handlers}>
        <text x={MARGIN.l - 10} y={9} class="bt-tick" text-anchor="end">{formatTick(max())}</text>
        <line x1={MARGIN.l} x2={width() - MARGIN.r} y1={H + 0.5} y2={H + 0.5} class="bt-axis" />
        <For each={stacks()}>
          {(parts, i) => (
            <g opacity={props.points[i()].status === 'failed' ? 0.3 : 1}>
              <For each={parts}>
                {(s) => (
                  <rect
                    x={barX(i())}
                    y={s.y}
                    width={barW()}
                    height={Math.max(0, s.h - (gap() ? 1 : 0))}
                    style={{ fill: `var(--ph-${s.phase})`, opacity: props.metric === 'total' || props.metric === s.phase ? 1 : 0.18 }}
                  />
                )}
              </For>
            </g>
          )}
        </For>
        <Show when={props.hover != null && props.hover < props.points.length}>
          <line x1={MARGIN.l + (props.hover! + 0.5) * slot()} x2={MARGIN.l + (props.hover! + 0.5) * slot()} y1={0} y2={H} class="bt-cross" />
        </Show>
      </svg>
    </div>
  );
}

/** A step's trend, oldest on the left. */
function Spark(props: { series: number[] }) {
  const W = 92;
  const H = 22;
  const path = createMemo(() => {
    const values = [...props.series].reverse();
    if (values.length < 2) return '';
    const max = Math.max(...values, 1);
    return values
      .map((v, i) => `${i ? 'L' : 'M'}${((i / (values.length - 1)) * (W - 4) + 2).toFixed(1)} ${(H - 3 - (v / max) * (H - 6)).toFixed(1)}`)
      .join('');
  });
  return (
    <svg width={W} height={H} class="bt-spark" aria-hidden="true">
      <path d={path()} />
    </svg>
  );
}

interface StepRow {
  stat: StepStat;
  children: StepStat[];
}

const IMAGE_STEPS = new Set(['key', 'docker_build', 'inspect', 'export', 'ext4', 'hash']);

function StepsTable(props: { steps: StepStat[]; metric: Metric }) {
  const [open, setOpen] = createSignal<ReadonlySet<string>>(new Set());
  const [allLayers, setAllLayers] = createSignal<ReadonlySet<string>>(new Set());
  const rows = createMemo<StepRow[]>(() => {
    const inPhase = props.steps.filter((s) => props.metric === 'total' || s.phase === props.metric);
    const top = inPhase.filter((s) => s.step !== 'layer' && !IMAGE_STEPS.has(s.step));
    return top
      .map((stat) => ({
        stat,
        children: stat.step === 'app' ? inPhase.filter((s) => s.app === stat.app && (s.step === 'layer' || IMAGE_STEPS.has(s.step))).sort((a, b) => b.median_ms - a.median_ms) : [],
      }))
      .sort((a, b) => b.stat.median_ms - a.stat.median_ms);
  });
  const peak = createMemo(() => Math.max(1, ...rows().map((r) => r.stat.median_ms)));
  const toggle = (set: () => ReadonlySet<string>, write: (s: ReadonlySet<string>) => void, key: string) => {
    const next = new Set(set());
    if (!next.delete(key)) next.add(key);
    write(next);
  };
  const delta = (s: StepStat) => (s.prior_median_ms && s.median_ms ? (s.median_ms - s.prior_median_ms) / s.prior_median_ms : null);

  const Row = (p: { stat: StepStat; depth: number; expandable?: boolean; open?: boolean; onToggle?: () => void; max: number }): JSX.Element => {
    const s = () => p.stat;
    return (
      <tr class="bt-step" classList={{ deep: p.depth > 0 }}>
        <td>
          <span class="bt-step-name" style={{ 'padding-left': `${p.depth * 18}px` }}>
            <Show when={p.expandable} fallback={<i class="bt-tw none" />}>
              <button class="bt-tw" aria-expanded={p.open} aria-label={`${p.open ? 'Collapse' : 'Expand'} ${s().name}`} onClick={p.onToggle}>{p.open ? '▾' : '▸'}</button>
            </Show>
            <Show when={p.depth === 0}><i class="bt-swatch" style={{ background: `var(--ph-${s().phase})` }} /></Show>
            <span class="t" title={s().name}>{s().name}</span>
          </span>
        </td>
        <td class="num">
          <span class="bt-median">
            <span class="bt-med"><Show when={s().median_ms > 0}><i style={{ width: `${Math.max(1, (s().median_ms / p.max) * 100)}%`, background: `var(--ph-${s().phase})` }} /></Show></span>
            <b>{s().median_ms ? formatBuildTime(s().median_ms) : '—'}</b>
          </span>
        </td>
        <td class="num dim opt">{s().p90_ms ? formatBuildTime(s().p90_ms) : '—'}</td>
        <td class="num">
          <Show when={delta(s()) != null} fallback={<span class="dim">—</span>}>
            <span class="bt-delta" classList={{ worse: delta(s())! >= 0.1, better: delta(s())! <= -0.1 }}>{delta(s())! > 0 ? '▲' : '▼'} {Math.abs(Math.round(delta(s())! * 100))}%</span>
          </Show>
        </td>
        <td class="num dim opt">{s().skipped ? `${Math.round((s().skipped / s().count) * 100)}%` : '—'}</td>
        <td class="sp opt"><Spark series={s().series} /></td>
      </tr>
    );
  };

  return (
    <div class="bt-table-wrap">
      <table class="bt-table keep-table">
        <thead>
          <tr>
            <th>Step</th>
            <th class="num">Median</th>
            <th class="num opt">P90</th>
            <th class="num">vs prior</th>
            <th class="num opt" title="How often it was cached or reused instead of run">Skipped</th>
            <th class="sp opt">Last 30</th>
          </tr>
        </thead>
        <For each={rows()}>
          {(r) => {
            const key = stepId(r.stat);
            const isOpen = () => open().has(key);
            const steps = () => r.children.filter((c) => c.step !== 'layer');
            const layers = () => r.children.filter((c) => c.step === 'layer');
            const showAll = () => allLayers().has(key);
            const childMax = () => Math.max(1, ...r.children.map((c) => c.median_ms));
            return (
              <tbody>
                <Row stat={r.stat} depth={0} expandable={r.children.length > 0} open={isOpen()} onToggle={() => toggle(open, setOpen, key)} max={peak()} />
                <Show when={isOpen()}>
                  <For each={steps()}>{(c) => <Row stat={c} depth={1} max={childMax()} />}</For>
                  <Show when={layers().length}>
                    <tr class="bt-step-sub"><td colspan="6"><span class="cap">Docker layers</span></td></tr>
                    <For each={showAll() ? layers() : layers().slice(0, LAYER_ROWS)}>{(c) => <Row stat={c} depth={1} max={childMax()} />}</For>
                    <Show when={layers().length > LAYER_ROWS}>
                      <tr class="bt-step-sub"><td colspan="6"><button class="cx-showall cap" onClick={() => toggle(allLayers, setAllLayers, key)}>{showAll() ? 'Show fewer' : `Show all ${layers().length}`}</button></td></tr>
                    </Show>
                  </Show>
                </Show>
              </tbody>
            );
          }}
        </For>
      </table>
    </div>
  );
}

const stepId = (s: StepStat) => `${s.phase}/${s.step}/${s.app}/${s.name}`;

function RecentBuilds(props: { points: BuildPoint[]; typical: number }) {
  const [all, setAll] = createSignal(false);
  const newest = createMemo(() => [...props.points].reverse());
  const scale = createMemo(() => Math.max(1, ...newest().map((p) => p.total_ms)));
  return (
    <div class="bt-runs">
      <For each={all() ? newest() : newest().slice(0, RECENT_ROWS)} fallback={<div class="cx-dep-empty dim">No builds yet</div>}>
        {(p) => {
          const pct = () => (props.typical ? (p.total_ms - props.typical) / props.typical : 0);
          return (
            <A class="bt-run" href={`/deployments/builds/${p.id}`} classList={{ failed: p.status === 'failed' }}>
              <span class={`cx-st ${toneOf(p)}`}><Sq tone={toneOf(p)} /></span>
              <span class="bt-run-v"><b>{p.version != null ? `v${p.version}` : '—'}</b></span>
              <span class="bt-run-what">
                <span class="t">{whatOf(p)}</span>
                <span class="m"><Show when={p.commit_sha}><span class="cx-sha">{sha7(p.commit_sha!)}</span> </Show>{p.commit_author ?? p.source ?? ''}</span>
              </span>
              <span class="bt-run-bar"><PhaseBar phases={p.phases} total={p.total_ms} scale={scale()} /></span>
              <span class="bt-run-total"><b>{formatBuildTime(p.total_ms)}</b></span>
              <span class="bt-run-delta">
                <Show when={p.status === 'success'} fallback={<span class="bad">failed</span>}>
                  <span class="bt-delta" classList={{ worse: pct() >= 0.2, better: pct() <= -0.2 }}>{pct() > 0 ? '+' : ''}{Math.round(pct() * 100)}%</span>
                </Show>
              </span>
              <span class="bt-run-when" title={formatStamp(p.created_at)}>{ago(p.created_at)}</span>
            </A>
          );
        }}
      </For>
      <Show when={newest().length > RECENT_ROWS}>
        <button class="cx-showall cap" onClick={() => setAll(!all())}>{all() ? 'Show fewer' : `Show all ${newest().length}`}</button>
      </Show>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* One build                                                            */
/* ------------------------------------------------------------------ */

export function BuildRun(props: Ctx) {
  const params = useParams<{ id: string }>();
  const [data, { refetch }] = createResource(
    () => ({ project: props.project.id, id: params.id }),
    (k) => consoleApi.build(k.project, k.id),
  );
  const [baseline] = useBuilds(() => props.project, () => 30, true);
  const run = () => data.latest?.run;
  createEffect(() => {
    if (run()?.status !== 'running') return;
    const t = setInterval(() => void refetch(), 3000);
    onCleanup(() => clearInterval(t));
  });

  const good = createMemo(() => (baseline.latest?.runs ?? []).filter((r) => r.status === 'success' && r.kind === run()?.kind && r.id !== run()?.id));
  const typicalTotal = createMemo(() => (good().length ? median(good().map((r) => r.total_ms)) : null));
  const phaseTypical = createMemo<Partial<BuildPhases>>(() =>
    good().length ? Object.fromEntries(PHASES.map((p) => [p, median(good().map((r) => r.phases[p]))])) : {},
  );
  const rows = createMemo(() => {
    const r = run();
    return r ? waterfall(r, data.latest?.steps ?? [], baseline.latest?.steps ?? [], phaseTypical()) : [];
  });
  const [touched, setTouched] = createSignal<Set<string> | null>(null);
  const open = createMemo<ReadonlySet<string>>(() => touched() ?? defaultOpen(rows()));
  const toggle = (key: string) => {
    const next = new Set(open());
    if (!next.delete(key)) next.add(key);
    setTouched(next);
  };
  const shown = createMemo(() => visibleRows(rows(), open()));
  const span = createMemo(() => Math.max(1, run()?.total_ms ?? 0, ...rows().map((r) => r.startMs + r.durationMs)));
  const ticks = createMemo(() => niceTicks(span(), 6).filter((t) => t <= span()));
  const pct = (ms: number) => `${(ms / span()) * 100}%`;
  const vsTypical = () => {
    const r = run();
    const t = typicalTotal();
    return r && t ? (r.total_ms - t) / t : null;
  };

  return (
    <>
      <PageHead crumb="Build time" crumbHref="/deployments/builds" title={run() ? (run()!.version != null ? `v${run()!.version}` : 'Build') : 'Build'} subtitle={run() ? <span class="bt-commit"><Show when={run()!.commit_sha}><b>{sha7(run()!.commit_sha!)}</b> </Show>{whatOf(run()!)}</span> : undefined} />
      <div class="fx-page">
        <Show when={data.error}>
          <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{String(data.error?.message ?? data.error)}</span></div></FxCell></section>
        </Show>
        <Show when={run()} fallback={<section class="fx-row"><FxCell><div class="bt-empty cap">{data.error ? '' : 'Loading'}</div></FxCell></section>}>
          {(r) => (
            <>
              <section class="fx-row bt-tiles">
                <FxCell label={r().status === 'running' ? 'So far' : 'Push to live'} meta={<span class={`cx-st ${toneOf(r())}`}><Sq tone={toneOf(r())} />{r().status}</span>}>
                  <span class="bt-figure hero">{formatBuildTime(r().total_ms)}</span>
                  <Show when={typicalTotal() != null && r().status === 'success'} fallback={<span class="bt-delta none">{ago(r().created_at)}</span>}>
                    <Delta pct={vsTypical()} from={typicalTotal()} label="typical" />
                  </Show>
                </FxCell>
                <FxCell label="Queue">
                  <span class="bt-figure">{formatBuildTime(r().phases.queue)}</span>
                  <span class="bt-delta none">waiting for a builder</span>
                </FxCell>
                <FxCell label="Build">
                  <span class="bt-figure">{formatBuildTime(r().phases.build)}</span>
                  <span class="bt-delta none">{r().detailed && r().kind === 'build' ? `${r().apps_built} built, ${r().apps_reused} reused` : 'no step timing'}</span>
                </FxCell>
                <FxCell label="Deploy">
                  <span class="bt-figure">{formatBuildTime(r().phases.infra + r().phases.migrate + r().phases.rollout)}</span>
                  <span class="bt-delta none">infra, migrations, rollout</span>
                </FxCell>
                <FxCell label="Layers cached">
                  <span class="bt-figure">{r().layers ? `${Math.round((r().layers_cached / r().layers) * 100)}%` : '—'}</span>
                  <span class="bt-delta none">{r().layers ? `${r().layers_cached} of ${r().layers}` : 'no layers recorded'}</span>
                </FxCell>
              </section>

              <Show when={r().error}>
                <section class="fx-row"><FxCell label="Why it failed"><div class="fx-log bad"><pre class="fx-what bt-err">{r().error}</pre></div></FxCell></section>
              </Show>

              <section class="fx-row">
                <FxCell
                  label="Waterfall"
                  meta={
                    <span class="bt-wf-key">
                      <span><i class="tick" />typical</span>
                      <span><i class="hollow" />cached</span>
                    </span>
                  }
                >
                  <Show when={!r().detailed}>
                    <p class="bt-note">{r().kind === 'deploy' ? 'A deployment nothing here built: only its rollout was timed.' : 'This build predates step timing, so only its phases are known.'}</p>
                  </Show>
                  <div class="bt-wf" role="table" aria-label="Where this build's time went">
                    <div class="bt-wf-axis" aria-hidden="true">
                      <span class="cap">Step</span>
                      <div class="bt-track">
                        <For each={ticks()}>
                          {(t) => <span class="bt-wf-tick" style={{ left: pct(t) }}>{formatTick(t)}</span>}
                        </For>
                      </div>
                      <span class="cap num">Took</span>
                    </div>
                    <For each={shown()}>
                      {(row) => <WaterfallLine row={row} pct={pct} open={open().has(row.key)} onToggle={() => toggle(row.key)} ticks={ticks()} minTick={span() * MIN_TICK_SHARE} />}
                    </For>
                  </div>
                </FxCell>
              </section>
            </>
          )}
        </Show>
      </div>
    </>
  );
}

function WaterfallLine(props: { row: WaterfallRow; pct: (ms: number) => string; open: boolean; onToggle: () => void; ticks: number[]; minTick: number }) {
  const r = () => props.row;
  const skipped = () => r().status === 'cached' || r().status === 'reused';
  const diff = () => (r().typicalMs != null ? r().durationMs - r().typicalMs! : null);
  const noteworthy = () => {
    const d = diff();
    return d != null && Math.abs(d) >= Math.max(2000, r().typicalMs! * 0.1);
  };
  return (
    <div class={`bt-wf-row ${r().kind}`} classList={{ failed: r().status === 'failed' }} role="row">
      <span class="bt-wf-name" style={{ 'padding-left': `${r().depth * 16}px` }} role="cell">
        <Show when={r().children > 0} fallback={<i class="bt-tw none" />}>
          <button class="bt-tw" aria-expanded={props.open} aria-label={`${props.open ? 'Collapse' : 'Expand'} ${r().label}`} onClick={props.onToggle}>{props.open ? '▾' : '▸'}</button>
        </Show>
        <Show when={r().kind === 'phase'}><i class="bt-swatch" style={{ background: `var(--ph-${r().phase})` }} /></Show>
        <span class="t" title={r().label}>{r().label}</span>
        <Show when={r().status === 'failed'}><span class="bad cap">failed</span></Show>
        <Show when={r().status === 'reused'}><span class="dim cap">reused</span></Show>
      </span>
      <div class="bt-track" role="cell">
        <For each={props.ticks}>{(t) => <i class="bt-wf-grid" style={{ left: props.pct(t) }} />}</For>
        <span
          class="bt-wf-bar"
          classList={{ hollow: skipped() }}
          style={{ left: props.pct(r().startMs), width: props.pct(r().durationMs), background: skipped() ? undefined : `var(--ph-${r().phase})`, 'border-color': skipped() ? `var(--ph-${r().phase})` : undefined }}
        />
        <Show when={r().typicalMs != null && r().typicalMs! >= props.minTick}>
          <i class="bt-wf-typical" style={{ left: props.pct(r().startMs + r().typicalMs!) }} title={`Typically ${formatBuildTime(r().typicalMs!)}`} />
        </Show>
      </div>
      <span class="bt-wf-took num" role="cell">
        <b>{skipped() && r().durationMs < 1000 ? r().status : formatBuildTime(r().durationMs)}</b>
        <Show when={noteworthy()}>
          <span class="bt-delta" classList={{ worse: diff()! > 0, better: diff()! < 0 }}>{formatDelta(diff()!)}</span>
        </Show>
      </span>
    </div>
  );
}
