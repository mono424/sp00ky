/**
 * Audience: how many people are online and how many views they hold, over time
 * and by hour of the week. The numbers are the control plane's, so this page
 * works whether or not the project's own dashboard could open.
 */

import { For, Show, createMemo, createResource, createSignal, onCleanup, type JSX } from 'solid-js';
import { PageHead } from '../components/Chrome';
import { FxCell } from '../components/Frame';
import { useOverview } from '../ProjectApp';
import { consoleApi, type Workspace, type WorkspaceProject } from './cloud';
import { ago } from './chrome';
import {
  DAY_NAMES,
  STEP_MS,
  cellLabel,
  changePct,
  countTicks,
  dayTicks,
  formatCount,
  heatLevel,
  heatMax,
  nearestIndex,
  onlineNow,
  peakPeople,
  people,
  segments,
  trailingAverage,
  viewsPerPerson,
  windowCells,
  windowLabel,
  type ActivityCell,
  type ActivityPoint,
  type ActivityReport,
} from './audienceStats';

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
const HOURS = Array.from({ length: 24 }, (_, h) => h);
const MARGIN = { l: 48, r: 14, t: 12, b: 26 };

const zone = () => Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
const stamp = (ms: number, step: ActivityReport['step']) =>
  new Date(ms).toLocaleString(undefined, step === '1d' ? { month: 'short', day: 'numeric', year: 'numeric' } : { weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });

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

/** More people is good news here, so the colors run the other way from build time. */
function Change(props: { pct: number | null; label: string }) {
  return (
    <Show when={props.pct != null} fallback={<span class="bt-delta none">no earlier data to compare</span>}>
      <span class="bt-delta" classList={{ better: props.pct! >= 0.02, worse: props.pct! <= -0.1 }}>
        <b>{props.pct! > 0 ? '▲' : props.pct! < 0 ? '▼' : '■'} {Math.abs(Math.round(props.pct! * 100))}%</b> {props.label}
      </span>
    </Show>
  );
}

export function Audience(props: Ctx) {
  const [days, setDays] = createSignal(30);
  const [data, { refetch }] = createResource(
    () => ({ id: props.project.id, days: days() }),
    (k) => consoleApi.activity(k.id, k.days, zone()),
  );
  // The clock moves on its own, so a live feed that froze stops reading as live.
  const [clock, setClock] = createSignal(Date.now());
  const timer = setInterval(() => {
    setClock(Date.now());
    void refetch();
  }, 60_000);
  onCleanup(() => clearInterval(timer));
  const overview = useOverview();

  const report = () => data.latest;
  const [hover, setHover] = createSignal<{ ms: number; owner: string } | null>(null);
  const to = () => Date.now();
  const from = () => to() - days() * 86_400_000;
  const series = createMemo(() => report()?.series ?? []);
  const label = () => `vs prior ${days()}d`;
  const trendMs = () => (days() >= 30 ? 7 : 1) * 86_400_000;
  const trendLabel = () => (days() >= 30 ? '7-day average' : '24-hour average');
  const change = (pick: (s: ActivityReport['current']) => number) => {
    const r = report();
    return r ? changePct(pick(r.current), pick(r.prior), r.prior.measured) : null;
  };
  const perPerson = (s: ActivityReport['current']) => (s.people_avg >= 0.5 ? s.views_avg / s.people_avg : 0);
  const online = (latest: ActivityPoint) => {
    clock();
    return onlineNow(overview()?.presence, latest);
  };
  const historyDays = () => {
    const first = report()?.first_at;
    return first ? Math.floor((Date.now() - Date.parse(first)) / 86_400_000) : 0;
  };

  return (
    <>
      <PageHead
        title="Audience"
        actions={
          <span class="bt-seg" role="group" aria-label="Time range">
            <For each={RANGES}>
              {(r) => <button aria-pressed={days() === r.days} onClick={() => setDays(r.days)}>{r.label}</button>}
            </For>
          </span>
        }
      />
      <div class="fx-page" classList={{ 'bt-stale': data.loading && !!data.latest }}>
        <Show when={data.error}>
          <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{String(data.error?.message ?? data.error)}</span></div></FxCell></section>
        </Show>
        <Show
          when={report()?.latest}
          fallback={
            <section class="fx-row">
              <FxCell>
                <div class="bt-empty">
                  <Show when={data.loading} fallback={<NoData project={props.project} />}><span class="cap">Loading</span></Show>
                </div>
              </FxCell>
            </section>
          }
        >
          {(latest) => (
            <>
              <section class="fx-row bt-tiles">
                <FxCell
                  label={online(latest()).live ? 'Online now' : 'Last measured peak'}
                  meta={<span>{online(latest()).live ? 'live' : ago(latest().at)}</span>}
                >
                  <span class="bt-figure hero">{online(latest()).people.toLocaleString()}</span>
                  <span class="bt-delta none">{online(latest()).signedIn.toLocaleString()} signed in, {online(latest()).signedOut.toLocaleString()} signed out</span>
                </FxCell>
                <FxCell label="Online on average">
                  <span class="bt-figure">{formatCount(report()!.current.people_avg)}</span>
                  <Change pct={change((s) => s.people_avg)} label={label()} />
                </FxCell>
                <FxCell label="Busiest moment">
                  <span class="bt-figure">{formatCount(report()!.current.people_peak)}</span>
                  <Change pct={change((s) => s.people_peak)} label={label()} />
                </FxCell>
                <FxCell label="Views registered">
                  <span class="bt-figure">{formatCount(report()!.current.views_avg)}</span>
                  <Change pct={change((s) => s.views_avg)} label={label()} />
                </FxCell>
                <FxCell label="Views per person">
                  <span class="bt-figure">{formatCount(perPerson(report()!.current))}</span>
                  <Change pct={change(perPerson)} label={label()} />
                </FxCell>
              </section>

              <section class="fx-row">
                <div class="fx-cell">
                  <div class="bt-chart-head">
                    <div>
                      <h2 class="bt-title">People online</h2>
                      <p class="bt-sub">Signed-in users and signed-out sessions with a live view. A tab that closes leaves within a minute; one that dies takes up to 10.</p>
                    </div>
                    <div class="bt-legend" aria-label="Legend">
                      <span><i class="stroke" style={{ background: 'var(--accent-strong)' }} />{trendLabel()}</span>
                      <span><i class="stroke au-faint" style={{ background: 'var(--accent-strong)' }} />Measured</span>
                      <span><i class="au-band" />Peak</span>
                    </div>
                  </div>
                  <TimeChart id="people" points={series()} step={report()!.step} from={from()} to={to()} height={230} value={people} peak={peakPeople} trendMs={trendMs()} color="var(--accent-strong)" hover={hover()} setHover={setHover} />

                  <div class="au-sub-chart">
                    <h2 class="bt-title">Views registered</h2>
                    <TimeChart id="views" points={series()} step={report()!.step} from={from()} to={to()} height={150} value={(p) => p.views_avg} peak={(p) => p.views_max} trendMs={trendMs()} color="var(--ph-build)" hover={hover()} setHover={setHover} />
                  </div>

                  <div class="au-sub-chart">
                    <h2 class="bt-title">Views per person</h2>
                    <TimeChart id="engagement" points={series()} step={report()!.step} from={from()} to={to()} height={110} value={(p) => viewsPerPerson(p)} trendMs={trendMs()} color="var(--ph-source)" hover={hover()} setHover={setHover} />
                  </div>
                </div>
              </section>

              <section class="fx-row au-week">
                <FxCell label="When people are online" meta={<span>{report()!.tz.replace(/_/g, ' ')} · last {days()} days</span>}>
                  <Heatmap cells={report()!.cells} quiet={report()!.quiet} />
                </FxCell>
                <FxCell label="Quietest hours">
                  <Show
                    when={report()!.quiet.length}
                    fallback={<p class="au-note">Needs a full week of history to say, {historyDays()} of 7 days so far.</p>}
                  >
                    <ol class="au-quiet">
                      <For each={report()!.quiet}>
                        {(w, i) => (
                          <li>
                            <span class="bt-flag">{i() + 1}</span>
                            <b>{windowLabel(w)}</b>
                            <span class="dim">{formatCount(w.people)} online on average</span>
                          </li>
                        )}
                      </For>
                    </ol>
                    <p class="au-note">The hours to aim backups and upgrades at. Nothing is scheduled from this yet.</p>
                  </Show>
                </FxCell>
              </section>
            </>
          )}
        </Show>
      </div>
    </>
  );
}

function NoData(props: { project: WorkspaceProject }): JSX.Element {
  return (
    <Show
      when={props.project.platform !== 'cloudflare'}
      fallback={<p class="au-note">Cloudflare projects are not measured yet.</p>}
    >
      <p class="au-note">Nothing measured yet. A project's scheduler reports every 15 minutes once it runs a release with activity reporting, so upgrade it and check back shortly after.</p>
    </Show>
  );
}

function TimeChart(props: {
  id: string;
  points: ActivityPoint[];
  step: ActivityReport['step'];
  from: number;
  to: number;
  height: number;
  value: (p: ActivityPoint) => number | null;
  peak?: (p: ActivityPoint) => number;
  trendMs: number;
  color: string;
  hover: { ms: number; owner: string } | null;
  setHover: (h: { ms: number; owner: string } | null) => void;
}) {
  const [measure, width] = useWidth();
  const plotH = () => props.height - MARGIN.t - MARGIN.b;
  const x = (ms: number) => MARGIN.l + ((ms - props.from) / (props.to - props.from)) * (width() - MARGIN.l - MARGIN.r);
  const ticks = createMemo(() => {
    const top = Math.max(0, ...props.points.map((p) => (props.peak ? props.peak(p) : props.value(p) ?? 0)));
    return countTicks(top);
  });
  const yMax = () => ticks()[ticks().length - 1] || 1;
  const y = (v: number) => MARGIN.t + plotH() * (1 - v / yMax());
  const parts = createMemo(() => segments(props.points.filter((p) => props.value(p) != null), STEP_MS[props.step]));
  const trend = createMemo(() => {
    const values = trailingAverage(props.points, props.value, props.trendMs);
    return new Map(props.points.map((p, i) => [p.at, values[i]]));
  });
  const trendParts = createMemo(() => segments(props.points.filter((p) => trend().get(p.at) != null), STEP_MS[props.step]));
  const line = (run: ActivityPoint[], pick: (p: ActivityPoint) => number) =>
    run.map((p, i) => `${i ? 'L' : 'M'}${x(Date.parse(p.at)).toFixed(1)} ${y(pick(p)).toFixed(1)}`).join('');
  const band = (run: ActivityPoint[]) => {
    const top = line(run, props.peak!);
    const bottom = [...run].reverse().map((p) => `L${x(Date.parse(p.at)).toFixed(1)} ${y(props.value(p)!).toFixed(1)}`).join('');
    return `${top}${bottom}Z`;
  };
  const dates = createMemo(() => dayTicks(props.from, props.to, Math.floor(width() / 80)));
  const at = (e: PointerEvent) => {
    const r = (e.currentTarget as Element).getBoundingClientRect();
    return props.from + ((e.clientX - r.left - MARGIN.l) / (width() - MARGIN.l - MARGIN.r)) * (props.to - props.from);
  };
  const hit = createMemo(() => {
    if (!props.hover) return null;
    const i = nearestIndex(props.points, props.hover.ms);
    return i < 0 ? null : props.points[i];
  });
  const tipLeft = () => (hit() ? x(Date.parse(hit()!.at)) : 0);
  return (
    <div class="bt-plot" ref={measure}>
      <svg
        width={width()}
        height={props.height}
        role="img"
        aria-label={`${props.id} over time`}
        onPointerMove={(e) => props.setHover({ ms: at(e), owner: props.id })}
        onPointerLeave={() => props.setHover(null)}
      >
        <For each={ticks()}>
          {(t) => (
            <>
              <line x1={MARGIN.l} x2={width() - MARGIN.r} y1={y(t)} y2={y(t)} class={t === 0 ? 'bt-axis' : 'bt-grid'} />
              <text x={MARGIN.l - 10} y={y(t) + 3.5} class="bt-tick" text-anchor="end">{formatCount(t)}</text>
            </>
          )}
        </For>
        <For each={dates()}>
          {(d) => <text x={x(d)} y={props.height - 8} class="bt-tick" text-anchor="middle">{new Date(d).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}</text>}
        </For>
        <For each={parts()}>
          {(run) => (
            <>
              <Show when={props.peak}><path d={band(run)} style={{ fill: props.color }} class="au-peak" /></Show>
              <path d={line(run, (p) => props.value(p)!)} class="au-detail" style={{ stroke: props.color }} />
              <Show when={run.length === 1}><circle cx={x(Date.parse(run[0].at))} cy={y(props.value(run[0])!)} r="3" style={{ fill: props.color }} /></Show>
            </>
          )}
        </For>
        <For each={trendParts()}>
          {(run) => <path d={line(run, (p) => trend().get(p.at)!)} class="bt-line" style={{ stroke: props.color }} />}
        </For>
        <Show when={hit()}>
          {(p) => (
            <>
              <line x1={x(Date.parse(p().at))} x2={x(Date.parse(p().at))} y1={MARGIN.t} y2={MARGIN.t + plotH()} class="bt-cross" />
              <Show when={props.value(p()) != null}>
                <circle cx={x(Date.parse(p().at))} cy={y(props.value(p())!)} r="4.5" class="bt-hot" style={{ fill: props.color }} />
              </Show>
            </>
          )}
        </Show>
      </svg>
      <Show when={props.hover?.owner === props.id && hit()}>
        {(p) => (
          <div class="bt-tip" style={{ left: `${tipLeft() + 240 > width() ? tipLeft() - 236 : tipLeft() + 16}px`, top: '8px', width: '220px' }}>
            <div class="bt-tip-head"><b>{formatCount(people(p()))}</b><span>online</span></div>
            <div class="bt-tip-rows">
              <div><i style={{ background: 'var(--accent-strong)' }} /><b>{formatCount(p().users_avg)}</b><span>signed in</span></div>
              <div><i style={{ background: 'var(--accent)' }} /><b>{formatCount(p().anon_avg)}</b><span>signed out</span></div>
              <div><i style={{ background: 'var(--ink-faint)' }} /><b>{formatCount(peakPeople(p()))}</b><span>at the peak</span></div>
              <div><i style={{ background: 'var(--ph-build)' }} /><b>{formatCount(p().views_avg)}</b><span>views</span></div>
            </div>
            <div class="bt-tip-foot"><span class="m">{stamp(Date.parse(p().at), props.step)}</span></div>
          </div>
        )}
      </Show>
    </div>
  );
}

function Heatmap(props: { cells: ActivityCell[]; quiet: ActivityReport['quiet'] }) {
  const byCell = createMemo(() => new Map(props.cells.map((c) => [`${c.day}-${c.hour}`, c])));
  const max = createMemo(() => heatMax(props.cells));
  const quiet = createMemo(() => new Set(props.quiet.flatMap(windowCells)));
  const [read, setRead] = createSignal<string>('');
  const describe = (day: number, hour: number) => {
    const c = byCell().get(`${day}-${hour}`);
    return `${cellLabel(day, hour)}: ${c ? `${formatCount(c.people)} online, ${formatCount(c.views)} views on average` : 'not measured'}`;
  };
  return (
    <div class="au-heat" onPointerLeave={() => setRead('')}>
      <div class="au-heat-grid" role="grid" aria-label="People online by hour of the week">
        <span />
        <For each={HOURS}>{(h) => <span class="au-hour" role="columnheader">{h % 3 === 0 ? String(h).padStart(2, '0') : ''}</span>}</For>
        <For each={DAY_NAMES}>
          {(name, d) => (
            <>
              <span class="au-day" role="rowheader">{name}</span>
              <For each={HOURS}>
                {(h) => {
                  const cell = () => byCell().get(`${d() + 1}-${h}`);
                  return (
                    <span
                      class="au-cell"
                      role="gridcell"
                      classList={{ none: !cell(), quiet: quiet().has(`${d() + 1}-${h}`) }}
                      style={cell() ? { '--level': heatLevel(cell()!.people, max()) } : undefined}
                      aria-label={describe(d() + 1, h)}
                      title={describe(d() + 1, h)}
                      onPointerEnter={() => setRead(describe(d() + 1, h))}
                    />
                  );
                }}
              </For>
            </>
          )}
        </For>
      </div>
      <div class="au-heat-foot">
        <span class="au-read">{read() || 'Hover an hour to read it'}</span>
        <span class="au-scale" aria-hidden="true">
          <span>Empty</span>
          <i />
          <span>{formatCount(max())} online</span>
        </span>
      </div>
    </div>
  );
}
