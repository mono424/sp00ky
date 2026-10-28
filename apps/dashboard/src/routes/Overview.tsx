import { For, Show, createMemo, type JSX } from 'solid-js';
import { A } from '@solidjs/router';
import { PageHead, Pill } from '../components/Chrome';
import { Sparkline, type Point } from '../components/Sparkline';
import { ActivityStrip } from '../components/OpCard';
import { SchedulerActions } from '../components/ClusterActions';
import { currentMode } from '../api/client';
import { formatCount, formatMs, formatUptime, splitValue } from '../lib/format';
import { sspTone, type Tone } from '../lib/status';
import type { Overview as OverviewData } from '../api/types';

/**
 * The home screen answers one question first: is anything wrong?
 *
 * Top to bottom:
 *
 *  1. A headline: "All systems normal", or how many things need attention.
 *  2. The things that need attention, one line each with where to go. This
 *     list is empty, and so absent, on a healthy cluster.
 *  3. Four numbers worth glancing at: sync latency, live users, SSPs ready,
 *     jobs queued.
 *  4. What is running right now (restarts, backups, restores).
 *
 * Everything else (scheduler details, sessions, view counts, the fleets as
 * tables, the incident strip) has its own page in the sidebar. A figure only
 * earns a place here by being the first thing an operator looks at, or by
 * being wrong.
 */

interface Issue {
  tone: 'warn' | 'bad';
  title: string;
  detail?: string;
  to: string;
}

const plural = (n: number, word: string) => `${formatCount(n)} ${word}${n === 1 ? '' : 's'}`;

/** Every condition worth an operator's attention, worst first. */
function issuesOf(d: OverviewData): Issue[] {
  const out: Issue[] = [];
  const s = d.scheduler;
  // An SSP an operation is already restarting has its own progress card.
  const busy = new Set(
    (d.operations ?? []).filter((o) => o.status === 'running').map((o) => o.target),
  );

  if (s && s.status !== 'ready') {
    out.push({
      tone: 'warn',
      title: `Scheduler is ${s.status}`,
      detail: 'Ingest and queries may pause until it is ready.',
      to: '/logs?source=scheduler',
    });
  }

  const hb = s?.heartbeat;
  if (hb?.enabled && (hb.stale || (hb.consecutive_failures ?? 0) > 0)) {
    out.push({
      tone: 'warn',
      title: 'Sync probe is failing',
      detail:
        hb.blocked_reason ??
        `${plural(hb.consecutive_failures ?? 0, 'probe')} in a row did not reach every SSP.`,
      to: '/ssps',
    });
  }

  if (d.totals.ssps === 0) {
    out.push({ tone: 'bad', title: 'No SSPs registered', detail: 'No client receives live updates.', to: '/ssps' });
  }
  for (const ssp of d.ssps) {
    if (ssp.status === 'ready' || busy.has(ssp.id)) continue;
    const b = ssp.bootstrap;
    out.push({
      tone: sspTone(ssp.status) === 'warn' ? 'warn' : 'bad',
      title: `${ssp.id} is ${ssp.status}`,
      detail: b
        ? `${b.tables_done}/${b.tables_total} tables loaded. Its clients wait until it is ready.`
        : 'Its clients have no live queries until it is ready.',
      to: '/ssps',
    });
  }

  for (const b of d.backends) {
    if (['healthy', 'idle', 'starting'].includes(b.status)) continue;
    out.push({
      tone: 'bad',
      title: `${b.name ?? b.id ?? 'Backend'} is ${b.status}`,
      detail: 'Work routed to it fails or waits.',
      to: '/backends',
    });
  }

  if ((s?.lag ?? 0) > 1000) {
    out.push({
      tone: 'warn',
      title: 'Ingest is falling behind',
      detail: `${formatCount(s!.lag)} events not yet applied to the replica.`,
      to: '/logs?source=scheduler',
    });
  }

  const j = d.jobs;
  if (j?.ready) {
    if (j.blind) {
      out.push({ tone: 'bad', title: 'Job tables are not answering', detail: 'Queue figures are unknown.', to: '/jobs' });
    } else if (j.stalled > 0) {
      out.push({ tone: 'bad', title: plural(j.stalled, 'stalled job'), detail: 'A lease expired with nobody holding it.', to: '/jobs' });
    }
    if (j.counts.failed > 0) {
      out.push({ tone: 'warn', title: `${plural(j.counts.failed, 'job')} failed in the last hour`, to: '/jobs' });
    }
  }

  const p = d.presence;
  if (p?.ready && p.totals.errored_views > 0) {
    out.push({ tone: 'bad', title: `${plural(p.totals.errored_views, 'view')} errored`, detail: 'Their clients see stale data.', to: '/views' });
  } else if (p?.ready && p.totals.slow_views > 0) {
    out.push({ tone: 'warn', title: plural(p.totals.slow_views, 'slow view'), to: '/views' });
  }

  const inc = d.incidents;
  if (inc?.storage_error) {
    out.push({ tone: 'warn', title: 'Incident history is unavailable', to: '/incidents' });
  } else if (inc && inc.open > 0) {
    const l = inc.latest;
    out.push({
      tone: 'warn',
      title: plural(inc.open, 'open incident'),
      detail: l ? `Latest: ${l.component} ${l.kind.replace(/_/g, ' ')}` : undefined,
      to: '/incidents',
    });
  }

  // Stable: faults first, then warnings, each in the order found above.
  return [...out.filter((i) => i.tone === 'bad'), ...out.filter((i) => i.tone === 'warn')];
}

function Chevron() {
  return (
    <svg class="ov-arrow" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M6 4l4 4-4 4" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" />
    </svg>
  );
}

/**
 * Change across the chart's window: first good sample to last. Null when the
 * window is too short or starts at zero, where a percentage means nothing.
 */
function trend(points: Point[] | undefined): number | null {
  const v = (points ?? []).filter((p) => p.ok && typeof p.ms === 'number').map((p) => p.ms as number);
  if (v.length < 3 || v[0] === 0) return null;
  const pct = ((v[v.length - 1] - v[0]) / v[0]) * 100;
  return Math.abs(pct) < 1 ? 0 : Math.round(pct);
}

function Stat(props: {
  label: string;
  value: JSX.Element;
  unit?: string;
  note?: JSX.Element;
  tone?: Tone;
  to: string;
  points?: Point[];
  format?: (v: number | null | undefined) => string;
  /** Drawn at the foot of the card in place of a chart. */
  children?: JSX.Element;
}) {
  return (
    <A href={props.to} class="ov-stat">
      <span class="ov-stat-label">
        {props.label}
        <Chevron />
      </span>
      <span class="ov-stat-value" classList={{ [`tone-${props.tone}`]: props.tone === 'warn' || props.tone === 'bad' }}>
        {props.value}
        <Show when={props.unit}>
          <span class="ov-stat-unit">{props.unit}</span>
        </Show>
      </span>
      <span class="ov-stat-note">
        <Show when={trend(props.points)}>
          {(t) => (
            <span class="ov-delta" classList={{ up: t() > 0, down: t() < 0 }}>
              {t() > 0 ? '↑' : '↓'} {Math.abs(t())}%
            </span>
          )}
        </Show>
        {props.note}
      </span>
      <Show when={(props.points?.length ?? 0) > 1}>
        <span class="ov-stat-plot">
          <Sparkline points={props.points!} fill bare format={props.format} ariaLabel={props.label} />
        </span>
      </Show>
      {props.children}
    </A>
  );
}

export function Overview(props: {
  data: OverviewData | undefined;
  error?: string;
  /** Painted from the stash; a live poll has not replaced it yet. */
  stale?: boolean;
  /** Re-poll the overview now, e.g. right after an action. */
  refresh: () => void;
}) {
  const issues = createMemo(() => (props.data ? issuesOf(props.data) : []));
  const worst = (): Tone =>
    issues().some((i) => i.tone === 'bad') ? 'bad' : issues().length ? 'warn' : 'ok';

  const sched = () => props.data?.scheduler;
  const hb = () => sched()?.heartbeat;
  const latency = () => splitValue(formatMs(hb()?.last_e2e_ms));
  const hbPoints = (): Point[] =>
    (hb()?.samples ?? []).map((s) => ({ ts: s.ts, ms: s.ms, ok: s.ok }));
  const presence = () => props.data?.presence;
  const userPoints = (): Point[] =>
    (presence()?.samples ?? []).map((s) => ({ ts: s.ts, ms: s.users, ok: true as const }));
  const jobs = () => props.data?.jobs;
  const jobPoints = (): Point[] =>
    (jobs()?.samples ?? []).map((s) => ({ ts: s.t, ms: s.pending, ok: true as const }));

  const headline = () => {
    const n = issues().length;
    if (n === 0) return 'All systems normal';
    return `${n} ${n === 1 ? 'thing needs' : 'things need'} attention`;
  };

  return (
    <>
      <PageHead
        title="Overview"
        actions={
          <>
            <Show when={props.data && !props.error && !props.stale}>
              <Pill tone="live" dot pulse>
                live
              </Pill>
            </Show>
            <Show when={props.stale}>
              <Pill tone="idle" dot pulse>
                refreshing
              </Pill>
            </Show>
            {/* Restart, reclone, rehash, cloud actions: the scheduler's own
                menu, kept here because the overview is where a cluster-wide
                fix starts. */}
            <Show when={props.data?.scheduler}>
              <SchedulerActions size="sm" label="Scheduler" onDone={props.refresh} />
            </Show>
          </>
        }
      />

      <div class="page-body">
        <Show when={props.error}>
          <div class="banner">
            <span class="dot bad" />
            {props.error}
          </div>
        </Show>

        <Show when={props.data} fallback={<div class="ov-skeleton" />}>
          {(d) => (
            <div class="ov">
              {/* 1. the headline */}
              <section class="ov-hero" classList={{ [worst()]: true }} style={{ '--n': 0 }}>
                <span class="ov-orb" aria-hidden="true">
                  <span class="ov-orb-ring" />
                  <span class="ov-orb-ring" />
                  <span class="ov-orb-core" />
                </span>
                <div class="ov-hero-text">
                  <div class="ov-hero-eyebrow">{currentMode().config?.project_slug ?? 'scheduler'}</div>
                  <h2 class="ov-hero-title">{headline()}</h2>
                  <div class="ov-hero-sub">
                    {issues().length === 0
                      ? 'Every sync processor is serving and nothing is queued up behind a fault.'
                      : 'Start at the top: problems are listed worst first.'}
                  </div>
                </div>
                <Show when={sched()}>
                  {(s) => (
                    <dl class="ov-facts">
                      <div>
                        <dt>Uptime</dt>
                        <dd>{formatUptime(s().uptime_seconds)}</dd>
                      </div>
                      <div>
                        <dt>Version</dt>
                        <dd class="mono">{s().version.replace(/^0\.0\.1-/, '')}</dd>
                      </div>
                      <div>
                        <dt>SurrealDB</dt>
                        <dd class="mono">{s().surrealdb_version ?? '—'}</dd>
                      </div>
                    </dl>
                  )}
                </Show>
              </section>

              {/* 2. what is wrong, only when something is */}
              <Show when={issues().length > 0}>
                <ul class="ov-issues" style={{ '--n': 1 }}>
                  <For each={issues()}>
                    {(i) => (
                      <li>
                        <A href={i.to} class="ov-issue" classList={{ [i.tone]: true }}>
                          <span class="ov-issue-icon" aria-hidden="true">
                            <svg viewBox="0 0 16 16">
                              <Show
                                when={i.tone === 'bad'}
                                fallback={<path d="M8 2.8 14 13H2zM8 6.6v3M8 11.4h.01" />}
                              >
                                <path d="M8 1.8a6.2 6.2 0 1 0 0 12.4A6.2 6.2 0 0 0 8 1.8zM5.9 5.9l4.2 4.2m0-4.2-4.2 4.2" />
                              </Show>
                            </svg>
                          </span>
                          <span class="ov-issue-text">
                            <span class="ov-issue-title">{i.title}</span>
                            <Show when={i.detail}>
                              <span class="ov-issue-detail">{i.detail}</span>
                            </Show>
                          </span>
                          <Chevron />
                        </A>
                      </li>
                    )}
                  </For>
                </ul>
              </Show>

              {/* 3. the four numbers */}
              <div class="ov-stats" style={{ '--n': 2 }}>
                <Stat
                  label="Sync latency"
                  to="/ssps"
                  value={latency().value}
                  unit={latency().unit}
                  note="write to every client"
                  points={hbPoints()}
                />
                <Stat
                  label="Live users"
                  to="/views"
                  value={presence()?.ready ? formatCount(presence()!.totals.users) : '—'}
                  note={
                    presence()?.ready
                      ? `${formatCount(presence()!.totals.sessions)} open tabs`
                      : 'waiting for a sample'
                  }
                  points={userPoints()}
                  format={formatCount}
                />
                <Stat
                  label="Sync processors"
                  to="/ssps"
                  value={`${d().totals.ssps_ready}/${d().totals.ssps}`}
                  unit="ready"
                  tone={d().totals.ssps > 0 && d().totals.ssps_ready === d().totals.ssps ? 'ok' : 'warn'}
                  note={`${formatCount(sched()?.views ?? 0)} live queries`}
                >
                  <span class="ov-fleet" aria-hidden="true">
                    <For each={d().ssps}>
                      {(ssp) => <span class="ov-fleet-bar" classList={{ [sspTone(ssp.status)]: true }} title={`${ssp.id} · ${ssp.status}`} />}
                    </For>
                  </span>
                </Stat>
                <Stat
                  label="Jobs queued"
                  to="/jobs"
                  value={jobs()?.ready ? formatCount(jobs()!.counts.pending) : '—'}
                  note={
                    jobs()?.ready
                      ? `${formatCount(jobs()!.throughput_1m)} done per minute`
                      : 'waiting for a sample'
                  }
                  points={jobPoints()}
                  format={formatCount}
                />
              </div>

              {/* 4. what is running */}
              <ActivityStrip operations={d().operations} ssps={d().ssps} />
            </div>
          )}
        </Show>
      </div>
    </>
  );
}
