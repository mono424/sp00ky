import { For, Show, createMemo, createResource, onCleanup, type JSX } from 'solid-js';
import { A } from '@solidjs/router';
import { PageHead } from '../components/Chrome';
import { SchedulerActions } from '../components/ClusterActions';
import { ActivityStrip } from '../components/OpCard';
import {
  Barcode,
  DayTicks,
  FxCell,
  Gauge,
  LineChart,
  Matrix,
  Meter,
  Slots,
  Sq,
  memText,
  usage,
} from '../components/Frame';
import { api } from '../api/client';
import { formatBytes, formatCount, formatMs, formatRelativeTime, formatUptime } from '../lib/format';
import { backendTone, sspTone } from '../lib/status';
import { retryJob } from '../lib/runActions';
import type {
  BackendSummary,
  CloudService,
  CloudStats,
  JobsListResponse,
  Overview as OverviewData,
  SspEntity,
} from '../api/types';

/**
 * The home screen, in three bands an operator reads top to bottom:
 *
 *  1. Sync: how long a write takes to reach every client, and who is here.
 *  2. Health: the database and the scheduler, each with its CPU, memory and
 *     disk; then every service that scales (sync processors and backends) in
 *     one list, trouble first.
 *  3. Work: the job queue, its latest failure, and each queue's pace.
 *
 * Problems are shown where they live (an amber square, an amber reading, a
 * log line with its one action) rather than gathered in a list above.
 *
 * CPU, memory and disk come from Sp00ky Cloud's container metrics; a
 * self-hosted scheduler cannot see its neighbours, and those figures read
 * "not reported" there rather than zero.
 */

const plural = (n: number, word: string) => `${formatCount(n)} ${word}${n === 1 ? '' : 's'}`;

/** p-th percentile of a list of numbers. */
function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const s = [...values].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

/** A short line for why a job failed. */
function reasonOf(e: unknown): string | null {
  if (!e) return null;
  if (typeof e === 'string') return e;
  const r = e as { reason?: unknown; code?: unknown };
  return r.reason ? String(r.reason) : r.code ? String(r.code) : null;
}

const ICON = {
  db: 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M4 12c0 4 16 4 16 0',
  clock: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z M12 7v5l3 2',
  ssp: 'M4 3h16v7H4z M4 14h16v7H4z M7 6.5h1 M7 17.5h1',
};

function Icon(props: { d: string; class?: string }) {
  return (
    <svg class={props.class} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path d={props.d} />
    </svg>
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
  const d = () => props.data;
  const now = () => d()?.server_time_ms ?? Date.now();
  const sched = () => d()?.scheduler;
  const hb = () => sched()?.heartbeat;
  const presence = () => d()?.presence;
  const jobs = () => d()?.jobs;
  const incidents = () => d()?.incidents?.recent;
  /** How long ago, by the scheduler's clock: "4s", "12m". */
  const since = (iso: string | null | undefined) => {
    if (!iso) return '—';
    const t = Date.parse(iso);
    return Number.isNaN(t) ? '—' : formatRelativeTime(Date.now() - (now() - t)).replace(' ago', '');
  };

  // Container metrics, only where Sp00ky Cloud can answer for them.
  const [cloud, { refetch: refetchCloud }] = createResource(
    () => d()?.cloud_linked,
    () => api.getResult<CloudStats>('/cloud/stats'),
  );
  // The queue page the jobs sampler already keeps: per-table pace and the
  // newest failures. Cheap: it is the sampler's memory, not a fresh scan.
  const [queue, { refetch: refetchQueue }] = createResource(() => api.getResult<JobsListResponse>('/jobs'));
  const timer = setInterval(() => {
    if (d()?.cloud_linked) void refetchCloud();
    void refetchQueue();
  }, 15_000);
  onCleanup(() => clearInterval(timer));

  const services = createMemo(() => {
    const r = cloud();
    return r?.ok && r.value.metrics_available ? r.value.services : [];
  });
  const svc = (pick: (s: CloudService) => boolean) => usage(services().find(pick));
  const dbUse = () => svc((s) => s.role === 'surrealdb');
  const schedUse = () => svc((s) => s.role === 'scheduler');
  const byName = (name: string) => svc((s) => s.name === name);

  // ---- 01 sync -------------------------------------------------------

  const rtt = () =>
    (hb()?.samples ?? []).map((s) => ({ ts: s.ts, v: s.ok && typeof s.ms === 'number' ? s.ms : null }));
  const rttOk = () => rtt().map((p) => p.v).filter((v): v is number => v != null);
  const failedProbes = () => rtt().filter((p) => p.v == null).length;

  const users = () => (presence()?.samples ?? []).map((s) => s.users);
  const viewTrouble = () => {
    const t = presence()?.totals;
    if (!presence()?.ready || !t) return null;
    const parts: string[] = [];
    if (t.errored_views) parts.push(plural(t.errored_views, 'errored view'));
    if (t.slow_views) parts.push(plural(t.slow_views, 'slow view'));
    if (t.hot_views) parts.push(`${plural(t.hot_views, 'hot view')}`);
    return parts.length ? { text: parts.join(' · '), bad: t.errored_views > 0 } : null;
  };

  // ---- 02 health -----------------------------------------------------

  const probeOk = () => {
    const h = hb();
    if (!h?.enabled) return null;
    return !h.stale && (h.consecutive_failures ?? 0) === 0;
  };

  const ssps = createMemo(() => {
    const rank = (s: SspEntity) => (s.status !== 'ready' ? 0 : (s.publication?.pending_operations ?? 0) > 0 ? 1 : 2);
    return [...(d()?.ssps ?? [])].sort((a, b) => rank(a) - rank(b) || a.id.localeCompare(b.id));
  });
  // Declared before the memos that sort by it: a memo runs as soon as it is
  // created, and on a reload the stash hands it data at once.
  const nameOf = (b: BackendSummary) => b.name ?? b.id ?? 'backend';
  const backends = createMemo(() => {
    const fine = ['healthy', 'idle', 'starting'];
    const rank = (b: BackendSummary) => (fine.includes(b.status) ? 1 : 0);
    return [...(d()?.backends ?? [])].sort((a, b) => rank(a) - rank(b) || nameOf(a).localeCompare(nameOf(b)));
  });

  // ---- 03 work -------------------------------------------------------

  const page = () => {
    const r = queue();
    return r?.ok ? r.value : null;
  };
  const failures = createMemo(() =>
    (page()?.jobs ?? [])
      .filter((j) => j.status === 'failed')
      .sort((a, b) => (b.updated_at ?? '').localeCompare(a.updated_at ?? '')),
  );
  const tables = createMemo(() =>
    [...(page()?.tables ?? [])].sort(
      (a, b) => b.counts.failed - a.counts.failed || b.throughput_1m - a.throughput_1m || a.table.localeCompare(b.table),
    ),
  );
  const jobSamples = () => jobs()?.samples ?? [];

  // Problems with no home of their own on this page.
  const loose = createMemo(() => {
    const out: { text: string; to: string; bad?: boolean }[] = [];
    const x = d();
    if (!x) return out;
    if (x.totals.ssps === 0) out.push({ text: 'No sync processor is registered · no client receives live updates', to: '/ssps', bad: true });
    if (x.incidents?.storage_error) out.push({ text: 'Incident history is unavailable', to: '/incidents' });
    else if ((x.incidents?.open ?? 0) > 0) {
      const l = x.incidents!.latest;
      out.push({
        text: `${plural(x.incidents!.open, 'open incident')}${l ? ` · ${l.component} ${l.kind.replace(/_/g, ' ')}` : ''}`,
        to: '/incidents',
      });
    }
    if (x.jobs?.ready && x.jobs.blind) out.push({ text: 'Job tables are not answering · queue figures are unknown', to: '/runs', bad: true });
    return out;
  });

  return (
    <>
      <PageHead
        title="Overview"
        actions={
          <>
            <Show when={props.stale}>
              <span class="cap">refreshing</span>
            </Show>
            <Show when={hb()?.last_e2e_ms != null}>
              <span class="cap" style={{ display: 'inline-flex', gap: '8px', 'align-items': 'center', 'margin-right': '6px' }}>
                <Sq tone={probeOk() === false ? 'warn' : ''} />
                Round trip <b style={{ color: 'var(--ink)' }}>{formatMs(hb()!.last_e2e_ms)}</b>
              </span>
            </Show>
            <Show when={sched()}>
              <SchedulerActions size="sm" label="Scheduler" onDone={props.refresh} />
            </Show>
            <A class="btn btn-sm" href="/backups">Back up</A>
          </>
        }
      />

      <Show when={props.error}>
        <div class="fx-row">
          <FxCell>
            <div class="fx-log bad">
              <span class="fx-what">{props.error}</span>
            </div>
          </FxCell>
        </div>
      </Show>

      <Show when={d()} fallback={<div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div>}>
        <div class="fx-page">
          <Show when={loose().length > 0}>
            <section class="fx-row">
              <FxCell label="Needs attention">
                <For each={loose()}>
                  {(l) => (
                    <div class="fx-log" classList={{ bad: l.bad }}>
                      <span class="fx-what">{l.text}</span>
                      <A class="btn btn-sm" href={l.to}>Open</A>
                    </div>
                  )}
                </For>
              </FxCell>
            </section>
          </Show>

          <Show when={(d()!.operations ?? []).length > 0}>
            <section class="fx-row">
              <FxCell label="Running now">
                <ActivityStrip operations={d()!.operations} ssps={d()!.ssps} />
              </FxCell>
            </section>
          </Show>

          {/* 01 sync ===================================================== */}
          <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
            <FxCell
              label="Sync round trip"
              meta={
                <>
                  <Show when={percentile(rttOk(), 99)}>
                    {(p) => (
                      <span>
                        p99 <b>{formatMs(p())}</b>
                      </span>
                    )}
                  </Show>
                  <Show when={rttOk().length}>
                    <span>
                      peak <b>{formatMs(Math.max(...rttOk()))}</b>
                    </span>
                  </Show>
                  <span classList={{ warn: failedProbes() > 0 }}>
                    <b>{failedProbes()}</b> failed
                  </span>
                </>
              }
            >
              <div class="fx-big">
                {hb()?.last_e2e_ms != null ? Math.round(hb()!.last_e2e_ms!) : '—'}
                <small>ms</small>
              </div>
              <LineChart points={rtt()} format={(v) => `${Math.round(v)} ms`} now={now()} />
            </FxCell>

            <FxCell label="Live users">
              <div class="fx-big">{presence()?.ready ? formatCount(presence()!.totals.users) : '—'}</div>
              <Show when={presence()?.ready}>
                <div class="fx-sub cap">
                  <span>
                    <b>{formatCount(presence()!.totals.sessions)}</b> tabs
                  </span>
                  <span>
                    <b>{formatCount(presence()!.totals.views)}</b> views
                  </span>
                  <span>
                    <b>{formatCount(presence()!.totals.anon_sessions)}</b> anon
                  </span>
                </div>
              </Show>
              <Matrix values={users()} />
              <Show when={viewTrouble()}>
                {(t) => (
                  <div class="fx-log" classList={{ bad: t().bad }} style={{ 'margin-top': '14px' }}>
                    <span class="fx-what">{t().text}</span>
                    <A class="btn btn-sm" href="/views">Views</A>
                  </div>
                )}
              </Show>
            </FxCell>
          </section>

          {/* 02 health: the two singletons ================================ */}
          <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1fr))' }}>
            <div class="fx-cell">
              <div class="fx-head fx-title">
                <Icon d={ICON.db} />
                <span class="fx-name">SurrealDB</span>
                <span class="cap" style={{ color: 'var(--ink-ghost)' }}>{sched()?.surrealdb_version ?? ''}</span>
                <span class="fx-meta cap">
                  <span class="fx-state" classList={{ warn: probeOk() === false }}>
                    <Sq tone={probeOk() === false ? 'warn' : probeOk() == null ? 'idle' : ''} />
                    {probeOk() === false ? 'Probe failing' : probeOk() == null ? 'Unprobed' : 'Healthy'}
                  </span>
                </span>
              </div>
              <Usage use={dbUse()} now={now()} />
              <div class="fx-foot">
                <div>
                  <span class="cap">Write to live</span>
                  <b classList={{ warn: probeOk() === false }}>
                    {probeOk() == null ? '—' : probeOk() ? 'OK' : `${hb()?.consecutive_failures ?? 0} failed`}
                  </b>
                </div>
                <div>
                  <span class="cap">Disk writes</span>
                  <b>
                    {dbUse()?.diskWriteBps != null ? (
                      <>
                        {Math.round(dbUse()!.diskWriteBps! / 1024)} <em>KB/s</em>
                      </>
                    ) : (
                      '—'
                    )}
                  </b>
                </div>
                <div>
                  <span class="cap">Up</span>
                  <b>{dbUse()?.since ? formatUptime((now() - Date.parse(dbUse()!.since)) / 1000) : '—'}</b>
                </div>
              </div>
            </div>

            <div class="fx-cell">
              <div class="fx-head fx-title">
                <Icon d={ICON.clock} />
                <span class="fx-name">Scheduler</span>
                <span class="cap" style={{ color: 'var(--ink-ghost)' }}>
                  {sched() ? `${formatCount(sched()!.views)} views` : ''}
                </span>
                <span class="fx-meta cap">
                  <span class="fx-state" classList={{ warn: !!sched() && sched()!.status !== 'ready' }}>
                    <Sq tone={!sched() ? 'idle' : sched()!.status === 'ready' ? '' : 'warn'} />
                    {sched()?.status ?? 'unknown'}
                  </span>
                </span>
              </div>
              <Usage use={schedUse()} now={now()} />
              <div class="fx-foot">
                <div>
                  <span class="cap">Ingest lag</span>
                  <b classList={{ warn: (sched()?.lag ?? 0) > 1000 }}>
                    {formatCount(sched()?.lag ?? 0)} <em>events</em>
                  </b>
                </div>
                <div>
                  <span class="cap">Pending events</span>
                  <b>{formatCount(sched()?.pending_events ?? 0)}</b>
                </div>
                <div>
                  <span class="cap">Up</span>
                  <b>{formatUptime(sched()?.uptime_seconds)}</b>
                </div>
              </div>
            </div>
          </section>

          {/* 02 health: every service that scales ========================= */}
          <section class="fx-row">
            <FxCell
              label="Services"
              meta={
                <span>
                  <b>{d()!.ssps.length + d()!.backends.length}</b> running
                </span>
              }
            >
              <table class="fx-list">
                <colgroup>
                  <col style={{ width: '24%' }} />
                  <col style={{ width: '8%' }} />
                  <col style={{ width: '8%' }} />
                  <col style={{ width: '8%' }} />
                  <col style={{ width: '17%' }} />
                  <col style={{ width: '17%' }} />
                  <col style={{ width: '6%' }} />
                  <col style={{ width: '12%' }} />
                </colgroup>
                <tbody>
                  <tr class="fx-gh">
                    <th class="cap">
                      <span class="fx-gt">Sync processors</span>
                      <span>
                        {d()!.totals.ssps_ready} of {d()!.totals.ssps} ready
                      </span>
                    </th>
                    <th class="cap r">Views</th>
                    <th class="cap r">Heartbeat</th>
                    <th class="cap r">Backlog</th>
                    <th class="cap">CPU</th>
                    <th class="cap">Memory</th>
                    <th class="cap r">Up</th>
                    <th class="cap r">Last 24h</th>
                  </tr>
                  <For each={ssps()} fallback={<EmptyRow text="No sync processor is registered" />}>
                    {(s) => {
                      const u = () => byName(s.id);
                      const backlog = () => s.publication?.pending_operations ?? 0;
                      const memHigh = () => (u()?.memPct ?? 0) >= 80;
                      const tone = () => (sspTone(s.status) !== 'ok' ? sspTone(s.status) : backlog() > 0 || memHigh() ? 'warn' : '');
                      return (
                        <tr>
                          <td>
                            <span class="fx-who">
                              <Icon d={ICON.ssp} class="acc" />
                              <Sq tone={tone()} />
                              <A href="/ssps">{s.id}</A>
                              <Show when={s.status !== 'ready'}>
                                <span class="cap warn">{s.status}</span>
                              </Show>
                              <Show when={s.status === 'ready' && memHigh()}>
                                <span class="cap warn">memory high</span>
                              </Show>
                            </span>
                          </td>
                          <td class="r">{formatCount(s.views)}</td>
                          <td class="r" classList={{ warn: s.last_heartbeat_seconds_ago > 30 }}>
                            {s.last_heartbeat_seconds_ago < 1 ? 'now' : `${Math.round(s.last_heartbeat_seconds_ago)}s`}
                          </td>
                          <td class="r" classList={{ warn: backlog() > 0 }}>
                            {backlog() > 0 ? `${formatCount(backlog())} ops` : '0'}
                          </td>
                          <td>
                            <Meter pct={u()?.cpuPct}>{Math.round(u()?.cpuPct ?? 0)}%</Meter>
                          </td>
                          <td>
                            <MemMeter use={u()} />
                          </td>
                          <td class="r">{formatUptime(s.uptime_seconds)}</td>
                          <td>
                            <DayTicks component={s.id} incidents={incidents()} uptimeSecs={s.uptime_seconds} now={now()} />
                          </td>
                        </tr>
                      );
                    }}
                  </For>
                </tbody>
                <Show when={backends().length > 0}>
                  <tbody>
                    <tr class="fx-gh">
                      <th class="cap">
                        <span class="fx-gt">Backends</span>
                        <span>
                          {d()!.totals.backends_healthy} of {d()!.totals.backends} healthy
                        </span>
                      </th>
                      <th class="cap r">Response</th>
                      <th class="cap r">Checked</th>
                      <th class="cap r">Machines</th>
                      <th class="cap">CPU</th>
                      <th class="cap">Memory</th>
                      <th class="cap r">Up</th>
                      <th class="cap r">Last 24h</th>
                    </tr>
                    <For each={backends()}>
                      {(b) => {
                        const u = () => byName(nameOf(b));
                        const t = () => backendTone(b.status);
                        return (
                          <tr>
                            <td>
                              <span class="fx-who">
                                <Icon d={ICON.db} />
                                <Sq tone={t() === 'ok' ? ((u()?.memPct ?? 0) >= 80 ? 'warn' : '') : t()} />
                                <A href={`/backends/${encodeURIComponent(nameOf(b))}`}>{nameOf(b)}</A>
                                <Show when={!['healthy', 'idle'].includes(b.status)}>
                                  <span class="cap warn">{b.status}</span>
                                </Show>
                              </span>
                            </td>
                            <td class="r">{b.response_time_ms != null ? formatMs(b.response_time_ms) : '—'}</td>
                            <td class="r">
                              {since(b.last_checked)}
                            </td>
                            <td class="r">{b.pool ? `${b.pool.ready}${b.pool.starting ? ` +${b.pool.starting}` : ''}` : '—'}</td>
                            <td>
                              <Meter pct={u()?.cpuPct}>{Math.round(u()?.cpuPct ?? 0)}%</Meter>
                            </td>
                            <td>
                              <MemMeter use={u()} />
                            </td>
                            <td class="r">
                              {u()?.since ? formatUptime((now() - Date.parse(u()!.since)) / 1000) : '—'}
                            </td>
                            <td>
                              <DayTicks component={nameOf(b)} incidents={incidents()} now={now()} />
                            </td>
                          </tr>
                        );
                      }}
                    </For>
                  </tbody>
                </Show>
              </table>
            </FxCell>
          </section>

          {/* 03 work ====================================================== */}
          <section class="fx-row">
            <FxCell
              label="Jobs"
              meta={
                <Show when={jobs()?.ready && jobs()!.counts.failed > 0} fallback={<A href="/runs">Open runs</A>}>
                  <span class="warn">{formatCount(jobs()!.counts.failed)} failed in 1h</span>
                </Show>
              }
            >
              <Show when={failures()[0]}>
                {(j) => (
                  <div class="fx-log" style={{ 'margin-bottom': '18px' }}>
                    <span class="cap">
                      {j().updated_at
                        ? new Date(j().updated_at!).toLocaleTimeString([], { hour12: false })
                        : ''}
                    </span>
                    <span class="fx-what">
                      {j().path ?? j().table}
                      {reasonOf(j().last_error) ? ` · ${reasonOf(j().last_error)}` : ''}
                      {j().max_retries != null ? ` · attempt ${j().attempts} of ${(j().max_retries ?? 0) + 1}` : ''}
                    </span>
                    <button class="btn btn-sm" onClick={() => retryJob(j().id, () => void refetchQueue())}>
                      Retry
                    </button>
                    <Show when={failures().length > 1}>
                      <A class="btn btn-sm" href="/runs?status=failed">
                        All {failures().length}
                      </A>
                    </Show>
                  </div>
                )}
              </Show>

              <div class="fx-figure">
                <div class="fx-big">
                  {jobs()?.ready ? formatCount(jobs()!.throughput_1m) : '—'}
                  <small>/ min</small>
                </div>
                <Show when={jobs()?.ready}>
                  <div class="fx-inline">
                    <Inline label="Pending" value={formatCount(jobs()!.counts.pending)} />
                    <Inline label="Running" value={formatCount(jobs()!.in_flight)} />
                    <Inline label="Stalled" value={formatCount(jobs()!.stalled)} warn={jobs()!.stalled > 0} />
                    <Inline
                      label="Oldest wait"
                      value={since(jobs()!.oldest_pending)}
                    />
                  </div>
                </Show>
              </div>

              <Barcode
                values={jobSamples().map((s) => s.throughput_1m)}
                warn={jobSamples().map((s, i) => s.failed > (jobSamples()[i - 1]?.failed ?? 0))}
              />

              <Show when={tables().length > 0}>
                <table class="fx-list" style={{ 'margin-top': '18px' }}>
                  <colgroup>
                    <col style={{ width: '30%' }} />
                    <col style={{ width: '22%' }} />
                    <col style={{ width: '12%' }} />
                    <col style={{ width: '12%' }} />
                    <col style={{ width: '12%' }} />
                    <col style={{ width: '12%' }} />
                  </colgroup>
                  <thead>
                    <tr>
                      <th class="cap">Queue</th>
                      <th class="cap">Running / limit</th>
                      <th class="cap r">Rate</th>
                      <th class="cap r">Waiting</th>
                      <th class="cap r">Failed 1h</th>
                      <th class="cap r">Oldest</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={tables()}>
                      {(t) => (
                        <tr>
                          <td>
                            <A href={`/runs?table=${encodeURIComponent(t.table)}`} style={{ color: 'var(--ink)' }}>
                              {t.table}
                            </A>
                            <Show when={t.error}>
                              <span class="cap warn" style={{ 'margin-left': '8px' }}>not answering</span>
                            </Show>
                          </td>
                          <td>
                            <Slots used={t.in_flight} limit={t.concurrency} />
                          </td>
                          <td class="r">{formatCount(t.throughput_1m)}/m</td>
                          <td class="r">{formatCount(t.counts.pending)}</td>
                          <td class="r" classList={{ warn: t.counts.failed > 0 }}>{formatCount(t.counts.failed)}</td>
                          <td class="r">
                            {since(t.oldest_pending)}
                          </td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </Show>
            </FxCell>
          </section>
        </div>
      </Show>
    </>
  );
}

/** CPU, memory and disk of one container; "not reported" where unknown. */
function Usage(props: { use: ReturnType<typeof usage>; now: number }) {
  const mem = () => (props.use?.memBytes != null ? memText(props.use.memBytes) : null);
  const limit = () => (props.use ? memText(props.use.memLimit) : null);
  const disk = () => (props.use?.diskBytes != null ? memText(props.use.diskBytes) : null);
  return (
    <div class="fx-gauges">
      <Gauge
        label="CPU"
        of={props.use ? `${props.use.vcpus} vCPU` : undefined}
        value={props.use?.cpuPct != null ? String(Math.round(props.use.cpuPct)) : null}
        unit="%"
        pct={props.use?.cpuPct ?? null}
        series={props.use?.cpuSeries}
      />
      <Gauge
        label="Memory"
        of={limit() ? `${limit()!.value} ${limit()!.unit}` : undefined}
        value={mem()?.value ?? null}
        unit={mem()?.unit}
        pct={props.use?.memPct ?? null}
        series={props.use?.memSeries}
      />
      <Gauge
        label="Disk"
        of={props.use?.diskRoom != null ? formatBytes(props.use.diskRoom) : undefined}
        value={disk()?.value ?? null}
        unit={disk()?.unit}
        pct={props.use?.diskPct ?? null}
        note={
          props.use?.diskFree != null ? (
            <>
              <span>{formatBytes(props.use.diskFree)} free on host</span>
              <Show when={props.use.diskAt}>
                {(at) => <span>measured {formatUptime(Math.max(0, props.now - Date.parse(at())) / 1000)} ago</span>}
              </Show>
            </>
          ) : undefined
        }
      />
    </div>
  );
}

function MemMeter(props: { use: ReturnType<typeof usage> }) {
  const m = () => (props.use?.memBytes != null ? memText(props.use.memBytes) : null);
  const l = () => (props.use ? memText(props.use.memLimit) : null);
  return (
    <Meter pct={props.use?.memPct}>
      {m()?.value} <em>/ {l()?.value} {l()?.unit}</em>
    </Meter>
  );
}

function Inline(props: { label: string; value: JSX.Element; warn?: boolean }) {
  return (
    <div>
      <span class="cap">{props.label}</span>
      <b classList={{ warn: props.warn }}>{props.value}</b>
    </div>
  );
}

function EmptyRow(props: { text: string }) {
  return (
    <tr>
      <td colSpan={8} class="cap" style={{ color: 'var(--ink-ghost)' }}>
        {props.text}
      </td>
    </tr>
  );
}
