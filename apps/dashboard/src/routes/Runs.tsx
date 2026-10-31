import { For, Index, Show, createMemo, createResource, createSignal, onCleanup, type JSX } from 'solid-js';
import { Navigate, useLocation, useNavigate, useParams, useSearchParams } from '@solidjs/router';
import { api, openStream, streamsAvailable } from '../api/client';
import { PageHead } from '../components/Chrome';
import { runAction, post } from '../components/Actions';
import { Barcode, FilterMenu, FxCell, Sheet, SheetSection, Slots, Sq, type FilterValue } from '../components/Frame';
import { describeCron } from '../lib/cron';
import { decodeParam, formatCount, formatDuration, formatRelativeTime, formatStamp } from '../lib/format';
import { cancelRun, clearJobs, killJob, pauseSchedule, rerunRun, retryJob, retryRun, triggerSchedule } from '../lib/runActions';
import { readStash, writeStash } from '../lib/stash';
import type {
  JobDetail,
  JobSummary,
  JobsListResponse,
  RunError,
  Schedule,
  ScheduleDetailData,
  ScheduleRun,
  StepRun,
  WorkflowRun,
  WorkflowRunDetail,
} from '../api/types';

/**
 * Runs: everything the scheduler executes, on one page.
 *
 * Schedules, workflows and jobs are one machine seen from three sides: a
 * schedule fires either a job (a row in an outbox table that a backend runs at
 * a path) or a workflow run, and every step of a workflow run is itself a job.
 * Three pages meant one failure showed up three times. Here:
 *
 *  - the numbers: running, waiting, done per minute, failed;
 *  - schedules: what fires, when next, and how its last fires went;
 *  - runs: one list of what executed. A workflow run is one row and its step
 *    jobs live inside it; a job of its own (from a schedule, the app or a
 *    durable instance) is one row;
 *  - queues: each outbox table's pace against its concurrency.
 *
 * A run, job or schedule opens in a sheet. The sheet is in the URL
 * (`?open=run:<id>`, `job:<id>`, `schedule:<name>`), so it survives a refresh
 * and the old `/jobs/:id`, `/workflows/:id` and `/schedules/:name` links land
 * on it.
 */

const POLL_MS = 5_000;
const SCHEDULE_POLL_MS = 10_000;

type Status = 'running' | 'waiting' | 'stalled' | 'success' | 'failed' | 'skipped' | 'replaced' | 'killed';
type Origin = 'schedule' | 'app' | 'durable' | 'manual' | 'rerun';

interface Item {
  /** What `?open=` takes for it. */
  open: string;
  kind: 'workflow' | 'job';
  id: string;
  name: string;
  status: Status;
  origin: Origin;
  schedule: string | null;
  table: string | null;
  started: number | null;
  finished: number | null;
  error: string | null;
  sub: string | null;
  /** Workflow runs: the trigger; jobs: the raw summary. */
  run?: WorkflowRun;
  job?: JobSummary;
}

const ACTIVE: Status[] = ['running', 'waiting', 'stalled'];
const STATUSES: Status[] = ['running', 'waiting', 'stalled', 'failed', 'success', 'skipped', 'replaced', 'killed'];

/** A square's tone for a status: running in the accent, trouble in amber or red. */
const toneOf = (s: string): string =>
  s === 'running' || s === 'processing' ? 'run'
    : s === 'failed' ? 'bad'
      : s === 'stalled' ? 'warn'
        : s === 'success' ? ''
          : 'idle';

const time = (iso: string | null | undefined) => (iso ? Date.parse(iso) : null);

/** A short line for a recorded cause. */
export function reasonOf(e: RunError | string | null | undefined): string | null {
  if (!e) return null;
  if (typeof e === 'string') return e;
  return e.reason ? String(e.reason) : e.code ? String(e.code) : null;
}

function jobStatus(j: JobSummary, now: number): Status {
  if (j.status === 'processing') {
    const lease = time(j.lease_until);
    return lease != null && lease < now ? 'stalled' : 'running';
  }
  if (j.status === 'pending') return 'waiting';
  return (STATUSES as string[]).includes(j.status) ? (j.status as Status) : 'failed';
}

function runItem(r: WorkflowRun): Item {
  const origin: Origin = r.schedule_name ? 'schedule' : r.rerun_of ? 'rerun' : 'manual';
  return {
    open: `run:${r.id}`,
    kind: 'workflow',
    id: r.id,
    name: r.workflow_name,
    status: (r.status === 'running' ? 'running' : r.status) as Status,
    origin,
    schedule: r.schedule_name,
    table: null,
    started: time(r.created_at),
    finished: time(r.finished_at),
    error: reasonOf(r.error),
    sub: [r.id, r.retry_count ? `retried ${r.retry_count}×` : null, r.rerun_of ? `rerun of ${r.rerun_of}` : null].filter(Boolean).join(' · '),
    run: r,
  };
}

function jobItem(j: JobSummary, now: number): Item {
  const status = jobStatus(j, now);
  const done = status !== 'running' && status !== 'waiting' && status !== 'stalled';
  return {
    open: `job:${j.id}`,
    kind: 'job',
    id: j.id,
    name: j.path ?? j.table,
    status,
    origin: (j.origin?.kind === 'schedule' ? 'schedule' : j.origin?.kind === 'durable' ? 'durable' : 'app') as Origin,
    schedule: j.origin?.schedule ?? null,
    table: j.table,
    started: time(j.created_at),
    finished: done ? time(j.updated_at) : null,
    error: reasonOf(j.last_error as RunError | null),
    sub: [j.table, j.max_retries != null && j.attempts > 1 ? `attempt ${j.attempts} of ${j.max_retries + 1}` : null].filter(Boolean).join(' · '),
    job: j,
  };
}

/** How often a schedule fires, in words. */
function cadence(s: Schedule): string {
  if (s.cron) return describeCron(s.cron) ?? s.cron;
  if (s.every_ms) return `every ${formatDuration(s.every_ms)}`;
  return 'on demand';
}

/* ------------------------------------------------------------------ */
/* Data: one source each, streamed where the host can                   */
/* ------------------------------------------------------------------ */

function useJobs() {
  const [page, setPage] = createSignal<JobsListResponse | null>(readStash<JobsListResponse>('jobs')?.value ?? null);
  const [error, setError] = createSignal<string | null>(null);
  if (streamsAvailable()) {
    const close = openStream('/jobs/stream', {
      onOpen: () => setError(null),
      onEvent: (event, data) => {
        if (event !== 'jobs') return;
        try {
          const frame = JSON.parse(data) as JobsListResponse;
          setPage(frame);
          writeStash('jobs', frame);
        } catch {
          /* a malformed frame must not tear down the stream */
        }
      },
      onError: (err) => setError(err instanceof Error ? err.message : 'Stream disconnected'),
    });
    onCleanup(close);
  } else {
    const load = async () => {
      const r = await api.getResult<JobsListResponse>('/jobs');
      if (r.ok) {
        setPage(r.value);
        setError(null);
      } else setError(r.message);
    };
    void load();
    const t = setInterval(() => !document.hidden && void load(), POLL_MS);
    onCleanup(() => clearInterval(t));
  }
  return { page, error, refresh: () => void api.getResult<JobsListResponse>('/jobs').then((r) => r.ok && setPage(r.value)) };
}

function useWorkflowRuns() {
  const [runs, setRuns] = createSignal<WorkflowRun[]>([]);
  const load = async () => {
    const r = await api.getResult<{ runs: WorkflowRun[] }>('/workflows/runs?limit=100');
    if (r.ok) setRuns(r.value.runs);
  };
  if (streamsAvailable()) {
    const close = openStream('/workflows/stream', {
      onEvent: (event, data) => {
        if (event !== 'runs') return;
        try {
          setRuns((JSON.parse(data) as { runs: WorkflowRun[] }).runs);
        } catch {
          /* keep the last good frame */
        }
      },
    });
    onCleanup(close);
  } else {
    void load();
    const t = setInterval(() => !document.hidden && void load(), POLL_MS);
    onCleanup(() => clearInterval(t));
  }
  return { runs, refresh: load };
}

/* ------------------------------------------------------------------ */
/* The page                                                             */
/* ------------------------------------------------------------------ */

export function Runs() {
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams<{ open?: string; status?: string; table?: string; kind?: string; schedule?: string }>();
  const jobs = useJobs();
  const wf = useWorkflowRuns();
  const [scheduleRes, { refetch: refetchSchedules }] = createResource(() => api.getResult<{ schedules: Schedule[] }>('/schedules'));
  const st = setInterval(() => !document.hidden && void refetchSchedules(), SCHEDULE_POLL_MS);
  onCleanup(() => clearInterval(st));
  const schedules = () => {
    const r = scheduleRes();
    return r?.ok ? r.value.schedules : [];
  };

  // The scheduler's clock, ticking: durations of live runs grow every second.
  const [tick, setTick] = createSignal(Date.now());
  const ticker = setInterval(() => setTick(Date.now()), 1000);
  onCleanup(() => clearInterval(ticker));
  const skew = createMemo(() => {
    const at = jobs.page()?.sampled_at_ms;
    return at ? at - Date.now() : 0;
  });
  const now = () => tick() + skew();
  /** How long ago, by the scheduler's clock; a past event never reads as future. */
  const ago = (t: number | null) => (t == null ? '—' : formatRelativeTime(Date.now() - Math.max(0, now() - t)));
  /** How long until, for what is due: next fires. */
  const until = (t: number | null) => (t == null ? '—' : formatRelativeTime(Date.now() - (now() - t)));

  const refreshAll = () => {
    jobs.refresh();
    void wf.refresh();
    void refetchSchedules();
  };

  // Filters start from the link that brought us here: `/jobs?status=failed`.
  const initial: FilterValue = {};
  if (params.kind === 'job' || params.kind === 'workflow') initial.kind = [params.kind];
  if (params.status) initial.status = [params.status === 'pending' ? 'waiting' : params.status === 'processing' ? 'running' : params.status];
  if (params.table) initial.table = [params.table];
  if (params.schedule) initial.schedule = [params.schedule];
  const [filters, setFilters] = createSignal<FilterValue>(initial);
  if (params.kind || params.status || params.table || params.schedule) {
    setParams({ kind: undefined, status: undefined, table: undefined, schedule: undefined }, { replace: true });
  }
  const [search, setSearch] = createSignal('');
  const [shown, setShown] = createSignal(40);

  const pick = (k: string) => filters()[k] ?? [];

  /**
   * The streamed page holds only the newest jobs. A filter the scheduler can
   * answer (one status, one queue, an origin, a search) is asked of it
   * instead, so "failed" on a busy table finds failures older than the page.
   */
  const jobQuery = createMemo(() => {
    const q = new URLSearchParams();
    const st = pick('status');
    if (st.length === 1) {
      const mapped = st[0] === 'waiting' ? 'pending' : st[0] === 'running' || st[0] === 'stalled' ? 'processing' : st[0];
      if (['pending', 'processing', 'success', 'failed'].includes(mapped)) q.set('status', mapped);
    }
    if (pick('table').length === 1) q.set('table', pick('table')[0]);
    const origins = pick('origin').filter((o) => o === 'schedule' || o === 'app' || o === 'durable');
    if (origins.length === 1) q.set('origin', origins[0]);
    if (search().trim()) q.set('q', search().trim());
    return q.toString();
  });
  const [filtered, { refetch: refetchFiltered }] = createResource(
    () => jobQuery() || null,
    (q) => api.getResult<JobsListResponse>(`/jobs?${q}`),
  );
  const ft = setInterval(() => jobQuery() && !document.hidden && void refetchFiltered(), POLL_MS);
  onCleanup(() => clearInterval(ft));
  const jobList = () => {
    const f = filtered();
    return jobQuery() && f?.ok ? f.value.jobs : jobs.page()?.jobs ?? [];
  };

  /** Everything that executed: workflow runs, and jobs that are not a step of one. */
  const items = createMemo<Item[]>(() => {
    const out: Item[] = wf.runs().map(runItem);
    for (const j of jobList()) if (j.origin?.kind !== 'workflow') out.push(jobItem(j, now()));
    return out;
  });

  const visible = createMemo(() => {
    const q = search().trim().toLowerCase();
    return items().filter((i) =>
      (!pick('kind').length || pick('kind').includes(i.kind)) &&
      (!pick('status').length || pick('status').includes(i.status)) &&
      (!pick('origin').length || pick('origin').includes(i.origin)) &&
      (!pick('schedule').length || pick('schedule').includes(i.schedule ?? '')) &&
      (!pick('table').length || pick('table').includes(i.table ?? '')) &&
      (!q || `${i.name} ${i.id} ${i.error ?? ''} ${i.schedule ?? ''}`.toLowerCase().includes(q)),
    );
  });
  const live = createMemo(() =>
    visible().filter((i) => ACTIVE.includes(i.status)).sort((a, b) => (a.started ?? 0) - (b.started ?? 0)),
  );
  const recent = createMemo(() =>
    visible().filter((i) => !ACTIVE.includes(i.status)).sort((a, b) => (b.finished ?? b.started ?? 0) - (a.finished ?? a.started ?? 0)),
  );
  const filtering = () => Object.values(filters()).some((v) => v.length) || !!search().trim();

  // Each schedule's own fire history, for the strip. A request per schedule,
  // but there are few of them and the strip is the point of the table.
  const [history, { refetch: refetchHistory }] = createResource(
    () => schedules().map((x) => x.name).join('\n'),
    async (key) => {
      const names = key ? key.split('\n') : [];
      const all = await Promise.all(names.map((n) => api.getResult<ScheduleDetailData>(`/schedules/${encodeURIComponent(n)}`)));
      const out: Record<string, ScheduleRun[]> = {};
      all.forEach((r, i) => {
        if (r.ok) out[names[i]] = r.value.runs;
      });
      return out;
    },
  );
  const ht = setInterval(() => !document.hidden && void refetchHistory(), 30_000);
  onCleanup(() => clearInterval(ht));
  /** The last fires of a schedule, oldest first. */
  const firesOf = (name: string) =>
    [...(history()?.[name] ?? [])].sort((a, b) => (a.fire_at ?? a.created_at ?? '').localeCompare(b.fire_at ?? b.created_at ?? '')).slice(-12);

  const totals = () => jobs.page()?.totals;
  /** The sampler's last hour, one point per sample, behind every figure's strip. */
  const samples = () => totals()?.samples ?? [];
  const failedRuns = () => wf.runs().filter((r) => r.status === 'failed' && (time(r.finished_at) ?? 0) > now() - 3_600_000).length;
  const tables = () => jobs.page()?.tables ?? [];

  const open = (key: string) => setParams({ open: key });
  const close = () => setParams({ open: undefined });
  const opened = () => params.open ?? '';

  const count = (f: (i: Item) => boolean) => items().filter(f).length;

  return (
    <>
      <PageHead
        title="Runs"
        actions={
          <span class="cap lx-live" classList={{ on: !jobs.error() }}>
            <span class="lx-pulse" />
            {jobs.error() ? 'Disconnected' : 'Live'}
          </span>
        }
      />

      <Show when={jobs.error()}>
        <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{jobs.error()}</span></div></FxCell></section>
      </Show>

      <div class="fx-page">
        {/* the numbers ======================================================= */}
        <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
          <FxCell label="Running">
            <div class="fx-big">{count((i) => i.status === 'running')}</div>
            <div class="fx-sub cap">
              <span><b>{count((i) => i.kind === 'workflow' && i.status === 'running')}</b> workflows</span>
              <span><b>{count((i) => i.kind === 'job' && i.status === 'running')}</b> jobs</span>
              <Show when={totals()?.stalled}><span class="warn"><b>{totals()!.stalled}</b> stalled</span></Show>
            </div>
            <Barcode values={samples().map((x) => x.processing)} />
          </FxCell>
          <FxCell label="Waiting">
            <div class="fx-big">{totals() ? formatCount(totals()!.counts.pending) : '—'}</div>
            <div class="fx-sub cap"><span>oldest <b>{ago(time(totals()?.oldest_pending))}</b></span></div>
            <Barcode values={samples().map((x) => x.pending)} />
          </FxCell>
          <FxCell label="Done" meta={<span>per minute</span>}>
            <div class="fx-big">{totals() ? formatCount(totals()!.throughput_1m) : '—'}</div>
            <div class="fx-sub cap"><span><b>{totals() ? formatCount(totals()!.counts.success) : '—'}</b> in the last hour</span></div>
            <Barcode values={samples().map((x) => x.throughput_1m)} />
          </FxCell>
          <FxCell label="Failed" meta={<span>last hour</span>}>
            <div class="fx-big">
              <span classList={{ bad: (totals()?.counts.failed ?? 0) + failedRuns() > 0 }}>{(totals()?.counts.failed ?? 0) + failedRuns()}</span>
            </div>
            <div class="fx-sub cap">
              <span><b>{totals()?.counts.failed ?? 0}</b> jobs</span>
              <span><b>{failedRuns()}</b> workflows</span>
              <Show when={(totals()?.counts.failed ?? 0) + failedRuns() > 0}>
                <button class="fx-link cap" onClick={() => setFilters({ status: ['failed'] })}>show</button>
              </Show>
            </div>
            <Barcode values={samples().map((x) => x.failed)} bad={samples().map((x) => x.failed > 0)} />
          </FxCell>
        </section>

        {/* what fires ======================================================== */}
        <section class="fx-row" id="schedules">
          <FxCell label="Schedules" meta={<span><b>{schedules().length}</b> · next <b>{(() => {
            const n = schedules().filter((s) => !s.paused && s.next_fire_at).sort((a, b) => a.next_fire_at!.localeCompare(b.next_fire_at!))[0];
            return n ? `${n.name} ${until(time(n.next_fire_at))}` : '—';
          })()}</b></span>}>
            <Show when={schedules().length} fallback={<div class="fx-empty cap">No schedules are defined</div>}>
              <table class="fx-list rn-sched">
                <colgroup>
                  <col style={{ width: '27%' }} /><col style={{ width: '17%' }} /><col style={{ width: '20%' }} />
                  <col style={{ width: '11%' }} /><col style={{ width: '10%' }} /><col style={{ width: '15%' }} />
                </colgroup>
                <thead>
                  <tr>
                    <th class="cap">Schedule</th><th class="cap">Fires</th><th class="cap">Last fires</th>
                    <th class="cap r">Last</th><th class="cap r">Next</th><th class="cap" />
                  </tr>
                </thead>
                <tbody>
                  <For each={[...schedules()].sort((a, b) => Number(a.paused) - Number(b.paused) || (a.next_fire_at ?? '').localeCompare(b.next_fire_at ?? ''))}>
                    {(s) => {
                      const tone = () => (s.paused || s.config_disabled ? 'idle' : s.last_run_status === 'failed' ? 'bad' : '');
                      return (
                        <tr class="rn-click" classList={{ 'bu-dead': s.paused }} onClick={() => open(`schedule:${s.name}`)}>
                          <td>
                            <span class="fx-who"><Sq tone={tone()} /><span style={{ color: 'var(--ink)' }}>{s.name}</span></span>
                            <span class="vw-meta cap">
                              <span>{s.kind === 'workflow' ? 'workflow' : [s.target_table, s.path].filter(Boolean).join(' · ') || 'job'}</span>
                              <Show when={s.for_each}><span>for each</span></Show>
                              <Show when={s.paused}><span>paused</span></Show>
                            </span>
                          </td>
                          <td class="rn-cadence">{cadence(s)}</td>
                          <td onClick={(e) => e.stopPropagation()}><Fires runs={firesOf(s.name)} onOpen={open} /></td>
                          <td class="r" classList={{ bad: s.last_run_status === 'failed' }}>{ago(time(s.last_fire_at))}</td>
                          <td class="r">{s.paused ? 'paused' : until(time(s.next_fire_at))}</td>
                          <td class="r">
                            <span class="bu-actions" onClick={(e) => e.stopPropagation()}>
                              <button class="btn btn-sm" onClick={() => void triggerSchedule(s.name, refreshAll)}>Run now</button>
                              <button class="btn btn-sm" onClick={() => void pauseSchedule(s.name, !s.paused, refreshAll)}>{s.paused ? 'Resume' : 'Pause'}</button>
                            </span>
                          </td>
                        </tr>
                      );
                    }}
                  </For>
                </tbody>
              </table>
            </Show>
          </FxCell>
        </section>

        {/* what ran ========================================================== */}
        <section class="fx-row">
          <FxCell label="Runs" meta={<span><b>{formatCount(visible().length)}</b>{filtering() ? ` of ${formatCount(items().length)}` : ''}</span>}>
            <div class="inc-filters">
              <FilterMenu
                value={filters()}
                onChange={(v) => { setFilters(v); setShown(40); }}
                fields={[
                  { key: 'status', label: 'Status', options: STATUSES.filter((s) => count((i) => i.status === s)).map((s) => ({ value: s, label: s, count: count((i) => i.status === s), tone: toneOf(s) === 'run' ? '' : toneOf(s) })) },
                  { key: 'kind', label: 'Kind', options: [
                    { value: 'workflow', label: 'workflow run', count: count((i) => i.kind === 'workflow') },
                    { value: 'job', label: 'job', count: count((i) => i.kind === 'job') },
                  ] },
                  { key: 'origin', label: 'Started by', options: (['schedule', 'app', 'durable', 'manual', 'rerun'] as Origin[])
                    .filter((o) => count((i) => i.origin === o)).map((o) => ({ value: o, label: o === 'app' ? 'the app' : o, count: count((i) => i.origin === o) })) },
                  { key: 'schedule', label: 'Schedule', single: true, options: schedules().map((s) => ({ value: s.name, label: s.name, count: count((i) => i.schedule === s.name) })) },
                  { key: 'table', label: 'Queue', single: true, options: tables().map((t) => ({ value: t.table, label: t.table, count: count((i) => i.table === t.table) })) },
                ]}
              />
              <div class="inc-find">
                <input type="search" placeholder="Search name, id, error" value={search()} onInput={(e) => { setSearch(e.currentTarget.value); setShown(40); }} aria-label="Search runs" />
              </div>
            </div>

            <Show when={visible().length} fallback={<div class="fx-empty cap">{filtering() ? 'Nothing matches these filters' : 'Nothing has run yet'}</div>}>
              <table class="fx-list rn-list">
                <colgroup>
                  <col /><col style={{ width: '14%' }} /><col style={{ width: '10%' }} /><col style={{ width: '9%' }} />
                  <col style={{ width: '22%' }} /><col style={{ width: '170px' }} />
                </colgroup>
                <Show when={live().length}>
                  <tbody>
                    <tr class="fx-gh"><th class="cap" colSpan={6}><span class="fx-gt">Now</span><span>{live().length}</span></th></tr>
                    <For each={live()}>{(i) => <RunRow item={i} now={now()} ago={ago} onOpen={open} onDone={refreshAll} navigate={navigate} />}</For>
                  </tbody>
                </Show>
                <Show when={recent().length}>
                  <tbody>
                    <tr class="fx-gh"><th class="cap" colSpan={6}><span class="fx-gt">Recent</span><span>{recent().length}</span></th></tr>
                    <For each={recent().slice(0, shown())}>{(i) => <RunRow item={i} now={now()} ago={ago} onOpen={open} onDone={refreshAll} navigate={navigate} />}</For>
                  </tbody>
                </Show>
              </table>
              <Show when={recent().length > shown()}>
                <div class="inc-pager cap">
                  <span>{shown()} of {formatCount(recent().length)}</span>
                  <button class="btn btn-sm" onClick={() => setShown(shown() + 40)}>Show more</button>
                </div>
              </Show>
            </Show>
          </FxCell>
        </section>

        {/* where jobs wait ==================================================== */}
        <Show when={tables().length}>
          <section class="fx-row">
            <FxCell label="Queues" meta={<button class="fx-link cap" onClick={() => void clearJobs({ all: true }, refreshAll)}>clear finished</button>}>
              <table class="fx-list rn-queues">
                <colgroup>
                  <col style={{ width: '26%' }} /><col style={{ width: '18%' }} /><col style={{ width: '11%' }} /><col style={{ width: '11%' }} />
                  <col style={{ width: '11%' }} /><col style={{ width: '11%' }} /><col />
                </colgroup>
                <thead>
                  <tr>
                    <th class="cap">Queue</th><th class="cap">Running / limit</th><th class="cap r">Rate</th>
                    <th class="cap r">Waiting</th><th class="cap r">Failed 1h</th><th class="cap r">Oldest</th><th class="cap" />
                  </tr>
                </thead>
                <tbody>
                  <For each={tables()}>
                    {(t) => (
                      <tr class="rn-click" onClick={() => setFilters({ table: [t.table] })}>
                        <td>
                          <span class="fx-who"><Sq tone={t.error ? 'bad' : t.stalled ? 'warn' : ''} /><span style={{ color: 'var(--ink)' }}>{t.table}</span></span>
                          <Show when={t.error}><span class="vw-meta cap"><span class="bad">not answering</span></span></Show>
                        </td>
                        <td><Slots used={t.in_flight} limit={t.concurrency} /></td>
                        <td class="r">{formatCount(t.throughput_1m)}/m</td>
                        <td class="r">{formatCount(t.counts.pending)}</td>
                        <td class="r" classList={{ bad: t.counts.failed > 0 }}>{formatCount(t.counts.failed)}</td>
                        <td class="r">{ago(time(t.oldest_pending))}</td>
                        <td class="r">
                          <span class="bu-actions" onClick={(e) => e.stopPropagation()}>
                            <button class="btn btn-sm" title="Clear finished jobs" onClick={() => void clearJobs({ table: t.table }, refreshAll)}>Clear</button>
                          </span>
                        </td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </FxCell>
          </section>
        </Show>
      </div>

      {/* one thing, in a sheet that lives in the URL */}
      <Show when={opened().startsWith('run:')}>
        <RunSheet id={opened().slice(4)} now={now()} ago={ago} onOpen={open} onClose={close} onDone={refreshAll} navigate={navigate} />
      </Show>
      <Show when={opened().startsWith('job:')}>
        <JobSheet id={opened().slice(4)} ago={ago} onOpen={open} onClose={close} onDone={refreshAll} />
      </Show>
      <Show when={opened().startsWith('schedule:')}>
        <ScheduleSheet name={opened().slice(9)} ago={ago} until={until} onOpen={open} onClose={close} onDone={refreshAll} />
      </Show>
    </>
  );
}

/** A schedule's last fires, oldest left: one square each, opening what it ran. */
function Fires(props: { runs: ScheduleRun[]; onOpen: (key: string) => void }) {
  const slots = () => Array.from({ length: 12 }, (_, i) => props.runs[i - (12 - props.runs.length)] ?? null);
  const target = (r: ScheduleRun) => (r.workflow_run ? `run:${r.workflow_run}` : r.job_id ? `job:${r.job_id}` : null);
  return (
    <span class="rn-fires">
      <Index each={slots()}>
        {(r) => (
          <i
            class={r() ? `t-${toneOf(r()!.status) || 'ok'}` : 'none'}
            title={r() ? `${r()!.status} · ${formatStamp(r()!.fire_at ?? r()!.created_at)}${reasonOf(r()!.error) ? ` · ${reasonOf(r()!.error)}` : ''}` : undefined}
            onClick={() => r() && target(r()!) && props.onOpen(target(r()!)!)}
          />
        )}
      </Index>
    </span>
  );
}

function RunRow(props: {
  item: Item;
  now: number;
  ago: (t: number | null) => string;
  onOpen: (key: string) => void;
  onDone: () => void;
  navigate: (to: string) => void;
}) {
  const i = () => props.item;
  const took = () => {
    const s = i().started;
    if (s == null) return '—';
    return formatDuration(Math.max(0, (i().finished ?? props.now) - s));
  };
  const origin = () =>
    i().origin === 'schedule' ? i().schedule ?? 'schedule' : i().origin === 'app' ? 'the app' : i().origin;
  const stop = (e: Event) => e.stopPropagation();
  return (
    <tr class="rn-click" onClick={() => props.onOpen(i().open)}>
      <td>
        <span class="fx-who">
          <Sq tone={toneOf(i().status)} />
          <span style={{ color: 'var(--ink)' }}>{i().name}</span>
          <span class="rn-kind cap">{i().kind === 'workflow' ? 'workflow' : 'job'}</span>
        </span>
        <span class="vw-meta cap"><span>{i().sub}</span></span>
      </td>
      <td>
        <Show when={i().schedule} fallback={origin()}>
          <button class="vw-user" onClick={(e) => { stop(e); props.onOpen(`schedule:${i().schedule}`); }}>{origin()}</button>
        </Show>
      </td>
      <td class="r">{props.ago(i().started)}</td>
      <td class="r" classList={{ warn: i().status === 'stalled' }}>{took()}</td>
      <td class="bu-state" classList={{ bad: i().status === 'failed', warn: i().status === 'stalled' }} title={i().error ?? undefined}>
        {i().error ?? i().status}
      </td>
      <td class="r">
        <span class="bu-actions" onClick={stop}>
          <Show when={i().kind === 'workflow'}>
            <Show when={i().status === 'running'}><button class="btn btn-sm" onClick={() => void cancelRun(i().id, i().name, props.onDone)}>Cancel</button></Show>
            <Show when={i().status === 'failed'}><button class="btn btn-sm" onClick={() => void retryRun(i().id, i().name, props.onDone)}>Retry</button></Show>
            <Show when={!ACTIVE.includes(i().status)}><button class="btn btn-sm" onClick={() => void rerunRun(i().id, props.navigate)}>Rerun</button></Show>
          </Show>
          <Show when={i().kind === 'job'}>
            <Show when={i().status === 'failed'}><button class="btn btn-sm" onClick={() => void retryJob(i().id, props.onDone)}>Retry</button></Show>
            <Show when={ACTIVE.includes(i().status)}><button class="btn btn-sm" onClick={() => void killJob(i().id, props.onDone)}>Kill</button></Show>
          </Show>
        </span>
      </td>
    </tr>
  );
}

/* ------------------------------------------------------------------ */
/* Sheets                                                               */
/* ------------------------------------------------------------------ */

/** Poll one thing while its sheet is open. */
function useDetail<T>(path: () => string, everyMs = 4_000) {
  const [res, { refetch }] = createResource(path, (p) => api.getResult<T>(p));
  const t = setInterval(() => !document.hidden && void refetch(), everyMs);
  onCleanup(() => clearInterval(t));
  return {
    data: () => {
      const r = res();
      return r?.ok ? r.value : undefined;
    },
    error: () => {
      const r = res();
      return r && !r.ok ? r.message : undefined;
    },
    refetch: () => void refetch(),
  };
}

function Kv(props: { rows: [string, JSX.Element, string?][] }) {
  return (
    <div class="sp-kv">
      <For each={props.rows}>
        {([k, v, cls]) => (
          <div>
            <span class="cap">{k}</span>
            <b class={cls}>{v}</b>
          </div>
        )}
      </For>
    </div>
  );
}

function Json(props: { value: unknown }) {
  return <pre class="vw-pre dim rn-json">{props.value == null ? '—' : JSON.stringify(props.value, null, 2)}</pre>;
}

function SheetError(props: { message: string | undefined }) {
  return (
    <Show when={props.message}>
      <section class="sheet-sec"><div class="fx-log bad"><span class="fx-what">{props.message}</span></div></section>
    </Show>
  );
}

function RunSheet(props: {
  id: string;
  now: number;
  ago: (t: number | null) => string;
  onOpen: (key: string) => void;
  onClose: () => void;
  onDone: () => void;
  navigate: (to: string) => void;
}) {
  const d = useDetail<{ run: WorkflowRunDetail; steps: StepRun[] }>(() => `/workflows/runs/${encodeURIComponent(props.id)}`);
  const run = () => d.data()?.run;
  const steps = () => d.data()?.steps ?? [];
  const after = () => {
    d.refetch();
    props.onDone();
  };

  /** The run's time axis: from its start to its end, or now. */
  const span = createMemo(() => {
    const r = run();
    const t0 = time(r?.created_at) ?? props.now;
    const ends = steps().map((s) => time(s.finished_at)).filter((x): x is number => x != null);
    const t1 = time(r?.finished_at) ?? (r?.status === 'running' ? props.now : Math.max(t0 + 1, ...ends));
    return { t0, t1: Math.max(t1, t0 + 1) };
  });
  const startOf = (s: StepRun) => {
    if (s.started_at) return time(s.started_at)!;
    // Older rows have no start: the step began when its last dependency ended.
    const deps = steps().filter((x) => s.depends_on.includes(x.step)).map((x) => time(x.finished_at)).filter((x): x is number => x != null);
    return deps.length ? Math.max(...deps) : null;
  };

  return (
    <Sheet
      open
      wide
      onClose={props.onClose}
      title={<><Sq tone={toneOf(run()?.status ?? '')} />{run()?.workflow_name ?? props.id}</>}
      meta={<>workflow run · {props.id}</>}
    >
      <SheetError message={d.error()} />
      <Show when={run()}>
        {(r) => (
          <>
            <section class="sheet-sec rn-sheet-actions">
              <Show when={r().status === 'running'}><button class="btn btn-sm" onClick={() => void cancelRun(r().id, r().workflow_name, after)}>Cancel</button></Show>
              <Show when={r().status === 'failed'}><button class="btn btn-sm btn-primary" onClick={() => void retryRun(r().id, r().workflow_name, after)}>Retry from failed step</button></Show>
              <Show when={r().status !== 'running'}><button class="btn btn-sm" onClick={() => void rerunRun(r().id, props.navigate)}>Rerun from start</button></Show>
            </section>
            <SheetSection label="Run" meta={<span classList={{ bad: r().status === 'failed' }}>{r().status}</span>}>
              <Kv
                rows={[
                  ['Started', props.ago(time(r().created_at))],
                  ['Took', formatDuration(span().t1 - span().t0)],
                  ['Started by', r().schedule_name ? (
                    <button class="fx-link" onClick={() => props.onOpen(`schedule:${r().schedule_name}`)}>{r().schedule_name}</button>
                  ) : r().rerun_of ? (
                    <button class="fx-link" onClick={() => props.onOpen(`run:${r().rerun_of}`)}>rerun of {r().rerun_of}</button>
                  ) : (r().trigger ?? 'manual')],
                  ['Retries', String(r().retry_count ?? 0)],
                ]}
              />
              <Show when={reasonOf(r().error)}>
                <div class="fx-log bad" style={{ 'margin-top': '14px' }}><span class="fx-what">{reasonOf(r().error)}</span></div>
              </Show>
            </SheetSection>
            <SheetSection label="Steps" meta={<span><b>{steps().filter((s) => s.status === 'success').length}</b> of {steps().length} done</span>}>
              <div class="rn-steps">
                <For each={steps()}>
                  {(s) => {
                    const start = () => startOf(s);
                    const end = () => time(s.finished_at) ?? (s.status === 'processing' || s.status === 'running' ? props.now : null);
                    const x = (t: number) => Math.max(0, Math.min(100, ((t - span().t0) / (span().t1 - span().t0)) * 100));
                    return (
                      <button class="rn-step" onClick={() => s.job_id && props.onOpen(`job:${s.job_id}`)} disabled={!s.job_id}>
                        <span class="rn-step-name">
                          <Sq tone={toneOf(s.status)} />
                          <span>{s.step}</span>
                          <Show when={s.depends_on.length}><small class="cap">after {s.depends_on.join(', ')}</small></Show>
                        </span>
                        <span class="rn-track">
                          <Show when={start() != null && end() != null} fallback={<i class="rn-wait" />}>
                            <i class={`rn-bar t-${toneOf(s.status) || 'ok'}`} style={{ left: `${x(start()!)}%`, width: `${Math.max(0.6, x(end()!) - x(start()!))}%` }} />
                          </Show>
                        </span>
                        <span class="rn-step-t">
                          {start() != null && end() != null ? formatDuration(end()! - start()!) : s.status}
                        </span>
                        <Show when={reasonOf(s.error)}>
                          <span class="rn-step-err">{reasonOf(s.error)}{s.dispatch_attempts && s.dispatch_attempts > 1 ? ` · ${s.dispatch_attempts} attempts` : ''}</span>
                        </Show>
                      </button>
                    );
                  }}
                </For>
              </div>
            </SheetSection>
            <SheetSection label="Input"><Json value={r().input} /></SheetSection>
          </>
        )}
      </Show>
    </Sheet>
  );
}

function JobSheet(props: {
  id: string;
  ago: (t: number | null) => string;
  onOpen: (key: string) => void;
  onClose: () => void;
  onDone: () => void;
}) {
  const d = useDetail<{ job: JobDetail }>(() => `/jobs/${encodeURIComponent(props.id)}`);
  const j = () => d.data()?.job;
  const after = () => {
    d.refetch();
    props.onDone();
  };
  return (
    <Sheet
      open
      onClose={props.onClose}
      title={<><Sq tone={toneOf(j()?.status ?? '')} />{j()?.path ?? props.id}</>}
      meta={<>job · {j()?.table ?? ''}</>}
    >
      <SheetError message={d.error()} />
      <Show when={j()}>
        {(job) => (
          <>
            <section class="sheet-sec rn-sheet-actions">
              <Show when={job().status === 'failed'}><button class="btn btn-sm btn-primary" onClick={() => void retryJob(job().id, after)}>Retry</button></Show>
              <Show when={job().status === 'pending' || job().status === 'processing'}><button class="btn btn-sm" onClick={() => void killJob(job().id, after)}>Kill</button></Show>
              <Show when={job().origin?.workflow_run}>
                <button class="btn btn-sm" onClick={() => props.onOpen(`run:${job().origin!.workflow_run}`)}>Open its workflow run</button>
              </Show>
              <Show when={job().origin?.schedule}>
                <button class="btn btn-sm" onClick={() => props.onOpen(`schedule:${job().origin!.schedule}`)}>Open its schedule</button>
              </Show>
            </section>
            <SheetSection label="Job" meta={<span classList={{ bad: job().status === 'failed' }}>{job().status}</span>}>
              <Kv
                rows={[
                  ['Queue', job().table ?? '—'],
                  ['Started by', job().origin?.kind === 'workflow' ? `step ${job().origin?.step ?? ''}` : job().origin?.kind === 'schedule' ? (job().origin?.schedule ?? 'schedule') : job().origin?.kind === 'durable' ? 'durable instance' : 'the app'],
                  ['Attempts', `${(job().retries ?? 0) + (job().status === 'pending' ? 0 : 1)} of ${(job().max_retries ?? 0) + 1}`],
                  ['Retry', job().retry_strategy ?? '—'],
                  ['Created', props.ago(time(job().created_at))],
                  ['Updated', props.ago(time(job().updated_at))],
                  ['Held by', job().assignee ?? '—'],
                  ['Timeout', job().timeout ? `${job().timeout}s` : '—'],
                ]}
              />
              <div class="vw-meta cap" style={{ 'margin': '12px 0 0' }}><span>{job().id}</span></div>
            </SheetSection>
            <Show when={job().errors?.length}>
              <SheetSection label="Attempts" meta={<span class="bad">{job().errors!.length} failed</span>}>
                <table class="fx-list">
                  <tbody>
                    <For each={job().errors!}>
                      {(e, n) => (
                        <tr>
                          <td style={{ width: '18%' }}>#{n() + 1}</td>
                          <td class="bad">{reasonOf(e) ?? JSON.stringify(e)}</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </SheetSection>
            </Show>
            <SheetSection label="Payload"><Json value={job().payload} /></SheetSection>
            <Show when={job().result != null}>
              <SheetSection label="Result"><Json value={job().result} /></SheetSection>
            </Show>
          </>
        )}
      </Show>
    </Sheet>
  );
}

/**
 * Release one quarantined key: the scheduler forgets its failure streak, so
 * the next fire runs it again and closes its incident.
 */
function releaseKey(name: string, key: string, after?: () => void) {
  return runAction<{ released: boolean }>({
    label: `Release ${key}`,
    confirm: {
      title: `Release ${key} from quarantine?`,
      verb: 'Release',
      consequences: [
        'Its failure streak is forgotten and the next fire runs it again.',
        'If the cause is not fixed, it fails and is quarantined again.',
      ],
    },
    request: post(`/schedules/${encodeURIComponent(name)}/release`, { key }),
    success: () => 'Released. It runs on the next fire.',
    after,
  });
}

interface QuarantinedKey {
  key: string;
  consecutive_failures: number | null;
  quarantined_at: string | null;
}

function ScheduleSheet(props: {
  name: string;
  ago: (t: number | null) => string;
  until: (t: number | null) => string;
  onOpen: (key: string) => void;
  onClose: () => void;
  onDone: () => void;
}) {
  const d = useDetail<ScheduleDetailData & { quarantined?: QuarantinedKey[] }>(() => `/schedules/${encodeURIComponent(props.name)}`, 8_000);
  const s = () => d.data()?.schedule;
  const after = () => {
    d.refetch();
    props.onDone();
  };
  const rollup = () => d.data()?.rollup ?? [];
  const peak = () => Math.max(1, ...rollup().map((b) => b.success + b.failed + b.skipped + b.killed));
  const tally = () => rollup().reduce((t, b) => ({ ok: t.ok + b.success, bad: t.bad + b.failed + b.killed }), { ok: 0, bad: 0 });
  return (
    <Sheet
      open
      onClose={props.onClose}
      title={<><Sq tone={s()?.paused ? 'idle' : s()?.last_run_status === 'failed' ? 'bad' : ''} />{props.name}</>}
      meta={<>schedule · {s() ? cadence(s()!) : ''}</>}
    >
      <SheetError message={d.error()} />
      <Show when={s()}>
        {(sc) => (
          <>
            <section class="sheet-sec rn-sheet-actions">
              <button class="btn btn-sm btn-primary" onClick={() => void triggerSchedule(sc().name, after)}>Run now</button>
              <button class="btn btn-sm" onClick={() => void pauseSchedule(sc().name, !sc().paused, after)}>{sc().paused ? 'Resume' : 'Pause'}</button>
            </section>
            <SheetSection label="Fires" meta={<span>{sc().paused ? 'paused' : `next ${props.until(time(sc().next_fire_at))}`}</span>}>
              <Kv
                rows={[
                  ['When', cadence(sc())],
                  ['Time zone', sc().timezone ?? 'UTC'],
                  ['Runs', sc().kind === 'workflow' ? 'a workflow' : [sc().target_table, sc().path].filter(Boolean).join(' ') || '—'],
                  ['For each', sc().for_each ?? '—'],
                  ['Overlap', sc().concurrency],
                  ['Retries', sc().max_retries != null ? `${sc().max_retries} · ${sc().retry_strategy ?? 'default'}` : '—'],
                  ['Timeout', sc().timeout ? `${sc().timeout}s` : '—'],
                  ['Last fired', props.ago(time(sc().last_fire_at))],
                ]}
              />
              <Show when={sc().last_error}>
                <div class="fx-log bad" style={{ 'margin-top': '14px' }}><span class="fx-what">{sc().last_error}</span></div>
              </Show>
            </SheetSection>
            <Show when={rollup().length}>
              <SheetSection label="Last 24 hours" meta={<span><b>{tally().ok}</b> ok · <b classList={{ bad: tally().bad > 0 }}>{tally().bad}</b> failed</span>}>
                <div class="rn-rollup">
                  <For each={rollup()}>
                    {(b) => (
                      <span title={`${new Date(b.bucket).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} · ${b.success} ok · ${b.failed} failed`}>
                        <i class="bad" style={{ height: `${((b.failed + b.killed) / peak()) * 100}%` }} />
                        <i style={{ height: `${(b.success / peak()) * 100}%` }} />
                      </span>
                    )}
                  </For>
                </div>
              </SheetSection>
            </Show>
            <Show when={d.data()?.quarantined?.length}>
              <SheetSection label="Quarantined" meta={<span class="warn">skipped until released</span>}>
                <table class="fx-list">
                  <tbody>
                    <For each={d.data()!.quarantined!}>
                      {(q) => (
                        <tr>
                          <td style={{ color: 'var(--ink)' }}>{q.key}</td>
                          <td class="r warn">{q.consecutive_failures ?? '?'} failures</td>
                          <td class="r"><button class="btn btn-sm" onClick={() => void releaseKey(sc().name, q.key, after)}>Release</button></td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </SheetSection>
            </Show>
            <SheetSection label="Recent fires" meta={<span>{d.data()!.runs.length}</span>}>
              <table class="fx-list">
                <tbody>
                  <For each={d.data()!.runs}>
                    {(r) => (
                      <tr class="rn-click" onClick={() => r.workflow_run ? props.onOpen(`run:${r.workflow_run}`) : r.job_id && props.onOpen(`job:${r.job_id}`)}>
                        <td><span class="fx-who"><Sq tone={toneOf(r.status)} /><span>{props.ago(time(r.fire_at ?? r.created_at))}</span></span></td>
                        <td>{r.trigger ?? 'cron'}</td>
                        <td class="r">{r.finished_at && r.created_at ? formatDuration(time(r.finished_at)! - time(r.created_at)!) : '—'}</td>
                        <td class="bu-state" classList={{ bad: r.status === 'failed' }}>{reasonOf(r.error) ?? r.status}</td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
              <div class="vw-meta cap" style={{ margin: '12px 0 0' }}><span>created {formatStamp(sc().created_at)}</span></div>
            </SheetSection>
          </>
        )}
      </Show>
    </Sheet>
  );
}

/* ------------------------------------------------------------------ */
/* The old addresses, landing on the new page                           */
/* ------------------------------------------------------------------ */

/** `/jobs?status=failed` and friends: the list, filtered the same way. */
export function RunsRedirect(props: { kind?: 'job' | 'workflow' }) {
  const loc = useLocation();
  const q = new URLSearchParams(loc.search);
  if (props.kind && !q.has('kind') && !q.has('status') && !q.has('table')) q.set('kind', props.kind);
  return <Navigate href={`/runs${q.toString() ? `?${q}` : ''}`} />;
}

/** `/jobs/:id`, `/workflows/:id`, `/schedules/:name`: the list with that one open. */
export function RunsOpen(props: { kind: 'job' | 'run' | 'schedule' }) {
  const params = useParams();
  const key = () => decodeParam(params.id ?? params.name);
  return <Navigate href={`/runs?open=${encodeURIComponent(`${props.kind}:${key()}`)}`} />;
}
