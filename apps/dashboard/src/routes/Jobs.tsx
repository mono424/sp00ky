import {
  For,
  Show,
  createEffect,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
  type JSX,
} from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api, openStream } from '../api/client';
import {
  Bento,
  CopyId,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Readout,
  Reason,
  ReasonLine,
  StatusDot,
  Tile,
} from '../components/Chrome';
import { Sparkline, type Point } from '../components/Sparkline';
import {
  decodeParam,
  elapsed,
  formatCount,
  formatDuration,
  formatStamp,
  orNull,
  relativeStamp,
} from '../lib/format';
import { jobTone } from '../lib/status';
import { clearJobs, killJob, retryJob } from '../lib/runActions';
import type {
  JobDetail,
  JobOrigin,
  JobSummary,
  JobTotals,
  JobsListResponse,
  OriginKind,
  RunError,
} from '../api/types';

/**
 * The outbox, in one place.
 *
 * # What this page is for
 *
 * A job used to be reachable only by opening a workflow run and expanding a
 * step. That left a `kind: job` schedule's fires and every job the application
 * created itself with no surface at all — which is most of the work on a busy
 * deployment. This lists all of them together and says where each came from.
 *
 * # Where the data comes from
 *
 * Unfiltered, from `GET /admin/api/jobs/stream`: the scheduler runs ONE sampler
 * for the whole plane and pushes only on change, so ten open tabs cost the
 * database what one does, and holding this page open is also what speeds the
 * sampler up from its idle cadence. Filtered, from `GET /admin/api/jobs`, which
 * pushes the filter into SurrealQL rather than narrowing the cached page —
 * filtering a page after its LIMIT is how a busy table answers "no application
 * jobs" while plainly holding thousands.
 */

/** How often a filtered view refetches. The stream covers the unfiltered one. */
const FILTERED_POLL_MS = 3000;

const STATUSES = ['pending', 'processing', 'success', 'failed'] as const;

const ORIGINS: { key: OriginKind; label: string; hint: string }[] = [
  {
    key: 'schedule',
    label: 'Schedule',
    hint: 'Fired by a kind: job schedule',
  },
  {
    key: 'workflow',
    label: 'Workflow',
    hint: 'One step of a workflow run',
  },
  {
    key: 'app',
    label: 'Application',
    hint: 'Created by your own code, belonging to no schedule or workflow',
  },
];

function originLabel(kind: OriginKind): string {
  return ORIGINS.find((o) => o.key === kind)?.label ?? kind;
}

/**
 * Where a job came from, as a link when the owning run still exists.
 *
 * A terminal run is pruned on its own retention window well before the jobs
 * that outlived it, so the class always renders and only the link is
 * conditional. "scheduled work whose fire has aged out" and "a job nobody can
 * account for" must not look the same.
 */
function OriginCell(props: { origin: JobOrigin }) {
  const o = () => props.origin;
  const schedule = () => orNull(o().schedule);
  const run = () => orNull(o().workflow_run);

  return (
    <Show
      when={schedule() ?? run()}
      fallback={
        <span class="ghost" title={ORIGINS.find((x) => x.key === o().kind)?.hint}>
          {originLabel(o().kind)}
        </span>
      }
    >
      <div class="stack" style={{ gap: '2px' }}>
        <Show
          when={schedule()}
          fallback={
            <A
              href={`/workflows/${encodeURIComponent(run()!)}`}
              onClick={(e) => e.stopPropagation()}
            >
              {orNull(o().step) ?? 'step'} →
            </A>
          }
        >
          {(name) => (
            <A
              href={`/schedules/${encodeURIComponent(name())}`}
              onClick={(e) => e.stopPropagation()}
            >
              {name()} →
            </A>
          )}
        </Show>
        <span class="mini">{originLabel(o().kind)}</span>
      </div>
    </Show>
  );
}

/** `retries` counts retries, so an attempt count is one more than it. */
function attemptLabel(retries: number | null, max: number | null): string {
  return `${(retries ?? 0) + 1} of ${(max ?? 0) + 1}`;
}

/**
 * A `processing` row whose lease has expired.
 *
 * Nobody is working on it: the SSP that claimed it is gone or wedged, and a
 * recovery sweep will reclaim it on its own clock. The schema has no status for
 * this, which is exactly why it earns a label of its own here rather than
 * sitting in the list looking like ordinary in-flight work.
 */
function isStalled(job: Pick<JobSummary, 'status' | 'lease_until'>): boolean {
  if (job.status !== 'processing') return false;
  const until = orNull(job.lease_until);
  if (!until) return false;
  const t = Date.parse(until);
  return !Number.isNaN(t) && t < Date.now();
}

/* ------------------------------------------------------------------ */
/* The list                                                             */
/* ------------------------------------------------------------------ */

export function Jobs() {
  const navigate = useNavigate();
  const [page, setPage] = createSignal<JobsListResponse | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  const [connected, setConnected] = createSignal(false);

  const [status, setStatus] = createSignal('');
  const [origin, setOrigin] = createSignal('');
  const [table, setTable] = createSignal('');
  const [search, setSearch] = createSignal('');

  const filtered = () => !!(status() || origin() || table() || search());

  const query = createMemo(() => {
    const params = new URLSearchParams();
    if (status()) params.set('status', status());
    if (origin()) params.set('origin', origin());
    if (table()) params.set('table', table());
    if (search().trim()) params.set('q', search().trim());
    const qs = params.toString();
    return qs ? `/jobs?${qs}` : '/jobs';
  });

  /**
   * One source at a time.
   *
   * The stream only ever carries the unfiltered page, so a filtered view has to
   * poll. Selecting between them in one effect keeps a filtered view from
   * quietly being overwritten by the next stream frame, which is the bug the
   * two-sources-at-once version has.
   */
  createEffect(() => {
    if (filtered()) {
      setConnected(false);
      const path = query();
      let cancelled = false;
      const load = async () => {
        const res = await api.getResult<JobsListResponse>(path);
        if (cancelled) return;
        if (res.ok) {
          setPage(res.value);
          setError(null);
        } else {
          setError(res.message);
        }
      };
      void load();
      const timer = setInterval(load, FILTERED_POLL_MS);
      onCleanup(() => {
        cancelled = true;
        clearInterval(timer);
      });
      return;
    }

    const close = openStream('/jobs/stream', {
      onOpen: () => {
        setConnected(true);
        setError(null);
      },
      onEvent: (event, data) => {
        if (event !== 'jobs') return;
        try {
          setPage(JSON.parse(data) as JobsListResponse);
        } catch {
          /* a malformed frame must not tear down the stream */
        }
      },
      onError: (err) => {
        setConnected(false);
        setError(err instanceof Error ? err.message : 'Stream disconnected');
      },
    });
    onCleanup(close);
  });

  const totals = (): JobTotals | undefined => page()?.totals;
  const jobs = () => page()?.jobs ?? [];
  const tables = () => page()?.tables ?? [];
  const stalled = () => totals()?.stalled ?? 0;

  const points = (): Point[] =>
    (totals()?.samples ?? []).map((s) => ({
      ts: s.t,
      ms: s.pending,
      ok: true,
    }));

  const refresh = () => {
    // The stream pushes the next frame on its own; a filtered view is on a
    // timer. Either way an action's `after` only has to not be empty.
    if (filtered()) void api.getResult<JobsListResponse>(query()).then((r) => {
      if (r.ok) setPage(r.value);
    });
  };

  const clearAll = () => void clearJobs({ table: table() || undefined }, refresh);

  return (
    <>
      <PageHead
        crumb="Dashboard"
        title="Jobs"
        subtitle="Every outbox job, whatever created it"
        actions={
          <div class="row">
            <Pill tone={connected() ? 'live' : 'idle'}>
              <StatusDot tone={connected() ? 'ok' : 'idle'} />
              {connected() ? 'live' : filtered() ? 'filtered' : 'connecting…'}
            </Pill>
          </div>
        }
      />

      <div class="page-body">
        <div class="stack">
          <Show when={error()}>
            <div class="banner">
              <span class="dot bad" />
              {error()}
            </div>
          </Show>

          {/* The sampler has not completed a pass. Said plainly: zeros here
              would read as an empty, healthy queue, which is a different claim
              from "not measured yet". */}
          <Show when={totals() && !totals()!.ready}>
            <div class="banner">
              <span class="dot warn" />
              <Show
                when={totals()!.enabled === false}
                fallback={<>The job sampler has not completed its first pass yet.</>}
              >
                The job sampler is switched off on this scheduler (
                <span class="mono">SPKY_ADMIN_JOB_INTERVAL_SECS=0</span>), so the
                totals below are not being measured. The listing still works.
              </Show>
            </div>
          </Show>

          <Show when={totals()?.blind}>
            <div class="banner">
              <span class="dot bad" />
              Every outbox table refused to answer, so these counts mean nothing.
              The per-table panel below has each table's own error.
            </div>
          </Show>

          <Show when={totals()}>
            {(t) => (
              <Bento>
                <Tile
                  i={0}
                  span={3}
                  label="Pending"
                  sub={
                    t().oldest_pending
                      ? `oldest ${relativeStamp(t().oldest_pending)}`
                      : 'nothing queued'
                  }
                  tone={t().counts.pending > 0 ? 'warn' : 'ok'}
                >
                  <Readout value={formatCount(t().counts.pending)} />
                  <Show when={points().length > 1}>
                    <div class="tile-plot tile-end">
                      <Sparkline
                        points={points()}
                        fill
                        bare
                        format={formatCount}
                        ariaLabel="Pending jobs"
                      />
                    </div>
                  </Show>
                </Tile>

                <Tile
                  i={1}
                  span={3}
                  label="In flight"
                  sub={
                    stalled() > 0
                      ? `${formatCount(stalled())} stalled: lease expired, nobody is working on them`
                      : 'leases live'
                  }
                  tone={stalled() > 0 ? 'bad' : 'ok'}
                  pulse={t().in_flight > 0}
                >
                  <Readout value={formatCount(t().in_flight)} />
                </Tile>

                <Tile
                  i={2}
                  span={3}
                  label="Failed"
                  sub="in the last hour"
                  tone={t().counts.failed > 0 ? 'bad' : 'ok'}
                >
                  <Readout value={formatCount(t().counts.failed)} />
                </Tile>

                <Tile i={3} span={3} label="Throughput" sub="succeeded in the last minute">
                  <Readout value={formatCount(t().throughput_1m)} unit="per min" />
                </Tile>
              </Bento>
            )}
          </Show>

          <Panel
            title="Filters"
            sub="Origin is the one this page exists for: application jobs have no other surface."
          >
            <div class="filters">
              <select
                value={origin()}
                onChange={(e) => setOrigin(e.currentTarget.value)}
                aria-label="Origin"
              >
                <option value="">Any origin</option>
                <For each={ORIGINS}>
                  {(o) => (
                    <option value={o.key} title={o.hint}>
                      {o.label}
                    </option>
                  )}
                </For>
              </select>
              <select
                value={status()}
                onChange={(e) => setStatus(e.currentTarget.value)}
                aria-label="Status"
              >
                <option value="">Any status</option>
                <For each={STATUSES}>{(s) => <option value={s}>{s}</option>}</For>
              </select>
              <Show when={tables().length > 1}>
                <select
                  value={table()}
                  onChange={(e) => setTable(e.currentTarget.value)}
                  aria-label="Table"
                >
                  <option value="">Every table</option>
                  <For each={tables()}>
                    {(t) => <option value={t.table}>{t.table}</option>}
                  </For>
                </select>
              </Show>
              <input
                class="grow"
                placeholder="Search the path or the id"
                value={search()}
                onInput={(e) => setSearch(e.currentTarget.value)}
              />
              <Show when={filtered()}>
                <button
                  class="btn btn-sm"
                  onClick={() => {
                    setStatus('');
                    setOrigin('');
                    setTable('');
                    setSearch('');
                  }}
                >
                  Clear filters
                </button>
              </Show>
            </div>
          </Panel>

          <Panel
            title="Outbox tables"
            sub="Depth against the concurrency the dispatcher admits on."
            flush
          >
            <Show
              when={tables().length > 0}
              fallback={
                <Empty>
                  No outbox tables. <span class="mono">spky deploy</span> writes
                  the list to <span class="mono">_00_retention.job_tables</span>.
                </Empty>
              }
            >
              <div class="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Table</th>
                      <th>Pending</th>
                      <th>In flight</th>
                      <th>Failed (1h)</th>
                      <th>Oldest pending</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    <For each={tables()}>
                      {(t) => (
                        <tr>
                          <td>
                            <div class="row">
                              <StatusDot tone={t.error ? 'bad' : 'ok'} />
                              <span class="mono">{t.table}</span>
                            </div>
                            <Show when={t.error}>
                              <ReasonLine error={t.error} />
                            </Show>
                          </td>
                          <td class="dim" data-label="Pending">
                            {formatCount(t.counts.pending)}
                          </td>
                          <td data-label="In flight">
                            {/* Against the ceiling, because "3 in flight" says
                                nothing until you know whether the limit is 3. */}
                            <span classList={{ 'tone-warn': t.in_flight >= t.concurrency }}>
                              {t.in_flight} / {t.concurrency}
                            </span>
                            <Show when={t.stalled > 0}>
                              <span class="mini">{t.stalled} stalled</span>
                            </Show>
                          </td>
                          <td class="dim" data-label="Failed (1h)">
                            {formatCount(t.counts.failed)}
                          </td>
                          <td class="ghost" data-label="Oldest pending">
                            {relativeStamp(t.oldest_pending)}
                          </td>
                          <td data-label="Actions" data-empty={true}>
                            <div class="row-actions">
                              <button
                                class="btn btn-sm"
                                onClick={() =>
                                  void clearJobs({ table: t.table }, refresh)
                                }
                              >
                                Clear terminal
                              </button>
                            </div>
                          </td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </Show>
          </Panel>

          <Panel
            title="Jobs"
            sub={`${jobs().length} shown${
              page()?.live === false ? ', matching the filters' : ', newest activity first'
            }`}
            actions={
              <Show when={tables().length > 0}>
                <button class="btn btn-sm" onClick={clearAll}>
                  Clear terminal jobs
                </button>
              </Show>
            }
            flush
          >
            <Show
              when={jobs().length > 0}
              fallback={
                <Empty>
                  <Show
                    when={filtered()}
                    fallback={
                      <>
                        No jobs. Terminal rows are pruned on your retention
                        window, so a quiet queue and a swept one look the same
                        here.
                      </>
                    }
                  >
                    Nothing matches those filters.
                  </Show>
                </Empty>
              }
            >
              <div class="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Job</th>
                      <th>Status</th>
                      <th>Origin</th>
                      <th>Attempts</th>
                      <th>Age</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    <For each={jobs()}>
                      {(job) => (
                        <tr
                          class="clickable"
                          onClick={() =>
                            navigate(`/jobs/${encodeURIComponent(job.id)}`)
                          }
                        >
                          <td>
                            <div class="row">
                              <StatusDot
                                tone={jobTone(job.status)}
                                pulse={job.status === 'processing' && !isStalled(job)}
                              />
                              <span class="truncate">{job.path ?? '—'}</span>
                            </div>
                            <div class="id" style={{ 'margin-top': '2px' }}>
                              {job.id}
                            </div>
                            {/* The reason on the row: a list where four say
                                `failed` and nothing else forces four clicks to
                                find out whether it is four problems or one. */}
                            <Show when={job.last_error}>
                              <ReasonLine error={job.last_error} />
                            </Show>
                          </td>
                          <td data-label="Status">
                            <Pill tone={isStalled(job) ? 'bad' : jobTone(job.status)}>
                              {isStalled(job) ? 'stalled' : job.status}
                            </Pill>
                          </td>
                          <td data-label="Origin">
                            <OriginCell origin={job.origin} />
                          </td>
                          <td class="dim" data-label="Attempts">
                            {attemptLabel(job.retries, job.max_retries)}
                          </td>
                          <td class="dim" data-label="Age">
                            {relativeStamp(job.created_at)}
                          </td>
                          <td data-label="Actions" data-empty={true}>
                            <div
                              class="row-actions"
                              onClick={(e) => e.stopPropagation()}
                            >
                              <Show when={job.status === 'processing'}>
                                <button
                                  class="btn btn-sm"
                                  onClick={() => void killJob(job.id, refresh)}
                                >
                                  Kill
                                </button>
                              </Show>
                              <Show
                                when={job.status === 'failed' || job.status === 'success'}
                              >
                                <button
                                  class="btn btn-sm"
                                  onClick={() => void retryJob(job.id, refresh)}
                                >
                                  Retry
                                </button>
                              </Show>
                            </div>
                          </td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </Show>
          </Panel>
        </div>
      </div>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* Attempts, shared with the workflow step lane                          */
/* ------------------------------------------------------------------ */

/**
 * The whole attempt history of one job.
 *
 * Exported because the workflow step lane shows the same thing, and a run's
 * view of an attempt must not be able to disagree with the job's own. Oldest
 * first, so it reads as the history it is: what the backend did on attempt 1,
 * then 2, then the one that exhausted the budget.
 */
export function JobAttempts(props: { job: JobDetail }) {
  const attempts = (): RunError[] => props.job.errors ?? [];
  return (
    <Show
      when={attempts().length > 0}
      fallback={
        <Empty>
          No failed attempt recorded.
          {props.job.status === 'failed'
            ? ' The runner could not append to the job’s `errors` array; its log has the rejection.'
            : ''}
        </Empty>
      }
    >
      <div class="stack" style={{ gap: '8px' }}>
        <For each={attempts()}>
          {(err, i) => (
            <div class="attempt">
              <span class="attempt-n">#{i() + 1}</span>
              <Reason error={err} />
            </div>
          )}
        </For>
      </div>
    </Show>
  );
}

/* ------------------------------------------------------------------ */
/* The detail                                                           */
/* ------------------------------------------------------------------ */

export function JobDetailView() {
  const params = useParams();
  const id = () => decodeParam(params.id);

  const [result, { refetch }] = createResource(id, (jobId) =>
    api.getResult<{ job: JobDetail }>(`/jobs/${encodeURIComponent(jobId)}`),
  );
  const job = () => {
    const r = result();
    return r?.ok ? r.value.job : undefined;
  };
  const failure = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  const origin = (): JobOrigin => job()?.origin ?? { kind: 'app' };

  return (
    <>
      <PageHead
        crumb="Jobs"
        title={job()?.path ?? 'Job'}
        subtitle={<CopyId value={id()} />}
        actions={
          <div class="row">
            <A href="/jobs" class="btn btn-sm">
              all jobs
            </A>
            <Show when={job()?.status === 'processing'}>
              <button
                class="btn btn-sm"
                onClick={() => void killJob(id(), () => void refetch())}
              >
                Kill
              </button>
            </Show>
            <Show when={job() && job()!.status !== 'processing' && job()!.status !== 'pending'}>
              <button
                class="btn btn-sm"
                onClick={() => void retryJob(id(), () => void refetch())}
              >
                Retry
              </button>
            </Show>
          </div>
        }
      />

      <div class="page-body">
        <Show when={failure()}>
          {(message) => (
            <Panel>
              <Empty>{message()}</Empty>
            </Panel>
          )}
        </Show>

        <Show when={job()} fallback={<Show when={!failure()}><Panel><Empty>Loading the job…</Empty></Panel></Show>}>
          {(j) => (
            <div class="stack">
              <Panel title="Job">
                <KeyValue
                  rows={[
                    [
                      'Status',
                      <div class="row">
                        <Pill tone={isStalled(j()) ? 'bad' : jobTone(j().status)}>
                          {j().status}
                        </Pill>
                        <Show when={isStalled(j())}>
                          <span class="mini">
                            lease expired; a recovery sweep will reclaim it
                          </span>
                        </Show>
                      </div>,
                    ],
                    ['Origin', <OriginCell origin={origin()} />],
                    ['Path', j().path],
                    ['Table', <span class="mono">{j().table ?? '—'}</span>],
                    [
                      'Attempts',
                      `${attemptLabel(j().retries, j().max_retries)} (${
                        j().retry_strategy ?? 'linear'
                      })`,
                    ],
                    ...((j().assignee
                      ? [['SSP', <span class="mono">{j().assignee!}</span>]]
                      : []) as [string, JSX.Element][]),
                    ...((j().timeout
                      ? [['Timeout', formatDuration(j().timeout! * 1000)]]
                      : []) as [string, JSX.Element][]),
                    ...((j().delay
                      ? [['Delay', formatDuration(j().delay!)]]
                      : []) as [string, JSX.Element][]),
                    ['Lease until', formatStamp(j().lease_until)],
                    ['Queued', formatStamp(j().created_at)],
                    ['Last write', formatStamp(j().updated_at)],
                    ['In the queue', elapsed(j().created_at, j().updated_at)],
                  ]}
                />
              </Panel>

              <Panel
                title="Attempts"
                sub="Every attempt the backend made, not only the one that ended the job."
              >
                <JobAttempts job={j()} />
              </Panel>

              <div class="grid grid-2">
                <Panel title="Payload">
                  <pre class="json">{JSON.stringify(j().payload, null, 2)}</pre>
                </Panel>
                <Panel title="Result">
                  <Show
                    when={j().result !== null && j().result !== undefined}
                    fallback={<Empty>No result recorded.</Empty>}
                  >
                    <pre class="json">{JSON.stringify(j().result, null, 2)}</pre>
                  </Show>
                </Panel>
              </div>
            </div>
          )}
        </Show>
      </div>
    </>
  );
}
