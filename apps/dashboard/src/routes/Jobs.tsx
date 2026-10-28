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
import { A, useParams } from '@solidjs/router';
import { api, openStream, streamsAvailable } from '../api/client';
import {
  Chips,
  CopyId,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Reason,
  Stale,
} from '../components/Chrome';
import { readStash, writeStash } from '../lib/stash';
import { Trace, hueOf, type TraceRow, type TraceState } from '../components/Trace';
import {
  decodeParam,
  formatCount,
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
    key: 'durable',
    label: 'Durable workflow',
    hint: 'A run of a durable workflow instance (step.do in a backend)',
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
function isStalled(job: Pick<JobSummary, 'status' | 'lease_until'>, now = Date.now()): boolean {
  if (job.status !== 'processing') return false;
  const until = orNull(job.lease_until);
  if (!until) return false;
  const t = Date.parse(until);
  return !Number.isNaN(t) && t < now;
}

/* ------------------------------------------------------------------ */
/* The list                                                             */
/* ------------------------------------------------------------------ */

export function Jobs() {
  // Paint the last frame we saw before the stream's first one arrives.
  const cached = readStash<JobsListResponse>('jobs');
  const [page, setPageRaw] = createSignal<JobsListResponse | null>(cached?.value ?? null);
  // The trace follows the SCHEDULER's clock: the frame says when it was
  // sampled, and the time since it arrived is added on. A browser clock that
  // is off by minutes would otherwise slide every bar.
  let receivedAt = Date.now();
  const setPage = (p: JobsListResponse | null) => {
    receivedAt = Date.now();
    setPageRaw(p);
  };
  const serverNow = () => {
    const sampled = page()?.sampled_at_ms;
    return sampled ? sampled + (Date.now() - receivedAt) : Date.now();
  };
  const [stale, setStale] = createSignal(cached != null);
  const [error, setError] = createSignal<string | null>(null);
  const [connected, setConnected] = createSignal(false);

  const [status, setStatus] = createSignal('');
  // Origin and table filters are still honoured by the query; the trace
  // groups by table instead of offering a table chip.
  const [origin] = createSignal('');
  const [table] = createSignal('');
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
    // A host without streams (an SSP serving the API alone) is polled.
    if (filtered() || !streamsAvailable()) {
      setConnected(false);
      const path = query();
      let cancelled = false;
      const load = async () => {
        const res = await api.getResult<JobsListResponse>(path);
        if (cancelled) return;
        if (res.ok) {
          setPage(res.value);
          setStale(false);
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
          const frame = JSON.parse(data) as JobsListResponse;
          setPage(frame);
          setStale(false);
          writeStash('jobs', frame);
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

  const refresh = () => {
    // The stream pushes the next frame on its own; a filtered view is on a
    // timer. Either way an action's `after` only has to not be empty.
    if (filtered()) void api.getResult<JobsListResponse>(query()).then((r) => {
      if (r.ok) setPage(r.value);
    });
  };

  const clearAll = () => void clearJobs({ table: table() || undefined }, refresh);

  const t = (iso: string | null | undefined) => {
    const v = orNull(iso ?? null);
    if (!v) return null;
    const n = Date.parse(v);
    return Number.isNaN(n) ? null : n;
  };

  /** What a job's origin says in one short phrase. */
  const originText = (o: JobOrigin) =>
    orNull(o.schedule ?? null) ??
    (orNull(o.workflow_run ?? null) ? `${orNull(o.step ?? null) ?? 'step'} of ${o.workflow_run}` : null) ??
    originLabel(o.kind).toLowerCase();

  const rowOf = (job: JobSummary, parent?: string): TraceRow => {
    const created = t(job.created_at);
    const updated = t(job.updated_at);
    const stuck = isStalled(job, serverNow());
    const state: TraceState =
      job.status === 'pending'
        ? 'waiting'
        : job.status === 'processing'
          ? stuck
            ? 'failed'
            : 'running'
          : job.status === 'failed'
            ? 'failed'
            : 'done';
    const attempts = (job.retries ?? 0) + 1;
    return {
      id: job.id,
      parent,
      depth: parent ? 1 : 0,
      label: job.path ?? job.key,
      sub: [originText(job.origin), job.assignee].filter(Boolean).join(' · '),
      badge: job.origin.kind === 'schedule' ? 'S' : job.origin.kind === 'workflow' ? 'W' : job.origin.kind === 'durable' ? 'D' : 'A',
      color: hueOf(job.table),
      state,
      // No pickup time is recorded, so a job's bar spans its row's life: from
      // creation to its last write, or to now while it runs.
      start: state === 'waiting' ? null : created,
      end: state === 'running' ? null : state === 'waiting' ? null : updated,
      queued: state === 'waiting' ? created : null,
      meta: [
        stuck ? 'stalled' : null,
        attempts > 1 ? `attempt ${attempts} of ${(job.max_retries ?? 0) + 1}` : null,
        state === 'failed' && job.last_error?.reason ? String(job.last_error.reason) : null,
      ]
        .filter(Boolean)
        .join(' · ') || undefined,
      href: `/jobs/${encodeURIComponent(job.id)}`,
      actions:
        job.status === 'processing'
          ? () => (
              <button class="btn btn-sm" onClick={() => void killJob(job.id, refresh)}>
                Kill
              </button>
            )
          : job.status === 'failed' || job.status === 'success'
            ? () => (
                <button class="btn btn-sm" onClick={() => void retryJob(job.id, refresh)}>
                  Retry
                </button>
              )
            : undefined,
    };
  };

  /**
   * One collapsible group per outbox table, its jobs under it. A table with
   * nothing in this page still gets its header line when it holds pending or
   * failed work, so a queue backing up is visible even when the listed page is
   * all other tables.
   */
  const rows = createMemo((): TraceRow[] => {
    const list = jobs();
    const byTable = new Map<string, JobSummary[]>();
    for (const j of list) byTable.set(j.table, [...(byTable.get(j.table) ?? []), j]);
    const out: TraceRow[] = [];
    const known = new Set<string>();
    for (const lane of tables()) {
      known.add(lane.table);
      const mine = byTable.get(lane.table) ?? [];
      if (mine.length === 0 && lane.counts.pending + lane.in_flight + lane.counts.failed === 0) continue;
      const saturated = lane.in_flight >= lane.concurrency && lane.counts.pending > 0;
      out.push({
        id: `table:${lane.table}`,
        group: true,
        label: lane.table,
        sub: [
          lane.counts.pending ? `${formatCount(lane.counts.pending)} pending` : null,
          `${lane.in_flight}/${lane.concurrency} running`,
          lane.counts.failed ? `${formatCount(lane.counts.failed)} failed` : null,
        ]
          .filter(Boolean)
          .join(' · '),
        color: hueOf(lane.table),
        state: lane.error || lane.stalled > 0 ? 'failed' : saturated ? 'running' : 'done',
        start: null,
        end: null,
      });
      for (const j of mine) out.push(rowOf(j, `table:${lane.table}`));
    }
    // Jobs from a table the sampler did not report (a filtered page).
    for (const j of list) if (!known.has(j.table)) out.push(rowOf(j));
    return out;
  });

  return (
    <>
      <PageHead
        title="Jobs"
        actions={
          <div class="row">
            <Stale when={stale()} />
            <Pill tone={connected() ? 'live' : 'idle'} dot pulse={connected()}>
              {connected() ? 'live' : filtered() ? 'filtered' : 'connecting'}
            </Pill>
            <Show when={tables().length > 0}>
              <button class="btn btn-sm" onClick={clearAll}>
                Clear finished
              </button>
            </Show>
          </div>
        }
      />

      <div class="page-body">
        <div class="page-stack">
          <Show when={error()}>
            <div class="banner">
              <span class="dot bad" />
              {error()}
            </div>
          </Show>

          <Show when={totals() && !totals()!.ready}>
            <div class="banner">
              <span class="dot warn" />
              {totals()!.enabled === false
                ? 'The job sampler is switched off on this scheduler, so the totals are not measured.'
                : 'Waiting for the first job sample.'}
            </div>
          </Show>

          <Show when={totals()?.blind}>
            <div class="banner">
              <span class="dot bad" />
              No outbox table answered, so these counts mean nothing.
            </div>
          </Show>

          <Show when={page()} fallback={<div class="ov-skeleton" />}>
            <Trace
              title="Queue"
              live
              clock={serverNow}
              windows={[
                { key: '5m', label: 'Last 5 min', ms: 5 * 60_000 },
                { key: '1m', label: 'Last minute', ms: 60_000 },
                { key: '15m', label: 'Last 15 min', ms: 15 * 60_000 },
                { key: '1h', label: 'Last hour', ms: 3_600_000 },
                { key: '6h', label: 'Last 6 hours', ms: 6 * 3_600_000 },
                { key: '24h', label: 'Last day', ms: 24 * 3_600_000 },
              ]}
              treeLabel="Queues"
              rows={rows()}
              legend={['run', 'wait', 'failed']}
              empty={filtered() ? 'Nothing matches those filters.' : 'No jobs. Finished ones are pruned on your retention window.'}
              stats={[
                { label: 'Pending', value: formatCount(totals()?.counts.pending ?? 0) },
                { label: 'Running', value: formatCount(totals()?.in_flight ?? 0) },
                { label: 'Stalled', value: formatCount(stalled()), tone: stalled() > 0 ? 'bad' : undefined },
                {
                  label: 'Failed, 1h',
                  value: formatCount(totals()?.counts.failed ?? 0),
                  tone: (totals()?.counts.failed ?? 0) > 0 ? 'warn' : undefined,
                },
                { label: 'Per minute', value: formatCount(totals()?.throughput_1m ?? 0) },
                {
                  label: 'Oldest pending',
                  value: totals()?.oldest_pending ? relativeStamp(totals()!.oldest_pending) : 'none',
                },
              ]}
              tools={
                <>
                  <Chips
                    value={status()}
                    onChange={setStatus}
                    options={[
                      { key: '', label: 'All' },
                      { key: 'pending', label: 'Waiting' },
                      { key: 'processing', label: 'Running' },
                      { key: 'failed', label: 'Failed', tone: 'bad' },
                      { key: 'success', label: 'Done' },
                    ]}
                  />
                  <input
                    type="search"
                    placeholder="Search path or id"
                    value={search()}
                    onInput={(e) => setSearch(e.currentTarget.value)}
                    aria-label="Search jobs"
                  />
                </>
              }
            />
          </Show>
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
        title={job()?.path ?? 'Job'}
        subtitle={<CopyId value={id()} />}
        actions={
          <div class="row">
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
                    ['Queued', relativeStamp(j().created_at)],
                    ['Last write', relativeStamp(j().updated_at)],
                  ]}
                />
              </Panel>

              <Panel title="Attempts">
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
