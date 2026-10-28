import { For, Show, createResource, onCleanup, type JSX } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Chevron,
  CopyId,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Reason,
} from '../components/Chrome';
import { Trace, hueOf, short, type TraceRow, type TraceState } from '../components/Trace';
import {
  decodeParam,
  elapsed,
  formatStamp,
  orNull,
  relativeStamp,
} from '../lib/format';
import { jobTone, runTone } from '../lib/status';
import { cancelRun, killJob, rerunRun, retryJob, retryRun } from '../lib/runActions';
import { JobAttempts } from './Jobs';
import type { JobDetail, StepRun, WorkflowRun, WorkflowRunDetail } from '../api/types';

/**
 * One workflow run.
 *
 * The trace IS the page: a strip of numbers, then every step as a bar on the
 * run's own time axis, with hand-offs from each dependency to the step that
 * waited for it. A stuck run is read off the bar that never ended and what
 * sits behind it. Clicking a step opens its job, attempts and output.
 */
export function WorkflowDetail() {
  const params = useParams<{ id: string }>();
  const navigate = useNavigate();
  const runId = () => decodeParam(params.id);

  const [result, { refetch }] = createResource(runId, (id) =>
    api.getResult<{ run: WorkflowRunDetail; steps: StepRun[] }>(
      `/workflows/runs/${encodeURIComponent(id)}`,
    ),
  );

  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  // Poll only while the run is still moving; a finished run is immutable.
  // `api.getResult` never rejects, so this cannot produce an unhandled
  // rejection the way reading an errored resource would.
  const timer = setInterval(() => {
    if (data()?.run.status === 'running') void refetch();
  }, 1000);
  onCleanup(() => clearInterval(timer));

  const run = () => data()?.run;
  const isRunning = () => run()?.status === 'running';
  const isTerminal = () => !!run() && !isRunning();
  const canRetry = () => run()?.status === 'failed' || run()?.status === 'killed';
  const rerunOf = () => orNull(run()?.rerun_of ?? null);

  // Runs created by rerunning this one. Refetched with the run itself, so a
  // rerun started from this page shows up in the list on the next poll.
  const [reruns, { refetch: refetchReruns }] = createResource(runId, (id) =>
    api.getResult<{ runs: WorkflowRun[] }>(
      `/workflows/runs?rerun_of=${encodeURIComponent(id)}&limit=20`,
    ),
  );
  const rerunList = () => {
    const r = reruns();
    return r?.ok ? r.value.runs : [];
  };

  const refreshAll = () => {
    void refetch();
    void refetchReruns();
  };

  /**
   * Steps in dependency order (Kahn). Any cycle — which the engine should never
   * emit — falls through to the tail in its original order rather than
   * silently dropping steps.
   */
  const orderedSteps = (): StepRun[] => {
    const steps = data()?.steps ?? [];
    const known = new Set(steps.map((s) => s.step));
    const emitted = new Set<string>();
    const out: StepRun[] = [];

    let progressed = true;
    while (progressed) {
      progressed = false;
      for (const s of steps) {
        if (emitted.has(s.step)) continue;
        if (s.depends_on.every((d) => emitted.has(d) || !known.has(d))) {
          emitted.add(s.step);
          out.push(s);
          progressed = true;
        }
      }
    }
    for (const s of steps) if (!emitted.has(s.step)) out.push(s);
    return out;
  };

  const parse = (iso: string | null | undefined): number | null => {
    if (!iso || iso === 'NONE') return null;
    const t = Date.parse(iso);
    return Number.isNaN(t) ? null : t;
  };

  /**
   * A step that has not been dispatched yet has NOT started, whatever its
   * `created_at` says.
   *
   * `_00_step_run.created_at` defaults to `time::now()` at row creation, so
   * every step of a workflow — including the ones still waiting on their
   * dependencies — carries a timestamp. Plotting a `blocked` step from that
   * stamp draws a bar for work that has not happened, which is the one thing a
   * timeline must never do. A `skipped` step is the same case: the engine
   * skipped it without ever dispatching, so its stamps are row-creation times.
   */
  const hasStarted = (status: string) =>
    status !== 'blocked' && status !== 'ready' && status !== 'skipped';

  /**
   * When a step actually began, and whether that is known or inferred.
   *
   * `started_at` is stamped when the step's job is created, which is the only
   * honest answer. `created_at` is when the step ROW was made, and the engine
   * makes every row up front — so for a step deep in a DAG it is the RUN's
   * start, and plotting it drew every dependent step from +0ms alongside the
   * step it was waiting for.
   *
   * Runs recorded before the engine stamped `started_at` have none, so falling
   * straight back to `created_at` would keep drawing exactly that. Instead,
   * infer the earliest instant the step COULD have been dispatched — when the
   * last of its dependencies finished — and mark the lane estimated so the bar
   * does not claim a precision it has not got. A step with no dependencies has
   * nothing to infer from, and for it `created_at` is the right answer anyway.
   */
  const startOf = (
    s: StepRun,
    byName: Map<string, StepRun>,
  ): { at: number | null; estimated: boolean } => {
    const recorded = parse(s.started_at);
    if (recorded !== null) return { at: recorded, estimated: false };

    const depEnds = s.depends_on
      .map((d) => parse(byName.get(d)?.finished_at))
      .filter((v): v is number => typeof v === 'number');
    if (depEnds.length) return { at: Math.max(...depEnds), estimated: true };

    const created = parse(s.created_at);
    // Estimated only when there were dependencies we failed to resolve; for a
    // root step `created_at` IS when the run began working on it.
    return { at: created, estimated: s.depends_on.length > 0 };
  };

  /** Longest dependency chain above each step: its indent in the tree. */
  const depthOf = (steps: StepRun[]) => {
    const byName = new Map(steps.map((s) => [s.step, s]));
    const memo = new Map<string, number>();
    const visit = (name: string, seen: Set<string>): number => {
      if (memo.has(name)) return memo.get(name)!;
      const st = byName.get(name);
      if (!st || seen.has(name)) return 0;
      seen.add(name);
      const d = st.depends_on.length
        ? 1 + Math.max(...st.depends_on.map((x) => visit(x, seen)))
        : 0;
      memo.set(name, d);
      return d;
    };
    return (name: string) => Math.min(4, visit(name, new Set()));
  };

  const stateOf = (status: string): TraceState => {
    switch (status) {
      case 'success':
        return 'done';
      case 'dispatched':
        return 'running';
      case 'failed':
        return 'failed';
      case 'skipped':
        return 'skipped';
      default:
        return 'waiting';
    }
  };

  /** When every dependency had finished, or null while one still runs. */
  const readyAt = (s: StepRun, byName: Map<string, StepRun>, runStart: number | null) => {
    if (!s.depends_on.length) return runStart;
    const ends = s.depends_on.map((d) => parse(byName.get(d)?.finished_at));
    if (ends.some((e) => e == null)) return null;
    return Math.max(...(ends as number[]));
  };

  const rows = (): TraceRow[] => {
    const steps = data()?.steps ?? [];
    const byName = new Map(steps.map((s) => [s.step, s]));
    const depth = depthOf(steps);
    const runStart = parse(data()?.run.created_at);
    return orderedSteps().map((s) => {
      const started = hasStarted(s.status) ? startOf(s, byName) : { at: null, estimated: false };
      const end = hasStarted(s.status) ? parse(s.finished_at) : null;
      const state = stateOf(s.status);
      const attempts = s.dispatch_attempts ?? 0;
      const took =
        started.at != null && end != null ? short(end - started.at) : null;
      return {
        id: s.step,
        label: s.step,
        sub:
          state === 'waiting'
            ? s.depends_on.length
              ? `after ${s.depends_on.join(', ')}`
              : s.status
            : state === 'skipped'
              ? 'skipped'
              : undefined,
        depth: depth(s.step),
        color: hueOf(s.step),
        state,
        start: started.at,
        end,
        estimated: started.estimated,
        // Hatched only once every dependency has finished: from then on the
        // step is waiting to be picked up. Before that it is not waiting on
        // the queue, it simply has not been reached.
        queued: state === 'waiting' && isRunning() ? readyAt(s, byName, runStart) : null,
        after: s.depends_on,
        meta: [took, attempts > 1 ? `${attempts} dispatches` : null].filter(Boolean).join(' · ') || undefined,
        detail: () => <StepDetail step={s} onChange={refreshAll} />,
      };
    });
  };

  const counts = () => {
    const steps = data()?.steps ?? [];
    return {
      total: steps.length,
      done: steps.filter((s) => s.status === 'success').length,
      failed: steps.filter((s) => s.status === 'failed').length,
      dispatches: steps.reduce((n, s) => n + Math.max(s.dispatch_attempts ?? 0, s.job_id ? 1 : 0), 0),
    };
  };

  return (
    <>
      <PageHead
        title={run()?.workflow_name ?? runId()}
        subtitle={
          <div class="row" style={{ gap: '10px', 'flex-wrap': 'wrap' }}>
            <CopyId value={runId()} />
            <Show when={rerunOf()}>
              {(src) => (
                <span>
                  rerun of{' '}
                  <A
                    href={`/workflows/${encodeURIComponent(src())}`}
                    style={{ 'text-decoration': 'underline' }}
                  >
                    {src()}
                  </A>
                </span>
              )}
            </Show>
            <Show when={(run()?.retry_count ?? 0) > 0}>
              <span>
                retried {run()!.retry_count} time{run()!.retry_count === 1 ? '' : 's'}
              </span>
            </Show>
          </div>
        }
        actions={
          <>
            <Show when={isRunning()}>
              <button
                class="btn btn-sm"
                disabled={run()?.kill_requested}
                onClick={() => void cancelRun(runId(), run()!.workflow_name, refreshAll)}
              >
                Cancel
              </button>
            </Show>
            <Show when={canRetry()}>
              <button
                class="btn btn-sm btn-primary"
                onClick={() => void retryRun(runId(), run()!.workflow_name, refreshAll)}
              >
                Retry from failed
              </button>
            </Show>
            <Show when={isTerminal()}>
              <button
                class="btn btn-sm"
                onClick={() => void rerunRun(runId(), (to) => navigate(to))}
              >
                Rerun
              </button>
            </Show>
            <Show when={run()?.schedule_name}>
              {(name) => (
                <A
                  href={`/schedules/${encodeURIComponent(name())}`}
                  class="btn btn-sm"
                >
                  Schedule
                </A>
              )}
            </Show>
            <Show when={run()}>
              {(r) => (
                <Pill tone={runTone(r().status)} dot pulse={isRunning()}>
                  {r().kill_requested && r().status === 'running'
                    ? 'stopping'
                    : r().status}
                </Pill>
              )}
            </Show>
          </>
        }
      />

      <div class="page-body">
        <Show
          when={!error()}
          fallback={
            <div class="panel">
              <Empty>{error()}</Empty>
            </div>
          }
        >
          <Show when={data()} fallback={<div class="ov-skeleton" />}>
            {(d) => (
              <div class="page-stack">
                <Show when={d().run.error}>
                  <Panel title={d().run.status === 'killed' ? 'Why it stopped' : 'Why it failed'}>
                    <Reason error={d().run.error} tone={d().run.status === 'killed' ? 'warn' : 'bad'} />
                  </Panel>
                </Show>

                <Trace
                  title="Trace"
                  live={isRunning()}
                  from={parse(d().run.created_at)}
                  to={isRunning() ? null : parse(d().run.finished_at)}
                  rows={rows()}
                  legend={['run', 'wait', 'handoff', 'skipped', 'failed']}
                  empty="This run has no steps."
                  stats={[
                    {
                      label: 'Duration',
                      value: short(
                        Math.max(0, (parse(d().run.finished_at) ?? Date.now()) - (parse(d().run.created_at) ?? Date.now())),
                      ),
                    },
                    { label: 'Steps', value: `${counts().done}/${counts().total}` },
                    { label: 'Failed', value: String(counts().failed), tone: counts().failed > 0 ? 'bad' : undefined },
                    { label: 'Dispatches', value: String(counts().dispatches) },
                    { label: 'Started', value: relativeStamp(d().run.created_at) },
                    { label: 'Trigger', value: d().run.trigger ?? (d().run.schedule_name ? 'cron' : 'manual') },
                  ]}
                />

                <Show when={d().run.input}>
                  <Panel title="Input">
                    <pre class="json">{JSON.stringify(d().run.input, null, 2)}</pre>
                  </Panel>
                </Show>

                <Show when={rerunList().length > 0}>
                  <Panel title="Reruns" flush>
                    <ul class="list">
                      <For each={rerunList()}>
                        {(r) => (
                          <li>
                            <A href={`/workflows/${encodeURIComponent(r.id)}`} class="list-item" classList={{ [runTone(r.status)]: true }}>
                              <span class="dot" classList={{ [runTone(r.status)]: true, pulse: r.status === 'running' }} />
                              <span class="list-text">
                                <span class="list-title mono">{r.id}</span>
                                <span class="list-detail">
                                  {r.status} · {relativeStamp(r.created_at)}
                                </span>
                              </span>
                              <span class="list-meta">{elapsed(r.created_at, r.finished_at)}</span>
                              <Chevron />
                            </A>
                          </li>
                        )}
                      </For>
                    </ul>
                  </Panel>
                </Show>
              </div>
            )}
          </Show>
        </Show>
      </div>
    </>
  );
}

/**
 * The opened body of one step in the trace.
 *
 * Two sources, and they answer different questions. The step row says what the
 * ENGINE decided — dispatched, skipped, and why. The job row says what the
 * BACKEND did, one line per attempt, and it is the only place the attempts before
 * the last one survive: `_00_step_run.error` keeps just the attempt that ended
 * the job. A step that failed after two timeouts and a 500 looks identical to one
 * that 500'd immediately until you read the job.
 *
 * The job is fetched here rather than with the run so the cost is per opened lane
 * — a fan-out run has hundreds of steps and nobody opens hundreds of them.
 */
function StepDetail(props: { step: StepRun; onChange: () => void }) {
  const s = () => props.step;
  const jobId = () => orNull(s().job_id);

  const [jobResult] = createResource(jobId, (id) =>
    api.getResult<{ job: JobDetail }>(`/jobs/${encodeURIComponent(id)}`),
  );
  const job = () => {
    const r = jobResult();
    return r?.ok ? r.value.job : undefined;
  };
  // A pruned job is the expected case on an old run, not a malfunction, so the
  // message is shown where the job would have been rather than as an error.
  const jobMissing = () => {
    const r = jobResult();
    return r && !r.ok ? r.message : undefined;
  };

  // `retries` counts retries, so the attempt count is one more than it.
  const attemptLabel = () => {
    const j = job();
    if (!j || j.retries === null) return null;
    return `attempt ${j.retries + 1} of ${(j.max_retries ?? 0) + 1}`;
  };
  const dispatches = () => s().dispatch_attempts ?? 0;

  return (
    <div class="stack" style={{ gap: '10px', 'padding-top': '10px' }}>
      <KeyValue
        rows={[
          [
            'Job',
            <div class="row" style={{ 'flex-wrap': 'wrap' }}>
              <Show when={jobId()} fallback={<span>—</span>}>
                {(id) => (
                  <A href={`/jobs/${encodeURIComponent(id())}`}>{id()}</A>
                )}
              </Show>
              <Show when={job()}>
                {(j) => (
                  <>
                    <Pill tone={jobTone(j().status)}>{j().status}</Pill>
                    <Show when={attemptLabel()}>
                      <span class="mini">{attemptLabel()}</span>
                    </Show>
                  </>
                )}
              </Show>
              {/* Job-level controls act on the SSP running it, not on the
                  workflow: killing fails this step; retrying re-runs the SAME
                  job row, which is the right tool when the step's own job
                  failed and the workflow has not moved on yet. */}
              <Show when={jobId() && s().status === 'dispatched'}>
                <button
                  class="btn btn-sm"
                  onClick={() => void killJob(jobId()!, props.onChange)}
                >
                  Kill job
                </button>
              </Show>
              <Show when={jobId() && s().status === 'failed'}>
                <button
                  class="btn btn-sm"
                  onClick={() => void retryJob(jobId()!, props.onChange)}
                >
                  Retry job
                </button>
              </Show>
            </div>,
          ],
          ['Started', formatStamp(s().started_at)],
          ['Finished', formatStamp(s().finished_at)],
          // Only worth a row when it is not the ordinary 0-or-1. Above that, the
          // step lost a dispatch and was re-sent, which is a different story from
          // a job that ran and failed.
          ...(dispatches() > 1
            ? ([
                [
                  'Dispatches',
                  <span class="tone-warn">
                    {dispatches()} (re-sent after a lost dispatch)
                  </span>,
                ],
              ] as [string, JSX.Element][])
            : []),
        ]}
      />

      <Show when={s().error}>
        <div>
          <div class="tag" style={{ 'margin-bottom': '6px' }}>
            {s().status === 'skipped' ? 'Why it did not run' : 'Error'}
          </div>
          <Reason
            error={s().error}
            tone={s().status === 'skipped' ? 'warn' : 'bad'}
          />
        </div>
      </Show>

      <Show when={jobId()}>
        <div>
          <div class="tag" style={{ 'margin-bottom': '6px' }}>
            Attempts
          </div>
          <Show
            when={job()}
            fallback={
              <Empty>
                {jobMissing() ?? 'Loading the job…'}
              </Empty>
            }
          >
            {(j) => (
              <div class="stack" style={{ gap: '8px' }}>
                <KeyValue
                  rows={[
                    ['Path', j().path],
                    [
                      'Retries',
                      `${j().retries ?? 0} of ${j().max_retries ?? 0}`,
                    ],
                    ...((j().assignee
                      ? [['SSP', j().assignee!]]
                      : []) as [string, JSX.Element][]),
                  ]}
                />
                {/* The Jobs page renders the same history; sharing the
                    component is what keeps a run's view of an attempt from
                    disagreeing with the job's own page. */}
                <JobAttempts job={j()} />
              </div>
            )}
          </Show>
        </div>
      </Show>

      <Show when={s().output}>
        <div>
          <div class="tag" style={{ 'margin-bottom': '6px' }}>
            Output
          </div>
          <pre class="json">{JSON.stringify(s().output, null, 2)}</pre>
        </div>
      </Show>
    </div>
  );
}
