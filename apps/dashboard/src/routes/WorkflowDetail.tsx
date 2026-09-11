import { For, Show, createResource, onCleanup, type JSX } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Cell,
  CopyId,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Rail,
  Reason,
} from '../components/Chrome';
import { Timeline, type Lane } from '../components/Timeline';
import {
  decodeParam,
  elapsed,
  formatDuration,
  formatStamp,
  orNull,
  relativeStamp,
} from '../lib/format';
import { jobTone, runTone, stepTone } from '../lib/status';
import { cancelRun, killJob, rerunRun, retryJob, retryRun } from '../lib/runActions';
import type { JobDetail, StepRun, WorkflowRun, WorkflowRunDetail } from '../api/types';

/**
 * One workflow run.
 *
 * Laid out the way a run is actually read: identity and outcome at the top,
 * then WHERE THE TIME WENT (the timeline), then the payloads. The timeline is
 * the primary object on this page, not a supplement to a table — a run that is
 * stuck is diagnosed by seeing which bar never ended and what it sits behind.
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

  const lanes = (): Lane[] => {
    const byName = new Map((data()?.steps ?? []).map((s) => [s.step, s]));
    return orderedSteps().map((s) => {
      const started = hasStarted(s.status)
        ? startOf(s, byName)
        : { at: null, estimated: false };
      return {
        id: s.step,
        label: s.step,
        start: started.at,
        estimated: started.estimated,
        end: hasStarted(s.status) ? parse(s.finished_at) : null,
        status: s.status,
        tone: stepTone(s.status),
        dependsOn: s.depends_on,
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
    };
  };

  return (
    <>
      <PageHead
        crumb="Workflows / Run"
        title={run()?.workflow_name ?? runId()}
        subtitle={
          <div class="stack" style={{ gap: '3px' }}>
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
                  schedule · {name()}
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
          <Show when={data()} fallback={<Empty>Loading…</Empty>}>
            {(d) => (
              <div class="stack">
                <Rail>
                  <Cell
                    label="Status"
                    tone={runTone(d().run.status)}
                    pulse={isRunning()}
                    value={d().run.status}
                    foot={d().run.kill_requested ? 'kill requested' : undefined}
                  />
                  <Cell
                    label="Duration"
                    value={elapsed(d().run.created_at, d().run.finished_at)}
                    foot={d().run.finished_at ? 'total' : 'still running'}
                  />
                  <Cell
                    label="Started"
                    value={relativeStamp(d().run.created_at)}
                    foot={formatStamp(d().run.created_at)}
                  />
                  <Cell
                    label="Steps"
                    value={`${counts().done}/${counts().total}`}
                    foot={
                      counts().failed > 0
                        ? `${counts().failed} failed`
                        : 'succeeded'
                    }
                    tone={counts().failed > 0 ? 'bad' : undefined}
                  />
                </Rail>

                <Show when={d().run.error}>
                  <Panel title="Run error">
                    <Reason
                      error={d().run.error}
                      tone={d().run.status === 'killed' ? 'warn' : 'bad'}
                    />
                  </Panel>
                </Show>

                <Panel
                  title="Timeline"
                  sub="Every step on one axis. Click a step for its payload."
                  actions={
                    <span class="tag">
                      <span class="val">
                        {formatDuration(
                          Math.max(
                            0,
                            (parse(d().run.finished_at) ?? Date.now()) -
                              (parse(d().run.created_at) ?? Date.now()),
                          ),
                        )}
                      </span>{' '}
                      total
                    </span>
                  }
                  flush
                >
                  <Timeline
                    lanes={lanes()}
                    windowStart={parse(d().run.created_at)}
                    windowEnd={parse(d().run.finished_at)}
                  />
                </Panel>

                <div class="grid grid-2">
                  <Panel title="Run">
                    <KeyValue
                      rows={[
                        ['Workflow', d().run.workflow_name],
                        ['Schedule', d().run.schedule_name ?? '—'],
                        ['Target table', d().run.target_table ?? '—'],
                        ['Created', formatStamp(d().run.created_at)],
                        ['Updated', formatStamp(d().run.updated_at)],
                        ['Finished', formatStamp(d().run.finished_at)],
                      ]}
                    />
                  </Panel>

                  <Panel title="Input">
                    <Show
                      when={d().run.input}
                      fallback={<Empty>This run carried no input.</Empty>}
                    >
                      <pre class="json">
                        {JSON.stringify(d().run.input, null, 2)}
                      </pre>
                    </Show>
                  </Panel>
                </div>

                <Show when={rerunList().length > 0}>
                  <Panel
                    title="Reruns"
                    sub="Runs created from this one, newest first"
                    flush
                  >
                    <div class="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>Run</th>
                            <th>Status</th>
                            <th>Started</th>
                            <th>Duration</th>
                          </tr>
                        </thead>
                        <tbody>
                          <For each={rerunList()}>
                            {(r) => (
                              <tr
                                class="clickable"
                                onClick={() =>
                                  navigate(`/workflows/${encodeURIComponent(r.id)}`)
                                }
                              >
                                <td>
                                  <div class="row">
                                    <span class="dot" classList={{ [runTone(r.status)]: true }} />
                                    <span class="id">{r.id}</span>
                                  </div>
                                </td>
                                <td data-label="Status">
                                  <Pill tone={runTone(r.status)}>{r.status}</Pill>
                                </td>
                                <td class="dim" data-label="Started">
                                  {relativeStamp(r.created_at)}
                                </td>
                                <td class="dim" data-label="Duration">
                                  {elapsed(r.created_at, r.finished_at)}
                                </td>
                              </tr>
                            )}
                          </For>
                        </tbody>
                      </table>
                    </div>
                  </Panel>
                </Show>

                <Panel title="DAG" sub="As stored on the run">
                  <pre class="json">{JSON.stringify(d().run.dag, null, 2)}</pre>
                </Panel>
              </div>
            )}
          </Show>
        </Show>
      </div>
    </>
  );
}

/**
 * The expanded body of one timeline lane.
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

  const attempts = () => job()?.errors ?? [];
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
          ['Status', <Pill tone={stepTone(s().status)}>{s().status}</Pill>],
          [
            'Depends on',
            s().depends_on.length ? s().depends_on.join(', ') : '—',
          ],
          [
            'Job',
            <div class="row" style={{ 'flex-wrap': 'wrap' }}>
              <span>{jobId() ?? '—'}</span>
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
          ['Queued', formatStamp(s().created_at)],
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
                      `${j().retries ?? 0} of ${j().max_retries ?? 0} (${j().retry_strategy ?? 'linear'})`,
                    ],
                    ...((j().assignee
                      ? [['SSP', j().assignee!]]
                      : []) as [string, JSX.Element][]),
                    ...((j().timeout
                      ? [['Timeout', formatDuration(j().timeout! * 1000)]]
                      : []) as [string, JSX.Element][]),
                    ...((j().delay
                      ? [['Delay', formatDuration(j().delay!)]]
                      : []) as [string, JSX.Element][]),
                    ['Last write', formatStamp(j().updated_at)],
                  ]}
                />
                <Show
                  when={attempts().length > 0}
                  fallback={
                    <Empty>
                      No failed attempt recorded.
                      {j().status === 'failed'
                        ? ' The runner could not append to the job’s `errors` array; its log has the rejection.'
                        : ''}
                    </Empty>
                  }
                >
                  {/* Oldest first, so the list reads as the history it is: what
                      the backend did on attempt 1, then 2, then the one that
                      exhausted the budget. */}
                  <For each={attempts()}>
                    {(err, i) => (
                      <div class="attempt">
                        <span class="attempt-n">#{i() + 1}</span>
                        <Reason error={err} />
                      </div>
                    )}
                  </For>
                </Show>
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
