import { For, Show, createSignal, onCleanup, onMount } from 'solid-js';
import { A, useNavigate } from '@solidjs/router';
import { openStream } from '../api/client';
import { Empty, PageHead, Panel, Pill, ReasonLine, StatusDot } from '../components/Chrome';
import { elapsed, relativeStamp } from '../lib/format';
import { runTone } from '../lib/status';
import { cancelRun, rerunRun } from '../lib/runActions';
import type { WorkflowRun } from '../api/types';

/**
 * Live workflow runs.
 *
 * Fed by `GET /admin/api/workflows/stream`, where the scheduler runs a single
 * 1Hz poller shared by every connected dashboard and pushes only on change. So
 * this view is realtime without each open tab costing the database a query per
 * second, and an idle dashboard costs it nothing at all — the poller stops when
 * the last subscriber leaves.
 */
export function Workflows() {
  const navigate = useNavigate();
  const [runs, setRuns] = createSignal<WorkflowRun[]>([]);
  const [connected, setConnected] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [filter, setFilter] = createSignal('');

  onMount(() => {
    const close = openStream('/workflows/stream', {
      onOpen: () => {
        setConnected(true);
        setError(null);
      },
      onEvent: (event, data) => {
        if (event !== 'runs') return;
        try {
          setRuns((JSON.parse(data) as { runs: WorkflowRun[] }).runs ?? []);
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

  const visible = () => {
    const q = filter().trim().toLowerCase();
    if (!q) return runs();
    return runs().filter(
      (r) =>
        r.workflow_name.toLowerCase().includes(q) ||
        r.status.toLowerCase().includes(q) ||
        (r.schedule_name ?? '').toLowerCase().includes(q) ||
        // The reason is on the row, so it has to be filterable: "timeout" is a
        // far more useful thing to type here than a workflow name.
        JSON.stringify(r.error ?? '').toLowerCase().includes(q),
    );
  };

  const running = () => runs().filter((r) => r.status === 'running').length;

  return (
    <>
      <PageHead
        crumb="Dashboard"
        title="Workflows"
        actions={
          <div class="row">
            <input
              placeholder="Filter…"
              value={filter()}
              onInput={(e) => setFilter(e.currentTarget.value)}
              style={{ width: '180px' }}
            />
            <Pill tone={connected() ? 'live' : 'idle'}>
              <StatusDot tone={connected() ? 'ok' : 'idle'} />
              {connected() ? 'live' : 'connecting…'}
            </Pill>
          </div>
        }
      />

      <div class="page-body">
        <Show when={error()}>
          <div class="banner">
            <span class="dot bad" />
            {error()}
          </div>
        </Show>

        <Panel
          title="Recent runs"
          sub={`${running()} running · ${runs().length} shown`}
          actions={
            <A href="/schedules" class="btn btn-sm">
              schedules
            </A>
          }
          flush
        >
        <Show
          when={visible().length > 0}
          fallback={
            <Empty>
              <Show
                when={runs().length > 0}
                fallback={
                  <>
                    No workflow runs recorded. A schedule with{' '}
                    <span class="mono">history: failures-only</span> leaves no
                    rows behind on success.
                  </>
                }
              >
                Nothing matches that filter.
              </Show>
            </Empty>
          }
        >
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Workflow</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th>Schedule</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                <For each={visible()}>
                  {(run) => (
                    <tr
                      class="clickable"
                      onClick={() =>
                        navigate(`/workflows/${encodeURIComponent(run.id)}`)
                      }
                    >
                      <td>
                        <div class="row">
                          <StatusDot
                            tone={runTone(run.status)}
                            pulse={run.status === 'running'}
                          />
                          {run.workflow_name}
                          <Show when={run.trigger === 'manual'}>
                            <span class="mini">manual</span>
                          </Show>
                        </div>
                        <div class="id" style={{ 'margin-top': '2px' }}>
                          {run.id}
                        </div>
                        {/* The reason, on the row itself. A list of runs where
                            four say `failed` and nothing else forces four clicks
                            to find out whether it is four problems or one. */}
                        <Show when={run.error}>
                          <ReasonLine
                            error={run.error}
                            tone={run.status === 'killed' ? 'warn' : 'bad'}
                          />
                        </Show>
                      </td>
                      <td data-label="Status">
                        <Pill tone={runTone(run.status)}>
                          {run.kill_requested && run.status === 'running'
                            ? 'stopping'
                            : run.status}
                        </Pill>
                      </td>
                      <td class="dim" data-label="Started">{relativeStamp(run.created_at)}</td>
                      <td class="dim" data-label="Duration">
                        {elapsed(run.created_at, run.finished_at)}
                      </td>
                      <td
                        class="ghost"
                        data-label="Schedule"
                        data-empty={!run.schedule_name}
                      >
                        {run.schedule_name ?? '—'}
                      </td>
                      <td data-label="Actions" data-empty={true}>
                        {/* The stream updates the row in place, so no refetch
                            is needed after either action. stopPropagation on
                            the cell keeps the row's own click from firing. */}
                        <div class="row-actions" onClick={(e) => e.stopPropagation()}>
                          <Show
                            when={run.status === 'running'}
                            fallback={
                              <button
                                class="btn btn-sm"
                                onClick={() => void rerunRun(run.id, navigate)}
                              >
                                Rerun
                              </button>
                            }
                          >
                            <button
                              class="btn btn-sm"
                              disabled={run.kill_requested}
                              onClick={() => void cancelRun(run.id, run.workflow_name)}
                            >
                              Cancel
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
    </>
  );
}
