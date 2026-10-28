import { Show, createSignal, onCleanup, onMount } from 'solid-js';
import { A, useNavigate } from '@solidjs/router';
import { api, openStream, streamsAvailable } from '../api/client';
import { PageHead, Pill } from '../components/Chrome';
import { Trace, hueOf, short, type TraceRow, type TraceState } from '../components/Trace';
import { relativeStamp } from '../lib/format';
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
/** How often runs are re-read where the host has no stream. */
const POLL_MS = 3000;

export function Workflows() {
  const navigate = useNavigate();
  const [runs, setRuns] = createSignal<WorkflowRun[]>([]);
  const [connected, setConnected] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [filter, setFilter] = createSignal('');

  onMount(() => {
    // A host without streams (an SSP serving the API alone) is polled.
    if (!streamsAvailable()) {
      let cancelled = false;
      const load = async () => {
        const res = await api.getResult<{ runs: WorkflowRun[] }>('/workflows/runs');
        if (cancelled) return;
        if (res.ok) {
          setRuns(res.value.runs ?? []);
          setConnected(true);
          setError(null);
        } else {
          setConnected(false);
          setError(res.message);
        }
      };
      void load();
      const timer = setInterval(load, POLL_MS);
      onCleanup(() => {
        cancelled = true;
        clearInterval(timer);
      });
      return;
    }
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

  const ms = (iso: string | null | undefined) => {
    if (!iso || iso === 'NONE') return null;
    const v = Date.parse(iso);
    return Number.isNaN(v) ? null : v;
  };

  const stateOf = (status: string): TraceState =>
    status === 'running'
      ? 'running'
      : status === 'success'
        ? 'done'
        : status === 'failed' || status === 'killed'
          ? 'failed'
          : 'skipped';

  /**
   * One collapsible group per workflow, its runs under it, newest first. The
   * group line shows every run's span, so a workflow that runs every five
   * minutes reads as a rhythm and a gap in it stands out.
   */
  const rows = (): TraceRow[] => {
    const byName = new Map<string, WorkflowRun[]>();
    for (const r of visible()) byName.set(r.workflow_name, [...(byName.get(r.workflow_name) ?? []), r]);
    const out: TraceRow[] = [];
    for (const [name, list] of byName) {
      const failed = list.filter((r) => r.status === 'failed' || r.status === 'killed').length;
      const live = list.filter((r) => r.status === 'running').length;
      out.push({
        id: `wf:${name}`,
        group: true,
        label: name,
        sub: [`${list.length} run${list.length === 1 ? '' : 's'}`, live ? `${live} running` : null, failed ? `${failed} failed` : null]
          .filter(Boolean)
          .join(' · '),
        color: hueOf(name),
        state: failed ? 'failed' : live ? 'running' : 'done',
        start: null,
        end: null,
      });
      for (const r of list) {
        const state = stateOf(r.status);
        const start = ms(r.created_at);
        const end = state === 'running' ? null : (ms(r.finished_at) ?? ms(r.updated_at));
        out.push({
          id: r.id,
          parent: `wf:${name}`,
          depth: 1,
          label: relativeStamp(r.created_at),
          sub: [r.trigger === 'manual' ? 'manual' : r.schedule_name ? 'scheduled' : null, r.kill_requested && state === 'running' ? 'stopping' : null]
            .filter(Boolean)
            .join(' · '),
          badge: r.trigger === 'manual' ? 'M' : 'S',
          color: hueOf(name),
          state,
          start,
          end,
          meta: [
            start != null && end != null ? short(end - start) : null,
            r.error?.reason ? String(r.error.reason) : null,
          ]
            .filter(Boolean)
            .join(' · ') || undefined,
          href: `/workflows/${encodeURIComponent(r.id)}`,
          actions: () => (
            <Show
              when={r.status === 'running'}
              fallback={
                <button class="btn btn-sm" onClick={() => void rerunRun(r.id, navigate)}>
                  Rerun
                </button>
              }
            >
              <button
                class="btn btn-sm"
                disabled={r.kill_requested}
                onClick={() => void cancelRun(r.id, r.workflow_name)}
              >
                Cancel
              </button>
            </Show>
          ),
        });
      }
    }
    return out;
  };

  const failedCount = () => runs().filter((r) => r.status === 'failed' || r.status === 'killed').length;
  const typical = () => {
    const ds = runs()
      .filter((r) => r.status === 'success')
      .map((r) => (ms(r.finished_at) ?? 0) - (ms(r.created_at) ?? 0))
      .filter((d) => d > 0)
      .sort((a, b) => a - b);
    return ds.length ? short(ds[Math.floor(ds.length / 2)]) : '—';
  };

  return (
    <>
      <PageHead
        title="Workflows"
        actions={
          <div class="row">
            <Pill tone={connected() ? 'live' : 'idle'} dot pulse={connected()}>
              {connected() ? 'live' : 'connecting'}
            </Pill>
            <A href="/schedules" class="btn btn-sm">
              Schedules
            </A>
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

          <Trace
            title="Runs"
            live
            windows={[
              { key: '24h', label: 'Last day', ms: 24 * 3_600_000 },
              { key: '1h', label: 'Last hour', ms: 3_600_000 },
              { key: '6h', label: 'Last 6 hours', ms: 6 * 3_600_000 },
              { key: '7d', label: 'Last week', ms: 7 * 24 * 3_600_000 },
            ]}
            treeLabel="Workflows"
            rows={rows()}
            legend={['run', 'skipped', 'failed']}
            empty={
              runs().length > 0
                ? 'Nothing matches that search.'
                : 'No workflow runs recorded yet.'
            }
            stats={[
              { label: 'Running', value: String(running()) },
              { label: 'Failed', value: String(failedCount()), tone: failedCount() > 0 ? 'bad' : undefined },
              { label: 'Typical run', value: typical() },
              { label: 'Shown', value: String(runs().length) },
            ]}
            tools={
              <input
                type="search"
                placeholder="Search workflow, schedule or error"
                value={filter()}
                onInput={(e) => setFilter(e.currentTarget.value)}
                aria-label="Search runs"
              />
            }
          />
        </div>
      </div>
    </>
  );
}
