import { For, Show, createResource, onCleanup } from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Chevron,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  ReasonLine,
  SkeletonList,
  StatCard,
} from '../components/Chrome';
import { Sparkline } from '../components/Sparkline';
import {
  decodeParam,
  formatCount,
  formatMs,
  formatStamp,
  isAbsent,
  relativeStamp,
  splitValue,
} from '../lib/format';
import { backendTone } from '../lib/status';
import type { BackendDetail, BackendSummary, PoolBacking } from '../api/types';

/** One line on a pool backend's machines, in place of a probe time. */
function poolSummary(p: PoolBacking): string {
  const machines = p.ready + p.starting;
  const parts = [`${machines} ${machines === 1 ? 'machine' : 'machines'}`];
  if (p.starting > 0) parts.push(`${p.starting} booting`);
  if (p.queued > 0) parts.push(`${p.queued} queued`);
  if (p.paused) parts.push('paused');
  if (p.breaker_open) parts.push('breaker open');
  return parts.join(', ');
}

/** A backend that is not answering its health check. */
const failing = (b: BackendSummary) => backendTone(b.status) === 'bad' || b.status === 'unhealthy';

/** When the backend last passed its check, phrased for its current state. */
function healthLine(b: BackendSummary): string {
  if (isAbsent(b.last_healthy)) return 'never healthy';
  return `${failing(b) ? 'last healthy' : 'healthy'} ${relativeStamp(b.last_healthy)}`;
}

export function Backends() {
  const [data, { refetch }] = createResource(() =>
    api.get<{ backends: BackendSummary[]; check_interval_secs: number }>('/backends'),
  );

  // Match the scheduler's own probe cadence rather than polling faster than
  // the data can change.
  const timer = setInterval(refetch, 10_000);
  onCleanup(() => clearInterval(timer));

  const healthy = () => data()?.backends.filter((b) => !failing(b)).length ?? 0;

  return (
    <>
      <PageHead
        title="Backends"
        subtitle={
          <Show when={data()?.backends.length}>
            {(n) => (healthy() === n() ? `All ${n()} answering` : `${n() - healthy()} of ${n()} failing`)}
          </Show>
        }
      />

      <div class="page-body">
        <Show when={data()} fallback={<SkeletonList rows={3} />}>
          {(d) => (
            <Show
              when={d().backends.length > 0}
              fallback={
                <Panel>
                  <Empty>No backends configured.</Empty>
                </Panel>
              }
            >
              <ul class="list">
                <For each={d().backends}>
                  {(b) => (
                    <li>
                      <A
                        href={`/backends/${encodeURIComponent(b.name)}`}
                        class="list-item"
                        classList={{ [backendTone(b.status)]: true }}
                      >
                        <span class="list-icon" aria-hidden="true">
                          <svg viewBox="0 0 16 16">
                            <rect x="2.5" y="3" width="11" height="4" rx="1.2" />
                            <rect x="2.5" y="9" width="11" height="4" rx="1.2" />
                            <path d="M5 5h.01M5 11h.01" />
                          </svg>
                        </span>
                        <span class="list-text">
                          <span class="list-title">{b.name}</span>
                          <span class="list-detail">
                            <Show
                              when={b.pool}
                              fallback={failing(b) ? `${b.status}, ${b.healthcheck_url}` : b.status}
                            >
                              {(p) => <>pool {p().pool}, {poolSummary(p())}</>}
                            </Show>
                          </span>
                        </span>
                        <span class="bk-meta">
                          <Show when={!b.pool && b.response_time_ms !== null}>
                            <span class="bk-ms" classList={{ 'tone-bad': failing(b) }}>
                              {formatMs(b.response_time_ms)}
                            </span>
                          </Show>
                          <span class="list-meta">{healthLine(b)}</span>
                        </span>
                        <Chevron />
                      </A>
                    </li>
                  )}
                </For>
              </ul>
            </Show>
          )}
        </Show>
      </div>
    </>
  );
}

export function BackendDetailView() {
  const params = useParams<{ name: string }>();
  // Same raw-path-segment caveat as the workflow detail view: the router does
  // not decode, so a backend named with anything URL-special would otherwise be
  // encoded twice on the way to the API.
  const name = () => decodeParam(params.name);
  const [result, { refetch }] = createResource(name, (n) =>
    api.getResult<BackendDetail>(`/backends/${encodeURIComponent(n)}`),
  );

  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  const timer = setInterval(refetch, 10_000);
  onCleanup(() => clearInterval(timer));

  const points = () => (data()?.history ?? []).map((h) => ({ ts: h.at, ms: h.ms, ok: h.ok }));

  const windowMinutes = () => {
    const d = data();
    if (!d || d.history.length === 0) return null;
    return Math.max(1, Math.round((d.history.length * d.check_interval_secs) / 60));
  };

  return (
    <>
      <PageHead
        title={name()}
        subtitle={<Show when={data()}>{(b) => <span class="mono">{b().url}</span>}</Show>}
        actions={
          <Show when={data()}>
            {(b) => (
              <Pill tone={backendTone(b().status)} dot>
                {b().status}
              </Pill>
            )}
          </Show>
        }
      />

      <div class="page-body">
        <Show
          when={!error()}
          fallback={
            <Panel>
              <Empty>{error()}</Empty>
            </Panel>
          }
        >
          <Show when={data()} fallback={<SkeletonList rows={4} />}>
            {(b) => (
              <div class="page-stack">
                <div class="stat-grid">
                  <Show
                    when={b().pool}
                    fallback={
                      <StatCard
                        label="Response time"
                        value={splitValue(formatMs(b().response_time_ms)).value}
                        unit={splitValue(formatMs(b().response_time_ms)).unit}
                        tone={failing(b()) ? 'bad' : undefined}
                        note={`checked ${relativeStamp(b().last_checked)}, every ${b().check_interval_secs}s`}
                      />
                    }
                  >
                    {(p) => (
                      <StatCard
                        label="Machines"
                        value={formatCount(p().ready + p().starting)}
                        note={p().starting > 0 ? `${p().starting} booting` : `pool ${p().pool}`}
                      />
                    )}
                  </Show>
                  <StatCard
                    label="Last healthy"
                    value={isAbsent(b().last_healthy) ? 'never' : relativeStamp(b().last_healthy)}
                    tone={failing(b()) ? 'warn' : undefined}
                    note={formatStamp(b().last_healthy)}
                  />
                </div>

                <Show when={b().pool}>
                  {(p) => (
                    <Panel title="Machine pool" sub="Machines start on demand, so there is nothing to probe between jobs">
                      <Show when={p().error}>
                        {(e) => <ReasonLine error={e()} />}
                      </Show>
                      <KeyValue
                        rows={[
                          ['Pool', p().pool],
                          ['Ready', formatCount(p().ready)],
                          ['Booting', formatCount(p().starting)],
                          ['Busy slots', formatCount(p().busy_slots)],
                          ['Queued jobs', formatCount(p().queued)],
                          ['Paused', p().paused ? 'yes' : 'no'],
                          ['Breaker', p().breaker_open ? 'open' : 'closed'],
                        ]}
                      />
                    </Panel>
                  )}
                </Show>

                <Show when={!b().pool}>
                  <Panel
                    title="Response time"
                    sub={
                      <Show when={windowMinutes()} fallback="No checks recorded yet">
                        {(m) => <>Last {m()} min, failed checks show as gaps</>}
                      </Show>
                    }
                  >
                    <Sparkline points={points()} height={90} />
                  </Panel>
                </Show>

                <Panel title="Target">
                  <KeyValue
                    rows={[
                      ['Health check', b().healthcheck_url],
                      ['Address', b().ip ? `${b().ip}${b().port ? `:${b().port}` : ''}` : '—'],
                    ]}
                  />
                  <Show when={b().env && Object.keys(b().env!).length > 0}>
                    <details class="bk-env">
                      <summary>Environment</summary>
                      <KeyValue rows={Object.entries(b().env!).map(([k, v]) => [k, v] as [string, string])} />
                    </details>
                  </Show>
                </Panel>
              </div>
            )}
          </Show>
        </Show>
      </div>
    </>
  );
}
