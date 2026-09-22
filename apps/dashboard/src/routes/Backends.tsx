import { For, Show, createResource, onCleanup } from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Cell,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Rail,
  StatusDot,
} from '../components/Chrome';
import { Sparkline } from '../components/Sparkline';
import { decodeParam, formatMs, formatStamp, relativeStamp } from '../lib/format';
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
  return parts.join(' · ');
}

export function Backends() {
  const [data, { refetch }] = createResource(() =>
    api.get<{ backends: BackendSummary[]; check_interval_secs: number }>('/backends'),
  );

  // Match the scheduler's own probe cadence rather than polling faster than
  // the data can change.
  const timer = setInterval(refetch, 10_000);
  onCleanup(() => clearInterval(timer));

  return (
    <>
      <PageHead crumb="Dashboard" title="Backends" />

      <div class="page-body">
      <Show when={data()} fallback={<Empty>Loading…</Empty>}>
        {(d) => (
          <Show
            when={d().backends.length > 0}
            fallback={
              <Panel>
                <Empty>
                  No backends configured. The scheduler reads them from{' '}
                  <span class="dim">SPKY_BACKENDS</span>.
                </Empty>
              </Panel>
            }
          >
            <Panel flush>
              <div class="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Status</th>
                      <th>Response</th>
                      <th>Last healthy</th>
                      <th>Healthcheck</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={d().backends}>
                      {(b) => (
                        <tr>
                          <td>
                            <A href={`/backends/${encodeURIComponent(b.name)}`}>
                              <div class="row">
                                <StatusDot tone={backendTone(b.status)} />
                                {b.name}
                              </div>
                            </A>
                          </td>
                          <td data-label="Status">
                            <Pill tone={backendTone(b.status)}>{b.status}</Pill>
                          </td>
                          <td class="dim" data-label="Response">
                            <Show when={b.pool} fallback={formatMs(b.response_time_ms)}>
                              {(p) => <>pool {p().pool} · {poolSummary(p())}</>}
                            </Show>
                          </td>
                          <td class="dim" data-label="Last healthy">{relativeStamp(b.last_healthy)}</td>
                          <td class="ghost truncate" data-label="Healthcheck">{b.healthcheck_url}</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </Panel>
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

  const points = () =>
    (data()?.history ?? []).map((h) => ({ ts: h.at, ms: h.ms, ok: h.ok }));

  const windowMinutes = () => {
    const d = data();
    if (!d || d.history.length === 0) return null;
    return Math.round((d.history.length * d.check_interval_secs) / 60);
  };

  return (
    <>
      <PageHead
        crumb="Backends"
        title={name()}
        subtitle={<Show when={data()}>{(b) => <>{b().healthcheck_url}</>}</Show>}
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
            <div class="panel">
              <Empty>{error()}</Empty>
            </div>
          }
        >
          <Show when={data()} fallback={<Empty>Loading…</Empty>}>
            {(b) => (
              <div class="stack">
                <Rail>
                  <Cell
                    label="Status"
                    tone={backendTone(b().status)}
                    value={b().status}
                  />
                  <Show
                    when={b().pool}
                    fallback={
                      <Cell
                        label="Last response"
                        value={formatMs(b().response_time_ms)}
                      />
                    }
                  >
                    {(p) => (
                      <Cell
                        label="Machines"
                        value={`${p().ready + p().starting}`}
                        foot={`pool ${p().pool}`}
                      />
                    )}
                  </Show>
                  <Cell
                    label="Last healthy"
                    value={relativeStamp(b().last_healthy)}
                    foot={formatStamp(b().last_healthy)}
                  />
                  <Cell
                    label="Check every"
                    value={`${b().check_interval_secs}`}
                    unit="s"
                    foot={`checked ${relativeStamp(b().last_checked)}`}
                  />
                </Rail>

                <Show when={b().pool}>
                  {(p) => (
                    <Panel
                      title="Machine pool"
                      sub="This backend runs on pool machines, created on demand; there is no container to probe between jobs"
                    >
                      <KeyValue
                        rows={[
                          ['Pool', p().pool],
                          ['Ready machines', `${p().ready}`],
                          ['Booting machines', `${p().starting}`],
                          ['Busy slots', `${p().busy_slots}`],
                          ['Queued jobs', `${p().queued}`],
                          ['Paused', p().paused ? 'yes' : 'no'],
                          ['Breaker', p().breaker_open ? 'open' : 'closed'],
                          ...(p().error ? [['Last error', p().error] as [string, string]] : []),
                        ]}
                      />
                    </Panel>
                  )}
                </Show>

                <Show when={!b().pool}>
                <Panel
                  title="Response time"
                  sub={
                    <Show when={windowMinutes()} fallback="No probes recorded yet">
                      {(m) => (
                        <>
                          Last {m()} {m() === 1 ? 'minute' : 'minutes'} · failures
                          drawn as gaps
                        </>
                      )}
                    </Show>
                  }
                >
                  <Sparkline points={points()} height={90} />
                </Panel>
                </Show>

                <div class="grid grid-2">
                  <Panel title="Target">
                    <KeyValue
                      rows={[
                        ['URL', b().url],
                        ['Healthcheck', b().healthcheck_url],
                        ['Address', b().ip ?? '—'],
                        ['Port', b().port ?? '—'],
                      ]}
                    />

                    {/* Said plainly rather than left as a missing tab: the
                        scheduler reaches backends over HTTP health checks and
                        has no pipe to their stdout. */}
                    <Show when={!b().logs_available}>
                      <div
                        class="ghost prose"
                        style={{ 'margin-top': '14px', 'font-size': '11.5px' }}
                      >
                        Log streaming is not available for backends — the
                        scheduler health-checks them over HTTP and has no access
                        to their output. Scheduler and SSP logs are under{' '}
                        <A href="/logs" style={{ 'text-decoration': 'underline' }}>
                          Logs
                        </A>
                        .
                      </div>
                    </Show>
                  </Panel>

                  <Show when={b().env && Object.keys(b().env!).length > 0}>
                    <Panel
                      title="Environment"
                      sub="Secrets are masked by the scheduler"
                    >
                      <KeyValue
                        rows={Object.entries(b().env!).map(
                          ([k, v]) => [k, v] as [string, string],
                        )}
                      />
                    </Panel>
                  </Show>
                </div>
              </div>
            )}
          </Show>
        </Show>
      </div>
    </>
  );
}
