import { For, Show } from 'solid-js';
import { A } from '@solidjs/router';
import { Bento, Empty, PageHead, Panel, Pill, StatusDot, Tile } from '../components/Chrome';
import { BootstrapProgress } from '../components/BootstrapProgress';
import { ActivityStrip, OpBadge } from '../components/Actions';
import { AllSspsActions, SspActions, SspLogsLink } from '../components/ClusterActions';
import { formatBytes, formatCount, formatDuration, formatUptime } from '../lib/format';
import { sspTone } from '../lib/status';
import type { Overview } from '../api/types';

/** Every SSP, with its env, its actions, and a link to its log stream. */
export function Ssps(props: { data: Overview | undefined; refresh: () => void }) {
  return (
    <>
      <PageHead
        crumb="Dashboard"
        title="Sync processors"
        actions={
          <Show when={props.data}>
            {(d) => <AllSspsActions count={d().ssps.length} onDone={props.refresh} />}
          </Show>
        }
      />

      <div class="page-body">
        <ActivityStrip operations={props.data?.operations} />

        <Show when={props.data} fallback={<Empty>Loading…</Empty>}>
          {(data) => (
            <Show
              when={data().ssps.length > 0}
              fallback={
                <Panel>
                  <Empty>
                    No SSPs are registered with this scheduler, so no client is
                    receiving live updates.
                  </Empty>
                </Panel>
              }
            >
              {/* One tile per SSP on the bento, so a row of processors shares
                  one bottom edge whatever each one is doing. */}
              <Bento>
                <For each={data().ssps}>
                  {(ssp, i) => (
                    <Tile
                      i={i()}
                      span={4}
                      label={ssp.id}
                      raw
                      sub={`${ssp.ip ?? 'no address'} · v${ssp.version}`}
                      tone={sspTone(ssp.status)}
                      pulse={ssp.status !== 'ready'}
                      actions={
                        <>
                          <OpBadge operations={data().operations} target={ssp.id} />
                          <Pill tone={sspTone(ssp.status)}>
                            <StatusDot tone={sspTone(ssp.status)} />
                            {ssp.status}
                          </Pill>
                        </>
                      }
                    >
                      <Show when={ssp.status !== 'ready'}>
                        <BootstrapProgress
                          ssp={ssp}
                          timeoutSecs={data().bootstrap_timeout_secs}
                        />
                      </Show>

                      <dl class="kv">
                        <dt>Live views</dt>
                        <dd>{formatCount(ssp.views)}</dd>
                        <dt>Uptime</dt>
                        <dd>{formatUptime(ssp.uptime_seconds)}</dd>
                        <dt>Last heartbeat</dt>
                        <dd>{formatUptime(ssp.last_heartbeat_seconds_ago)} ago</dd>
                        <dt>In this phase</dt>
                        <dd>{formatUptime(ssp.state_seconds)}</dd>
                        <dt>Buffered events</dt>
                        <dd>{formatCount(ssp.buffered_events)}</dd>
                      </dl>

                      <Show when={ssp.publication}>{(publication) => <dl class="kv">
                        <dt>Pending publication</dt><dd>{formatCount(publication().pending_operations)} operations · {formatBytes(publication().pending_bytes)}</dd>
                        <dt>Oldest publication wait</dt><dd>{formatDuration(publication().oldest_age_ms)}</dd>
                        <dt>Parked batches</dt><dd>{formatCount(publication().parked_batches)}</dd>
                      </dl>}</Show>

                      <Show when={ssp.env && Object.keys(ssp.env).length > 0}>
                        <details>
                          <summary class="faint" style={{ cursor: 'pointer', 'font-size': '12px' }}>
                            Environment
                          </summary>
                          <dl class="kv" style={{ 'margin-top': '10px' }}>
                            <For each={Object.entries(ssp.env!)}>
                              {([k, v]) => (
                                <>
                                  <dt>{k}</dt>
                                  <dd>{v}</dd>
                                </>
                              )}
                            </For>
                          </dl>
                        </details>
                      </Show>

                      <div class="spread tile-end" style={{ 'padding-top': '4px' }}>
                        <A
                          href={`/logs?source=ssp:${encodeURIComponent(ssp.id)}`}
                          class="btn btn-sm"
                        >
                          Stream logs
                        </A>
                        <SspActions ssp={ssp} size="sm" onDone={props.refresh} />
                        <SspLogsLink ssp={ssp} />
                      </div>
                    </Tile>
                  )}
                </For>
              </Bento>
            </Show>
          )}
        </Show>
      </div>
    </>
  );
}
