import { For, Show } from 'solid-js';
import { Empty, PageHead, Panel, Pill } from '../components/Chrome';
import { BootstrapProgress } from '../components/BootstrapProgress';
import { OpBadge } from '../components/Actions';
import { ActivityStrip } from '../components/OpCard';
import { AllSspsActions, SspActions, SspLogsLink } from '../components/ClusterActions';
import { formatBytes, formatCount, formatDuration, formatUptime } from '../lib/format';
import { sspTone } from '../lib/status';
import type { Overview, PublicationMetrics, SspEntity } from '../api/types';

/**
 * One card per SSP: its status, how many live views it serves and how long
 * it has been up. Bootstrap progress shows only while it is not ready, and a
 * publication backlog only when there is one. The rest (queue figures, env)
 * sits behind a disclosure for the rare debugging session.
 */

/** A backlog worth surfacing on the card face: parked work or a stale queue. */
function backlog(p: PublicationMetrics | null | undefined): string | null {
  if (!p) return null;
  if (p.parked_batches > 0) {
    return `${formatCount(p.parked_batches)} publication ${p.parked_batches === 1 ? 'batch' : 'batches'} parked`;
  }
  if (p.oldest_age_ms >= 5_000) {
    return `${formatCount(p.pending_operations)} updates waiting ${formatDuration(p.oldest_age_ms)} to publish`;
  }
  return null;
}

function SspCard(props: { ssp: SspEntity; data: Overview; refresh: () => void }) {
  const tone = () => sspTone(props.ssp.status);
  const ready = () => props.ssp.status === 'ready';
  const pub = () => props.ssp.publication;
  const env = () => Object.entries(props.ssp.env ?? {});

  return (
    <section class="ssp-card" classList={{ [tone()]: true }}>
      <header class="ssp-head">
        <span class="dot" classList={{ [tone()]: true, pulse: !ready() }} />
        <span class="ssp-name mono">{props.ssp.id}</span>
        <Pill tone={tone()}>{props.ssp.status}</Pill>
        <OpBadge operations={props.data.operations} target={props.ssp.id} />
        <span class="ssp-menu">
          <SspActions ssp={props.ssp} size="sm" onDone={props.refresh} />
          <SspLogsLink ssp={props.ssp} />
        </span>
      </header>

      <dl class="ssp-figs">
        <div>
          <dt>Live views</dt>
          <dd>{formatCount(props.ssp.views)}</dd>
        </div>
        <div>
          <dt>Uptime</dt>
          <dd>{formatUptime(props.ssp.uptime_seconds)}</dd>
        </div>
        <div>
          <dt>Heartbeat</dt>
          <dd>{formatUptime(props.ssp.last_heartbeat_seconds_ago)} ago</dd>
        </div>
      </dl>

      <Show when={!ready()}>
        <BootstrapProgress ssp={props.ssp} timeoutSecs={props.data.bootstrap_timeout_secs} />
      </Show>

      <Show when={backlog(pub())}>
        {(line) => (
          <div class="ssp-alert">
            <span class="dot warn" />
            {line()}
          </div>
        )}
      </Show>

      <details class="ssp-more">
        <summary>
          <span class="mono">{props.ssp.ip ?? 'no address'}</span>
          <span class="ssp-sep">·</span>
          <span class="mono">{props.ssp.version.replace(/^0\.0\.1-/, '')}</span>
          <span class="ssp-more-toggle" />
        </summary>
        <dl class="kv">
          <dt>In this phase</dt>
          <dd>{formatUptime(props.ssp.state_seconds)}</dd>
          <dt>Buffered events</dt>
          <dd>{formatCount(props.ssp.buffered_events)}</dd>
          <Show when={pub()}>
            {(p) => (
              <>
                <dt>Waiting to publish</dt>
                <dd>
                  {formatCount(p().pending_operations)} updates, {formatBytes(p().pending_bytes)}
                </dd>
                <dt>Oldest wait</dt>
                <dd>{formatDuration(p().oldest_age_ms)}</dd>
                <dt>Parked batches</dt>
                <dd>{formatCount(p().parked_batches)}</dd>
              </>
            )}
          </Show>
          <For each={env()}>
            {([k, v]) => (
              <>
                <dt>{k}</dt>
                <dd>{v}</dd>
              </>
            )}
          </For>
        </dl>
      </details>
    </section>
  );
}

export function Ssps(props: { data: Overview | undefined; refresh: () => void }) {
  const ready = () => props.data?.ssps.filter((s) => s.status === 'ready').length ?? 0;
  return (
    <>
      <PageHead
        title="Sync processors"
        subtitle={
          <Show when={props.data?.ssps.length}>
            {(n) => `${ready()} of ${n()} ready`}
          </Show>
        }
        actions={
          <Show when={props.data}>
            {(d) => <AllSspsActions count={d().ssps.length} onDone={props.refresh} />}
          </Show>
        }
      />

      <div class="page-body page-stack">
        <ActivityStrip operations={props.data?.operations} ssps={props.data?.ssps} />

        <Show when={props.data} fallback={<div class="ssp-grid"><div class="ssp-card skeleton" /><div class="ssp-card skeleton" /></div>}>
          {(data) => (
            <Show
              when={data().ssps.length > 0}
              fallback={
                <Panel>
                  <Empty>No SSPs are registered, so no client gets live updates.</Empty>
                </Panel>
              }
            >
              <div class="ssp-grid">
                <For each={data().ssps}>
                  {(ssp) => <SspCard ssp={ssp} data={data()} refresh={props.refresh} />}
                </For>
              </div>
            </Show>
          )}
        </Show>
      </div>
    </>
  );
}
