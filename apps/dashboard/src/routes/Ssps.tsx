import { For, Index, Show, createMemo, createSignal } from 'solid-js';
import { A } from '@solidjs/router';
import { PageHead } from '../components/Chrome';
import { BootstrapProgress } from '../components/BootstrapProgress';
import { OpBadge } from '../components/Actions';
import { ActivityStrip } from '../components/OpCard';
import { AllSspsActions, SspActions } from '../components/ClusterActions';
import { DayTicks, FxCell, Gauge, Sheet, SheetSection, Sq, memText, useCloudServices } from '../components/Frame';
import { formatBytes, formatCount, formatDuration, formatUptime } from '../lib/format';
import { sspTone } from '../lib/status';
import type { Overview, SspEntity } from '../api/types';

/**
 * The sync processors, for any number of them.
 *
 * First the fleet as a whole: how many are ready, how the live views are
 * spread across them, what is waiting to publish, and the stalest heartbeat.
 * Then one full-width row per SSP, trouble first: who it is and how it has
 * been (left), then its CPU and memory, its share of the views and its
 * publication backlog. A bootstrapping SSP shows its progress in place of the
 * last two. Environment and internals sit behind each row's details toggle.
 */

const ICON = 'M4 3h16v7H4z M4 14h16v7H4z M7 6.5h1 M7 17.5h1';

/** Shades of the accent, one per SSP, for the views share. */
const SHADES = ['#c6cbea', '#9ca5d3', '#7f88bd', '#b7bede', '#6f78a8', '#a8adc4', '#8e96b8', '#5f68ad'];

/** Seconds as "2s", "4m", "1h". */
const short = (secs: number) => (secs < 60 ? `${Math.round(secs)}s` : formatUptime(secs));

export function Ssps(props: { data: Overview | undefined; refresh: () => void }) {
  const cloud = useCloudServices(() => props.data?.cloud_linked);
  const now = () => props.data?.server_time_ms ?? Date.now();
  const ssps = createMemo(() => {
    const rank = (s: SspEntity) =>
      s.status !== 'ready' ? 0 : (s.publication?.pending_operations ?? 0) > 0 || (s.publication?.parked_batches ?? 0) > 0 ? 1 : 2;
    return [...(props.data?.ssps ?? [])].sort((a, b) => rank(a) - rank(b) || a.id.localeCompare(b.id));
  });
  const byId = createMemo(() => [...(props.data?.ssps ?? [])].sort((a, b) => a.id.localeCompare(b.id)));
  const shade = (id: string) => SHADES[Math.max(0, byId().findIndex((s) => s.id === id)) % SHADES.length];

  const ready = () => ssps().filter((s) => s.status === 'ready').length;
  const totalViews = () => ssps().reduce((n, s) => n + s.views, 0);
  const pending = () => ssps().reduce((n, s) => n + (s.publication?.pending_operations ?? 0), 0);
  const oldestWait = () => Math.max(0, ...ssps().map((s) => s.publication?.oldest_age_ms ?? 0));
  const stalest = () => (ssps().length ? Math.max(...ssps().map((s) => s.last_heartbeat_seconds_ago)) : null);
  /** The SSP whose heartbeat is overdue, if any: past 30 s nothing has been heard. */
  const late = () => ssps().find((s) => s.last_heartbeat_seconds_ago > 30 && s.last_heartbeat_seconds_ago === stalest());

  return (
    <>
      <PageHead
        title="Sync processors"
        actions={
          <>
            <Show when={props.data}>
              <span class="cap">
                <b style={{ color: ready() === ssps().length ? 'var(--ink-dim)' : 'var(--warn)' }}>{ready()}</b> of {ssps().length} ready
              </span>
            </Show>
            <Show when={props.data}>{(d) => <AllSspsActions count={d().ssps.length} onDone={props.refresh} />}</Show>
          </>
        }
      />

      <Show when={props.data} fallback={<div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div>}>
        {(d) => (
          <div class="fx-page">
            <Show when={(d().operations ?? []).some((o) => o.status === 'running')}>
              <section class="fx-row">
                <FxCell label="Running now">
                  <ActivityStrip operations={d().operations} ssps={d().ssps} />
                </FxCell>
              </section>
            </Show>

            <Show
              when={ssps().length > 0}
              fallback={
                <section class="fx-row">
                  <FxCell>
                    <div class="fx-log bad">
                      <span class="fx-what">No sync processor is registered · no client receives live updates</span>
                    </div>
                  </FxCell>
                </section>
              }
            >
              {/* the fleet ====================================================== */}
              <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
                <FxCell label="Ready">
                  <div class="fx-big">
                    <span classList={{ warn: ready() < ssps().length }}>{ready()}</span>
                    <small>/ {ssps().length}</small>
                  </div>
                  <div class="sp-squares">
                    <For each={byId()}>
                      {(s) => <span class={`sp-sq ${sspTone(s.status) === 'ok' ? '' : sspTone(s.status)}`} title={`${s.id} · ${s.status}`} />}
                    </For>
                  </div>
                </FxCell>
                <FxCell label="Live views">
                  <div class="fx-big">{formatCount(totalViews())}</div>
                  <div class="sp-share" title="Each SSP's share of the live views">
                    <For each={byId()}>
                      {(s) => <i style={{ 'flex-grow': s.views || 0.0001, background: shade(s.id) }} title={`${s.id}: ${formatCount(s.views)}`} />}
                    </For>
                  </div>
                  <div class="sp-legend cap">
                    <For each={byId()}>
                      {(s) => (
                        <span>
                          <i style={{ background: shade(s.id) }} />
                          {s.id} <b>{formatCount(s.views)}</b>
                        </span>
                      )}
                    </For>
                  </div>
                </FxCell>
                <FxCell label="Backlog">
                  <div class="fx-big">
                    <span classList={{ warn: pending() > 0 }}>{formatCount(pending())}</span>
                    <small>ops</small>
                  </div>
                  <div class="fx-sub cap">
                    <span>
                      oldest <b classList={{ warn: oldestWait() >= 5_000 }}>{oldestWait() ? formatDuration(oldestWait()) : '—'}</b>
                    </span>
                  </div>
                </FxCell>
                <FxCell label="Heartbeats">
                  <div class="fx-big">
                    <span classList={{ warn: (stalest() ?? 0) > 30 }}>{stalest() == null ? '—' : short(stalest()!)}</span>
                    <small>stalest</small>
                  </div>
                  <div class="fx-sub cap">
                    <Show when={late()} fallback={<span>all within <b>{stalest() == null ? '—' : short(stalest()!)}</b></span>}>
                      {(l) => <span class="warn">{l().id} last seen {short(l().last_heartbeat_seconds_ago)} ago</span>}
                    </Show>
                  </div>
                </FxCell>
              </section>

              {/* one row per SSP ================================================ */}
              {/* Keyed by id: each poll brings new objects, and a row rebuilt
                  for every one of them would close its open details. */}
              <For each={ssps().map((s) => s.id)}>
                {(id) => (
                  <SspRow
                    ssp={ssps().find((s) => s.id === id)!}
                    data={d()}
                    share={totalViews() ? ((ssps().find((s) => s.id === id)?.views ?? 0) / totalViews()) * 100 : 0}
                    shade={shade(id)}
                    use={cloud.byName(id)}
                    now={now()}
                    refresh={props.refresh}
                  />
                )}
              </For>
            </Show>
          </div>
        )}
      </Show>
    </>
  );
}

function SspRow(props: {
  ssp: SspEntity;
  data: Overview;
  share: number;
  shade: string;
  use: ReturnType<ReturnType<typeof useCloudServices>['byName']>;
  now: number;
  refresh: () => void;
}) {
  const [open, setOpen] = createSignal(false);
  const s = () => props.ssp;
  const ready = () => s().status === 'ready';
  const pub = () => s().publication;
  const backlog = () => pub()?.pending_operations ?? 0;
  const memHigh = () => (props.use?.memPct ?? 0) >= 80;
  const tone = () => (!ready() ? sspTone(s().status) : backlog() > 0 || memHigh() || (pub()?.parked_batches ?? 0) > 0 ? 'warn' : '');
  const mem = () => (props.use?.memBytes != null ? memText(props.use.memBytes) : null);
  const limit = () => (props.use ? memText(props.use.memLimit) : null);
  const env = () => Object.entries(s().env ?? {}).sort((a, b) => a[0].localeCompare(b[0]));

  return (
    <>
      <section class="fx-row sp-row" classList={{ warn: tone() === 'warn', bad: tone() === 'bad' }}>
        {/* who it is and how it has been */}
        <div class="fx-cell sp-id">
          <div class="fx-head fx-title" style={{ 'margin-bottom': '10px' }}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true" style={{ color: 'var(--accent)' }}>
              <path d={ICON} />
            </svg>
            <span class="fx-name">{s().id}</span>
            <span class="cap" style={{ color: 'var(--ink-ghost)' }}>{s().version.replace(/^0\.0\.1-/, '')}</span>
          </div>
          <div class="fx-state cap" classList={{ warn: tone() === 'warn', bad: tone() === 'bad' }}>
            <Sq tone={tone()} />
            {ready() && memHigh() ? 'memory high' : ready() && backlog() > 0 ? 'publishing behind' : s().status}
          </div>
          <OpBadge operations={props.data.operations} target={s().id} />
          <dl class="sp-facts">
            <div><dt class="cap">Address</dt><dd>{s().ip ?? '—'}</dd></div>
            <div><dt class="cap">Up</dt><dd>{formatUptime(s().uptime_seconds)}</dd></div>
            <div>
              <dt class="cap">Heartbeat</dt>
              <dd classList={{ warn: s().last_heartbeat_seconds_ago > 30 }}>{short(s().last_heartbeat_seconds_ago)} ago</dd>
            </div>
            <Show when={props.use?.diskBytes != null}>
              <div title={`circuit snapshot and row arena · ${formatBytes(props.use!.diskFree)} free on host`}>
                <dt class="cap">Disk</dt>
                <dd classList={{ warn: (props.use!.diskPct ?? 0) >= 80 }}>{formatBytes(props.use!.diskBytes)}</dd>
              </div>
            </Show>
          </dl>
          <DayTicks component={s().id} incidents={props.data.incidents?.recent} uptimeSecs={s().uptime_seconds} now={props.now} />
          <div class="sp-actions">
            <SspActions ssp={s()} size="sm" onDone={props.refresh} />
            <A class="btn btn-sm" href={`/logs?source=ssp:${encodeURIComponent(s().id)}`}>Logs</A>
            <button class="btn btn-sm" aria-haspopup="dialog" onClick={() => setOpen(true)}>
              Details
            </button>
          </div>
        </div>

        {/* what it costs */}
        <div class="fx-cell sp-gauge">
          <Gauge
            label="CPU"
            of={props.use ? `${props.use.vcpus} vCPU` : undefined}
            value={props.use?.cpuPct != null ? String(Math.round(props.use.cpuPct)) : null}
            unit="%"
            pct={props.use?.cpuPct ?? null}
            series={props.use?.cpuSeries}
          />
        </div>
        <div class="fx-cell sp-gauge">
          <Gauge
            label="Memory"
            of={limit() ? `${limit()!.value} ${limit()!.unit}` : undefined}
            value={mem()?.value ?? null}
            unit={mem()?.unit}
            pct={props.use?.memPct ?? null}
            series={props.use?.memSeries}
          />
        </div>

        {/* what it does: views and publication, or its bootstrap */}
        <Show
          when={ready()}
          fallback={
            <div class="fx-cell sp-boot" style={{ 'grid-column': 'span 2' }}>
              <div class="fx-head"><span class="cap">{s().status === 'replaying' ? 'Replaying' : 'Bootstrapping'}</span></div>
              <BootstrapProgress ssp={s()} timeoutSecs={props.data.bootstrap_timeout_secs} />
              <div class="fx-sub cap" style={{ 'margin-top': '12px' }}>
                <span>clients on this SSP wait until it is ready</span>
              </div>
            </div>
          }
        >
          <div class="fx-cell">
            <div class="fx-head"><span class="cap">Live views</span><span class="fx-meta cap"><span>{Math.round(props.share)}% of fleet</span></span></div>
            <div class="fx-big">{formatCount(s().views)}</div>
            <div class="sp-sharebar">
              <i style={{ width: `${props.share}%`, background: props.shade }} />
            </div>
          </div>
          <div class="fx-cell">
            <div class="fx-head">
              <span class="cap">Backlog</span>
              <Show when={(pub()?.parked_batches ?? 0) > 0}>
                <span class="fx-meta cap"><span class="warn">{formatCount(pub()!.parked_batches)} parked</span></span>
              </Show>
            </div>
            <div class="fx-big">
              <span classList={{ warn: backlog() > 0 }}>{formatCount(backlog())}</span>
              <small>ops</small>
            </div>
            <div class="fx-sub cap">
              <span>
                <b>{formatBytes(pub()?.pending_bytes ?? 0)}</b>
              </span>
              <span>
                oldest <b classList={{ warn: (pub()?.oldest_age_ms ?? 0) >= 5_000 }}>{pub()?.oldest_age_ms ? formatDuration(pub()!.oldest_age_ms) : '—'}</b>
              </span>
            </div>
          </div>
        </Show>
      </section>

      {/* internals, on request, in a sheet beside the list */}
      <Sheet
        open={open()}
        onClose={() => setOpen(false)}
        title={
          <>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true" style={{ color: 'var(--accent)' }}>
              <path d={ICON} />
            </svg>
            {s().id}
          </>
        }
        meta={<>{s().ip ?? 'no address'} · {s().version.replace(/^0\.0\.1-/, '')}</>}
      >
        <SheetSection label="State" meta={<span classList={{ warn: tone() === 'warn', bad: tone() === 'bad' }}>{s().status}</span>}>
          <div class="sp-kv">
            <div><span class="cap">In this phase</span><b>{formatUptime(s().state_seconds)}</b></div>
            <div><span class="cap">Up</span><b>{formatUptime(s().uptime_seconds)}</b></div>
            <div><span class="cap">Heartbeat</span><b>{short(s().last_heartbeat_seconds_ago)} ago</b></div>
            <div><span class="cap">Buffered events</span><b>{formatCount(s().buffered_events)}</b></div>
          </div>
        </SheetSection>
        <SheetSection label="Publication">
          <div class="sp-kv">
            <div><span class="cap">Pending</span><b classList={{ warn: backlog() > 0 }}>{formatCount(backlog())} <em>ops</em></b></div>
            <div><span class="cap">Pending bytes</span><b>{formatBytes(pub()?.pending_bytes ?? 0)}</b></div>
            <div><span class="cap">Oldest wait</span><b>{pub()?.oldest_age_ms ? formatDuration(pub()!.oldest_age_ms) : '—'}</b></div>
            <div><span class="cap">Parked batches</span><b classList={{ warn: (pub()?.parked_batches ?? 0) > 0 }}>{formatCount(pub()?.parked_batches ?? 0)}</b></div>
            <div><span class="cap">Overload rejects</span><b>{formatCount(pub()?.overload_total ?? 0)}</b></div>
            <div><span class="cap">Last published</span><b>{pub()?.last_success_at_ms ? new Date(pub()!.last_success_at_ms!).toLocaleTimeString() : '—'}</b></div>
            <Show when={pub()?.connection}>
              {(c) => (
                <>
                  <div><span class="cap">Session generation</span><b>{formatCount(c().generation)}</b></div>
                  <div><span class="cap">Failed reconnects</span><b classList={{ warn: c().reconnect_failures > 0 }}>{formatCount(c().reconnect_failures)}</b></div>
                </>
              )}
            </Show>
          </div>
        </SheetSection>
        <Show when={(pub()?.worst_views.length ?? 0) > 0}>
          <SheetSection label="Slowest to publish">
            <table class="fx-list">
              <thead><tr><th class="cap">View</th><th class="cap r">Ops</th><th class="cap r">Bytes</th><th class="cap r">Oldest</th></tr></thead>
              <tbody>
                <For each={pub()!.worst_views}>
                  {(v) => (
                    <tr>
                      <td><A href={`/views/${encodeURIComponent(v.query_id)}`} style={{ color: 'var(--ink)' }}>{v.query_id}</A></td>
                      <td class="r">{formatCount(v.pending_operations)}</td>
                      <td class="r">{formatBytes(v.pending_bytes)}</td>
                      <td class="r">{formatDuration(v.oldest_age_ms)}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </SheetSection>
        </Show>
        <Show when={env().length}>
          <SheetSection label="Environment" meta={<span>{env().length}</span>}>
            <table class="fx-list">
              <tbody>
                <Index each={env()}>
                  {(kv) => (
                    <tr>
                      <td style={{ width: '42%' }}>{kv()[0]}</td>
                      <td style={{ color: 'var(--ink)' }}>{kv()[1]}</td>
                    </tr>
                  )}
                </Index>
              </tbody>
            </table>
          </SheetSection>
        </Show>
      </Sheet>
    </>
  );
}

