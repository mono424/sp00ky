import { For, Show, createResource, onCleanup } from 'solid-js';
import { api } from '../api/client';
import type { CloudService, CloudStats } from '../api/types';
import {
  Bento,
  Empty,
  PageHead,
  Pill,
  Readout,
  Segments,
  SkeletonBento,
  StatusDot,
  Tile,
} from '../components/Chrome';
import { Sparkline, type Point } from '../components/Sparkline';
import { formatBytes, relativeStamp, splitValue } from '../lib/format';
import { containerTone, fillTone } from '../lib/status';

/**
 * Resources: what the deployment is running on, as Sp00ky Cloud measures it.
 *
 * Only a cloud-linked scheduler has this page (the nav hides it otherwise):
 * the scheduler cannot see its neighbours' containers or the database's
 * volume, so all of it comes from the control plane through
 * `GET /admin/api/cloud/stats`. CPU and memory are the collector's 15 s
 * samples; the bucket volume is measured every 5 minutes.
 */

const POLL_MS = 15_000;
/** The collector's sample interval, used to lay series out on a time axis. */
const SAMPLE_MS = 15_000;

function series(values: number[], now: number): Point[] {
  return values.map((v, i) => ({ ts: now - (values.length - 1 - i) * SAMPLE_MS, ms: v, ok: true }));
}

/** Sum several series aligned on their newest point. */
function sumSeries(all: number[][]): number[] {
  const len = Math.max(0, ...all.map((s) => s.length));
  const out = new Array<number>(len).fill(0);
  for (const s of all) {
    const offset = len - s.length;
    s.forEach((v, i) => {
      out[offset + i]! += v;
    });
  }
  return out;
}

const pct = (v: number | null | undefined) =>
  v === null || v === undefined || !Number.isFinite(v) ? '—' : `${v < 10 ? v.toFixed(1) : Math.round(v)}%`;
const cores = (v: number | null | undefined) =>
  v === null || v === undefined || !Number.isFinite(v) ? '—' : (v / 100).toFixed(2);
const rate = (v: number | null | undefined) => (v === null || v === undefined ? '—' : `${formatBytes(v)}/s`);

function roleLabel(s: CloudService): string {
  if (s.machine) return `forwarder → ${s.machine}`;
  return s.role;
}

export function Resources() {
  const [result, { refetch }] = createResource(() => api.getResult<CloudStats>('/cloud/stats'));
  const timer = setInterval(() => void refetch(), POLL_MS);
  onCleanup(() => clearInterval(timer));

  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };
  const now = () => (data() ? Date.parse(data()!.server_time) : Date.now());

  const running = () => (data()?.services ?? []).filter((s) => s.status === 'running');
  const cpuNow = () => running().reduce((a, s) => a + (s.sample?.cpu_pct ?? 0), 0);
  const cpuAlloc = () => running().reduce((a, s) => a + s.vcpus, 0);
  const memNow = () => running().reduce((a, s) => a + (s.sample?.mem_bytes ?? 0), 0);
  const memAlloc = () => running().reduce((a, s) => a + s.memory_mb * 1024 * 1024, 0);
  const cpuSeries = () => sumSeries(running().map((s) => s.cpu_series));
  const memSeries = () => sumSeries(running().map((s) => s.mem_series));
  const machineCount = () => (data()?.machines.length ?? 0) + (data()?.pool_machines.length ?? 0);

  const volume = () => data()?.bucket_volume ?? null;
  const usedFraction = () => {
    const u = volume()?.usage;
    return u && u.fs_bytes > 0 ? u.used_bytes / u.fs_bytes : null;
  };

  return (
    <>
      <PageHead
        crumb="Sp00ky Cloud"
        title="Resources"
        subtitle={
          <Show when={data()?.deployment} fallback="Services, machines and storage of this deployment">
            {(d) => (
              <>
                deployment v{d().version} · {d().status}
                <Show when={d().deployed_at}> · deployed {relativeStamp(d().deployed_at)}</Show>
              </>
            )}
          </Show>
        }
      />

      <div class="page-body">
        <Show when={error()}>
          {(message) => (
            <Tile label="Sp00ky Cloud" tone="bad">
              <Empty>{message()}</Empty>
            </Tile>
          )}
        </Show>

        <Show
          when={data()}
          fallback={
            <Show when={!error()}>
              <SkeletonBento shape={[{ span: 6, rows: 2 }, { span: 3 }, { span: 3 }, { span: 3 }, { span: 3 }, { span: 12 }]} />
            </Show>
          }
        >
          {(d) => (
            <Bento>
              {/* ---- Bucket storage ---- */}
              <Tile
                i={0}
                span={6}
                rows={2}
                hero
                label="Bucket storage"
                tone={volume()?.usage ? fillTone(usedFraction()) : undefined}
                sub={
                  <Show when={volume()} fallback="deployment.storage is not set">
                    {(v) => (
                      <>
                        {v().size_gb} GB declared
                        <Show when={v().provisioned_gb !== v().size_gb}> · {v().provisioned_gb} GB provisioned</Show>
                        <Show when={v().usage}>{(u) => <> · measured {relativeStamp(u().measured_at)}</>}</Show>
                      </>
                    )}
                  </Show>
                }
              >
                <Show
                  when={volume()}
                  fallback={
                    <Empty>
                      No bucket volume. Set <span class="dim">deployment.storage.sizeGB</span> in sp00ky.yml
                      to give <span class="dim">file:</span> buckets a disk of their own.
                    </Empty>
                  }
                >
                  {(v) => (
                    <Show
                      when={v().usage}
                      fallback={<Empty>Waiting for the first measurement (every 5 minutes)…</Empty>}
                    >
                      {(u) => {
                        const free = splitValue(formatBytes(u().free_bytes).replace(' ', ''));
                        return (
                          <>
                            <Readout value={free.value} unit={`${free.unit} free`} />
                            <div class="tile-foot">
                              {formatBytes(u().used_bytes)} used of {formatBytes(u().fs_bytes)} ·{' '}
                              {pct((usedFraction() ?? 0) * 100)} full
                            </div>
                            <div class="tile-end" style={{ 'padding-top': '18px' }}>
                              <div class="bar" style={{ height: '10px', 'border-radius': '5px' }}>
                                <div
                                  class="bar-fill"
                                  style={{
                                    width: `${Math.min(100, Math.max(0.5, (usedFraction() ?? 0) * 100))}%`,
                                    background:
                                      fillTone(usedFraction()) === 'ok' ? 'var(--accent)' : `var(--${fillTone(usedFraction())})`,
                                  }}
                                />
                              </div>
                              <div class="row tile-foot" style={{ 'justify-content': 'space-between', 'margin-top': '8px' }}>
                                <span>0</span>
                                <span>{formatBytes(u().fs_bytes)}</span>
                              </div>
                            </div>
                          </>
                        );
                      }}
                    </Show>
                  )}
                </Show>
              </Tile>

              {/* ---- CPU ---- */}
              <Tile i={1} span={3} label="CPU" sub={`${cpuAlloc()} vCPU allocated`}>
                <Show when={d().metrics_available} fallback={<Empty>No metrics from Sp00ky Cloud.</Empty>}>
                  <Readout value={cores(cpuNow())} unit="cores" />
                  <div class="tile-plot tile-end">
                    <Sparkline points={series(cpuSeries(), now())} fill bare format={pct} ariaLabel="CPU, last 15 minutes" />
                  </div>
                </Show>
              </Tile>

              {/* ---- Memory ---- */}
              <Tile i={2} span={3} label="Memory" sub={`${formatBytes(memAlloc())} allocated`}>
                <Show when={d().metrics_available} fallback={<Empty>No metrics from Sp00ky Cloud.</Empty>}>
                  {(() => {
                    const m = splitValue(formatBytes(memNow()).replace(' ', ''));
                    return <Readout value={m.value} unit={m.unit} />;
                  })()}
                  <div class="tile-plot tile-end">
                    <Sparkline points={series(memSeries(), now())} fill bare format={formatBytes} ariaLabel="Memory, last 15 minutes" />
                  </div>
                </Show>
              </Tile>

              {/* ---- Services ---- */}
              <Tile i={3} span={3} label="Services" sub="containers of this deployment">
                <Readout value={`${running().length}/${d().services.length}`} unit="running" />
                <div class="tile-end">
                  <Segments
                    items={d().services.map((s) => ({ id: s.name, tone: containerTone(s.status), title: `${s.name}: ${s.status}` }))}
                  />
                </div>
              </Tile>

              {/* ---- Machines ---- */}
              <Tile i={4} span={3} label="Machines" sub="dedicated and pool VMs">
                <Readout value={String(machineCount())} unit={machineCount() === 1 ? 'machine' : 'machines'} />
                <div class="tile-end">
                  <Segments
                    items={[
                      ...d().machines.map((m) => ({ id: m.name, tone: containerTone(m.status), title: `${m.name}: ${m.phase || m.status}` })),
                      ...d().pool_machines.map((m) => ({ id: m.machine_id, tone: containerTone(m.status), title: `${m.pool}: ${m.status}` })),
                    ]}
                  />
                </div>
              </Tile>

              {/* ---- Services table ---- */}
              <Tile i={5} span={12} flush label="Services" sub="CPU and memory over the last 15 minutes">
                <Show when={d().services.length > 0} fallback={<Empty>No containers are running.</Empty>}>
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Status</th>
                          <th>CPU</th>
                          <th>Memory</th>
                          <th>Network in / out</th>
                          <th>Allocated</th>
                          <th>Since</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={d().services}>
                          {(s) => {
                            const memFrac = () =>
                              s.sample && s.memory_mb > 0 ? s.sample.mem_bytes / (s.memory_mb * 1024 * 1024) : null;
                            return (
                              <tr>
                                <td>
                                  <div class="row">
                                    <StatusDot tone={containerTone(s.status)} />
                                    <div style={{ 'min-width': '0' }}>
                                      <div>{s.name}</div>
                                      <div class="ghost truncate" style={{ 'font-size': '11.5px' }}>
                                        {roleLabel(s)}
                                        <Show when={s.image}> · {s.image}</Show>
                                      </div>
                                    </div>
                                  </div>
                                </td>
                                <td data-label="Status">
                                  <Pill tone={containerTone(s.status)}>{s.status}</Pill>
                                </td>
                                <td data-label="CPU" style={{ 'min-width': '150px' }}>
                                  <Show when={s.sample} fallback={<span class="ghost">no sample</span>}>
                                    <div class="row" style={{ gap: '10px' }}>
                                      <span style={{ 'min-width': '44px' }}>{pct(s.sample!.cpu_pct)}</span>
                                      <div style={{ width: '90px', height: '22px' }}>
                                        <Sparkline points={series(s.cpu_series, now())} bare height={22} format={pct} ariaLabel={`${s.name} CPU`} />
                                      </div>
                                    </div>
                                  </Show>
                                </td>
                                <td data-label="Memory" style={{ 'min-width': '170px' }}>
                                  <Show when={s.sample} fallback={<span class="ghost">no sample</span>}>
                                    <div>
                                      {formatBytes(s.sample!.mem_bytes)}
                                      <span class="ghost"> / {formatBytes(s.memory_mb * 1024 * 1024)}</span>
                                    </div>
                                    <div class="bar" style={{ 'margin-top': '5px' }}>
                                      <div
                                        class="bar-fill"
                                        style={{
                                          width: `${Math.min(100, (memFrac() ?? 0) * 100)}%`,
                                          background:
                                            fillTone(memFrac()) === 'ok' ? 'var(--accent)' : `var(--${fillTone(memFrac())})`,
                                        }}
                                      />
                                    </div>
                                  </Show>
                                </td>
                                <td class="dim" data-label="Network">
                                  <Show when={s.sample} fallback="—">
                                    {rate(s.sample!.net_rx_bps)} / {rate(s.sample!.net_tx_bps)}
                                  </Show>
                                </td>
                                <td class="dim" data-label="Allocated">
                                  {s.vcpus} vCPU · {formatBytes(s.memory_mb * 1024 * 1024)}
                                </td>
                                <td class="dim" data-label="Since">{relativeStamp(s.since)}</td>
                              </tr>
                            );
                          }}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </Show>
              </Tile>

              {/* ---- Machines table ---- */}
              <Show when={machineCount() > 0}>
                <Tile i={6} span={12} flush label="Machines" sub="VMs of their own, outside the shared host">
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Kind</th>
                          <th>Status</th>
                          <th>Type</th>
                          <th>Location</th>
                          <th>Address</th>
                          <th>Since</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={d().machines}>
                          {(m) => (
                            <tr>
                              <td>
                                <div class="row">
                                  <StatusDot tone={containerTone(m.status)} />
                                  {m.name}
                                  <span class="ghost">gen {m.generation}</span>
                                </div>
                              </td>
                              <td class="dim" data-label="Kind">dedicated · {m.backend || '—'}</td>
                              <td data-label="Status">
                                <Pill tone={containerTone(m.status)}>{m.phase || m.status}</Pill>
                              </td>
                              <td class="dim" data-label="Type">{m.server_type}</td>
                              <td class="dim" data-label="Location">{m.location || '—'}</td>
                              <td class="dim" data-label="Address">{m.ip || '—'}</td>
                              <td class="dim" data-label="Since">{relativeStamp(m.created_at)}</td>
                            </tr>
                          )}
                        </For>
                        <For each={d().pool_machines}>
                          {(m) => (
                            <tr>
                              <td>
                                <div class="row">
                                  <StatusDot tone={containerTone(m.status)} />
                                  <span class="truncate">{m.machine_id}</span>
                                </div>
                              </td>
                              <td class="dim" data-label="Kind">pool · {m.pool}</td>
                              <td data-label="Status">
                                <Pill tone={containerTone(m.status)}>{m.status}</Pill>
                              </td>
                              <td class="dim" data-label="Type">{m.server_type}</td>
                              <td class="dim" data-label="Location">{m.location || '—'}</td>
                              <td class="dim" data-label="Address">—</td>
                              <td class="dim" data-label="Since">{relativeStamp(m.created_at)}</td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </Tile>
              </Show>
            </Bento>
          )}
        </Show>
      </div>
    </>
  );
}
