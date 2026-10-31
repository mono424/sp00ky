import { For, Show, createMemo, createResource, onCleanup } from 'solid-js';
import { api } from '../api/client';
import type { CloudService, CloudStats } from '../api/types';
import { PageHead } from '../components/Chrome';
import { FxCell, LineChart, Meter, Seg, Sq } from '../components/Frame';
import { formatBytes, formatUptime, relativeStamp } from '../lib/format';
import { containerTone } from '../lib/status';

/**
 * Resources: what the deployment runs on, as Sp00ky Cloud measures it.
 *
 * Top to bottom: the deployment's CPU, memory and bucket disk against what is
 * allocated, over the last fifteen minutes; how that CPU and memory splits
 * across the containers; every container, closest to a limit first; and the
 * machines behind dedicated and pooled backends.
 *
 * Only a cloud-linked scheduler has this page (the nav hides it otherwise):
 * the scheduler cannot see its neighbours' containers or the database's
 * volume, so all of it comes from `GET /admin/api/cloud/stats`. CPU and
 * memory are the collector's 15 s samples; the bucket volume is measured
 * every 5 minutes.
 */

const POLL_MS = 15_000;
/** The collector's sample interval, used to lay series out on a time axis. */
const SAMPLE_MS = 15_000;

/** Shades of the accent, one per container in the split bars. */
const SHADES = ['#c6cbea', '#9ca5d3', '#7f88bd', '#b7bede', '#6f78a8', '#a8adc4', '#8e96b8', '#5f68ad', '#4a5396'];

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

const memLimit = (s: CloudService) => s.sample?.mem_limit_bytes || s.memory_mb * 1024 * 1024;
/** CPU as a share of the container's vCPUs; `cpu_pct` is per core. */
const cpuPct = (s: CloudService) => (s.sample && s.vcpus > 0 ? s.sample.cpu_pct / s.vcpus : null);
const memPct = (s: CloudService) => (s.sample ? (s.sample.mem_bytes / memLimit(s)) * 100 : null);
/** Its volumes against the room they have: what they take plus what the host disk has free. */
const volumesPct = (s: CloudService) => {
  const room = s.disk ? s.disk.used_bytes + s.disk.free_bytes : 0;
  return s.disk && room > 0 ? (s.disk.used_bytes / room) * 100 : null;
};

/** How close a container is to any limit; a stopped one sorts first. */
function pressure(s: CloudService): number {
  if (s.status !== 'running') return 1000;
  return Math.max(cpuPct(s) ?? 0, memPct(s) ?? 0);
}

function toneOf(s: CloudService): string {
  if (s.status !== 'running') {
    const t = containerTone(s.status);
    return t === 'ok' ? '' : t;
  }
  const p = pressure(s);
  return p >= 90 ? 'bad' : p >= 80 ? 'warn' : '';
}

const pct = (v: number | null | undefined) =>
  v == null || !Number.isFinite(v) ? '—' : `${v < 10 ? v.toFixed(1) : Math.round(v)}%`;
const rate = (bps: number | null | undefined) => (bps == null ? '—' : `${formatBytes(bps)}/s`);
const cores = (v: number) => (v < 10 ? v.toFixed(2) : v.toFixed(1));

export function Resources() {
  const [result, { refetch }] = createResource(() => api.getResult<CloudStats>('/cloud/stats'));
  const timer = setInterval(() => !document.hidden && void refetch(), POLL_MS);
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
  const metrics = () => !!data()?.metrics_available;

  const running = () => (data()?.services ?? []).filter((s) => s.status === 'running');
  const cpuNow = () => running().reduce((a, s) => a + (s.sample?.cpu_pct ?? 0), 0) / 100;
  const cpuAlloc = () => running().reduce((a, s) => a + s.vcpus, 0);
  const memNow = () => running().reduce((a, s) => a + (s.sample?.mem_bytes ?? 0), 0);
  const memAlloc = () => running().reduce((a, s) => a + memLimit(s), 0);

  const points = (values: number[]) =>
    values.map((v, i) => ({ ts: now() - (values.length - 1 - i) * SAMPLE_MS, v }));
  const cpuPoints = () => points(sumSeries(running().map((s) => s.cpu_series)).map((v) => v / 100));
  const memPoints = () => points(sumSeries(running().map((s) => s.mem_series)));

  const services = createMemo(() =>
    [...(data()?.services ?? [])].sort((a, b) => pressure(b) - pressure(a) || a.name.localeCompare(b.name)),
  );
  /** Stable shades: by name, not by the order pressure puts them in. */
  const shade = (name: string) => {
    const names = [...(data()?.services ?? [])].map((s) => s.name).sort();
    return SHADES[Math.max(0, names.indexOf(name)) % SHADES.length];
  };

  const volume = () => data()?.bucket_volume ?? null;
  const diskPct = () => {
    const u = volume()?.usage;
    return u && u.fs_bytes > 0 ? (u.used_bytes / u.fs_bytes) * 100 : null;
  };

  const machines = () => [
    ...(data()?.machines ?? []).map((m) => ({
      name: m.name,
      kind: 'dedicated',
      runs: m.backend ?? '—',
      type: m.server_type,
      where: m.location ?? '—',
      ip: m.ip ?? '—',
      status: m.phase || m.status,
      tone: m.last_error ? 'bad' : containerTone(m.status) === 'ok' ? '' : containerTone(m.status),
      since: m.created_at,
      error: m.last_error,
    })),
    ...(data()?.pool_machines ?? []).map((m) => ({
      name: m.machine_id,
      kind: 'pool',
      runs: m.pool,
      type: m.server_type,
      where: m.location ?? '—',
      ip: '—',
      status: m.status,
      tone: containerTone(m.status) === 'ok' ? '' : containerTone(m.status),
      since: m.created_at,
      error: undefined as string | undefined,
    })),
  ];

  return (
    <>
      <PageHead
        title="Resources"
        actions={
          <Show when={data()?.deployment}>
            {(d) => (
              <span class="cap">
                deployment <b style={{ color: 'var(--ink-dim)' }}>v{d().version}</b>
                <Show when={d().deployed_at}> · deployed {relativeStamp(d().deployed_at)}</Show>
              </span>
            )}
          </Show>
        }
      />

      <Show when={error()}>
        {(message) => (
          <section class="fx-row">
            <FxCell>
              <div class="fx-log bad"><span class="fx-what">{message()}</span></div>
            </FxCell>
          </section>
        )}
      </Show>

      <Show when={data()} fallback={<Show when={!error()}><div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div></Show>}>
        {(d) => (
          <div class="fx-page">
            <Show when={!d().metrics_available}>
              <section class="fx-row">
                <FxCell>
                  <div class="fx-log"><span class="fx-what">Sp00ky Cloud has no metrics for this project · usage reads as not reported</span></div>
                </FxCell>
              </section>
            </Show>

            {/* the deployment as a whole, last 15 minutes ====================== */}
            <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1.4fr)) minmax(0, 1fr)' }}>
              <FxCell label="CPU" meta={<span>of <b>{cpuAlloc()}</b> vCPU · last 15 min</span>}>
                <div class="fx-big">
                  {metrics() ? cores(cpuNow()) : '—'}
                  <small>cores · {pct(cpuAlloc() ? (cpuNow() / cpuAlloc()) * 100 : null)}</small>
                </div>
                <Show when={metrics()}>
                  <LineChart points={cpuPoints()} format={(v) => `${cores(v)}`} now={now()} />
                </Show>
              </FxCell>
              <FxCell label="Memory" meta={<span>of <b>{formatBytes(memAlloc())}</b> · last 15 min</span>}>
                <div class="fx-big">
                  {metrics() ? formatBytes(memNow()) : '—'}
                  <small>{pct(memAlloc() ? (memNow() / memAlloc()) * 100 : null)}</small>
                </div>
                <Show when={metrics()}>
                  <LineChart points={memPoints()} format={(v) => formatBytes(v)} now={now()} />
                </Show>
              </FxCell>
              <FxCell label="Bucket disk" meta={<Show when={volume()?.usage}>{(u) => <span>measured {relativeStamp(u().measured_at)}</span>}</Show>}>
                <div class="fx-big">
                  <span classList={{ warn: (diskPct() ?? 0) >= 80 && (diskPct() ?? 0) < 90, bad: (diskPct() ?? 0) >= 90 }}>
                    {diskPct() != null ? pct(diskPct()) : '—'}
                  </span>
                  <Show when={diskPct() != null}><small>full</small></Show>
                </div>
                <Show
                  when={volume()?.usage}
                  fallback={<div class="fx-sub cap"><span>{volume() ? 'waiting for the first measurement' : 'no bucket volume configured'}</span></div>}
                >
                  {(u) => (
                    <>
                      <div style={{ 'margin-top': '16px' }}><Seg pct={diskPct()} wide /></div>
                      <div class="fx-foot rs-disk">
                        <div><span class="cap">Used</span><b>{formatBytes(u().used_bytes)}</b></div>
                        <div><span class="cap">Free</span><b>{formatBytes(u().free_bytes)}</b></div>
                        <div><span class="cap">Volume</span><b>{volume()!.provisioned_gb} <em>GB</em></b></div>
                      </div>
                    </>
                  )}
                </Show>
              </FxCell>
            </section>

            {/* who uses it ===================================================== */}
            <Show when={metrics() && running().length > 0}>
              <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1fr))' }}>
                <Split
                  label="CPU by container"
                  items={running().map((s) => ({ name: s.name, value: (s.sample?.cpu_pct ?? 0) / 100, shade: shade(s.name) }))}
                  format={(v) => `${cores(v)} cores`}
                />
                <Split
                  label="Memory by container"
                  items={running().map((s) => ({ name: s.name, value: s.sample?.mem_bytes ?? 0, shade: shade(s.name) }))}
                  format={(v) => formatBytes(v)}
                />
              </section>
            </Show>

            {/* every container ================================================== */}
            <section class="fx-row">
              <FxCell label="Containers" meta={<span><b>{running().length}</b> of {services().length} running · closest to a limit first</span>}>
                <Show when={services().length} fallback={<div class="fx-empty cap">No containers are running</div>}>
                  <table class="fx-list rs-containers">
                    <colgroup>
                      <col style={{ width: '13%' }} /><col style={{ width: '18%' }} /><col style={{ width: '18%' }} />
                      <col style={{ width: '19%' }} /><col style={{ width: '8%' }} /><col style={{ width: '17%' }} /><col style={{ width: '7%' }} />
                    </colgroup>
                    <thead>
                      <tr>
                        <th class="cap">Container</th><th class="cap">CPU</th><th class="cap">Memory</th>
                        <th class="cap" title="its volumes, against what they take plus what is still free on the host disk">Disk</th>
                        <th class="cap r">Disk writes</th><th class="cap r">Network in / out</th><th class="cap r">Up</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={services()}>
                        {(s) => (
                          <tr>
                            <td>
                              <span class="fx-who">
                                <Sq tone={toneOf(s)} />
                                <span style={{ color: 'var(--ink)' }}>{s.name}</span>
                              </span>
                              <span class="vw-meta cap">
                                <span>{s.machine ? `forwards to ${s.machine}` : s.role}</span>
                                <Show when={s.status !== 'running'}><span classList={{ warn: toneOf(s) === 'warn', bad: toneOf(s) === 'bad' }}>{s.status}</span></Show>
                              </span>
                            </td>
                            <td>
                              <Meter pct={s.status === 'running' && s.sample ? cpuPct(s) : null}>
                                {pct(cpuPct(s))} <em>/ {s.vcpus} vCPU</em>
                              </Meter>
                            </td>
                            <td>
                              <Meter pct={s.status === 'running' && s.sample ? memPct(s) : null}>
                                {s.sample ? formatBytes(s.sample.mem_bytes) : '—'} <em>/ {formatBytes(memLimit(s))}</em>
                              </Meter>
                            </td>
                            <td>
                              <Meter pct={volumesPct(s)}>
                                {formatBytes(s.disk?.used_bytes)} <em>/ {formatBytes(s.disk ? s.disk.used_bytes + s.disk.free_bytes : null)}</em>
                              </Meter>
                            </td>
                            <td class="r">{rate(s.sample?.disk_write_bps)}</td>
                            <td class="r">{rate(s.sample?.net_rx_bps)} <span style={{ color: 'var(--ink-ghost)' }}>/</span> {rate(s.sample?.net_tx_bps)}</td>
                            <td class="r">{s.since ? formatUptime((now() - Date.parse(s.since)) / 1000) : '—'}</td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </Show>
              </FxCell>
            </section>

            {/* the machines behind dedicated and pooled backends ================ */}
            <Show when={machines().length > 0}>
              <section class="fx-row">
                <FxCell label="Machines" meta={<span><b>{machines().length}</b></span>}>
                  <table class="fx-list">
                    <thead>
                      <tr>
                        <th class="cap">Machine</th><th class="cap">Kind</th><th class="cap">Runs</th><th class="cap">Type</th>
                        <th class="cap">Location</th><th class="cap">Address</th><th class="cap">State</th><th class="cap r">Up</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={machines()}>
                        {(m) => (
                          <tr>
                            <td><span class="fx-who"><Sq tone={m.tone} /><span style={{ color: 'var(--ink)' }}>{m.name}</span></span></td>
                            <td>{m.kind}</td>
                            <td>{m.runs}</td>
                            <td>{m.type}</td>
                            <td>{m.where}</td>
                            <td>{m.ip}</td>
                            <td classList={{ warn: m.tone === 'warn', bad: m.tone === 'bad' }} title={m.error}>{m.error ?? m.status}</td>
                            <td class="r">{formatUptime((now() - Date.parse(m.since)) / 1000)}</td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </FxCell>
              </section>
            </Show>
          </div>
        )}
      </Show>
    </>
  );
}

/** How a total splits across containers: one bar in shades, then the biggest few. */
function Split(props: { label: string; items: { name: string; value: number; shade: string }[]; format: (v: number) => string }) {
  const sorted = () => [...props.items].sort((a, b) => b.value - a.value);
  const total = () => props.items.reduce((n, i) => n + i.value, 0);
  return (
    <FxCell label={props.label} meta={<span><b>{props.format(total())}</b></span>}>
      <div class="sp-share">
        <For each={sorted()}>{(i) => <i style={{ 'flex-grow': i.value || 0.0001, background: i.shade }} title={`${i.name}: ${props.format(i.value)}`} />}</For>
      </div>
      <table class="fx-list vw-mini">
        <tbody>
          <For each={sorted().slice(0, 5)}>
            {(i) => (
              <tr>
                <td><span class="fx-who"><i class="vw-key" style={{ background: i.shade }} /><span style={{ color: 'var(--ink)' }}>{i.name}</span></span></td>
                <td class="r">{props.format(i.value)}</td>
                <td class="r" style={{ width: '16%' }}>{total() ? Math.round((i.value / total()) * 100) : 0}%</td>
              </tr>
            )}
          </For>
          <Show when={sorted().length > 5}>
            <tr>
              <td class="cap" style={{ color: 'var(--ink-ghost)' }}>{sorted().length - 5} more</td>
              <td class="r">{props.format(sorted().slice(5).reduce((n, i) => n + i.value, 0))}</td>
              <td class="r" style={{ width: '16%' }}>{total() ? Math.round((sorted().slice(5).reduce((n, i) => n + i.value, 0) / total()) * 100) : 0}%</td>
            </tr>
          </Show>
        </tbody>
      </table>
    </FxCell>
  );
}
