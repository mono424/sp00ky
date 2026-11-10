import { For, Show, createMemo, createResource, onCleanup, type JSX } from 'solid-js';
import { api } from '../api/client';
import type { CloudHost, CloudService, CloudStats } from '../api/types';
import { kindLabel } from '../console/hostPlan';
import { PageHead } from '../components/Chrome';
import { FxCell, LineChart, Meter, Seg, Sq } from '../components/Frame';
import { formatBytes, formatUptime, relativeStamp } from '../lib/format';
import { containerTone } from '../lib/status';

/**
 * Resources: what the deployment runs on, as Sp00ky Cloud measures it.
 *
 * A project on its own VMs reads one VM at a time, the database's first: the
 * machine's CPU, memory and disk against its real size, then what runs on it.
 * Container caps cannot say this: they are limits, and on one machine they overlap.
 *
 * A project on the shared host has no machine of its own to measure, so it reads
 * as a whole: CPU, memory and bucket disk against what is allocated, how that
 * splits across the containers, then every container.
 *
 * Either way the machines behind dedicated and pooled backends come last.
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

  const hosts = () => data()?.hosts ?? [];
  const onHost = (kind: string) => services().filter((s) => s.host === kind);
  /** Anything not on a VM of the project's, which should only be for a moment. */
  const elsewhere = () => services().filter((s) => !hosts().some((h) => h.kind === s.host));

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

            <Show
              when={hosts().length > 0}
              fallback={
                <>
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

                  <section class="fx-row">
                    <ContainerTable services={services()} now={now()} />
                  </section>
                </>
              }
            >
              <For each={hosts()}>
                {(host) => (
                  <HostSection
                    host={host}
                    services={onHost(host.kind)}
                    bucket={host.kind === 'db' ? volume() : null}
                    metrics={metrics()}
                    now={now()}
                    shade={shade}
                  />
                )}
              </For>
              <Show when={elsewhere().length > 0}>
                <section class="fx-row">
                  <ContainerTable label="Not on a VM yet" services={elsewhere()} now={now()} />
                </section>
              </Show>
            </Show>

            {/* the machines behind dedicated and pooled backends ================ */}
            <Show when={machines().length > 0}>
              <section class="fx-row">
                <FxCell label="Machines" meta={<span><b>{machines().length}</b></span>}>
                  <table class="fx-list rs-machines">
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

/** One VM: the machine against its real size, how it is used, what runs on it. */
function HostSection(props: {
  host: CloudHost;
  services: CloudService[];
  bucket: CloudStats['bucket_volume'];
  metrics: boolean;
  now: number;
  shade: (name: string) => string | undefined;
}) {
  const h = () => props.host;
  const running = () => props.services.filter((s) => s.status === 'running');
  const points = (values: number[]) =>
    values.map((v, i) => ({ ts: props.now - (values.length - 1 - i) * SAMPLE_MS, v }));
  const cpuPoints = () => points(sumSeries(running().map((s) => s.cpu_series)).map((v) => v / 100));
  const memPoints = () => points(sumSeries(running().map((s) => s.mem_series)));
  const cpuNow = () => running().reduce((a, s) => a + (s.sample?.cpu_pct ?? 0), 0) / 100;
  const memPct = () => (h().memory_bytes > 0 ? (h().memory_used_bytes / h().memory_bytes) * 100 : null);
  const measured = () => !!h().measured_at;

  // The database's disk is its volume, read off the container using it. Everything
  // else on any VM lives on the system disk.
  const data = () => props.services.find((s) => s.role === 'surrealdb')?.disk ?? null;
  const dataUsed = () => {
    const d = data();
    return d ? d.fs_bytes - d.free_bytes : null;
  };
  const dataPct = () => {
    const d = data();
    return d && d.fs_bytes > 0 ? ((d.fs_bytes - d.free_bytes) / d.fs_bytes) * 100 : null;
  };
  const systemPct = () => (h().disk_bytes > 0 ? (h().disk_used_bytes / h().disk_bytes) * 100 : null);
  const bucketPct = () => {
    const u = props.bucket?.usage;
    return u && u.fs_bytes > 0 ? (u.used_bytes / u.fs_bytes) * 100 : null;
  };
  const systemDisk = () => (
    <DiskCell
      label="System disk"
      meta={<Show when={h().measured_at}>{(m) => <span>measured {relativeStamp(m())}</span>}</Show>}
      pct={systemPct()}
      used={measured() ? h().disk_used_bytes : null}
      free={measured() ? h().disk_bytes - h().disk_used_bytes : null}
      total={measured() ? formatBytes(h().disk_bytes) : '—'}
      totalLabel="Disk"
    />
  );
  // Limits are ceilings and they overlap: together they can pass the machine's size,
  // which is normal. It says what could happen if every container peaked at once.
  const limits = () => (
    <FxCell label="Container limits" meta={<span>what they may use</span>}>
      <div class="fx-big">
        {formatBytes(running().reduce((a, s) => a + memLimit(s), 0))}
        <small>memory</small>
      </div>
      <div class="fx-foot rs-disk">
        <div><span class="cap">vCPU</span><b>{running().reduce((a, s) => a + s.vcpus, 0)}</b></div>
        <div><span class="cap">Machine</span><b>{h().vcpus || '—'} <em>vCPU</em></b></div>
        <div><span class="cap">Running</span><b>{running().length}</b></div>
      </div>
    </FxCell>
  );

  return (
    <>
      <section class="fx-row">
        <FxCell
          class="rs-vm"
          label={<span class="rs-vm-name">{kindLabel(h().kind)}</span>}
          meta={
            <span class="rs-vm-spec">
              <b>{h().server_type}</b>
              <span>{h().location}</span>
              <Show when={h().vcpus > 0}><span>{h().vcpus} vCPU</span></Show>
              <Show when={h().memory_bytes > 0}><span>{formatBytes(h().memory_bytes)}</span></Show>
              <Show when={h().status !== 'ready'}><span class="warn">{h().status}</span></Show>
            </span>
          }
        >
          <></>
        </FxCell>
      </section>
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1.4fr)) minmax(0, 1fr)' }}>
        <FxCell label="CPU" meta={<span>containers <b>{props.metrics ? cores(cpuNow()) : '—'}</b> cores · last 15 min</span>}>
          <div class="fx-big">
            <span classList={{ warn: (h().cpu_pct ?? 0) >= 80 && (h().cpu_pct ?? 0) < 90, bad: (h().cpu_pct ?? 0) >= 90 }}>
              {pct(h().cpu_pct)}
            </span>
            <small>of the machine<Show when={h().load1 != null}> · load {h().load1!.toFixed(2)}</Show></small>
          </div>
          <Show when={props.metrics}>
            <LineChart points={cpuPoints()} format={(v) => `${cores(v)}`} now={props.now} />
          </Show>
        </FxCell>
        <FxCell label="Memory" meta={<span>containers <b>{formatBytes(running().reduce((a, s) => a + (s.sample?.mem_bytes ?? 0), 0))}</b> · last 15 min</span>}>
          <div class="fx-big">
            <span classList={{ warn: (memPct() ?? 0) >= 80 && (memPct() ?? 0) < 90, bad: (memPct() ?? 0) >= 90 }}>
              {measured() ? formatBytes(h().memory_used_bytes) : '—'}
            </span>
            <Show when={measured()}><small>of {formatBytes(h().memory_bytes)} · {pct(memPct())}</small></Show>
          </div>
          <Show when={props.metrics}>
            <LineChart points={memPoints()} format={(v) => formatBytes(v)} now={props.now} />
          </Show>
        </FxCell>
        <div class="rs-stack">
          <Show when={h().kind === 'db'} fallback={systemDisk()}>
            <DiskCell
              label="Data volume"
              meta={<Show when={data()}>{(d) => <span>measured {relativeStamp(d().measured_at)}</span>}</Show>}
              pct={dataPct()}
              used={dataUsed()}
              free={data()?.free_bytes ?? null}
              total={h().data_volume_gb ? `${h().data_volume_gb} GB` : '—'}
              totalLabel="Volume"
            />
          </Show>
          <Show when={props.bucket} fallback={h().kind === 'db' ? systemDisk() : limits()}>
            {(b) => (
              <DiskCell
                label="Bucket volume"
                meta={<Show when={b().usage}>{(u) => <span>measured {relativeStamp(u().measured_at)}</span>}</Show>}
                pct={bucketPct()}
                used={b().usage?.used_bytes ?? null}
                free={b().usage?.free_bytes ?? null}
                total={`${b().provisioned_gb} GB`}
                totalLabel="Volume"
              />
            )}
          </Show>
        </div>
      </section>
      <Show when={props.metrics && running().length > 3}>
        <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1fr))' }}>
          <Split
            label="CPU by container"
            items={running().map((s) => ({ name: s.name, value: (s.sample?.cpu_pct ?? 0) / 100, shade: props.shade(s.name) ?? SHADES[0]! }))}
            format={(v) => `${cores(v)} cores`}
          />
          <Split
            label="Memory by container"
            items={running().map((s) => ({ name: s.name, value: s.sample?.mem_bytes ?? 0, shade: props.shade(s.name) ?? SHADES[0]! }))}
            format={(v) => formatBytes(v)}
          />
        </section>
      </Show>
      <section class="fx-row">
        <ContainerTable label="On this VM" services={props.services} now={props.now} />
      </section>
    </>
  );
}

/** A disk: how full, used, free and its size. */
function DiskCell(props: {
  label: string;
  meta?: JSX.Element;
  pct: number | null;
  used: number | null;
  free: number | null;
  total: string;
  totalLabel: string;
}) {
  return (
    <FxCell label={props.label} meta={props.meta}>
      <div class="fx-big">
        <span classList={{ warn: (props.pct ?? 0) >= 80 && (props.pct ?? 0) < 90, bad: (props.pct ?? 0) >= 90 }}>
          {props.pct != null ? pct(props.pct) : '—'}
        </span>
        <Show when={props.pct != null}><small>full</small></Show>
      </div>
      <Show when={props.pct != null} fallback={<div class="fx-sub cap"><span>waiting for the first measurement</span></div>}>
        <div style={{ 'margin-top': '16px' }}><Seg pct={props.pct} wide /></div>
        <div class="fx-foot rs-disk">
          <div><span class="cap">Used</span><b>{formatBytes(props.used)}</b></div>
          <div><span class="cap">Free</span><b>{formatBytes(props.free)}</b></div>
          <div><span class="cap">{props.totalLabel}</span><b>{props.total}</b></div>
        </div>
      </Show>
    </FxCell>
  );
}

/** Containers, closest to a limit first. */
function ContainerTable(props: { label?: string; services: CloudService[]; now: number }) {
  const running = () => props.services.filter((s) => s.status === 'running').length;
  const sorted = () => [...props.services].sort((a, b) => pressure(b) - pressure(a) || a.name.localeCompare(b.name));
  const roleText = (s: CloudService) =>
    s.machine ? `forwards to ${s.machine}` : s.role === 'dblink' ? 'tunnel' : s.role;
  return (
    <FxCell label={props.label ?? 'Containers'} meta={<span><b>{running()}</b> of {props.services.length} running · closest to a limit first</span>}>
      <Show when={props.services.length} fallback={<div class="fx-empty cap">No containers are running</div>}>
        <table class="fx-list rs-containers">
          <colgroup>
            <col style={{ width: '13%' }} /><col style={{ width: '18%' }} /><col style={{ width: '18%' }} />
            <col style={{ width: '19%' }} /><col style={{ width: '8%' }} /><col style={{ width: '17%' }} /><col style={{ width: '7%' }} />
          </colgroup>
          <thead>
            <tr>
              <th class="cap">Container</th><th class="cap" title="against this container's own limit">CPU</th><th class="cap" title="against this container's own limit">Memory</th>
              <th class="cap" title="its volumes, against what they take plus what is still free on their disk">Disk</th>
              <th class="cap r">Disk writes</th><th class="cap r">Network in / out</th><th class="cap r">Up</th>
            </tr>
          </thead>
          <tbody>
            <For each={sorted()}>
              {(s) => (
                <tr>
                  <td>
                    <span class="fx-who">
                      <Sq tone={toneOf(s)} />
                      <span style={{ color: 'var(--ink)' }}>{s.name}</span>
                    </span>
                    <span class="vw-meta cap">
                      <span>{roleText(s)}</span>
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
                  <td class="r">{s.since ? formatUptime((props.now - Date.parse(s.since)) / 1000) : '—'}</td>
                </tr>
              )}
            </For>
          </tbody>
        </table>
      </Show>
    </FxCell>
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
