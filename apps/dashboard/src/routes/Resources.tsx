import { For, Show, createResource, onCleanup } from 'solid-js';
import { api } from '../api/client';
import type { CloudService, CloudStats } from '../api/types';
import { Empty, PageHead, Panel, Pill, StatCard } from '../components/Chrome';
import { Sparkline, type Point } from '../components/Sparkline';
import { formatBytes, relativeStamp, splitValue } from '../lib/format';
import { containerTone, fillTone, type Tone } from '../lib/status';

/**
 * Resources: what the deployment runs on, as Sp00ky Cloud measures it, with
 * whatever is closest to its limit first.
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

const memLimit = (s: CloudService) => s.sample?.mem_limit_bytes || s.memory_mb * 1024 * 1024;

/** CPU as a share of the container's vCPUs; `cpu_pct` is per core. */
const cpuFrac = (s: CloudService) =>
  s.sample && s.vcpus > 0 ? s.sample.cpu_pct / (s.vcpus * 100) : null;
const memFrac = (s: CloudService) => (s.sample ? s.sample.mem_bytes / memLimit(s) : null);

/** How close a container is to any limit, 0..1; a stopped one sorts first. */
function pressure(s: CloudService): number {
  if (s.status !== 'running') return 2;
  return Math.max(cpuFrac(s) ?? 0, memFrac(s) ?? 0);
}

function rowTone(s: CloudService): Tone {
  if (s.status !== 'running') return containerTone(s.status);
  const t = fillTone(Math.max(cpuFrac(s) ?? 0, memFrac(s) ?? 0));
  return t === 'idle' ? 'ok' : t;
}

/** One usage bar with its caption. Neutral until it nears the limit. */
function Meter(props: { label: string; value: string; fraction: number | null; title?: string }) {
  const tone = () => fillTone(props.fraction);
  return (
    <span class="res-meter" title={props.title}>
      <span class="res-meter-head">
        <span>{props.label}</span>
        <span class="res-meter-value" classList={{ [`tone-${tone()}`]: tone() === 'warn' || tone() === 'bad' }}>
          {props.value}
        </span>
      </span>
      <span class="res-bar">
        <span
          class="res-bar-fill"
          classList={{ [tone()]: true }}
          style={{ width: `${Math.min(100, Math.max(1, (props.fraction ?? 0) * 100))}%` }}
        />
      </span>
    </span>
  );
}

const BoxIcon = () => (
  <svg viewBox="0 0 16 16">
    <path d="M8 1.8 13.5 4.8v6.4L8 14.2 2.5 11.2V4.8zM2.5 4.8 8 7.8l5.5-3M8 7.8v6.4" />
  </svg>
);

const ServerIcon = () => (
  <svg viewBox="0 0 16 16">
    <path d="M2.5 3h11v4h-11zM2.5 9h11v4h-11zM5 5h.01M5 11h.01" />
  </svg>
);

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
  const cpuNow = () => running().reduce((a, s) => a + (s.sample?.cpu_pct ?? 0), 0) / 100;
  const cpuAlloc = () => running().reduce((a, s) => a + s.vcpus, 0);
  const memNow = () => running().reduce((a, s) => a + (s.sample?.mem_bytes ?? 0), 0);
  const memAlloc = () => running().reduce((a, s) => a + memLimit(s), 0);
  const cpuSeries = () => sumSeries(running().map((s) => s.cpu_series));
  const memSeries = () => sumSeries(running().map((s) => s.mem_series));

  const services = () => [...(data()?.services ?? [])].sort((a, b) => pressure(b) - pressure(a));

  const volume = () => data()?.bucket_volume ?? null;
  const diskFrac = () => {
    const u = volume()?.usage;
    return u && u.fs_bytes > 0 ? u.used_bytes / u.fs_bytes : null;
  };

  const machines = () => [
    ...(data()?.machines ?? []).map((m) => ({
      name: m.name,
      status: m.status,
      label: m.phase || m.status,
      detail: ['dedicated', m.backend, m.server_type, m.location, m.ip].filter(Boolean).join(' · '),
    })),
    ...(data()?.pool_machines ?? []).map((m) => ({
      name: m.machine_id,
      status: m.status,
      label: m.status,
      detail: [`pool ${m.pool}`, m.server_type, m.location].filter(Boolean).join(' · '),
    })),
  ];

  const memSplit = () => splitValue(formatBytes(memNow()));

  return (
    <>
      <PageHead
        title="Resources"
        subtitle={
          <Show when={data()?.deployment}>
            {(d) => (
              <>
                Deployment v{d().version}
                <Show when={d().deployed_at}>, deployed {relativeStamp(d().deployed_at)}</Show>
              </>
            )}
          </Show>
        }
      />

      <div class="page-body">
        <Show when={error()}>
          {(message) => (
            <div class="banner">
              <span class="dot bad" />
              {message()}
            </div>
          )}
        </Show>

        <Show when={data()} fallback={<Show when={!error()}><Empty>Loading…</Empty></Show>}>
          {(d) => (
            <div class="page-stack">
              <div class="stat-grid">
                <StatCard
                  label="CPU"
                  value={d().metrics_available ? cpuNow().toFixed(2) : '—'}
                  unit={`of ${cpuAlloc()} vCPU`}
                  note={d().metrics_available ? 'last 15 minutes' : 'no metrics from Sp00ky Cloud'}
                >
                  <Show when={d().metrics_available && cpuSeries().length > 1}>
                    <span class="stat-plot">
                      <Sparkline points={series(cpuSeries(), now())} fill bare format={pct} ariaLabel="CPU, last 15 minutes" />
                    </span>
                  </Show>
                </StatCard>
                <StatCard
                  label="Memory"
                  value={d().metrics_available ? memSplit().value : '—'}
                  unit={`${d().metrics_available ? memSplit().unit + ' ' : ''}of ${formatBytes(memAlloc())}`}
                  tone={fillTone(memAlloc() > 0 ? memNow() / memAlloc() : null)}
                  note={d().metrics_available ? 'last 15 minutes' : 'no metrics from Sp00ky Cloud'}
                >
                  <Show when={d().metrics_available && memSeries().length > 1}>
                    <span class="stat-plot">
                      <Sparkline points={series(memSeries(), now())} fill bare format={formatBytes} ariaLabel="Memory, last 15 minutes" />
                    </span>
                  </Show>
                </StatCard>
                <StatCard
                  label="Bucket disk"
                  value={diskFrac() != null ? pct(diskFrac()! * 100) : '—'}
                  unit={diskFrac() != null ? 'full' : undefined}
                  tone={fillTone(diskFrac())}
                  note={
                    volume()?.usage
                      ? `${formatBytes(volume()!.usage!.free_bytes)} free of ${formatBytes(volume()!.usage!.fs_bytes)}`
                      : volume()
                        ? 'waiting for the first measurement'
                        : 'no bucket volume configured'
                  }
                >
                  <Show when={diskFrac() != null}>
                    <span class="res-disk">
                      <span class="res-bar">
                        <span
                          class="res-bar-fill"
                          classList={{ [fillTone(diskFrac())]: true }}
                          style={{ width: `${Math.min(100, Math.max(1, diskFrac()! * 100))}%` }}
                        />
                      </span>
                    </span>
                  </Show>
                </StatCard>
              </div>

              <Panel title="Containers" sub="Closest to a limit first" flush>
                <Show when={services().length > 0} fallback={<Empty>No containers are running.</Empty>}>
                  <ul class="list">
                    <For each={services()}>
                      {(s) => (
                        <li class="list-item res-row" classList={{ [rowTone(s)]: true }}>
                          <span class="list-icon" aria-hidden="true">
                            <BoxIcon />
                          </span>
                          <span class="list-text">
                            <span class="list-title">{s.name}</span>
                            <span class="list-detail">
                              {s.machine ? `forwards to ${s.machine}` : s.role}
                            </span>
                          </span>
                          <Show
                            when={s.status === 'running' && s.sample}
                            fallback={
                              <span class="res-meters">
                                <Show when={s.status !== 'running'} fallback={<span class="list-meta">no recent sample</span>}>
                                  <Pill tone={containerTone(s.status)}>{s.status}</Pill>
                                </Show>
                              </span>
                            }
                          >
                            {(sample) => (
                              <span class="res-meters">
                                <Meter
                                  label="CPU"
                                  value={pct((cpuFrac(s) ?? 0) * 100)}
                                  fraction={cpuFrac(s)}
                                  title={`${(sample().cpu_pct / 100).toFixed(2)} of ${s.vcpus} vCPU`}
                                />
                                <Meter
                                  label="Memory"
                                  value={`${formatBytes(sample().mem_bytes)} / ${formatBytes(memLimit(s))}`}
                                  fraction={memFrac(s)}
                                />
                              </span>
                            )}
                          </Show>
                        </li>
                      )}
                    </For>
                  </ul>
                </Show>
              </Panel>

              <Show when={machines().length > 0}>
                <Panel title="Machines" flush>
                  <ul class="list">
                    <For each={machines()}>
                      {(m) => (
                        <li class="list-item" classList={{ [containerTone(m.status)]: true }}>
                          <span class="list-icon" aria-hidden="true">
                            <ServerIcon />
                          </span>
                          <span class="list-text">
                            <span class="list-title">{m.name}</span>
                            <span class="list-detail">{m.detail}</span>
                          </span>
                          <span class="list-meta">{m.label}</span>
                        </li>
                      )}
                    </For>
                  </ul>
                </Panel>
              </Show>
            </div>
          )}
        </Show>
      </div>
    </>
  );
}
