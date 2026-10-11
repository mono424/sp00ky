import fs from 'node:fs';
import os from 'node:os';
import { execa } from 'execa';

const GiB = 1024 ** 3;

export interface ResourceSnapshot {
  diskFreeBytes: number;
  hostMemoryBytes: number;
  hostFreeMemoryBytes: number;
  dockerMemoryBytes: number;
  dockerVersion: string;
}

export async function readResources(directory: string): Promise<ResourceSnapshot> {
  // Docker CLI honors the selected context; testcontainers otherwise tries only
  // fixed socket paths. OrbStack and rootless installations often use neither.
  if (!process.env.DOCKER_HOST) {
    const { stdout: endpoint } = await execa(
      'docker',
      ['context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'],
      { timeout: 15_000 }
    );
    if (endpoint.trim()) process.env.DOCKER_HOST = endpoint.trim();
  }
  const disk = fs.statfsSync(directory);
  const { stdout } = await execa('docker', ['info', '--format', '{{json .}}'], { timeout: 15_000 });
  const docker = JSON.parse(stdout);
  return {
    diskFreeBytes: disk.bavail * disk.bsize,
    hostMemoryBytes: os.totalmem(),
    hostFreeMemoryBytes: os.freemem(),
    dockerMemoryBytes: docker.MemTotal,
    dockerVersion: docker.ServerVersion,
  };
}

export interface ResourceEstimate {
  rows: number;
  loadedRows?: number;
  pgnPayload: boolean;
  diskBytes: number;
  hostPeakBytes: number;
  dockerPeakBytes: number;
  basis: string;
}

/** Conservative initial estimates; completed cases replace these with observed bytes/row. */
export function estimateResources(
  rows: number,
  pgnPayload: boolean,
  measured: ResourceEstimate[] = [],
  additionalRows = 0
): ResourceEstimate {
  const peers = measured.filter((m) => m.pgnPayload === pgnPayload && m.rows <= rows);
  const previous = peers.sort((a, b) => b.rows - a.rows)[0];
  const loadedRows = rows + additionalRows;
  const initial: ResourceEstimate = {
    rows,
    loadedRows,
    pgnPayload,
    diskBytes: loadedRows * (pgnPayload ? 8192 : 4096),
    hostPeakBytes: 256 * 1024 ** 2 + loadedRows * (pgnPayload ? 6000 : 4500),
    dockerPeakBytes: 128 * 1024 ** 2 + loadedRows * (pgnPayload ? 1500 : 750),
    basis: 'conservative initial estimate',
  };
  if (previous) {
    const scale = loadedRows / (previous.loadedRows ?? previous.rows);
    return {
      ...initial,
      diskBytes: previous.diskBytes * scale * 1.25,
      hostPeakBytes: previous.hostPeakBytes * scale * 1.25,
      dockerPeakBytes: previous.dockerPeakBytes * scale * 1.25,
      basis: `1.25 × observed ${previous.rows}-row ${pgnPayload ? 'PGN' : 'compact'} case`,
    };
  }
  if (pgnPayload) {
    const compact = measured
      .filter((m) => !m.pgnPayload && m.rows <= rows)
      .sort((a, b) => b.rows - a.rows)[0];
    if (compact) {
      const scale = loadedRows / (compact.loadedRows ?? compact.rows);
      return {
        ...initial,
        diskBytes: Math.max(
          initial.diskBytes,
          compact.diskBytes * scale * 1.25 + loadedRows * 4096
        ),
        hostPeakBytes: Math.max(
          initial.hostPeakBytes,
          compact.hostPeakBytes * scale * 1.25 + loadedRows * 1500
        ),
        dockerPeakBytes: Math.max(
          initial.dockerPeakBytes,
          compact.dockerPeakBytes * scale * 1.25 + loadedRows * 750
        ),
        basis: `compact-derived PGN estimate: 1.25 × observed ${compact.rows}-row compact case + PGN payload premiums`,
      };
    }
  }
  return initial;
}

export function resourceLimit(
  resources: ResourceSnapshot,
  estimate: ResourceEstimate
): string | undefined {
  const reasons: string[] = [];
  if (estimate.diskBytes + 5 * GiB > resources.diskFreeBytes)
    reasons.push('estimated disk use would leave less than 5 GiB free');
  // Docker consumes the same physical host memory as the native processes.
  if (estimate.hostPeakBytes + estimate.dockerPeakBytes > resources.hostMemoryBytes * 0.8)
    reasons.push('estimated scheduler + SSP + database memory exceeds 80% of host RAM');
  if (estimate.dockerPeakBytes > resources.dockerMemoryBytes * 0.8)
    reasons.push('estimated database RSS exceeds 80% of Docker RAM');
  return reasons.length ? reasons.join('; ') : undefined;
}

export interface ProcessSample {
  t: number;
  entity: 'scheduler' | 'ssp';
  pid: number;
  rssBytes: number;
  /** ps %cpu is the OS-reported process CPU percentage, not a derived per-request cost. */
  cpuPercent: number;
}

export class ProcessSampler {
  readonly samples: ProcessSample[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private pending?: Promise<void>;
  private stopped = false;
  private readonly started = performance.now();

  constructor(
    private entity: ProcessSample['entity'],
    private pid: number
  ) {}

  start(): void {
    const sample = async () => {
      try {
        const { stdout } = await execa('ps', ['-o', 'rss=,%cpu=', '-p', String(this.pid)], {
          timeout: 2000,
        });
        const [rss, cpu] = stdout.trim().split(/\s+/).map(Number);
        if (Number.isFinite(rss) && Number.isFinite(cpu))
          this.samples.push({
            t: performance.now() - this.started,
            entity: this.entity,
            pid: this.pid,
            rssBytes: rss! * 1024,
            cpuPercent: cpu!,
          });
      } catch {
        /* A process may exit between sampling and ps. */
      }
      if (!this.stopped)
        this.timer = setTimeout(() => {
          this.pending = sample();
        }, 250);
    };
    this.pending = sample();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    clearTimeout(this.timer);
    await this.pending;
  }
}

export async function directoryBytes(directory: string): Promise<number> {
  const { stdout } = await execa('du', ['-sk', directory], { timeout: 30_000 });
  return Number(stdout.trim().split(/\s+/)[0]) * 1024;
}

export async function databaseStats(
  containerId: string
): Promise<{ memoryBytes: number; cpuPercent: number }> {
  const { stdout } = await execa(
    'docker',
    ['stats', '--no-stream', '--format', '{{json .}}', containerId],
    { timeout: 10_000 }
  );
  const stats = JSON.parse(stdout);
  const used = String(stats.MemUsage).split('/')[0]!.trim();
  const match = /^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|kB|MB|GB)$/.exec(used);
  if (!match) throw new Error(`Cannot parse Docker memory usage: ${used}`);
  const units: Record<string, number> = {
    B: 1,
    KiB: 1024,
    MiB: 1024 ** 2,
    GiB,
    kB: 1000,
    MB: 1e6,
    GB: 1e9,
  };
  return {
    memoryBytes: Number(match[1]) * units[match[2]!]!,
    cpuPercent: Number.parseFloat(stats.CPUPerc),
  };
}
