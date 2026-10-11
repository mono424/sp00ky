import path from 'node:path';
import fs from 'node:fs';
import type { ResultPromise } from 'execa';
import { log } from '../util/log.js';
import type { SurrealHandle } from './surrealdb.js';
import {
  availablePort,
  processEnvironment,
  processWorkDir,
  startProcess,
  type ProcessOptions,
} from './process.js';

export interface SchedulerHandle {
  baseUrl: string;
  pid: number | undefined;
  child: ResultPromise;
  /** Time from process spawn to /health/ready. */
  readyMs: number;
  stop: () => Promise<void>;
}

export interface SchedulerOptions extends ProcessOptions {
  /** Path to a prebuilt scheduler binary; if absent we use cargo run --release. */
  binPath?: string;
  repoRoot?: string;
  /** Listen port. Default 9667; zero selects an available ephemeral port. */
  port?: number;
  verbose?: boolean;
  env?: Record<string, string>;
  /** Caller-owned state directory. An omitted directory is temporary and removed on stop. */
  workDir?: string;
}

export async function startScheduler(
  surreal: SurrealHandle,
  opts: SchedulerOptions = {}
): Promise<SchedulerHandle> {
  const port = await availablePort(opts.port ?? 9667, 'Scheduler');
  const repoRoot = opts.repoRoot ?? path.resolve(import.meta.dirname, '../../../..');
  const { workDir, cleanup } = processWorkDir(opts.workDir, 'spooky-bench-scheduler-');
  try {
    // The scheduler reads listener settings from this file; there is no port env override.
    if (opts.port !== undefined || !opts.workDir) {
      const config = path.join(workDir, 'sp00ky.yml');
      if (fs.existsSync(config))
        throw new Error(
          `Scheduler working directory already contains ${config}; use an empty directory for a configured benchmark port`
        );
      fs.writeFileSync(config, `ingest_host: 127.0.0.1\ningest_port: ${port}\n`);
    }
    const env = {
      ...processEnvironment(opts.isolateEnv),
      ...(opts.isolateEnv
        ? { SPKY_ADMIN_ENABLED: 'false', SPKY_POOL_ENABLED: 'false', SPKY_POOL_CLOUD: 'false' }
        : {}),
      SPKY_DB_URL: surreal.httpUrl,
      SPKY_DB_WS: surreal.wsUrl,
      SPKY_DB_NS: surreal.namespace,
      SPKY_DB_NAME: surreal.database,
      SPKY_DB_USER: surreal.username,
      SPKY_DB_PASS: surreal.password,
      SPKY_AUTH_SECRET:
        opts.env?.SPKY_AUTH_SECRET ??
        (opts.isolateEnv ? undefined : process.env.SPKY_AUTH_SECRET) ??
        'bench',
      RUST_LOG: process.env.RUST_LOG ?? 'warn,scheduler=info',
      ...(opts.env ?? {}),
    };
    const workspaceBin = path.join(repoRoot, 'target/release/scheduler');
    const perAppBin = path.join(repoRoot, 'apps/scheduler/target/release/scheduler');
    const candidate = opts.binPath
      ? path.resolve(opts.binPath)
      : fs.existsSync(workspaceBin)
        ? workspaceBin
        : perAppBin;
    if (opts.binPath && !fs.existsSync(candidate))
      throw new Error(`Scheduler binary not found: ${candidate}`);
    const useCargo = !fs.existsSync(candidate);
    const cmd = useCargo ? 'cargo' : candidate;
    const args = useCargo
      ? ['run', '--release', '--manifest-path', path.join(repoRoot, 'apps/scheduler/Cargo.toml')]
      : [];
    const baseUrl = `http://127.0.0.1:${port}`;
    log.info(
      `Starting scheduler (${useCargo ? 'cargo run' : candidate}) on ${baseUrl}, workDir=${workDir}`
    );
    const launched = await startProcess({
      ...opts,
      label: 'Scheduler',
      cmd,
      args,
      workDir,
      env,
      healthUrl: `${baseUrl}/health/ready`,
      cleanup,
    });
    log.info(`Scheduler ready at ${baseUrl}`);
    return { baseUrl, pid: launched.child.pid, ...launched };
  } catch (error) {
    cleanup();
    throw error;
  }
}
