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

export interface SspHandle {
  baseUrl: string;
  sspId: string;
  pid: number | undefined;
  child: ResultPromise;
  /** Time from process spawn to /health. */
  readyMs: number;
  stop: () => Promise<void>;
  /** Stop and restart with a fresh SSP identity. */
  restart: () => Promise<SspHandle>;
}

export interface SspOptions extends ProcessOptions {
  binPath?: string;
  repoRoot?: string;
  /** Default 8667; zero selects an available ephemeral port. */
  port?: number;
  schedulerUrl: string;
  sspId?: string;
  verbose?: boolean;
  env?: Record<string, string>;
  /** Caller-owned state directory; omitted directories are removed on stop. */
  workDir?: string;
}

export async function startSsp(surreal: SurrealHandle, opts: SspOptions): Promise<SspHandle> {
  const port = await availablePort(opts.port ?? 8667, 'SSP');
  const repoRoot = opts.repoRoot ?? path.resolve(import.meta.dirname, '../../../..');
  const sspId = opts.sspId ?? `bench-ssp-${Date.now()}`;
  const { workDir, cleanup } = processWorkDir(opts.workDir, 'spooky-bench-ssp-');
  try {
    const baseUrl = `http://127.0.0.1:${port}`;
    const env = {
      ...processEnvironment(opts.isolateEnv),
      SPKY_SSP_LISTEN_ADDR: `127.0.0.1:${port}`,
      SPKY_SSP_ADVERTISE_ADDR: `127.0.0.1:${port}`,
      SPKY_DB_URL: surreal.httpUrl,
      SPKY_DB_WS: surreal.wsUrl,
      SPKY_DB_NS: surreal.namespace,
      SPKY_DB_NAME: surreal.database,
      SPKY_DB_USER: surreal.username,
      SPKY_DB_PASS: surreal.password,
      SPKY_SCHEDULER_URL: opts.schedulerUrl,
      SPKY_SSP_ID: sspId,
      SPKY_AUTH_SECRET:
        opts.env?.SPKY_AUTH_SECRET ??
        (opts.isolateEnv ? undefined : process.env.SPKY_AUTH_SECRET) ??
        'bench',
      RUST_LOG: process.env.RUST_LOG ?? 'warn,ssp_server=info,ssp=info',
      ...(opts.env ?? {}),
    };
    const workspaceBin = path.join(repoRoot, 'target/release/ssp-server');
    const perAppBin = path.join(repoRoot, 'apps/ssp/target/release/ssp-server');
    const candidate = opts.binPath
      ? path.resolve(opts.binPath)
      : fs.existsSync(workspaceBin)
        ? workspaceBin
        : perAppBin;
    if (opts.binPath && !fs.existsSync(candidate))
      throw new Error(`SSP binary not found: ${candidate}`);
    const useCargo = !fs.existsSync(candidate);
    const cmd = useCargo ? 'cargo' : candidate;
    const args = useCargo
      ? ['run', '--release', '--manifest-path', path.join(repoRoot, 'apps/ssp/Cargo.toml')]
      : [];
    log.info(`Starting SSP (${useCargo ? 'cargo run' : candidate}) on ${baseUrl}, id=${sspId}`);
    const launched = await startProcess({
      ...opts,
      label: 'SSP',
      cmd,
      args,
      workDir,
      env,
      healthUrl: `${baseUrl}/health`,
      cleanup,
    });
    log.info(`SSP up at ${baseUrl}`);
    let successor: SspHandle | undefined;
    const stop = async () => {
      await (successor ? successor.stop() : launched.stop());
    };
    return {
      baseUrl,
      sspId,
      pid: launched.child.pid,
      ...launched,
      stop,
      restart: async () => {
        await stop();
        successor = await startSsp(surreal, {
          ...opts,
          port,
          sspId: `bench-ssp-restart-${Date.now()}`,
        });
        return successor;
      },
    };
  } catch (error) {
    cleanup();
    throw error;
  }
}
