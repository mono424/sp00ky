import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import { execa, type ResultPromise } from 'execa';
import { waitForHttp } from '../util/wait.js';

export interface ProcessOptions {
  startupTimeoutMs?: number;
  logFile?: string;
  isolateEnv?: boolean;
  onSpawn?: (pid: number) => void;
  signal?: AbortSignal;
}

export function processEnvironment(isolate: boolean = false): Record<string, string> {
  return Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] =>
        entry[1] !== undefined && (!isolate || !entry[0].startsWith('SPKY_'))
    )
  );
}

export function processWorkDir(
  provided: string | undefined,
  prefix: string
): {
  workDir: string;
  cleanup: () => void;
} {
  const workDir = provided
    ? path.resolve(provided)
    : fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  fs.mkdirSync(workDir, { recursive: true });
  return {
    workDir,
    cleanup: () => {
      if (!provided) fs.rmSync(workDir, { recursive: true, force: true });
    },
  };
}

/** Port zero requests a currently available ephemeral port. */
export async function availablePort(port: number, label: string): Promise<number> {
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535)
    throw new Error(`Invalid ${label} port: ${port}`);
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', () => reject(new Error(`${label} port ${port} is in use`)));
    server.once('listening', () => {
      const address = server.address();
      const selected = typeof address === 'object' && address ? address.port : port;
      server.close((error) => (error ? reject(error) : resolve(selected)));
    });
    server.listen(port, '127.0.0.1');
  });
}

/** Own the child before readiness so every failure path can terminate it. */
export async function startProcess(
  opts: ProcessOptions & {
    label: string;
    cmd: string;
    args: string[];
    workDir: string;
    env: Record<string, string>;
    healthUrl: string;
    verbose?: boolean;
    cleanup: () => void;
  }
): Promise<{ child: ResultPromise; readyMs: number; stop: () => Promise<void> }> {
  opts.signal?.throwIfAborted();
  let output: fs.WriteStream | undefined;
  if (opts.logFile) {
    const file = path.resolve(opts.logFile);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    output = fs.createWriteStream(file, { fd: fs.openSync(file, 'a') });
  }
  const startedAt = performance.now();
  const child = execa(opts.cmd, opts.args, {
    cwd: opts.workDir,
    env: opts.env,
    extendEnv: false,
    stdout: output || opts.verbose ? 'pipe' : 'ignore',
    stderr: 'pipe',
    buffer: false,
    reject: false,
    forceKillAfterDelay: 5_000,
  });
  let stderrTail = '';
  child.stdout?.on('data', (chunk: Buffer) => {
    output?.write(chunk);
    if (opts.verbose) process.stdout.write(chunk);
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    stderrTail = (stderrTail + chunk.toString()).slice(-4000);
    output?.write(chunk);
    if (opts.verbose) process.stderr.write(chunk);
  });
  let outputError: Error | undefined;
  output?.on('error', (error) => {
    outputError = error;
  });
  let stopping: Promise<void> | undefined;
  const stop = (): Promise<void> =>
    (stopping ??= (async () => {
      try {
        child.kill('SIGTERM');
      } catch {
        /* already exited */
      }
      await child.catch(() => {});
      if (output)
        await new Promise<void>((resolve) => {
          if (output!.destroyed) resolve();
          else output!.end(resolve);
        });
      opts.cleanup();
    })());
  const readyController = new AbortController();
  const readySignal = opts.signal
    ? AbortSignal.any([opts.signal, readyController.signal])
    : readyController.signal;
  try {
    if (child.pid !== undefined) opts.onSpawn?.(child.pid);
    await Promise.race([
      waitForHttp(opts.healthUrl, {
        timeoutMs: opts.startupTimeoutMs ?? 600_000,
        label: `${opts.label} readiness`,
        signal: readySignal,
      }),
      child.then((result) => {
        throw new Error(
          `${opts.label} exited before readiness (code=${result.exitCode ?? 'unknown'}). Stderr tail:\n${stderrTail}`
        );
      }),
    ]);
    if (outputError) throw outputError;
    return { child, readyMs: performance.now() - startedAt, stop };
  } catch (error) {
    await stop();
    throw error;
  } finally {
    readyController.abort();
  }
}
