import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  GenericContainer,
  type StartedTestContainer,
  Wait,
  waitForContainer,
  type WaitStrategy,
} from 'testcontainers';
import { Surreal } from 'surrealdb';
import { execa } from 'execa';
import { startScheduler } from './scheduler.js';
import { startSsp } from './ssp.js';
import { startSurrealDB, SurrealStartupCleanupError, type SurrealHandle } from './surrealdb.js';
import { processWorkDir, startProcess } from './process.js';
import { waitFor } from '../util/wait.js';

function assertExited(pid: number | undefined): void {
  assert.ok(pid !== undefined, 'the test process was spawned');
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
}

test('launchers use the isolated database, dynamic ports and provided auth, and release their processes', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spooky-harness-test-'));
  const binary = path.join(root, 'fake-backend');
  fs.writeFileSync(
    binary,
    `#!/usr/bin/env node
const fs = require('node:fs');
const http = require('node:http');
const config = fs.existsSync('sp00ky.yml') ? fs.readFileSync('sp00ky.yml', 'utf8') : '';
const port = process.env.SPKY_SSP_LISTEN_ADDR?.split(':').at(-1) ?? config.match(/ingest_port: (\\d+)/)[1];
console.log(JSON.stringify({cwd:process.cwd(),url:process.env.SPKY_DB_URL,poison:process.env.SPKY_BENCH_TEST_POISON,secret:process.env.SPKY_AUTH_SECRET}));
http.createServer((req,res) => {res.writeHead(200);res.end('{}');}).listen(Number(port),'127.0.0.1');
`,
    { mode: 0o755 }
  );
  const database: SurrealHandle = {
    httpUrl: 'http://127.0.0.1:12345',
    wsUrl: 'ws://127.0.0.1:12345',
    namespace: 'bench',
    database: 'bench',
    username: 'root',
    password: 'root',
    containerId: 'fake',
    readyMs: 0,
    stop: async () => {},
  };
  const previous = Object.fromEntries(
    ['SPKY_DB_URL', 'SPKY_BENCH_TEST_POISON', 'SPKY_AUTH_SECRET'].map((key) => [
      key,
      process.env[key],
    ])
  );
  process.env.SPKY_DB_URL = 'http://production.invalid';
  process.env.SPKY_BENCH_TEST_POISON = 'should not leak';
  process.env.SPKY_AUTH_SECRET = 'ambient';
  try {
    for (const kind of ['scheduler', 'ssp'] as const) {
      let spawned: number | undefined;
      const logFile = path.join(root, `${kind}.log`);
      const options = {
        binPath: binary,
        port: 0,
        isolateEnv: true,
        startupTimeoutMs: 5_000,
        logFile,
        onSpawn: (pid: number) => {
          spawned = pid;
        },
        env: { SPKY_AUTH_SECRET: 'provided' },
      };
      const handle =
        kind === 'scheduler'
          ? await startScheduler(database, options)
          : await startSsp(database, { ...options, schedulerUrl: 'http://127.0.0.1:1234' });
      try {
        assert.equal(spawned, handle.pid);
        assert.ok(handle.readyMs >= 0);
        assert.match(handle.baseUrl, /^http:\/\/127\.0\.0\.1:[1-9]\d*$/);
      } finally {
        await handle.stop();
      }
      const snapshot = JSON.parse(fs.readFileSync(logFile, 'utf8').trim());
      assert.equal(snapshot.url, database.httpUrl);
      assert.equal(snapshot.secret, 'provided');
      assert.equal(snapshot.poison, undefined);
      assert.equal(fs.existsSync(snapshot.cwd), false);
      assertExited(handle.pid);
    }
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('a child that exits before readiness fails immediately and removes owned state', async () => {
  const owned = processWorkDir(undefined, 'spooky-harness-exit-');
  const startedAt = Date.now();
  let pid: number | undefined;
  await assert.rejects(
    startProcess({
      ...owned,
      cmd: process.execPath,
      args: ['-e', 'process.stderr.write("expected failure");process.exit(7)'],
      label: 'exit test',
      env: {},
      healthUrl: 'http://127.0.0.1:1/health',
      startupTimeoutMs: 10_000,
      onSpawn: (value) => {
        pid = value;
      },
    }),
    /expected failure/
  );
  assert.ok(Date.now() - startedAt < 5_000, 'should not wait for the readiness deadline');
  assert.equal(fs.existsSync(owned.workDir), false);
  assertExited(pid);
});

test('aborting startup kills the child and removes its owned state', async () => {
  const owned = processWorkDir(undefined, 'spooky-harness-abort-');
  const controller = new AbortController();
  let pid: number | undefined;
  const starting = startProcess({
    ...owned,
    cmd: process.execPath,
    args: ['-e', 'setInterval(()=>{},1000)'],
    label: 'abort test',
    env: {},
    healthUrl: 'http://127.0.0.1:1/health',
    startupTimeoutMs: 10_000,
    signal: controller.signal,
    onSpawn: (value) => {
      pid = value;
    },
  });
  const timer = setTimeout(() => controller.abort(new Error('cancel test')), 50);
  try {
    await assert.rejects(starting, /cancel test/);
  } finally {
    clearTimeout(timer);
  }
  assert.equal(fs.existsSync(owned.workDir), false);
  assertExited(pid);
});

test("a hung predicate is bounded by the waiter's deadline and cancellation", async () => {
  await assert.rejects(
    waitFor(() => new Promise(() => {}), {
      timeoutMs: 40,
      label: 'hung predicate',
    }),
    /Timed out/
  );
  const controller = new AbortController();
  const waiting = waitFor(() => new Promise(() => {}), {
    timeoutMs: 10_000,
    signal: controller.signal,
  });
  controller.abort(new Error('cancel waiter'));
  await assert.rejects(waiting, /cancel waiter/);
});

test('failed database initialization marks incomplete container cleanup and preserves storage', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spooky-harness-database-'));
  const schemaPath = path.join(root, 'schema.surql');
  const storagePath = path.join(root, 'database');
  fs.writeFileSync(schemaPath, 'DEFINE TABLE game SCHEMALESS;');
  const startupError = new Error('expected connection failure');
  const cleanupError = new Error('expected container stop failure');
  let stopAttempts = 0;
  const container = {
    getMappedPort: () => 12345,
    getHost: () => '127.0.0.1',
    stop: async () => {
      stopAttempts++;
      throw cleanupError;
    },
  } as unknown as StartedTestContainer;
  t.mock.method(GenericContainer.prototype, 'start', async () => container);
  t.mock.method(Surreal.prototype, 'connect', async () => {
    throw startupError;
  });
  try {
    await assert.rejects(
      startSurrealDB({ modules: false, schemaPath, storagePath }),
      (error: unknown) => {
        assert.ok(error instanceof SurrealStartupCleanupError);
        assert.equal(error.cause, startupError);
        assert.deepEqual(error.errors, [startupError, cleanupError]);
        assert.equal(error.storagePath, storagePath);
        return true;
      }
    );
    assert.equal(stopAttempts, 1);
    assert.equal(fs.existsSync(storagePath), true);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('database readiness cancellation awaits Testcontainers stop and remove', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spooky-harness-db-cancel-'));
  const schemaPath = path.join(root, 'schema.surql');
  fs.writeFileSync(schemaPath, 'DEFINE TABLE game SCHEMALESS;');
  const controller = new AbortController();
  const cleanup: string[] = [];
  let waiting: () => void = () => {};
  const entered = new Promise<void>((resolve) => {
    waiting = resolve;
  });
  const delegate: WaitStrategy = {
    waitUntilReady: async () => {
      waiting();
      await new Promise<void>(() => {});
    },
    withStartupTimeout: () => delegate,
    isStartupTimeoutSet: () => true,
    getStartupTimeout: () => 3_600_000,
  };
  t.mock.method(Wait, 'forLogMessage', () => delegate as ReturnType<typeof Wait.forLogMessage>);
  t.mock.method(GenericContainer.prototype, 'start', async function (this: GenericContainer) {
    const strategy = (this as unknown as { waitStrategy: WaitStrategy }).waitStrategy;
    const runtime = {
      container: {
        stop: async () => {
          await new Promise((resolve) => setTimeout(resolve, 20));
          cleanup.push('stop');
        },
        remove: async () => {
          cleanup.push('remove');
        },
      },
    } as unknown as Parameters<typeof waitForContainer>[0];
    await waitForContainer(
      runtime,
      { id: 'fake' } as Parameters<typeof waitForContainer>[1],
      strategy,
      {} as Parameters<typeof waitForContainer>[3]
    );
    throw new Error('the hanging wait unexpectedly returned');
  });
  const starting = startSurrealDB({
    modules: false,
    schemaPath,
    signal: controller.signal,
    startupTimeoutMs: 3_600_000,
  });
  try {
    await entered;
    const startedAt = Date.now();
    controller.abort(new Error('cancel database readiness'));
    await assert.rejects(starting, /cancel database readiness/);
    assert.ok(
      Date.now() - startedAt < 5_000,
      'cancellation must not wait for the hour-long readiness timeout'
    );
    assert.deepEqual(cleanup, ['stop', 'remove'], 'cleanup finishes before startup rejects');
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('a completed image pull cannot create a container after cancellation', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spooky-harness-db-pull-'));
  const schemaPath = path.join(root, 'schema.surql');
  fs.writeFileSync(schemaPath, 'DEFINE TABLE game SCHEMALESS;');
  const controller = new AbortController();
  let finishPull: () => void = () => {};
  const pull = new Promise<void>((resolve) => {
    finishPull = resolve;
  });
  let created = false;
  t.mock.method(GenericContainer.prototype, 'start', async function (this: GenericContainer) {
    await pull;
    await (this as unknown as { beforeContainerCreated(): Promise<void> }).beforeContainerCreated();
    created = true;
    throw new Error('container creation should have been prevented');
  });
  const starting = startSurrealDB({ modules: false, schemaPath, signal: controller.signal });
  try {
    controller.abort(new Error('cancel database pull'));
    finishPull();
    await assert.rejects(starting, /cancel database pull/);
    assert.equal(created, false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('stack signal cleanup completes before Execa exit hooks terminate the process', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spooky-harness-signal-'));
  const schemaPath = path.join(root, 'schema.surql');
  const markerPath = path.join(root, 'container-stopped');
  fs.writeFileSync(schemaPath, 'DEFINE TABLE game SCHEMALESS;');
  const script = `
import fs from 'node:fs';
import { mock } from 'node:test';
import { execa } from 'execa';
import { GenericContainer } from 'testcontainers';
import { Surreal } from 'surrealdb';
import { startSurrealOnly } from ${JSON.stringify(new URL('./orchestrator.js', import.meta.url).href)};
mock.method(GenericContainer.prototype, 'start', async () => ({
  getMappedPort: () => 12345, getHost: () => '127.0.0.1', getId: () => 'fake',
  stop: async () => {
    await new Promise(resolve => setTimeout(resolve, 20));
    fs.writeFileSync(${JSON.stringify(markerPath)}, 'stopped');
  },
}));
for (const name of ['connect', 'signin', 'use', 'query', 'close']) {
  mock.method(Surreal.prototype, name, async () => []);
}
await startSurrealOnly({ surreal: { modules: false, schemaPath: ${JSON.stringify(schemaPath)} } });
// Register signal-exit after the stack's handlers, matching a real backend spawn.
const child = execa(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { reject: false });
setTimeout(() => process.kill(process.pid, 'SIGINT'), 50);
await child;
`;
  try {
    const result = await execa(process.execPath, ['--input-type=module', '-e', script], {
      cwd: path.resolve(import.meta.dirname, '../..'),
      reject: false,
      timeout: 5000,
    });
    assert.equal(result.exitCode, 130);
    assert.equal(fs.readFileSync(markerPath, 'utf8'), 'stopped');
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
