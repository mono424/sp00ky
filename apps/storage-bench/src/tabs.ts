/**
 * Shared tabs, measured with two real tabs. With `sharedTabs` one tab (the
 * leader) holds the store and every other tab (a follower) reaches it through
 * a port into the leader's worker. `followerReads: 'direct'` gives a follower
 * a worker of its own that reads the store's file beside the leader.
 *
 * This file is both sides. A tab opened with `?leader` leads: it opens the
 * store, loads the dataset, serves the ports it is handed and, on request,
 * keeps writing sync-down batches the way a tab that is syncing does. The
 * benchmark tab follows it twice, once each way, and times the same reads
 * while the leader is idle and while it is writing.
 */
import { engineBulk, sharedRowsEngine } from './backends';
import { generate, pick, rng, type Row } from './data';
import type { Metric } from './view';

const TABS_BUCKET = 'bench-tabs';
const LOCK = 'sp00ky-bench-tabs-leader';

type Port = MessagePort;

function connect(role: 'leader' | 'bench'): Port {
  const worker = new SharedWorker(new URL('./relay.ts', import.meta.url), { type: 'module', name: 'sp00ky-bench-relay' });
  worker.port.start();
  worker.port.postMessage({ type: 'hello', role });
  return worker.port;
}

/** The next message of `type` on `port`. */
function next<T = Record<string, unknown>>(port: Port, type: string, timeoutMs = 60_000): Promise<T & { ports: readonly MessagePort[] }> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      port.removeEventListener('message', onMessage);
      reject(new Error(`shared tabs: no "${type}" from the other tab`));
    }, timeoutMs);
    const onMessage = (ev: MessageEvent): void => {
      if ((ev.data as { type?: string })?.type !== type) return;
      clearTimeout(timer);
      port.removeEventListener('message', onMessage);
      resolve({ ...(ev.data as T), ports: ev.ports });
    };
    port.addEventListener('message', onMessage);
  });
}

// ---- the leader tab ------------------------------------------------------------

export async function serveAsLeader(say: (text: string) => void): Promise<void> {
  say('Leader tab of the shared-tabs test. It closes itself when the test is done.');
  const engine = sharedRowsEngine('direct');
  await engine.adoptOwner(TABS_BUCKET, { workerLockName: LOCK, allowMemoryFallback: false, resumeHeld: false });
  const port = connect('leader');
  let loading = false;
  let batches = 0;

  /** What a tab that is syncing down does to the store: batch after batch, with barely a pause. */
  const load = async (): Promise<void> => {
    const rnd = rng(99);
    while (loading) {
      const rows: Row[] = [];
      for (let i = 0; i < 250; i++) {
        rows.push({ id: `inbox:${rnd(20_000)}`, _00_rv: batches, subject: `Message ${i}`, body: 'x'.repeat(600 + rnd(600)), read: i % 3 === 0 });
      }
      // oxlint-disable-next-line no-await-in-loop
      await engineBulk(engine, rows);
      batches++;
      // The sync layer's own work between two batches: a breath, not a rest.
      // oxlint-disable-next-line no-await-in-loop
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
  };

  port.addEventListener('message', (ev: MessageEvent) => {
    const msg = ev.data as { type: string; clientId?: string; games?: number; comments?: number; on?: boolean };
    void (async () => {
      if (msg.type === 'seed') {
        const have = await engine.select({ table: 'game', limit: 1 });
        const data = generate(msg.games ?? 20_000, msg.comments ?? 60_000);
        if (have.length === 0) {
          say(`Leader tab: loading ${data.games.length + data.comments.length} rows`);
          await engineBulk(engine, data.users);
          for (let i = 0; i < data.games.length; i += 250) await engineBulk(engine, data.games.slice(i, i + 250));
          for (let i = 0; i < data.comments.length; i += 500) await engineBulk(engine, data.comments.slice(i, i + 500));
        }
        say('Leader tab: serving followers');
        port.postMessage({ to: 'bench', type: 'seeded', ids: pick(data.games, 2000, 5) });
      } else if (msg.type === 'attach' && msg.clientId) {
        await engine.exposeClientPort(msg.clientId, ev.ports[0]);
        port.postMessage({ to: 'bench', type: 'attached', clientId: msg.clientId, storageHealth: engine.storageHealth });
      } else if (msg.type === 'load') {
        if (msg.on && !loading) {
          loading = true;
          void load();
        } else if (!msg.on) loading = false;
        port.postMessage({ to: 'bench', type: 'loading', on: loading, batches });
      } else if (msg.type === 'bye') {
        loading = false;
        await engine.shutdownOwnedWorker().catch(() => undefined);
        port.postMessage({ to: 'bench', type: 'gone' });
        window.close();
      }
    })();
  });
  port.postMessage({ to: 'bench', type: 'ready' });
}

// ---- the benchmark tab ------------------------------------------------------------

export type TabsResults = Record<string, { leader?: Metric; direct?: Metric; note?: string }>;

function stats(samples: number[]): Metric {
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number): number => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * q))];
  return { ms: at(0.5), p95: at(0.95), runs: sorted.length };
}

async function timed(runs: number, fn: () => Promise<unknown>): Promise<Metric> {
  for (let i = 0; i < 5; i++) await fn();
  const samples: number[] = [];
  for (let i = 0; i < runs; i++) {
    const t0 = performance.now();
    await fn();
    samples.push(performance.now() - t0);
  }
  return stats(samples);
}

/**
 * Follow a leader tab both ways and time the same reads. `openLeader` opens
 * the leader tab; it is only called when none answers yet, and must run in
 * the click that started the test (a tab cannot be opened later).
 */
export async function runTabsTest(
  config: { games: number; comments: number },
  say: (text: string) => void,
  openLeader: () => void
): Promise<TabsResults> {
  const port = connect('bench');
  const ready = Promise.race([next(port, 'ready'), next(port, 'peer')]);
  // No leader tab yet? Open one, in its own process as another tab would be.
  const quick = await Promise.race([ready.then(() => true), new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 400))]);
  if (!quick) {
    openLeader();
    say('Shared tabs: waiting for the leader tab');
    await next(port, 'ready');
  }
  say('Shared tabs: leader is loading the dataset');
  port.postMessage({ to: 'leader', type: 'seed', games: config.games, comments: config.comments });
  const { ids } = await next<{ ids: string[] }>(port, 'seeded', 180_000);

  const rnd = rng(3);
  const window = (n: number): string[] => [...new Set(Array.from({ length: n }, () => ids[rnd(ids.length)]))];
  const out: TabsResults = {};
  const put = (key: string, mode: 'leader' | 'direct', metric: Metric): void => {
    (out[key] ??= {})[mode] = metric;
  };

  for (const mode of ['leader', 'direct'] as const) {
    say(`Shared tabs: a follower reading ${mode === 'leader' ? 'through the leader' : 'the file itself'}`);
    const engine = sharedRowsEngine(mode);
    const channel = new MessageChannel();
    const attached = next<{ storageHealth: { status: 'persistent' | 'memory'; fallback: boolean } }>(port, 'attached');
    port.postMessage({ to: 'leader', type: 'attach', clientId: `bench-${mode}-${Date.now()}` }, [channel.port2]);
    const { storageHealth } = await attached;
    await engine.adoptAttached(channel.port1, { bucketId: TABS_BUCKET, storageHealth }, () => undefined);
    if (mode === 'direct' && !engine.readsDirectly) {
      out.window50_idle = { ...out.window50_idle, note: 'This browser has no shared file handles: direct reads fall back to the leader.' };
      await engine.close();
      continue;
    }
    const reads: [string, () => Promise<unknown>][] = [
      ['window50', () => engine.selectByIds('game', window(50))],
      [
        'filter',
        () =>
          engine.select({
            table: 'game',
            where: [{ field: 'collection', op: '=', value: `collection:${String(rnd(40)).padStart(4, '0')}` }],
            orderBy: [['created_ms', 'desc']],
            limit: 50,
          }),
      ],
    ];
    for (const [name, read] of reads) put(`${name}_idle`, mode, await timed(150, read));
    // The same reads while the leader tab writes sync-down batches.
    const loading = next(port, 'loading');
    port.postMessage({ to: 'leader', type: 'load', on: true });
    await loading;
    await new Promise((resolve) => setTimeout(resolve, 300));
    for (const [name, read] of reads) put(`${name}_busy`, mode, await timed(150, read));
    const stopped = next(port, 'loading');
    port.postMessage({ to: 'leader', type: 'load', on: false });
    await stopped;
    await new Promise((resolve) => setTimeout(resolve, 300));
    await engine.close();
  }
  const gone = next(port, 'gone', 5000).catch(() => undefined);
  port.postMessage({ to: 'leader', type: 'bye' });
  await gone;
  return out;
}
