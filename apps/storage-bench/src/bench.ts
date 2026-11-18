/**
 * The benchmark's runner. One backend at a time, each in page loads of its
 * own so no backend runs beside another's worker or wasm heap:
 *
 *   warm    wipe everything, load the dataset, run every scenario in one session
 *   boot    open the store in a fresh page and make a query's first read, as
 *           an app does: after its script arrived (the main thread idle
 *           meanwhile) and ran its startup work (the main thread busy)
 *   cold    the same the moment the script runs, with nothing else to do
 *   reload  the same, after 200 writes and a reload that closed nothing
 *   start   the row store's worker, started on an idle page with and without
 *           the compile hint
 *
 * The queue of steps and the results live in localStorage, and the page
 * reloads itself from step to step. Finished runs are kept (the last eight),
 * so a run can be shown against an earlier one. The shared-tabs test needs a
 * second tab and has a button of its own (`tabs.ts`).
 */
import { CborCodec } from 'surrealdb';
import { BACKENDS, type Backend } from './backends';
import { generate, pick, rng, type Dataset, type Row } from './data';
import { runTabsTest, serveAsLeader } from './tabs';
import { ORDER, installTooltip, render, type Metric, type Results, type RunRecord } from './view';

interface Step {
  backend: string;
  phase: 'warm' | 'cold' | 'boot' | 'reload' | 'start';
  i: number;
}

// Fresh-page loads per phase. The first is left out; of the rest, every other one peeks (see `runStep`).
const COLD_RUNS = 13;
const HISTORY = 8;
const GAME_FIELDS = ['white_name', 'black_name', 'result', 'opening', 'played_at', 'moves'];

const load = <T>(key: string, fallback: T): T => {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
};
const save = (key: string, value: unknown): void => localStorage.setItem(key, JSON.stringify(value));

const config = { boot: 50, idle: 30, ...load('bench:config', { games: 20_000, comments: 60_000 }) };
const status = document.getElementById('status')!;
const say = (text: string): void => {
  status.textContent = text;
};

function stats(samples: number[]): Metric {
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number): number => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * q))];
  return { ms: at(0.5), p95: at(0.95), runs: sorted.length };
}

/** Time `fn` `runs` times after `warmup` unmeasured ones. */
async function timed(runs: number, fn: (i: number) => Promise<unknown>, warmup = 3): Promise<Metric> {
  for (let i = 0; i < warmup; i++) await fn(-1 - i);
  const samples: number[] = [];
  for (let i = 0; i < runs; i++) {
    const t0 = performance.now();
    await fn(i);
    samples.push(performance.now() - t0);
  }
  return stats(samples);
}

async function once(fn: () => Promise<unknown>): Promise<number> {
  const t0 = performance.now();
  await fn();
  return performance.now() - t0;
}

/** Let the page paint and the backend's own housekeeping settle. */
const settle = (ms = 700): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function warm(backend: Backend, data: Dataset): Promise<Record<string, Metric>> {
  const out: Record<string, Metric> = {};
  const rnd = rng(7);
  const gameIds = data.games.map((g) => String(g.id));
  const window = (n: number): string[] => {
    const ids: string[] = [];
    for (let i = 0; i < n; i++) ids.push(gameIds[rnd(gameIds.length)]);
    return [...new Set(ids)];
  };

  say(`${backend.name}: loading ${data.games.length + data.comments.length + data.users.length} rows`);
  out.ingest = {
    ms: await once(async () => {
      await backend.bulk('user', data.users);
      for (let i = 0; i < data.games.length; i += 250) await backend.bulk('game', data.games.slice(i, i + 250));
      for (let i = 0; i < data.comments.length; i += 500) await backend.bulk('comment', data.comments.slice(i, i + 500));
    }),
    note: `${data.games.length} games in batches of 250, ${data.comments.length} comments in batches of 500`,
  };
  // Past the few seconds in which the row store takes reads for an app's
  // startup reads and keeps a copy: the hundreds of random windows below are
  // not what an app reads at startup.
  await settle(3500);

  // First calls on a store that has just been loaded: whatever a backend
  // builds lazily (an index, a cache) is paid here and shown, not hidden in a warmup.
  say(`${backend.name}: first calls`);
  out.first_screen30 = { ms: await once(() => backend.screen(window(30))) };
  out.first_where_scan = { ms: await once(() => backend.where('game', 'eco', 'B90', 'created_ms', 50)) };

  say(`${backend.name}: reads`);
  out.window50 = await timed(200, () => backend.getMany('game', window(50)));
  out.window50_fields = await timed(200, () => backend.getMany('game', window(50), GAME_FIELDS));
  out.point = await timed(500, () => backend.get('game', gameIds[rnd(gameIds.length)]));
  out.screen30 = await timed(100, () => backend.screen(window(30)));
  out.children30 = await timed(100, () => backend.children('comment', 'game', window(30)));
  out.where_indexable = await timed(30, (i) =>
    backend.where('game', 'collection', `collection:${String(Math.abs(i) % 40).padStart(4, '0')}`, 'created_ms', 50)
  );
  const eco = ['B90', 'C65', 'D37', 'C11', 'B12', 'E60', 'A20', 'C50'];
  out.where_scan = await timed(15, (i) => backend.where('game', 'eco', eco[Math.abs(i) % eco.length], 'created_ms', 50), 1);
  out.versions = await timed(15, () => backend.versions('game'), 1);

  say(`${backend.name}: writes`);
  let version = 100;
  out.merge250 = await timed(
    20,
    async () => {
      version++;
      const start = rnd(Math.max(1, gameIds.length - 250));
      const patch: Row[] = [];
      for (let k = 0; k < 250; k++) patch.push({ id: gameIds[start + k], favorite: k % 2 === 0, _00_rv: version });
      await backend.bulk('game', patch);
    },
    2
  );
  out.put1 = await timed(200, () => backend.put('game', { id: gameIds[rnd(gameIds.length)], favorite: true, _00_rv: ++version }));
  let note = 0;
  out.user_write = await timed(100, () => {
    note++;
    return backend.userWrite({ id: `note:n${note}`, title: `Note ${note}`, body: 'x'.repeat(200), created_ms: note }, `m${note}`);
  });
  // Reads again, now that a few thousand rows were rewritten.
  out.window50_after_writes = await timed(100, () => backend.getMany('game', window(50)));

  await settle(1500);
  const bytes = await backend.size();
  if (bytes !== null) out.disk_mb = { ms: bytes / (1 << 20), note: 'megabytes on disk, not a time' };
  return out;
}

/** JSON against CBOR for the same rows, each as the stores would use it: one body at a time. */
function codec(data: Dataset): Record<string, Metric> {
  const rows = data.games.slice(0, 5000);
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const out: Record<string, Metric> = {};
  const best = (fn: () => void): number => {
    let min = Infinity;
    for (let i = 0; i < 5; i++) {
      const t0 = performance.now();
      fn();
      min = Math.min(min, performance.now() - t0);
    }
    return min;
  };
  let json: Uint8Array[] = [];
  out.json_encode = { ms: best(() => (json = rows.map((r) => enc.encode(JSON.stringify(r))))) };
  out.json_decode = { ms: best(() => json.map((b) => JSON.parse(dec.decode(b)))) };
  const whole = enc.encode(JSON.stringify(rows));
  out.json_decode_one_parse = { ms: best(() => JSON.parse(dec.decode(whole))), note: 'all rows as one array, how the row store ships a window' };
  let cbor: Uint8Array[] = [];
  out.cbor_encode = { ms: best(() => (cbor = rows.map((r) => CborCodec.DEFAULT.encode(r)))) };
  out.cbor_decode = { ms: best(() => cbor.map((b) => CborCodec.DEFAULT.decode(b))) };
  out.json_mb = { ms: json.reduce((n, b) => n + b.length, 0) / (1 << 20), note: 'megabytes' };
  out.cbor_mb = { ms: cbor.reduce((n, b) => n + b.length, 0) / (1 << 20), note: 'megabytes' };
  return out;
}

async function wipeAll(): Promise<void> {
  for (const name of ORDER) {
    try {
      await BACKENDS[name]().wipe();
    } catch (error) {
      console.warn(`wipe ${name}:`, error);
    }
  }
}

/**
 * The row store's worker, started on an idle page: from `new Worker()` to the
 * store being open. Twice over, alternating: the worker as built (the compile
 * hint on its first line) and a copy without the hint (`vite.config.ts`
 * writes both and says where).
 */
async function workerStart(): Promise<Record<string, Metric>> {
  const urls = (await (await fetch('/assets/workers.json')).json()) as { hint: string; plain: string };
  const g = globalThis as { __sp00kyRowsPrewarm?: { worker: Worker } };
  g.__sp00kyRowsPrewarm?.worker.terminate();
  delete g.__sp00kyRowsPrewarm;
  await settle(400);
  const once = async (url: string): Promise<{ total: number; inside: number }> => {
    const t0 = performance.now();
    const worker = new Worker(url, { type: 'module' });
    const reply = await new Promise<{ ok: boolean; wt?: number; error?: string }>((resolve) => {
      worker.onmessage = (ev) => resolve(ev.data as { ok: boolean; wt?: number });
      worker.onerror = (ev) => resolve({ ok: false, error: ev.message });
      worker.postMessage({ id: 1, type: 'open', payload: { dbName: 'bench-start', useOpfs: true } });
    });
    const total = performance.now() - t0;
    if (!reply.ok) throw new Error(`worker start: ${reply.error ?? 'open failed'}`);
    await new Promise<void>((resolve) => {
      worker.onmessage = () => resolve();
      worker.postMessage({ id: 2, type: 'close' });
    });
    worker.terminate();
    // The files are let go a moment after the worker is: do not time the wait for them.
    await settle(250);
    return { total, inside: reply.wt ?? 0 };
  };
  const samples: Record<'hint' | 'plain', { total: number; inside: number }[]> = { hint: [], plain: [] };
  for (let round = 0; round < 14; round++) {
    for (const kind of round % 2 ? (['hint', 'plain'] as const) : (['plain', 'hint'] as const)) {
      say(`rows: worker start ${round + 1} of 14`);
      const sample = await once(urls[kind]);
      // The first rounds fill the browser's code cache for each file.
      if (round >= 4) samples[kind].push(sample);
    }
  }
  return {
    start_hint: stats(samples.hint.map((s) => s.total)),
    start_plain: stats(samples.plain.map((s) => s.total)),
    start_hint_inside: stats(samples.hint.map((s) => s.inside)),
    start_plain_inside: stats(samples.plain.map((s) => s.inside)),
  };
}

async function runStep(step: Step, results: Results): Promise<void> {
  const mine = (results[step.backend] ??= {});
  if (step.phase === 'start') {
    Object.assign(mine, await workerStart());
    return;
  }
  const backend = BACKENDS[step.backend]();
  if (step.phase === 'warm') {
    say(`${step.backend}: wiping`);
    await wipeAll();
    const data = generate(config.games, config.comments);
    save('bench:ids', pick(data.games, 50, 99));
    if (!results.codec) results.codec = codec(data);
    await backend.open();
    Object.assign(mine, await warm(backend, data));
    await backend.close();
    return;
  }
  const ids = load<string[]>('bench:ids', []);
  if (step.phase === 'boot') {
    // What a real page goes through before its app asks for data: the script
    // is fetched (nothing runs on the main thread), then parsed and run (the
    // main thread is busy). A store started from the page's head has the first
    // of the two to itself; `cold` is the same read without either.
    if (config.idle > 0) await new Promise((resolve) => setTimeout(resolve, config.idle));
    const until = performance.now() + config.boot;
    while (performance.now() < until);
  }
  // A backend that keeps rows to paint from is timed two ways, on alternate
  // loads: until it has those rows, and until the read itself answers with no
  // peek in its way. One load cannot give both: the peek is main-thread work.
  const peeks = backend.peekFirstQuery !== undefined && step.i % 2 === 1;
  const t0 = performance.now();
  const opening = backend.open();
  // What the client does when a query is registered: seed it with the rows the store kept.
  const painted = peeks ? (backend.peekFirstQuery?.(ids) ?? null) : null;
  const tPaint = performance.now();
  await opening;
  const t1 = performance.now();
  const rows = await backend.firstQuery(ids);
  const t2 = performance.now();
  if (rows.length !== ids.length) throw new Error(`${step.backend}: read ${rows.length} of ${ids.length} rows after reopening`);
  const key = step.phase;
  type Sample = { open: number; read: number; total: number; paint: number; start: number; at: number; differs: number; peeked: boolean };
  const samples = load<Sample[]>(`bench:${key}:${step.backend}`, []);
  // The first load of a phase follows another phase's teardown; leave it out.
  // `at` is the time since the navigation started: `performance.now()` counts from there.
  if (step.i > 0) {
    samples.push({
      open: t1 - t0,
      read: t2 - t1,
      total: t2 - t0,
      paint: (painted ? tPaint : t2) - t0,
      start: t0,
      at: t2,
      differs: painted && JSON.stringify(painted) !== JSON.stringify(rows) ? 1 : 0,
      peeked: peeks,
    });
  }
  save(`bench:${key}:${step.backend}`, samples);
  // Without a peek, the rows to paint from are the rows the read answers with.
  const plain = samples.filter((s) => !s.peeked);
  const painting = backend.peekFirstQuery ? samples.filter((s) => s.peeked) : samples;
  if (plain.length > 0) {
    mine[`${key}_open`] = stats(plain.map((s) => s.open));
    mine[`${key}_first_window`] = stats(plain.map((s) => s.read));
    mine[`${key}_total`] = stats(plain.map((s) => s.total));
    // Since the navigation started: when the step began asking, and when it had its rows.
    mine[`${key}_start`] = stats(plain.map((s) => s.start));
    mine[`${key}_at`] = stats(plain.map((s) => s.at));
  }
  if (painting.length > 0) {
    mine[`${key}_paint`] = stats(painting.map((s) => s.paint));
    // How many loads painted rows that the read then corrected.
    mine[`${key}_paint_differs`] = { ms: painting.reduce((n, s) => n + s.differs, 0), note: `of ${painting.length} loads` };
  }
  if (step.phase !== 'reload') {
    await backend.close();
    return;
  }
  // Leave work behind for the next load to find: writes no checkpoint has
  // covered, and a store nobody closed. The query is read once more after
  // them, as the client does when rows on screen change (it re-materializes
  // the query), and then the page goes without a goodbye.
  const rnd = rng(1000 + step.i);
  for (let i = 0; i < 200; i++) await backend.put('game', { id: ids[rnd(ids.length)], favorite: i % 2 === 0, _00_rv: 1000 + i });
  await backend.firstQuery(ids);
}

// ---- the queue -------------------------------------------------------------------

function plan(): Step[] {
  const steps: Step[] = [];
  for (const backend of ORDER) {
    steps.push({ backend, phase: 'warm', i: 0 });
    for (let i = 0; i < COLD_RUNS; i++) steps.push({ backend, phase: 'boot', i });
    for (let i = 0; i < COLD_RUNS; i++) steps.push({ backend, phase: 'cold', i });
    for (let i = 0; i < COLD_RUNS; i++) steps.push({ backend, phase: 'reload', i });
  }
  steps.push({ backend: 'rows', phase: 'start', i: 0 });
  return steps;
}

function clearRun(): void {
  for (const key of Object.keys(localStorage)) {
    if (key.startsWith('bench:') && !['bench:config', 'bench:history', 'bench:label', 'bench:baseline'].includes(key)) localStorage.removeItem(key);
  }
}

const stamp = (at: number): string => new Date(at).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });

/** Keep a finished run, so a later one can be shown against it. */
function remember(results: Results): void {
  const history = load<RunRecord[]>('bench:history', []);
  const at = Date.now();
  history.unshift({ at, label: load('bench:label', '') || stamp(at), config: { ...config }, results });
  save('bench:history', history.slice(0, HISTORY));
  save('bench:shown', at);
}

function show(): void {
  const results = load<Results>('bench:results', {});
  const history = load<RunRecord[]>('bench:history', []);
  const shown = load<number | null>('bench:shown', null);
  const picker = document.getElementById('baseline') as HTMLSelectElement;
  const chosen = load<number | null>('bench:baseline', null);
  // Any kept run but the one on screen.
  const others = history.filter((run) => run.at !== shown);
  picker.innerHTML =
    '<option value="">nothing</option>' +
    others.map((run) => `<option value="${run.at}"${run.at === chosen ? ' selected' : ''}>${run.label.replace(/[<>&"]/g, '')} (${stamp(run.at)})</option>`).join('');
  picker.disabled = others.length === 0;
  const baseline = others.find((run) => run.at === chosen)?.results ?? null;
  render(document.getElementById('results')!, results, baseline);
  (document.getElementById('json') as HTMLTextAreaElement).value = JSON.stringify({ config, userAgent: navigator.userAgent, results }, null, 2);
  (window as unknown as { __bench: unknown }).__bench = { config, results, done: load<Step[]>('bench:queue', []).length === 0 };
}

async function main(): Promise<void> {
  if (new URLSearchParams(location.search).has('leader')) {
    document.body.classList.add('leader');
    await serveAsLeader(say);
    return;
  }
  const input = (id: string): HTMLInputElement => document.getElementById(id) as HTMLInputElement;
  input('games').value = String(config.games);
  input('comments').value = String(config.comments);
  input('boot').value = String(config.boot);
  input('idle').value = String(config.idle);
  input('label').value = load('bench:label', '');
  installTooltip(document.getElementById('results')!, document.getElementById('tip')!);

  document.getElementById('run')!.addEventListener('click', () => {
    save('bench:config', {
      games: Number(input('games').value),
      comments: Number(input('comments').value),
      boot: Number(input('boot').value),
      idle: Number(input('idle').value),
    });
    save('bench:label', input('label').value.trim());
    clearRun();
    save('bench:queue', plan());
    location.reload();
  });
  document.getElementById('reset')!.addEventListener('click', async () => {
    clearRun();
    localStorage.removeItem('bench:history');
    localStorage.removeItem('bench:baseline');
    await wipeAll();
    location.reload();
  });
  document.getElementById('baseline')!.addEventListener('change', (ev) => {
    const value = (ev.target as HTMLSelectElement).value;
    save('bench:baseline', value ? Number(value) : null);
    show();
  });
  document.getElementById('tabs')!.addEventListener('click', async () => {
    const button = document.getElementById('tabs') as HTMLButtonElement;
    button.disabled = true;
    try {
      // `noopener`: a tab of its own, in a process of its own, as a second tab of an app is.
      const tabs = await runTabsTest(config, say, () => void window.open(`${location.pathname}?leader=1`, '_blank', 'noopener'));
      const results = load<Results>('bench:results', {});
      results.tabs = tabs;
      save('bench:results', results);
      // The run on screen is in the history too: keep the two the same.
      const history = load<RunRecord[]>('bench:history', []);
      const current = history.find((run) => run.at === load<number | null>('bench:shown', null));
      if (current) {
        current.results = results;
        save('bench:history', history);
      }
      say('Shared tabs: done.');
      show();
    } catch (error) {
      console.error(error);
      say(`Shared tabs failed: ${String(error)}`);
    } finally {
      button.disabled = false;
    }
  });

  show();
  const queue = load<Step[]>('bench:queue', []);
  if (queue.length === 0) {
    say(Object.keys(load<Results>('bench:results', {})).length > 0 ? 'Done.' : 'Idle.');
    return;
  }
  const results = load<Results>('bench:results', {});
  const step = queue[0];
  say(`${step.backend}: ${step.phase} ${step.i + 1} (${queue.length} steps left)`);
  try {
    await runStep(step, results);
  } catch (error) {
    console.error(error);
    (results[step.backend] ??= {})[`error_${step.phase}`] = { ms: 0, note: String(error) };
    say(`${step.backend} ${step.phase} failed: ${String(error)}`);
    // Skip what is left of a backend whose step failed.
    while (queue.length > 1 && queue[1].backend === step.backend) queue.splice(1, 1);
  }
  save('bench:results', results);
  save('bench:queue', queue.slice(1));
  if (queue.length === 1) remember(results);
  location.reload();
}

void main();
