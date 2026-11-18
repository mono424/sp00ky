/**
 * The benchmark's runner. One backend at a time, each in page loads of its
 * own so no backend runs beside another's worker or wasm heap:
 *
 *   warm    wipe everything, load the dataset, run every scenario in one session
 *   boot    open the store in a fresh page and read one window, as an app does:
 *           after its script arrived (the main thread idle meanwhile) and
 *           ran its startup work (the main thread busy)
 *   cold    the same the moment the script runs, with nothing else to do
 *   reload  the same, after 200 writes and a reload that closed nothing
 *
 * The queue of steps and the results live in localStorage, and the page
 * reloads itself from step to step.
 */
import { CborCodec } from 'surrealdb';
import { BACKENDS, type Backend } from './backends';
import { generate, pick, rng, type Dataset, type Row } from './data';

interface Step {
  backend: string;
  phase: 'warm' | 'cold' | 'boot' | 'reload';
  i: number;
}

interface Metric {
  /** Median of the timed runs, or the one total for a single run. */
  ms: number;
  p95?: number;
  runs?: number;
  note?: string;
}

type Results = Record<string, Record<string, Metric>>;

const ORDER = ['indexeddb', 'sqlite', 'rows'];
// Fresh-page loads per phase. The first is left out; of the rest, every other one peeks (see `runStep`).
const COLD_RUNS = 13;
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

async function runStep(step: Step, results: Results): Promise<void> {
  const backend = BACKENDS[step.backend]();
  const mine = (results[step.backend] ??= {});
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
  const peeks = backend.peek !== undefined && step.i % 2 === 1;
  const t0 = performance.now();
  const opening = backend.open();
  const painted = peeks ? (backend.peek?.('game', ids) ?? null) : null;
  const tPaint = performance.now();
  await opening;
  const t1 = performance.now();
  const rows = await backend.getMany('game', ids);
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
  const painting = backend.peek ? samples.filter((s) => s.peeked) : samples;
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
  // covered, and a store nobody closed. The window is read once more after
  // them, as the client does when rows on screen change (it re-materializes
  // the query), and then the page goes without a goodbye.
  const rnd = rng(1000 + step.i);
  for (let i = 0; i < 200; i++) await backend.put('game', { id: ids[rnd(ids.length)], favorite: i % 2 === 0, _00_rv: 1000 + i });
  await backend.getMany('game', ids);
}

// ---- rendering -----------------------------------------------------------------

const LABELS: [key: string, label: string][] = [
  ['cold_paint', 'Fresh page, asked the moment the script runs: 50 rows to paint from'],
  ['cold_total', ' the read itself (open the store, read the 50 rows)'],
  ['boot_paint', 'Fresh page, after a script download and app startup work: 50 rows to paint from'],
  ['boot_total', ' the read itself'],
  ['reload_paint', 'After 200 writes and a reload that closed nothing: 50 rows to paint from'],
  ['reload_total', ' the read itself'],
  ['reload_paint_differs', ' loads where the painted rows were then corrected'],
  ['window50', 'Read a window of 50 rows by id'],
  ['window50_fields', 'Read 6 fields of 50 rows by id'],
  ['point', 'Read one row by id'],
  ['screen30', 'A screen: 30 games, each with its player and top 3 comments'],
  ['first_screen30', ' first time after loading'],
  ['children30', 'Comments of 30 games (foreign key)'],
  ['where_indexable', 'Filter + sort + limit 50, on a field IndexedDB has an index for'],
  ['where_scan', 'Filter + sort + limit 50, on a field nobody indexes'],
  ['first_where_scan', ' first time after loading'],
  ['versions', 'Read (id, version) of every game'],
  ['ingest', 'Load the whole dataset (sync-down batches)'],
  ['merge250', 'Merge a patch into 250 stored rows'],
  ['put1', 'Merge one row'],
  ['user_write', 'A user write: row + outbox entry, durable'],
  ['window50_after_writes', 'Read a window of 50 rows, after the writes'],
  ['disk_mb', 'Size on disk (MB)'],
];

function fmt(value: number): string {
  if (value >= 1000) return value.toFixed(0);
  if (value >= 100) return value.toFixed(0);
  if (value >= 10) return value.toFixed(1);
  return value.toFixed(2);
}

function render(results: Results): void {
  const root = document.getElementById('results')!;
  const present = ORDER.filter((name) => results[name]);
  let html = '';
  if (present.length > 0) {
    html += `<table><thead><tr><th>median ms <small>(p95)</small></th>${present.map((n) => `<th>${n}</th>`).join('')}</tr></thead><tbody>`;
    for (const [key, label] of LABELS) {
      const cells = present.map((name) => results[name][key]);
      if (cells.every((c) => !c)) continue;
      const least = Math.min(...cells.filter(Boolean).map((c) => c.ms));
      html += `<tr><td>${label}</td>`;
      for (const cell of cells) {
        if (!cell) html += '<td>-</td>';
        else {
          const p95 = cell.p95 !== undefined && key !== 'disk_mb' ? ` <small>(${fmt(cell.p95)})</small>` : '';
          html += `<td class="${cell.ms === least ? 'best' : ''}">${fmt(cell.ms)}${p95}</td>`;
        }
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
  }
  if (results.codec) {
    const c = results.codec;
    html += `<table><thead><tr><th>Body codec, 5000 game rows</th><th>encode ms</th><th>decode ms</th><th>MB</th></tr></thead><tbody>`;
    html += `<tr><td>JSON text, a parse per row</td><td>${fmt(c.json_encode.ms)}</td><td>${fmt(c.json_decode.ms)}</td><td>${fmt(c.json_mb.ms)}</td></tr>`;
    html += `<tr><td>JSON text, one parse for all</td><td>-</td><td>${fmt(c.json_decode_one_parse.ms)}</td><td>-</td></tr>`;
    html += `<tr><td>CBOR (the SurrealDB SDK's codec)</td><td>${fmt(c.cbor_encode.ms)}</td><td>${fmt(c.cbor_decode.ms)}</td><td>${fmt(c.cbor_mb.ms)}</td></tr>`;
    html += '</tbody></table>';
  }
  root.innerHTML = html;
  (document.getElementById('json') as HTMLTextAreaElement).value = JSON.stringify({ config, userAgent: navigator.userAgent, results }, null, 2);
  (window as unknown as { __bench: unknown }).__bench = { config, results, done: load<Step[]>('bench:queue', []).length === 0 };
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
  return steps;
}

function clearRun(): void {
  for (const key of Object.keys(localStorage)) {
    if (key.startsWith('bench:') && key !== 'bench:config') localStorage.removeItem(key);
  }
}

async function main(): Promise<void> {
  const games = document.getElementById('games') as HTMLInputElement;
  const comments = document.getElementById('comments') as HTMLInputElement;
  const boot = document.getElementById('boot') as HTMLInputElement;
  const idle = document.getElementById('idle') as HTMLInputElement;
  idle.value = String(config.idle);
  games.value = String(config.games);
  comments.value = String(config.comments);
  boot.value = String(config.boot);
  document.getElementById('run')!.addEventListener('click', () => {
    save('bench:config', { games: Number(games.value), comments: Number(comments.value), boot: Number(boot.value), idle: Number(idle.value) });
    clearRun();
    save('bench:queue', plan());
    location.reload();
  });
  document.getElementById('reset')!.addEventListener('click', async () => {
    clearRun();
    await wipeAll();
    location.reload();
  });

  const results = load<Results>('bench:results', {});
  render(results);
  const queue = load<Step[]>('bench:queue', []);
  if (queue.length === 0) {
    say(Object.keys(results).length > 0 ? 'Done.' : 'Idle.');
    return;
  }
  const step = queue[0];
  say(`${step.backend}: ${step.phase} ${step.i + 1}`);
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
  location.reload();
}

void main();
