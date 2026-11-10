/**
 * The log explorer's arithmetic, free of Solid and the DOM so it runs (and is
 * tested) on its own: the time columns of the histogram, binary search into a
 * buffer kept sorted by time, merging lines from two sources without doubling
 * the ones both carry, and the labels the page reads them out with.
 *
 * Every window here is half-open, `[t0, t1)` in epoch milliseconds.
 */

export interface Line {
  ts: number;
  level: string;
  target: string;
  message: string;
  fields?: string;
}

export interface Win {
  t0: number;
  t1: number;
}

/** Column widths the histogram may use, finest first: round numbers of ms, s, min, h, d. */
export const STEPS = [
  10, 20, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000, 600_000,
  900_000, 1_800_000, 3_600_000, 7_200_000, 10_800_000, 21_600_000, 43_200_000, 86_400_000,
];

const DAY = 86_400_000;

/** The time columns of a histogram: `cols` of `step` ms each from `t0`. */
export interface Frame {
  t0: number;
  step: number;
  cols: number;
}

export const frameEnd = (f: Frame) => f.t0 + f.cols * f.step;

/**
 * Columns for a window, as many as fit `maxCols` at the finest round step.
 *
 * `align` (the live view) puts the edges on multiples of the step in local
 * time, so a column keeps its place and its height while the tail grows and
 * only the last one fills. Without it (a zoomed window) the first column
 * starts exactly at `t0`, so the window the reader picked is the one drawn.
 */
export function frameFor(w: Win, maxCols: number, align: number | null): Frame {
  const span = Math.max(1, w.t1 - w.t0);
  const max = Math.max(1, maxCols);
  for (const step of STEPS) {
    if (span / step > max) continue;
    if (align === null) return { t0: w.t0, step, cols: Math.max(1, Math.ceil(span / step)) };
    const t0 = Math.floor((w.t0 + align) / step) * step - align;
    const cols = Math.max(1, Math.ceil((w.t1 - t0) / step));
    if (cols <= max) return { t0, step, cols };
  }
  const step = Math.ceil(span / max / DAY) * DAY;
  const t0 = align === null ? w.t0 : Math.floor((w.t0 + align) / step) * step - align;
  return { t0, step, cols: Math.max(1, Math.ceil((w.t1 - t0) / step)) };
}

/** Index of the first line at or after `t` in lines sorted by time. */
export function lowerBound(lines: readonly { ts: number }[], t: number): number {
  let lo = 0;
  let hi = lines.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (lines[mid].ts < t) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** The `[lo, hi)` index range of the lines inside a window. */
export function span(lines: readonly { ts: number }[], w: Win | null): [number, number] {
  return w ? [lowerBound(lines, w.t0), lowerBound(lines, w.t1)] : [0, lines.length];
}

export interface Columns {
  all: Uint32Array;
  warn: Uint32Array;
  error: Uint32Array;
  /** The tallest column, the height everything is drawn against. */
  hi: number;
}

/** Count `lines[lo, hi)` into the frame's columns, warnings and errors apart. */
export function count(lines: readonly Line[], lo: number, hi: number, f: Frame): Columns {
  const all = new Uint32Array(f.cols);
  const warn = new Uint32Array(f.cols);
  const error = new Uint32Array(f.cols);
  const last = f.cols - 1;
  for (let i = lo; i < hi; i++) {
    const l = lines[i];
    const c = Math.min(last, Math.max(0, Math.floor((l.ts - f.t0) / f.step)));
    all[c]++;
    if (l.level === 'ERROR') error[c]++;
    else if (l.level === 'WARN') warn[c]++;
  }
  let top = 0;
  for (let c = 0; c < f.cols; c++) if (all[c] > top) top = all[c];
  return { all, warn, error, hi: top };
}

/* ------------------------------------------------------------------ */
/* the buffer: sorted by time, fed by two sources that overlap         */
/* ------------------------------------------------------------------ */

const ident = (l: Line) => `${l.level}\u0000${l.target}\u0000${l.message}\u0000${l.fields ?? ''}`;

/** The lines `buf` holds at instant `ts` (at one of its ends), counted by identity. */
function heldAt(buf: readonly Line[], ts: number, from: 'start' | 'end'): Map<string, number> {
  const held = new Map<string, number>();
  if (from === 'end') for (let i = buf.length - 1; i >= 0 && buf[i].ts === ts; i--) held.set(ident(buf[i]), (held.get(ident(buf[i])) ?? 0) + 1);
  else for (let i = 0; i < buf.length && buf[i].ts === ts; i++) held.set(ident(buf[i]), (held.get(ident(buf[i])) ?? 0) + 1);
  return held;
}

/** True, and one fewer left, when `held` still has an identical line. */
function take(held: Map<string, number>, l: Line): boolean {
  const k = ident(l);
  const n = held.get(k);
  if (!n) return false;
  held.set(k, n - 1);
  return true;
}

/** Put `l` in its place from the end: lines arrive in order, give or take a millisecond. */
function insert(buf: Line[], l: Line) {
  let i = buf.length;
  buf.push(l);
  while (i > 0 && buf[i - 1].ts > l.ts) {
    buf[i] = buf[i - 1];
    i--;
  }
  buf[i] = l;
}

/**
 * Append what `incoming` has past the end of `buf`, in place; returns how many.
 *
 * `incoming` may repeat lines `buf` already holds: the live socket resends its
 * whole tail every tick, and a stream opened after it backfills what the
 * socket already delivered. Anything before `buf`'s last instant is taken as
 * held; at that instant, a line counts as new only once `buf`'s identical
 * lines there are used up, so two real lines in one millisecond both stay.
 */
export function appendNew(buf: Line[], incoming: readonly Line[]): number {
  if (!buf.length) {
    for (const l of incoming) insert(buf, l);
    return incoming.length;
  }
  const last = buf[buf.length - 1].ts;
  let start = incoming.length;
  while (start > 0 && incoming[start - 1].ts >= last) start--;
  if (start === incoming.length) return 0;
  const held = heldAt(buf, last, 'end');
  let added = 0;
  for (let i = start; i < incoming.length; i++) {
    const l = incoming[i];
    if (l.ts === last && take(held, l)) continue;
    insert(buf, l);
    added++;
  }
  return added;
}

/**
 * `older` in front of `buf`: the history fetched once, behind what the socket
 * has already delivered. Only what lies before `buf`'s first instant is new
 * (plus lines at that instant `buf` lacks); the rest `buf` already holds.
 */
export function prependOlder(buf: readonly Line[], older: readonly Line[]): Line[] {
  if (!buf.length) {
    const out: Line[] = [];
    for (const l of older) insert(out, l);
    return out;
  }
  const first = buf[0].ts;
  const held = heldAt(buf, first, 'start');
  const head: Line[] = [];
  for (const l of older) {
    if (l.ts < first || (l.ts === first && !take(held, l))) insert(head, l);
  }
  return head.length ? head.concat(buf) : (buf as Line[]);
}

/* ------------------------------------------------------------------ */
/* reading one line                                                    */
/* ------------------------------------------------------------------ */

/**
 * `key=value` pairs from a line's fields. The ring renders them space
 * separated, and a value may itself hold spaces (a `Display` string), so a
 * pair runs until the next `word=` after a space. Debug-quoted strings lose
 * their quotes.
 */
export function parseFields(fields: string | undefined): [string, string][] {
  if (!fields) return [];
  const keys: { k: string; at: number; end: number }[] = [];
  const re = /([A-Za-z_][\w.:-]*)=/g;
  for (let m = re.exec(fields); m; m = re.exec(fields)) {
    if (m.index === 0 || /\s/.test(fields[m.index - 1])) keys.push({ k: m[1], at: m.index, end: re.lastIndex });
  }
  if (!keys.length) return [['', fields.trim()]];
  return keys.map((key, i) => {
    let v = fields.slice(key.end, i + 1 < keys.length ? keys[i + 1].at : undefined).trim();
    if (v.length > 1 && v.startsWith('"') && v.endsWith('"')) {
      try {
        v = JSON.parse(v);
      } catch {
        /* not JSON-quoted after all; keep it as written */
      }
    }
    return [key.k, v];
  });
}

/**
 * A message with its particulars taken out, so "applied 96 events in 24ms"
 * and "applied 142 events in 38ms" read as the same thing happening again:
 * hex ids and numbers become `#`.
 */
export function shape(message: string): string {
  return message.replace(/\b(?=[0-9a-f]*\d)[0-9a-f]{6,}\b/gi, '#').replace(/\d+(?:[.,]\d+)*/g, '#');
}

/* ------------------------------------------------------------------ */
/* labels                                                              */
/* ------------------------------------------------------------------ */

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const p2 = (n: number) => n.toString().padStart(2, '0');

/** A local clock: `HH:MM`, `HH:MM:SS` or `HH:MM:SS.mmm`. */
export function clock(ms: number, unit: 'm' | 's' | 'ms' = 's'): string {
  const d = new Date(ms);
  const hm = `${p2(d.getHours())}:${p2(d.getMinutes())}`;
  if (unit === 'm') return hm;
  const s = `${hm}:${p2(d.getSeconds())}`;
  return unit === 's' ? s : `${s}.${d.getMilliseconds().toString().padStart(3, '0')}`;
}

export const day = (ms: number) => {
  const d = new Date(ms);
  return `${MONTHS[d.getMonth()]} ${d.getDate()}`;
};

const sameDay = (a: number, b: number) => new Date(a).toDateString() === new Date(b).toDateString();

/** The unit a window is read in: milliseconds for a few seconds, minutes for half a day. */
const unitFor = (spanMs: number): 'm' | 's' | 'ms' => (spanMs < 10_000 ? 'ms' : spanMs < 6 * 3_600_000 ? 's' : 'm');

/** A window as the pill and the chip read it: `09:21:40 – 09:27:10`, a date only once it crosses midnight. */
export function rangeLabel(w: Win): string {
  // The end is exclusive; the last instant inside is one ms before it.
  const end = Math.max(w.t0, w.t1 - 1);
  const unit = unitFor(w.t1 - w.t0);
  if (sameDay(w.t0, end)) return `${clock(w.t0, unit)} – ${clock(end, unit)}`;
  return `${day(w.t0)} ${clock(w.t0, 'm')} – ${day(end)} ${clock(end, 'm')}`;
}

/** An axis label for an instant on a frame of this step. */
export function tick(ms: number, step: number): string {
  if (step >= DAY) return day(ms);
  return clock(ms, step < 1_000 ? 'ms' : step < 60_000 ? 's' : 'm');
}

/** How long a window lasts: `850ms`, `4.2s`, `19m 55s`, `2h 05m`, `3d 4h`. */
export function lasting(ms: number): string {
  if (ms < 1_000) return `${Math.round(ms)}ms`;
  if (ms < 10_000) return `${(ms / 1_000).toFixed(1)}s`;
  const s = Math.round(ms / 1_000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${p2(s % 60)}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${p2(m % 60)}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}
