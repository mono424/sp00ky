/**
 * The arithmetic behind the Audience page. Pure, so it is tested without a
 * browser (test/audienceStats.test.ts).
 */

export interface ActivityPoint {
  at: string;
  users_avg: number;
  users_max: number;
  anon_avg: number;
  anon_max: number;
  views_avg: number;
  views_max: number;
}

export interface ActivityCell {
  /** 1 is Monday, 7 is Sunday, in the reader's time zone. */
  day: number;
  hour: number;
  people: number;
  views: number;
}

export interface QuietWindow {
  day: number;
  hour: number;
  hours: number;
  people: number;
}

export interface ActivitySummary {
  people_avg: number;
  people_peak: number;
  views_avg: number;
  views_peak: number;
  measured: number;
}

export interface ActivityReport {
  step: '15m' | '1h' | '1d';
  series: ActivityPoint[];
  cells: ActivityCell[];
  quiet: QuietWindow[];
  current: ActivitySummary;
  prior: ActivitySummary;
  latest: ActivityPoint | null;
  first_at: string | null;
  tz: string;
}

export const STEP_MS: Record<ActivityReport['step'], number> = {
  '15m': 15 * 60_000,
  '1h': 3_600_000,
  '1d': 86_400_000,
};

export const DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

export const people = (p: ActivityPoint): number => p.users_avg + p.anon_avg;
export const peakPeople = (p: ActivityPoint): number => p.users_max + p.anon_max;

/** Views a person holds open. Unknown with nobody there, rather than zero. */
export function viewsPerPerson(p: ActivityPoint): number | null {
  const n = people(p);
  return n >= 0.5 ? p.views_avg / n : null;
}

/**
 * Each value's average over the `windowMs` before it. The rhythm of a day
 * swamps a chart; this is what is left of the trend. Null until a full window
 * has passed: an average of the first few hours would draw a rise or a fall
 * that never happened.
 */
export function trailingAverage<T extends { at: string }>(points: readonly T[], value: (p: T) => number | null, windowMs: number): (number | null)[] {
  let sum = 0;
  let count = 0;
  let tail = 0;
  return points.map((p, i) => {
    const v = value(p);
    if (v != null) {
      sum += v;
      count += 1;
    }
    const cutoff = Date.parse(p.at) - windowMs;
    while (tail < i && Date.parse(points[tail].at) <= cutoff) {
      const gone = value(points[tail]);
      if (gone != null) {
        sum -= gone;
        count -= 1;
      }
      tail += 1;
    }
    const whole = Date.parse(p.at) - Date.parse(points[0].at) >= windowMs;
    return whole && count && v != null ? sum / count : null;
  });
}

/** A count as it reads: `0.4`, `12`, `3.2K`. */
export function formatCount(n: number): string {
  if (n >= 1000) return `${(n / 1000).toFixed(n >= 10_000 ? 0 : 1)}K`;
  if (n >= 100 || Number.isInteger(n)) return String(Math.round(n));
  return n.toFixed(1);
}

/** The change against an earlier period; null when that period measured nothing. */
export function changePct(current: number, prior: number, priorMeasured: number): number | null {
  if (!priorMeasured || prior <= 0) return null;
  return (current - prior) / prior;
}

/** Runs of consecutive points. A bucket nothing measured breaks the line. */
export function segments<T extends { at: string }>(points: readonly T[], stepMs: number): T[][] {
  const out: T[][] = [];
  let last = -Infinity;
  for (const p of points) {
    const at = Date.parse(p.at);
    if (at - last > stepMs * 2.5 || !out.length) out.push([]);
    out[out.length - 1].push(p);
    last = at;
  }
  return out;
}

export function nearestIndex(points: readonly { at: string }[], ms: number): number {
  if (!points.length) return -1;
  let lo = 0;
  let hi = points.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (Date.parse(points[mid].at) < ms) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && ms - Date.parse(points[lo - 1].at) < Date.parse(points[lo].at) - ms) return lo - 1;
  return lo;
}

/** Whole-number axis ticks from 0 that cover `max`. */
export function countTicks(max: number, count = 4): number[] {
  if (max <= 0) return [0, 1];
  const raw = max / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = Math.max(1, ([1, 2, 5, 10].find((m) => m * mag >= raw) ?? 10) * mag);
  const out: number[] = [];
  for (let v = 0; v < max + step; v += step) out.push(v);
  return out;
}

/** Local midnights between two moments, at most about `maxTicks` of them. */
export function dayTicks(fromMs: number, toMs: number, maxTicks: number): number[] {
  const d = new Date(fromMs);
  d.setHours(24, 0, 0, 0);
  const days = Math.max(1, Math.ceil((toMs - fromMs) / 86_400_000 / Math.max(1, maxTicks)));
  const out: number[] = [];
  while (d.getTime() <= toMs) {
    out.push(d.getTime());
    d.setDate(d.getDate() + days);
  }
  return out;
}

const pad = (n: number) => String(n).padStart(2, '0');

export const cellLabel = (day: number, hour: number): string => `${DAY_NAMES[day - 1]} ${pad(hour)}:00`;

/** `Tue 03:00 to 05:00`, and across midnight `Sun 23:00 to Mon 01:00`. */
export function windowLabel(w: QuietWindow): string {
  const start = (w.day - 1) * 24 + w.hour;
  const end = (start + w.hours) % (7 * 24);
  const endDay = Math.floor(end / 24) + 1;
  const endHour = end % 24;
  return `${cellLabel(w.day, w.hour)} to ${endDay === w.day ? '' : `${DAY_NAMES[endDay - 1]} `}${pad(endHour)}:00`;
}

/** The hours of the week a quiet window covers, as `day-hour` keys. */
export function windowCells(w: QuietWindow): string[] {
  const start = (w.day - 1) * 24 + w.hour;
  return Array.from({ length: w.hours }, (_, i) => {
    const at = (start + i) % (7 * 24);
    return `${Math.floor(at / 24) + 1}-${at % 24}`;
  });
}

/**
 * What the darkest heatmap color stands for: near the busiest hour, so one
 * spike does not wash the rest of the week out.
 */
export function heatMax(cells: readonly ActivityCell[]): number {
  if (!cells.length) return 1;
  const sorted = cells.map((c) => c.people).sort((a, b) => a - b);
  return Math.max(sorted[Math.floor((sorted.length - 1) * 0.97)], 0.001);
}

/** A value's shade from 0 to 1. Square root, so a quiet hour still reads apart from an empty one. */
export const heatLevel = (value: number, max: number): number => Math.sqrt(Math.min(1, Math.max(0, value) / max));
