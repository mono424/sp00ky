/**
 * What a retention policy keeps, worked out in the browser so the schedule
 * panel can show it before it is saved.
 *
 * A port of KeepPolicy.Prune in Sp00ky Cloud (internal/backups/retention.go).
 * The tiers run in order (newest, hourly, daily, weekly, monthly, quarterly,
 * yearly). Each keeps the newest backup of each of its next N periods and
 * skips any period an earlier tier already holds a backup of, so the tiers
 * stack: with newest 3 and daily 3 on a nightly schedule, the dailies are the
 * three nights before the newest three. Periods are UTC calendar periods and
 * weeks are ISO weeks (Monday first). A policy of all zeros prunes nothing.
 */

export const TIERS = ['last', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'] as const;
export type Tier = (typeof TIERS)[number];
export type Policy = Record<Tier, number>;

export const EMPTY_POLICY: Policy = { last: 0, hourly: 0, daily: 0, weekly: 0, monthly: 0, quarterly: 0, yearly: 0 };

/** One backup on the timeline, newest first, with the tier it belongs to. */
export interface Mark {
  at: number;
  tier: Tier;
}

export interface Retention {
  /** False for a policy of all zeros: every backup is kept. */
  prunes: boolean;
  /** The backups the policy keeps, newest first. */
  kept: Mark[];
  /**
   * Periods a tier passed over because an earlier tier already holds a backup
   * of them, marked at that backup. They are why a tier reaches further back
   * than its own count suggests.
   */
  held: Mark[];
}

/** At most this many held marks per tier; past it they only pile up. */
const HELD_PER_TIER = 60;

function isoWeek(d: Date): string {
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const year = t.getUTCFullYear();
  const week = Math.ceil(((t.getTime() - Date.UTC(year, 0, 1)) / 86_400_000 + 1) / 7);
  return `${year}-W${week}`;
}

function period(tier: Tier, index: number, d: Date): string {
  switch (tier) {
    case 'last':
      return String(index); // every backup is its own period
    case 'hourly':
      return d.toISOString().slice(0, 13);
    case 'daily':
      return d.toISOString().slice(0, 10);
    case 'weekly':
      return isoWeek(d);
    case 'monthly':
      return d.toISOString().slice(0, 7);
    case 'quarterly':
      return `${d.getUTCFullYear()}-Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
    case 'yearly':
      return String(d.getUTCFullYear());
  }
}

/** What `policy` keeps of `backups` (epoch ms, newest first). */
export function retain(policy: Policy, backups: readonly number[]): Retention {
  const prunes = TIERS.some((t) => policy[t] > 0);
  if (!prunes) return { prunes, kept: backups.map((at) => ({ at, tier: 'last' as Tier })), held: [] };

  const dates = backups.map((t) => new Date(t));
  const keptBy: (Tier | null)[] = backups.map(() => null);
  const held: Mark[] = [];
  for (const tier of TIERS) {
    const want = policy[tier];
    if (want <= 0) continue;
    const covered = new Set<string>();
    for (let i = 0; i < backups.length; i++) if (keptBy[i]) covered.add(period(tier, i, dates[i]!));
    const taken = new Set<string>();
    const passed = new Set<string>();
    for (let i = 0; i < backups.length; i++) {
      if (keptBy[i]) {
        const key = period(tier, i, dates[i]!);
        if (!passed.has(key) && !taken.has(key) && passed.size < HELD_PER_TIER) {
          passed.add(key);
          held.push({ at: backups[i]!, tier });
        }
        continue;
      }
      const key = period(tier, i, dates[i]!);
      if (covered.has(key) || taken.has(key)) continue;
      if (taken.size >= want) break;
      taken.add(key);
      keptBy[i] = tier;
    }
  }
  const kept: Mark[] = [];
  for (let i = 0; i < backups.length; i++) if (keptBy[i]) kept.push({ at: backups[i]!, tier: keptBy[i]! });
  // A held mark only means something inside the stretch its tier walked.
  const oldestOf = new Map<Tier, number>();
  for (const k of kept) oldestOf.set(k.tier, k.at);
  return { prunes, kept, held: held.filter((h) => h.at >= (oldestOf.get(h.tier) ?? Infinity)) };
}

/* ---- the schedule, as a run of backups ------------------------------------ */

/** How often a cron line runs, for the shapes the panel can draw. */
export interface Cadence {
  stepMs: number;
  /** The most recent run at or before `now`. */
  last: number;
}

const HOUR = 3_600_000;
const DAY = 86_400_000;

/**
 * `M * * * *`, `M *\/N * * *`, `M H * * *` and `M H * * D`; anything else is
 * null, and the caller draws a nightly run instead.
 */
export function cadence(cron: string, now: number): Cadence | null {
  const f = cron.trim().split(/\s+/);
  if (f.length !== 5) return null;
  const [mi, h, dom, mon, dow] = f as [string, string, string, string, string];
  const num = (v: string) => (/^\d+$/.test(v) ? Number(v) : null);
  const minute = num(mi);
  if (minute == null || minute > 59 || dom !== '*' || mon !== '*') return null;
  const d = new Date(now);
  const at = (hour: number) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), hour, minute);

  if (dow === '*' && (h === '*' || /^\*\/\d+$/.test(h))) {
    const every = h === '*' ? 1 : Number(h.slice(2));
    if (every < 1 || every > 23) return null;
    let hour = d.getUTCHours() - (d.getUTCHours() % every);
    let last = at(hour);
    while (last > now) {
      hour -= every;
      last = hour >= 0 ? at(hour) : at(Math.floor(23 / every) * every) - DAY;
    }
    // A step that does not divide the day still restarts at midnight; drawing
    // it as even steps is close enough for a preview.
    return { stepMs: every * HOUR, last };
  }
  const hour = num(h);
  if (hour == null || hour > 23) return null;
  let last = at(hour);
  if (last > now) last -= DAY;
  if (dow === '*') return { stepMs: DAY, last };
  const day = num(dow);
  if (day == null || day > 7) return null;
  while (new Date(last).getUTCDay() !== day % 7) last -= DAY;
  return { stepMs: 7 * DAY, last };
}

/** Enough past runs of `c` for every tier of `policy` to fill, newest first. */
export function pastRuns(c: Cadence, policy: Policy): number[] {
  const stepDays = c.stepMs / DAY;
  const horizon = Math.min(
    4000,
    Math.max(
      40,
      policy.last * stepDays + 10,
      policy.hourly / 24 + 10,
      policy.daily + 10,
      policy.weekly * 7 + 20,
      policy.monthly * 31 + 40,
      policy.quarterly * 92 + 100,
      policy.yearly * 366 + 400,
    ),
  );
  const n = Math.min(60_000, Math.floor(horizon / stepDays) + 1);
  return Array.from({ length: n }, (_, i) => c.last - i * c.stepMs);
}

/* ---- words --------------------------------------------------------------- */

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** "13 Nov", or "31 Dec 2025" outside the current year. */
export function shortDate(t: number, now: number): string {
  const d = new Date(t);
  const s = `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]}`;
  return d.getUTCFullYear() === new Date(now).getUTCFullYear() ? s : `${s} ${d.getUTCFullYear()}`;
}

/**
 * A backup named by the period its tier keeps it for: "14:00" (today) or
 * "13 Nov 22:00" for an hourly, "13 Nov" for the newest, a day or a week,
 * "Oct" for a month, "Q4 2025" for a quarter, "2024" for a year. The year is
 * left out inside the current one.
 */
export function periodName(tier: Tier, t: number, now: number): string {
  const d = new Date(t);
  const year = d.getUTCFullYear() === new Date(now).getUTCFullYear() ? '' : ` ${d.getUTCFullYear()}`;
  switch (tier) {
    case 'hourly': {
      const hm = `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
      return new Date(now).toISOString().slice(0, 10) === d.toISOString().slice(0, 10) ? hm : `${shortDate(t, now)} ${hm}`;
    }
    case 'monthly':
      return `${MONTHS[d.getUTCMonth()]}${year}`;
    case 'quarterly':
      return `Q${Math.floor(d.getUTCMonth() / 3) + 1}${year}`;
    case 'yearly':
      return String(d.getUTCFullYear());
    default:
      return shortDate(t, now);
  }
}

/** "3 days", "5 weeks", "11 months", "2.1 years". */
export function spanWords(ms: number): string {
  const days = ms / DAY;
  if (days < 1.5) return `${Math.max(1, Math.round(ms / HOUR))} hours`;
  if (days < 14) return `${Math.round(days)} days`;
  if (days < 62) return `${Math.round(days / 7)} weeks`;
  if (days < 700) return `${Math.round(days / 30.44)} months`;
  return `${(days / 365.25).toFixed(1)} years`;
}
