/**
 * Value formatters.
 *
 * `formatRelativeTime`, `formatDuration`, `formatMs` and `formatBytes` are
 * copied verbatim in behaviour from `apps/devtools/src/utils/formatters.ts`.
 * That app is a Chrome extension with no package exports, so a workspace
 * import is not available; these are small, stable functions and the two
 * surfaces should read the same to an operator looking at both.
 */

/**
 * Distance from now, in either direction.
 *
 * Future timestamps matter here in a way they do not in the DevTools panel this
 * was adapted from: `next_fire_at` on a schedule is always ahead of now, and
 * rendering it as "just now" says the opposite of the truth.
 */
export function formatRelativeTime(timestamp: number): string {
  const diff = Date.now() - timestamp;
  const future = diff < 0;
  const seconds = Math.floor(Math.abs(diff) / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  const amount =
    days > 0
      ? `${days}d`
      : hours > 0
        ? `${hours}h`
        : minutes > 0
          ? `${minutes}m`
          : seconds > 0
            ? `${seconds}s`
            : null;

  if (amount === null) return 'just now';
  return future ? `in ${amount}` : `${amount} ago`;
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = seconds / 60;
  if (minutes < 60) return `${minutes.toFixed(1)}m`;
  const hours = minutes / 60;
  if (hours < 24) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

/** Uptime and phase durations arrive as whole seconds. */
export function formatUptime(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) {
    return '—';
  }
  return formatDuration(seconds * 1000);
}

export function formatMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return '—';
  if (ms < 1) return `${ms.toFixed(2)}ms`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/** An update rate in updates per second: `125/s`, `4.9/s`, `—`. */
export function formatRate(perSec: number | null | undefined): string {
  if (perSec === null || perSec === undefined || !Number.isFinite(perSec)) return '—';
  if (perSec === 0) return '0/s';
  return perSec >= 10 ? `${Math.round(perSec).toLocaleString()}/s` : `${perSec.toFixed(1)}/s`;
}

/** Split a formatted measurement so the unit can be styled down. */
export function splitValue(text: string): { value: string; unit: string } {
  const m = /^([\d.,]+)(\D+)$/.exec(text);
  return m ? { value: m[1]!, unit: m[2]! } : { value: text, unit: '' };
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || !Number.isFinite(bytes)) return '—';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = bytes / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

export function formatCount(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return '—';
  return n.toLocaleString();
}

export function formatClock(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number, w = 2) => n.toString().padStart(w, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(
    d.getMilliseconds(),
    3,
  )}`;
}

/**
 * An absent `option<...>` selected through `type::string()` comes back as the
 * literal string "NONE", not null.
 *
 * This bites anywhere a nullable column is stringified in SurrealQL: not just
 * timestamps, but record links too — an unset `workflow_run` arrives as the
 * truthy string "NONE", which silently turns "no run to open" into a dead link.
 * Every check against such a column must go through here.
 */
export function isAbsent(value: string | null | undefined): boolean {
  return !value || value === 'NONE';
}

/** The value, or `null` when SurrealQL says it is absent. */
export function orNull(value: string | null | undefined): string | null {
  return isAbsent(value) ? null : value!;
}

/** ISO timestamps come off SurrealDB; render them, or an em dash for null. */
export function formatStamp(iso: string | null | undefined): string {
  if (isAbsent(iso)) return '—';
  const t = Date.parse(iso!);
  if (Number.isNaN(t)) return iso!;
  return new Date(t).toLocaleString();
}

export function relativeStamp(iso: string | null | undefined): string {
  if (isAbsent(iso)) return '—';
  const t = Date.parse(iso!);
  if (Number.isNaN(t)) return iso!;
  return formatRelativeTime(t);
}

/** Elapsed time between two ISO stamps, or from the start until now. */
export function elapsed(
  startIso: string | null | undefined,
  endIso: string | null | undefined,
): string {
  if (isAbsent(startIso)) return '—';
  const start = Date.parse(startIso!);
  if (Number.isNaN(start)) return '—';
  const end = isAbsent(endIso) ? Date.now() : Date.parse(endIso!);
  if (Number.isNaN(end)) return '—';
  return formatDuration(Math.max(0, end - start));
}

/**
 * Read a path parameter.
 *
 * `@solidjs/router` hands back the RAW path segment, not a decoded one, so a
 * record id like `_00_workflow_run:ingest_1` — which links must encode, because
 * of the `:` — arrives here still percent-encoded. Passing that straight to
 * `encodeURIComponent` when building the API URL encodes the `%` a second time
 * and the lookup 404s.
 */
export function decodeParam(raw: string | undefined): string {
  if (!raw) return '';
  try {
    return decodeURIComponent(raw);
  } catch {
    // Malformed escape sequence: use it as-is rather than throwing out of a
    // component's render.
    return raw;
  }
}
