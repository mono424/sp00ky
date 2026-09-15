/**
 * The last thing the scheduler told us, kept in localStorage so a page paints
 * it the instant it opens instead of after its first round trip.
 *
 * The dashboard is served from the scheduler and authenticates with a bearer
 * token, so the server cannot inline a snapshot into `index.html` for a signed
 * -in operator the way a cookie session would allow. This is the equivalent
 * from the other side: stale state now, live state a poll later, and every
 * reader marks itself `stale` until that poll lands.
 */
import { currentMode } from '../api/client';

interface Entry<T> {
  at: number;
  value: T;
}

function key(name: string): string {
  return `spky.stash:${currentMode().baseUrl || 'embedded'}:${name}`;
}

export function readStash<T>(name: string, maxAgeMs = 6 * 60 * 60 * 1000): Entry<T> | null {
  try {
    const raw = localStorage.getItem(key(name));
    if (!raw) return null;
    const entry = JSON.parse(raw) as Entry<T>;
    if (!entry || typeof entry.at !== 'number') return null;
    if (Date.now() - entry.at > maxAgeMs) return null;
    return entry;
  } catch {
    return null;
  }
}

export function writeStash<T>(name: string, value: T): void {
  try {
    localStorage.setItem(key(name), JSON.stringify({ at: Date.now(), value } satisfies Entry<T>));
  } catch {
    /* quota or privacy mode: painting fresh data next time is fine */
  }
}

export function clearStash(name: string): void {
  try {
    localStorage.removeItem(key(name));
  } catch {
    /* nothing to clear */
  }
}
