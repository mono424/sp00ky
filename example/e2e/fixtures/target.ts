/**
 * What the suite runs against. Local dev by default; a deployed project when
 * E2E_BASE_URL is set (then no local servers start), with the database the
 * tests inspect and clean given by E2E_DB_URL (the `/sql` endpoint),
 * E2E_DB_NS, E2E_DB_DB and E2E_DB_AUTH (a full Authorization header value),
 * and the sync node by E2E_SSP_URL.
 */
// The package has no Node types; Playwright runs this under Node.
declare const process: { env: Record<string, string | undefined> };

export const BASE_URL = process.env.E2E_BASE_URL || 'http://localhost:3006';
export const REMOTE = !!process.env.E2E_BASE_URL;

export const UPSTREAM = {
  url: process.env.E2E_DB_URL || 'http://localhost:8666/sql',
  ns: process.env.E2E_DB_NS || 'main',
  db: process.env.E2E_DB_DB || 'example',
  authorization: process.env.E2E_DB_AUTH || 'Basic ' + btoa('root:root'),
};

/** The database's origin, for `/signup` and `/sql` built by hand. */
export const UPSTREAM_ORIGIN = UPSTREAM.url.replace(/\/sql$/, '');

/** The sync node (SSP), whose `/info` is public. */
export const SSP_URL = process.env.E2E_SSP_URL || 'http://localhost:8667';

/** Headers for a root `/sql` request against the upstream database. */
export function upstreamHeaders(): Record<string, string> {
  return {
    Accept: 'application/json',
    'surreal-ns': UPSTREAM.ns,
    'surreal-db': UPSTREAM.db,
    Authorization: UPSTREAM.authorization,
  };
}
