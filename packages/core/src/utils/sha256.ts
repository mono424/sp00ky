/**
 * Lowercase hex SHA-256 of a UTF-8 string, through WebCrypto.
 *
 * Pure and dependency free so it can live in `@spooky-sync/core/pure` and run
 * in a service worker, a dedicated worker or Node (`globalThis.crypto` is
 * there on Node 19+). Query keys are `sha256Hex(JSON.stringify(..))`, see
 * `query/hash.ts`.
 */
export async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}
