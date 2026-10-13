import type * as Loro from 'loro-crdt';

/**
 * Which edit-log rows may enter a document.
 *
 * Every client and the SSP fold (`packages/ssp-node/src/crdt/acceptance.rs`)
 * apply this exact rule before importing a row, so they import the same rows
 * and converge on the same document: a row one side accepted and another
 * refused would leave them different forever (a CRDT never forgets an op).
 *
 * - the blob decodes as a Loro **update** (a snapshot could smuggle in
 *   arbitrary history from any peer);
 * - it is no larger than the column allows;
 * - every op in it was made by the row's `peer`, which the database already
 *   checked belongs to the row's author (`_00_crdt_peer`).
 *
 * Loro's decoder can trap on crafted input with a valid checksum, so a raw
 * header check rejects non-update blobs before decoding, and decoding runs
 * under `try`.
 */

export type Rejection =
  | 'empty'
  | 'too_large'
  | 'bad_peer'
  | 'header'
  | 'undecodable'
  | 'not_an_update'
  | 'no_ops'
  | 'foreign_peer';

export interface Accepted {
  peer: `${number}`;
  /** Exclusive end counter of the row's ops for `peer`. */
  end: number;
}

export type Verdict = { ok: true; accepted: Accepted } | { ok: false; reason: Rejection };

/** The largest blob a row may carry (the generated column's cap). */
export const MAX_DELTA_BYTES = 256 * 1024;

const PEER = /^\d{1,20}$/;
/** `"loro"`: every Loro blob starts with it. */
const MAGIC = [0x6c, 0x6f, 0x72, 0x6f];
/** Header bytes 20..22 (big-endian) carry the encoding mode; 4 is an update. */
const MODE_UPDATES = 4;

export function check(
  loro: Pick<typeof Loro, 'decodeImportBlobMeta'>,
  blob: Uint8Array | undefined,
  peer: unknown,
  maxBytes = MAX_DELTA_BYTES
): Verdict {
  if (!blob || blob.length === 0) return { ok: false, reason: 'empty' };
  if (blob.length > maxBytes) return { ok: false, reason: 'too_large' };
  if (typeof peer !== 'string' || !PEER.test(peer)) return { ok: false, reason: 'bad_peer' };
  if (blob.length < 22 || MAGIC.some((b, i) => blob[i] !== b)) return { ok: false, reason: 'header' };
  if (((blob[20] << 8) | blob[21]) !== MODE_UPDATES) return { ok: false, reason: 'not_an_update' };
  let meta: ReturnType<typeof Loro.decodeImportBlobMeta>;
  try {
    meta = loro.decodeImportBlobMeta(blob, true);
  } catch {
    return { ok: false, reason: 'undecodable' };
  }
  if (meta.mode !== 'update') return { ok: false, reason: 'not_an_update' };
  const start = meta.partialStartVersionVector.toJSON();
  const authors: Array<[string, number]> = [];
  for (const [p, end] of meta.partialEndVersionVector.toJSON()) {
    if (end > (start.get(p) ?? 0)) authors.push([String(p), end]);
  }
  if (authors.length === 0) return { ok: false, reason: 'no_ops' };
  if (authors.length > 1 || BigInt(authors[0][0]) !== BigInt(peer)) return { ok: false, reason: 'foreign_peer' };
  return { ok: true, accepted: { peer: peer as `${number}`, end: authors[0][1] } };
}
