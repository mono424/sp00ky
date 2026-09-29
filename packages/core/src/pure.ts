/**
 * `@spooky-sync/core/pure`: the side-effect-free builders of the sync protocol.
 *
 * Everything here is plain functions and constants over `surrealdb` value
 * types: no wasm, no logger, no `window`/`document`/`localStorage`. Safe to
 * import from a service worker, a dedicated worker or Node. The full client
 * imports the same modules, so what this entry builds is byte for byte what
 * `Sp00kyClient` sends.
 */
import { queryHashInput, type QueryKeyInput } from './query/hash';
import { sha256Hex } from './utils/sha256';

// Statement builders: register + snapshot, heartbeat, bodies, views.
export * from './query/sql';
// Query keys.
export { queryHashInput, viewKeyInput, type QueryKeyInput } from './query/hash';
// `_00_list_ref` table naming and user-id sanitizing (mirrors ssp-protocol).
export {
  ANON_USER_ID,
  DEFAULT_REF_MODE,
  bucketIdForUser,
  listRefTableFor,
  sanitizeUserId,
  type RefMode,
} from './modules/ref-tables';
// Membership snapshot parsing (the answer to the register read-back).
export {
  FAILED,
  dedupeRecordVersions,
  metaFromRow,
  snapshotFromSingle,
  type Answer,
  type ListRefSnapshot,
  type QueryMetaRow,
} from './query/membership';
// LIVE edge decoding.
export { hashOfEdge, rowOfEdge } from './utils/edges';
// SurrealQL string helpers.
export {
  surql,
  type SealOptions,
  type SealedQuery,
  type SurqlHelper,
  type TxQuery,
} from './utils/surql';
// Param typing the full client applies before registering.
export { parseQueryParams } from './utils/parser';
export { encodeRecordId, parseRecordIdString, parseDuration } from './utils/index';
// Unverified JWT claim reading (user id, access method, impersonation).
export {
  decodeTokenClaims,
  IMPERSONATION_ACCESS,
  type TokenClaims,
} from './modules/auth/impersonation';
export { sha256Hex } from './utils/sha256';
// Web Push wire types.
export * from './push/types';

/**
 * The remote `_00_query` key of a query for one session salt: what the full
 * client names its view (`_00_query:<key>`).
 */
export function queryKey(input: QueryKeyInput, sessionId: string | null): Promise<string> {
  return sha256Hex(queryHashInput(input, sessionId));
}
