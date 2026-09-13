/// Client-side mirror of `ssp-protocol`'s `list_ref_table_for`. Same naming so
/// the LIVE subscription, the initial fetch, and the SSP writes land on the
/// same table. Faithful port of `modules/ref-tables.ts`.

enum RefMode { single, dedicated }

/// Default ref-storage mode (mirrors the SSP default `RefMode::Dedicated`).
const RefMode defaultRefMode = RefMode.dedicated;

/// Sentinel user id for unauthenticated clients when anonymous live queries are
/// enabled. Mirrors `ssp_protocol::ANON_AUTH_ID` and the TS `ANON_USER_ID`. It
/// carries no `user:` prefix so it can never collide with a real user id (those
/// arrive as `user:<id>`); both sides resolve it to the shared
/// `_00_list_ref_anon` table.
const String anonUserId = 'anon';

final _userIdPattern = RegExp(r'^[A-Za-z0-9_]+$');

/// Sanitize a user id (`"user:abc"` -> `"abc"`). Returns null when the id is
/// empty or contains characters invalid in a SurrealDB table identifier.
String? sanitizeUserId(Object? userId) {
  if (userId == null) return null;
  final asString = userId.toString();
  if (asString.isEmpty) return null;
  final raw = asString.startsWith('user:')
      ? asString.substring('user:'.length)
      : asString;
  if (raw.isEmpty) return null;
  if (!_userIdPattern.hasMatch(raw)) return null;
  return raw;
}

/// `_00_list_ref` table name for `(mode, userId)`. Falls back to the global
/// table in single mode or when sanitization fails.
String listRefTableFor(RefMode mode, Object? userId) {
  // Anonymous clients (flag-enabled) share one dedicated table in both modes —
  // checked before the mode split so it never lands on the per-user or the
  // auth-gated global table. Matches `ssp_protocol::list_ref_table_for`.
  if (userId == anonUserId) return '_00_list_ref_anon';
  if (mode == RefMode.single) return '_00_list_ref';
  final uid = sanitizeUserId(userId);
  return uid != null ? '_00_list_ref_user_$uid' : '_00_list_ref';
}

/// cyrb53, the hash the TypeScript client uses for a bucket id that fails
/// sanitization. Ported so the two clients name the same bucket for the same
/// principal.
///
/// Every intermediate is kept as an UNSIGNED 32-bit value. JavaScript's `>>>`
/// coerces to uint32 before shifting, so a Dart port that leaves values signed
/// (`int.toSigned(32)`) produces different digests for the same input.
int cyrb53(String str, [int seed = 0]) {
  var h1 = (0xdeadbeef ^ seed) & _mask32;
  var h2 = (0x41c6ce57 ^ seed) & _mask32;
  for (var i = 0; i < str.length; i++) {
    final ch = str.codeUnitAt(i);
    h1 = _imul(h1 ^ ch, 2654435761);
    h2 = _imul(h2 ^ ch, 1597334677);
  }
  h1 = _imul(h1 ^ (h1 >> 16), 2246822507);
  h1 = (h1 ^ _imul(h2 ^ (h2 >> 13), 3266489909)) & _mask32;
  h2 = _imul(h2 ^ (h2 >> 16), 2246822507);
  h2 = (h2 ^ _imul(h1 ^ (h1 >> 13), 3266489909)) & _mask32;
  return 4294967296 * (2097151 & h2) + h1;
}

const int _mask32 = 0xFFFFFFFF;

/// JavaScript `Math.imul` over unsigned 32-bit operands. Multiplication is the
/// same modulo 2^32 whether the operands are read as signed or unsigned, so
/// masking the product is enough.
int _imul(int a, int b) => ((a & _mask32) * (b & _mask32)) & _mask32;

/// The local store a principal owns.
///
/// An id that fails sanitization still gets a DETERMINISTIC per-user bucket
/// (the cyrb53 hex of the raw id); falling back to `anon` here would put an
/// authenticated user in the shared bucket and recreate the cross-user leak.
String bucketIdForUser(Object? userId) {
  if (userId == null || userId == anonUserId) return anonUserId;
  final uid = sanitizeUserId(userId);
  if (uid != null) return uid;
  return 'u${cyrb53(userId.toString()).toRadixString(16)}';
}
