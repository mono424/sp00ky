import 'dart:math';

/// Outbox mutation ids.
///
/// The shape is sortable AND collision-free:
///
/// ```text
/// <13-digit zero-padded ms timestamp>_<4-digit base36 seq>_<clientId>
/// ```
///
/// Lexicographic order equals chronological order, so ordering the drain by id
/// is meaningful. The previous `_00_pending_mutations:${now}` collided whenever
/// two mutations landed in the same millisecond.
const String pendingTable = '_00_pending_mutations';

int _seq = 0;

/// Client identity for mutation ids, minted once per process.
final String _fallbackClientId =
    Random().nextInt(1 << 32).toRadixString(36).padLeft(6, '0').substring(0, 6);

String mintMutationId([String? clientId]) {
  final ts = DateTime.now().millisecondsSinceEpoch.toString().padLeft(13, '0');
  _seq = (_seq + 1) % 1679616; // 36^4
  final n = _seq.toRadixString(36).padLeft(4, '0');
  return '$pendingTable:${ts}_${n}_${clientId ?? _fallbackClientId}';
}

/// The client that created the mutation, or null for a legacy numeric id.
String? mutationOwnerClientId(String mutationId) {
  final raw = mutationId.startsWith('$pendingTable:')
      ? mutationId.substring('$pendingTable:'.length)
      : mutationId;
  final parts = raw.split('_');
  return parts.length >= 3 ? parts.last : null;
}
