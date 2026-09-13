/// Compatibility shim while the imperative sync engine is still in the tree.
///
/// The policy functions moved to `lib/src/sync/policy.dart` (mirroring
/// `packages/core/src/sync/policy.ts`) and the statement builders to
/// `lib/src/query/sql.dart`. This file goes away with `sync.dart`.
library;

export '../../query/sql.dart' show listRefSelect, subqueryListRefSelect;
export '../../sync/policy.dart';
