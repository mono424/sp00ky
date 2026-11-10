//! The id-set a query renders from.
//!
//! PORT: `packages/spooky_core/lib/src/query/render_set.dart`.

use std::collections::BTreeSet;

use crate::state::lifecycle::QueryPhase;
use crate::state::selectors::Overlay;
use crate::types::RecordVersionArray;

/// The membership a query renders.
///
/// Divergence from the TypeScript core, where a cold non-window query falls
/// back to a local predicate scan, re-running its own SurrealQL against the
/// local SurrealDB. sqlite cannot run SurrealQL, so a cold query renders the
/// circuit's local window instead, which is the same set once the circuit has
/// been primed from the local store. A query rendering from the local window is
/// still `cold`, so it is not authoritative and a binding shows its loader
/// until the server answers.
pub fn resolve_membership<'a>(
    phase: QueryPhase,
    remote_array: &'a RecordVersionArray,
    local_array: &'a RecordVersionArray,
) -> &'a RecordVersionArray {
    if phase == QueryPhase::Cold {
        local_array
    } else {
        remote_array
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RenderOptions {
    pub has_explicit_order: bool,
    pub is_window: bool,
}

/// ```text
/// render = (membership ∪ (writes ∩ local_view)) − deletes
/// ```
///
/// `writes`/`deletes` are the outbox overlay (pending and acked items). The
/// middle term keeps optimistic writes visible without re-admitting stale
/// rows: `local_view` (the circuit's id-set) says whether the written row
/// matches the query per local truth. Sorted unless the query orders itself or
/// is a window, whose id order is the slice order.
pub fn build_render_ids(
    membership: &RecordVersionArray,
    local_view: &RecordVersionArray,
    overlay: &Overlay,
    opts: RenderOptions,
) -> Vec<String> {
    let mut ordered = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (id, _) in membership {
        if overlay.deletes.contains(id) || !seen.insert(id) {
            continue;
        }
        ordered.push(id.clone());
    }
    if !overlay.writes.is_empty() {
        for (id, _) in local_view {
            if !overlay.writes.contains(id) || overlay.deletes.contains(id) || !seen.insert(id) {
                continue;
            }
            ordered.push(id.clone());
        }
    }
    if !opts.has_explicit_order && !opts.is_window {
        ordered.sort();
    }
    ordered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arr(ids: &[&str]) -> RecordVersionArray {
        ids.iter().map(|id| (id.to_string(), 1)).collect()
    }

    fn ov(writes: &[&str], deletes: &[&str]) -> Overlay {
        Overlay {
            writes: writes.iter().map(|s| s.to_string()).collect(),
            deletes: deletes.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn cold_renders_the_local_window_anything_else_membership() {
        let remote = arr(&["t:1"]);
        let local = arr(&["t:2"]);
        assert_eq!(
            resolve_membership(QueryPhase::Cold, &remote, &local),
            &local
        );
        assert_eq!(
            resolve_membership(QueryPhase::Cached, &remote, &local),
            &remote
        );
        assert_eq!(
            resolve_membership(QueryPhase::ViewLost, &remote, &local),
            &remote
        );
        assert_eq!(
            resolve_membership(QueryPhase::Live, &remote, &local),
            &remote
        );
    }

    #[test]
    fn membership_minus_deletes_sorted() {
        let ids = build_render_ids(
            &arr(&["t:b", "t:a", "t:c"]),
            &vec![],
            &ov(&[], &["t:c"]),
            RenderOptions::default(),
        );
        assert_eq!(ids, ["t:a", "t:b"]);
    }

    #[test]
    fn adds_writes_the_local_view_admits_never_twice_never_deleted() {
        let ids = build_render_ids(
            &arr(&["t:1"]),
            &arr(&["t:1", "t:2", "t:3", "t:4"]),
            &ov(&["t:1", "t:2", "t:3", "t:9"], &["t:3"]),
            RenderOptions::default(),
        );
        assert_eq!(ids, ["t:1", "t:2"]);
    }

    #[test]
    fn keeps_server_order_for_windows_and_explicit_order_and_dedupes() {
        let m = arr(&["t:b", "t:a", "t:b"]);
        let explicit = RenderOptions {
            has_explicit_order: true,
            is_window: false,
        };
        let window = RenderOptions {
            has_explicit_order: false,
            is_window: true,
        };
        assert_eq!(
            build_render_ids(&m, &vec![], &ov(&[], &[]), explicit),
            ["t:b", "t:a"]
        );
        assert_eq!(
            build_render_ids(&m, &vec![], &ov(&[], &[]), window),
            ["t:b", "t:a"]
        );
    }
}
