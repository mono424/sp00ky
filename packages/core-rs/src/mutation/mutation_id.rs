//! Outbox mutation ids.
//!
//! The shape is sortable AND collision-free:
//!
//! ```text
//! <13-digit zero-padded ms timestamp>_<4-digit base36 seq>_<clientId>
//! ```
//!
//! Lexicographic order equals chronological order, so ordering the drain by id
//! is meaningful. The previous `_00_pending_mutations:${now}` collided whenever
//! two mutations landed in the same millisecond.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/mutation_id.dart`.

use std::sync::atomic::{AtomicU32, Ordering};

/// The local outbox table.
pub const PENDING_TABLE: &str = "_00_pending_mutations";

/// 36^4: the sequence wraps before it outgrows its four digits.
const SEQ_SPAN: u32 = 1_679_616;

static SEQ: AtomicU32 = AtomicU32::new(0);

fn base36(mut n: u32, width: usize) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    loop {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
        if n == 0 {
            break;
        }
    }
    while out.len() < width {
        out.push(b'0');
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// A fresh `_00_pending_mutations:<ts>_<seq>_<client_id>` id.
pub fn mint_mutation_id(client_id: &str) -> String {
    let ts = chrono::Utc::now().timestamp_millis();
    let prev = SEQ
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |s| {
            Some((s + 1) % SEQ_SPAN)
        })
        .unwrap_or(0);
    let seq = (prev + 1) % SEQ_SPAN;
    format!("{PENDING_TABLE}:{ts:013}_{}_{client_id}", base36(seq, 4))
}

/// The client that created the mutation, or `None` for a legacy numeric id.
pub fn mutation_owner_client_id(mutation_id: &str) -> Option<&str> {
    let raw = mutation_id
        .strip_prefix(PENDING_TABLE)
        .and_then(|rest| rest.strip_prefix(':'))
        .unwrap_or(mutation_id);
    let parts: Vec<&str> = raw.split('_').collect();
    if parts.len() >= 3 {
        parts.last().copied()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_sort_chronologically_and_name_their_client() {
        let a = mint_mutation_id("tabA");
        let b = mint_mutation_id("tabA");
        assert!(a < b, "{a} < {b}");
        assert!(a.starts_with("_00_pending_mutations:"));
        assert_eq!(mutation_owner_client_id(&a), Some("tabA"));
        assert_eq!(
            mutation_owner_client_id("_00_pending_mutations:1700000000000"),
            None
        );
    }

    #[test]
    fn base36_pads() {
        assert_eq!(base36(0, 4), "0000");
        assert_eq!(base36(35, 4), "000z");
        assert_eq!(base36(36, 4), "0010");
        assert_eq!(base36(SEQ_SPAN - 1, 4), "zzzz");
    }
}
