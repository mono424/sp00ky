//! Client-side mirror of `ssp-protocol`'s `list_ref_table_for`: the same naming
//! so the LIVE subscription, the membership reads and the SSP's writes land on
//! the same table. Port of `modules/ref-tables.ts`.

/// Ref storage mode (mirrors the SSP's `RefMode`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RefMode {
    Single,
    #[default]
    Dedicated,
}

/// Sentinel user id for unauthenticated clients with anonymous live queries.
/// No `user:` prefix, so it can never collide with a real user id.
pub const ANON_USER_ID: &str = "anon";

/// The one `_00_list_ref` table every signed-out session shares.
pub const ANON_LIST_REF_TABLE: &str = "_00_list_ref_anon";

/// `user:abc` -> `abc`; `None` when empty or not a valid table identifier part.
pub fn sanitize_user_id(user_id: Option<&str>) -> Option<String> {
    let s = user_id?;
    if s.is_empty() {
        return None;
    }
    let raw = s.strip_prefix("user:").unwrap_or(s);
    if raw.is_empty() || !raw.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some(raw.to_string())
}

/// The `_00_list_ref` table for `(mode, user_id)`.
pub fn list_ref_table_for(mode: RefMode, user_id: Option<&str>) -> String {
    if user_id == Some(ANON_USER_ID) {
        return ANON_LIST_REF_TABLE.to_string();
    }
    if mode == RefMode::Single {
        return "_00_list_ref".to_string();
    }
    match sanitize_user_id(user_id) {
        Some(uid) => format!("_00_list_ref_user_{uid}"),
        None => "_00_list_ref".to_string(),
    }
}

fn imul(a: u32, b: u32) -> u32 {
    a.wrapping_mul(b)
}

/// cyrb53, the hash the TypeScript client uses for a bucket id that fails
/// sanitization, over UTF-16 code units like JavaScript's `charCodeAt`.
pub fn cyrb53(s: &str, seed: u32) -> u64 {
    let mut h1: u32 = 0xdeadbeef ^ seed;
    let mut h2: u32 = 0x41c6ce57 ^ seed;
    for ch in s.encode_utf16() {
        let ch = u32::from(ch);
        h1 = imul(h1 ^ ch, 2_654_435_761);
        h2 = imul(h2 ^ ch, 1_597_334_677);
    }
    h1 = imul(h1 ^ (h1 >> 16), 2_246_822_507);
    h1 ^= imul(h2 ^ (h2 >> 13), 3_266_489_909);
    h2 = imul(h2 ^ (h2 >> 16), 2_246_822_507);
    h2 ^= imul(h1 ^ (h1 >> 13), 3_266_489_909);
    4_294_967_296u64 * u64::from(2_097_151 & h2) + u64::from(h1)
}

/// The local store a principal owns. An id that fails sanitization still gets
/// a deterministic per-user bucket; falling back to `anon` would put an
/// authenticated user in the shared bucket.
pub fn bucket_id_for_user(user_id: Option<&str>) -> String {
    match user_id {
        None => ANON_USER_ID.to_string(),
        Some(ANON_USER_ID) => ANON_USER_ID.to_string(),
        Some(id) => sanitize_user_id(Some(id)).unwrap_or_else(|| format!("u{:x}", cyrb53(id, 0))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables() {
        assert_eq!(list_ref_table_for(RefMode::Dedicated, Some("user:abc")), "_00_list_ref_user_abc");
        assert_eq!(list_ref_table_for(RefMode::Single, Some("user:abc")), "_00_list_ref");
        assert_eq!(list_ref_table_for(RefMode::Dedicated, Some("anon")), "_00_list_ref_anon");
        assert_eq!(list_ref_table_for(RefMode::Dedicated, None), "_00_list_ref");
        assert_eq!(list_ref_table_for(RefMode::Dedicated, Some("user:a-b")), "_00_list_ref");
    }

    #[test]
    fn cyrb53_matches_js() {
        // cyrb53('a') in JavaScript.
        assert_eq!(cyrb53("a", 0), 7_929_297_801_672_961);
        assert_eq!(cyrb53("é😀x", 0), 3_090_441_161_899_839);
        assert_eq!(bucket_id_for_user(Some("user:a-b")), "uf0a53e47db8d");
        assert_eq!(bucket_id_for_user(Some("user:abc")), "abc");
        assert_eq!(bucket_id_for_user(None), "anon");
    }
}
