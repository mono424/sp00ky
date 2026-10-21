//! Admin sessions.
//!
//! Two stores behind one interface, chosen by whether the scheduler has a
//! cluster secret:
//!
//! * **Signed tokens** when `SPKY_AUTH_SECRET` is set (always, in cloud). The
//!   token carries its own subject, label, mode and expiry under an HMAC keyed
//!   from the secret, so it verifies without any server-side state and
//!   survives a scheduler restart. That matters now that the dashboard has a
//!   "restart scheduler" button: an action that logs you out on success is a
//!   loop nobody wants to be in. Logout adds the token to an in-memory
//!   revocation set, which a restart loses; the consequence is that a revoked
//!   token comes back to life for the remainder of its own 8h expiry, which is
//!   the trade being made, and is stated here rather than hidden.
//!
//! * **A random-token map** otherwise. With no secret there is nothing to sign
//!   with that outlives the process, so a stateless token would be no better
//!   than a map entry. Sessions do not survive a restart in this mode.
//!
//! The HMAC is hand-built over `sha2` rather than pulling in a crate for two
//! XORs and two hashes; the construction is RFC 2104 exactly and is tested
//! against a known vector below.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

// One token format for every host of the admin API (the scheduler and the SSP
// node): `ssp_protocol::admin_session`.
pub use ssp_protocol::admin_session::{Scope, SessionMode};
use ssp_protocol::admin_session as signed;

#[derive(Debug, Clone)]
pub struct Session {
    /// The `_00_admin` user record id, or `"breakglass"`, or `"mcp:<label>"`.
    pub subject: String,
    /// Display name for the UI.
    pub label: String,
    pub mode: SessionMode,
    pub scope: Scope,
    pub expires_at: Instant,
}

enum Backend {
    /// Random tokens looked up in a map. Lost on restart.
    Map(Mutex<HashMap<String, Session>>),
    /// Self-describing signed tokens plus a revocation set.
    Signed {
        key: [u8; 32],
        revoked: Mutex<HashSet<String>>,
    },
}

pub struct SessionStore {
    backend: Backend,
    ttl: Duration,
}

impl SessionStore {
    /// `secret` is the cluster secret, when there is one. Its presence is
    /// what selects signed tokens; see the module docs for why.
    pub fn new(ttl: Duration, secret: Option<&str>) -> Arc<Self> {
        let backend = match secret.filter(|s| !s.is_empty()) {
            Some(secret) => Backend::Signed {
                // Derived, so a session key never equals the bearer that SSPs
                // present to the scheduler.
                key: signed::session_key(secret),
                revoked: Mutex::new(HashSet::new()),
            },
            None => Backend::Map(Mutex::new(HashMap::new())),
        };
        Arc::new(Self { backend, ttl })
    }

    /// Whether sessions outlive this process.
    pub fn persistent(&self) -> bool {
        matches!(self.backend, Backend::Signed { .. })
    }

    /// Mint a full-scope session with the store's default TTL.
    pub fn create(&self, subject: String, label: String, mode: SessionMode) -> String {
        self.create_with(subject, label, mode, Scope::Full, self.ttl)
    }

    /// Mint a session with an explicit scope and lifetime, and return its
    /// bearer token. MCP tokens come through here with a TTL of days.
    pub fn create_with(
        &self,
        subject: String,
        label: String,
        mode: SessionMode,
        scope: Scope,
        ttl: Duration,
    ) -> String {
        match &self.backend {
            Backend::Map(map) => {
                // Two v4 UUIDs: ~242 bits of entropy from the same CSPRNG
                // `rand` would give us, without another dependency for it.
                let token = format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    uuid::Uuid::new_v4().simple()
                );
                let session = Session {
                    subject,
                    label,
                    mode,
                    scope,
                    expires_at: Instant::now() + ttl,
                };
                let mut map = map.lock().expect("session store poisoned");
                // Sweep here rather than on a timer: sessions are few, and
                // this is the only place the map grows.
                let now = Instant::now();
                map.retain(|_, s| s.expires_at > now);
                map.insert(token.clone(), session);
                token
            }
            Backend::Signed { key, .. } => {
                // A nonce so two logins by the same subject in the same second
                // do not share a token (and revoking one keeps the other).
                let nonce = uuid::Uuid::new_v4().simple().to_string();
                let claims = signed::Claims {
                    subject,
                    label,
                    mode,
                    scope,
                    expires_at_secs: epoch_secs() + ttl.as_secs(),
                };
                signed::mint(key, &claims, &nonce)
            }
        }
    }

    /// Look up a live session, or `None` if unknown, expired, or revoked.
    pub fn get(&self, token: &str) -> Option<Session> {
        match &self.backend {
            Backend::Map(map) => {
                let mut map = map.lock().expect("session store poisoned");
                match map.get(token) {
                    Some(s) if s.expires_at > Instant::now() => Some(s.clone()),
                    Some(_) => {
                        map.remove(token);
                        None
                    }
                    None => None,
                }
            }
            Backend::Signed { key, revoked } => {
                let now = epoch_secs();
                let claims = signed::verify(key, token, now)?;
                if revoked.lock().expect("revocation set poisoned").contains(token) {
                    return None;
                }
                Some(Session {
                    subject: claims.subject,
                    label: claims.label,
                    mode: claims.mode,
                    scope: claims.scope,
                    expires_at: Instant::now() + Duration::from_secs(claims.expires_at_secs - now),
                })
            }
        }
    }

    pub fn revoke(&self, token: &str) {
        match &self.backend {
            Backend::Map(map) => {
                map.lock().expect("session store poisoned").remove(token);
            }
            Backend::Signed { revoked, .. } => {
                let mut set = revoked.lock().expect("revocation set poisoned");
                // Bound the set: only tokens that still verify can matter, and
                // a set of a few thousand strings is nothing, but an unbounded
                // one from a hostile logout loop is not. Oldest entries are
                // not tracked; clearing wholesale at a generous size is the
                // simple, honest bound.
                if set.len() > 10_000 {
                    set.clear();
                }
                set.insert(token.to_string());
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        match &self.backend {
            Backend::Map(map) => map.lock().expect("session store poisoned").len(),
            Backend::Signed { revoked, .. } => {
                revoked.lock().expect("revocation set poisoned").len()
            }
        }
    }
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// RFC 2104 HMAC over SHA-256.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        let digest = Sha256::digest(key);
        k[..32].copy_from_slice(&digest);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(message)
        .finalize();
    let outer = Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&outer);
    out
}

#[cfg(test)]
const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

#[cfg(test)]
fn base64_url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(B64[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(B64[n as usize & 63] as char);
        }
    }
    out
}

#[cfg(test)]
fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> { B64.iter().position(|&b| b == c).map(|p| p as u32) };
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            return None;
        }
        let mut n: u32 = 0;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_store(ttl: Duration) -> Arc<SessionStore> {
        SessionStore::new(ttl, None)
    }

    fn signed_store(ttl: Duration) -> Arc<SessionStore> {
        SessionStore::new(ttl, Some("cluster-secret"))
    }

    #[test]
    fn a_fresh_token_resolves_to_its_session() {
        for store in [
            map_store(Duration::from_secs(60)),
            signed_store(Duration::from_secs(60)),
        ] {
            let token = store.create("user:a".into(), "alice".into(), SessionMode::Roster);
            let s = store.get(&token).expect("session should be live");
            assert_eq!(s.subject, "user:a");
            assert_eq!(s.label, "alice");
            assert_eq!(s.mode, SessionMode::Roster);
        }
    }

    #[test]
    fn tokens_are_unique_per_session() {
        for store in [
            map_store(Duration::from_secs(60)),
            signed_store(Duration::from_secs(60)),
        ] {
            let a = store.create("user:a".into(), "a".into(), SessionMode::Roster);
            let b = store.create("user:a".into(), "a".into(), SessionMode::Roster);
            assert_ne!(a, b);
        }
    }

    #[test]
    fn an_expired_session_is_rejected_and_dropped() {
        let store = map_store(Duration::from_millis(0));
        let token = store.create("user:a".into(), "a".into(), SessionMode::Roster);
        std::thread::sleep(Duration::from_millis(5));
        assert!(store.get(&token).is_none());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn an_expired_signed_token_is_rejected() {
        let store = signed_store(Duration::from_secs(0));
        let token = store.create("user:a".into(), "a".into(), SessionMode::Roster);
        assert!(store.get(&token).is_none());
    }

    #[test]
    fn revoke_invalidates_immediately() {
        for store in [
            map_store(Duration::from_secs(60)),
            signed_store(Duration::from_secs(60)),
        ] {
            let token = store.create("user:a".into(), "a".into(), SessionMode::Roster);
            store.revoke(&token);
            assert!(store.get(&token).is_none());
        }
    }

    #[test]
    fn an_unknown_token_is_rejected() {
        for store in [
            map_store(Duration::from_secs(60)),
            signed_store(Duration::from_secs(60)),
        ] {
            assert!(store.get("not-a-token").is_none());
            assert!(store.get("not.a.token").is_none());
            assert!(store.get("").is_none());
        }
    }

    #[test]
    fn signed_tokens_survive_a_new_store_with_the_same_secret() {
        let first = signed_store(Duration::from_secs(60));
        let token = first.create("user:a".into(), "alice".into(), SessionMode::Breakglass);
        // A "restart": a brand new store, same secret, no shared state.
        let second = signed_store(Duration::from_secs(60));
        let s = second.get(&token).expect("token must verify after restart");
        assert_eq!(s.subject, "user:a");
        assert_eq!(s.mode, SessionMode::Breakglass);
        assert!(second.persistent());
        assert!(!map_store(Duration::from_secs(60)).persistent());
    }

    #[test]
    fn a_different_secret_rejects_the_token() {
        let a = signed_store(Duration::from_secs(60));
        let token = a.create("user:a".into(), "a".into(), SessionMode::Roster);
        let b = SessionStore::new(Duration::from_secs(60), Some("other"));
        assert!(b.get(&token).is_none());
    }

    #[test]
    fn a_tampered_payload_is_rejected() {
        let store = signed_store(Duration::from_secs(60));
        let token = store.create("user:a".into(), "a".into(), SessionMode::Roster);
        let (payload, sig) = token.split_once('.').unwrap();
        // Forge a different subject with the same signature.
        let forged_claims = "user:admin\nroot\nroster\n9999999999\nnonce";
        let forged = format!("{}.{}", base64_url_encode(forged_claims.as_bytes()), sig);
        assert!(store.get(&forged).is_none());
        // And a flipped signature byte.
        let mut flipped = sig.to_string().into_bytes();
        flipped[0] = if flipped[0] == b'A' { b'B' } else { b'A' };
        let flipped = format!("{}.{}", payload, String::from_utf8(flipped).unwrap());
        assert!(store.get(&flipped).is_none());
    }

    #[test]
    fn scope_and_lifetime_travel_with_the_token() {
        for store in [
            map_store(Duration::from_secs(60)),
            signed_store(Duration::from_secs(60)),
        ] {
            let token = store.create_with(
                "mcp:agent".into(),
                "agent".into(),
                SessionMode::Mcp,
                Scope::Read,
                Duration::from_secs(86_400 * 30),
            );
            let s = store.get(&token).expect("live");
            assert_eq!(s.mode, SessionMode::Mcp);
            assert_eq!(s.scope, Scope::Read);
            assert!(s.expires_at > Instant::now() + Duration::from_secs(86_000));
            let plain = store.create("user:a".into(), "a".into(), SessionMode::Roster);
            assert_eq!(store.get(&plain).unwrap().scope, Scope::Full);
        }
    }

    #[test]
    fn a_five_claim_token_from_before_scopes_is_still_full() {
        let store = signed_store(Duration::from_secs(60));
        let Backend::Signed { key, .. } = &store.backend else {
            panic!("signed")
        };
        let claims = format!("user:a\nalice\nroster\n{}\nnonce", epoch_secs() + 60);
        let payload = base64_url_encode(claims.as_bytes());
        let sig = base64_url_encode(&hmac_sha256(key, payload.as_bytes()));
        let s = store
            .get(&format!("{payload}.{sig}"))
            .expect("old token verifies");
        assert_eq!(s.scope, Scope::Full);
    }

    #[test]
    fn hmac_matches_a_known_vector() {
        // RFC 4231 test case 2.
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn base64_url_round_trips() {
        for input in [
            &b""[..],
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
            b"\xff\xfe\x00",
        ] {
            let enc = base64_url_encode(input);
            assert!(!enc.contains('=') && !enc.contains('+') && !enc.contains('/'));
            assert_eq!(base64_url_decode(&enc).unwrap(), input);
        }
        assert!(base64_url_decode("!!!").is_none());
    }
}
