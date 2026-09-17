//! Admin impersonation tokens.
//!
//! An admin impersonates a user by holding a real SurrealDB session whose
//! `$auth.id` is that user. SurrealDB only mints tokens for passwords it
//! checks itself, so the backend signs one for a dedicated record-access
//! method (`_00_impersonate`, defined by the CLI only when `impersonation`
//! is enabled in sp00ky.yml) and SurrealDB verifies it with the same key.
//!
//! The signer is deliberately stateless. Every check that can change after a
//! token is issued lives in the access method's `AUTHENTICATE` block and runs
//! on each authenticate: the `_00_impersonation` session row must be open and
//! unexpired, its target must match `$auth.id`, and its admin must still be on
//! the `_00_admin` roster. The only caller is `fn::_00_impersonate::start` /
//! `renew`, which run the roster check in the admin's own session and reach
//! the backend with the shared bearer.
//!
//! The key is derived from `SPKY_AUTH_SECRET`, so the CLI (which renders the
//! `DEFINE ACCESS`), the SSP and the scheduler agree without another secret,
//! and rotating the secret invalidates every outstanding token. An empty
//! secret disables the feature: the derived key would be public.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Name of the record-access method impersonation tokens are issued for.
pub const ACCESS_NAME: &str = "_00_impersonate";

/// Env var that switches the mint route on in the SSP and the scheduler.
pub const ENV_FLAG: &str = "SPKY_IMPERSONATION";

/// Upper bound on a single token's lifetime, whatever the request asks for.
pub const MAX_TOKEN_TTL_SECS: u64 = 3600;

/// `iss` claim on every impersonation token.
pub const ISSUER: &str = "sp00ky-impersonation";

/// Whether an `SPKY_IMPERSONATION` value means "on".
pub fn env_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "on" | "yes")
    )
}

/// The HS256 key the `_00_impersonate` access method verifies with, as the
/// literal string placed in `DEFINE ACCESS ... KEY '<key>'`. SurrealDB uses
/// the string's bytes as the HMAC key, so signing uses the same bytes.
/// `None` when the secret is empty.
pub fn derive_key(auth_secret: &str) -> Option<String> {
    if auth_secret.is_empty() {
        return None;
    }
    Some(hex(&hmac_sha256(auth_secret.as_bytes(), b"spky-impersonate-jwt")))
}

/// Body of `POST /impersonate/mint`, sent by the SurrealDB functions.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MintRequest {
    /// `_00_impersonation:<id>` row the token is bound to.
    pub session: String,
    /// Record id the session acts as (`user:<id>`).
    pub target: String,
    /// Record id of the admin who started it. Informational (audit log);
    /// the roster check happens in `AUTHENTICATE`.
    pub admin: String,
    /// The target's regular access method, carried so the SSP's permission
    /// rewrite can keep resolving `$access` as the app expects.
    #[serde(default)]
    pub access: String,
    pub ns: String,
    pub db: String,
    /// Requested token lifetime in seconds.
    pub ttl_secs: u64,
    /// Seconds until the session row's `expires_at`. The token never outlives it.
    pub session_remaining_secs: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MintResponse {
    pub token: String,
    /// Unix seconds.
    pub exp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MintError {
    /// The feature is off or the secret is empty.
    Disabled,
    /// The request cannot produce a valid token.
    Invalid(&'static str),
}

/// Sign an impersonation token. `now_secs` is the caller's wall clock.
pub fn mint(auth_secret: &str, req: &MintRequest, now_secs: u64) -> Result<MintResponse, MintError> {
    let key = derive_key(auth_secret).ok_or(MintError::Disabled)?;
    if !req.session.starts_with("_00_impersonation:") {
        return Err(MintError::Invalid("session must be an _00_impersonation record"));
    }
    if req.target.is_empty() || !req.target.contains(':') {
        return Err(MintError::Invalid("target must be a record id"));
    }
    if req.target == req.admin {
        return Err(MintError::Invalid("an admin cannot impersonate themselves"));
    }
    if req.ns.is_empty() || req.db.is_empty() {
        return Err(MintError::Invalid("ns and db are required"));
    }
    if req.session_remaining_secs <= 0 {
        return Err(MintError::Invalid("session has expired"));
    }
    let ttl = req
        .ttl_secs
        .clamp(1, MAX_TOKEN_TTL_SECS)
        .min(req.session_remaining_secs as u64);
    let exp = now_secs + ttl;

    let header = serde_json::json!({ "alg": "HS256", "typ": "JWT" });
    let claims = serde_json::json!({
        "iss": ISSUER,
        "iat": now_secs,
        "nbf": now_secs,
        "exp": exp,
        "NS": req.ns,
        "DB": req.db,
        "AC": ACCESS_NAME,
        "ID": req.target,
        "spky_imp": req.session,
        "spky_admin": req.admin,
        "spky_as_access": req.access,
    });
    let signing_input = format!(
        "{}.{}",
        base64url(header.to_string().as_bytes()),
        base64url(claims.to_string().as_bytes())
    );
    let sig = hmac_sha256(key.as_bytes(), signing_input.as_bytes());
    Ok(MintResponse { token: format!("{signing_input}.{}", base64url(&sig)), exp })
}

/// Body of `POST /impersonate/users`, sent by `fn::_00_impersonate::list_users`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UsersRequest {
    pub table: String,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub search: String,
    #[serde(default = "default_users_limit")]
    pub limit: u32,
}

fn default_users_limit() -> u32 {
    25
}

/// Most rows one search returns.
pub const MAX_USERS_LIMIT: u32 = 100;

/// The root query behind the user picker, as `(surql, search)`: bind the
/// search text as `$q`. The table and field names come from sp00ky.yml via
/// the function body, and are validated again here because they are spliced
/// into the statement. Each row carries `is_admin`, since admins cannot be
/// impersonated and the picker should say so rather than fail on start.
pub fn users_query(req: &UsersRequest) -> Result<(String, String), MintError> {
    if !is_identifier(&req.table) {
        return Err(MintError::Invalid("table is not a plain identifier"));
    }
    if req.fields.iter().any(|f| !is_identifier(f) || f == "id" || f == "is_admin") {
        return Err(MintError::Invalid("fields must be plain identifiers"));
    }
    let limit = req.limit.clamp(1, MAX_USERS_LIMIT);
    let mut projection = String::from("<string>id AS id");
    let mut matches = vec!["string::contains(string::lowercase(<string>id), $q)".to_string()];
    for f in &req.fields {
        projection.push_str(&format!(", {f}"));
        matches.push(format!("string::contains(string::lowercase(<string>({f} OR '')), $q)"));
    }
    projection.push_str(
        ", array::len((SELECT VALUE id FROM _00_admin WHERE user = $parent.id LIMIT 1)) > 0 AS is_admin",
    );
    let surql = format!(
        "SELECT {projection} FROM {table} WHERE $q = '' OR {cond} LIMIT {limit};",
        table = req.table,
        cond = matches.join(" OR "),
    );
    Ok((surql, req.search.trim().to_lowercase()))
}

fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && s.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// RFC 2104 HMAC over SHA-256.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new().chain_update(ipad).chain_update(message).finalize();
    Sha256::new().chain_update(opad).chain_update(inner).finalize().into()
}

/// Constant-time equality, for comparing bearer secrets.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = match chunk.len() {
            3 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32,
            2 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8,
            _ => (chunk[0] as u32) << 16,
        };
        for i in 0..=chunk.len() {
            out.push(ALPHABET[(n >> (18 - 6 * i) & 0x3f) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> MintRequest {
        MintRequest {
            session: "_00_impersonation:s1".into(),
            target: "user:bob".into(),
            admin: "user:alice".into(),
            access: "account".into(),
            ns: "main".into(),
            db: "main".into(),
            ttl_secs: 900,
            session_remaining_secs: 7200,
        }
    }

    fn claims(token: &str) -> serde_json::Value {
        let payload = token.split('.').nth(1).unwrap();
        let mut padded = payload.replace('-', "+").replace('_', "/");
        while padded.len() % 4 != 0 {
            padded.push('=');
        }
        // Tiny standard-base64 decoder so the crate needs no extra dependency.
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut bits = 0u32;
        let mut nbits = 0;
        let mut out = Vec::new();
        for c in padded.bytes().filter(|c| *c != b'=') {
            bits = bits << 6 | alphabet.iter().position(|a| *a == c).unwrap() as u32;
            nbits += 6;
            if nbits >= 8 {
                nbits -= 8;
                out.push((bits >> nbits) as u8);
            }
        }
        serde_json::from_slice(&out).unwrap()
    }

    #[test]
    fn hmac_matches_rfc4231_case_2() {
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn base64url_has_no_padding() {
        assert_eq!(base64url(b"f"), "Zg");
        assert_eq!(base64url(b"fo"), "Zm8");
        assert_eq!(base64url(b"foo"), "Zm9v");
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
    }

    #[test]
    fn empty_secret_disables() {
        assert_eq!(derive_key(""), None);
        assert_eq!(mint("", &req(), 0), Err(MintError::Disabled));
    }

    #[test]
    fn key_depends_on_secret() {
        assert_ne!(derive_key("a"), derive_key("b"));
        assert_eq!(derive_key("a").unwrap().len(), 64);
    }

    #[test]
    fn token_carries_the_session_claims() {
        let out = mint("s3cret", &req(), 1_000).unwrap();
        let c = claims(&out.token);
        assert_eq!(c["AC"], ACCESS_NAME);
        assert_eq!(c["ID"], "user:bob");
        assert_eq!(c["spky_imp"], "_00_impersonation:s1");
        assert_eq!(c["spky_admin"], "user:alice");
        assert_eq!(c["spky_as_access"], "account");
        assert_eq!(c["exp"], 1_900);
        assert_eq!(out.exp, 1_900);
    }

    #[test]
    fn signature_verifies_with_the_derived_key() {
        let out = mint("s3cret", &req(), 1_000).unwrap();
        let (input, sig) = out.token.rsplit_once('.').unwrap();
        let key = derive_key("s3cret").unwrap();
        assert_eq!(sig, base64url(&hmac_sha256(key.as_bytes(), input.as_bytes())));
    }

    #[test]
    fn expiry_is_clamped_to_the_session_and_the_cap() {
        let mut r = req();
        r.session_remaining_secs = 60;
        assert_eq!(mint("s", &r, 0).unwrap().exp, 60);
        r.session_remaining_secs = 100_000;
        r.ttl_secs = 100_000;
        assert_eq!(mint("s", &r, 0).unwrap().exp, MAX_TOKEN_TTL_SECS);
    }

    #[test]
    fn rejects_bad_requests() {
        let mut r = req();
        r.session_remaining_secs = 0;
        assert!(matches!(mint("s", &r, 0), Err(MintError::Invalid(_))));
        let mut r = req();
        r.session = "user:x".into();
        assert!(matches!(mint("s", &r, 0), Err(MintError::Invalid(_))));
        let mut r = req();
        r.target = r.admin.clone();
        assert!(matches!(mint("s", &r, 0), Err(MintError::Invalid(_))));
    }

    #[test]
    fn users_query_validates_identifiers() {
        let mut r = UsersRequest { table: "user".into(), fields: vec!["email".into()], search: " Bo ".into(), limit: 500 };
        let (sql, q) = users_query(&r).unwrap();
        assert_eq!(q, "bo");
        assert!(sql.contains("FROM user WHERE"));
        assert!(sql.contains("LIMIT 100;"));
        assert!(sql.contains("<string>(email OR '')"));
        r.table = "user; DELETE user".into();
        assert!(users_query(&r).is_err());
        r.table = "user".into();
        r.fields = vec!["email FROM x".into()];
        assert!(users_query(&r).is_err());
    }

    #[test]
    fn env_flag_parsing() {
        assert!(env_enabled(Some("1")));
        assert!(env_enabled(Some(" On ")));
        assert!(!env_enabled(Some("0")));
        assert!(!env_enabled(None));
    }
}
