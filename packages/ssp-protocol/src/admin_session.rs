//! Signed admin session tokens, the bearer of the admin API (`/admin/api`).
//!
//! One format for every host that serves the API: the cluster scheduler and
//! the SSP node (a Cloudflare project has no scheduler). A token carries its
//! own subject, label, mode, scope and expiry under an HMAC keyed from the
//! project's auth secret, so it verifies without server-side state and a
//! token minted by one host verifies on another with the same secret.
//!
//! Format: `base64url(claims) "." base64url(HMAC(key, base64url(claims)))`,
//! where the claims are `subject\nlabel\nmode\nexp_epoch_secs\nnonce\nscope`
//! in that order. A five-claim token (from before scopes) is a full one. The
//! order is part of the format and must not change.

use serde::Serialize;

use crate::impersonation::{constant_time_eq, hmac_sha256};

/// How a session was established. Surfaced to the UI so a break-glass session
/// is visibly flagged: the roster was bypassed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionMode {
    /// Signed in as a user on the `_00_admin` roster.
    Roster,
    /// Signed in with `SPKY_ADMIN_PASSWORD`.
    Breakglass,
    /// A long-lived token minted for an MCP client; cannot mint further tokens.
    Mcp,
    /// The node's own bearer (`SPKY_AUTH_SECRET`), presented by the control
    /// plane. Never minted into a token.
    Node,
    /// A Sp00ky Cloud console user, signed in with a grant from the control
    /// plane (`console_grant`). Short-lived; the console renews it.
    Cloud,
}

impl SessionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionMode::Roster => "roster",
            SessionMode::Breakglass => "breakglass",
            SessionMode::Mcp => "mcp",
            SessionMode::Node => "node",
            SessionMode::Cloud => "cloud",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "roster" => Some(SessionMode::Roster),
            "breakglass" => Some(SessionMode::Breakglass),
            "mcp" => Some(SessionMode::Mcp),
            "cloud" => Some(SessionMode::Cloud),
            _ => None,
        }
    }
}

/// What a session may do: a `Read` one only makes `GET` requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Read,
    Full,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Full => "full",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "read" => Some(Scope::Read),
            "full" => Some(Scope::Full),
            _ => None,
        }
    }
}

/// What a token says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The `_00_admin` user record id, `"breakglass"`, or `"mcp:<label>"`.
    pub subject: String,
    /// Display name for the UI.
    pub label: String,
    pub mode: SessionMode,
    pub scope: Scope,
    pub expires_at_secs: u64,
}

/// The token key for a project's auth secret. Derived, so a session key never
/// equals the bearer the SSPs present.
pub fn session_key(secret: &str) -> [u8; 32] {
    hmac_sha256(secret.as_bytes(), b"spky-admin-session")
}

/// Sign `claims` into a token. `nonce` makes two tokens minted for the same
/// subject in the same second distinct (and revocable apart); it must not
/// contain a newline.
pub fn mint(key: &[u8; 32], claims: &Claims, nonce: &str) -> String {
    let body = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        claims.subject.replace('\n', " "),
        claims.label.replace('\n', " "),
        claims.mode.as_str(),
        claims.expires_at_secs,
        nonce.replace('\n', " "),
        claims.scope.as_str()
    );
    let payload = base64_url_encode(body.as_bytes());
    let sig = base64_url_encode(&hmac_sha256(key, payload.as_bytes()));
    format!("{payload}.{sig}")
}

/// The claims of a token signed with `key` that has not expired by `now_secs`.
pub fn verify(key: &[u8; 32], token: &str, now_secs: u64) -> Option<Claims> {
    let (payload, sig) = token.split_once('.')?;
    let expected = base64_url_encode(&hmac_sha256(key, payload.as_bytes()));
    if !constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
        return None;
    }
    let body = String::from_utf8(base64_url_decode(payload)?).ok()?;
    let mut parts = body.split('\n');
    let subject = parts.next()?.to_string();
    let label = parts.next()?.to_string();
    let mode = SessionMode::parse(parts.next()?)?;
    let expires_at_secs: u64 = parts.next()?.parse().ok()?;
    let _nonce = parts.next()?;
    let scope = match parts.next() {
        Some(s) => Scope::parse(s)?,
        None => Scope::Full,
    };
    if expires_at_secs <= now_secs {
        return None;
    }
    Some(Claims { subject, label, mode, scope, expires_at_secs })
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

pub(crate) fn base64_url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [chunk[0], chunk.get(1).copied().unwrap_or(0), chunk.get(2).copied().unwrap_or(0)];
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

pub(crate) fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
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

    fn claims(exp: u64) -> Claims {
        Claims {
            subject: "user:a".into(),
            label: "alice".into(),
            mode: SessionMode::Roster,
            scope: Scope::Read,
            expires_at_secs: exp,
        }
    }

    #[test]
    fn a_token_verifies_with_its_key_until_it_expires() {
        let key = session_key("cluster-secret");
        let token = mint(&key, &claims(1_000), "n1");
        assert_eq!(verify(&key, &token, 999), Some(claims(1_000)));
        assert_eq!(verify(&key, &token, 1_000), None, "expired");
        assert_eq!(verify(&session_key("other"), &token, 999), None, "other key");
        let (payload, _) = token.split_once('.').unwrap();
        assert_eq!(verify(&key, &format!("{payload}.AAAA"), 999), None, "bad signature");
    }

    #[test]
    fn a_five_claim_token_is_a_full_one() {
        let key = session_key("s");
        let payload = base64_url_encode(b"user:a\nalice\nroster\n5000\nnonce");
        let sig = base64_url_encode(&hmac_sha256(&key, payload.as_bytes()));
        let got = verify(&key, &format!("{payload}.{sig}"), 1).unwrap();
        assert_eq!(got.scope, Scope::Full);
    }

    #[test]
    fn the_node_mode_never_parses_from_a_token() {
        assert_eq!(SessionMode::parse("node"), None);
    }
}
