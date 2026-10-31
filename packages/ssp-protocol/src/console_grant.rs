//! Console grants: how Sp00ky Cloud vouches for a signed-in console user to
//! one project's admin API.
//!
//! The console (`console.sp00ky.cloud`) talks to each project's scheduler
//! directly; the control plane never relays admin traffic. To open a session
//! there, the console asks the control plane for a *grant*: a short-lived
//! EdDSA (Ed25519) JWT naming the user, the project and the scope. The admin
//! host verifies it against the control plane's published keys (JWKS) and
//! exchanges it for an ordinary admin session.
//!
//! Why a signature and not the project's shared secret: the control plane's
//! private key never sits in a tenant container, a host can verify a grant
//! but never forge one, and the signing key rotates (by `kid`) without
//! touching `SPKY_AUTH_SECRET`, which would restart the SSPs.
//!
//! What a valid grant is, all checked here:
//! - header `alg` is exactly `EdDSA` and `kid` names a known Ed25519 key (no
//!   algorithm is negotiated, so `none`, `HS256` or `RS256` never verify);
//! - the signature verifies strictly (`verify_strict`: no malleable forms);
//! - `iss` is the expected issuer and `aud` the expected audience, which names
//!   one project (`spky-admin:<project>`), so a grant for one project never
//!   opens another;
//! - it is current (`iat`/`nbf` not in the future, `exp` not past, both with
//!   [`LEEWAY_SECS`]) and short: `exp - iat` is at most [`MAX_LIFETIME_SECS`];
//! - `scope` is `read` or `full`, and `sub` and `jti` are present.
//!
//! Replay (one `jti`, used twice) is the host's to refuse: it needs memory.

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::admin_session::{base64_url_decode, base64_url_encode};

/// Longest grant a host accepts: grants are exchanged at once, so anything
/// longer is only a wider window for a leaked one.
pub const MAX_LIFETIME_SECS: u64 = 300;
/// Clock skew tolerated between the control plane and a host.
pub const LEEWAY_SECS: u64 = 30;

/// The audience naming one project's admin API.
pub fn audience(project: &str) -> String {
    format!("spky-admin:{project}")
}

/// One public key, as RFC 8037 spells an Ed25519 JWK.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    #[serde(default)]
    pub crv: String,
    pub kid: String,
    /// The 32-byte public key, base64url without padding.
    pub x: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alg: Option<String>,
    #[serde(default, rename = "use", skip_serializing_if = "Option::is_none")]
    pub key_use: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

impl Jwks {
    fn find(&self, kid: &str) -> Option<VerifyingKey> {
        let jwk = self.keys.iter().find(|k| k.kid == kid)?;
        if jwk.kty != "OKP" || jwk.crv != "Ed25519" || jwk.key_use.as_deref().is_some_and(|u| u != "sig") {
            return None;
        }
        let bytes: [u8; 32] = base64_url_decode(&jwk.x)?.try_into().ok()?;
        VerifyingKey::from_bytes(&bytes).ok()
    }

    pub fn has(&self, kid: &str) -> bool {
        self.find(kid).is_some()
    }
}

/// What a grant says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantClaims {
    pub iss: String,
    /// A string or a list of strings; see [`verify`].
    pub aud: Value,
    /// The Sp00ky Cloud account id.
    pub sub: String,
    /// Display name (the account's login or email).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `read` or `full`.
    pub scope: String,
    /// Finer permissions, reserved for per-member access rights. Hosts that
    /// do not know them yet ignore them; `scope` is what they enforce today.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub perms: Vec<String>,
    /// The workspace (tenant) the grant was minted through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub jti: String,
    pub iat: u64,
    pub exp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    /// When the user last proved who they are to the control plane. Hosts can
    /// demand a recent one before a destructive action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_time: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantError {
    Malformed,
    /// Anything but `EdDSA`.
    Algorithm,
    /// The header names a key this host does not have (yet: refetch the JWKS).
    UnknownKey(String),
    Signature,
    Issuer,
    Audience,
    Expired,
    NotYetValid,
    /// Valid for longer than [`MAX_LIFETIME_SECS`].
    TooLong,
    Claims(&'static str),
}

impl std::fmt::Display for GrantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrantError::Malformed => f.write_str("grant is not a JWT"),
            GrantError::Algorithm => f.write_str("grant is not signed with EdDSA"),
            GrantError::UnknownKey(kid) => write!(f, "grant is signed with an unknown key `{kid}`"),
            GrantError::Signature => f.write_str("grant signature does not verify"),
            GrantError::Issuer => f.write_str("grant is from another issuer"),
            GrantError::Audience => f.write_str("grant is for another project"),
            GrantError::Expired => f.write_str("grant has expired"),
            GrantError::NotYetValid => f.write_str("grant is not valid yet"),
            GrantError::TooLong => f.write_str("grant lives too long"),
            GrantError::Claims(what) => write!(f, "grant claims are invalid: {what}"),
        }
    }
}

#[derive(Deserialize)]
struct Header {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

fn parts(token: &str) -> Result<(&str, &str, &str), GrantError> {
    let mut it = token.split('.');
    match (it.next(), it.next(), it.next(), it.next()) {
        (Some(h), Some(p), Some(s), None) if !h.is_empty() && !p.is_empty() && !s.is_empty() => Ok((h, p, s)),
        _ => Err(GrantError::Malformed),
    }
}

fn header_of(encoded: &str) -> Result<Header, GrantError> {
    let bytes = base64_url_decode(encoded).ok_or(GrantError::Malformed)?;
    serde_json::from_slice(&bytes).map_err(|_| GrantError::Malformed)
}

/// The `kid` a grant names, so a host can tell "refetch the keys" from
/// "reject" before it verifies.
pub fn kid_of(token: &str) -> Option<String> {
    let (h, _, _) = parts(token).ok()?;
    header_of(h).ok()?.kid
}

fn trim_issuer(s: &str) -> &str {
    s.trim_end_matches('/')
}

/// Verify `token` against `keys`, for `issuer` and `audience`, at `now`
/// (epoch seconds). The claims of a valid grant.
pub fn verify(token: &str, keys: &Jwks, issuer: &str, audience: &str, now: u64) -> Result<GrantClaims, GrantError> {
    let (h, p, s) = parts(token)?;
    let header = header_of(h)?;
    if header.alg != "EdDSA" {
        return Err(GrantError::Algorithm);
    }
    let kid = header.kid.ok_or(GrantError::Malformed)?;
    let key = keys.find(&kid).ok_or_else(|| GrantError::UnknownKey(kid.clone()))?;
    let sig_bytes: [u8; 64] = base64_url_decode(s)
        .ok_or(GrantError::Malformed)?
        .try_into()
        .map_err(|_| GrantError::Malformed)?;
    let signed = format!("{h}.{p}");
    key.verify_strict(signed.as_bytes(), &Signature::from_bytes(&sig_bytes))
        .map_err(|_| GrantError::Signature)?;

    let payload = base64_url_decode(p).ok_or(GrantError::Malformed)?;
    let claims: GrantClaims = serde_json::from_slice(&payload).map_err(|_| GrantError::Malformed)?;

    if trim_issuer(&claims.iss) != trim_issuer(issuer) {
        return Err(GrantError::Issuer);
    }
    let aud_ok = match &claims.aud {
        Value::String(a) => a == audience,
        Value::Array(list) => list.iter().any(|a| a.as_str() == Some(audience)),
        _ => false,
    };
    if !aud_ok {
        return Err(GrantError::Audience);
    }
    if claims.exp.saturating_add(LEEWAY_SECS) <= now {
        return Err(GrantError::Expired);
    }
    if claims.iat > now.saturating_add(LEEWAY_SECS) || claims.nbf.is_some_and(|nbf| nbf > now.saturating_add(LEEWAY_SECS)) {
        return Err(GrantError::NotYetValid);
    }
    if claims.exp <= claims.iat || claims.exp - claims.iat > MAX_LIFETIME_SECS {
        return Err(GrantError::TooLong);
    }
    if claims.scope != "read" && claims.scope != "full" {
        return Err(GrantError::Claims("scope must be read or full"));
    }
    if claims.sub.is_empty() || claims.jti.is_empty() {
        return Err(GrantError::Claims("sub and jti are required"));
    }
    Ok(claims)
}

/// Sign `claims` with a 32-byte Ed25519 secret key as `kid`. What the control
/// plane does (in Go); here for tests and local tooling.
pub fn sign(claims: &GrantClaims, kid: &str, secret: &[u8; 32]) -> String {
    let header = serde_json::json!({ "alg": "EdDSA", "typ": "JWT", "kid": kid });
    let h = base64_url_encode(header.to_string().as_bytes());
    let p = base64_url_encode(serde_json::to_string(claims).unwrap_or_default().as_bytes());
    let signed = format!("{h}.{p}");
    let sig = SigningKey::from_bytes(secret).sign(signed.as_bytes());
    format!("{signed}.{}", base64_url_encode(&sig.to_bytes()))
}

/// The JWK for a 32-byte Ed25519 secret key.
pub fn public_jwk(kid: &str, secret: &[u8; 32]) -> Jwk {
    let public = SigningKey::from_bytes(secret).verifying_key();
    Jwk {
        kty: "OKP".into(),
        crv: "Ed25519".into(),
        kid: kid.into(),
        x: base64_url_encode(public.as_bytes()),
        alg: Some("EdDSA".into()),
        key_use: Some("sig".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ISS: &str = "https://api.sp00ky.cloud";
    const SECRET: [u8; 32] = [7; 32];
    const NOW: u64 = 1_800_000_000;

    fn claims() -> GrantClaims {
        GrantClaims {
            iss: ISS.into(),
            aud: json!(audience("whitepawn")),
            sub: "acct_1".into(),
            name: Some("mono424".into()),
            scope: "full".into(),
            perms: vec![],
            tenant: Some("t_1".into()),
            jti: "g_1".into(),
            iat: NOW,
            exp: NOW + 60,
            nbf: None,
            auth_time: Some(NOW - 10),
        }
    }

    fn keys() -> Jwks {
        Jwks { keys: vec![public_jwk("k1", &SECRET)] }
    }

    fn check(token: &str) -> Result<GrantClaims, GrantError> {
        verify(token, &keys(), ISS, &audience("whitepawn"), NOW)
    }

    #[test]
    fn a_signed_grant_verifies() {
        let token = sign(&claims(), "k1", &SECRET);
        assert_eq!(check(&token).unwrap(), claims());
        assert_eq!(kid_of(&token).as_deref(), Some("k1"));
        // A trailing slash on either issuer spelling is the same issuer.
        assert!(verify(&token, &keys(), "https://api.sp00ky.cloud/", &audience("whitepawn"), NOW).is_ok());
    }

    #[test]
    fn another_project_or_issuer_is_refused() {
        let token = sign(&claims(), "k1", &SECRET);
        assert_eq!(verify(&token, &keys(), ISS, &audience("other"), NOW), Err(GrantError::Audience));
        assert_eq!(verify(&token, &keys(), "https://evil.example", &audience("whitepawn"), NOW), Err(GrantError::Issuer));
        let mut c = claims();
        c.aud = json!(["x", audience("whitepawn")]);
        assert!(check(&sign(&c, "k1", &SECRET)).is_ok(), "a list containing the audience is fine");
    }

    #[test]
    fn time_is_checked_both_ways() {
        let token = sign(&claims(), "k1", &SECRET);
        assert_eq!(verify(&token, &keys(), ISS, &audience("whitepawn"), NOW + 60 + LEEWAY_SECS), Err(GrantError::Expired));
        assert!(verify(&token, &keys(), ISS, &audience("whitepawn"), NOW + 60 + LEEWAY_SECS - 1).is_ok());
        assert_eq!(verify(&token, &keys(), ISS, &audience("whitepawn"), NOW - LEEWAY_SECS - 1), Err(GrantError::NotYetValid));
        let mut c = claims();
        c.exp = c.iat + MAX_LIFETIME_SECS + 1;
        assert_eq!(check(&sign(&c, "k1", &SECRET)), Err(GrantError::TooLong));
    }

    #[test]
    fn no_algorithm_but_eddsa_is_accepted() {
        let token = sign(&claims(), "k1", &SECRET);
        let (_, p, s) = parts(&token).unwrap();
        for alg in ["none", "HS256", "RS256", "eddsa"] {
            let h = base64_url_encode(json!({"alg": alg, "kid": "k1"}).to_string().as_bytes());
            assert_eq!(check(&format!("{h}.{p}.{s}")), Err(GrantError::Algorithm), "{alg}");
        }
    }

    #[test]
    fn tampering_or_a_foreign_key_fails() {
        let token = sign(&claims(), "k1", &SECRET);
        let (h, _, s) = parts(&token).unwrap();
        let mut c = claims();
        c.scope = "full".into();
        c.sub = "acct_attacker".into();
        let forged_payload = base64_url_encode(serde_json::to_string(&c).unwrap().as_bytes());
        assert_eq!(check(&format!("{h}.{forged_payload}.{s}")), Err(GrantError::Signature));
        // Signed by a key the host does not trust, under a kid it does.
        assert_eq!(check(&sign(&claims(), "k1", &[9; 32])), Err(GrantError::Signature));
        assert_eq!(check(&sign(&claims(), "k2", &SECRET)), Err(GrantError::UnknownKey("k2".into())));
        assert_eq!(check("a.b"), Err(GrantError::Malformed));
        assert_eq!(check("a.b.c.d"), Err(GrantError::Malformed));
    }

    #[test]
    fn claims_must_be_complete() {
        let mut c = claims();
        c.scope = "admin".into();
        assert!(matches!(check(&sign(&c, "k1", &SECRET)), Err(GrantError::Claims(_))));
        let mut c = claims();
        c.jti = String::new();
        assert!(matches!(check(&sign(&c, "k1", &SECRET)), Err(GrantError::Claims(_))));
    }

    /// A grant signed by the control plane's Go signer
    /// (`TestCrossLanguageFixture` in spooky-cloud `internal/console`), same
    /// seed and claims: the two implementations agree on the wire.
    #[test]
    fn go_signed_grant_verifies() {
        const GO: &str = "eyJhbGciOiJFZERTQSIsImtpZCI6ImsxIiwidHlwIjoiSldUIn0.eyJpc3MiOiJodHRwczovL2FwaS5zcDAwa3kuY2xvdWQiLCJhdWQiOiJzcGt5LWFkbWluOndoaXRlcGF3biIsInN1YiI6ImFjY3RfMSIsIm5hbWUiOiJtb25vNDI0Iiwic2NvcGUiOiJmdWxsIiwidGVuYW50IjoidF8xIiwianRpIjoiZ18xIiwiaWF0IjoxODAwMDAwMDAwLCJleHAiOjE4MDAwMDAwNjAsImF1dGhfdGltZSI6MTc5OTk5OTk5MH0.FgAV9XvNNbcfzfQPWpX4Rox0L-OEunS77MIV2kF4lTYEocht4OmyGUKLn3sVYAQq2ZZ5jgW3trMHwSojoJkhCg";
        assert_eq!(check(GO).unwrap(), claims());
    }

    #[test]
    fn non_signing_or_non_ed25519_keys_are_ignored() {
        let mut jwk = public_jwk("k1", &SECRET);
        jwk.key_use = Some("enc".into());
        let token = sign(&claims(), "k1", &SECRET);
        assert!(matches!(verify(&token, &Jwks { keys: vec![jwk] }, ISS, &audience("whitepawn"), NOW), Err(GrantError::UnknownKey(_))));
        let mut jwk = public_jwk("k1", &SECRET);
        jwk.crv = "X25519".into();
        assert!(matches!(verify(&token, &Jwks { keys: vec![jwk] }, ISS, &audience("whitepawn"), NOW), Err(GrantError::UnknownKey(_))));
    }
}
