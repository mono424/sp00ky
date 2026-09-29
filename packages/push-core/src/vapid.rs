//! VAPID (RFC 8292) application server keys.
//!
//! One P-256 key per project. It is derived from `SPKY_AUTH_SECRET` so every
//! host of a project (scheduler replicas, the standalone SSP, a restarted
//! machine) signs with the same key without anyone storing or distributing a
//! new secret; `SPKY_VAPID_PRIVATE_KEY` overrides it for projects that bring
//! a key from another push setup and must keep their existing subscriptions.

use hkdf::Hkdf;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::SecretKey;
use sha2::Sha256;

use crate::util::{b64url, decode_b64_any, hex, sha256};

const HKDF_SALT: &[u8] = b"sp00ky-web-push";
const HKDF_INFO: &str = "vapid-p256-v1";

/// How long a VAPID JWT is valid. RFC 8292 caps it at 24 h; Apple rejects more
/// than one hour less than that, 12 h is what the libraries use.
pub const JWT_TTL_SECS: u64 = 12 * 3600;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VapidError {
    #[error("empty secret")]
    EmptySecret,
    #[error("invalid VAPID private key: {0}")]
    InvalidKey(String),
}

#[derive(Clone)]
pub struct VapidKeys {
    signing: SigningKey,
    public: Vec<u8>,
    public_b64: String,
    kid: String,
}

impl std::fmt::Debug for VapidKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the private half.
        f.debug_struct("VapidKeys")
            .field("kid", &self.kid)
            .field("public", &self.public_b64)
            .finish()
    }
}

impl VapidKeys {
    /// Derive the project key from the auth secret: HKDF-SHA256 with a fixed
    /// salt and info. The 32 output bytes are a valid scalar with overwhelming
    /// probability; when they are not (0 or >= n) the info gets a counter
    /// suffix, so the derivation stays deterministic.
    pub fn from_secret(secret: &str) -> Result<VapidKeys, VapidError> {
        if secret.trim().is_empty() {
            return Err(VapidError::EmptySecret);
        }
        let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), secret.as_bytes());
        for attempt in 0u32..64 {
            let info = if attempt == 0 {
                HKDF_INFO.to_string()
            } else {
                format!("{HKDF_INFO}/{attempt}")
            };
            let mut okm = [0u8; 32];
            // 32 bytes is far below HKDF-SHA256's 8160-byte ceiling.
            if hk.expand(info.as_bytes(), &mut okm).is_err() {
                continue;
            }
            if let Ok(key) = SecretKey::from_slice(&okm) {
                return Ok(Self::from_secret_key(&key));
            }
        }
        Err(VapidError::InvalidKey("no valid scalar derived".into()))
    }

    /// A raw 32-byte P-256 scalar in base64url, the format `web-push
    /// generate-vapid-keys` and friends print.
    pub fn from_private_b64url(private: &str) -> Result<VapidKeys, VapidError> {
        let bytes = decode_b64_any(private).map_err(VapidError::InvalidKey)?;
        Self::from_private_bytes(&bytes)
    }

    pub fn from_private_bytes(bytes: &[u8]) -> Result<VapidKeys, VapidError> {
        if bytes.len() != 32 {
            return Err(VapidError::InvalidKey(format!(
                "expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let key = SecretKey::from_slice(bytes)
            .map_err(|_| VapidError::InvalidKey("not a valid P-256 scalar".into()))?;
        Ok(Self::from_secret_key(&key))
    }

    fn from_secret_key(key: &SecretKey) -> VapidKeys {
        let signing = SigningKey::from(key);
        let public = key.public_key().to_encoded_point(false).as_bytes().to_vec();
        let public_b64 = b64url(&public);
        let kid = hex(&sha256(&public)[..8]);
        VapidKeys {
            signing,
            public,
            public_b64,
            kid,
        }
    }

    /// Uncompressed SEC1 point, base64url without padding: the browser's
    /// `applicationServerKey`.
    pub fn public_key_b64url(&self) -> &str {
        &self.public_b64
    }

    /// The 65-byte uncompressed point.
    pub fn public_key_bytes(&self) -> &[u8] {
        &self.public
    }

    /// Short stable id of the key (hex of the first 8 bytes of sha256 of the
    /// public key). Stored on every subscription so rows made under an older
    /// key are recognizable.
    pub fn kid(&self) -> &str {
        &self.kid
    }

    /// The raw private scalar, base64url. For `spky push keys` style tooling.
    pub fn private_key_b64url(&self) -> String {
        b64url(&self.signing.to_bytes())
    }

    /// An ES256 JWT for one push service origin.
    pub fn jwt(&self, audience: &str, subject: &str, exp_secs: u64) -> String {
        let header = b64url(br#"{"typ":"JWT","alg":"ES256"}"#);
        let claims = serde_json::json!({ "aud": audience, "exp": exp_secs, "sub": subject });
        let claims = b64url(claims.to_string().as_bytes());
        let signing_input = format!("{header}.{claims}");
        let signature: Signature = self.signing.sign(signing_input.as_bytes());
        // JOSE wants the fixed-size r || s form, not DER.
        format!("{signing_input}.{}", b64url(&signature.to_bytes()))
    }

    /// `scheme://host[:port]` of a push endpoint, the JWT `aud`. Default ports
    /// are dropped, as a browser's `URL.origin` would.
    pub fn audience_of(endpoint: &str) -> Option<String> {
        let endpoint = endpoint.trim();
        let (scheme, rest) = endpoint.split_once("://")?;
        let scheme = scheme.to_ascii_lowercase();
        if scheme != "https" && scheme != "http" {
            return None;
        }
        let authority = rest.split(['/', '?', '#']).next()?;
        let authority = authority.rsplit('@').next()?.to_ascii_lowercase();
        if authority.is_empty() {
            return None;
        }
        let authority = match (
            scheme.as_str(),
            authority.strip_suffix(":443"),
            authority.strip_suffix(":80"),
        ) {
            ("https", Some(host), _) => host.to_string(),
            ("http", _, Some(host)) => host.to_string(),
            _ => authority,
        };
        Some(format!("{scheme}://{authority}"))
    }

    /// `Authorization: vapid t=<jwt>, k=<public key>` for an endpoint, valid
    /// for [`JWT_TTL_SECS`] from `now_secs`.
    pub fn authorization_header(
        &self,
        endpoint: &str,
        subject: &str,
        now_secs: u64,
    ) -> Option<String> {
        let aud = Self::audience_of(endpoint)?;
        Some(self.authorization_for_audience(&aud, subject, now_secs))
    }

    pub(crate) fn authorization_for_audience(
        &self,
        aud: &str,
        subject: &str,
        now_secs: u64,
    ) -> String {
        let jwt = self.jwt(aud, subject, now_secs + JWT_TTL_SECS);
        format!("vapid t={jwt}, k={}", self.public_b64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use p256::ecdsa::signature::Verifier;
    use p256::ecdsa::VerifyingKey;

    #[test]
    fn derivation_is_deterministic_and_secret_specific() {
        let a = VapidKeys::from_secret("s3cret").unwrap();
        let b = VapidKeys::from_secret("s3cret").unwrap();
        let c = VapidKeys::from_secret("other").unwrap();
        assert_eq!(a.public_key_b64url(), b.public_key_b64url());
        assert_eq!(a.kid(), b.kid());
        assert_ne!(a.public_key_b64url(), c.public_key_b64url());
        assert_ne!(a.kid(), c.kid());
        assert_eq!(a.public_key_bytes().len(), 65);
        assert_eq!(a.public_key_bytes()[0], 0x04);
        assert_eq!(a.kid().len(), 16);
        assert_eq!(
            VapidKeys::from_secret("  ").unwrap_err(),
            VapidError::EmptySecret
        );
    }

    /// Pinned so a change to the derivation (salt, info, encoding) cannot
    /// slip through: it would silently invalidate every subscription of
    /// every project. Cross-checked against an independent HKDF + P-256
    /// implementation (Python `cryptography`) when it was pinned.
    #[test]
    fn known_answer() {
        let keys = VapidKeys::from_secret("sp00ky-test-secret").unwrap();
        assert_eq!(keys.public_key_b64url(), KAT_PUBLIC);
        assert_eq!(keys.kid(), KAT_KID);
    }

    const KAT_PUBLIC: &str =
        "BGEt-XFiIkykESxg2av2u3BFQjapyTTWIdhiicsTmaUKKvjU5BgNiLzEWH4EO4G6u6_rKAzjbeZgpIFmvOIn2SM";
    const KAT_KID: &str = "d304e3832c963e76";

    #[test]
    fn private_key_round_trip() {
        let a = VapidKeys::from_secret("s3cret").unwrap();
        let priv_b64 = a.private_key_b64url();
        let b = VapidKeys::from_private_b64url(&priv_b64).unwrap();
        assert_eq!(a.public_key_b64url(), b.public_key_b64url());
        // Padded and standard-alphabet forms are accepted too.
        let std_b64 = base64::engine::general_purpose::STANDARD
            .encode(URL_SAFE_NO_PAD.decode(&priv_b64).unwrap());
        assert_eq!(
            VapidKeys::from_private_b64url(&std_b64).unwrap().kid(),
            a.kid()
        );
        assert!(VapidKeys::from_private_b64url("AAAA").is_err());
        assert!(VapidKeys::from_private_bytes(&[0u8; 32]).is_err());
        assert!(VapidKeys::from_private_bytes(&[0xffu8; 32]).is_err());
        assert!(!format!("{a:?}").contains(&priv_b64));
    }

    #[test]
    fn jwt_verifies_and_carries_the_claims() {
        let keys = VapidKeys::from_secret("s3cret").unwrap();
        let jwt = keys.jwt(
            "https://fcm.googleapis.com",
            "mailto:ops@example.com",
            1_700_000_000,
        );
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header, serde_json::json!({"typ": "JWT", "alg": "ES256"}));
        let claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(
            claims,
            serde_json::json!({"aud": "https://fcm.googleapis.com", "exp": 1_700_000_000u64, "sub": "mailto:ops@example.com"})
        );
        let sig_bytes = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        assert_eq!(sig_bytes.len(), 64);
        let sig = Signature::from_slice(&sig_bytes).unwrap();
        let vk = VerifyingKey::from_sec1_bytes(keys.public_key_bytes()).unwrap();
        vk.verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig)
            .expect("signature verifies");
        assert!(vk.verify(b"tampered", &sig).is_err());
    }

    #[test]
    fn audiences() {
        let aud = VapidKeys::audience_of;
        assert_eq!(
            aud("https://fcm.googleapis.com/fcm/send/abc").as_deref(),
            Some("https://fcm.googleapis.com")
        );
        assert_eq!(
            aud("https://updates.push.services.mozilla.com/wpush/v2/x?y").as_deref(),
            Some("https://updates.push.services.mozilla.com")
        );
        assert_eq!(
            aud("https://web.push.apple.com:443/x").as_deref(),
            Some("https://web.push.apple.com")
        );
        assert_eq!(
            aud("http://localhost:8080/push").as_deref(),
            Some("http://localhost:8080")
        );
        assert_eq!(
            aud("HTTPS://Push.Example.COM").as_deref(),
            Some("https://push.example.com")
        );
        assert_eq!(aud("ftp://x/y"), None);
        assert_eq!(aud("not a url"), None);
        assert_eq!(aud("https:///x"), None);
    }

    #[test]
    fn authorization_header_shape() {
        let keys = VapidKeys::from_secret("s3cret").unwrap();
        let h = keys
            .authorization_header("https://push.example.com/abc", "mailto:a@b.c", 1000)
            .unwrap();
        assert!(h.starts_with("vapid t="));
        assert!(h.ends_with(&format!(", k={}", keys.public_key_b64url())));
        let jwt = h.trim_start_matches("vapid t=").split(',').next().unwrap();
        let claims: serde_json::Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(jwt.split('.').nth(1).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claims["exp"], serde_json::json!(1000 + JWT_TTL_SECS));
        assert_eq!(claims["aud"], serde_json::json!("https://push.example.com"));
        assert!(keys
            .authorization_header("nope", "mailto:a@b.c", 1000)
            .is_none());
    }
}
