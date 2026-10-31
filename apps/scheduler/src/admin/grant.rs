//! Signing in with a Sp00ky Cloud console grant.
//!
//! The console never holds a password for this scheduler. It asks the
//! control plane for a grant (a short Ed25519 JWT for this project, see
//! [`ssp_protocol::console_grant`]) and posts it to `POST /admin/api/session`,
//! which exchanges it for an ordinary admin session of mode `cloud`. Who may
//! do that is decided by the control plane (workspace membership); this side
//! only checks that the control plane said so, for this project, just now,
//! and only once.
//!
//! Keys come from the control plane's JWKS: inline from the environment when
//! the deploy put them there, otherwise fetched from
//! `{SPKY_CLOUD_API_URL}/.well-known/jwks.json`. A grant under a `kid` this
//! process has not seen triggers a refetch (at most every
//! [`REFETCH_EVERY`]), which is how a key rotation reaches running
//! schedulers without a redeploy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ssp_protocol::console_grant::{self, GrantClaims, GrantError, Jwks, LEEWAY_SECS};
use tracing::{info, warn};

use super::cloud::CloudLink;
use super::config::AdminConfig;

/// The fastest a scheduler re-reads the control plane's keys. A stream of
/// grants under a bogus `kid` cannot turn this host into a fetch amplifier.
const REFETCH_EVERY: Duration = Duration::from_secs(30);
/// Grants remembered for replay protection. Each is forgotten once it
/// expires, so this only fills under an attack the login rate limit already
/// slows.
const MAX_REMEMBERED: usize = 10_000;

pub struct GrantVerifier {
    issuer: String,
    audience: String,
    jwks_url: Option<String>,
    inline: Jwks,
    fetched: RwLock<Jwks>,
    last_fetch: Mutex<Option<Instant>>,
    /// `jti` -> `exp` of every grant exchanged and not yet expired.
    used: Mutex<HashMap<String, u64>>,
    client: reqwest::Client,
    /// How long the session a grant opens lasts.
    pub session_ttl: Duration,
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

impl GrantVerifier {
    /// The verifier for a cloud-linked scheduler, or `None` when console
    /// sign-in is off or there is no link (a self-hosted scheduler).
    pub fn from_config(config: &AdminConfig, cloud: Option<&CloudLink>) -> Option<Arc<Self>> {
        if !config.cloud_login {
            return None;
        }
        let cloud = cloud?;
        let inline = match config.cloud_jwks.as_deref() {
            Some(raw) => match serde_json::from_str::<Jwks>(raw) {
                Ok(jwks) => jwks,
                Err(e) => {
                    warn!(error = %e, "SPKY_ADMIN_CLOUD_JWKS is not a JWKS document; fetching keys instead");
                    Jwks::default()
                }
            },
            None => Jwks::default(),
        };
        let issuer = config.cloud_issuer.clone().unwrap_or_else(|| cloud.api_url.clone());
        info!(project = %cloud.project, %issuer, "Sp00ky Cloud console sign-in enabled");
        Some(Arc::new(Self::new(
            issuer,
            &cloud.project,
            inline,
            Some(format!("{}/.well-known/jwks.json", cloud.api_url)),
            config.cloud_session_ttl.min(config.session_ttl),
        )))
    }

    pub fn new(issuer: String, project: &str, inline: Jwks, jwks_url: Option<String>, session_ttl: Duration) -> Self {
        Self {
            issuer,
            audience: console_grant::audience(project),
            jwks_url,
            inline,
            fetched: RwLock::new(Jwks::default()),
            last_fetch: Mutex::new(None),
            used: Mutex::new(HashMap::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            session_ttl,
        }
    }

    fn keys(&self) -> Jwks {
        let mut keys = self.inline.clone();
        if let Ok(fetched) = self.fetched.read() {
            keys.keys.extend(fetched.keys.iter().cloned());
        }
        keys
    }

    /// Re-read the control plane's keys, unless that happened moments ago.
    async fn refetch(&self) -> bool {
        let Some(url) = self.jwks_url.as_deref() else { return false };
        {
            let mut last = self.last_fetch.lock().expect("jwks clock poisoned");
            if last.is_some_and(|t| t.elapsed() < REFETCH_EVERY) {
                return false;
            }
            *last = Some(Instant::now());
        }
        let fetched = async {
            let res = self.client.get(url).send().await?.error_for_status()?;
            res.json::<Jwks>().await
        }
        .await;
        match fetched {
            Ok(jwks) => {
                info!(keys = jwks.keys.len(), "Fetched Sp00ky Cloud grant keys");
                *self.fetched.write().expect("jwks poisoned") = jwks;
                true
            }
            Err(e) => {
                warn!(error = %e, url, "Could not fetch Sp00ky Cloud grant keys");
                false
            }
        }
    }

    /// The claims of a valid, unused grant. The error is safe to show.
    pub async fn verify(&self, token: &str) -> Result<GrantClaims, GrantError> {
        let now = now_secs();
        let check = |keys: &Jwks| console_grant::verify(token, keys, &self.issuer, &self.audience, now);
        let claims = match check(&self.keys()) {
            Err(GrantError::UnknownKey(kid)) => {
                if self.refetch().await {
                    check(&self.keys())?
                } else {
                    return Err(GrantError::UnknownKey(kid));
                }
            }
            other => other?,
        };

        let mut used = self.used.lock().expect("grant memory poisoned");
        used.retain(|_, exp| exp.saturating_add(LEEWAY_SECS) > now);
        if used.contains_key(&claims.jti) {
            return Err(GrantError::Claims("grant was already used"));
        }
        if used.len() >= MAX_REMEMBERED {
            return Err(GrantError::Claims("too many grants in flight; try again shortly"));
        }
        used.insert(claims.jti.clone(), claims.exp);
        Ok(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SECRET: [u8; 32] = [3; 32];

    fn verifier() -> GrantVerifier {
        GrantVerifier::new(
            "https://api.example".into(),
            "p1",
            Jwks { keys: vec![console_grant::public_jwk("k1", &SECRET)] },
            None,
            Duration::from_secs(900),
        )
    }

    fn grant(jti: &str) -> String {
        let now = now_secs();
        console_grant::sign(
            &GrantClaims {
                iss: "https://api.example".into(),
                aud: json!(console_grant::audience("p1")),
                sub: "acct".into(),
                name: None,
                scope: "full".into(),
                perms: vec![],
                tenant: None,
                jti: jti.into(),
                iat: now,
                exp: now + 60,
                nbf: None,
                auth_time: None,
            },
            "k1",
            &SECRET,
        )
    }

    #[tokio::test]
    async fn a_grant_works_once() {
        let v = verifier();
        let token = grant("g1");
        assert!(v.verify(&token).await.is_ok());
        assert_eq!(v.verify(&token).await, Err(GrantError::Claims("grant was already used")));
        assert!(v.verify(&grant("g2")).await.is_ok(), "another grant is fine");
    }

    #[tokio::test]
    async fn an_unknown_key_without_a_jwks_url_is_refused() {
        let v = GrantVerifier::new("https://api.example".into(), "p1", Jwks::default(), None, Duration::from_secs(900));
        assert!(matches!(v.verify(&grant("g1")).await, Err(GrantError::UnknownKey(_))));
    }
}
