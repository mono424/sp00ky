//! `POST /admin/api/session` — the only way to obtain an admin bearer token.
//!
//! Two paths, both landing on the same session:
//!
//! * **Roster.** Sign in to the tenant database as the submitted user, through
//!   the app's own record-access method, then check that user against
//!   `_00_admin` — the roster `spky admin add` writes. This is what makes
//!   "every admin account set via the CLI" work with no extra credential.
//! * **Break-glass.** `SPKY_ADMIN_PASSWORD`, when set. The roster path needs a
//!   working tenant database, and a dead tenant database is precisely when an
//!   operator most needs to see the dashboard.
//!
//! Neither path ever hands the browser a database credential: the tenant JWT is
//! used for the roster check and dropped before the response is built.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::session::SessionMode;
use super::AdminState;

/// Attempts allowed per source address inside [`RATE_WINDOW`].
const RATE_LIMIT: usize = 10;
const RATE_WINDOW: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// Omitted for a break-glass login.
    #[serde(default)]
    pub username: Option<String>,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub mode: SessionMode,
    pub subject: String,
    pub label: String,
    pub expires_in_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: String,
}

fn error(status: StatusCode, msg: &str) -> (StatusCode, Json<ErrorBody>) {
    (
        status,
        Json(ErrorBody {
            error: msg.to_string(),
        }),
    )
}

/// The answer when the tenant database could not be reached at all.
///
/// Deliberately NOT [`unauthorized`]: see the call site. Kept beside it so the
/// two are read together — every future edit to one has to look at the other.
fn database_unavailable() -> (StatusCode, Json<ErrorBody>) {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "Cannot reach the tenant database, so no sign-in can be checked. \
         This is not a password problem — check the scheduler's log and the \
         database container.",
    )
}

/// The single 401 every failed login returns.
///
/// Deliberately identical for "no such user", "wrong password", "no record
/// access method exists" and "not on the roster is not distinguishable here".
/// A login endpoint that explains *why* it said no is a user enumeration
/// oracle. The scheduler's own logs carry the real reason.
fn unauthorized() -> (StatusCode, Json<ErrorBody>) {
    error(StatusCode::UNAUTHORIZED, "Invalid credentials")
}

/// Constant-time byte comparison, so a wrong break-glass password cannot be
/// recovered one character at a time from response timing. The length is not
/// hidden, which is fine — password length is not the secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Per-address attempt counter.
///
/// In-memory and per-process, like the session store. It exists to make online
/// password guessing expensive, not to be a distributed rate limiter — an
/// attacker with many source addresses is not what this defends against.
#[derive(Default)]
pub struct RateLimiter {
    attempts: Mutex<HashMap<IpAddr, Vec<Instant>>>,
}

impl RateLimiter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record an attempt. Returns false when the caller is over the limit.
    pub fn allow(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut map = self.attempts.lock().expect("rate limiter poisoned");

        // Prune every address on each call. The map only grows on login
        // attempts, so this stays cheap and needs no background sweeper.
        map.retain(|_, hits| {
            hits.retain(|t| now.duration_since(*t) < RATE_WINDOW);
            !hits.is_empty()
        });

        let hits = map.entry(ip).or_default();
        if hits.len() >= RATE_LIMIT {
            return false;
        }
        hits.push(now);
        true
    }

    /// Forget an address's attempts, called after a successful login so a
    /// legitimate operator who fumbled a few times is not locked out.
    pub fn clear(&self, ip: IpAddr) {
        self.attempts
            .lock()
            .expect("rate limiter poisoned")
            .remove(&ip);
    }
}

/// Read the `ID` claim (the `$auth.id` record id) out of a SurrealDB record
/// JWT, WITHOUT verifying the signature.
///
/// Safe in this specific context, and only here: SurrealDB minted this token
/// microseconds ago and handed it back over our own connection, so there is no
/// attacker-controlled path into this value. The claim is used purely to pick
/// which `_00_admin` row to look for, and that lookup runs as root against the
/// same database that issued the token. Mirrors `decodeTokenClaims` in
/// `packages/core/src/modules/auth/index.ts`.
fn record_id_from_jwt(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let decoded = base64_url_decode(payload)?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    claims
        .get("ID")
        .or_else(|| claims.get("id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Minimal unpadded base64url decoder. Written out rather than pulling in a
/// dependency for one call site.
fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    const INVALID: u8 = 0xFF;
    let mut table = [INVALID; 256];
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut i = 0usize;
    while i < alphabet.len() {
        table[alphabet[i] as usize] = i as u8;
        i += 1;
    }

    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &b in input.as_bytes() {
        if b == b'=' {
            break;
        }
        let v = table[b as usize];
        if v == INVALID {
            return None;
        }
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Record-access method names to try, in order.
///
/// `SPKY_ADMIN_ACCESS` pins it when a deployment knows its own schema. Without
/// that, ask the database: the access method is defined in the *app's* schema
/// (`DEFINE ACCESS account ON DATABASE TYPE RECORD ...`), so the scheduler
/// cannot hardcode a name and `INFO FOR DB` is the only authority.
async fn candidate_accesses(state: &AdminState) -> Vec<String> {
    if let Some(ac) = &state.config.access {
        return vec![ac.clone()];
    }

    let Some(db) = state.db() else {
        return Vec::new();
    };
    let handle = db.handle();
    let info: Option<serde_json::Value> = match handle.query("INFO FOR DB").await {
        Ok(mut resp) => resp.take(0).ok().flatten(),
        Err(e) => {
            db.note_error(&format!("{e:#}"));
            warn!(error = %e, "INFO FOR DB failed while discovering record accesses");
            return Vec::new();
        }
    };

    let Some(accesses) = info
        .as_ref()
        .and_then(|v| v.get("accesses"))
        .and_then(|v| v.as_object())
    else {
        return Vec::new();
    };

    accesses
        .iter()
        .filter(|(_, def)| {
            def.as_str()
                .map(|d| d.contains("TYPE RECORD"))
                .unwrap_or(false)
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// What one signin attempt concluded.
///
/// The distinction is the whole point: "this password is wrong" and "the database
/// is unreachable" are the same 401 to a browser, and conflating them is what
/// turns a database outage into an hour of hunting for a password problem. Only
/// the first is a credential answer; the second must never be one.
enum Signin {
    /// The access method accepted the credentials. Carries the tenant JWT.
    Accepted(String),
    /// The access method ran and said no.
    Rejected,
    /// We never got to ask. Carries the reason, for the log and the operator.
    Unavailable(String),
}

/// Try one access method with both common parameter spellings.
///
/// Apps name the identifier field themselves — the shipped templates use
/// `$username`, plenty of real schemas use `$email` — and the SIGNIN clause
/// simply fails to match when the parameter it reads is absent. Trying both is
/// cheaper and far less brittle than asking the operator to configure it.
async fn try_signin(
    state: &AdminState,
    access: &str,
    username: &str,
    password: &str,
) -> Signin {
    for field in ["username", "email"] {
        // A dedicated connection per attempt, never the scheduler's shared root
        // handle: `signin` rebinds the session it runs on, so doing this on the
        // shared handle would silently downgrade the scheduler from root to a
        // record user for every other caller.
        let db = match maintenance::db::connect_http_raw(&state.db_config).await {
            Ok(db) => db,
            // `{:#}` and not `{}`: an anyhow error prints only its outermost
            // context, and here that context is always "Failed to open HTTP to
            // <url>" — which says where we were going and never what went wrong.
            // The chain underneath is the diagnosis.
            Err(e) => {
                let reason = format!("{e:#}");
                warn!(error = %reason, "Admin signin could not open a database connection");
                return Signin::Unavailable(reason);
            }
        };
        if let Err(e) = db
            .use_ns(&state.db_config.namespace)
            .use_db(&state.db_config.database)
            .await
        {
            let reason = format!("{e:#}");
            warn!(error = %reason, "Admin signin could not select the tenant namespace");
            return Signin::Unavailable(reason);
        }

        let mut params = HashMap::new();
        params.insert(field.to_string(), username.to_string());
        params.insert("password".to_string(), password.to_string());

        match db
            .signin(surrealdb::opt::auth::Record {
                namespace: state.db_config.namespace.clone(),
                database: state.db_config.database.clone(),
                access: access.to_string(),
                params,
            })
            .await
        {
            Ok(token) => {
                return Signin::Accepted(token.access.as_insecure_token().to_string())
            }
            Err(e) => {
                debug!(access, field, error = %e, "Admin record signin attempt failed");
            }
        }
    }
    Signin::Rejected
}

/// Is this record id on the `_00_admin` roster?
///
/// Runs as root deliberately. `_00_admin` is selectable by its owning user, so
/// the record session could ask this itself — but doing it as root means one
/// answer regardless of how the app's own permissions are shaped, and keeps the
/// roster check independent of the token we just minted.
async fn is_admin(state: &AdminState, user_id: &str) -> bool {
    let Some(db) = state.db() else {
        return false;
    };
    let handle = db.handle();
    let result: Result<Vec<serde_json::Value>, _> = handle
        .query("SELECT VALUE id FROM _00_admin WHERE <string>user = $uid LIMIT 1;")
        .bind(("uid", user_id.to_string()))
        .await
        .and_then(|mut r| r.take(0));

    match result {
        Ok(rows) => !rows.is_empty(),
        Err(e) => {
            db.note_error(&format!("{e:#}"));
            warn!(error = %e, "Roster lookup against _00_admin failed");
            false
        }
    }
}

pub async fn login(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorBody>)> {
    let ip = peer.ip();
    if !state.rate_limiter.allow(ip) {
        warn!(%ip, "Admin login rate limit exceeded");
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many attempts. Try again later.",
        ));
    }

    let ttl = state.config.session_ttl.as_secs();

    // Break-glass: password only, and only when one is configured.
    let Some(username) = req.username.as_ref().filter(|u| !u.is_empty()) else {
        let Some(expected) = state.config.password.as_ref() else {
            debug!("Password-only login attempted but SPKY_ADMIN_PASSWORD is unset");
            return Err(unauthorized());
        };
        if !constant_time_eq(expected.as_bytes(), req.password.as_bytes()) {
            warn!(%ip, "Break-glass login failed");
            return Err(unauthorized());
        }
        state.rate_limiter.clear(ip);
        let token = state.sessions.create(
            "breakglass".to_string(),
            "Break-glass".to_string(),
            SessionMode::Breakglass,
        );
        info!(%ip, "Break-glass admin login");
        return Ok(Json(LoginResponse {
            token,
            mode: SessionMode::Breakglass,
            subject: "breakglass".to_string(),
            label: "Break-glass".to_string(),
            expires_in_secs: ttl,
        }));
    };

    // Roster path.
    let accesses = candidate_accesses(&state).await;
    if accesses.is_empty() {
        warn!(
            "No record access method available for admin signin (set SPKY_ADMIN_ACCESS \
             if the database is reachable but has none defined)"
        );
        return Err(unauthorized());
    }

    let mut jwt = None;
    let mut unavailable = None;
    for access in &accesses {
        match try_signin(&state, access, username, &req.password).await {
            Signin::Accepted(token) => {
                jwt = Some(token);
                break;
            }
            Signin::Rejected => {}
            // Stop at the first one: every access method signs in through the
            // same database, so if that database cannot be reached, trying the
            // rest is a slower way to learn the same thing.
            Signin::Unavailable(reason) => {
                unavailable = Some(reason);
                break;
            }
        }
    }

    // A database we could not reach is not a credential verdict. Answering 401
    // here is what made a database outage read as "your password is wrong" —
    // for everyone, at once, with the real reason only in the container log.
    // 503 says whose problem it is, and leaks nothing: it is the same answer for
    // a username that exists and one that does not.
    if let Some(reason) = unavailable {
        warn!(%ip, username, error = %reason, "Admin login could not reach the tenant database");
        return Err(database_unavailable());
    }

    let Some(jwt) = jwt else {
        warn!(%ip, username, "Admin login failed: no access method accepted the credentials");
        return Err(unauthorized());
    };

    let Some(user_id) = record_id_from_jwt(&jwt) else {
        warn!("Signed in but the token carried no ID claim");
        return Err(unauthorized());
    };
    // The tenant token has done its job. Nothing below needs it, and it is
    // never handed to the browser.
    drop(jwt);

    if !is_admin(&state, &user_id).await {
        warn!(%ip, username, user_id, "Authenticated but not on the _00_admin roster");
        return Err(error(
            StatusCode::FORBIDDEN,
            "This account is not an administrator. Grant access with `spky admin add`.",
        ));
    }

    state.rate_limiter.clear(ip);
    let token = state
        .sessions
        .create(user_id.clone(), username.clone(), SessionMode::Roster);
    info!(%ip, username, user_id, "Admin login");

    Ok(Json(LoginResponse {
        token,
        mode: SessionMode::Roster,
        subject: user_id,
        label: username.clone(),
        expires_in_secs: ttl,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A database outage must never be reported as a credential verdict.
    ///
    /// This is the regression that cost hours: every login answered
    /// `401 Invalid credentials` while the tenant database was refusing new
    /// sessions, so the outage read as "my password stopped working" and the only
    /// trace of the truth was a warning in the scheduler's container log.
    #[test]
    fn an_unreachable_database_is_not_a_credential_answer() {
        let (status, body) = database_unavailable();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(status, unauthorized().0);
        assert!(
            body.error.contains("not a password problem"),
            "the message has to send the reader at the database, not at their password: {}",
            body.error
        );
        // And the credential answer stays the anti-enumeration 401 it was.
        assert_eq!(unauthorized().0, StatusCode::UNAUTHORIZED);
        assert_eq!(unauthorized().1.error, "Invalid credentials");
    }

    /// The same 503 whether or not the username exists — the outage answer must
    /// not become the enumeration oracle the 401 was designed to avoid.
    #[test]
    fn the_unavailable_answer_carries_nothing_about_the_account() {
        let (_, body) = database_unavailable();
        assert!(!body.error.contains("user"), "{}", body.error);
        assert!(!body.error.contains("account"), "{}", body.error);
    }

    #[test]
    fn constant_time_eq_matches_semantics_of_eq() {
        assert!(constant_time_eq(b"hunter2", b"hunter2"));
        assert!(!constant_time_eq(b"hunter2", b"hunter3"));
        assert!(!constant_time_eq(b"hunter2", b"hunter22"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn base64_url_decodes_unpadded_input() {
        // {"ID":"user:abc"}
        assert_eq!(
            base64_url_decode("eyJJRCI6InVzZXI6YWJjIn0").unwrap(),
            br#"{"ID":"user:abc"}"#.to_vec()
        );
    }

    #[test]
    fn base64_url_rejects_non_alphabet_bytes() {
        assert!(base64_url_decode("not base64!").is_none());
    }

    #[test]
    fn record_id_is_read_from_either_claim_spelling() {
        // header.payload.signature, payload = {"ID":"user:abc"}
        let jwt = "x.eyJJRCI6InVzZXI6YWJjIn0.y";
        assert_eq!(record_id_from_jwt(jwt).as_deref(), Some("user:abc"));
        // payload = {"id":"user:xyz"}
        let lower = "x.eyJpZCI6InVzZXI6eHl6In0.y";
        assert_eq!(record_id_from_jwt(lower).as_deref(), Some("user:xyz"));
    }

    #[test]
    fn record_id_from_malformed_jwt_is_none() {
        assert!(record_id_from_jwt("nodots").is_none());
        assert!(record_id_from_jwt("a.!!!.c").is_none());
        // Valid base64, but no ID claim.
        assert!(record_id_from_jwt("a.eyJ4IjoxfQ.c").is_none());
    }

    #[test]
    fn rate_limiter_blocks_after_the_limit_and_clears_on_success() {
        let rl = RateLimiter::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        for _ in 0..RATE_LIMIT {
            assert!(rl.allow(ip));
        }
        assert!(!rl.allow(ip), "the 11th attempt must be refused");

        rl.clear(ip);
        assert!(rl.allow(ip), "a cleared address starts over");
    }

    #[test]
    fn rate_limiter_is_per_address() {
        let rl = RateLimiter::new();
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        for _ in 0..RATE_LIMIT {
            assert!(rl.allow(a));
        }
        assert!(!rl.allow(a));
        assert!(rl.allow(b), "one address must not lock out another");
    }
}
