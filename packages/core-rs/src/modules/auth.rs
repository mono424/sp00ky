//! Auth state management (TS `AuthService`).
//!
//! PORT: `packages/spooky_core/lib/src/modules/auth/{auth_service,sp00ky_auth}.dart`.
//! The TS conditional access-param types collapse to runtime validation
//! against [`crate::Schema::access`]. Listeners are a plain list, called
//! synchronously (the Dart event system deferred them to a microtask).
//!
//! The service talks to the server through [`AuthRemote`] (the transport's
//! `RemoteDatabaseService` implements it) and keeps the token through
//! [`AuthPersistence`] (the shape of `services::persistence::PersistenceClient`,
//! defined here so this module does not depend on it).

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::{Rc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::future::{join_all, FutureExt, LocalBoxFuture, Shared};

use crate::error::{Error, Result};
use crate::runtime::Subscription;
use crate::types::{AccessDef, AccessMethod, Schema};
use crate::utils::{classify_error, ErrorKind};
use crate::value::{Object, Row, Value};

/// The persistence key of the session token.
pub const TOKEN_KEY: &str = "sp00ky_auth_token";

/// The access name the platform issues impersonation sessions under (TS
/// `IMPERSONATION_ACCESS`). Per-device state (push registrations) must never
/// be attached to a user while an operator is looking through their eyes.
pub const IMPERSONATION_ACCESS: &str = "_00_impersonate";

/// How long a sign-out waits for the server to drop the session.
const INVALIDATE_TIMEOUT: Duration = Duration::from_secs(2);

// ---- seams ---------------------------------------------------------------------------

/// Exactly the remote calls [`AuthService`] makes.
#[async_trait(?Send)]
pub trait AuthRemote {
    /// The `signin` RPC with `{access, variables}`; answers the token (a string
    /// or `{access: token}`).
    async fn signin(&self, params: Object) -> Result<Value>;
    /// The `signup` RPC with `{access, variables}`.
    async fn signup(&self, params: Object) -> Result<Value>;
    /// Authenticate the socket with `token`.
    async fn authenticate(&self, token: &str) -> Result<Value>;
    /// Drop the server session.
    async fn invalidate(&self) -> Result<()>;
    /// `SELECT * FROM ONLY $auth.id`, one value per statement. Must NOT wait on
    /// the identity gate a session transition holds: sign-in runs it inside
    /// one.
    async fn query_auth_user(&self) -> Result<Vec<Value>>;
    /// Hold app RPCs while the session's identity changes; returns a ticket for
    /// [`AuthRemote::end_session_transition`].
    fn begin_session_transition(&self) -> u64;
    fn end_session_transition(&self, transition: u64);
    /// Tear the socket down so it reconnects under the still-current session.
    async fn force_close(&self);
    /// The token a rebuilt socket authenticates with.
    fn set_auth_token(&self, token: Option<&str>);
}

/// Per-device key/value persistence, as `PersistenceClient` defines it.
#[async_trait(?Send)]
pub trait AuthPersistence {
    async fn set(&self, key: &str, value: Value) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Value>>;
    async fn remove(&self, key: &str) -> Result<()>;
}

/// [`AuthPersistence`] in process memory (tests, a client with no store).
#[derive(Default)]
pub struct MemoryPersistence(RefCell<BTreeMap<String, Value>>);

#[async_trait(?Send)]
impl AuthPersistence for MemoryPersistence {
    async fn set(&self, key: &str, value: Value) -> Result<()> {
        self.0.borrow_mut().insert(key.to_string(), value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Value>> {
        Ok(self.0.borrow().get(key).cloned())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.0.borrow_mut().remove(key);
        Ok(())
    }
}

/// Called with the signed-in user id (or `None`) on every auth change.
pub type AuthListener = Rc<dyn Fn(Option<&str>)>;

/// What a module needs from auth: the user id now and on every change.
pub trait AuthSource {
    /// Fires immediately with the current user id, then on every change, until
    /// the guard drops.
    fn subscribe_user(&self, listener: AuthListener) -> Subscription;
}

/// Who is signing out, captured before anything is cleared.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SignOutContext {
    pub user_id: Option<String>,
    pub token: Option<String>,
    pub impersonating: bool,
}

/// Runs while the session is still valid, so the hook can reach the server as
/// the leaving user. Each hook bounds itself; one failing never blocks the
/// others or the sign-out.
pub type SignOutHook = Rc<dyn Fn(SignOutContext) -> LocalBoxFuture<'static, Result<()>>>;

/// Runs on every session change before listeners hear of it: the client flips
/// its local account (bucket) here, so a listener that registers a query
/// already sees the new account's store.
pub type SessionChangedHook = Rc<dyn Fn(Option<String>) -> LocalBoxFuture<'static, Result<()>>>;

/// A deferred verification failure. The restored session remains usable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthVerificationError {
    pub kind: ErrorKind,
    pub message: String,
}

impl std::fmt::Display for AuthVerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

// ---- service ----------------------------------------------------------------------------

#[derive(Default)]
struct State {
    token: Option<String>,
    current_user: Option<Rc<Row>>,
    is_authenticated: bool,
    is_loading: bool,
    /// The record-access method the session was opened with (TS `auth.access`),
    /// e.g. "account". Needed for SSP permission injection: a table permission
    /// written against `$access` cannot resolve locally without it. Set on
    /// sign-in, and recovered from the token's `AC` claim on [`AuthService::check`]
    /// so it survives a restart (where no sign-in call happens).
    access: Option<String>,
    needs_verification: bool,
    verification_error: Option<AuthVerificationError>,
}

type CheckFuture = Shared<LocalBoxFuture<'static, Result<()>>>;

struct Inner {
    access: BTreeMap<String, AccessDef>,
    remote: Rc<dyn AuthRemote>,
    persistence: Rc<dyn AuthPersistence>,
    state: RefCell<State>,
    generation: Cell<u64>,
    /// The one in-flight re-verification of the stored token, shared by every
    /// concurrent `check(None)`.
    checking: RefCell<Option<(u64, CheckFuture)>>,
    next_id: Cell<u64>,
    listeners: RefCell<Vec<(u64, AuthListener)>>,
    hooks: RefCell<Vec<(u64, SignOutHook)>>,
    on_session_changed: RefCell<Option<SessionChangedHook>>,
}

impl Inner {
    fn next_id(&self) -> u64 {
        self.next_id.set(self.next_id.get() + 1);
        self.next_id.get()
    }
}

/// Auth state for one client. Cheap to clone; clones share one state. Lives on
/// the engine thread.
#[derive(Clone)]
pub struct AuthService(Rc<Inner>);

impl AuthService {
    pub fn new(
        schema: &Schema,
        remote: Rc<dyn AuthRemote>,
        persistence: Rc<dyn AuthPersistence>,
    ) -> Self {
        AuthService(Rc::new(Inner {
            access: schema.access.clone(),
            remote,
            persistence,
            state: RefCell::new(State {
                is_loading: true,
                ..State::default()
            }),
            generation: Cell::new(0),
            checking: RefCell::new(None),
            next_id: Cell::new(0),
            listeners: RefCell::new(Vec::new()),
            hooks: RefCell::new(Vec::new()),
            on_session_changed: RefCell::new(None),
        }))
    }

    // ---- snapshot ------------------------------------------------------------------------

    pub fn token(&self) -> Option<String> {
        self.0.state.borrow().token.clone()
    }

    /// The signed-in user's row. A session restored from the token holds only
    /// its `id` until the server answers.
    pub fn current_user(&self) -> Option<Rc<Row>> {
        self.0.state.borrow().current_user.clone()
    }

    /// The signed-in user's record id (`user:abc`).
    pub fn user_id(&self) -> Option<String> {
        self.0
            .state
            .borrow()
            .current_user
            .as_ref()
            .and_then(|u| u.get("id"))
            .filter(|v| !v.is_nullish())
            .map(Value::to_id_string)
    }

    pub fn is_authenticated(&self) -> bool {
        self.0.state.borrow().is_authenticated
    }

    pub fn is_loading(&self) -> bool {
        self.0.state.borrow().is_loading
    }

    pub fn access(&self) -> Option<String> {
        self.0.state.borrow().access.clone()
    }

    /// The session was restored from the cached token and the server has not
    /// confirmed it yet.
    pub fn needs_verification(&self) -> bool {
        self.0.state.borrow().needs_verification
    }

    pub fn verification_error(&self) -> Option<AuthVerificationError> {
        self.0.state.borrow().verification_error.clone()
    }

    /// The session was opened through the platform's impersonation access.
    pub fn is_impersonating(&self) -> bool {
        let s = self.0.state.borrow();
        s.is_authenticated && s.access.as_deref() == Some(IMPERSONATION_ACCESS)
    }

    /// The declared `DEFINE ACCESS` of `name`.
    pub fn access_definition(&self, name: &str) -> Option<&AccessDef> {
        self.0.access.get(name)
    }

    // ---- listeners ---------------------------------------------------------------------

    /// Subscribe to auth state. Fires immediately with the current user id.
    pub fn subscribe(&self, listener: impl Fn(Option<&str>) + 'static) -> Subscription {
        self.subscribe_user(Rc::new(listener))
    }

    /// Run on every session change, before listeners are told.
    pub fn set_on_session_changed(&self, hook: Option<SessionChangedHook>) {
        *self.0.on_session_changed.borrow_mut() = hook;
    }

    /// Register work that must happen before the session is dropped (drain the
    /// outbox, unregister this device from push).
    ///
    /// Sign-out flips the bucket, and a bucket switch abandons the outgoing
    /// outbox in the old store: it is only picked up again the next time that
    /// user signs in, long after the screen that made the write is gone.
    /// Pushing after the token is cleared is not an option either: the
    /// statements would run unauthenticated and come back as rejections, which
    /// roll the writes back. So the flush has to happen first, and it is
    /// best-effort: a failure must never block signing out.
    pub fn on_before_sign_out(&self, hook: SignOutHook) -> Subscription {
        let id = self.0.next_id();
        self.0.hooks.borrow_mut().push((id, hook));
        let weak = Rc::downgrade(&self.0);
        Subscription::new(move || {
            if let Some(inner) = weak.upgrade() {
                inner.hooks.borrow_mut().retain(|(i, _)| *i != id);
            }
        })
    }

    /// Detach: in-flight work is fenced off, hooks dropped.
    pub fn dispose(&self) {
        self.bump();
        *self.0.on_session_changed.borrow_mut() = None;
        self.0.hooks.borrow_mut().clear();
    }

    fn generation(&self) -> u64 {
        self.0.generation.get()
    }

    fn bump(&self) -> u64 {
        self.0.generation.set(self.0.generation.get() + 1);
        self.0.generation.get()
    }

    fn notify(&self) {
        let user = self.user_id();
        let listeners: Vec<AuthListener> = self
            .0
            .listeners
            .borrow()
            .iter()
            .map(|(_, l)| l.clone())
            .collect();
        for l in listeners {
            l(user.as_deref());
        }
    }

    /// Run the session-changed hook, then tell listeners (unless the session
    /// moved on meanwhile).
    pub async fn publish_session(&self) -> Result<()> {
        let generation = self.generation();
        let hook = self.0.on_session_changed.borrow().clone();
        if let Some(hook) = hook {
            hook(self.user_id()).await?;
        }
        if generation == self.generation() {
            self.notify();
        }
        Ok(())
    }

    // ---- session -----------------------------------------------------------------------

    pub async fn init(&self) -> Result<()> {
        self.check(None).await
    }

    async fn stored_token(&self) -> Result<Option<String>> {
        Ok(match self.0.persistence.get(TOKEN_KEY).await? {
            Some(Value::String(s)) => Some(s),
            _ => None,
        })
    }

    /// Restore a session from the cached token WITHOUT a round trip, returning
    /// the user id it names.
    ///
    /// The server still enforces the token on every request; this only lets
    /// the client act on what it already holds so a warm boot paints as the
    /// right user before the network answers. [`AuthService::check`] replaces
    /// the user row wholesale once the server does answer.
    pub async fn restore_session_from_token(&self, notify: bool) -> Result<Option<String>> {
        self.0.state.borrow_mut().is_loading = false;
        let Some(token) = self.stored_token().await? else {
            return Ok(None);
        };
        let claims = claims_from_token(&token);
        let Some(user_id) = claims.user_id else {
            return Ok(None);
        };
        // Hand it to the transport too, so a socket rebuilt from scratch later
        // (the supervisor's revive loop) comes back authenticated. Without this
        // the client kept reporting this user while its session was anonymous,
        // and every view registered afterwards was stamped with an empty
        // identity.
        self.0.remote.set_auth_token(Some(&token));
        {
            let mut s = self.0.state.borrow_mut();
            s.token = Some(token);
            s.needs_verification = true;
            // Only the id: the full row is not in the token. It lands from the
            // local cache when the app's own `user` query paints.
            let mut user = Row::new();
            user.insert("id".into(), Value::String(user_id.clone()));
            s.current_user = Some(Rc::new(user));
            s.is_authenticated = true;
            s.access = claims.access;
            s.is_loading = false;
        }
        if notify {
            self.publish_session().await?;
        }
        tracing::debug!(target: "spooky", "Session restored optimistically from the cached token");
        Ok(Some(user_id))
    }

    /// Validate a token with the server and open the session as its account
    /// (TS `auth.check(token)`). Without `token` it re-verifies the stored one,
    /// sharing one round trip between concurrent callers. Use it for a token
    /// minted outside the client, such as an OAuth exchange: unlike a raw
    /// `authenticate`, the session is persisted and listeners fire.
    pub async fn check(&self, token: Option<&str>) -> Result<()> {
        if let Some(token) = token {
            return self.check_inner(Some(token.to_string())).await;
        }
        let existing = self.0.checking.borrow().as_ref().map(|(_, f)| f.clone());
        if let Some(running) = existing {
            return running.await;
        }
        let seq = self.0.next_id();
        let weak: Weak<Inner> = Rc::downgrade(&self.0);
        let fut: CheckFuture = async move {
            let Some(inner) = weak.upgrade() else {
                return Err(Error::Closed);
            };
            let me = AuthService(inner);
            let out = me.check_inner(None).await;
            let mut checking = me.0.checking.borrow_mut();
            if checking.as_ref().is_some_and(|(s, _)| *s == seq) {
                *checking = None;
            }
            out
        }
        .boxed_local()
        .shared();
        *self.0.checking.borrow_mut() = Some((seq, fut.clone()));
        fut.await
    }

    async fn check_inner(&self, access_token: Option<String>) -> Result<()> {
        let generation = self.generation();
        let previous_token = self.token();
        self.0.state.borrow_mut().is_loading = true;
        let supplied = access_token.is_some();
        let out = match self.check_body(generation, access_token).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.check_failed(generation, error, supplied, previous_token.is_some())
                    .await
            }
        };
        if generation == self.generation() {
            self.0.state.borrow_mut().is_loading = false;
            self.notify();
        }
        out
    }

    async fn check_body(&self, generation: u64, access_token: Option<String>) -> Result<()> {
        let token = match access_token {
            Some(t) => Some(t),
            None => self.stored_token().await?,
        };
        if generation != self.generation() {
            return Ok(());
        }
        let Some(token) = token else {
            self.0.state.borrow_mut().needs_verification = false;
            return Ok(());
        };
        self.0.remote.authenticate(&token).await?;
        if generation != self.generation() {
            return Ok(());
        }
        let user = self.fetch_auth_user().await?;
        if generation != self.generation() {
            return Ok(());
        }
        match user {
            Some(user) if user.get("id").is_some_and(|v| !v.is_nullish()) => {
                self.set_session(token, user).await?;
                if generation == self.generation() {
                    let mut s = self.0.state.borrow_mut();
                    s.needs_verification = false;
                    s.verification_error = None;
                }
                Ok(())
            }
            _ => Err(Error::Auth(
                "Invalid authentication: account not found".into(),
            )),
        }
    }

    async fn check_failed(
        &self,
        generation: u64,
        error: Error,
        supplied: bool,
        had_token: bool,
    ) -> Result<()> {
        if generation != self.generation() {
            return Ok(());
        }
        let kind = classify_error(&error);
        self.0.state.borrow_mut().verification_error = Some(AuthVerificationError {
            kind,
            message: error.to_string(),
        });
        let message = error.to_string().to_lowercase();
        let rejected = [
            "invalid authentication",
            "invalid token",
            "token expired",
            "authentication failed",
            "problem with authentication",
        ]
        .iter()
        .any(|p| message.contains(p));
        if rejected {
            if let Err(e) = self.sign_out_inner(false).await {
                tracing::debug!(target: "spooky", "Sign-out after a rejected token failed: {e}");
            }
        } else {
            self.0.state.borrow_mut().needs_verification = had_token;
            tracing::warn!(target: "spooky", "Auth verification deferred: {error}");
        }
        // A newly supplied credential must never appear to succeed offline.
        if supplied || !had_token || kind != ErrorKind::Network {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn fetch_auth_user(&self) -> Result<Option<Row>> {
        let result = self.0.remote.query_auth_user().await?;
        Ok(match result.into_iter().next() {
            Some(Value::Array(rows)) => rows.into_iter().next().and_then(Value::into_object),
            Some(Value::Object(row)) => Some(row),
            _ => None,
        })
    }

    async fn set_session(&self, token: String, user: Row) -> Result<()> {
        self.0.remote.set_auth_token(Some(&token));
        {
            let mut s = self.0.state.borrow_mut();
            // The token is authoritative for the access method, and is the only
            // source on a restored session (no sign-in call ran). Keep any
            // explicitly set value as the fallback for tokens without an AC
            // claim.
            s.access = claims_from_token(&token).access.or(s.access.take());
            s.token = Some(token.clone());
            s.current_user = Some(Rc::new(user));
            s.is_authenticated = true;
        }
        self.0
            .persistence
            .set(TOKEN_KEY, Value::String(token))
            .await?;
        // Listeners are told LAST: they may register `$auth`-gated queries
        // synchronously, and need token, user and access in place.
        self.0.state.borrow_mut().is_loading = false;
        self.publish_session().await
    }

    pub async fn sign_out(&self) -> Result<()> {
        self.sign_out_inner(true).await
    }

    async fn sign_out_inner(&self, flush: bool) -> Result<()> {
        let generation = self.bump();
        self.0.state.borrow_mut().is_loading = false;
        if flush {
            let context = SignOutContext {
                user_id: self.user_id(),
                token: self.token(),
                impersonating: self.is_impersonating(),
            };
            let hooks: Vec<SignOutHook> = self
                .0
                .hooks
                .borrow()
                .iter()
                .map(|(_, h)| h.clone())
                .collect();
            for result in join_all(hooks.iter().map(|h| h(context.clone()))).await {
                if let Err(e) = result {
                    tracing::debug!(target: "spooky", "A sign-out hook failed: {e}");
                }
            }
        }
        if generation != self.generation() {
            return Ok(());
        }
        let transition = self.0.remote.begin_session_transition();
        let out = async {
            {
                let mut s = self.0.state.borrow_mut();
                s.needs_verification = false;
                s.verification_error = None;
                s.token = None;
                s.current_user = None;
                s.is_authenticated = false;
                s.access = None;
            }
            self.0.remote.set_auth_token(None);
            self.0.persistence.remove(TOKEN_KEY).await?;
            self.publish_session().await?;
            // Local sign-out already cleared the session above; a failed remote
            // invalidate (server unreachable) must not block signing out.
            match tokio::time::timeout(INVALIDATE_TIMEOUT, self.0.remote.invalidate()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::debug!(target: "spooky", "Remote token invalidate failed during signOut: {e}"),
                Err(_) => tracing::debug!(target: "spooky", "Remote token invalidate timed out during signOut"),
            }
            Ok(())
        }
        .await;
        self.0.remote.end_session_transition(transition);
        out
    }

    pub async fn sign_up(&self, access: &str, params: Object) -> Result<()> {
        self.open_session(access, Method::SignUp, params).await
    }

    pub async fn sign_in(&self, access: &str, params: Object) -> Result<()> {
        self.open_session(access, Method::SignIn, params).await
    }

    async fn open_session(&self, access: &str, method: Method, params: Object) -> Result<()> {
        self.validate_access_params(access, method, &params)?;
        let generation = self.bump();
        let remote = self.0.remote.clone();
        let transition = remote.begin_session_transition();
        let out = async {
            let mut request = Object::new();
            request.insert("access".into(), Value::String(access.to_string()));
            request.insert("variables".into(), Value::Object(params));
            let result = match method {
                Method::SignIn => remote.signin(request).await?,
                Method::SignUp => remote.signup(request).await?,
            };
            if generation != self.generation() {
                remote.force_close().await;
                return Ok(());
            }
            let credential = extract_access_token(&result)
                .ok_or_else(|| Error::Auth("Authentication returned no token".into()))?;
            self.check(Some(&credential)).await
        }
        .await;
        if out.is_err() {
            // A failed verification may follow a successful socket sign-in.
            // Restore the transport to the still-current saved session before
            // app work runs.
            remote.force_close().await;
        }
        remote.end_session_transition(transition);
        out
    }

    fn validate_access_params(&self, access: &str, method: Method, params: &Object) -> Result<()> {
        let def = self.0.access.get(access).ok_or_else(|| {
            Error::InvalidArgument(format!("Access definition '{access}' not found"))
        })?;
        let declared: &AccessMethod = match method {
            Method::SignIn => &def.signin,
            Method::SignUp => &def.signup,
        };
        let missing: Vec<&str> = declared
            .params
            .iter()
            .filter(|(name, schema)| !schema.optional && !params.contains_key(name.as_str()))
            .map(|(name, _)| name.as_str())
            .collect();
        if !missing.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "Missing required {} params for '{access}': {}",
                method.as_str(),
                missing.join(", ")
            )));
        }
        Ok(())
    }
}

impl AuthSource for AuthService {
    fn subscribe_user(&self, listener: AuthListener) -> Subscription {
        listener(self.user_id().as_deref());
        let id = self.0.next_id();
        self.0.listeners.borrow_mut().push((id, listener));
        let weak = Rc::downgrade(&self.0);
        Subscription::new(move || {
            if let Some(inner) = weak.upgrade() {
                inner.listeners.borrow_mut().retain(|(i, _)| *i != id);
            }
        })
    }
}

#[derive(Clone, Copy)]
enum Method {
    SignIn,
    SignUp,
}

impl Method {
    fn as_str(self) -> &'static str {
        match self {
            Method::SignIn => "signIn",
            Method::SignUp => "signup",
        }
    }
}

fn extract_access_token(result: &Value) -> Option<String> {
    match result {
        Value::Object(o) => o
            .get("access")
            .filter(|v| !v.is_nullish())
            .map(Value::to_id_string),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// The `AC` (access method) and `ID` (auth record id) claims of a SurrealDB
/// record-access JWT.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenClaims {
    pub access: Option<String>,
    pub user_id: Option<String>,
}

/// Read the claims of a JWT WITHOUT verifying it (verification is the
/// server's job; this only recovers which access method and account the
/// existing session used). Empty on malformed input: a malformed token is not
/// worth failing auth over.
pub fn claims_from_token(jwt: &str) -> TokenClaims {
    let Some(payload) = jwt.split('.').nth(1) else {
        return TokenClaims::default();
    };
    let mut payload = payload.replace('-', "+").replace('_', "/");
    while payload.len() % 4 != 0 {
        payload.push('=');
    }
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(payload) else {
        return TokenClaims::default();
    };
    let Ok(serde_json::Value::Object(map)) = serde_json::from_slice::<serde_json::Value>(&bytes)
    else {
        return TokenClaims::default();
    };
    let string = |a: &str, b: &str| {
        map.get(a)
            .or_else(|| map.get(b))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    TokenClaims {
        access: string("AC", "ac"),
        user_id: string("ID", "id"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::run_local;
    use crate::types::ColumnSchema;
    use crate::value::object_from_json;
    use serde_json::json;
    use tokio::sync::Semaphore;

    /// Records auth calls and answers the `$auth.id` fetch.
    #[derive(Default)]
    struct FakeAuthRemote {
        auth_user: RefCell<Option<Row>>,
        authentication_error: RefCell<Option<Error>>,
        query_gate: RefCell<Option<Rc<Semaphore>>>,
        authenticated: Cell<bool>,
        invalidated: Cell<bool>,
        force_closed: Cell<u32>,
        last_signin: RefCell<Option<Object>>,
        last_signup: RefCell<Option<Object>>,
        token: RefCell<Option<String>>,
        transitions: Cell<u64>,
    }

    #[async_trait(?Send)]
    impl AuthRemote for FakeAuthRemote {
        async fn signin(&self, params: Object) -> Result<Value> {
            *self.last_signin.borrow_mut() = Some(params);
            Ok(Value::from(json!({"access": "signed-in-token"})))
        }
        async fn signup(&self, params: Object) -> Result<Value> {
            *self.last_signup.borrow_mut() = Some(params);
            Ok(Value::from(json!({"access": "signed-up-token"})))
        }
        async fn authenticate(&self, _token: &str) -> Result<Value> {
            if let Some(e) = self.authentication_error.borrow().clone() {
                return Err(e);
            }
            self.authenticated.set(true);
            Ok(Value::Bool(true))
        }
        async fn invalidate(&self) -> Result<()> {
            self.invalidated.set(true);
            Ok(())
        }
        async fn query_auth_user(&self) -> Result<Vec<Value>> {
            let gate = self.query_gate.borrow().clone();
            if let Some(gate) = gate {
                gate.acquire().await.unwrap().forget();
            }
            let rows = match self.auth_user.borrow().clone() {
                Some(u) => vec![Value::Object(u)],
                None => vec![],
            };
            Ok(vec![Value::Array(rows)])
        }
        fn begin_session_transition(&self) -> u64 {
            self.transitions.set(self.transitions.get() + 1);
            self.transitions.get()
        }
        fn end_session_transition(&self, _transition: u64) {}
        async fn force_close(&self) {
            self.force_closed.set(self.force_closed.get() + 1);
        }
        fn set_auth_token(&self, token: Option<&str>) {
            *self.token.borrow_mut() = token.map(str::to_string);
        }
    }

    struct Rig {
        remote: Rc<FakeAuthRemote>,
        persistence: Rc<MemoryPersistence>,
    }

    impl Rig {
        fn new() -> Self {
            Rig {
                remote: Rc::new(FakeAuthRemote::default()),
                persistence: Rc::new(MemoryPersistence::default()),
            }
        }

        fn build(&self) -> AuthService {
            let required = || ColumnSchema::default();
            let mut schema = Schema::default();
            schema.access.insert(
                "account".into(),
                AccessDef {
                    signin: AccessMethod {
                        params: [
                            ("email".to_string(), required()),
                            ("password".to_string(), required()),
                        ]
                        .into(),
                    },
                    signup: AccessMethod {
                        params: [("email".to_string(), required())].into(),
                    },
                },
            );
            AuthService::new(&schema, self.remote.clone(), self.persistence.clone())
        }

        fn user(&self, v: serde_json::Value) {
            *self.remote.auth_user.borrow_mut() = Some(object_from_json(v));
        }

        async fn stored(&self) -> Option<Value> {
            self.persistence.get(TOKEN_KEY).await.unwrap()
        }

        async fn store(&self, token: &str) {
            self.persistence.set(TOKEN_KEY, token.into()).await.unwrap();
        }
    }

    fn jwt() -> String {
        let claims =
            base64::engine::general_purpose::URL_SAFE.encode(br#"{"ID":"user:1","AC":"account"}"#);
        format!("h.{claims}.s")
    }

    fn params(v: serde_json::Value) -> Object {
        object_from_json(v)
    }

    #[test]
    fn claims_are_read_without_verifying() {
        let c = claims_from_token(&jwt());
        assert_eq!(c.user_id.as_deref(), Some("user:1"));
        assert_eq!(c.access.as_deref(), Some("account"));
        assert_eq!(claims_from_token("garbage"), TokenClaims::default());
        assert_eq!(claims_from_token("a.!!!.c"), TokenClaims::default());
    }

    #[tokio::test]
    async fn unreachable_verification_retains_a_restored_session_and_later_refreshes_it() {
        let rig = Rig::new();
        rig.store(&jwt()).await;
        let auth = rig.build();
        auth.restore_session_from_token(true).await.unwrap();
        *rig.remote.authentication_error.borrow_mut() = Some(Error::Network("offline".into()));
        auth.check(None).await.unwrap();
        assert_eq!(auth.user_id().as_deref(), Some("user:1"));
        assert!(auth.is_authenticated());
        assert_eq!(rig.stored().await, Some(Value::String(jwt())));
        assert_eq!(auth.verification_error().unwrap().kind, ErrorKind::Network);
        assert!(auth.needs_verification());

        *rig.remote.authentication_error.borrow_mut() = None;
        rig.user(json!({"id": "user:1", "username": "restored"}));
        auth.check(None).await.unwrap();
        assert_eq!(
            auth.current_user().unwrap()["username"],
            Value::from("restored")
        );
        assert!(auth.verification_error().is_none());
        assert!(!auth.needs_verification());
        assert_eq!(auth.access().as_deref(), Some("account"));
    }

    #[tokio::test]
    async fn explicit_rejection_clears_restored_auth_but_a_new_offline_login_fails() {
        let rig = Rig::new();
        rig.store(&jwt()).await;
        let auth = rig.build();
        auth.restore_session_from_token(true).await.unwrap();
        *rig.remote.authentication_error.borrow_mut() = Some(Error::Auth(
            "There was a problem with authentication".into(),
        ));
        assert!(auth.check(None).await.is_err());
        assert!(!auth.is_authenticated());
        assert_eq!(rig.stored().await, None);

        *rig.remote.authentication_error.borrow_mut() = Some(Error::Network("offline".into()));
        let err = auth
            .sign_in("account", params(json!({"email": "a", "password": "b"})))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Network(_)), "{err}");
        assert!(!auth.is_authenticated());
        assert_eq!(
            rig.remote.force_closed.get(),
            1,
            "a failed sign-in restores the transport"
        );
    }

    #[tokio::test]
    async fn late_verification_cannot_restore_a_session_after_sign_out() {
        run_local(async {
            let rig = Rig::new();
            rig.store(&jwt()).await;
            let auth = rig.build();
            auth.restore_session_from_token(true).await.unwrap();
            rig.user(json!({"id": "user:1"}));
            let gate = Rc::new(Semaphore::new(0));
            *rig.remote.query_gate.borrow_mut() = Some(gate.clone());

            let a = auth.clone();
            let check = tokio::task::spawn_local(async move { a.check(None).await });
            tokio::task::yield_now().await;
            auth.sign_out().await.unwrap();
            gate.add_permits(1);
            check.await.unwrap().unwrap();
            assert!(auth.current_user().is_none());
            assert_eq!(rig.stored().await, None);
        })
        .await;
    }

    #[tokio::test]
    async fn auth_notifications_wait_for_the_local_account_transition() {
        run_local(async {
            let rig = Rig::new();
            let auth = rig.build();
            rig.user(json!({"id": "user:1"}));
            let gate = Rc::new(Semaphore::new(0));
            let entered = Rc::new(Semaphore::new(0));
            let (g, e) = (gate.clone(), entered.clone());
            auth.set_on_session_changed(Some(Rc::new(move |_| {
                e.add_permits(1);
                let g = g.clone();
                async move {
                    g.acquire().await.unwrap().forget();
                    Ok(())
                }
                .boxed_local()
            })));
            let seen = Rc::new(RefCell::new(Vec::<Option<String>>::new()));
            let s = seen.clone();
            let _sub = auth.subscribe(move |id| s.borrow_mut().push(id.map(str::to_string)));

            let a = auth.clone();
            let login = tokio::task::spawn_local(async move {
                a.sign_in("account", params(json!({"email": "a", "password": "b"})))
                    .await
            });
            entered.acquire().await.unwrap().forget();
            assert_eq!(*seen.borrow(), vec![None]);
            gate.add_permits(1);
            login.await.unwrap().unwrap();
            assert_eq!(seen.borrow().last().unwrap().as_deref(), Some("user:1"));
        })
        .await;
    }

    #[tokio::test]
    async fn check_with_no_token_leaves_unauthenticated() {
        let rig = Rig::new();
        let auth = rig.build();
        assert!(auth.is_loading());
        auth.init().await.unwrap();
        assert!(!auth.is_authenticated());
        assert!(!auth.is_loading());
    }

    #[tokio::test]
    async fn check_with_a_stored_token_hydrates_the_user() {
        let rig = Rig::new();
        rig.store("tok").await;
        rig.user(json!({"id": "user:1", "email": "a@b.c"}));
        let auth = rig.build();
        auth.init().await.unwrap();
        assert!(auth.is_authenticated());
        assert_eq!(auth.user_id().as_deref(), Some("user:1"));
        assert!(rig.remote.authenticated.get());
        assert_eq!(rig.remote.token.borrow().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn concurrent_checks_share_one_round_trip() {
        run_local(async {
            let rig = Rig::new();
            rig.store("tok").await;
            rig.user(json!({"id": "user:1"}));
            let gate = Rc::new(Semaphore::new(0));
            *rig.remote.query_gate.borrow_mut() = Some(gate.clone());
            let auth = rig.build();
            let (a, b) = (auth.clone(), auth.clone());
            let first = tokio::task::spawn_local(async move { a.check(None).await });
            let second = tokio::task::spawn_local(async move { b.check(None).await });
            tokio::task::yield_now().await;
            gate.add_permits(1);
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();
            assert!(auth.is_authenticated());
            assert!(auth.0.checking.borrow().is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn subscribe_fires_immediately_then_on_change() {
        let rig = Rig::new();
        rig.store("tok").await;
        rig.user(json!({"id": "user:1"}));
        let auth = rig.build();
        auth.init().await.unwrap();

        let seen = Rc::new(RefCell::new(Vec::<Option<String>>::new()));
        let s = seen.clone();
        let sub = auth.subscribe(move |id| s.borrow_mut().push(id.map(str::to_string)));
        assert_eq!(*seen.borrow(), vec![Some("user:1".to_string())]);

        auth.sign_out().await.unwrap();
        assert_eq!(*seen.borrow().last().unwrap(), None);
        let n = seen.borrow().len();
        drop(sub);
        auth.check(Some("tok")).await.unwrap();
        assert_eq!(
            seen.borrow().len(),
            n,
            "a dropped subscription hears nothing"
        );
    }

    #[tokio::test]
    async fn sign_out_clears_state_removes_token_invalidates() {
        let rig = Rig::new();
        rig.store("tok").await;
        rig.user(json!({"id": "user:1"}));
        let auth = rig.build();
        auth.init().await.unwrap();
        auth.sign_out().await.unwrap();
        assert!(!auth.is_authenticated());
        assert!(auth.current_user().is_none());
        assert!(rig.remote.invalidated.get());
        assert_eq!(rig.stored().await, None);
        assert_eq!(*rig.remote.token.borrow(), None);
    }

    #[tokio::test]
    async fn sign_in_validates_required_params_then_authenticates() {
        let rig = Rig::new();
        rig.user(json!({"id": "user:9"}));
        let auth = rig.build();
        let err = auth
            .sign_in("account", params(json!({"email": "a@b.c"})))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
        assert!(err.to_string().contains("password"), "{err}");

        auth.sign_in(
            "account",
            params(json!({"email": "a@b.c", "password": "pw"})),
        )
        .await
        .unwrap();
        let signin = rig.remote.last_signin.borrow().clone().unwrap();
        assert_eq!(signin["access"], Value::from("account"));
        assert_eq!(
            signin["variables"].get("email"),
            Some(&Value::from("a@b.c"))
        );
        assert!(auth.is_authenticated());
        assert_eq!(rig.stored().await, Some(Value::from("signed-in-token")));
    }

    #[tokio::test]
    async fn sign_up_validates_required_params() {
        let rig = Rig::new();
        let auth = rig.build();
        assert!(matches!(
            auth.sign_up("account", Object::new()).await,
            Err(Error::InvalidArgument(_))
        ));
        rig.user(json!({"id": "user:2"}));
        auth.sign_up("account", params(json!({"email": "x"})))
            .await
            .unwrap();
        assert!(rig.remote.last_signup.borrow().is_some());
        assert_eq!(auth.user_id().as_deref(), Some("user:2"));
    }

    #[tokio::test]
    async fn unknown_access_name_fails() {
        let rig = Rig::new();
        let auth = rig.build();
        let err = auth
            .sign_in("nope", params(json!({"email": "a", "password": "b"})))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("'nope' not found"), "{err}");
    }

    #[tokio::test]
    async fn a_sign_in_without_a_user_row_is_rejected() {
        let rig = Rig::new();
        let auth = rig.build();
        let err = auth
            .sign_in("account", params(json!({"email": "a", "password": "b"})))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("account not found"), "{err}");
        assert!(!auth.is_authenticated());
    }

    // ---- sign-out flushes the outbox first ------------------------------------------------

    async fn signed_in(rig: &Rig) -> AuthService {
        let auth = rig.build();
        rig.user(json!({"id": "user:a"}));
        auth.check(Some("tok")).await.unwrap();
        rig.remote.invalidated.set(false);
        auth
    }

    #[tokio::test]
    async fn the_hook_runs_while_the_session_is_still_valid() {
        let rig = Rig::new();
        let auth = signed_in(&rig).await;
        // (token, invalidated) as the hook saw them.
        type Seen = Option<(Option<String>, bool)>;
        let during: Rc<RefCell<Seen>> = Rc::default();
        let (a, r, d) = (auth.clone(), rig.remote.clone(), during.clone());
        auth.on_before_sign_out(Rc::new(move |_| {
            *d.borrow_mut() = Some((a.token(), r.invalidated.get()));
            async { Ok(()) }.boxed_local()
        }))
        .forget();

        auth.sign_out().await.unwrap();
        assert_eq!(*during.borrow(), Some((Some("tok".to_string()), false)));
        assert_eq!(auth.token(), None);
        assert!(!auth.is_authenticated());
        assert!(rig.remote.invalidated.get());
    }

    #[tokio::test]
    async fn a_failing_flush_never_blocks_signing_out() {
        let rig = Rig::new();
        let auth = signed_in(&rig).await;
        auth.on_before_sign_out(Rc::new(|_| {
            async { Err(Error::other("server gone")) }.boxed_local()
        }))
        .forget();
        auth.sign_out().await.unwrap();
        assert_eq!(auth.token(), None);
        assert!(!auth.is_authenticated());
    }

    #[tokio::test]
    async fn every_hook_runs_in_parallel_with_who_is_leaving() {
        run_local(async {
            let rig = Rig::new();
            let auth = signed_in(&rig).await;
            let started: Rc<RefCell<Vec<&'static str>>> = Rc::default();
            let seen: Rc<RefCell<Option<SignOutContext>>> = Rc::default();
            let gate = Rc::new(Semaphore::new(0));

            let (st, se, g) = (started.clone(), seen.clone(), gate.clone());
            auth.on_before_sign_out(Rc::new(move |ctx| {
                st.borrow_mut().push("slow");
                *se.borrow_mut() = Some(ctx);
                let g = g.clone();
                async move {
                    g.acquire().await.unwrap().forget();
                    Ok(())
                }
                .boxed_local()
            }))
            .forget();
            let st = started.clone();
            auth.on_before_sign_out(Rc::new(move |_| {
                st.borrow_mut().push("failing");
                async { Err(Error::other("nope")) }.boxed_local()
            }))
            .forget();
            let st = started.clone();
            let off = auth.on_before_sign_out(Rc::new(move |_| {
                st.borrow_mut().push("removed");
                async { Ok(()) }.boxed_local()
            }));
            off.cancel();

            let a = auth.clone();
            let done = tokio::task::spawn_local(async move { a.sign_out().await });
            tokio::task::yield_now().await;
            assert_eq!(*started.borrow(), vec!["slow", "failing"]);
            assert_eq!(
                auth.token().as_deref(),
                Some("tok"),
                "still signed in while a hook runs"
            );
            gate.add_permits(1);
            done.await.unwrap().unwrap();

            let ctx = seen.borrow().clone().unwrap();
            assert_eq!(ctx.user_id.as_deref(), Some("user:a"));
            assert_eq!(ctx.token.as_deref(), Some("tok"));
            assert!(!ctx.impersonating);
            assert_eq!(auth.token(), None);
        })
        .await;
    }

    #[tokio::test]
    async fn dispose_drops_the_hooks() {
        let rig = Rig::new();
        let auth = signed_in(&rig).await;
        let ran = Rc::new(Cell::new(false));
        let r = ran.clone();
        auth.on_before_sign_out(Rc::new(move |_| {
            r.set(true);
            async { Ok(()) }.boxed_local()
        }))
        .forget();
        auth.dispose();
        auth.sign_out().await.unwrap();
        assert!(!ran.get());
    }

    #[tokio::test]
    async fn impersonation_is_read_from_the_access_claim() {
        let rig = Rig::new();
        let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"ID":"user:1","AC":"_00_impersonate"}"#);
        rig.store(&format!("h.{claims}.s")).await;
        let auth = rig.build();
        auth.restore_session_from_token(false).await.unwrap();
        assert!(auth.is_impersonating());
    }
}
