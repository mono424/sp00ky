//! Native delivery: iOS through APNs (token auth over HTTP/2) and Android (or
//! FCM-registered iOS) through FCM HTTP v1.
//!
//! Everything here is pure except the token caches: rendering a rule's
//! `native:` block, building each provider's request, reading its answer.
//! The engine owns the I/O and the bookkeeping, exactly as for Web Push.
//!
//! Credentials never come from the manifest row. The CLI resolves the
//! `push.apns.key` / `push.fcm.serviceAccount` references at migrate and
//! writes `_00_push_credential:apns` / `:fcm`; the engine builds
//! [`Providers`] from those rows on every config reload.

use std::sync::{Arc, Mutex};

use p256::ecdsa::signature::Signer as _;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::config::{
    AndroidOptions, ApnsOptions, NativeTemplate, NotificationTemplate, Platform, PushPayload, Rule,
    RuleDefaults, Urgency,
};
use crate::rules;
use crate::template;
use crate::util::{b64url, split_record_id, unquote_key};

/// Whole-payload cap of both services.
pub const MAX_PAYLOAD: usize = 4096;
pub const APNS_HOST: &str = "https://api.push.apple.com";
pub const APNS_SANDBOX_HOST: &str = "https://api.sandbox.push.apple.com";
pub const FCM_HOST: &str = "https://fcm.googleapis.com";
/// Fixed, whatever the service account's `token_uri` says: the engine runs
/// inside the platform network and must not POST where a manifest points it.
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
pub const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
/// Apple accepts a provider token for an hour and refuses new ones more often
/// than every 20 minutes.
pub const APNS_TOKEN_REFRESH_MS: u64 = 50 * 60_000;
/// Keys of a rendered notification the app still needs after the OS shows it.
const KEPT_NOTIFICATION_KEYS: [&str; 4] = ["url", "tag", "image", "data"];

// ── Devices ──────────────────────────────────────────────────────────────

/// `_00_push_subscription.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeviceKind {
    Web,
    Apns,
    Fcm,
}

impl DeviceKind {
    /// Rows written before native push have no kind: web.
    pub fn parse(kind: Option<&str>) -> Option<DeviceKind> {
        match kind {
            None | Some("web") => Some(DeviceKind::Web),
            Some("apns") => Some(DeviceKind::Apns),
            Some("fcm") => Some(DeviceKind::Fcm),
            Some(_) => None,
        }
    }

    /// The platform a row counts as for `platforms:` filters.
    pub fn platform(&self, platform: Option<&str>) -> Platform {
        match (self, platform) {
            (DeviceKind::Web, _) => Platform::Web,
            (_, Some("ios" | "macos")) => Platform::Ios,
            (_, Some("android")) => Platform::Android,
            (DeviceKind::Apns, _) => Platform::Ios,
            (DeviceKind::Fcm, _) => Platform::Android,
        }
    }
}

/// An APNs device token: hex, 32 bytes today, longer allowed. It goes into
/// the request path, so nothing else passes.
pub fn valid_apns_token(t: &str) -> bool {
    (64..=200).contains(&t.len()) && t.chars().all(|c| c.is_ascii_hexdigit())
}

/// An FCM registration token.
pub fn valid_fcm_token(t: &str) -> bool {
    (20..=4096).contains(&t.len())
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '-'))
}

// ── Credentials ──────────────────────────────────────────────────────────

/// A PEM that went through an env var or a JSON string sometimes arrives
/// with literal `\n`.
fn pem(s: &str) -> String {
    if s.contains("-----BEGIN") && !s.contains('\n') && s.contains("\\n") {
        s.replace("\\n", "\n")
    } else {
        s.trim().to_string()
    }
}

/// APNs token-auth signer (`_00_push_credential:apns`, secret
/// `{ "teamId", "keyId", "key": "<.p8 PEM>" }`).
pub struct Apns {
    team_id: String,
    key_id: String,
    signing: p256::ecdsa::SigningKey,
    token: Mutex<Option<(String, u64)>>,
}

impl std::fmt::Debug for Apns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Apns")
            .field("team_id", &self.team_id)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl Apns {
    pub fn from_secret(secret: &str) -> Result<Apns, String> {
        use p256::pkcs8::DecodePrivateKey;
        let v: Value = serde_json::from_str(secret)
            .map_err(|e| format!("apns credential is not JSON: {e}"))?;
        let field = |k: &str| {
            v.get(k)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("apns credential has no `{k}`"))
        };
        let (team_id, key_id, key) = (field("teamId")?, field("keyId")?, field("key")?);
        let secret = p256::SecretKey::from_pkcs8_pem(&pem(key))
            .map_err(|e| format!("apns key is not a P-256 .p8 key: {e}"))?;
        Ok(Apns {
            team_id: team_id.to_string(),
            key_id: key_id.to_string(),
            signing: p256::ecdsa::SigningKey::from(&secret),
            token: Mutex::new(None),
        })
    }

    /// The provider token (ES256 JWT), re-signed after 50 minutes.
    pub fn bearer(&self, now_ms: u64) -> String {
        let mut cached = self.token.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((token, at)) = cached.as_ref() {
            if now_ms < at + APNS_TOKEN_REFRESH_MS {
                return token.clone();
            }
        }
        let header = b64url(
            json!({ "alg": "ES256", "kid": self.key_id })
                .to_string()
                .as_bytes(),
        );
        let claims = b64url(
            json!({ "iss": self.team_id, "iat": now_ms / 1000 })
                .to_string()
                .as_bytes(),
        );
        let input = format!("{header}.{claims}");
        let sig: p256::ecdsa::Signature = self.signing.sign(input.as_bytes());
        let token = format!("{input}.{}", b64url(&sig.to_bytes()));
        *cached = Some((token.clone(), now_ms));
        token
    }

    /// Apple refused the token: sign a new one next time.
    pub fn forget_token(&self) {
        *self.token.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
}

/// FCM HTTP v1 client credentials (`_00_push_credential:fcm`, secret = the
/// service account JSON) and the OAuth2 access token cache.
pub struct Fcm {
    project_id: String,
    client_email: String,
    key_id: Option<String>,
    key: rsa::pkcs1v15::SigningKey<sha2::Sha256>,
    token: Mutex<Option<(String, u64)>>,
    /// One token exchange at a time; the others wait and reuse it.
    pub(crate) refresh: futures::lock::Mutex<()>,
}

impl std::fmt::Debug for Fcm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fcm")
            .field("project_id", &self.project_id)
            .field("client_email", &self.client_email)
            .finish_non_exhaustive()
    }
}

impl Fcm {
    pub fn from_secret(secret: &str) -> Result<Fcm, String> {
        use rsa::pkcs8::DecodePrivateKey;
        let v: Value = serde_json::from_str(secret)
            .map_err(|e| format!("fcm service account is not JSON: {e}"))?;
        let field = |k: &str| {
            v.get(k)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("fcm service account has no `{k}`"))
        };
        let project_id = field("project_id")?;
        // It goes into the request path.
        if !project_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(format!(
                "fcm project_id `{project_id}` is not a Firebase project id"
            ));
        }
        let key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem(field("private_key")?))
            .map_err(|e| format!("fcm private_key is not a PKCS#8 RSA key: {e}"))?;
        Ok(Fcm {
            project_id: project_id.to_string(),
            client_email: field("client_email")?.to_string(),
            key_id: v
                .get("private_key_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            key: rsa::pkcs1v15::SigningKey::<sha2::Sha256>::new(key),
            token: Mutex::new(None),
            refresh: futures::lock::Mutex::new(()),
        })
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn cached_token(&self, now_ms: u64) -> Option<String> {
        let cached = self.token.lock().unwrap_or_else(|p| p.into_inner());
        cached
            .as_ref()
            .filter(|(_, until)| now_ms < *until)
            .map(|(t, _)| t.clone())
    }

    pub fn forget_token(&self) {
        *self.token.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// The JWT-bearer grant (RFC 7523) that trades the service account for an
    /// access token.
    pub fn token_request(&self, now_secs: u64) -> Request {
        use rsa::signature::{SignatureEncoding, Signer};
        let mut header = json!({ "alg": "RS256", "typ": "JWT" });
        if let Some(kid) = &self.key_id {
            header["kid"] = Value::String(kid.clone());
        }
        let claims = json!({
            "iss": self.client_email,
            "scope": FCM_SCOPE,
            "aud": GOOGLE_TOKEN_URL,
            "iat": now_secs,
            "exp": now_secs + 3600,
        });
        let input = format!(
            "{}.{}",
            b64url(header.to_string().as_bytes()),
            b64url(claims.to_string().as_bytes())
        );
        let sig = self.key.sign(input.as_bytes());
        let jwt = format!("{input}.{}", b64url(&sig.to_bytes()));
        Request {
            url: GOOGLE_TOKEN_URL.to_string(),
            headers: vec![(
                "Content-Type".into(),
                "application/x-www-form-urlencoded".into(),
            )],
            // The JWT is base64url and dots: nothing to escape.
            body: format!(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={jwt}"
            )
            .into_bytes(),
        }
    }

    /// Read the token endpoint's answer and cache the token until five
    /// minutes before it expires.
    pub fn accept_token(&self, body: &str, now_ms: u64) -> Result<String, String> {
        let v: Value =
            serde_json::from_str(body).map_err(|_| "token answer is not JSON".to_string())?;
        let token = v
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| "token answer has no access_token".to_string())?
            .to_string();
        let expires = v.get("expires_in").and_then(Value::as_u64).unwrap_or(3600);
        let until = now_ms + expires.saturating_sub(300).max(60) * 1000;
        *self.token.lock().unwrap_or_else(|p| p.into_inner()) = Some((token.clone(), until));
        Ok(token)
    }
}

/// The native providers the credential rows describe. `Err` keeps a
/// credential that does not parse visible in the status without taking the
/// other provider (or Web Push) down.
#[derive(Debug, Default)]
pub struct Providers {
    pub apns: Option<Result<Arc<Apns>, String>>,
    pub fcm: Option<Result<Arc<Fcm>, String>>,
}

impl Providers {
    /// From `SELECT id, secret, hash FROM _00_push_credential`.
    pub fn from_rows(rows: &[Value]) -> Providers {
        let mut out = Providers::default();
        for row in rows {
            let Some(name) = credential_name(row) else {
                continue;
            };
            let secret = row
                .get("secret")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match name.as_str() {
                "apns" => out.apns = Some(Apns::from_secret(secret).map(Arc::new)),
                "fcm" => out.fcm = Some(Fcm::from_secret(secret).map(Arc::new)),
                _ => {}
            }
        }
        out
    }

    /// What changes when a credential is rotated: `apns=<hash>;fcm=<hash>`.
    pub fn fingerprint(rows: &[Value]) -> String {
        let mut parts: Vec<String> = rows
            .iter()
            .filter_map(|r| {
                let name = credential_name(r)?;
                let hash = r.get("hash").and_then(Value::as_str).unwrap_or_default();
                Some(format!("{name}={hash}"))
            })
            .collect();
        parts.sort();
        parts.join(";")
    }

    pub fn apns(&self) -> Option<&Arc<Apns>> {
        self.apns.as_ref().and_then(|r| r.as_ref().ok())
    }

    pub fn fcm(&self) -> Option<&Arc<Fcm>> {
        self.fcm.as_ref().and_then(|r| r.as_ref().ok())
    }

    pub fn any(&self) -> bool {
        self.apns().is_some() || self.fcm().is_some()
    }
}

fn credential_name(row: &Value) -> Option<String> {
    let id = row.get("id").and_then(Value::as_str)?;
    let (_, key) = split_record_id(id)?;
    Some(unquote_key(key).to_string())
}

// ── Rendering ────────────────────────────────────────────────────────────

/// What the OS shows. `None` on [`NativePush::alert`] is a silent push.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Alert {
    pub title: String,
    pub body: Option<String>,
    pub image: Option<String>,
    pub tag: Option<String>,
    pub silent: bool,
}

/// One push rendered for native devices, before a provider shapes it.
#[derive(Debug, Clone, PartialEq)]
pub struct NativePush {
    pub alert: Option<Alert>,
    /// Final `aps` keys (kebab-case), from `native.apns`.
    pub apns: Map<String, Value>,
    /// Final `android.notification` keys (snake_case), from `native.android`.
    pub android: Map<String, Value>,
    pub android_priority: Option<String>,
    /// The `sp00ky` key the app reads: the web payload, its notification cut
    /// down to what the OS does not show (`url`, `tag`, `image`, `data`).
    pub payload: PushPayload,
}

impl NativePush {
    fn new(
        notification: Option<Value>,
        apns: Map<String, Value>,
        android: Map<String, Value>,
        android_priority: Option<String>,
        payload: &PushPayload,
    ) -> NativePush {
        let n = notification.and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        });
        let text = |m: &Map<String, Value>, k: &str| {
            m.get(k)
                .and_then(Value::as_str)
                .map(str::to_string)
                .filter(|s| !s.is_empty())
        };
        let alert = n.as_ref().map(|m| Alert {
            title: text(m, "title").unwrap_or_default(),
            body: text(m, "body"),
            image: text(m, "image"),
            tag: text(m, "tag"),
            silent: m.get("silent").and_then(Value::as_bool).unwrap_or(false),
        });
        let mut payload = payload.clone();
        payload.notification = n.map(|m| {
            Value::Object(
                m.into_iter()
                    .filter(|(k, v)| KEPT_NOTIFICATION_KEYS.contains(&k.as_str()) && !v.is_null())
                    .collect(),
            )
        });
        NativePush {
            alert,
            apns,
            android,
            android_priority,
            payload,
        }
    }
}

fn text(s: &str, ctx: Option<&Value>) -> String {
    match ctx {
        Some(c) => template::render(s, c),
        None => s.to_string(),
    }
}

fn value(v: &Value, ctx: Option<&Value>) -> Value {
    match ctx {
        Some(c) => template::render_value(v, c),
        None => v.clone(),
    }
}

fn number(v: Value) -> Option<Value> {
    match v {
        Value::Number(_) => Some(v),
        Value::String(s) => {
            let s = s.trim();
            s.parse::<i64>().ok().map(Value::from).or_else(|| {
                s.parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
            })
        }
        _ => None,
    }
}

fn put_text(m: &mut Map<String, Value>, key: &str, v: &Option<String>, ctx: Option<&Value>) {
    if let Some(s) = v {
        let t = text(s, ctx);
        if !t.is_empty() {
            m.insert(key.to_string(), Value::String(t));
        }
    }
}

/// `native.apns` to `aps` keys. `ctx: None` takes the values as written.
pub fn apns_keys(o: &ApnsOptions, ctx: Option<&Value>) -> Map<String, Value> {
    let mut m: Map<String, Value> = o
        .extra
        .iter()
        .map(|(k, v)| (k.clone(), value(v, ctx)))
        .collect();
    put_text(&mut m, "sound", &o.sound, ctx);
    put_text(&mut m, "category", &o.category, ctx);
    put_text(&mut m, "thread-id", &o.thread_id, ctx);
    put_text(&mut m, "interruption-level", &o.interruption_level, ctx);
    if let Some(n) = o.badge.as_ref().and_then(|b| number(value(b, ctx))) {
        m.insert("badge".into(), n);
    }
    if let Some(n) = o
        .relevance_score
        .as_ref()
        .and_then(|r| number(value(r, ctx)))
    {
        m.insert("relevance-score".into(), n);
    }
    if o.mutable_content == Some(true) {
        m.insert("mutable-content".into(), Value::from(1));
    }
    m
}

/// `native.android` to `android.notification` keys and the message priority.
pub fn android_keys(
    o: &AndroidOptions,
    ctx: Option<&Value>,
) -> (Map<String, Value>, Option<String>) {
    let mut m: Map<String, Value> = o
        .extra
        .iter()
        .map(|(k, v)| (k.clone(), value(v, ctx)))
        .collect();
    put_text(&mut m, "channel_id", &o.channel_id, ctx);
    put_text(&mut m, "sound", &o.sound, ctx);
    put_text(&mut m, "icon", &o.icon, ctx);
    put_text(&mut m, "color", &o.color, ctx);
    (m, o.priority.clone())
}

fn layered(layers: [Option<&NotificationTemplate>; 4]) -> Option<NotificationTemplate> {
    layers.into_iter().flatten().fold(None, |acc, l| {
        Some(match acc {
            None => l.clone(),
            Some(a) => a.over(l),
        })
    })
}

/// A rule rendered for native devices. It shows something when the rule has
/// a `notification` or a `native.notification`; the layers, first wins:
/// `native.notification`, `notification`, `defaults.native.notification`,
/// `defaults.notification`.
pub fn rule_native(
    rule: &Rule,
    defaults: &RuleDefaults,
    ctx: &Value,
    topic: &str,
    payload: &PushPayload,
) -> NativePush {
    let own = rule.native.clone().unwrap_or_default();
    let base = defaults.native.clone().unwrap_or_default();
    let notification = if rule.notification.is_some() || own.notification.is_some() {
        layered([
            own.notification.as_ref(),
            rule.notification.as_ref(),
            base.notification.as_ref(),
            defaults.notification.as_ref(),
        ])
        .map(|t| rules::render_notification(&t, ctx, Some(topic)))
    } else {
        None
    };
    let merged = own.over(&base);
    let apns = merged
        .apns
        .as_ref()
        .map(|a| apns_keys(a, Some(ctx)))
        .unwrap_or_default();
    let (android, priority) = merged
        .android
        .as_ref()
        .map(|a| android_keys(a, Some(ctx)))
        .unwrap_or_default();
    NativePush::new(notification, apns, android, priority, payload)
}

/// A `_00_push_message` rendered for native devices. Its own `native` and
/// `notification` are taken as written; only the manifest defaults are
/// templates.
pub fn message_native(
    row: &Value,
    defaults: &RuleDefaults,
    payload: &PushPayload,
    now_ms: u64,
) -> NativePush {
    let ctx = json!({
        "id": payload.message.clone().unwrap_or_default(),
        "table": "_00_push_message",
        "now": now_ms,
    });
    let own = row.get("native").and_then(Value::as_object);
    let own_notification = own
        .and_then(|o| o.get("notification"))
        .and_then(Value::as_object);
    let row_notification = row.get("notification").and_then(Value::as_object);
    let base = defaults.native.clone().unwrap_or_default();

    let notification = if row_notification.is_some() || own_notification.is_some() {
        let mut map = layered([
            base.notification.as_ref(),
            defaults.notification.as_ref(),
            None,
            None,
        ])
        .map(|t| rules::render_notification(&t, &ctx, None))
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
        for layer in [row_notification, own_notification].into_iter().flatten() {
            for (k, v) in layer {
                if !v.is_null() {
                    map.insert(k.clone(), v.clone());
                }
            }
        }
        if !map.contains_key("tag") {
            if let Some(t) = &payload.topic {
                map.insert("tag".into(), Value::String(t.clone()));
            }
        }
        Some(Value::Object(map))
    } else {
        None
    };

    let mut apns = base
        .apns
        .as_ref()
        .map(|a| apns_keys(a, Some(&ctx)))
        .unwrap_or_default();
    if let Some(a) = own
        .and_then(|o| o.get("apns"))
        .and_then(|v| serde_json::from_value::<ApnsOptions>(v.clone()).ok())
    {
        apns.extend(apns_keys(&a, None));
    }
    let (mut android, mut priority) = base
        .android
        .as_ref()
        .map(|a| android_keys(a, Some(&ctx)))
        .unwrap_or_default();
    if let Some(a) = own
        .and_then(|o| o.get("android"))
        .and_then(|v| serde_json::from_value::<AndroidOptions>(v.clone()).ok())
    {
        let (keys, p) = android_keys(&a, None);
        android.extend(keys);
        priority = p.or(priority);
    }
    NativePush::new(notification, apns, android, priority, payload)
}

/// Does a rule's `native` block (or the defaults') use `{{recipient}}`?
pub fn native_references(rule: &Rule, defaults: &RuleDefaults, root: &str) -> bool {
    [rule.native.as_ref(), defaults.native.as_ref()]
        .into_iter()
        .flatten()
        .any(|n: &NativeTemplate| {
            serde_json::to_value(n).is_ok_and(|v| template::value_references(&v, root))
        })
}

// ── Requests ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// One push as the engine hands it to a provider.
#[derive(Debug, Clone, Copy)]
pub struct Send<'a> {
    pub push: &'a NativePush,
    pub ttl_secs: u64,
    pub urgency: Urgency,
    /// The readable topic (collapse key).
    pub topic: Option<&'a str>,
    pub now_ms: u64,
}

/// `apns-collapse-id` / `collapse_key`: the topic when it fits in 64 bytes,
/// else its 32-character hash.
pub fn collapse_id(topic: &str) -> String {
    if topic.len() <= 64 {
        topic.to_string()
    } else {
        rules::topic_header(topic)
    }
}

fn low(u: Urgency) -> bool {
    matches!(u, Urgency::Low | Urgency::VeryLow)
}

/// The body over [`MAX_PAYLOAD`] degrades in a fixed order: `data`, the
/// notification's `data`, the body text, then everything of the notification
/// but its `url`.
fn fit(
    push: &NativePush,
    build: impl Fn(&NativePush) -> (Vec<u8>, usize),
) -> Result<Vec<u8>, String> {
    let mut p = push.clone();
    let (mut body, mut size) = build(&p);
    if size <= MAX_PAYLOAD {
        return Ok(body);
    }
    let steps: [fn(&mut NativePush) -> bool; 2] = [
        |p| p.payload.data.take().is_some(),
        |p| match p.payload.notification.as_mut() {
            Some(Value::Object(n)) => n.remove("data").is_some(),
            _ => false,
        },
    ];
    for step in steps {
        if step(&mut p) {
            (body, size) = build(&p);
            if size <= MAX_PAYLOAD {
                return Ok(body);
            }
        }
    }
    for _ in 0..8 {
        let Some(text) = p.alert.as_ref().and_then(|a| a.body.clone()) else {
            break;
        };
        let over = size.saturating_sub(MAX_PAYLOAD);
        if over == 0 || text.is_empty() {
            break;
        }
        // Escaped bytes count double at worst; cut generously.
        let keep = text.len().saturating_sub(over * 2 + 3);
        let mut cut: String = text
            .chars()
            .take_while({
                let mut n = 0;
                move |c| {
                    n += c.len_utf8();
                    n <= keep
                }
            })
            .collect();
        cut = cut.trim_end().to_string();
        if let Some(a) = p.alert.as_mut() {
            a.body = (!cut.is_empty()).then(|| format!("{cut}..."));
        }
        (body, size) = build(&p);
        if size <= MAX_PAYLOAD {
            return Ok(body);
        }
    }
    if let Some(Value::Object(n)) = p.payload.notification.as_mut() {
        n.retain(|k, _| k == "url");
        (body, size) = build(&p);
        if size <= MAX_PAYLOAD {
            return Ok(body);
        }
    }
    Err(format!("{size} bytes even trimmed (limit {MAX_PAYLOAD})"))
}

/// The `aps` dictionary. Alerts default to the `default` sound (`sound:
/// none` or `silent: true` turn it off) and group under their tag; silent
/// pushes carry `content-available` and nothing else.
fn aps(p: &NativePush) -> Map<String, Value> {
    let mut aps = Map::new();
    let Some(a) = &p.alert else {
        aps.insert("content-available".into(), Value::from(1));
        return aps;
    };
    let mut alert = Map::new();
    if !a.title.is_empty() {
        alert.insert("title".into(), Value::String(a.title.clone()));
    }
    if let Some(b) = &a.body {
        alert.insert("body".into(), Value::String(b.clone()));
    }
    aps.insert("alert".into(), Value::Object(alert));
    for (k, v) in &p.apns {
        aps.insert(k.clone(), v.clone());
    }
    if !aps.contains_key("thread-id") {
        if let Some(tag) = &a.tag {
            aps.insert("thread-id".into(), Value::String(tag.clone()));
        }
    }
    let muted = a.silent || aps.get("sound").and_then(Value::as_str) == Some("none");
    if muted {
        aps.remove("sound");
    } else if !aps.contains_key("sound") {
        aps.insert("sound".into(), Value::String("default".into()));
    }
    aps
}

fn apns_headers(s: &Send<'_>) -> Vec<(String, String)> {
    let shows = s.push.alert.is_some();
    let priority = if shows && !low(s.urgency) { "10" } else { "5" };
    let expiration = if s.ttl_secs == 0 {
        0
    } else {
        s.now_ms / 1000 + s.ttl_secs
    };
    let mut h = vec![
        (
            "apns-push-type".to_string(),
            if shows { "alert" } else { "background" }.to_string(),
        ),
        ("apns-priority".to_string(), priority.to_string()),
        ("apns-expiration".to_string(), expiration.to_string()),
    ];
    if let (true, Some(t)) = (shows, s.topic) {
        h.push(("apns-collapse-id".to_string(), collapse_id(t)));
    }
    h
}

/// `POST /3/device/<token>` to APNs.
pub fn apns_request(
    s: &Send<'_>,
    token: &str,
    app_id: &str,
    sandbox: bool,
    bearer: &str,
) -> Result<Request, String> {
    let body = fit(s.push, |p| {
        let b =
            serde_json::to_vec(&json!({ "aps": aps(p), "sp00ky": p.payload })).unwrap_or_default();
        let n = b.len();
        (b, n)
    })?;
    let host = if sandbox {
        APNS_SANDBOX_HOST
    } else {
        APNS_HOST
    };
    let mut headers = vec![
        ("authorization".to_string(), format!("bearer {bearer}")),
        ("apns-topic".to_string(), app_id.to_string()),
        ("content-type".to_string(), "application/json".to_string()),
    ];
    headers.extend(apns_headers(s));
    Ok(Request {
        url: format!("{host}/3/device/{}", token.to_ascii_lowercase()),
        headers,
        body,
    })
}

fn fcm_message(s: &Send<'_>, p: &NativePush, token: &str, platform: Platform) -> Value {
    let shows = p.alert.is_some();
    let mut msg = Map::new();
    msg.insert("token".into(), Value::String(token.to_string()));
    msg.insert(
        "data".into(),
        json!({ "sp00ky": serde_json::to_string(&p.payload).unwrap_or_default() }),
    );
    if let Some(a) = &p.alert {
        let mut n = Map::new();
        if !a.title.is_empty() {
            n.insert("title".into(), Value::String(a.title.clone()));
        }
        if let Some(b) = &a.body {
            n.insert("body".into(), Value::String(b.clone()));
        }
        if let Some(i) = &a.image {
            n.insert("image".into(), Value::String(i.clone()));
        }
        msg.insert("notification".into(), Value::Object(n));
    }
    if platform == Platform::Ios {
        let headers: Map<String, Value> = apns_headers(s)
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        msg.insert(
            "apns".into(),
            json!({ "headers": headers, "payload": { "aps": aps(p) } }),
        );
    } else {
        let high = match p.android_priority.as_deref() {
            Some("high") => true,
            Some("normal") => false,
            _ if shows => !low(s.urgency),
            _ => s.urgency == Urgency::High,
        };
        let mut android = Map::new();
        android.insert(
            "priority".into(),
            Value::String(if high { "HIGH" } else { "NORMAL" }.into()),
        );
        android.insert("ttl".into(), Value::String(format!("{}s", s.ttl_secs)));
        if let Some(t) = s.topic {
            android.insert("collapse_key".into(), Value::String(collapse_id(t)));
        }
        if let Some(a) = &p.alert {
            let mut n = p.android.clone();
            if let Some(tag) = &a.tag {
                n.entry("tag").or_insert_with(|| Value::String(tag.clone()));
            }
            if !a.silent && !n.contains_key("sound") {
                n.insert("default_sound".into(), Value::Bool(true));
            }
            if !n.is_empty() {
                android.insert("notification".into(), Value::Object(n));
            }
        }
        msg.insert("android".into(), Value::Object(android));
    }
    json!({ "message": msg })
}

/// `POST /v1/projects/<project>/messages:send` to FCM. The size FCM counts
/// is `data` plus `notification`.
pub fn fcm_request(
    s: &Send<'_>,
    project_id: &str,
    token: &str,
    platform: Platform,
    access_token: &str,
) -> Result<Request, String> {
    let body = fit(s.push, |p| {
        let v = fcm_message(s, p, token, platform);
        let counted = ["data", "notification"]
            .iter()
            .filter_map(|k| v["message"].get(*k))
            .map(|x| serde_json::to_vec(x).map(|b| b.len()).unwrap_or(0))
            .sum();
        (serde_json::to_vec(&v).unwrap_or_default(), counted)
    })?;
    Ok(Request {
        url: format!("{FCM_HOST}/v1/projects/{project_id}/messages:send"),
        headers: vec![
            (
                "Authorization".to_string(),
                format!("Bearer {access_token}"),
            ),
            ("Content-Type".to_string(), "application/json".to_string()),
        ],
        body,
    })
}

// ── Answers ──────────────────────────────────────────────────────────────

/// A provider's answer, as the engine acts on it.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    Ok,
    /// The token is dead: delete the row.
    Gone(String),
    /// The row can never work (wrong app for this key or project): disable.
    Rejected(String),
    /// This push is broken; keep the row.
    GiveUp(String),
    Retry(String),
    /// Our credentials were refused: new provider token, retry, row untouched.
    Provider(String),
    /// APNs `BadDeviceToken`: usually a sandbox token sent to production or
    /// the reverse.
    BadDeviceToken(String),
}

fn short(s: &str) -> String {
    let t: String = s.chars().take(200).collect();
    t.trim().to_string()
}

fn reason(prefix: &str, status: u16, detail: &str) -> String {
    if detail.is_empty() {
        format!("{prefix}_{status}")
    } else {
        format!("{prefix}_{status}: {detail}")
    }
}

/// APNs answers `{"reason": "..."}`.
pub fn classify_apns(status: u16, body: &str) -> Answer {
    let detail = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("reason").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| short(body));
    let r = reason("apns", status, &detail);
    match (status, detail.as_str()) {
        (200..=299, _) => Answer::Ok,
        (410, _) => Answer::Gone(r),
        (400, "BadDeviceToken") => Answer::BadDeviceToken(r),
        (400, "DeviceTokenNotForTopic" | "TopicDisallowed" | "BadTopic" | "MissingTopic") => {
            Answer::Rejected(r)
        }
        (403, _) => Answer::Provider(r),
        (429 | 500..=599, _) => Answer::Retry(r),
        _ => Answer::GiveUp(r),
    }
}

/// FCM answers `{"error": {"status", "message", "details": [{"errorCode"}]}}`.
pub fn classify_fcm(status: u16, body: &str) -> Answer {
    let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let err = &v["error"];
    let code = err["details"]
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|d| d.get("errorCode").and_then(Value::as_str))
        .or_else(|| err["status"].as_str())
        .unwrap_or_default()
        .to_string();
    let message = err["message"]
        .as_str()
        .map(short)
        .unwrap_or_else(|| short(body));
    let detail = match (code.is_empty(), message.is_empty()) {
        (false, false) => format!("{code}: {message}"),
        (false, true) => code.clone(),
        _ => message.clone(),
    };
    let r = reason("fcm", status, &detail);
    let bad_token = message.to_ascii_lowercase().contains("registration token");
    match (status, code.as_str()) {
        (200..=299, _) => Answer::Ok,
        (_, "UNREGISTERED") | (404, _) => Answer::Gone(r),
        (400, _) if bad_token => Answer::Gone(r),
        (403, "SENDER_ID_MISMATCH") => Answer::Rejected(r),
        (401 | 403, _) => Answer::Provider(r),
        (429 | 500..=599, _) => Answer::Retry(r),
        _ => Answer::GiveUp(r),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{PayloadKind, PushConfig, PAYLOAD_VERSION};

    // A throwaway key made for these tests (`openssl genpkey -algorithm EC
    // -pkeyopt ec_paramgen_curve:P-256`), in Apple's .p8 layout.
    pub(crate) const TEST_P8: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgDj6koxo5KtMn6QIs
mrvkvEiFcippdURoL9yWat4LgbmhRANCAASK73+cisqPRLJ6m0WyjceaF5+C+VfX
LOqVeDEOSxeUqCOJg7HdC08v+atq9wk5Fu0JdcHCuFAb76lQMm4Kq8BS
-----END PRIVATE KEY-----";

    // Same, RSA 2048 (`openssl genpkey -algorithm RSA`), as a service
    // account's `private_key`.
    pub(crate) const TEST_RSA: &str = include_str!("../testdata/fcm_test_key.pem");

    fn payload() -> PushPayload {
        PushPayload {
            v: PAYLOAD_VERSION,
            kind: PayloadKind::Rule,
            rule: Some("r".into()),
            message: None,
            table: Some("message".into()),
            id: Some("message:a".into()),
            op: Some(crate::config::Op::Create),
            topic: Some("dm:x".into()),
            notification: Some(json!({ "title": "web" })),
            data: Some(json!({ "k": 1 })),
            ts: 1,
        }
    }

    fn rule(yaml: &str) -> (Rule, RuleDefaults) {
        let cfg: PushConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.is_valid(), "{:?}", cfg.validate());
        (cfg.rules["r"].clone(), cfg.defaults)
    }

    fn send(p: &NativePush) -> Send<'_> {
        Send {
            push: p,
            ttl_secs: 60,
            urgency: Urgency::Normal,
            topic: Some("dm:x"),
            now_ms: 1_000_000,
        }
    }

    #[test]
    fn tokens_are_checked_before_they_reach_a_url_or_a_body() {
        assert!(valid_apns_token(&"ab".repeat(32)));
        assert!(!valid_apns_token("abc"));
        assert!(!valid_apns_token(&format!("{}/../x", "a".repeat(64))));
        assert!(valid_fcm_token("fXk3:APA91bH-x_yz0123456789"));
        assert!(!valid_fcm_token("short"));
        assert!(!valid_fcm_token("has space in it and is long enough"));
        assert_eq!(DeviceKind::parse(None), Some(DeviceKind::Web));
        assert_eq!(DeviceKind::parse(Some("bogus")), None);
        assert_eq!(DeviceKind::Fcm.platform(Some("ios")), Platform::Ios);
        assert_eq!(DeviceKind::Apns.platform(None), Platform::Ios);
    }

    #[test]
    fn native_overrides_the_web_notification_key_by_key() {
        let (r, d) = rule(
            r#"
defaults:
  notification: { icon: /i.png, body: "d" }
  native: { apns: { sound: ping.caf }, android: { channelId: general } }
rules:
  r:
    table: message
    to: recipient
    notification: { title: "Web {{name}}", url: "/m/{{name}}" }
    native:
      notification: { title: "Phone {{name}}" }
      apns: { badge: "{{unread}}", category: MSG }
      android: { channelId: chat, priority: high }
"#,
        );
        let ctx = json!({ "name": "ann", "unread": 3 });
        let p = rule_native(&r, &d, &ctx, "dm:x", &payload());
        let a = p.alert.as_ref().unwrap();
        assert_eq!(a.title, "Phone ann");
        assert_eq!(a.body.as_deref(), Some("d"));
        assert_eq!(a.tag.as_deref(), Some("dm:x"));
        assert_eq!(p.apns["badge"], json!(3));
        assert_eq!(p.apns["sound"], json!("ping.caf"));
        assert_eq!(p.apns["category"], json!("MSG"));
        assert_eq!(p.android["channel_id"], json!("chat"));
        assert_eq!(p.android_priority.as_deref(), Some("high"));
        // The app keeps what the OS does not show.
        assert_eq!(
            p.payload.notification,
            Some(json!({ "url": "/m/ann", "tag": "dm:x" }))
        );
    }

    #[test]
    fn a_web_nudge_can_show_content_on_phones_and_a_bare_nudge_stays_silent() {
        let (r, d) =
            rule("rules: { r: { table: t, to: u, native: { notification: { title: Hi } } } }");
        let p = rule_native(
            &r,
            &d,
            &json!({}),
            "t",
            &PushPayload {
                notification: None,
                ..payload()
            },
        );
        assert_eq!(p.alert.as_ref().unwrap().title, "Hi");
        assert!(p.payload.notification.is_some());

        let (r, d) =
            rule("defaults: { notification: { title: D } }\nrules: { r: { table: t, to: u } }");
        let p = rule_native(
            &r,
            &d,
            &json!({}),
            "t",
            &PushPayload {
                notification: None,
                ..payload()
            },
        );
        assert!(
            p.alert.is_none(),
            "defaults never turn a nudge into an alert"
        );
        assert!(p.payload.notification.is_none());
    }

    #[test]
    fn messages_take_their_native_block_as_written() {
        let (_, d) = rule("defaults: { native: { apns: { sound: \"{{table}}.caf\" } } }\nrules: { r: { table: t, to: u } }");
        let row = json!({
            "notification": { "title": "T", "body": "{{not a template}}" },
            "native": { "apns": { "badge": 2 }, "android": { "channelId": "c", "priority": "normal" } },
        });
        let pl = PushPayload {
            kind: PayloadKind::Message,
            message: Some("_00_push_message:m".into()),
            ..payload()
        };
        let p = message_native(&row, &d, &pl, 5);
        assert_eq!(
            p.alert.as_ref().unwrap().body.as_deref(),
            Some("{{not a template}}")
        );
        assert_eq!(p.apns["sound"], json!("_00_push_message.caf"));
        assert_eq!(p.apns["badge"], json!(2));
        assert_eq!(p.android["channel_id"], json!("c"));
        assert_eq!(p.android_priority.as_deref(), Some("normal"));

        let silent = message_native(
            &json!({ "data": { "x": 1 } }),
            &d,
            &PushPayload {
                notification: None,
                ..pl
            },
            5,
        );
        assert!(silent.alert.is_none());
    }

    #[test]
    fn apns_alert_and_background_requests() {
        let (r, d) = rule(
            "rules: { r: { table: t, to: u, urgency: high, notification: { title: T, body: B } } }",
        );
        let p = rule_native(&r, &d, &json!({}), "dm:x", &payload());
        let req = apns_request(&send(&p), &"AB".repeat(32), "im.app", true, "jwt").unwrap();
        assert_eq!(
            req.url,
            format!("{APNS_SANDBOX_HOST}/3/device/{}", "ab".repeat(32))
        );
        let h = |k: &str| {
            req.headers
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(h("authorization").as_deref(), Some("bearer jwt"));
        assert_eq!(h("apns-topic").as_deref(), Some("im.app"));
        assert_eq!(h("apns-push-type").as_deref(), Some("alert"));
        assert_eq!(h("apns-priority").as_deref(), Some("10"));
        assert_eq!(h("apns-expiration").as_deref(), Some("1060"));
        assert_eq!(h("apns-collapse-id").as_deref(), Some("dm:x"));
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["aps"]["alert"], json!({ "title": "T", "body": "B" }));
        assert_eq!(body["aps"]["sound"], json!("default"));
        assert_eq!(body["aps"]["thread-id"], json!("dm:x"));
        assert_eq!(body["sp00ky"]["rule"], json!("r"));

        let (r, d) = rule("rules: { r: { table: t, to: u } }");
        let nudge = rule_native(
            &r,
            &d,
            &json!({}),
            "dm:x",
            &PushPayload {
                notification: None,
                ..payload()
            },
        );
        let req = apns_request(&send(&nudge), &"ab".repeat(32), "im.app", false, "jwt").unwrap();
        assert!(req.url.starts_with(APNS_HOST));
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["aps"], json!({ "content-available": 1 }));
        assert!(req
            .headers
            .contains(&("apns-push-type".into(), "background".into())));
        assert!(req.headers.contains(&("apns-priority".into(), "5".into())));
        assert!(!req.headers.iter().any(|(k, _)| k == "apns-collapse-id"));
    }

    #[test]
    fn fcm_android_and_ios_messages() {
        let (r, d) = rule(
            "rules: { r: { table: t, to: u, notification: { title: T, body: B }, native: { android: { channelId: c } } } }",
        );
        let p = rule_native(&r, &d, &json!({}), "dm:x", &payload());
        let req = fcm_request(
            &send(&p),
            "proj-1",
            "tok_abcdefghijklmnopqrstuvwxyz",
            Platform::Android,
            "at",
        )
        .unwrap();
        assert_eq!(
            req.url,
            "https://fcm.googleapis.com/v1/projects/proj-1/messages:send"
        );
        assert!(req
            .headers
            .contains(&("Authorization".into(), "Bearer at".into())));
        let m: Value = serde_json::from_slice(&req.body).unwrap();
        let m = &m["message"];
        assert_eq!(m["token"], json!("tok_abcdefghijklmnopqrstuvwxyz"));
        assert_eq!(m["notification"], json!({ "title": "T", "body": "B" }));
        assert_eq!(m["android"]["priority"], json!("HIGH"));
        assert_eq!(m["android"]["ttl"], json!("60s"));
        assert_eq!(m["android"]["collapse_key"], json!("dm:x"));
        assert_eq!(m["android"]["notification"]["channel_id"], json!("c"));
        assert_eq!(m["android"]["notification"]["tag"], json!("dm:x"));
        let inner: Value = serde_json::from_str(m["data"]["sp00ky"].as_str().unwrap()).unwrap();
        assert_eq!(inner["id"], json!("message:a"));
        assert!(m.get("apns").is_none());

        let req = fcm_request(
            &send(&p),
            "proj-1",
            "tok_abcdefghijklmnopqrstuvwxyz",
            Platform::Ios,
            "at",
        )
        .unwrap();
        let m: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(
            m["message"]["apns"]["headers"]["apns-push-type"],
            json!("alert")
        );
        assert_eq!(
            m["message"]["apns"]["payload"]["aps"]["sound"],
            json!("default")
        );
        assert!(m["message"].get("android").is_none());

        let (r, d) = rule("rules: { r: { table: t, to: u } }");
        let nudge = rule_native(
            &r,
            &d,
            &json!({}),
            "dm:x",
            &PushPayload {
                notification: None,
                ..payload()
            },
        );
        let req = fcm_request(
            &send(&nudge),
            "p",
            "tok_abcdefghijklmnopqrstuvwxyz",
            Platform::Android,
            "at",
        )
        .unwrap();
        let m: Value = serde_json::from_slice(&req.body).unwrap();
        assert!(m["message"].get("notification").is_none());
        assert_eq!(m["message"]["android"]["priority"], json!("NORMAL"));
        assert!(m["message"]["android"].get("notification").is_none());
    }

    #[test]
    fn oversized_pushes_degrade_then_fail() {
        let (r, d) = rule("rules: { r: { table: t, to: u, notification: { title: T, body: \"{{text}}\", url: /x } } }");
        let long = "é".repeat(3000);
        let p = rule_native(
            &r,
            &d,
            &json!({ "text": long }),
            "dm:x",
            &PushPayload {
                data: Some(json!({ "blob": "y".repeat(2000) })),
                ..payload()
            },
        );
        let req = apns_request(&send(&p), &"ab".repeat(32), "im.app", false, "j").unwrap();
        assert!(req.body.len() <= MAX_PAYLOAD, "{}", req.body.len());
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        assert!(body["sp00ky"].get("data").is_none());
        assert!(body["aps"]["alert"]["body"]
            .as_str()
            .unwrap()
            .ends_with("..."));
        assert_eq!(body["sp00ky"]["notification"]["url"], json!("/x"));

        let huge = PushPayload {
            topic: Some("t".repeat(5000)),
            ..payload()
        };
        let (r, d) = rule("rules: { r: { table: t, to: u } }");
        let p = rule_native(&r, &d, &json!({}), "t", &huge);
        assert!(apns_request(&send(&p), &"ab".repeat(32), "im.app", false, "j").is_err());
    }

    #[test]
    fn collapse_ids_fit_the_64_byte_limit() {
        assert_eq!(collapse_id("dm:x"), "dm:x");
        let long = "x".repeat(65);
        assert_eq!(collapse_id(&long).len(), 32);
    }

    #[test]
    fn apns_answers() {
        assert_eq!(classify_apns(200, ""), Answer::Ok);
        assert!(
            matches!(classify_apns(410, r#"{"reason":"Unregistered","timestamp":1}"#), Answer::Gone(r) if r == "apns_410: Unregistered")
        );
        assert!(matches!(
            classify_apns(400, r#"{"reason":"BadDeviceToken"}"#),
            Answer::BadDeviceToken(_)
        ));
        assert!(matches!(
            classify_apns(400, r#"{"reason":"DeviceTokenNotForTopic"}"#),
            Answer::Rejected(_)
        ));
        assert!(matches!(
            classify_apns(400, r#"{"reason":"BadPriority"}"#),
            Answer::GiveUp(_)
        ));
        assert!(matches!(
            classify_apns(403, r#"{"reason":"ExpiredProviderToken"}"#),
            Answer::Provider(_)
        ));
        assert!(matches!(
            classify_apns(429, r#"{"reason":"TooManyRequests"}"#),
            Answer::Retry(_)
        ));
        assert!(matches!(classify_apns(503, ""), Answer::Retry(_)));
        assert!(matches!(classify_apns(413, ""), Answer::GiveUp(_)));
    }

    #[test]
    fn fcm_answers() {
        let err = |status: u16, code: &str, msg: &str| {
            json!({ "error": { "code": status, "status": "X", "message": msg,
                "details": [{ "@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", "errorCode": code }] } })
            .to_string()
        };
        assert_eq!(classify_fcm(200, "{}"), Answer::Ok);
        assert!(matches!(
            classify_fcm(
                404,
                &err(404, "UNREGISTERED", "Requested entity was not found.")
            ),
            Answer::Gone(_)
        ));
        assert!(matches!(
            classify_fcm(
                400,
                &err(
                    400,
                    "INVALID_ARGUMENT",
                    "The registration token is not a valid FCM registration token"
                )
            ),
            Answer::Gone(_)
        ));
        assert!(matches!(
            classify_fcm(400, &err(400, "INVALID_ARGUMENT", "Invalid JSON payload")),
            Answer::GiveUp(_)
        ));
        assert!(matches!(
            classify_fcm(403, &err(403, "SENDER_ID_MISMATCH", "")),
            Answer::Rejected(_)
        ));
        assert!(matches!(
            classify_fcm(401, r#"{"error":{"status":"UNAUTHENTICATED"}}"#),
            Answer::Provider(_)
        ));
        assert!(matches!(
            classify_fcm(429, &err(429, "QUOTA_EXCEEDED", "")),
            Answer::Retry(_)
        ));
        assert!(matches!(classify_fcm(503, "oops"), Answer::Retry(_)));
    }

    #[test]
    fn apns_provider_token_is_a_verifiable_es256_jwt_cached_for_50_minutes() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;
        use p256::ecdsa::signature::Verifier;
        use p256::pkcs8::DecodePrivateKey;
        let secret =
            json!({ "teamId": "TEAM123456", "keyId": "KEY1234567", "key": TEST_P8 }).to_string();
        let apns = Apns::from_secret(&secret).unwrap();
        let t = apns.bearer(1_000_000);
        let parts: Vec<&str> = t.split('.').collect();
        let header: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(header, json!({ "alg": "ES256", "kid": "KEY1234567" }));
        assert_eq!(claims, json!({ "iss": "TEAM123456", "iat": 1000 }));
        let key = p256::SecretKey::from_pkcs8_pem(TEST_P8).unwrap();
        let verifying = p256::ecdsa::VerifyingKey::from(&p256::ecdsa::SigningKey::from(&key));
        let sig =
            p256::ecdsa::Signature::from_slice(&URL_SAFE_NO_PAD.decode(parts[2]).unwrap()).unwrap();
        verifying
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig)
            .unwrap();
        assert_eq!(apns.bearer(1_000_000 + APNS_TOKEN_REFRESH_MS - 1), t);
        assert_ne!(apns.bearer(1_000_000 + APNS_TOKEN_REFRESH_MS), t);
        apns.forget_token();
        assert_ne!(apns.bearer(1_000_000 + APNS_TOKEN_REFRESH_MS), t);
        // An env var that flattened the PEM still works.
        let flat =
            json!({ "teamId": "T", "keyId": "K", "key": TEST_P8.replace('\n', "\\n") }).to_string();
        assert!(Apns::from_secret(&flat).is_ok());
        assert!(Apns::from_secret(r#"{"teamId":"T","keyId":"K","key":"nope"}"#).is_err());
    }

    pub(crate) fn service_account() -> String {
        json!({
            "type": "service_account",
            "project_id": "sp00ky-test",
            "private_key_id": "kid-1",
            "private_key": TEST_RSA,
            "client_email": "push@sp00ky-test.iam.gserviceaccount.com",
            "token_uri": "https://internal.example/steal",
        })
        .to_string()
    }

    #[test]
    fn fcm_token_exchange_is_a_verifiable_rs256_grant_to_google_only() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;
        use rsa::pkcs8::DecodePrivateKey;
        use rsa::signature::Verifier;
        let fcm = Fcm::from_secret(&service_account()).unwrap();
        assert_eq!(fcm.project_id(), "sp00ky-test");
        let req = fcm.token_request(1_000);
        assert_eq!(
            req.url, GOOGLE_TOKEN_URL,
            "token_uri from the JSON is ignored"
        );
        let body = String::from_utf8(req.body).unwrap();
        let jwt = body
            .strip_prefix(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=",
            )
            .unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        let header: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(
            header,
            json!({ "alg": "RS256", "typ": "JWT", "kid": "kid-1" })
        );
        assert_eq!(
            claims["iss"],
            json!("push@sp00ky-test.iam.gserviceaccount.com")
        );
        assert_eq!(claims["scope"], json!(FCM_SCOPE));
        assert_eq!(claims["aud"], json!(GOOGLE_TOKEN_URL));
        assert_eq!(
            (claims["iat"].as_u64(), claims["exp"].as_u64()),
            (Some(1_000), Some(4_600))
        );
        let public = rsa::RsaPrivateKey::from_pkcs8_pem(TEST_RSA)
            .unwrap()
            .to_public_key();
        let verifying = rsa::pkcs1v15::VerifyingKey::<sha2::Sha256>::new(public);
        let sig = rsa::pkcs1v15::Signature::try_from(
            URL_SAFE_NO_PAD.decode(parts[2]).unwrap().as_slice(),
        )
        .unwrap();
        verifying
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig)
            .unwrap();

        assert_eq!(fcm.cached_token(0), None);
        let t = fcm
            .accept_token(
                r#"{"access_token":"ya29.x","expires_in":3599,"token_type":"Bearer"}"#,
                0,
            )
            .unwrap();
        assert_eq!(t, "ya29.x");
        assert_eq!(fcm.cached_token(3_298_000).as_deref(), Some("ya29.x"));
        assert_eq!(
            fcm.cached_token(3_300_000),
            None,
            "refreshed five minutes early"
        );
        fcm.forget_token();
        assert_eq!(fcm.cached_token(0), None);
        assert!(fcm.accept_token(r#"{"error":"invalid_grant"}"#, 0).is_err());

        let bad =
            json!({ "project_id": "Bad/../id", "private_key": TEST_RSA, "client_email": "x" })
                .to_string();
        assert!(Fcm::from_secret(&bad).is_err());
    }

    #[test]
    fn providers_come_from_credential_rows() {
        let rows = vec![
            json!({ "id": "_00_push_credential:apns", "secret": json!({ "teamId": "T", "keyId": "K", "key": TEST_P8 }).to_string(), "hash": "a1" }),
            json!({ "id": "_00_push_credential:⟨fcm⟩", "secret": "{}", "hash": "f1" }),
        ];
        let p = Providers::from_rows(&rows);
        assert!(p.apns().is_some());
        assert!(matches!(&p.fcm, Some(Err(e)) if e.contains("project_id")));
        assert!(p.any());
        assert_eq!(Providers::fingerprint(&rows), "apns=a1;fcm=f1");
        assert!(!Providers::from_rows(&[]).any());
    }
}
