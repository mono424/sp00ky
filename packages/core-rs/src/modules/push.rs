//! Native push (APNs, FCM) for a sp00ky client: `db.push`.
//!
//! PORT: `packages/spooky_core/lib/src/modules/push/push.dart`.
//!
//! Rules in `sp00ky.yml` (`push:`) decide what is sent; this module only tells
//! the server which device belongs to the signed-in user, through the
//! `fn::push::*` functions (`_00_` tables have no client codegen). It does not
//! talk to the OS: the token comes from the caller (`spooky_push` in Flutter,
//! or any other source), so this stays free of any APNs or FCM SDK.
//!
//! Mirrors `db.webPush` in `@spooky-sync/core` minus the browser parts.
//!
//! The sync decision is a pure function of state ([`decide_push_sync`]); the
//! module only executes it. Lives on the engine thread (`Rc`), like every
//! other module.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::{Rc, Weak};
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::FutureExt;

use crate::error::Error;
use crate::modules::auth::{AuthService, SignOutContext, SignOutHook};
use crate::modules::bucket::RemoteQuery;
use crate::runtime::Subscription;
use crate::services::persistence::PersistenceClient;
use crate::value::{iso8601, Object, RecordId, Value};

/// The persistence key of this device's [`PushRecord`].
pub const PUSH_ENDPOINT_KEY: &str = "sp00ky_push_endpoint";

// ---- enums ------------------------------------------------------------------------------

/// How a device is reached (`_00_push_subscription.kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PushKind {
    Web,
    Apns,
    Fcm,
}

impl PushKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PushKind::Web => "web",
            PushKind::Apns => "apns",
            PushKind::Fcm => "fcm",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "web" => Some(PushKind::Web),
            "apns" => Some(PushKind::Apns),
            "fcm" => Some(PushKind::Fcm),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushPlatform {
    Web,
    Ios,
    Android,
    Macos,
}

impl PushPlatform {
    pub fn as_str(self) -> &'static str {
        match self {
            PushPlatform::Web => "web",
            PushPlatform::Ios => "ios",
            PushPlatform::Android => "android",
            PushPlatform::Macos => "macos",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "web" => Some(PushPlatform::Web),
            "ios" => Some(PushPlatform::Ios),
            "android" => Some(PushPlatform::Android),
            "macos" => Some(PushPlatform::Macos),
            _ => None,
        }
    }
}

/// Which of Apple's hosts an APNs token belongs to. Development builds get
/// sandbox tokens, TestFlight and the App Store production ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ApnsEnvironment {
    Sandbox,
    Production,
}

impl ApnsEnvironment {
    pub fn as_str(self) -> &'static str {
        match self {
            ApnsEnvironment::Sandbox => "sandbox",
            ApnsEnvironment::Production => "production",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "sandbox" => Some(ApnsEnvironment::Sandbox),
            "production" => Some(ApnsEnvironment::Production),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushPermission {
    Granted,
    /// iOS "deliver quietly": delivered to the notification center only.
    Provisional,
    Denied,
    NotDetermined,
}

impl PushPermission {
    pub fn allows_delivery(self) -> bool {
        matches!(self, PushPermission::Granted | PushPermission::Provisional)
    }
}

// ---- value helpers ------------------------------------------------------------------------

fn str_of(m: &Object, key: &str) -> Option<String> {
    m.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Dart `(v as num?)?.toInt()`: truncates.
fn int_of(v: Option<&Value>) -> Option<i64> {
    v.and_then(Value::as_f64).map(|f| f as i64)
}

/// Dart `(m[key] ?? '').toString()`.
fn text_of(m: &Object, key: &str) -> String {
    m.get(key)
        .filter(|v| !v.is_nullish())
        .map(Value::to_id_string)
        .unwrap_or_default()
}

/// An ISO-8601 datetime. A string without an offset reads as UTC (Dart reads
/// it as local time); a typed datetime is taken as is.
fn date_of(v: Option<&Value>) -> Option<DateTime<Utc>> {
    match v? {
        Value::Datetime(d) => Some(*d),
        Value::String(s) => DateTime::parse_from_rfc3339(s)
            .map(|d| d.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .map(|n| n.and_utc())
            }),
        _ => None,
    }
}

fn enum_of<T>(v: Option<&Value>, parse: fn(&str) -> Option<T>) -> Option<T> {
    v.and_then(Value::as_str).and_then(parse)
}

/// Decode a JSON string into a value; anything else as is.
fn decoded(v: &Value) -> Option<Value> {
    match v {
        Value::String(s) => serde_json::from_str::<serde_json::Value>(s)
            .ok()
            .map(Value::from),
        other => Some(other.clone()),
    }
}

fn vars<const N: usize>(pairs: [(&str, Value); N]) -> Option<Object> {
    Some(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

// ---- token and server shapes --------------------------------------------------------------

/// What the OS gave this install.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PushToken {
    pub kind: PushKind,
    pub token: String,
    /// Bundle id / package name: the APNs topic.
    pub app_id: String,
    pub platform: Option<PushPlatform>,
    /// APNs only; the server assumes production when absent.
    pub environment: Option<ApnsEnvironment>,
}

impl PushToken {
    pub fn new(kind: PushKind, token: impl Into<String>, app_id: impl Into<String>) -> Self {
        PushToken {
            kind,
            token: token.into(),
            app_id: app_id.into(),
            platform: None,
            environment: None,
        }
    }

    pub fn with_platform(mut self, platform: PushPlatform) -> Self {
        self.platform = Some(platform);
        self
    }

    pub fn with_environment(mut self, environment: ApnsEnvironment) -> Self {
        self.environment = Some(environment);
        self
    }

    /// The endpoint the server stores for this device (`<kind>:<token>`).
    /// APNs tokens are hex, and the server keys them lowercased.
    pub fn endpoint(&self) -> String {
        let token = if self.kind == PushKind::Apns {
            self.token.to_lowercase()
        } else {
            self.token.clone()
        };
        format!("{}:{token}", self.kind.as_str())
    }

    /// The `$device` argument of `fn::push::register`.
    pub fn to_object(&self) -> Object {
        let mut o = Object::new();
        o.insert("kind".into(), self.kind.as_str().into());
        o.insert("token".into(), self.token.clone().into());
        o.insert("appId".into(), self.app_id.clone().into());
        if let Some(p) = self.platform {
            o.insert("platform".into(), p.as_str().into());
        }
        if let (PushKind::Apns, Some(e)) = (self.kind, self.environment) {
            o.insert("environment".into(), e.as_str().into());
        }
        o
    }
}

/// The Firebase client config an Android app initialises FCM with, served by
/// `fn::push::info()` so the app ships no google-services.json.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirebaseClientConfig {
    pub project_id: String,
    pub app_id: String,
    pub api_key: String,
    pub sender_id: String,
}

impl FirebaseClientConfig {
    pub fn from_value(v: Option<&Value>) -> Option<Self> {
        let m = v?.as_object()?;
        let sender = m.get("senderId").filter(|s| !s.is_nullish())?;
        Some(FirebaseClientConfig {
            project_id: str_of(m, "projectId")?,
            app_id: str_of(m, "appId")?,
            api_key: str_of(m, "apiKey")?,
            sender_id: sender.to_id_string(),
        })
    }

    pub fn to_object(&self) -> Object {
        let mut o = Object::new();
        o.insert("projectId".into(), self.project_id.clone().into());
        o.insert("appId".into(), self.app_id.clone().into());
        o.insert("apiKey".into(), self.api_key.clone().into());
        o.insert("senderId".into(), self.sender_id.clone().into());
        o
    }
}

/// `fn::push::info()`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PushInfo {
    /// A host published a VAPID key (browsers can subscribe).
    pub web_enabled: bool,
    pub public_key: Option<String>,
    pub kid: Option<String>,
    /// Every kind of device the project can reach.
    pub providers: BTreeSet<PushKind>,
    pub android: Option<FirebaseClientConfig>,
}

impl PushInfo {
    pub fn supports(&self, kind: PushKind) -> bool {
        self.providers.contains(&kind)
    }

    pub fn from_value(v: &Value) -> Self {
        let empty = Object::new();
        let m = v.as_object().unwrap_or(&empty);
        let providers = m
            .get("providers")
            .and_then(Value::as_array)
            .map(|ps| {
                ps.iter()
                    .filter_map(|p| p.as_str().and_then(PushKind::parse))
                    .collect()
            })
            .unwrap_or_default();
        PushInfo {
            web_enabled: m.get("enabled").and_then(Value::as_bool) == Some(true),
            public_key: str_of(m, "publicKey"),
            kid: str_of(m, "kid"),
            providers,
            android: FirebaseClientConfig::from_value(m.get("android")),
        }
    }
}

/// One of the user's devices (`fn::push::list()`), without its keys.
#[derive(Clone, Debug, PartialEq)]
pub struct PushDevice {
    pub id: String,
    pub endpoint: String,
    pub kind: PushKind,
    pub platform: Option<PushPlatform>,
    pub app_id: Option<String>,
    pub environment: Option<ApnsEnvironment>,
    pub label: Option<String>,
    pub user_agent: Option<String>,
    /// Rule names this device wants; `None` = every rule.
    pub rules: Option<Vec<String>>,
    pub meta: Option<Object>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_ok_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub failures: i64,
    pub disabled_at: Option<DateTime<Utc>>,
    pub disabled_reason: Option<String>,
    /// `false`: a web row made under an older VAPID key.
    pub current: bool,
    /// The device this client registered.
    pub this_device: bool,
}

impl PushDevice {
    /// A device with every optional field empty (a web row, current).
    pub fn new(id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        PushDevice {
            id: id.into(),
            endpoint: endpoint.into(),
            kind: PushKind::Web,
            platform: None,
            app_id: None,
            environment: None,
            label: None,
            user_agent: None,
            rules: None,
            meta: None,
            created_at: None,
            updated_at: None,
            last_ok_at: None,
            last_error: None,
            failures: 0,
            disabled_at: None,
            disabled_reason: None,
            current: true,
            this_device: false,
        }
    }

    pub fn disabled(&self) -> bool {
        self.disabled_at.is_some()
    }

    /// A row of `fn::push::list()`. Rows without a kind are web rows.
    pub fn from_object(m: &Object, this_endpoint: Option<&str>) -> Self {
        let endpoint = text_of(m, "endpoint");
        PushDevice {
            id: text_of(m, "id"),
            kind: enum_of(m.get("kind"), PushKind::parse).unwrap_or(PushKind::Web),
            platform: enum_of(m.get("platform"), PushPlatform::parse),
            app_id: str_of(m, "app_id"),
            environment: enum_of(m.get("environment"), ApnsEnvironment::parse),
            label: str_of(m, "label"),
            user_agent: str_of(m, "user_agent"),
            rules: m
                .get("rules")
                .and_then(Value::as_array)
                .map(|rs| rs.iter().map(Value::to_id_string).collect()),
            meta: m.get("meta").and_then(Value::as_object).cloned(),
            created_at: date_of(m.get("created_at")),
            updated_at: date_of(m.get("updated_at")),
            last_ok_at: date_of(m.get("last_ok_at")),
            last_error: str_of(m, "last_error"),
            failures: int_of(m.get("failures")).unwrap_or(0),
            disabled_at: date_of(m.get("disabled_at")),
            disabled_reason: str_of(m, "disabled_reason"),
            current: m.get("current").and_then(Value::as_bool) != Some(false),
            this_device: this_endpoint.is_some_and(|e| e == endpoint),
            endpoint,
        }
    }
}

/// A push to yourself (`fn::push::notify`): a reminder, a test.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PushMessageInput {
    pub notification: Option<Object>,
    /// `{ notification?, apns?, android? }`, as a rule's `native:` block.
    pub native: Option<Object>,
    pub data: Option<Object>,
    pub topic: Option<String>,
    /// `very-low`, `low`, `normal` or `high`.
    pub urgency: Option<String>,
    pub ttl: Option<Duration>,
    /// In the future: scheduled.
    pub send_at: Option<DateTime<Utc>>,
}

impl PushMessageInput {
    pub fn to_object(&self) -> Object {
        let mut o = Object::new();
        if let Some(n) = &self.notification {
            o.insert("notification".into(), Value::Object(n.clone()));
        }
        if let Some(n) = &self.native {
            o.insert("native".into(), Value::Object(n.clone()));
        }
        if let Some(d) = &self.data {
            o.insert("data".into(), Value::Object(d.clone()));
        }
        if let Some(t) = &self.topic {
            o.insert("topic".into(), t.clone().into());
        }
        if let Some(u) = &self.urgency {
            o.insert("urgency".into(), u.clone().into());
        }
        if let Some(ttl) = self.ttl {
            o.insert("ttl".into(), Value::Int(ttl.as_secs() as i64));
        }
        if let Some(at) = &self.send_at {
            o.insert("sendAt".into(), iso8601(at).into());
        }
        o
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushMessage {
    pub id: String,
    pub status: String,
    pub send_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

impl PushMessage {
    pub fn from_value(v: &Value) -> Self {
        let empty = Object::new();
        let m = v.as_object().unwrap_or(&empty);
        PushMessage {
            id: text_of(m, "id"),
            status: text_of(m, "status"),
            send_at: date_of(m.get("send_at")),
            created_at: date_of(m.get("created_at")),
        }
    }
}

/// What a push carries for the app (the `sp00ky` key of an APNs payload, the
/// `sp00ky` data field of an FCM message). The OS shows the title and body;
/// `notification` keeps what the app still needs (`url`, `tag`, ...). No
/// `notification`: a silent push (a nudge to refresh).
#[derive(Clone, Debug, PartialEq)]
pub struct PushPayload {
    pub v: i64,
    /// `rule` or `message`.
    pub kind: String,
    pub rule: Option<String>,
    pub message: Option<String>,
    pub table: Option<String>,
    pub id: Option<String>,
    pub op: Option<String>,
    pub topic: Option<String>,
    pub notification: Option<Object>,
    pub data: Value,
    pub ts: i64,
}

impl PushPayload {
    pub fn is_nudge(&self) -> bool {
        self.notification.is_none()
    }

    pub fn url(&self) -> Option<&str> {
        self.notification.as_ref()?.get("url")?.as_str()
    }

    /// From a map (iOS `userInfo['sp00ky']`) or a JSON string (FCM data).
    /// `None` for anything that is not a v1 sp00ky payload.
    pub fn try_parse(value: &Value) -> Option<Self> {
        let v = decoded(value)?;
        let m = v.as_object()?;
        if m.get("v").and_then(Value::as_f64) != Some(1.0) {
            return None;
        }
        let kind = m.get("kind").and_then(Value::as_str)?;
        if kind != "rule" && kind != "message" {
            return None;
        }
        Some(PushPayload {
            v: 1,
            kind: kind.to_string(),
            rule: str_of(m, "rule"),
            message: str_of(m, "message"),
            table: str_of(m, "table"),
            id: str_of(m, "id"),
            op: str_of(m, "op"),
            topic: str_of(m, "topic"),
            notification: m.get("notification").and_then(Value::as_object).cloned(),
            data: m.get("data").cloned().unwrap_or_default(),
            ts: int_of(m.get("ts")).unwrap_or(0),
        })
    }
}

// ---- errors and statuses ------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushErrorCode {
    SignedOut,
    Impersonating,
    /// The project cannot reach this kind of device (no `push.apns` /
    /// `push.fcm`).
    Disabled,
    NotSubscribed,
    /// The OS refused notifications (raised by platform code such as
    /// `spooky_push`, never by this module).
    PermissionDenied,
    /// The OS gave no device token (no Play services, no network, simulator
    /// without APNs).
    NoRegistration,
    /// The server, the transport or the client's storage failed.
    Server,
}

impl PushErrorCode {
    /// The Dart name of the code.
    pub fn as_str(self) -> &'static str {
        match self {
            PushErrorCode::SignedOut => "signedOut",
            PushErrorCode::Impersonating => "impersonating",
            PushErrorCode::Disabled => "disabled",
            PushErrorCode::NotSubscribed => "notSubscribed",
            PushErrorCode::PermissionDenied => "permissionDenied",
            PushErrorCode::NoRegistration => "noRegistration",
            PushErrorCode::Server => "server",
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("PushError({}): {}", .code.as_str(), .message)]
pub struct PushError {
    pub code: PushErrorCode,
    pub message: String,
    #[source]
    pub cause: Option<Error>,
}

impl PushError {
    pub fn new(code: PushErrorCode, message: impl Into<String>) -> Self {
        PushError {
            code,
            message: message.into(),
            cause: None,
        }
    }
}

/// Any core failure under a push call reads as a server error, as Dart wraps
/// every remote failure.
impl From<Error> for PushError {
    fn from(e: Error) -> Self {
        PushError {
            code: PushErrorCode::Server,
            message: e.to_string(),
            cause: Some(e),
        }
    }
}

impl From<PushError> for Error {
    fn from(e: PushError) -> Self {
        Error::Other(e.to_string())
    }
}

/// What [`PushModule::sync`] found or did. It never fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushSyncStatus {
    /// Registered and current.
    Ok,
    /// The server had no usable row for this token; it has one now.
    Registered,
    /// The token changed (rotation, sandbox vs production); the old row went.
    Resubscribed,
    SignedOut,
    Impersonating,
    PermissionDefault,
    PermissionDenied,
    /// The user never enabled push on this device, or turned it off.
    NotSubscribed,
    /// The project cannot reach this kind of device.
    Disabled,
    /// No token (yet).
    NoRegistration,
    Error,
}

impl PushSyncStatus {
    /// The Dart name of the status.
    pub fn as_str(self) -> &'static str {
        match self {
            PushSyncStatus::Ok => "ok",
            PushSyncStatus::Registered => "registered",
            PushSyncStatus::Resubscribed => "resubscribed",
            PushSyncStatus::SignedOut => "signedOut",
            PushSyncStatus::Impersonating => "impersonating",
            PushSyncStatus::PermissionDefault => "permissionDefault",
            PushSyncStatus::PermissionDenied => "permissionDenied",
            PushSyncStatus::NotSubscribed => "notSubscribed",
            PushSyncStatus::Disabled => "disabled",
            PushSyncStatus::NoRegistration => "noRegistration",
            PushSyncStatus::Error => "error",
        }
    }
}

// ---- the stored choice --------------------------------------------------------------------

/// The per-account record of this device's choice, kept in the client's
/// persistence under [`PUSH_ENDPOINT_KEY`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRecord {
    pub user_id: String,
    /// `false`: the user turned push off here; nothing re-enables it but them.
    pub enabled: bool,
    pub endpoint: Option<String>,
    pub at: Option<DateTime<Utc>>,
}

impl PushRecord {
    pub fn to_object(&self) -> Object {
        let mut o = Object::new();
        o.insert("userId".into(), self.user_id.clone().into());
        o.insert("enabled".into(), Value::Bool(self.enabled));
        if let Some(e) = &self.endpoint {
            o.insert("endpoint".into(), e.clone().into());
        }
        o.insert(
            "at".into(),
            iso8601(&self.at.unwrap_or_else(Utc::now)).into(),
        );
        o
    }

    /// The stored form: a JSON string, as the Dart client writes it.
    pub fn to_value(&self) -> Value {
        Value::String(Value::Object(self.to_object()).to_json_string())
    }

    /// From the stored JSON string, or a map.
    pub fn from_value(v: &Value) -> Option<Self> {
        let v = decoded(v)?;
        let m = v.as_object()?;
        Some(PushRecord {
            user_id: str_of(m, "userId")?,
            enabled: m.get("enabled").and_then(Value::as_bool) == Some(true),
            endpoint: str_of(m, "endpoint"),
            at: date_of(m.get("at")),
        })
    }
}

// ---- the sync decision --------------------------------------------------------------------

/// Everything [`decide_push_sync`] looks at. `info` and `devices` are `None`
/// until fetched; the first call tells whether they are needed.
#[derive(Clone, Debug, PartialEq)]
pub struct PushSyncInput {
    pub user_id: Option<String>,
    pub impersonating: bool,
    pub permission: PushPermission,
    pub token: Option<PushToken>,
    /// The stored record, only if it belongs to `user_id`.
    pub record: Option<PushRecord>,
    pub auto_register: bool,
    pub info: Option<PushInfo>,
    pub devices: Option<Vec<PushDevice>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PushSyncDecision {
    /// Nothing to do but report.
    Done(PushSyncStatus),
    /// Ask the server (`info`, `list`) and decide again.
    NeedsServer,
    /// Permission gone: remove these rows, keep the user's choice so the device
    /// comes back when the permission does.
    DropRows {
        status: PushSyncStatus,
        endpoints: Vec<String>,
    },
    /// (Re-)register `token`, carrying label, rules and meta over, then remove
    /// `remove` (the previous endpoint) if set.
    Register {
        status: PushSyncStatus,
        token: PushToken,
        label: Option<String>,
        rules: Option<Vec<String>>,
        meta: Option<Object>,
        remove: Option<String>,
    },
}

impl PushSyncDecision {
    pub fn status(&self) -> PushSyncStatus {
        match self {
            PushSyncDecision::Done(s) => *s,
            PushSyncDecision::NeedsServer => PushSyncStatus::Ok,
            PushSyncDecision::DropRows { status, .. }
            | PushSyncDecision::Register { status, .. } => *status,
        }
    }
}

/// What `sync` does, as a function of state only.
pub fn decide_push_sync(i: &PushSyncInput) -> PushSyncDecision {
    if i.user_id.is_none() {
        return PushSyncDecision::Done(PushSyncStatus::SignedOut);
    }
    if i.impersonating {
        return PushSyncDecision::Done(PushSyncStatus::Impersonating);
    }
    let record = i.record.as_ref();
    let opted_in = record.is_some_and(|r| r.enabled);
    let opted_out = record.is_some_and(|r| !r.enabled);
    if !i.permission.allows_delivery() {
        let status = if i.permission == PushPermission::Denied {
            PushSyncStatus::PermissionDenied
        } else {
            PushSyncStatus::PermissionDefault
        };
        let mut endpoints: Vec<String> = Vec::new();
        let candidates = record
            .and_then(|r| r.endpoint.clone())
            .into_iter()
            .chain(i.token.as_ref().map(PushToken::endpoint));
        for e in candidates {
            if !endpoints.contains(&e) {
                endpoints.push(e);
            }
        }
        return if opted_in && !endpoints.is_empty() {
            PushSyncDecision::DropRows { status, endpoints }
        } else {
            PushSyncDecision::Done(status)
        };
    }
    let Some(token) = &i.token else {
        return PushSyncDecision::Done(PushSyncStatus::NoRegistration);
    };
    if opted_out || (!opted_in && !i.auto_register) {
        return PushSyncDecision::Done(PushSyncStatus::NotSubscribed);
    }
    let (Some(info), Some(devices)) = (&i.info, &i.devices) else {
        return PushSyncDecision::NeedsServer;
    };
    if !info.supports(token.kind) {
        return PushSyncDecision::Done(PushSyncStatus::Disabled);
    }

    let find = |e: Option<&str>| e.and_then(|e| devices.iter().find(|d| d.endpoint == e));
    let endpoint = token.endpoint();
    let row = find(Some(&endpoint));
    let previous = record
        .and_then(|r| r.endpoint.clone())
        .filter(|e| *e != endpoint);
    let expected_env = (token.kind == PushKind::Apns)
        .then(|| token.environment.unwrap_or(ApnsEnvironment::Production));
    let healthy = row.is_some_and(|r| {
        !r.disabled()
            && r.current
            && r.app_id.as_deref() == Some(token.app_id.as_str())
            && r.environment == expected_env
    });
    if healthy && previous.is_none() {
        return PushSyncDecision::Done(PushSyncStatus::Ok);
    }
    let carry = row.or_else(|| find(previous.as_deref()));
    PushSyncDecision::Register {
        status: if previous.is_some() {
            PushSyncStatus::Resubscribed
        } else {
            PushSyncStatus::Registered
        },
        token: token.clone(),
        label: carry.and_then(|d| d.label.clone()),
        rules: carry.and_then(|d| d.rules.clone()),
        meta: carry.and_then(|d| d.meta.clone()),
        remove: previous,
    }
}

// ---- the module ---------------------------------------------------------------------------

/// What the push module needs from auth (Dart `Sp00kyAuth`).
pub trait PushAuth {
    /// The signed-in user's id (`user:abc`).
    fn user_id(&self) -> Option<String>;
    /// The session was opened through the platform's impersonation access.
    fn is_impersonating(&self) -> bool;
    /// Run `hook` before the session is dropped, while it is still valid.
    fn on_before_sign_out(&self, hook: SignOutHook) -> Subscription;
}

impl PushAuth for AuthService {
    fn user_id(&self) -> Option<String> {
        AuthService::user_id(self)
    }
    fn is_impersonating(&self) -> bool {
        AuthService::is_impersonating(self)
    }
    fn on_before_sign_out(&self, hook: SignOutHook) -> Subscription {
        AuthService::on_before_sign_out(self, hook)
    }
}

/// Options of [`PushModule::register`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PushRegisterOptions {
    pub label: Option<String>,
    /// Rule names this device wants; empty or `None` = every rule.
    pub rules: Option<Vec<String>>,
    pub meta: Option<Object>,
    pub user_agent: Option<String>,
}

/// Options of [`PushModule::sync`].
#[derive(Clone, Debug, PartialEq)]
pub struct PushSyncOptions {
    pub permission: PushPermission,
    /// Register a device the user never opted in on (never one they opted
    /// out on).
    pub auto_register: bool,
    pub user_agent: Option<String>,
}

impl Default for PushSyncOptions {
    fn default() -> Self {
        PushSyncOptions {
            permission: PushPermission::Granted,
            auto_register: false,
            user_agent: None,
        }
    }
}

/// The change [`PushModule::update`] makes to this device's row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PushDeviceUpdate {
    pub label: Option<String>,
    /// `Some(vec![])` = every rule again.
    pub rules: Option<Vec<String>>,
    pub meta: Option<Object>,
}

struct Inner {
    remote: RemoteQuery,
    auth: Rc<dyn PushAuth>,
    storage: Rc<dyn PersistenceClient>,
    unsubscribe_on_sign_out: Cell<bool>,
    info: RefCell<Option<PushInfo>>,
    last_token: RefCell<Option<PushToken>>,
    off_sign_out: RefCell<Option<Subscription>>,
    /// One at a time: an auto-sync must never race an explicit enable.
    serial: tokio::sync::Mutex<()>,
}

/// Native push registration for this device (Dart `PushModule`). Cheap to
/// clone; clones share one state.
#[derive(Clone)]
pub struct PushModule(Rc<Inner>);

type PushResult<T> = Result<T, PushError>;

impl PushModule {
    /// How long sign-out waits for the server to drop this device's row.
    pub const SIGN_OUT_TIMEOUT: Duration = Duration::from_millis(1500);

    pub fn new(
        remote: RemoteQuery,
        auth: Rc<dyn PushAuth>,
        storage: Rc<dyn PersistenceClient>,
    ) -> Self {
        PushModule(Rc::new(Inner {
            remote,
            auth,
            storage,
            unsubscribe_on_sign_out: Cell::new(true),
            info: RefCell::new(None),
            last_token: RefCell::new(None),
            off_sign_out: RefCell::new(None),
            serial: tokio::sync::Mutex::new(()),
        }))
    }

    /// Remove this device's row when the user signs out (the choice itself is
    /// kept, so it comes back on their next sign-in). On by default.
    pub fn unsubscribe_on_sign_out(&self) -> bool {
        self.0.unsubscribe_on_sign_out.get()
    }

    pub fn set_unsubscribe_on_sign_out(&self, on: bool) {
        self.0.unsubscribe_on_sign_out.set(on);
    }

    /// Registers the sign-out hook. Idempotent.
    pub fn attach(&self) {
        if self.0.off_sign_out.borrow().is_some() {
            return;
        }
        let weak: Weak<Inner> = Rc::downgrade(&self.0);
        let hook: SignOutHook = Rc::new(move |ctx| {
            let module = weak.upgrade().map(PushModule);
            async move {
                if let Some(module) = module {
                    module.on_sign_out(ctx).await;
                }
                Ok(())
            }
            .boxed_local()
        });
        let off = self.0.auth.on_before_sign_out(hook);
        *self.0.off_sign_out.borrow_mut() = Some(off);
    }

    pub fn dispose(&self) {
        let off = self.0.off_sign_out.borrow_mut().take();
        if let Some(off) = off {
            off.cancel();
        }
    }

    fn user_id(&self) -> Option<String> {
        self.0.auth.user_id()
    }

    async fn call(&self, sql: &str, vars: Option<Object>) -> PushResult<Vec<Value>> {
        Ok((self.0.remote)(sql.to_string(), vars).await?)
    }

    async fn one(&self, sql: &str, vars: Option<Object>) -> PushResult<Value> {
        Ok(self.call(sql, vars).await?.pop().unwrap_or_default())
    }

    async fn record(&self, user_id: &str) -> PushResult<Option<PushRecord>> {
        let stored = self.0.storage.get(PUSH_ENDPOINT_KEY).await?;
        Ok(stored
            .as_ref()
            .and_then(PushRecord::from_value)
            .filter(|r| r.user_id == user_id))
    }

    async fn save_record(&self, record: PushRecord) -> PushResult<()> {
        Ok(self
            .0
            .storage
            .set(PUSH_ENDPOINT_KEY, record.to_value())
            .await?)
    }

    async fn this_endpoint(&self, user_id: Option<&str>) -> PushResult<Option<String>> {
        let last = self.0.last_token.borrow().as_ref().map(PushToken::endpoint);
        if last.is_some() {
            return Ok(last);
        }
        let Some(user_id) = user_id else {
            return Ok(None);
        };
        Ok(self.record(user_id).await?.and_then(|r| r.endpoint))
    }

    fn require_user(&self) -> PushResult<String> {
        let Some(user_id) = self.user_id() else {
            return Err(PushError::new(PushErrorCode::SignedOut, "sign in first"));
        };
        if self.0.auth.is_impersonating() {
            return Err(PushError::new(
                PushErrorCode::Impersonating,
                "not available while impersonating",
            ));
        }
        Ok(user_id)
    }

    /// `fn::push::info()`, cached; a failed read is not.
    pub async fn info(&self, refresh: bool) -> PushResult<PushInfo> {
        if !refresh {
            if let Some(cached) = self.0.info.borrow().clone() {
                return Ok(cached);
            }
        }
        let info = PushInfo::from_value(&self.one("RETURN fn::push::info();", None).await?);
        *self.0.info.borrow_mut() = Some(info.clone());
        Ok(info)
    }

    /// This device has a stored, enabled registration for the signed-in user.
    /// Local only.
    pub async fn is_registered(&self) -> PushResult<bool> {
        let Some(user_id) = self.user_id() else {
            return Ok(false);
        };
        Ok(self
            .record(&user_id)
            .await?
            .is_some_and(|r| r.enabled && r.endpoint.is_some()))
    }

    /// Register this device for the signed-in user. Remembers the choice.
    pub async fn register(
        &self,
        token: PushToken,
        options: PushRegisterOptions,
    ) -> PushResult<PushDevice> {
        let _serial = self.0.serial.lock().await;
        self.register_now(token, options, None).await
    }

    async fn register_now(
        &self,
        token: PushToken,
        options: PushRegisterOptions,
        remove: Option<String>,
    ) -> PushResult<PushDevice> {
        let user_id = self.require_user()?;
        let info = self.info(true).await?;
        if !info.supports(token.kind) {
            return Err(PushError::new(
                PushErrorCode::Disabled,
                format!("the project cannot reach {} devices", token.kind.as_str()),
            ));
        }
        let previous = match remove {
            Some(r) => Some(r),
            None => self.record(&user_id).await?.and_then(|r| r.endpoint),
        };
        let mut opts = Object::new();
        if let Some(label) = options.label {
            opts.insert("label".into(), label.into());
        }
        if let Some(rules) = options.rules.filter(|r| !r.is_empty()) {
            opts.insert(
                "rules".into(),
                Value::Array(rules.into_iter().map(Value::from).collect()),
            );
        }
        if let Some(meta) = options.meta {
            opts.insert("meta".into(), Value::Object(meta));
        }
        if let Some(ua) = options.user_agent {
            opts.insert("userAgent".into(), ua.into());
        }
        let row = self
            .one(
                "RETURN fn::push::register($device, $opts);",
                vars([
                    ("device", Value::Object(token.to_object())),
                    ("opts", Value::Object(opts)),
                ]),
            )
            .await?;
        let endpoint = token.endpoint();
        *self.0.last_token.borrow_mut() = Some(token);
        // The new row first: a failure here leaves the old one working.
        if let Some(previous) = previous.filter(|p| *p != endpoint) {
            let removed = self
                .call(
                    "RETURN fn::push::unsubscribe($e);",
                    vars([("e", previous.into())]),
                )
                .await;
            if let Err(e) = removed {
                tracing::debug!(target: "spooky", "Removing the previous push endpoint failed: {e}");
            }
        }
        self.save_record(PushRecord {
            user_id,
            enabled: true,
            endpoint: Some(endpoint.clone()),
            at: None,
        })
        .await?;
        let empty = Object::new();
        Ok(PushDevice::from_object(
            row.as_object().unwrap_or(&empty),
            Some(&endpoint),
        ))
    }

    /// Turn push off on this device (`all`: on every device of the user).
    /// Remembered, so no automatic sync turns it back on. Answers how many
    /// rows the server removed.
    pub async fn unsubscribe(&self, all: bool) -> PushResult<i64> {
        let _serial = self.0.serial.lock().await;
        let Some(user_id) = self.user_id() else {
            return Err(PushError::new(PushErrorCode::SignedOut, "sign in first"));
        };
        let mut removed = 0;
        let endpoint = self.this_endpoint(Some(&user_id)).await?;
        // An impersonating session cannot touch the rows; the choice is still
        // recorded.
        if !self.0.auth.is_impersonating() && (all || endpoint.is_some()) {
            // A bound absent value would travel as NULL, which
            // `option<string>` refuses: spell "every device" as NONE.
            let n = match endpoint.filter(|_| !all) {
                Some(e) => {
                    self.one("RETURN fn::push::unsubscribe($e);", vars([("e", e.into())]))
                        .await?
                }
                None => {
                    self.one("RETURN fn::push::unsubscribe(NONE);", None)
                        .await?
                }
            };
            removed = int_of(Some(&n)).unwrap_or(0);
        }
        self.save_record(PushRecord {
            user_id,
            enabled: false,
            endpoint: None,
            at: None,
        })
        .await?;
        Ok(removed)
    }

    /// Bring the server in line with this device: register after a token
    /// rotation, an APNs environment switch, a disabled row, or a permission
    /// that came back; remove the row when the permission went. Never fails.
    pub async fn sync(&self, token: Option<PushToken>, options: PushSyncOptions) -> PushSyncStatus {
        let _serial = self.0.serial.lock().await;
        match self.sync_now(token, options).await {
            Ok(status) => status,
            Err(e) => {
                tracing::debug!(target: "spooky", "push sync failed: {e}");
                PushSyncStatus::Error
            }
        }
    }

    async fn sync_now(
        &self,
        token: Option<PushToken>,
        options: PushSyncOptions,
    ) -> PushResult<PushSyncStatus> {
        let user_id = self.user_id();
        if let Some(t) = &token {
            *self.0.last_token.borrow_mut() = Some(t.clone());
        }
        let record = match &user_id {
            Some(u) => self.record(u).await?,
            None => None,
        };
        let mut input = PushSyncInput {
            user_id: user_id.clone(),
            impersonating: self.0.auth.is_impersonating(),
            permission: options.permission,
            token,
            record,
            auto_register: options.auto_register,
            info: None,
            devices: None,
        };
        let mut decision = decide_push_sync(&input);
        if decision == PushSyncDecision::NeedsServer {
            let info = self.info(true).await?;
            let list = self.one("RETURN fn::push::list();", None).await?;
            input.info = Some(info);
            input.devices = Some(
                list.as_array()
                    .map(|rows| {
                        rows.iter()
                            .filter_map(Value::as_object)
                            .map(|row| PushDevice::from_object(row, None))
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            decision = decide_push_sync(&input);
        }
        let status = decision.status();
        match decision {
            PushSyncDecision::Done(_) | PushSyncDecision::NeedsServer => {
                let endpoint = input.token.as_ref().map(PushToken::endpoint);
                let stored = input.record.as_ref().and_then(|r| r.endpoint.clone());
                if let (PushSyncStatus::Ok, Some(user_id)) = (status, user_id) {
                    if stored != endpoint {
                        self.save_record(PushRecord {
                            user_id,
                            enabled: true,
                            endpoint,
                            at: None,
                        })
                        .await?;
                    }
                }
            }
            PushSyncDecision::DropRows { endpoints, .. } => {
                for e in endpoints {
                    self.call("RETURN fn::push::unsubscribe($e);", vars([("e", e.into())]))
                        .await?;
                }
            }
            PushSyncDecision::Register {
                token,
                label,
                rules,
                meta,
                remove,
                ..
            } => {
                let options = PushRegisterOptions {
                    label,
                    rules,
                    meta,
                    user_agent: options.user_agent,
                };
                self.register_now(token, options, remove).await?;
            }
        }
        Ok(status)
    }

    /// Change this device's label / rules (`Some(vec![])` = every rule again)
    /// / meta.
    pub async fn update(&self, changes: PushDeviceUpdate) -> PushResult<Option<PushDevice>> {
        let user_id = self.require_user()?;
        let Some(endpoint) = self.this_endpoint(Some(&user_id)).await? else {
            return Err(PushError::new(
                PushErrorCode::NotSubscribed,
                "this device is not registered",
            ));
        };
        let mut o = Object::new();
        if let Some(label) = changes.label {
            o.insert("label".into(), label.into());
        }
        if let Some(rules) = changes.rules {
            o.insert(
                "rules".into(),
                Value::Array(rules.into_iter().map(Value::from).collect()),
            );
        }
        if let Some(meta) = changes.meta {
            o.insert("meta".into(), Value::Object(meta));
        }
        let rows = self
            .one(
                "RETURN fn::push::update($e, $o);",
                vars([("e", endpoint.clone().into()), ("o", Value::Object(o))]),
            )
            .await?;
        Ok(rows
            .as_array()
            .and_then(|r| r.first())
            .and_then(Value::as_object)
            .map(|first| PushDevice::from_object(first, Some(&endpoint))))
    }

    /// Every device of the signed-in user (web and native).
    pub async fn devices(&self) -> PushResult<Vec<PushDevice>> {
        let Some(user_id) = self.user_id() else {
            return Ok(Vec::new());
        };
        let endpoint = self.this_endpoint(Some(&user_id)).await?;
        let rows = self.one("RETURN fn::push::list();", None).await?;
        Ok(rows
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(Value::as_object)
                    .map(|row| PushDevice::from_object(row, endpoint.as_deref()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// A push to yourself, now or at [`PushMessageInput::send_at`].
    pub async fn notify(&self, message: &PushMessageInput) -> PushResult<PushMessage> {
        self.require_user()?;
        let out = self
            .one(
                "RETURN fn::push::notify($m);",
                vars([("m", Value::Object(message.to_object()))]),
            )
            .await?;
        Ok(PushMessage::from_value(&out))
    }

    /// Cancel one of your scheduled pushes.
    pub async fn cancel(&self, id: &str) -> PushResult<bool> {
        self.require_user()?;
        let out = self
            .one(
                "RETURN fn::push::cancel($id);",
                vars([("id", Value::RecordId(RecordId::parse(id)))]),
            )
            .await?;
        Ok(out.as_bool() == Some(true))
    }

    /// A visible test push to every device of the user.
    pub async fn test(&self, title: Option<&str>, body: Option<&str>) -> PushResult<PushMessage> {
        self.require_user()?;
        let mut o = Object::new();
        if let Some(t) = title {
            o.insert("title".into(), t.into());
        }
        if let Some(b) = body {
            o.insert("body".into(), b.into());
        }
        let out = self
            .one(
                "RETURN fn::push::test($o);",
                vars([("o", Value::Object(o))]),
            )
            .await?;
        Ok(PushMessage::from_value(&out))
    }

    async fn on_sign_out(&self, ctx: SignOutContext) {
        if !self.unsubscribe_on_sign_out() || ctx.impersonating || ctx.user_id.is_none() {
            return;
        }
        let out: PushResult<()> = async {
            let Some(endpoint) = self.this_endpoint(ctx.user_id.as_deref()).await? else {
                return Ok(());
            };
            let unsubscribe = (self.0.remote)(
                "RETURN fn::push::unsubscribe($e);".to_string(),
                vars([("e", endpoint.into())]),
            );
            match tokio::time::timeout(Self::SIGN_OUT_TIMEOUT, unsubscribe).await {
                Ok(r) => r.map(drop).map_err(PushError::from),
                Err(_) => Err(PushError::new(PushErrorCode::Server, "timed out")),
            }
        }
        .await;
        if let Err(e) = out {
            // Best effort: the next owner of this device (newest owner wins)
            // or the push service (token gone) cleans up otherwise.
            tracing::debug!(target: "spooky", "push unsubscribe on sign-out failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::TimeZone;
    use futures::future::{join_all, LocalBoxFuture};
    use serde_json::json;

    use super::*;
    use crate::modules::auth::IMPERSONATION_ACCESS;
    use crate::services::persistence::MemoryPersistence;
    use crate::testing::run_local;
    use crate::value::object_from_json;

    fn apns() -> PushToken {
        PushToken::new(PushKind::Apns, "AB12", "im.app")
            .with_platform(PushPlatform::Ios)
            .with_environment(ApnsEnvironment::Sandbox)
    }

    fn fcm() -> PushToken {
        PushToken::new(PushKind::Fcm, "fcm-tok", "im.app").with_platform(PushPlatform::Android)
    }

    fn info() -> PushInfo {
        PushInfo {
            providers: [PushKind::Apns, PushKind::Fcm].into(),
            ..PushInfo::default()
        }
    }

    struct Spec {
        disabled: bool,
        env: Option<ApnsEnvironment>,
        app: &'static str,
        label: Option<&'static str>,
    }

    impl Default for Spec {
        fn default() -> Self {
            Spec {
                disabled: false,
                env: Some(ApnsEnvironment::Sandbox),
                app: "im.app",
                label: None,
            }
        }
    }

    fn row(endpoint: &str, r: Spec) -> PushDevice {
        PushDevice {
            kind: PushKind::Apns,
            app_id: Some(r.app.to_string()),
            environment: r.env,
            label: r.label.map(str::to_string),
            rules: r.label.map(|_| vec!["dm".to_string()]),
            disabled_at: r
                .disabled
                .then(|| Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()),
            ..PushDevice::new("_00_push_subscription:x", endpoint)
        }
    }

    fn input() -> PushSyncInput {
        PushSyncInput {
            user_id: Some("user:a".into()),
            impersonating: false,
            permission: PushPermission::Granted,
            token: Some(apns()),
            record: None,
            auto_register: false,
            info: None,
            devices: None,
        }
    }

    fn opted_in() -> Option<PushRecord> {
        Some(PushRecord {
            user_id: "user:a".into(),
            enabled: true,
            endpoint: Some("apns:ab12".into()),
            at: None,
        })
    }

    fn decide(i: PushSyncInput) -> PushSyncDecision {
        decide_push_sync(&i)
    }

    // ---- decide_push_sync -------------------------------------------------------------

    #[test]
    fn decide_answers_that_need_no_server() {
        let status = |i: PushSyncInput| decide(i).status();
        assert_eq!(
            status(PushSyncInput {
                user_id: None,
                ..input()
            }),
            PushSyncStatus::SignedOut
        );
        assert_eq!(
            status(PushSyncInput {
                impersonating: true,
                ..input()
            }),
            PushSyncStatus::Impersonating
        );
        assert_eq!(
            status(PushSyncInput {
                token: None,
                record: opted_in(),
                ..input()
            }),
            PushSyncStatus::NoRegistration
        );
        assert_eq!(
            status(input()),
            PushSyncStatus::NotSubscribed,
            "never opted in"
        );
        assert_eq!(
            status(PushSyncInput {
                record: Some(PushRecord {
                    user_id: "user:a".into(),
                    enabled: false,
                    endpoint: None,
                    at: None,
                }),
                auto_register: true,
                ..input()
            }),
            PushSyncStatus::NotSubscribed,
            "an explicit opt-out beats auto_register"
        );
        assert_eq!(
            decide(PushSyncInput {
                record: opted_in(),
                ..input()
            }),
            PushSyncDecision::NeedsServer
        );
        assert_eq!(
            decide(PushSyncInput {
                auto_register: true,
                ..input()
            }),
            PushSyncDecision::NeedsServer
        );
    }

    #[test]
    fn decide_a_permission_that_went_away_drops_the_rows_but_keeps_the_choice() {
        let d = decide(PushSyncInput {
            permission: PushPermission::Denied,
            record: opted_in(),
            token: Some(fcm()),
            ..input()
        });
        assert_eq!(
            d,
            PushSyncDecision::DropRows {
                status: PushSyncStatus::PermissionDenied,
                endpoints: vec!["apns:ab12".into(), "fcm:fcm-tok".into()],
            }
        );
        assert_eq!(
            decide(PushSyncInput {
                permission: PushPermission::NotDetermined,
                ..input()
            })
            .status(),
            PushSyncStatus::PermissionDefault
        );
        assert_eq!(
            decide(PushSyncInput {
                permission: PushPermission::Provisional,
                record: opted_in(),
                ..input()
            }),
            PushSyncDecision::NeedsServer
        );
    }

    #[test]
    fn decide_with_the_server_ok_registered_resubscribed_disabled() {
        let with = |info: PushInfo, devices: Vec<PushDevice>| {
            decide(PushSyncInput {
                record: opted_in(),
                info: Some(info),
                devices: Some(devices),
                ..input()
            })
        };
        let web_only = PushInfo {
            providers: [PushKind::Web].into(),
            ..PushInfo::default()
        };
        assert_eq!(with(web_only, vec![]).status(), PushSyncStatus::Disabled);
        assert_eq!(
            with(info(), vec![row("apns:ab12", Spec::default())]).status(),
            PushSyncStatus::Ok
        );

        let cases = [
            ("no row", vec![]),
            (
                "disabled",
                vec![row(
                    "apns:ab12",
                    Spec {
                        disabled: true,
                        ..Spec::default()
                    },
                )],
            ),
            (
                "other environment",
                vec![row(
                    "apns:ab12",
                    Spec {
                        env: Some(ApnsEnvironment::Production),
                        ..Spec::default()
                    },
                )],
            ),
            (
                "other app",
                vec![row(
                    "apns:ab12",
                    Spec {
                        app: "im.other",
                        ..Spec::default()
                    },
                )],
            ),
        ];
        for (why, devices) in cases {
            let d = with(info(), devices);
            assert!(matches!(d, PushSyncDecision::Register { .. }), "{why}");
            assert_eq!(d.status(), PushSyncStatus::Registered, "{why}");
        }

        // The token rotated: register the new one, carry label/rules over,
        // remove the old one.
        let d = decide(PushSyncInput {
            record: Some(PushRecord {
                user_id: "user:a".into(),
                enabled: true,
                endpoint: Some("apns:old".into()),
                at: None,
            }),
            info: Some(info()),
            devices: Some(vec![row(
                "apns:old",
                Spec {
                    label: Some("iPhone"),
                    ..Spec::default()
                },
            )]),
            ..input()
        });
        let PushSyncDecision::Register {
            status,
            remove,
            label,
            rules,
            ..
        } = d
        else {
            panic!("expected a registration");
        };
        assert_eq!(status, PushSyncStatus::Resubscribed);
        assert_eq!(remove.as_deref(), Some("apns:old"));
        assert_eq!(label.as_deref(), Some("iPhone"));
        assert_eq!(rules, Some(vec!["dm".to_string()]));
    }

    #[test]
    fn a_production_row_is_healthy_for_a_token_without_an_environment() {
        let token = PushToken::new(PushKind::Apns, "AB12", "im.app");
        let d = decide(PushSyncInput {
            token: Some(token),
            record: opted_in(),
            info: Some(info()),
            devices: Some(vec![row(
                "apns:ab12",
                Spec {
                    env: Some(ApnsEnvironment::Production),
                    ..Spec::default()
                },
            )]),
            ..input()
        });
        assert_eq!(d, PushSyncDecision::Done(PushSyncStatus::Ok));
    }

    // ---- fakes --------------------------------------------------------------------------

    type Call = (String, Option<Object>);

    #[derive(Default)]
    struct FakeRemote {
        answers: RefCell<HashMap<String, Value>>,
        calls: RefCell<Vec<Call>>,
        fail: Cell<bool>,
        hang: Cell<bool>,
    }

    impl FakeRemote {
        fn answer(&self, function: &str, v: serde_json::Value) {
            self.answers
                .borrow_mut()
                .insert(format!("fn::push::{function}"), Value::from(v));
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.borrow().clone()
        }

        fn last(&self) -> Call {
            self.calls.borrow().last().cloned().expect("a call")
        }

        fn query(self: &Rc<Self>) -> RemoteQuery {
            let me = self.clone();
            Rc::new(
                move |sql: String,
                      vars: Option<Object>|
                      -> LocalBoxFuture<'static, crate::Result<Vec<Value>>> {
                    me.calls.borrow_mut().push((sql.clone(), vars));
                    if me.hang.get() {
                        return futures::future::pending().boxed_local();
                    }
                    let out = if me.fail.get() {
                        Err(Error::Network("offline".into()))
                    } else {
                        let function = regex::Regex::new(r"fn::push::[a-z]+")
                            .unwrap()
                            .find(&sql)
                            .map(|m| m.as_str().to_string())
                            .unwrap_or_default();
                        Ok(vec![me
                            .answers
                            .borrow()
                            .get(&function)
                            .cloned()
                            .unwrap_or_default()])
                    };
                    async move { out }.boxed_local()
                },
            )
        }
    }

    struct FakeAuth {
        user: RefCell<Option<String>>,
        access: RefCell<String>,
        hooks: RefCell<Vec<(u64, SignOutHook)>>,
        next: Cell<u64>,
    }

    impl Default for FakeAuth {
        fn default() -> Self {
            FakeAuth {
                user: RefCell::new(Some("user:a".into())),
                access: RefCell::new("account".into()),
                hooks: RefCell::default(),
                next: Cell::new(0),
            }
        }
    }

    impl FakeAuth {
        fn set_user(&self, user: Option<&str>) {
            *self.user.borrow_mut() = user.map(str::to_string);
        }

        fn set_access(&self, access: &str) {
            *self.access.borrow_mut() = access.to_string();
        }

        async fn run_hooks(&self, impersonating: bool) {
            let ctx = SignOutContext {
                user_id: self.user.borrow().clone(),
                token: Some("t".into()),
                impersonating,
            };
            let hooks: Vec<SignOutHook> =
                self.hooks.borrow().iter().map(|(_, h)| h.clone()).collect();
            let futures: Vec<LocalBoxFuture<'static, crate::Result<()>>> =
                hooks.iter().map(|h| h(ctx.clone())).collect();
            for r in join_all(futures).await {
                r.unwrap();
            }
        }
    }

    impl PushAuth for Rc<FakeAuth> {
        fn user_id(&self) -> Option<String> {
            self.user.borrow().clone()
        }
        fn is_impersonating(&self) -> bool {
            *self.access.borrow() == IMPERSONATION_ACCESS
        }
        fn on_before_sign_out(&self, hook: SignOutHook) -> Subscription {
            let id = self.next.get() + 1;
            self.next.set(id);
            self.hooks.borrow_mut().push((id, hook));
            let me = Rc::downgrade(self);
            Subscription::new(move || {
                if let Some(me) = me.upgrade() {
                    me.hooks.borrow_mut().retain(|(i, _)| *i != id);
                }
            })
        }
    }

    struct Rig {
        remote: Rc<FakeRemote>,
        auth: Rc<FakeAuth>,
        storage: Rc<MemoryPersistence>,
        push: PushModule,
    }

    fn rig() -> Rig {
        let remote = Rc::new(FakeRemote::default());
        let auth = Rc::new(FakeAuth::default());
        let storage = Rc::new(MemoryPersistence::new());
        let push = PushModule::new(remote.query(), Rc::new(auth.clone()), storage.clone());
        push.attach();
        remote.answer(
            "info",
            json!({
                "enabled": false,
                "providers": ["apns", "fcm"],
                "android": {"projectId": "p", "appId": "1:2:android:3", "apiKey": "k", "senderId": 42},
            }),
        );
        remote.answer(
            "register",
            json!({"id": "_00_push_subscription:n", "endpoint": "apns:ab12", "kind": "apns"}),
        );
        remote.answer("list", json!([]));
        remote.answer("unsubscribe", json!(1));
        Rig {
            remote,
            auth,
            storage,
            push,
        }
    }

    impl Rig {
        async fn store(&self, raw: &str) {
            self.storage
                .set(PUSH_ENDPOINT_KEY, Value::String(raw.into()))
                .await
                .unwrap();
        }

        async fn stored(&self) -> String {
            match self.storage.get(PUSH_ENDPOINT_KEY).await.unwrap() {
                Some(Value::String(s)) => s,
                other => panic!("expected a stored string, got {other:?}"),
            }
        }
    }

    fn vars_of(pairs: serde_json::Value) -> Option<Object> {
        Some(object_from_json(pairs))
    }

    // ---- PushModule ---------------------------------------------------------------------

    #[tokio::test]
    async fn info_parses_providers_and_the_firebase_config_and_caches() {
        run_local(async {
            let r = rig();
            r.remote.fail.set(true);
            let err = r.push.info(false).await.unwrap_err();
            assert_eq!(err.code, PushErrorCode::Server);
            r.remote.fail.set(false);
            let i = r.push.info(false).await.unwrap();
            assert!(i.supports(PushKind::Fcm));
            assert!(!i.supports(PushKind::Web));
            assert_eq!(i.android.as_ref().map(|a| a.sender_id.as_str()), Some("42"));
            r.push.info(false).await.unwrap();
            let reads = r
                .remote
                .calls()
                .iter()
                .filter(|c| c.0.contains("info"))
                .count();
            assert_eq!(reads, 2, "the failed read was not cached, the good one is");
        })
        .await;
    }

    #[tokio::test]
    async fn register_sends_the_device_remembers_the_choice_removes_the_previous_endpoint() {
        run_local(async {
            let r = rig();
            r.store(r#"{"userId":"user:a","enabled":true,"endpoint":"apns:old"}"#)
                .await;
            let d = r
                .push
                .register(
                    apns(),
                    PushRegisterOptions {
                        label: Some("iPhone".into()),
                        rules: Some(vec![]),
                        ..PushRegisterOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(d.this_device);
            let calls = r.remote.calls();
            let reg = calls.iter().find(|c| c.0.contains("register")).unwrap();
            let reg_vars = reg.1.clone().unwrap();
            assert_eq!(
                reg_vars["device"],
                Value::from(json!({
                    "kind": "apns",
                    "token": "AB12",
                    "appId": "im.app",
                    "platform": "ios",
                    "environment": "sandbox",
                }))
            );
            assert_eq!(
                reg_vars["opts"],
                Value::from(json!({"label": "iPhone"})),
                "empty rules mean every rule: not sent"
            );
            let unsub = calls
                .iter()
                .rev()
                .find(|c| c.0.contains("unsubscribe"))
                .unwrap();
            assert_eq!(unsub.1, vars_of(json!({"e": "apns:old"})));
            assert!(r.push.is_registered().await.unwrap());
            assert!(r.stored().await.contains(r#""endpoint":"apns:ab12""#));
        })
        .await;
    }

    #[tokio::test]
    async fn refusals_make_no_call() {
        run_local(async {
            let r = rig();
            r.auth.set_access(IMPERSONATION_ACCESS);
            let err = r
                .push
                .register(apns(), PushRegisterOptions::default())
                .await
                .unwrap_err();
            assert_eq!(err.code, PushErrorCode::Impersonating);
            assert_eq!(
                r.push.sync(Some(apns()), PushSyncOptions::default()).await,
                PushSyncStatus::Impersonating
            );
            r.auth.set_access("account");
            r.auth.set_user(None);
            let err = r
                .push
                .register(apns(), PushRegisterOptions::default())
                .await
                .unwrap_err();
            assert_eq!(err.code, PushErrorCode::SignedOut);
            assert!(r.remote.calls().is_empty());

            r.auth.set_user(Some("user:a"));
            r.remote.answer("info", json!({"providers": ["web"]}));
            let err = r
                .push
                .register(fcm(), PushRegisterOptions::default())
                .await
                .unwrap_err();
            assert_eq!(err.code, PushErrorCode::Disabled);
        })
        .await;
    }

    #[tokio::test]
    async fn sync_registers_an_opted_in_device_once_then_reports_ok() {
        run_local(async {
            let r = rig();
            let sync = || r.push.sync(Some(apns()), PushSyncOptions::default());
            assert_eq!(sync().await, PushSyncStatus::NotSubscribed);
            r.store(r#"{"userId":"user:a","enabled":true,"endpoint":"apns:ab12"}"#)
                .await;
            assert_eq!(sync().await, PushSyncStatus::Registered);
            r.remote.answer(
                "list",
                json!([{
                    "id": "x",
                    "endpoint": "apns:ab12",
                    "kind": "apns",
                    "app_id": "im.app",
                    "environment": "sandbox",
                    "current": true,
                }]),
            );
            let before = r.remote.calls().len();
            assert_eq!(sync().await, PushSyncStatus::Ok);
            assert!(r.remote.calls()[before..]
                .iter()
                .all(|c| !c.0.contains("register")));
            r.remote.fail.set(true);
            assert_eq!(sync().await, PushSyncStatus::Error, "never fails");
        })
        .await;
    }

    #[tokio::test]
    async fn unsubscribe_is_remembered_and_auto_register_does_not_undo_it() {
        run_local(async {
            let r = rig();
            r.push
                .register(fcm(), PushRegisterOptions::default())
                .await
                .unwrap();
            assert_eq!(r.push.unsubscribe(false).await.unwrap(), 1);
            assert_eq!(r.remote.last().1, vars_of(json!({"e": "fcm:fcm-tok"})));
            let auto = PushSyncOptions {
                auto_register: true,
                ..PushSyncOptions::default()
            };
            assert_eq!(
                r.push.sync(Some(fcm()), auto).await,
                PushSyncStatus::NotSubscribed
            );
            r.push.unsubscribe(true).await.unwrap();
            assert!(r.remote.last().0.contains("unsubscribe(NONE)"));
        })
        .await;
    }

    #[tokio::test]
    async fn sign_out_removes_this_device_but_keeps_the_choice() {
        run_local(async {
            let r = rig();
            r.push
                .register(fcm(), PushRegisterOptions::default())
                .await
                .unwrap();
            r.auth.run_hooks(false).await;
            let last = r.remote.last();
            assert!(last.0.contains("unsubscribe"));
            assert_eq!(last.1, vars_of(json!({"e": "fcm:fcm-tok"})));
            assert!(
                r.push.is_registered().await.unwrap(),
                "the user signs back in to push"
            );

            let before = r.remote.calls().len();
            r.auth.run_hooks(true).await;
            r.push.set_unsubscribe_on_sign_out(false);
            r.auth.run_hooks(false).await;
            assert_eq!(r.remote.calls().len(), before);

            r.push.set_unsubscribe_on_sign_out(true);
            r.push.dispose();
            r.auth.run_hooks(false).await;
            assert_eq!(
                r.remote.calls().len(),
                before,
                "a disposed module has no hook"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn sign_out_never_waits_more_than_one_and_a_half_seconds_for_the_server() {
        run_local(async {
            let r = rig();
            r.push
                .register(fcm(), PushRegisterOptions::default())
                .await
                .unwrap();
            r.remote.hang.set(true);
            let start = tokio::time::Instant::now();
            r.auth.run_hooks(false).await;
            let waited = start.elapsed();
            assert!(waited >= Duration::from_millis(1500), "{waited:?}");
            assert!(waited < Duration::from_millis(1502), "{waited:?}");
        })
        .await;
    }

    #[tokio::test]
    async fn devices_update_notify_cancel_and_test() {
        run_local(async {
            let r = rig();
            r.push
                .register(fcm(), PushRegisterOptions::default())
                .await
                .unwrap();
            r.remote.answer(
                "list",
                json!([
                    {
                        "id": "_00_push_subscription:1",
                        "endpoint": "fcm:fcm-tok",
                        "kind": "fcm",
                        "platform": "android",
                        "created_at": "2026-09-29T10:00:00Z",
                        "current": true,
                    },
                    {"id": "_00_push_subscription:2", "endpoint": "https://push.example/x", "current": false},
                ]),
            );
            let list = r.push.devices().await.unwrap();
            assert!(list[0].this_device);
            assert_eq!(list[0].platform, Some(PushPlatform::Android));
            assert_eq!(
                list[0].created_at,
                Some(Utc.with_ymd_and_hms(2026, 9, 29, 10, 0, 0).unwrap())
            );
            assert_eq!(list[1].kind, PushKind::Web, "rows without a kind are web");
            assert!(!list[1].current);

            r.remote.answer(
                "update",
                json!([{"id": "_00_push_subscription:1", "endpoint": "fcm:fcm-tok", "label": "Pixel"}]),
            );
            let updated = r
                .push
                .update(PushDeviceUpdate {
                    label: Some("Pixel".into()),
                    rules: Some(vec![]),
                    meta: None,
                })
                .await
                .unwrap();
            assert_eq!(updated.and_then(|d| d.label).as_deref(), Some("Pixel"));
            assert_eq!(
                r.remote.last().1,
                vars_of(json!({"e": "fcm:fcm-tok", "o": {"label": "Pixel", "rules": []}}))
            );

            r.remote.answer(
                "notify",
                json!({"id": "_00_push_message:m", "status": "pending"}),
            );
            let m = r
                .push
                .notify(&PushMessageInput {
                    notification: Some(object_from_json(json!({"title": "Later"}))),
                    send_at: Some(Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap()),
                    ttl: Some(Duration::from_secs(300)),
                    ..PushMessageInput::default()
                })
                .await
                .unwrap();
            assert_eq!(m.id, "_00_push_message:m");
            assert_eq!(
                r.remote.last().1.unwrap()["m"],
                Value::from(json!({
                    "notification": {"title": "Later"},
                    "ttl": 300,
                    "sendAt": "2030-01-01T00:00:00.000Z",
                }))
            );

            r.remote.answer("cancel", json!(true));
            assert!(r.push.cancel("_00_push_message:m").await.unwrap());
            assert_eq!(
                r.remote.last().1.unwrap()["id"],
                Value::RecordId(RecordId::new("_00_push_message", "m"))
            );

            r.remote.answer(
                "test",
                json!({"id": "_00_push_message:t", "status": "pending"}),
            );
            r.push.test(Some("Hi"), None).await.unwrap();
            assert_eq!(r.remote.last().1, vars_of(json!({"o": {"title": "Hi"}})));
        })
        .await;
    }

    #[tokio::test]
    async fn update_without_a_registration_is_refused() {
        run_local(async {
            let r = rig();
            let err = r
                .push
                .update(PushDeviceUpdate::default())
                .await
                .unwrap_err();
            assert_eq!(err.code, PushErrorCode::NotSubscribed);
            assert!(r.remote.calls().is_empty());
        })
        .await;
    }

    // ---- PushPayload ----------------------------------------------------------------------

    #[test]
    fn payload_reads_an_ios_map_and_an_fcm_json_string_and_refuses_anything_else() {
        let ios = PushPayload::try_parse(&Value::from(json!({
            "v": 1,
            "kind": "rule",
            "rule": "dm",
            "id": "message:1",
            "notification": {"url": "/m/1", "tag": "dm:1"},
            "ts": 5,
        })))
        .unwrap();
        assert_eq!(ios.rule.as_deref(), Some("dm"));
        assert_eq!(ios.url(), Some("/m/1"));
        assert!(!ios.is_nudge());
        assert_eq!(ios.ts, 5);
        let android = PushPayload::try_parse(&Value::from(
            r#"{"v":1,"kind":"message","message":"_00_push_message:m","data":{"k":1}}"#,
        ))
        .unwrap();
        assert_eq!(android.message.as_deref(), Some("_00_push_message:m"));
        assert!(android.is_nudge());
        assert_eq!(android.data, Value::from(json!({"k": 1})));
        assert!(PushPayload::try_parse(&Value::from(json!({"v": 2, "kind": "rule"}))).is_none());
        assert!(PushPayload::try_parse(&Value::from("not json")).is_none());
        assert!(PushPayload::try_parse(&Value::None).is_none());
    }

    #[test]
    fn a_record_round_trips_through_its_stored_string() {
        let r = PushRecord {
            user_id: "user:a".into(),
            enabled: true,
            endpoint: Some("apns:ab12".into()),
            at: Some(Utc.with_ymd_and_hms(2026, 11, 10, 12, 0, 0).unwrap()),
        };
        assert_eq!(PushRecord::from_value(&r.to_value()), Some(r));
        assert_eq!(
            PushRecord::from_value(&Value::from(json!({"enabled": true}))),
            None,
            "a record names its user"
        );
    }
}
