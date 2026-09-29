//! The `push:` block of `sp00ky.yml`.
//!
//! One set of types for both sides so they can never drift: the CLI parses the
//! YAML into [`PushConfig`], runs [`PushConfig::validate`] (`spky lint`,
//! `spky migrate`), and stores the JSON form in `_00_push_config:default`; the
//! engine reads that row back into the same struct.
//!
//! ```yaml
//! push:
//!   subject: mailto:ops@example.com
//!   rules:
//!     new-message:
//!       table: message
//!       on: [create]
//!       when: { kind: text }
//!       to: recipient
//!       except: sender
//!       topic: "dm:{{conversation | key}}"
//!       throttle: 30s
//!       with:
//!         sender: SELECT username FROM ONLY $row.sender
//!       notification:
//!         title: "{{sender.username}}"
//!         body: "{{text | truncate(120)}}"
//!         url: "/messages/{{conversation | key}}"
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_true() -> bool {
    true
}

fn is_true(b: &bool) -> bool {
    *b
}

/// The whole `push:` block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushConfig {
    /// Master switch. `false` stops every rule and every direct message;
    /// subscriptions are kept.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub enabled: bool,
    /// VAPID `sub` claim: a `mailto:` or `https:` contact the push services can
    /// reach about this sender. Apple rejects pushes without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Applied to every rule (and to direct messages) unless the rule says
    /// otherwise.
    #[serde(default, skip_serializing_if = "RuleDefaults::is_empty")]
    pub defaults: RuleDefaults,
    #[serde(default, skip_serializing_if = "Limits::is_empty")]
    pub limits: Limits,
    /// Keyed by rule name. The name travels in every push (`payload.rule`) and
    /// is what a device filters on (`_00_push_subscription.rules`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rules: BTreeMap<String, Rule>,
}

impl Default for PushConfig {
    fn default() -> Self {
        PushConfig {
            enabled: true,
            subject: None,
            defaults: RuleDefaults::default(),
            limits: Limits::default(),
            rules: BTreeMap::new(),
        }
    }
}

/// Subject used when the manifest names none.
pub const DEFAULT_SUBJECT: &str = "https://sp00ky.cloud";

impl PushConfig {
    pub fn subject(&self) -> &str {
        self.subject.as_deref().unwrap_or(DEFAULT_SUBJECT)
    }

    /// Tables at least one enabled rule watches.
    pub fn watched_tables(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .rules
            .values()
            .filter(|r| r.enabled && self.enabled)
            .map(|r| r.table.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Every problem with the block. Errors fail `spky lint` / `spky migrate`;
    /// warnings are printed.
    pub fn validate(&self) -> Vec<Issue> {
        let mut issues = Vec::new();
        if let Some(subject) = &self.subject {
            if !(subject.starts_with("mailto:") || subject.starts_with("https:")) {
                issues.push(Issue::error(
                    "push.subject",
                    format!("`{subject}` must be a mailto: or https: URI"),
                ));
            }
        } else if !self.rules.is_empty() {
            issues.push(Issue::warning(
                "push.subject",
                format!(
                    "no subject set; pushes are signed with `{DEFAULT_SUBJECT}`. Apple and Mozilla want a contact of yours (mailto: or https:)"
                ),
            ));
        }
        self.defaults.validate("push.defaults", &mut issues);
        self.limits.validate("push.limits", &mut issues);
        for (name, rule) in &self.rules {
            let at = format!("push.rules.{name}");
            if !valid_rule_name(name) {
                issues.push(Issue::error(
                    &at,
                    "rule names may only contain letters, digits, `-`, `_` and `.`",
                ));
            }
            rule.validate(&at, &mut issues);
        }
        issues
    }

    /// `true` when [`PushConfig::validate`] reports no error.
    pub fn is_valid(&self) -> bool {
        self.validate().iter().all(|i| i.severity != Severity::Error)
    }
}

pub fn valid_rule_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn valid_ident(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A field path into a row: `state`, `data.kind`, `members.0`.
pub fn valid_field_path(path: &str) -> bool {
    !path.is_empty() && path.split('.').all(|seg| !seg.is_empty() && !seg.contains(char::is_whitespace))
}

// ── Issues ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Issue {
    pub severity: Severity,
    /// Where in the manifest: `push.rules.new-message.when.state`.
    pub path: String,
    pub message: String,
}

impl Issue {
    pub fn error(path: impl Into<String>, message: impl Into<String>) -> Self {
        Issue { severity: Severity::Error, path: path.into(), message: message.into() }
    }
    pub fn warning(path: impl Into<String>, message: impl Into<String>) -> Self {
        Issue { severity: Severity::Warning, path: path.into(), message: message.into() }
    }
}

impl std::fmt::Display for Issue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

// ── Defaults and limits ──────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuleDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<DurationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgency: Option<Urgency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle: Option<DurationSpec>,
    /// Merged under every rule's `notification` (icon, badge, lang, ...). A
    /// rule's own keys win. Not applied to nudge-only rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationTemplate>,
}

impl RuleDefaults {
    pub fn is_empty(&self) -> bool {
        self == &RuleDefaults::default()
    }

    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        if let Some(n) = &self.notification {
            n.validate(&format!("{at}.notification"), issues);
        }
    }
}

/// Built-in ceilings. Every value has a default; the block only overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Limits {
    /// Pushes one user may receive per minute, across rules and devices
    /// counted once per matched recipient. Default 60.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_user_per_minute: Option<u32>,
    /// Pushes the whole project may send per minute (one per subscription).
    /// Default 6000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_minute: Option<u32>,
    /// Recipients one matched row may fan out to (`to.query`). Default 10000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_recipients: Option<u32>,
}

impl Limits {
    pub const DEFAULT_PER_USER_PER_MINUTE: u32 = 60;
    pub const DEFAULT_PER_MINUTE: u32 = 6000;
    pub const DEFAULT_MAX_RECIPIENTS: u32 = 10_000;

    pub fn is_empty(&self) -> bool {
        self == &Limits::default()
    }
    pub fn per_user_per_minute(&self) -> u32 {
        self.per_user_per_minute.unwrap_or(Self::DEFAULT_PER_USER_PER_MINUTE)
    }
    pub fn per_minute(&self) -> u32 {
        self.per_minute.unwrap_or(Self::DEFAULT_PER_MINUTE)
    }
    pub fn max_recipients(&self) -> u32 {
        self.max_recipients.unwrap_or(Self::DEFAULT_MAX_RECIPIENTS)
    }

    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        for (key, v) in [
            ("perUserPerMinute", self.per_user_per_minute),
            ("perMinute", self.per_minute),
            ("maxRecipients", self.max_recipients),
        ] {
            if v == Some(0) {
                issues.push(Issue::error(format!("{at}.{key}"), "must be at least 1"));
            }
        }
    }
}

// ── Rules ────────────────────────────────────────────────────────────────

/// One push rule: "when a row of `table` looks like `when`, push to `to`".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Rule {
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub enabled: bool,
    /// Table whose ingested rows this rule watches. Must be a synced table
    /// (a `-- @nosync` table never reaches the ingest path).
    pub table: String,
    /// Which operations. Default `[create]`. A `delete` sees the row as it
    /// was before the delete.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<Op>,
    /// Conditions on the row after the change, all of which must hold. Keys
    /// are field paths (`data.kind`); see [`Condition`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub when: BTreeMap<String, Condition>,
    /// Fire at most once per record for each distinct combination of these
    /// fields' values: `once: [state]` pushes when a row reaches `ready`, not
    /// on every later update while it stays `ready`. Unset = once per record
    /// version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub once: Option<Vec<String>>,
    /// Who receives it.
    pub to: Target,
    /// Users removed from the recipients (field paths, like `to`): the
    /// author of a group message, the actor of an event.
    #[serde(default, skip_serializing_if = "OneOrMany::is_empty")]
    pub except: OneOrMany<String>,
    /// Collapse key (template). Undelivered pushes with the same topic replace
    /// each other at the push service, the service worker uses it as the
    /// notification `tag` unless the notification names one, and throttling
    /// is per (rule, recipient, topic). Default: the record id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Minimum gap between two pushes of this rule to one recipient under one
    /// topic. Pushes inside the gap collapse into one trailing push carrying
    /// the latest row, sent when the gap ends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle: Option<DurationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgency: Option<Urgency>,
    /// How long the push service keeps an undelivered push. Default 1d.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<DurationSpec>,
    /// Ignore rows older than this: `{ field: created_at, within: 45s }`.
    /// The field may hold a datetime or epoch milliseconds / seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<MaxAge>,
    /// Extra template context, one root SurrealQL query per name with `$row`
    /// (the changed row) and `$rule` bound: `sender: SELECT username FROM ONLY
    /// $row.sender`. Evaluated only when the rule matched.
    #[serde(default, rename = "with", skip_serializing_if = "BTreeMap::is_empty")]
    pub with: BTreeMap<String, String>,
    /// What the device shows. Absent: a content-free nudge, and the app's
    /// service worker decides (typically by reading its live data).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationTemplate>,
    /// Structured payload (`payload.data`): a list of row field paths to copy,
    /// or a map of name -> template. Sent with nudges too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<DataSpec>,
}

impl Rule {
    /// Operations after defaulting.
    pub fn ops(&self) -> Vec<Op> {
        if self.on.is_empty() {
            vec![Op::Create]
        } else {
            self.on.clone()
        }
    }

    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        if !valid_ident(&self.table) {
            issues.push(Issue::error(format!("{at}.table"), format!("`{}` is not a table name", self.table)));
        } else if self.table.starts_with("_00_") {
            issues.push(Issue::error(
                format!("{at}.table"),
                "platform tables cannot be watched; use _00_push_message for direct pushes",
            ));
        }
        for (path, cond) in &self.when {
            let here = format!("{at}.when.{path}");
            if !valid_field_path(path) {
                issues.push(Issue::error(&here, "not a field path"));
            }
            cond.validate(&here, issues);
        }
        if let Some(once) = &self.once {
            if once.is_empty() {
                issues.push(Issue::error(format!("{at}.once"), "list at least one field"));
            }
            for p in once {
                if !valid_field_path(p) {
                    issues.push(Issue::error(format!("{at}.once"), format!("`{p}` is not a field path")));
                }
            }
        }
        self.to.validate(&format!("{at}.to"), issues);
        for p in self.except.iter() {
            if !valid_field_path(p) {
                issues.push(Issue::error(format!("{at}.except"), format!("`{p}` is not a field path")));
            }
        }
        if let Some(topic) = &self.topic {
            template_issue(&format!("{at}.topic"), topic, issues);
        }
        if let Some(max_age) = &self.max_age {
            if !valid_field_path(&max_age.field) {
                issues.push(Issue::error(format!("{at}.maxAge.field"), "not a field path"));
            }
        }
        for (name, surql) in &self.with {
            if !valid_ident(name) {
                issues.push(Issue::error(format!("{at}.with.{name}"), "names must be identifiers"));
            }
            if surql.trim().is_empty() {
                issues.push(Issue::error(format!("{at}.with.{name}"), "empty query"));
            }
        }
        if let Some(n) = &self.notification {
            n.validate(&format!("{at}.notification"), issues);
        }
        if let Some(data) = &self.data {
            data.validate(&format!("{at}.data"), issues);
        }
        if self.ops() == vec![Op::Delete] && self.once.is_some() {
            issues.push(Issue::warning(format!("{at}.once"), "a deleted record never changes again; `once` has no effect"));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Create,
    Update,
    Delete,
}

impl Op {
    /// Case-insensitive, from the ingest wire (`CREATE`, `UPDATE`, `DELETE`).
    pub fn parse(s: &str) -> Option<Op> {
        match s.to_ascii_lowercase().as_str() {
            "create" => Some(Op::Create),
            "update" | "merge" => Some(Op::Update),
            "delete" => Some(Op::Delete),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Create => "create",
            Op::Update => "update",
            Op::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Urgency {
    VeryLow,
    Low,
    Normal,
    High,
}

impl Urgency {
    /// The `Urgency` header value (RFC 8030 §5.3).
    pub fn as_header(&self) -> &'static str {
        match self {
            Urgency::VeryLow => "very-low",
            Urgency::Low => "low",
            Urgency::Normal => "normal",
            Urgency::High => "high",
        }
    }
    pub fn parse(s: &str) -> Option<Urgency> {
        match s {
            "very-low" => Some(Urgency::VeryLow),
            "low" => Some(Urgency::Low),
            "normal" => Some(Urgency::Normal),
            "high" => Some(Urgency::High),
            _ => None,
        }
    }
}

/// `string | [string]` in YAML.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> Default for OneOrMany<T> {
    fn default() -> Self {
        OneOrMany::Many(Vec::new())
    }
}

impl<T> OneOrMany<T> {
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        match self {
            OneOrMany::One(v) => std::slice::from_ref(v).iter(),
            OneOrMany::Many(v) => v.iter(),
        }
    }
    pub fn is_empty(&self) -> bool {
        matches!(self, OneOrMany::Many(v) if v.is_empty())
    }
}

/// Recipients of a rule.
///
/// - `to: recipient` / `to: [owner, assignee]`: field paths on the row. A
///   value may be a record id (`user:abc`), its string form, or an array of
///   either.
/// - `to: { query: "SELECT VALUE user FROM member WHERE club = $row.club" }`:
///   a root SurrealQL query with `$row` bound; it may return ids, strings, or
///   rows with an `id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Target {
    Fields(OneOrMany<String>),
    Query {
        query: String,
    },
}

impl Target {
    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        match self {
            Target::Fields(fields) => {
                if fields.is_empty() {
                    issues.push(Issue::error(at, "name at least one field"));
                }
                for p in fields.iter() {
                    if !valid_field_path(p) {
                        issues.push(Issue::error(at, format!("`{p}` is not a field path")));
                    }
                }
            }
            Target::Query { query } => {
                if query.trim().is_empty() {
                    issues.push(Issue::error(format!("{at}.query"), "empty query"));
                }
            }
        }
    }
}

/// A condition on one field.
///
/// - scalar: equality (`state: ready`, `count: 3`, `muted: false`, `x: null`)
/// - list: membership (`state: [ready, urgent]`)
/// - operator map: `{ ne: x }`, `{ in: [..] }`, `{ nin: [..] }`,
///   `{ gt|gte|lt|lte: n }` (numbers, or strings compared as strings),
///   `{ exists: true|false }` (present and not null),
///   `{ contains: x }` (array element or substring),
///   `{ startsWith: "x" }`. Several operators in one map must all hold.
// Not boxed: conditions are parsed once per config load and matched by
// reference, and a Box would change the type every consumer pattern-matches.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Condition {
    // `Any` before `Ops`: serde's derived struct visitor also accepts a
    // sequence (positionally), so `[ready, urgent]` would otherwise parse as
    // `{ eq: ready, ne: urgent }`.
    Any(Vec<Value>),
    Ops(ConditionOps),
    Eq(Value),
}

impl Condition {
    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        match self {
            Condition::Ops(ops) => {
                if ops == &ConditionOps::default() {
                    issues.push(Issue::error(at, "an operator map needs at least one operator"));
                }
            }
            Condition::Any(list) if list.is_empty() => {
                issues.push(Issue::warning(at, "an empty list never matches"));
            }
            Condition::Eq(Value::Object(_)) => {
                issues.push(Issue::error(at, "unknown operator map"));
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConditionOps {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eq: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ne: Option<Value>,
    #[serde(default, rename = "in", skip_serializing_if = "Option::is_none")]
    pub in_: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nin: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_with: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaxAge {
    pub field: String,
    pub within: DurationSpec,
}

/// `payload.data`: row fields to copy, or named templates. A template that is
/// exactly one `{{expr}}` keeps the value's JSON type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataSpec {
    Fields(Vec<String>),
    Map(BTreeMap<String, Value>),
}

impl DataSpec {
    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        match self {
            DataSpec::Fields(fields) => {
                for p in fields {
                    if !valid_field_path(p) {
                        issues.push(Issue::error(at, format!("`{p}` is not a field path")));
                    }
                }
            }
            DataSpec::Map(map) => {
                for (k, v) in map {
                    value_templates(&format!("{at}.{k}"), v, issues);
                }
            }
        }
    }
}

/// What the device shows. Every string is a template. Keys this struct does
/// not name (`vibrate`, `timestamp`, anything a browser adds later) pass
/// through to `showNotification` untouched, templated too.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationTemplate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub badge: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
    /// Defaults to the rule's topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Opened (or focused) on click by the service worker bridge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_interaction: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renotify: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<NotificationAction>,
    /// Merged into the notification's `data` next to the bridge's own keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, Value>,
}

impl NotificationTemplate {
    /// `self` over `base`: every key `self` sets wins, `extra` and `actions`
    /// included.
    pub fn over(&self, base: &NotificationTemplate) -> NotificationTemplate {
        let mut extra = base.extra.clone();
        extra.extend(self.extra.clone());
        NotificationTemplate {
            title: self.title.clone().or_else(|| base.title.clone()),
            body: self.body.clone().or_else(|| base.body.clone()),
            icon: self.icon.clone().or_else(|| base.icon.clone()),
            badge: self.badge.clone().or_else(|| base.badge.clone()),
            image: self.image.clone().or_else(|| base.image.clone()),
            lang: self.lang.clone().or_else(|| base.lang.clone()),
            dir: self.dir.clone().or_else(|| base.dir.clone()),
            tag: self.tag.clone().or_else(|| base.tag.clone()),
            url: self.url.clone().or_else(|| base.url.clone()),
            require_interaction: self.require_interaction.or(base.require_interaction),
            renotify: self.renotify.or(base.renotify),
            silent: self.silent.or(base.silent),
            actions: if self.actions.is_empty() { base.actions.clone() } else { self.actions.clone() },
            data: self.data.clone().or_else(|| base.data.clone()),
            extra,
        }
    }

    fn validate(&self, at: &str, issues: &mut Vec<Issue>) {
        for (key, v) in [
            ("title", &self.title),
            ("body", &self.body),
            ("icon", &self.icon),
            ("badge", &self.badge),
            ("image", &self.image),
            ("lang", &self.lang),
            ("dir", &self.dir),
            ("tag", &self.tag),
            ("url", &self.url),
        ] {
            if let Some(t) = v {
                template_issue(&format!("{at}.{key}"), t, issues);
            }
        }
        for (i, a) in self.actions.iter().enumerate() {
            if a.action.trim().is_empty() {
                issues.push(Issue::error(format!("{at}.actions.{i}.action"), "empty action id"));
            }
            template_issue(&format!("{at}.actions.{i}.title"), &a.title, issues);
        }
        if let Some(d) = &self.data {
            value_templates(&format!("{at}.data"), d, issues);
        }
        for (k, v) in &self.extra {
            value_templates(&format!("{at}.{k}"), v, issues);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationAction {
    pub action: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, Value>,
}

fn template_issue(at: &str, template: &str, issues: &mut Vec<Issue>) {
    if let Err(e) = crate::template::check(template) {
        issues.push(Issue::error(at, e));
    }
}

fn value_templates(at: &str, v: &Value, issues: &mut Vec<Issue>) {
    match v {
        Value::String(s) => template_issue(at, s, issues),
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                value_templates(&format!("{at}.{i}"), item, issues);
            }
        }
        Value::Object(map) => {
            for (k, item) in map {
                value_templates(&format!("{at}.{k}"), item, issues);
            }
        }
        _ => {}
    }
}

// ── Durations ────────────────────────────────────────────────────────────

/// `30s`, `10m`, `1h30m`, `2d`, `500ms`, or a plain number of seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurationSpec {
    millis: u64,
    text: String,
}

impl DurationSpec {
    pub fn parse(s: &str) -> Result<DurationSpec, String> {
        let text = s.trim();
        if text.is_empty() {
            return Err("empty duration".into());
        }
        if let Ok(secs) = text.parse::<u64>() {
            return Ok(DurationSpec { millis: secs.saturating_mul(1000), text: text.to_string() });
        }
        let mut millis: u64 = 0;
        let mut num = String::new();
        let mut chars = text.chars().peekable();
        let mut any = false;
        while let Some(c) = chars.next() {
            if c.is_ascii_digit() {
                num.push(c);
                continue;
            }
            let mut unit = String::from(c);
            while let Some(&n) = chars.peek() {
                if n.is_ascii_alphabetic() {
                    unit.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            if num.is_empty() {
                return Err(format!("`{text}` is not a duration (30s, 10m, 1h, 1d)"));
            }
            let n: u64 = num.parse().map_err(|_| format!("`{text}`: number too large"))?;
            let factor: u64 = match unit.as_str() {
                "ms" => 1,
                "s" => 1_000,
                "m" => 60_000,
                "h" => 3_600_000,
                "d" => 86_400_000,
                "w" => 604_800_000,
                _ => return Err(format!("`{text}`: unknown unit `{unit}` (ms, s, m, h, d, w)")),
            };
            millis = millis.saturating_add(n.saturating_mul(factor));
            num.clear();
            any = true;
        }
        if !num.is_empty() || !any {
            return Err(format!("`{text}` is not a duration (30s, 10m, 1h, 1d)"));
        }
        Ok(DurationSpec { millis, text: text.to_string() })
    }

    pub fn from_secs(secs: u64) -> DurationSpec {
        DurationSpec { millis: secs.saturating_mul(1000), text: format!("{secs}s") }
    }

    pub fn as_millis(&self) -> u64 {
        self.millis
    }
    pub fn as_secs(&self) -> u64 {
        self.millis / 1000
    }
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

impl Serialize for DurationSpec {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.text)
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Num(u64),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Num(n) => Ok(DurationSpec::from_secs(n)),
            Raw::Text(t) => DurationSpec::parse(&t).map_err(serde::de::Error::custom),
        }
    }
}

// ── Wire payload ─────────────────────────────────────────────────────────

/// Payload format version. Bump on a breaking change; the service worker
/// bridge ignores versions it does not know.
pub const PAYLOAD_VERSION: u32 = 1;

/// What one push carries (JSON, then RFC 8291 encrypted). The contract with
/// `@spooky-sync/core/sw`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushPayload {
    pub v: u32,
    pub kind: PayloadKind,
    /// Rule name (`kind: rule`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    /// `_00_push_message` id (`kind: message`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The row that fired the rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<Op>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Rendered notification. Absent: a nudge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Epoch ms when the engine built it.
    pub ts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadKind {
    Rule,
    Message,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> PushConfig {
        serde_yaml::from_str(yaml).expect("parse")
    }

    #[test]
    fn durations() {
        assert_eq!(DurationSpec::parse("30s").unwrap().as_millis(), 30_000);
        assert_eq!(DurationSpec::parse("1h30m").unwrap().as_secs(), 5400);
        assert_eq!(DurationSpec::parse("500ms").unwrap().as_millis(), 500);
        assert_eq!(DurationSpec::parse("45").unwrap().as_secs(), 45);
        assert!(DurationSpec::parse("10").is_ok());
        assert!(DurationSpec::parse("10x").is_err());
        assert!(DurationSpec::parse("m").is_err());
        assert!(DurationSpec::parse("").is_err());
    }

    #[test]
    fn full_rule_round_trips_through_json() {
        let cfg = parse(
            r#"
subject: mailto:ops@example.com
defaults:
  ttl: 1d
  notification: { icon: /icon.png }
limits: { perUserPerMinute: 30 }
rules:
  new-message:
    table: message
    on: [create]
    when:
      kind: text
      state: [ready, urgent]
      count: { gte: 2, lt: 10 }
      muted: { exists: false }
    once: [state]
    to: recipient
    except: sender
    topic: "dm:{{conversation | key}}"
    throttle: 30s
    urgency: high
    ttl: 45s
    maxAge: { field: created_at, within: 1m }
    with:
      sender: SELECT username FROM ONLY $row.sender
    notification:
      title: "{{sender.username}}"
      body: "{{text | truncate(120)}}"
      url: "/m/{{conversation | key}}"
      vibrate: [100, 50, 100]
      actions: [{ action: open, title: Open }]
    data: [conversation, kind]
  club-post:
    table: post
    to: { query: "SELECT VALUE user FROM member WHERE club = $row.club" }
    data: { club: "{{club}}", title: "Post: {{title}}" }
"#,
        );
        assert!(cfg.is_valid(), "{:?}", cfg.validate());
        let json = serde_json::to_string(&cfg).unwrap();
        let back: PushConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
        let rule = &cfg.rules["new-message"];
        assert!(matches!(rule.when["kind"], Condition::Eq(_)));
        assert!(matches!(rule.when["state"], Condition::Any(_)));
        assert!(matches!(rule.when["count"], Condition::Ops(_)));
        assert!(matches!(rule.to, Target::Fields(OneOrMany::One(_))));
        assert_eq!(rule.notification.as_ref().unwrap().extra.len(), 1);
        assert!(matches!(cfg.rules["club-post"].to, Target::Query { .. }));
        assert_eq!(cfg.rules["club-post"].ops(), vec![Op::Create]);
        assert_eq!(cfg.watched_tables(), vec!["message".to_string(), "post".to_string()]);
    }

    #[test]
    fn validation_reports_the_obvious_mistakes() {
        let cfg = parse(
            r#"
subject: ops@example.com
rules:
  "bad name":
    table: _00_query
    to: []
    when: { state: { } }
"#,
        );
        let issues = cfg.validate();
        let errors: Vec<_> = issues.iter().filter(|i| i.severity == Severity::Error).collect();
        assert!(errors.iter().any(|i| i.path == "push.subject"));
        assert!(errors.iter().any(|i| i.path.ends_with(".table")));
        assert!(errors.iter().any(|i| i.path.ends_with(".to")));
        assert!(errors.iter().any(|i| i.path.ends_with(".when.state")));
        assert!(errors.iter().any(|i| i.message.contains("rule names")));
    }

    #[test]
    fn unknown_keys_are_rejected_on_rules_but_pass_through_on_notifications() {
        assert!(serde_yaml::from_str::<PushConfig>("rules: { r: { table: t, to: u, bogus: 1 } }").is_err());
        let cfg = parse("rules: { r: { table: t, to: u, notification: { title: x, timestamp: 5 } } }");
        assert_eq!(cfg.rules["r"].notification.as_ref().unwrap().extra["timestamp"], serde_json::json!(5));
    }

    #[test]
    fn defaults_merge_under_rule_notification() {
        let base = NotificationTemplate { icon: Some("/i.png".into()), title: Some("x".into()), ..Default::default() };
        let rule = NotificationTemplate { title: Some("y".into()), ..Default::default() };
        let merged = rule.over(&base);
        assert_eq!(merged.title.as_deref(), Some("y"));
        assert_eq!(merged.icon.as_deref(), Some("/i.png"));
    }
}
