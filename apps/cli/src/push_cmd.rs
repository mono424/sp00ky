//! `spky push <status|send|devices|sync>`: the operator side of Web Push.
//!
//! Everything goes straight to SurrealDB as root, like `spky schedules` and
//! `spky flag`: the stored rules (`_00_push_config:default`), the key the host
//! published (`$sp00ky_vapid_public_key` / `$sp00ky_vapid_kid`), the
//! subscriptions (`_00_push_subscription`, never their keys) and direct
//! messages (`_00_push_message`). A send is a plain CREATE of a message row;
//! the push host (scheduler, or the standalone SSP in singlenode) picks it up
//! from the ingest path and writes the delivery result back onto the row,
//! which is what `send` waits for.
//!
//! Bare `spky push` (no subcommand) is the older schema push for free
//! (Cloudflare) projects and is dispatched in `main.rs`.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use push_core::{DurationSpec, PushConfig, Target, Urgency};
use serde_json::{json, Map, Value};

use crate::backend::DEFAULT_CONFIG_PATH;
use crate::push_sync::surql_string;
use crate::surreal_client::{MigrationDB, SurrealClient};
use crate::{ConnectionArgs, PushCommands};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

/// How long `send` waits for the host's verdict before it stops watching.
/// Normally the host answers within a second of the ingest. The bound covers
/// the engine's sweep for a message whose ingest it missed (a pending row
/// older than 30 s, swept every 5 s), so a slow path still reports its result.
const SEND_WAIT: Duration = Duration::from_secs(40);

pub fn run(action: PushCommands) -> Result<()> {
    match action {
        PushCommands::Status { json, conn, config } => {
            let client = client_from(&conn, &config)?;
            status(&client, json)
        }
        PushCommands::Send {
            to,
            title,
            body,
            link,
            icon,
            tag,
            topic,
            at,
            ttl,
            urgency,
            data,
            no_wait,
            json,
            conn,
            config,
        } => {
            let msg = Message::build(MessageArgs {
                to,
                title,
                body,
                link,
                icon,
                tag,
                topic,
                at,
                ttl,
                urgency,
                data,
            })?;
            if conn.cloud
                && !crate::ui::consent(&format!(
                    "Send this push to {} on the cloud deployment?",
                    plural(msg.to.len(), "user", "users")
                ))?
            {
                println!("{YELLOW}Aborted; nothing sent.{RESET}");
                return Ok(());
            }
            let client = client_from(&conn, &config)?;
            send(&client, &msg, !no_wait, json)
        }
        PushCommands::Devices {
            user,
            json,
            conn,
            config,
        } => {
            let client = client_from(&conn, &config)?;
            devices(&client, &user, json)
        }
        PushCommands::Sync { conn, config } => sync(&conn, &config),
    }
}

fn client_from(conn: &ConnectionArgs, config: &Option<PathBuf>) -> Result<SurrealClient> {
    let c = conn.resolve(config)?;
    Ok(SurrealClient::new(
        &c.url,
        &c.namespace,
        &c.database,
        &c.username,
        &c.password,
    ))
}

/// One statement's result as a list of rows.
fn rows_of(result: Option<&Value>) -> Vec<Value> {
    match result {
        Some(Value::Array(rows)) => rows.clone(),
        Some(Value::Null) | None => vec![],
        Some(other) => vec![other.clone()],
    }
}

/// Every statement's result, in order.
fn results(client: &SurrealClient, sql: &str) -> Result<Vec<Value>> {
    Ok(client
        .execute(sql)
        .context("query failed")?
        .into_iter()
        .map(|r| r.result.unwrap_or(Value::Null))
        .collect())
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn int_of(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

// =============================================================
// `spky push status`
// =============================================================

/// Statements, in the order `parse_status` reads them.
pub(crate) const STATUS_SQL: &str = "\
SELECT spec_json, hash, type::string(updated_at) AS updated_at FROM _00_push_config:default;
RETURN { public_key: $sp00ky_vapid_public_key, kid: $sp00ky_vapid_kid };
RETURN {
    total: array::len(SELECT VALUE id FROM _00_push_subscription),
    disabled: array::len(SELECT VALUE id FROM _00_push_subscription WHERE disabled_at != NONE),
    stale: array::len(SELECT VALUE id FROM _00_push_subscription WHERE disabled_at = NONE AND kid != ($sp00ky_vapid_kid ?? '')),
    users: array::len(array::distinct(SELECT VALUE auth_id FROM _00_push_subscription WHERE disabled_at = NONE))
};
SELECT status, count() AS n FROM _00_push_message GROUP BY status;
RETURN array::len(SELECT VALUE id FROM _00_push_message WHERE status = 'pending' AND send_at != NONE AND send_at > time::now());
SELECT <string> id AS id, to, error, type::string(created_at) AS created_at FROM _00_push_message WHERE status = 'failed' ORDER BY created_at DESC LIMIT 5;
";

#[derive(Debug, Default, PartialEq)]
pub(crate) struct Status {
    /// `None`: no `_00_push_config` row yet (never migrated with this CLI).
    pub config: Option<PushConfig>,
    pub config_error: Option<String>,
    pub config_updated_at: Option<String>,
    pub public_key: Option<String>,
    pub kid: Option<String>,
    pub subscriptions: i64,
    pub disabled: i64,
    pub stale: i64,
    pub users: i64,
    /// Message count per status.
    pub messages: std::collections::BTreeMap<String, i64>,
    /// Pending with a future `send_at`.
    pub scheduled: i64,
    pub recent_failures: Vec<Value>,
}

pub(crate) fn parse_status(results: &[Value]) -> Status {
    let at = |i: usize| results.get(i).cloned().unwrap_or(Value::Null);
    let mut s = Status::default();

    if let Some(row) = rows_of(Some(&at(0))).into_iter().next() {
        s.config_updated_at = str_of(&row, "updated_at").map(str::to_owned);
        match str_of(&row, "spec_json").map(serde_json::from_str::<PushConfig>) {
            Some(Ok(cfg)) => s.config = Some(cfg),
            Some(Err(e)) => s.config_error = Some(e.to_string()),
            None => s.config_error = Some("row has no spec_json".into()),
        }
    }

    let keys = at(1);
    s.public_key = str_of(&keys, "public_key").map(str::to_owned);
    s.kid = str_of(&keys, "kid").map(str::to_owned);

    let subs = at(2);
    s.subscriptions = int_of(&subs, "total");
    s.disabled = int_of(&subs, "disabled");
    s.stale = int_of(&subs, "stale");
    s.users = int_of(&subs, "users");

    for row in rows_of(Some(&at(3))) {
        if let Some(status) = str_of(&row, "status") {
            s.messages.insert(status.to_string(), int_of(&row, "n"));
        }
    }
    s.scheduled = at(4).as_i64().unwrap_or(0);
    s.recent_failures = rows_of(Some(&at(5)));
    s
}

fn status(client: &SurrealClient, json: bool) -> Result<()> {
    let s = parse_status(&results(client, STATUS_SQL)?);
    if json {
        let out = json!({
            "config": s.config,
            "configError": s.config_error,
            "configUpdatedAt": s.config_updated_at,
            "publicKey": s.public_key,
            "kid": s.kid,
            "subscriptions": {
                "total": s.subscriptions,
                "disabled": s.disabled,
                "staleKey": s.stale,
                "users": s.users,
            },
            "messages": s.messages,
            "scheduled": s.scheduled,
            "recentFailures": s.recent_failures,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    println!("{BOLD}Web Push{RESET}");

    // Host / key.
    match (&s.public_key, &s.kid) {
        (Some(key), kid) => {
            println!(
                "  host        : {GREEN}active{RESET} {DIM}(a host published its VAPID key){RESET}"
            );
            println!("  kid         : {}", kid.as_deref().unwrap_or("-"));
            println!("  public key  : {DIM}{key}{RESET}");
        }
        (None, _) => {
            println!("  host        : {YELLOW}not active{RESET}");
            println!(
                "  {DIM}No host has published a VAPID key. The scheduler (cluster) or the SSP\n  \
                 (singlenode) publishes one at boot when SPKY_AUTH_SECRET or\n  \
                 SPKY_VAPID_PRIVATE_KEY is set and SPKY_PUSH is not `off`.{RESET}"
            );
        }
    }

    // Rules.
    match (&s.config, &s.config_error) {
        (Some(cfg), _) => print_rules(cfg, s.config_updated_at.as_deref()),
        (None, Some(e)) => println!("  rules       : {RED}stored config does not parse: {e}{RESET}"),
        (None, None) => println!(
            "  rules       : {DIM}none stored yet (`spky migrate`, `spky deploy` or `spky push sync` writes them){RESET}"
        ),
    }

    // Devices.
    let enabled = s.subscriptions - s.disabled;
    let mut line = format!(
        "{} across {}",
        plural(enabled.max(0) as usize, "device", "devices"),
        plural(s.users.max(0) as usize, "user", "users")
    );
    if s.disabled > 0 {
        line.push_str(&format!(", {YELLOW}{} disabled{RESET}", s.disabled));
    }
    if s.stale > 0 && s.public_key.is_some() {
        line.push_str(&format!(
            ", {YELLOW}{} on an older key{RESET} {DIM}(re-subscribe on their next boot){RESET}",
            s.stale
        ));
    }
    println!("  devices     : {line}");

    // Messages.
    let count = |k: &str| s.messages.get(k).copied().unwrap_or(0);
    let due = (count("pending") - s.scheduled).max(0);
    let mut parts = vec![];
    if due > 0 {
        parts.push(format!("{YELLOW}{due} pending{RESET}"));
    }
    if s.scheduled > 0 {
        parts.push(format!("{} scheduled", s.scheduled));
    }
    if count("sending") > 0 {
        parts.push(format!("{} sending", count("sending")));
    }
    parts.push(format!("{GREEN}{} sent{RESET}", count("sent")));
    if count("failed") > 0 {
        parts.push(format!("{RED}{} failed{RESET}", count("failed")));
    }
    if count("cancelled") > 0 {
        parts.push(format!("{DIM}{} cancelled{RESET}", count("cancelled")));
    }
    println!(
        "  messages    : {} {DIM}(kept 7 days){RESET}",
        parts.join(", ")
    );
    for f in &s.recent_failures {
        println!(
            "    {RED}failed{RESET} {} {DIM}{}{RESET}: {}",
            str_of(f, "id").unwrap_or("-"),
            str_of(f, "created_at")
                .map(|t| &t[..t.len().min(19)])
                .unwrap_or(""),
            str_of(f, "error").unwrap_or("(no error recorded)")
        );
    }
    Ok(())
}

fn print_rules(cfg: &PushConfig, updated_at: Option<&str>) {
    let when = updated_at
        .map(|t| format!(" {DIM}(stored {}){RESET}", &t[..t.len().min(19)]))
        .unwrap_or_default();
    if !cfg.enabled {
        println!("  rules       : {YELLOW}push disabled{RESET} (`push.enabled: false`){when}");
        return;
    }
    println!("  subject     : {}", cfg.subject());
    if cfg.rules.is_empty() {
        println!("  rules       : {DIM}none, direct messages only{RESET}{when}");
        return;
    }
    println!("  rules       : {}{when}", cfg.rules.len());
    for (name, rule) in &cfg.rules {
        let ops: Vec<&str> = rule.ops().iter().map(|o| o.as_str()).collect();
        let to = match &rule.to {
            Target::Fields(f) => format!("to {}", f.iter().cloned().collect::<Vec<_>>().join(", ")),
            Target::Query { .. } => "to query".to_string(),
        };
        let kind = if rule.notification.is_some() {
            "content"
        } else {
            "nudge"
        };
        let mut extras = vec![kind.to_string()];
        if let Some(t) = rule.throttle.as_ref().or(cfg.defaults.throttle.as_ref()) {
            extras.push(format!("throttle {}", t.as_str()));
        }
        if rule.once.is_some() {
            extras.push("once".into());
        }
        let state = if rule.enabled {
            String::new()
        } else {
            format!(" {YELLOW}disabled{RESET}")
        };
        println!(
            "    {BOLD}{name}{RESET}{state}  {} on {}  {to}  {DIM}{}{RESET}",
            rule.table,
            ops.join("/"),
            extras.join(", ")
        );
    }
}

// =============================================================
// `spky push send`
// =============================================================

pub(crate) struct MessageArgs {
    pub to: Vec<String>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub link: Option<String>,
    pub icon: Option<String>,
    pub tag: Option<String>,
    pub topic: Option<String>,
    pub at: Option<String>,
    pub ttl: Option<String>,
    pub urgency: Option<String>,
    pub data: Option<String>,
}

/// A validated `_00_push_message`, ready to become SQL.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Message {
    pub to: Vec<String>,
    pub notification: Option<Value>,
    pub data: Option<Value>,
    pub topic: Option<String>,
    pub urgency: Option<Urgency>,
    pub ttl_secs: Option<u64>,
    /// RFC 3339, UTC.
    pub send_at: Option<String>,
}

impl Message {
    pub(crate) fn build(a: MessageArgs) -> Result<Message> {
        let mut to: Vec<String> = Vec::new();
        for raw in &a.to {
            let id = raw.trim();
            if id.is_empty() {
                continue;
            }
            let Some((table, key)) = id.split_once(':') else {
                bail!("`{id}` is not a record id: pass the user's id as `$auth.id` reads, e.g. `user:abc`");
            };
            if table.is_empty() || key.is_empty() {
                bail!("`{id}` is not a record id: expected `table:key`, e.g. `user:abc`");
            }
            if !to.iter().any(|t| t == id) {
                to.push(id.to_string());
            }
        }
        if to.is_empty() {
            bail!("name at least one recipient with --to user:abc");
        }

        let mut n = Map::new();
        for (key, value) in [
            ("title", &a.title),
            ("body", &a.body),
            ("url", &a.link),
            ("icon", &a.icon),
            ("tag", &a.tag),
        ] {
            if let Some(v) = value {
                n.insert(key.to_string(), Value::String(v.clone()));
            }
        }
        let notification = if n.is_empty() {
            None
        } else if !n.contains_key("title") {
            bail!("a visible notification needs --title (leave out every notification flag to send a content-free nudge)");
        } else {
            Some(Value::Object(n))
        };

        let data = match &a.data {
            None => None,
            Some(text) => {
                let v: Value = serde_json::from_str(text).context("--data is not valid JSON")?;
                if !v.is_object() {
                    bail!("--data must be a JSON object, e.g. '{{\"conversation\":\"c1\"}}'");
                }
                Some(v)
            }
        };

        let urgency = match &a.urgency {
            None => None,
            Some(u) => Some(Urgency::parse(u.trim()).ok_or_else(|| {
                anyhow!("--urgency must be one of very-low, low, normal, high (got `{u}`)")
            })?),
        };

        let ttl_secs = match &a.ttl {
            None => None,
            Some(t) => Some(
                DurationSpec::parse(t)
                    .map_err(|e| anyhow!("--ttl: {e}"))?
                    .as_secs(),
            ),
        };

        let send_at = match &a.at {
            None => None,
            Some(text) => Some(parse_when(text, chrono::Utc::now())?),
        };

        let topic = a
            .topic
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());

        Ok(Message {
            to,
            notification,
            data,
            topic,
            urgency,
            ttl_secs,
            send_at,
        })
    }

    /// `CREATE` as root, then the new id, status and send time as strings.
    pub(crate) fn create_sql(&self) -> String {
        let mut sets = vec![format!("to = {}", json_literal(&json!(self.to)))];
        if let Some(n) = &self.notification {
            sets.push(format!("notification = {}", json_literal(n)));
        }
        if let Some(d) = &self.data {
            sets.push(format!("data = {}", json_literal(d)));
        }
        if let Some(t) = &self.topic {
            sets.push(format!("topic = {}", surql_string(t)));
        }
        if let Some(u) = &self.urgency {
            sets.push(format!("urgency = {}", surql_string(u.as_header())));
        }
        if let Some(ttl) = self.ttl_secs {
            sets.push(format!("ttl = {ttl}"));
        }
        if let Some(at) = &self.send_at {
            sets.push(format!("send_at = <datetime> {}", surql_string(at)));
        }
        sets.push("status = 'pending'".to_string());
        format!(
            "LET $m = CREATE ONLY _00_push_message SET {};\n\
             RETURN {{ id: <string> $m.id, status: $m.status, send_at: IF $m.send_at = NONE {{ NONE }} ELSE {{ <string> $m.send_at }} }};",
            sets.join(", ")
        )
    }
}

/// A JSON value as a SurrealQL literal. serde's string escapes (`\"`, `\\`,
/// `\n`, `\u00XX`) are all SurrealQL escapes too, and it never writes `\/`.
fn json_literal(v: &Value) -> String {
    serde_json::to_string(v).expect("a serde_json::Value always serializes")
}

/// `--at`: an RFC 3339 time, or a delay from now (`10m`, `+2h`, `in 1d`).
pub(crate) fn parse_when(text: &str, now: chrono::DateTime<chrono::Utc>) -> Result<String> {
    let t = text.trim();
    if let Ok(at) = chrono::DateTime::parse_from_rfc3339(t) {
        return Ok(at
            .with_timezone(&chrono::Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    }
    let delay = t
        .strip_prefix("in ")
        .or_else(|| t.strip_prefix('+'))
        .unwrap_or(t)
        .trim();
    match DurationSpec::parse(delay) {
        Ok(d) => {
            let at = now + chrono::Duration::milliseconds(d.as_millis() as i64);
            Ok(at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        }
        Err(_) => bail!(
            "--at `{text}` is neither an RFC 3339 time (2026-10-01T09:00:00Z) nor a delay (10m, 2h, 1d)"
        ),
    }
}

/// Where a send stands, read back from the row.
pub(crate) fn message_state_sql(id: &str) -> Result<String> {
    let key = id
        .strip_prefix("_00_push_message:")
        .ok_or_else(|| anyhow!("unexpected message id `{id}`"))?
        .trim_start_matches('⟨')
        .trim_end_matches('⟩');
    Ok(format!(
        "SELECT status, delivered, error FROM type::record('_00_push_message', {});",
        surql_string(key)
    ))
}

fn send(client: &SurrealClient, msg: &Message, wait: bool, json: bool) -> Result<()> {
    let key_set = results(client, "RETURN $sp00ky_vapid_public_key != NONE;")?
        .first()
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !key_set && !json {
        crate::ui::warn(
            "no push host has published a VAPID key yet: the message is queued, but nothing delivers it until one does (see `spky push status`)",
        );
    }

    let created = results(client, &msg.create_sql())?
        .pop()
        .ok_or_else(|| anyhow!("the CREATE returned nothing"))?;
    let id = str_of(&created, "id")
        .ok_or_else(|| anyhow!("the CREATE returned no id: {created}"))?
        .to_string();
    let scheduled = msg.send_at.as_deref().and_then(|at| {
        chrono::DateTime::parse_from_rfc3339(at)
            .ok()
            .filter(|t| t.with_timezone(&chrono::Utc) > chrono::Utc::now())
            .map(|_| at.to_string())
    });

    if !json {
        let kind = if msg.notification.is_some() {
            "push"
        } else {
            "nudge"
        };
        match &scheduled {
            Some(at) => println!(
                "{GREEN}Scheduled{RESET} {kind} {id} for {} at {at} {}",
                plural(msg.to.len(), "user", "users"),
                crate::schedules::relative(at)
            ),
            None => println!(
                "{GREEN}Queued{RESET} {kind} {id} for {}",
                plural(msg.to.len(), "user", "users")
            ),
        }
    }

    // A scheduled message has nothing to report yet; neither has an unwatched one.
    let mut outcome = json!({ "id": id, "status": "pending", "sendAt": scheduled });
    if wait && scheduled.is_none() && key_set {
        let sql = message_state_sql(&id)?;
        let started = Instant::now();
        let step = (!json).then(|| crate::ui::step("Delivery"));
        loop {
            let row = rows_of(results(client, &sql)?.first())
                .into_iter()
                .next()
                .unwrap_or(Value::Null);
            let status = str_of(&row, "status").unwrap_or("pending").to_string();
            outcome["status"] = json!(status);
            outcome["delivered"] = row.get("delivered").cloned().unwrap_or(Value::Null);
            outcome["error"] = row.get("error").cloned().unwrap_or(Value::Null);
            match status.as_str() {
                "sent" => {
                    let n = int_of(&row, "delivered").max(0) as usize;
                    if let Some(step) = step {
                        if n == 0 {
                            step.warn("sent, but no device accepted it (no enabled subscription? `spky push devices <user>`)");
                        } else {
                            step.done(format!("accepted for {}", plural(n, "device", "devices")));
                        }
                    }
                    break;
                }
                "failed" | "cancelled" => {
                    if let Some(step) = step {
                        step.fail(format!(
                            "{status}: {}",
                            str_of(&row, "error").unwrap_or("(no error recorded)")
                        ));
                    }
                    break;
                }
                _ if started.elapsed() >= SEND_WAIT => {
                    if let Some(step) = step {
                        step.warn(format!(
                            "still {status} after {}s (check the scheduler / SSP logs and `spky push status`)",
                            SEND_WAIT.as_secs()
                        ));
                    }
                    break;
                }
                _ => {
                    if let Some(step) = &step {
                        step.set_message(format!("{status}, waiting for the push host"));
                    }
                    std::thread::sleep(Duration::from_millis(400));
                }
            }
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    }
    Ok(())
}

// =============================================================
// `spky push devices <user>`
// =============================================================

/// A user's subscriptions, newest first, without `p256dh` / `auth`.
/// `type::string(NONE)` is the string "NONE", hence the guards.
pub(crate) fn devices_sql(user: &str) -> String {
    format!(
        "SELECT <string> id AS id, endpoint, kid, label, user_agent, rules, meta, failures, last_error, \
         disabled_reason, type::string(created_at) AS created_at, type::string(updated_at) AS updated_at, \
         IF last_ok_at = NONE {{ NONE }} ELSE {{ type::string(last_ok_at) }} AS last_ok_at, \
         IF disabled_at = NONE {{ NONE }} ELSE {{ type::string(disabled_at) }} AS disabled_at, \
         (kid = ($sp00ky_vapid_kid ?? '')) AS current \
         FROM _00_push_subscription WHERE auth_id = {} ORDER BY updated_at DESC;",
        surql_string(user)
    )
}

/// `https://fcm.googleapis.com/fcm/send/abc...` -> `fcm.googleapis.com`.
fn push_service(endpoint: &str) -> &str {
    let rest = endpoint
        .split_once("://")
        .map(|(_, r)| r)
        .unwrap_or(endpoint);
    rest.split(['/', '?']).next().unwrap_or(rest)
}

fn devices(client: &SurrealClient, user: &str, json: bool) -> Result<()> {
    let user = user.trim();
    if !user.contains(':') {
        bail!(
            "`{user}` is not a record id: pass the user's id as `$auth.id` reads, e.g. `user:abc`"
        );
    }
    let rows = rows_of(results(client, &devices_sql(user))?.first());
    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("{DIM}{user} has no push subscriptions.{RESET}");
        return Ok(());
    }
    println!(
        "{BOLD}{user}{RESET}: {}",
        plural(rows.len(), "device", "devices")
    );
    for row in &rows {
        let (state, color) = if str_of(row, "disabled_at").is_some() {
            ("disabled", RED)
        } else if row.get("current").and_then(Value::as_bool) == Some(false) {
            ("old key", YELLOW)
        } else {
            ("active", GREEN)
        };
        let label = str_of(row, "label")
            .or_else(|| str_of(row, "user_agent"))
            .unwrap_or("(unlabelled)");
        println!("  {color}{state:<9}{RESET} {BOLD}{label}{RESET}");
        println!(
            "            {DIM}{} · updated {} · last ok {}{RESET}",
            push_service(str_of(row, "endpoint").unwrap_or("")),
            str_of(row, "updated_at")
                .map(|t| &t[..t.len().min(19)])
                .unwrap_or("-"),
            str_of(row, "last_ok_at")
                .map(|t| &t[..t.len().min(19)])
                .unwrap_or("never"),
        );
        if let Some(rules) = row.get("rules").and_then(Value::as_array) {
            let names: Vec<&str> = rules.iter().filter_map(Value::as_str).collect();
            println!("            {DIM}rules: {}{RESET}", names.join(", "));
        }
        if let Some(reason) = str_of(row, "disabled_reason") {
            println!("            {RED}{reason}{RESET}");
        } else if let Some(err) = str_of(row, "last_error") {
            println!(
                "            {YELLOW}{} failure(s), last: {err}{RESET}",
                int_of(row, "failures")
            );
        }
    }
    Ok(())
}

// =============================================================
// `spky push sync`
// =============================================================

fn sync(conn: &ConnectionArgs, config: &Option<PathBuf>) -> Result<()> {
    let config_path = config
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    if !config_path.exists() {
        bail!("Config file not found: {}", config_path.display());
    }
    let cfg = crate::push_sync::load(&config_path)?;
    let client = client_from(conn, config)?;
    match crate::push_sync::sync(&client, &cfg)? {
        crate::push_sync::Outcome::Unchanged => println!(
            "{DIM}Push config already up to date ({}).{RESET}",
            crate::push_sync::summary(&cfg)
        ),
        crate::push_sync::Outcome::Written => println!(
            "{GREEN}Synced push config{RESET}: {}",
            crate::push_sync::summary(&cfg)
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(to: &[&str]) -> MessageArgs {
        MessageArgs {
            to: to.iter().map(|s| s.to_string()).collect(),
            title: None,
            body: None,
            link: None,
            icon: None,
            tag: None,
            topic: None,
            at: None,
            ttl: None,
            urgency: None,
            data: None,
        }
    }

    #[test]
    fn a_bare_send_is_a_nudge() {
        let m = Message::build(args(&["user:a", "user:b", "user:a"])).unwrap();
        assert_eq!(m.to, vec!["user:a", "user:b"], "deduplicated, order kept");
        assert!(m.notification.is_none());
        let sql = m.create_sql();
        assert!(sql.starts_with("LET $m = CREATE ONLY _00_push_message SET to = [\"user:a\",\"user:b\"], status = 'pending';"), "{sql}");
        assert!(!sql.contains("notification"), "{sql}");
    }

    #[test]
    fn every_flag_lands_on_its_field() {
        let mut a = args(&["user:a"]);
        a.title = Some("It's \"ready\" \\o/".into());
        a.body = Some("line\nnext é".into());
        a.link = Some("/m/1".into());
        a.topic = Some("dm:1".into());
        a.urgency = Some("high".into());
        a.ttl = Some("1h".into());
        a.data = Some(r#"{"k":[1,2]}"#.into());
        a.at = Some("2030-01-02T03:04:05Z".into());
        let m = Message::build(a).unwrap();
        let sql = m.create_sql();
        assert!(sql.contains("notification = {"), "{sql}");
        for part in [
            r#""title":"It's \"ready\" \\o/""#,
            r#""body":"line\nnext é""#,
            r#""url":"/m/1""#,
        ] {
            assert!(sql.contains(part), "{part} in {sql}");
        }
        assert!(sql.contains(r#"data = {"k":[1,2]}"#), "{sql}");
        assert!(sql.contains("topic = 'dm:1'"), "{sql}");
        assert!(sql.contains("urgency = 'high'"), "{sql}");
        assert!(sql.contains("ttl = 3600"), "{sql}");
        assert!(
            sql.contains("send_at = <datetime> '2030-01-02T03:04:05.000Z'"),
            "{sql}"
        );
        surrealdb_core::syn::parse(&sql).expect("the CREATE parses");

        // Every column written is one the DDL defines.
        let ddl = include_str!("push_tables.surql");
        for field in [
            "to",
            "notification",
            "data",
            "topic",
            "urgency",
            "ttl",
            "send_at",
            "status",
        ] {
            assert!(
                ddl.contains(&format!(
                    "DEFINE FIELD OVERWRITE {field} ON TABLE _00_push_message"
                )),
                "{field}"
            );
        }
    }

    #[test]
    fn bad_input_is_refused_before_anything_is_written() {
        assert!(Message::build(args(&["abc"]))
            .unwrap_err()
            .to_string()
            .contains("record id"));
        assert!(Message::build(args(&[])).is_err());
        let mut a = args(&["user:a"]);
        a.body = Some("no title".into());
        assert!(Message::build(a)
            .unwrap_err()
            .to_string()
            .contains("--title"));
        let mut a = args(&["user:a"]);
        a.urgency = Some("urgent".into());
        assert!(Message::build(a).is_err());
        let mut a = args(&["user:a"]);
        a.data = Some("[1]".into());
        assert!(Message::build(a)
            .unwrap_err()
            .to_string()
            .contains("object"));
        let mut a = args(&["user:a"]);
        a.at = Some("tomorrow".into());
        assert!(Message::build(a).is_err());
    }

    #[test]
    fn at_takes_a_time_or_a_delay() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-09-29T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(parse_when("10m", now).unwrap(), "2026-09-29T10:10:00.000Z");
        assert_eq!(
            parse_when("+1h30m", now).unwrap(),
            "2026-09-29T11:30:00.000Z"
        );
        assert_eq!(
            parse_when("in 1d", now).unwrap(),
            "2026-09-30T10:00:00.000Z"
        );
        assert_eq!(
            parse_when("2026-10-01T09:00:00+02:00", now).unwrap(),
            "2026-10-01T07:00:00.000Z"
        );
    }

    #[test]
    fn read_side_sql_parses() {
        surrealdb_core::syn::parse(STATUS_SQL).expect("status SQL parses");
        surrealdb_core::syn::parse(&devices_sql("user:⟨a'b⟩")).expect("devices SQL parses");
        surrealdb_core::syn::parse(&message_state_sql("_00_push_message:abc").unwrap())
            .expect("state SQL parses");
        assert!(message_state_sql("user:abc").is_err());
    }

    #[test]
    fn status_reads_every_statement() {
        let spec = serde_json::to_string(
            &serde_yaml::from_str::<PushConfig>("rules: { r: { table: message, to: recipient } }")
                .unwrap(),
        )
        .unwrap();
        let s = parse_status(&[
            json!([{ "spec_json": spec, "hash": "h", "updated_at": "2026-09-29T10:00:00Z" }]),
            json!({ "public_key": "BPk", "kid": "0011" }),
            json!({ "total": 5, "disabled": 1, "stale": 2, "users": 3 }),
            json!([{ "status": "pending", "n": 4 }, { "status": "sent", "n": 10 }]),
            json!(3),
            json!([{ "id": "_00_push_message:x", "error": "boom" }]),
        ]);
        assert_eq!(s.config.unwrap().rules.len(), 1);
        assert_eq!(s.public_key.as_deref(), Some("BPk"));
        assert_eq!(
            (s.subscriptions, s.disabled, s.stale, s.users),
            (5, 1, 2, 3)
        );
        assert_eq!(s.messages["pending"], 4);
        assert_eq!(s.scheduled, 3);
        assert_eq!(s.recent_failures.len(), 1);

        let empty = parse_status(&[
            json!([]),
            json!({}),
            json!({}),
            json!([]),
            json!(0),
            json!([]),
        ]);
        assert!(
            empty.config.is_none() && empty.config_error.is_none() && empty.public_key.is_none()
        );
    }

    #[test]
    fn push_service_is_the_endpoint_host() {
        assert_eq!(
            push_service("https://fcm.googleapis.com/fcm/send/abc"),
            "fcm.googleapis.com"
        );
        assert_eq!(
            push_service("https://web.push.apple.com:443/x?y"),
            "web.push.apple.com:443"
        );
    }
}
