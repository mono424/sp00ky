//! A WhitePawn-shaped dataset, generated deterministically.
//!
//! Every value is a pure function of (table, row index, seed), so the same
//! command produces the same records on every server and every run. Rows are
//! written as `OPTION IMPORT; INSERT IGNORE INTO ...`: import mode runs no
//! events and writes no changefeed entries (checked on 3.3.5 and 4.0.1), which
//! is what a restore or the replay upgrade does too. It also skips VALUE and
//! DEFAULT clauses, so the generator fills every field itself, typed the way
//! the field is defined.
//!
//! The shape comes from the schema the server holds (`INFO FOR TABLE`), i.e.
//! from what `spky migrate apply` deployed. Most tables are filled from their
//! field types; the tables WhitePawn's load and sp00ky's queries depend on get
//! realistic distributions: a few heavy libraries (Zipf over users), games in
//! their owner's collections, about 40 analysed positions per analysed game,
//! the `_00_version` ledger, feature flags, job queues and schedule runs.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::db::Http;

/// The password of every generated user (`bench<i>@example.test`).
pub const BENCH_PASSWORD: &str = "bench-password";

pub const SEED: u64 = 0x5DB4_7A11_0F0D_2026;

/// "Now" for the dataset: every timestamp is relative to it, so runs on
/// different days produce identical data.
pub const NOW_SECS: i64 = 1_794_787_200; // 2026-11-16T00:00:00Z

// ------------------------------------------------------------------ rng

pub fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    mix(h ^ SEED)
}

#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(a: u64, b: u64) -> Rng {
        Rng(mix(a ^ mix(b.wrapping_add(0x1234_5678))))
    }
    pub fn of(table: &str, idx: u64, salt: &str) -> Rng {
        Rng::new(hash_str(table) ^ hash_str(salt), idx)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
    pub fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo).max(1) as u64) as i64
    }
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

const B36: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

/// The record key of row `idx` of `table`: 20 lowercase base-36 characters
/// starting with a letter, the shape of a SurrealDB-generated id.
pub fn key(table: &str, idx: u64) -> String {
    let h1 = mix(hash_str(table) ^ idx.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let h2 = mix(h1 ^ 0xA5A5_A5A5);
    let mut s = String::with_capacity(20);
    s.push((b'a' + (h1 % 26) as u8) as char);
    let mut x = h1 / 26;
    for _ in 0..11 {
        s.push(B36[(x % 36) as usize] as char);
        x /= 36;
    }
    let mut y = h2;
    for _ in 0..8 {
        s.push(B36[(y % 36) as usize] as char);
        y /= 36;
    }
    s
}

pub fn rid(table: &str, idx: u64) -> String {
    format!("{table}:{}", key(table, idx))
}

pub fn dt(secs: i64) -> String {
    let d = chrono_like(secs);
    format!("d\"{d}\"")
}

/// RFC 3339 UTC for unix seconds (no chrono dependency).
pub fn chrono_like(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // civil from days, Howard Hinnant
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

pub fn q(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

// ------------------------------------------------------------------ zipf

/// A Zipf(s) sampler over `n` items by inverse CDF.
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: u64, s: f64) -> Zipf {
        let mut cdf = Vec::with_capacity(n as usize);
        let mut acc = 0.0;
        for k in 0..n {
            acc += 1.0 / ((k + 1) as f64).powf(s);
            cdf.push(acc);
        }
        for c in &mut cdf {
            *c /= acc;
        }
        Zipf { cdf }
    }
    pub fn sample(&self, rng: &mut Rng) -> u64 {
        let u = rng.unit();
        match self.cdf.binary_search_by(|c| c.partial_cmp(&u).unwrap()) {
            Ok(i) | Err(i) => i.min(self.cdf.len().saturating_sub(1)) as u64,
        }
    }
}

// ------------------------------------------------------------------ types

#[derive(Clone, Debug)]
pub enum Ty {
    Any,
    Str,
    Int,
    Float,
    Bool,
    Datetime,
    Duration,
    Uuid,
    Object,
    Record(Vec<String>),
    Option(Box<Ty>),
    Array(Box<Ty>),
    Union(Vec<Ty>),
    Null,
    Other,
}

fn split_top(s: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for c in s.chars() {
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if c == sep && depth == 0 {
            out.push(cur.trim().to_string());
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

pub fn parse_ty(s: &str) -> Ty {
    let parts = split_top(s.trim(), '|');
    if parts.len() > 1 {
        let tys: Vec<Ty> = parts.iter().map(|p| parse_ty(p)).collect();
        let has_none = parts.iter().any(|p| p == "none");
        let rest: Vec<Ty> = parts
            .iter()
            .filter(|p| *p != "none")
            .map(|p| parse_ty(p))
            .collect();
        let inner = if rest.len() == 1 {
            rest[0].clone()
        } else {
            Ty::Union(rest)
        };
        return if has_none {
            Ty::Option(Box::new(inner))
        } else if tys.is_empty() {
            Ty::Other
        } else {
            inner
        };
    }
    let p = parts.first().map(String::as_str).unwrap_or("any");
    let inner = |p: &str, pre: &str| -> String {
        let body = &p[pre.len()..p.len() - 1];
        split_top(body, ',').first().cloned().unwrap_or_default()
    };
    match p {
        "any" => Ty::Any,
        "string" => Ty::Str,
        "int" => Ty::Int,
        "float" | "number" | "decimal" => Ty::Float,
        "bool" => Ty::Bool,
        "datetime" => Ty::Datetime,
        "duration" => Ty::Duration,
        "uuid" => Ty::Uuid,
        "object" => Ty::Object,
        "record" => Ty::Record(vec![]),
        "none" => Ty::Option(Box::new(Ty::Null)),
        "null" => Ty::Null,
        _ if p.starts_with("option<") => Ty::Option(Box::new(parse_ty(&inner(p, "option<")))),
        _ if p.starts_with("array<") => Ty::Array(Box::new(parse_ty(&inner(p, "array<")))),
        _ if p.starts_with("set<") => Ty::Array(Box::new(parse_ty(&inner(p, "set<")))),
        _ if p.starts_with("record<") => {
            let body = &p["record<".len()..p.len() - 1];
            Ty::Record(split_top(body, '|'))
        }
        _ => Ty::Other,
    }
}

#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub ty: Ty,
    pub choices: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub name: String,
    pub relation: bool,
    pub fields: Vec<Field>,
}

fn field_of(def: &str) -> Option<Field> {
    // DEFINE FIELD [OVERWRITE] name ON table [TYPE ...] ...
    let rest = def.strip_prefix("DEFINE FIELD ")?;
    let rest = rest.strip_prefix("OVERWRITE ").unwrap_or(rest);
    let name_end = rest.find(" ON ")?;
    let name = rest[..name_end].trim().trim_matches('`').to_string();
    if name.contains('.') || name.contains('[') {
        return None; // nested definitions: the parent value covers them
    }
    let ty = match def.find(" TYPE ") {
        Some(i) => {
            let tail = &def[i + 6..];
            let stops = [
                " FLEXIBLE",
                " DEFAULT ",
                " READONLY",
                " VALUE ",
                " ASSERT ",
                " PERMISSIONS ",
                " COMMENT ",
                " REFERENCE",
            ];
            let end = stops
                .iter()
                .filter_map(|s| tail.find(s))
                .min()
                .unwrap_or(tail.len());
            parse_ty(&tail[..end])
        }
        None => Ty::Any,
    };
    let mut choices = Vec::new();
    if let Some(i) = def.find(" ASSERT ") {
        let tail = &def[i..];
        if let (Some(a), Some(b)) = (tail.find("INSIDE ["), tail.find(']')) {
            if a < b {
                for part in tail[a + 8..b].split(',') {
                    let p = part.trim().trim_matches('\'').trim_matches('"');
                    if !p.is_empty() {
                        choices.push(p.to_string());
                    }
                }
            }
        }
    }
    Some(Field { name, ty, choices })
}

pub async fn read_schema(http: &Http) -> Result<BTreeMap<String, TableInfo>> {
    let info = http.last("INFO FOR DB").await?;
    let tables = info
        .get("tables")
        .and_then(Value::as_object)
        .context("INFO FOR DB has no tables")?;
    let mut sql = String::new();
    let names: Vec<String> = tables.keys().cloned().collect();
    for n in &names {
        sql.push_str(&format!("INFO FOR TABLE `{n}`;"));
    }
    let infos = http.sql(&sql).await?;
    let mut out = BTreeMap::new();
    for (name, info) in names.iter().zip(infos) {
        let def = tables[name].as_str().unwrap_or("");
        let relation = def.contains("TYPE RELATION");
        let mut fields = Vec::new();
        if let Some(map) = info.get("fields").and_then(Value::as_object) {
            for v in map.values() {
                if let Some(f) = v.as_str().and_then(field_of) {
                    fields.push(f);
                }
            }
        }
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        out.insert(
            name.clone(),
            TableInfo {
                name: name.clone(),
                relation,
                fields,
            },
        );
    }
    Ok(out)
}

// ------------------------------------------------------------------ plan

/// Row counts at scale 1.0. Production (2026-11-16): 17.16M rows, analysis
/// 11.45M, `_00_version` 3.0M, game 750k; the rest is spread over the other
/// tables the way the app uses them.
pub fn base_counts() -> BTreeMap<&'static str, u64> {
    BTreeMap::from([
        ("analysis", 11_450_000),
        ("game", 750_000),
        ("_00_version", 3_000_000),
        ("user", 5_000),
        ("game_database", 25_000),
        ("player_name", 150_000),
        ("database_players", 60_000),
        ("contact", 40_000),
        ("connection", 3_000),
        ("collection_share", 2_000),
        ("statistics", 25_000),
        ("game_insight", 120_000),
        ("comment", 50_000),
        ("branch", 80_000),
        ("annotation", 120_000),
        ("puzzle", 30_000),
        ("puzzle_progress", 150_000),
        ("puzzle_rating", 50_000),
        ("puzzle_save", 20_000),
        ("puzzle_chapter", 3_000),
        ("puzzle_lecture", 1_000),
        ("puzzle_facet", 2_000),
        ("puzzle_trainer", 3_000),
        ("notification", 200_000),
        ("message", 60_000),
        ("conversation", 3_000),
        ("message_link", 10_000),
        ("activity_day", 100_000),
        ("metric_event", 300_000),
        ("elo_adjustment", 30_000),
        ("profile_stats", 5_000),
        ("pgn_import", 5_000),
        ("prep", 2_000),
        ("scout", 2_000),
        ("scout_position", 50_000),
        ("analysis_board", 10_000),
        ("live_game", 5_000),
        ("challenge", 5_000),
        ("play_rating", 5_000),
        ("club", 200),
        ("club_member", 2_000),
        ("club_post", 2_000),
        ("club_post_comment", 3_000),
        ("club_event", 300),
        ("event_signup", 1_000),
        ("chat_message", 50_000),
        ("call", 2_000),
        ("friendship", 5_000),
        ("presence", 2_000),
        ("broadcast", 1_000),
        ("broadcast_eval", 20_000),
        ("broadcast_share", 500),
        ("stream_presence", 500),
        ("cloud_analysis_usage", 10_000),
        ("cloud_render_usage", 2_000),
        ("cloud_render_session", 2_000),
        ("commentator_session", 1_000),
        ("commentator_usage", 1_000),
        ("battery_warning", 2_000),
        ("renderer_device", 500),
        ("link_preview_cache", 5_000),
        ("mcp_action", 2_000),
        ("mcp_token", 500),
        ("mcp_grant", 500),
        ("mcp_client", 100),
        ("mcp_flow", 200),
        ("email_notice", 20_000),
        ("email_verification", 5_000),
        ("password_reset", 1_000),
        ("signup_code", 200),
        ("app_release", 50),
        ("commented_on", 20_000),
        // outbox job queues (the job tables `spky` defines per backend)
        ("job", 8_000),
        ("gameanalysis_job", 8_000),
        ("statistics_job", 4_000),
        ("email_job", 4_000),
        ("linkpreview_job", 2_000),
        ("camera_job", 500),
        ("render_job", 500),
        // sp00ky internal
        ("_00_user_feature", 4_600),
        ("_00_feature_flag", 20),
        ("_00_admin", 3),
        ("_00_schedule_run", 40_000),
        ("_00_schedule_key", 3_000),
        ("_00_workflow_run", 20_000),
        ("_00_step_run", 60_000),
        ("_00_wf_instance", 2_000),
        ("_00_wf_step", 6_000),
        ("_00_wf_event", 2_000),
        ("_00_run_rollup", 2_000),
        ("_00_heartbeat_rollup", 2_000),
        ("_00_push_subscription", 2_000),
        ("_00_push_message", 20_000),
        ("_00_impersonation", 100),
        ("_00_impersonation_write", 500),
        ("_00_app_release", 20),
    ])
}

/// Tables written by `spky migrate apply`, the SSP or the scheduler at run
/// time, never by the generator.
const SKIP: &[&str] = &[
    "_00_migrations",
    "_00_schedule",
    "_00_schema_state",
    "_00_push_config",
    "_00_push_credential",
    "_00_retention",
    "_00_job_policy",
    "_00_session_guard",
    "_00_module_state",
    "_00_pool",
    "_00_machine",
    "_00_query_allowlist",
    "_00_sync_evidence",
    "_00_heartbeat",
    "_00_query",
    "_00_list_ref",
    "broadcast_job",
    "play_config",
    "play_instance",
    "play_lease",
    "play_player",
    "live_game_state",
    "broadcast_head",
    "broadcast_scenes",
];

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Plan {
    pub scale: f64,
    pub analysis_rows: u64,
    /// Share of `_00_version` rows with a random id (the pre-keyed ledger
    /// WhitePawn still had on 2026-11-16); the rest are keyed by record.
    pub legacy_ledger: f64,
    pub counts: BTreeMap<String, u64>,
}

impl Plan {
    pub fn new(scale: f64, analysis_rows: Option<u64>, legacy_ledger: f64) -> Plan {
        let mut counts = BTreeMap::new();
        for (t, n) in base_counts() {
            let n = match t {
                // the feature-flag and admin tables do not grow with data
                "_00_feature_flag" | "_00_admin" | "app_release" | "_00_app_release" => n,
                _ => ((n as f64) * scale).round().max(1.0) as u64,
            };
            counts.insert(t.to_string(), n);
        }
        let analysis = analysis_rows.unwrap_or(counts["analysis"]);
        counts.insert("analysis".into(), analysis);
        // the ledger: every game and the newest analysis rows, 3M at scale 1
        let ledger = ((3_000_000f64) * scale).round() as u64;
        counts.insert("_00_version".into(), ledger.min(counts["game"] + analysis));
        Plan {
            scale,
            analysis_rows: analysis,
            legacy_ledger,
            counts,
        }
    }

    pub fn count(&self, t: &str) -> u64 {
        self.counts.get(t).copied().unwrap_or(0)
    }

    pub fn total(&self) -> u64 {
        self.counts.values().sum()
    }
}

// ------------------------------------------------------------------ rows

pub struct Gen {
    pub plan: Plan,
    /// One argon2 hash every bench user shares (`BENCH_PASSWORD`).
    pub password_hash: String,
    pub schema: BTreeMap<String, TableInfo>,
    users: Zipf,
    games_hot: Zipf,
    flags: Vec<String>,
}

pub const FLAG_KEYS: &[&str] = &[
    "new_board_ui",
    "cloud_analysis",
    "prep_mode",
    "puzzle_trainer_v2",
    "club_events",
    "livestream_beta",
    "voice_commentator",
    "camera_board",
    "pgn_import_v3",
    "notifications_v2",
    "explore_store",
    "play_live",
    "chess_vision",
    "insights_panel",
    "opening_tree",
    "mcp_access",
    "dark_board",
    "scout_v2",
    "elo_adjust",
    "activity_heatmap",
];

const WORDS: &[&str] = &[
    "alpha",
    "bishop",
    "castle",
    "dragon",
    "endgame",
    "fianchetto",
    "gambit",
    "hedgehog",
    "isolani",
    "jaenisch",
    "knight",
    "london",
    "marshall",
    "najdorf",
    "outpost",
    "pawn",
    "queen",
    "rook",
    "sicilian",
    "tempo",
    "under",
    "vienna",
    "winawer",
    "xray",
    "yugoslav",
    "zugzwang",
    "berlin",
    "caro",
    "dutch",
    "english",
    "french",
    "grunfeld",
    "italian",
    "kings",
];

const SAN: &[&str] = &[
    "e4", "e5", "Nf3", "Nc6", "Bb5", "a6", "Ba4", "Nf6", "O-O", "Be7", "Re1", "b5", "Bb3", "d6",
    "c3", "O-O", "h3", "Nb8", "d4", "Nbd7", "c4", "c5", "d5", "exd5", "Nxd5", "Qd2", "Bg5", "h6",
    "Bh4", "g5", "Bg3", "Rad1", "Rfe8", "Qe2", "Bxf7+", "Kxf7", "Ng5+", "Kg8", "Qh5", "Qf6",
];

const ENGINES: &[&str] = &["sf18-lite", "sf18-lite", "sf18-lite", "cloud", "sf11"];

fn words(rng: &mut Rng, n: usize) -> String {
    let mut s = String::new();
    for i in 0..n {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(rng.pick(WORDS));
    }
    s
}

fn pgn(rng: &mut Rng) -> String {
    let moves = rng.range(30, 90);
    let mut s = String::with_capacity(1200);
    s.push_str("[Event \"Rated blitz game\"]\n[Site \"https://lichess.org/");
    s.push_str(&key("pgn", rng.next() % 1_000_000)[..8]);
    s.push_str("\"]\n[White \"");
    s.push_str(rng.pick(WORDS));
    s.push_str("\"]\n[Black \"");
    s.push_str(rng.pick(WORDS));
    s.push_str("\"]\n[Result \"1-0\"]\n\n");
    for m in 1..=moves {
        s.push_str(&format!("{m}. {} {} ", rng.pick(SAN), rng.pick(SAN)));
    }
    s.push_str("1-0");
    s
}

fn fen(rng: &mut Rng, ply: u64) -> String {
    const PIECES: &[u8] = b"rnbqkpRNBQKP12345678";
    let mut s = String::with_capacity(64);
    for rank in 0..8 {
        if rank > 0 {
            s.push('/');
        }
        for _ in 0..rng.range(3, 8) {
            s.push(PIECES[rng.below(PIECES.len() as u64) as usize] as char);
        }
    }
    s.push_str(if ply % 2 == 0 { " w" } else { " b" });
    s.push_str(" KQkq - ");
    s.push_str(&ply.to_string());
    s
}

fn variants(rng: &mut Rng) -> String {
    let mut lines = Vec::new();
    for pv in 1..=3 {
        let mut moves = Vec::new();
        for _ in 0..rng.range(8, 16) {
            moves.push(format!(
                "{}{}{}{}",
                (b'a' + rng.below(8) as u8) as char,
                1 + rng.below(8),
                (b'a' + rng.below(8) as u8) as char,
                1 + rng.below(8)
            ));
        }
        lines.push(format!(
            "{{\"multipv\":{pv},\"depth\":{},\"cp\":{},\"mate\":null,\"pv\":\"{}\"}}",
            rng.range(16, 30),
            rng.range(-300, 300),
            moves.join(" ")
        ));
    }
    format!("[{}]", lines.join(","))
}

impl Gen {
    pub fn new(plan: Plan, schema: BTreeMap<String, TableInfo>) -> Gen {
        let users = Zipf::new(plan.count("user").max(1), 1.05);
        let games_hot = Zipf::new(plan.count("game").max(1), 0.6);
        Gen {
            password_hash: String::new(),
            flags: FLAG_KEYS.iter().map(|s| s.to_string()).collect(),
            plan,
            schema,
            users,
            games_hot,
        }
    }

    pub fn n(&self, t: &str) -> u64 {
        self.plan.count(t)
    }

    /// The tables to fill, in dependency order (referenced tables first,
    /// the big ones last).
    pub fn tables(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .schema
            .keys()
            .filter(|t| {
                !SKIP.contains(&t.as_str())
                    && !t.starts_with("_00_list_ref_user_")
                    && !t.starts_with("_sdb4p")
            })
            .filter(|t| self.count_for(t) > 0)
            .cloned()
            .collect();
        let rank = |t: &str| match t {
            "user" => 0,
            "game_database" => 1,
            "player_name" => 2,
            "game" => 50,
            "_00_version" => 60,
            "analysis" => 70,
            _ => 10,
        };
        out.sort_by_key(|t| (rank(t), t.clone()));
        out
    }

    pub fn count_for(&self, t: &str) -> u64 {
        match self.plan.counts.get(t) {
            Some(n) => *n,
            None if t.starts_with("_00_") => 0,
            None => ((200.0 * self.plan.scale).round() as u64).max(20),
        }
    }

    /// The owner (user index) of game `g`.
    pub fn game_owner(&self, g: u64) -> u64 {
        self.users.sample(&mut Rng::of("game", g, "owner"))
    }

    /// The collection (game_database index) of game `g`: 80% in the owner's
    /// main collection, the rest in its other four.
    pub fn game_database(&self, g: u64, owner: u64) -> u64 {
        let n_users = self.n("user").max(1);
        let per_user = (self.n("game_database") / n_users).max(1);
        let mut rng = Rng::of("game", g, "database");
        let k = if rng.chance(0.8) {
            0
        } else {
            rng.below(per_user)
        };
        (owner + n_users * k).min(self.n("game_database").saturating_sub(1))
    }

    /// The `date` of game `g` (its `sort_index` is `-date`).
    pub fn game_date(&self, g: u64) -> i64 {
        NOW_SECS - Rng::of("game", g, "date").range(0, 5 * 365 * 86_400)
    }

    fn user_ref(&self, rng: &mut Rng) -> String {
        rid("user", self.users.sample(rng))
    }

    /// One row as a SurrealQL object literal.
    pub fn row(&self, table: &str, idx: u64) -> String {
        let mut rng = Rng::of(table, idx, "row");
        let info = &self.schema[table];
        let mut fields: BTreeMap<String, String> = BTreeMap::new();
        for f in &info.fields {
            if f.name == "id" {
                continue;
            }
            if let Some(v) = self.value(table, f, &f.ty, &mut rng, 0) {
                fields.insert(f.name.clone(), v);
            }
        }
        let id = self.special(table, idx, &mut rng, &mut fields);
        // SCHEMAFULL tables refuse unknown fields even under OPTION IMPORT
        if table != "_00_version" {
            fields.retain(|k, _| info.fields.iter().any(|f| f.name == *k));
        }
        let mut s = String::with_capacity(256);
        s.push_str("{id:");
        s.push_str(&id.unwrap_or_else(|| rid(table, idx)));
        for (k, v) in &fields {
            s.push(',');
            if k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                s.push_str(k);
            } else {
                s.push('`');
                s.push_str(k);
                s.push('`');
            }
            s.push(':');
            s.push_str(v);
        }
        s.push('}');
        s
    }

    fn value(&self, table: &str, f: &Field, ty: &Ty, rng: &mut Rng, depth: u32) -> Option<String> {
        if !f.choices.is_empty() && depth == 0 {
            return Some(q(rng.pick(&f.choices)));
        }
        let name = f.name.as_str();
        Some(match ty {
            Ty::Option(inner) => {
                if rng.chance(0.45) {
                    return None;
                }
                return self.value(table, f, inner, rng, depth);
            }
            Ty::Null => return None,
            Ty::Union(alts) => {
                let alt = rng.pick(alts).clone();
                return self.value(table, f, &alt, rng, depth);
            }
            Ty::Str | Ty::Any => q(&self.string_for(name, rng)),
            Ty::Int => {
                let v = if name.ends_with("_ms") || name.ends_with("Ms") {
                    (NOW_SECS - rng.range(0, 400 * 86_400)) * 1000
                } else if name.to_lowercase().contains("elo") || name.contains("rating") {
                    rng.range(800, 2800)
                } else {
                    rng.range(0, 1000)
                };
                v.to_string()
            }
            Ty::Float => format!("{:.3}f", rng.unit() * 100.0),
            Ty::Bool => rng.chance(0.5).to_string(),
            Ty::Datetime => dt(NOW_SECS - rng.range(0, 5 * 365 * 86_400)),
            Ty::Duration => "1h".into(),
            Ty::Uuid => format!(
                "u\"{:08x}-0000-4000-8000-{:012x}\"",
                rng.next() as u32,
                rng.next() & 0xffff_ffff_ffff
            ),
            Ty::Object => "{}".into(),
            Ty::Array(inner) => {
                if depth > 0 || matches!(**inner, Ty::Object | Ty::Any) {
                    return Some("[]".into());
                }
                let n = rng.below(3);
                let mut items = Vec::new();
                for _ in 0..n {
                    if let Some(v) = self.value(table, f, inner, rng, depth + 1) {
                        items.push(v);
                    }
                }
                format!("[{}]", items.join(","))
            }
            Ty::Record(tables) => {
                let target = if tables.is_empty() {
                    "user".to_string()
                } else {
                    rng.pick(tables).clone()
                };
                let n = self.count_for(&target);
                if n == 0 {
                    return None;
                }
                if target == "user" {
                    self.user_ref(rng)
                } else {
                    rid(&target, rng.below(n))
                }
            }
            Ty::Other => return None,
        })
    }

    fn string_for(&self, name: &str, rng: &mut Rng) -> String {
        match name {
            "pgn" => pgn(rng),
            "fen" => {
                let ply = rng.below(120);
                fen(rng, ply)
            }
            "variants" => variants(rng),
            "email" => format!("{}@example.test", key("email", rng.next() % 1_000_000_000)),
            n if n.ends_with("_url") || n == "url" => {
                format!(
                    "https://example.test/{}",
                    key("url", rng.next() % 1_000_000)
                )
            }
            n if n.contains("token") || n.contains("key") || n.contains("secret") => {
                key("tok", rng.next() % 1_000_000_000)
            }
            "body" | "text" | "content" | "description" | "bio" | "message" | "comment" => {
                words(rng, 12)
            }
            _ => {
                let n = 1 + rng.below(3) as usize;
                words(rng, n)
            }
        }
    }

    /// Distributions for the tables the workloads depend on. Returns the id
    /// when it is not the default key.
    fn special(
        &self,
        table: &str,
        idx: u64,
        rng: &mut Rng,
        f: &mut BTreeMap<String, String>,
    ) -> Option<String> {
        let n_users = self.n("user").max(1);
        match table {
            "user" => {
                f.insert("email".into(), q(&format!("bench{idx}@example.test")));
                f.insert("username".into(), q(&format!("bench{idx}")));
                f.insert("password".into(), q(&self.password_hash));
                f.remove("session_epoch");
                f.remove("google_sub");
                f.remove("lichess_id");
                f.insert(
                    "created_at".into(),
                    dt(NOW_SECS - rng.range(0, 3 * 365 * 86_400)),
                );
                if idx % 7 == 0 {
                    f.insert("plan".into(), q("pro"));
                } else {
                    f.remove("plan");
                }
            }
            "game_database" => {
                let owner = idx % n_users;
                f.insert("owner".into(), rid("user", owner));
                f.insert("name".into(), q(&words(rng, 2)));
            }
            "player_name" => {
                f.insert("author".into(), rid("user", idx % n_users));
                f.insert("name".into(), q(&format!("{} {}", words(rng, 1), idx)));
            }
            "notification" => {
                f.insert("recipient".into(), self.user_ref(rng));
                let at = (NOW_SECS - rng.range(0, 90 * 86_400)) * 1000;
                f.insert("created_ms".into(), at.to_string());
                f.insert("updated_ms".into(), at.to_string());
            }
            "game" => {
                let owner = self.game_owner(idx);
                let db = self.game_database(idx, owner);
                let date = self.game_date(idx);
                let per_user = (self.n("player_name") / n_users).max(1);
                let pn = |rng: &mut Rng| {
                    rid(
                        "player_name",
                        (owner + n_users * rng.below(per_user))
                            .min(self.n("player_name").saturating_sub(1)),
                    )
                };
                f.insert("owner".into(), rid("user", owner));
                f.insert("database".into(), rid("game_database", db));
                f.insert("white".into(), pn(rng));
                f.insert("black".into(), pn(rng));
                f.insert("date".into(), dt(date));
                f.insert("created_at".into(), dt(date + rng.range(0, 86_400)));
                f.insert("sort_index".into(), (-date).to_string());
                f.insert("pgn".into(), q(&pgn(rng)));
                f.insert("status".into(), q("finished"));
                f.insert("result".into(), q(rng.pick(&["1-0", "0-1", "1/2-1/2"])));
                if rng.chance(0.6) {
                    f.insert("source".into(), q(rng.pick(&["lichess", "chesscom"])));
                    f.insert("source_id".into(), q(&key("src", idx)));
                } else {
                    f.remove("source");
                    f.remove("source_id");
                }
                f.remove("broadcast");
            }
            "analysis" => {
                // about 40 positions per analysed game; hot games analysed by
                // several viewers
                let block = idx / 40;
                let g = self
                    .games_hot
                    .sample(&mut Rng::of("analysis", block, "game"));
                let owner = if rng.chance(0.85) {
                    self.game_owner(g)
                } else {
                    self.users.sample(rng)
                };
                let created = NOW_SECS - rng.range(0, 2 * 365 * 86_400);
                f.insert("game".into(), rid("game", g));
                f.insert("owner".into(), rid("user", owner));
                f.insert("fen".into(), q(&format!("{} #{idx}", fen(rng, idx % 40))));
                f.insert("engine".into(), q(rng.pick(ENGINES)));
                f.insert("depth".into(), rng.range(14, 32).to_string());
                f.insert("variants".into(), q(&variants(rng)));
                f.insert("created_at".into(), dt(created));
                f.insert("updated_at".into(), dt(created + rng.range(0, 86_400)));
            }
            "_00_version" => {
                let n_games = self.n("game");
                let record = if idx < n_games {
                    rid("game", idx)
                } else {
                    // the newest analysis rows
                    rid(
                        "analysis",
                        self.n("analysis").saturating_sub(1 + idx - n_games),
                    )
                };
                f.clear();
                f.insert("record_id".into(), record.clone());
                f.insert("version".into(), (1 + rng.below(12)).to_string());
                let legacy = Rng::of("_00_version", idx, "legacy").unit() < self.plan.legacy_ledger;
                if !legacy {
                    return Some(format!("_00_version:[{record}]"));
                }
            }
            "connection" => {
                f.insert("owner".into(), rid("user", idx % n_users));
                f.insert(
                    "provider".into(),
                    q(rng.pick(&["lichess", "lichess", "chesscom", "twitch"])),
                );
                if rng.chance(0.9) {
                    f.insert("last_synced_at".into(), dt(NOW_SECS - rng.range(0, 3600)));
                }
                if rng.chance(0.97) {
                    f.remove("sync_error");
                }
                f.remove("sync_failed_at");
            }
            "collection_share" => {
                f.insert("user".into(), self.user_ref(rng));
                f.insert("role".into(), q(rng.pick(&["viewer", "editor"])));
            }
            "_00_feature_flag" => {
                let key = FLAG_KEYS[(idx as usize) % FLAG_KEYS.len()];
                f.insert("key".into(), q(key));
                f.insert("enabled".into(), "true".into());
                f.insert("default_variant".into(), q("off"));
                f.insert("variants".into(), "['off','on']".into());
                f.insert("rules".into(), "[{ variant: 'on', percent: 20 }]".into());
                return Some(format!("_00_feature_flag:{key}"));
            }
            "_00_user_feature" => {
                // a few flags per user for the most active users
                let per = self.flags.len() as u64;
                let user = idx / per;
                let key = &self.flags[(idx % per) as usize];
                f.insert("user".into(), rid("user", user % n_users));
                f.insert("key".into(), q(key));
                f.insert(
                    "variant".into(),
                    q(if rng.chance(0.3) { "on" } else { "off" }),
                );
                f.insert(
                    "evaluated_at".into(),
                    dt(NOW_SECS - rng.range(0, 30 * 86_400)),
                );
            }
            "_00_admin" => {
                f.insert("user".into(), rid("user", idx));
            }
            "_00_schedule_run" => {
                f.insert(
                    "schedule_name".into(),
                    q(rng.pick(&[
                        "game-sync",
                        "game-sync",
                        "game-sync",
                        "stats-rollup",
                        "digest",
                    ])),
                );
                f.insert(
                    "key".into(),
                    q(&rid("connection", rng.below(self.n("connection").max(1)))),
                );
                f.insert("kind".into(), q("job"));
                let fire = NOW_SECS - rng.range(0, 30 * 86_400);
                f.insert("fire_at".into(), dt(fire));
                f.insert("created_at".into(), dt(fire));
                f.insert("updated_at".into(), dt(fire + 5));
                let status = if rng.chance(0.85) {
                    "success"
                } else if rng.chance(0.6) {
                    "failed"
                } else {
                    "skipped"
                };
                f.insert("status".into(), q(status));
                f.insert("finished_at".into(), dt(fire + rng.range(1, 60)));
                f.insert(
                    "job_id".into(),
                    q(&rid("job", rng.below(self.n("job").max(1)))),
                );
                f.remove("schedule");
                f.remove("workflow_run");
            }
            t if t.ends_with("job") && f.contains_key("status") => {
                // outbox queues: mostly finished, a few pending / processing
                let r = rng.unit();
                let status = if r < 0.02 {
                    "pending"
                } else if r < 0.03 {
                    "processing"
                } else if r < 0.93 {
                    "success"
                } else {
                    "failed"
                };
                f.insert("status".into(), q(status));
                let created = NOW_SECS - rng.range(0, 14 * 86_400);
                f.insert("created_at".into(), dt(created));
                f.insert("updated_at".into(), dt(created + rng.range(1, 300)));
                f.insert("retries".into(), "0".into());
                f.insert("max_retries".into(), "3".into());
                f.insert("errors".into(), "[]".into());
                f.insert("retry_strategy".into(), q("linear"));
                if status == "processing" {
                    f.insert("assignee".into(), q("ssp-0"));
                    f.insert("lease_until".into(), dt(NOW_SECS + 60));
                } else {
                    f.remove("assignee");
                    f.remove("lease_until");
                }
            }
            _ => {}
        }
        None
    }
}

// ------------------------------------------------------------------ load

pub struct LoadReport {
    pub rows: u64,
    pub elapsed: Duration,
}

/// Rows per INSERT, by table: about 1-2 MB per request.
fn batch_rows(table: &str) -> u64 {
    match table {
        "game" => 600,
        "analysis" => 1500,
        "_00_version" => 3000,
        _ => 1000,
    }
}

/// Fill one table. `INSERT IGNORE` makes a re-run after an interruption
/// finish the table without duplicating rows.
pub async fn load_table(
    http: &Http,
    gen: Arc<Gen>,
    table: &str,
    parallel: usize,
) -> Result<LoadReport> {
    let n = gen.count_for(table);
    let started = Instant::now();
    let per = batch_rows(table);
    // Concurrent inserts into one table conflict on shared index keys
    // ("Resource busy"); only the big tables are worth loading in parallel.
    let parallel = if matches!(table, "game" | "analysis" | "_00_version") {
        parallel
    } else {
        1
    };
    let batches = n.div_ceil(per);
    let next = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let relation = gen.schema[table].relation;
    let mut tasks = Vec::new();
    for _ in 0..parallel {
        let http = http.clone();
        let gen = gen.clone();
        let next = next.clone();
        let done = done.clone();
        let table = table.to_string();
        tasks.push(tokio::spawn(async move {
            loop {
                let b = next.fetch_add(1, Ordering::Relaxed);
                if b >= batches {
                    return Ok::<(), anyhow::Error>(());
                }
                let lo = b * per;
                let hi = ((b + 1) * per).min(n);
                // Requests stay under the server's 1 MiB body limit.
                let head = if relation {
                    format!("OPTION IMPORT;\nINSERT RELATION IGNORE INTO `{table}` [")
                } else {
                    format!("OPTION IMPORT;\nINSERT IGNORE INTO `{table}` [")
                };
                let mut requests = Vec::new();
                let mut sql = head.clone();
                let mut rows_in = 0;
                for i in lo..hi {
                    let row = gen.row(&table, i);
                    if rows_in > 0 && sql.len() + row.len() > 700_000 {
                        sql.push_str("] RETURN NONE;");
                        requests.push(std::mem::replace(&mut sql, head.clone()));
                        rows_in = 0;
                    }
                    if rows_in > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&row);
                    rows_in += 1;
                }
                sql.push_str("] RETURN NONE;");
                requests.push(sql);
                for sql in requests {
                    let mut attempt = 0;
                    loop {
                        attempt += 1;
                        match http.sql_raw(&sql, Duration::from_secs(600)).await {
                            Ok(stmts) if stmts.iter().all(|s| s.ok) => break,
                            Ok(stmts) => {
                                let err = stmts
                                    .iter()
                                    .find(|s| !s.ok)
                                    .map(|s| s.result.to_string())
                                    .unwrap_or_default();
                                let retryable =
                                    err.contains("can be retried") || err.contains("conflict");
                                if attempt >= 30 || !retryable {
                                    bail!("{table} batch {b}: {err}");
                                }
                                if attempt % 5 == 0 {
                                    eprintln!(
                                        "  {table} batch {b} attempt {attempt}: {}",
                                        crate::db::truncate(&err, 300)
                                    );
                                }
                            }
                            Err(e) => {
                                if attempt >= 5 {
                                    return Err(e.context(format!("{table} batch {b}")));
                                }
                                eprintln!("  {table} batch {b} attempt {attempt}: {e:#}");
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(200 * attempt.min(20))).await;
                    }
                }
                done.fetch_add(hi - lo, Ordering::Relaxed);
            }
        }));
    }
    let progress = {
        let done = done.clone();
        let table = table.to_string();
        tokio::spawn(async move {
            let started = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let d = done.load(Ordering::Relaxed);
                let rate = d as f64 / started.elapsed().as_secs_f64();
                eprintln!(
                    "  {table}: {d}/{n} ({:.0}/s, eta {:.0}s)",
                    rate,
                    (n.saturating_sub(d)) as f64 / rate.max(1.0)
                );
            }
        })
    };
    let mut first_err = None;
    for t in tasks {
        if let Err(e) = t.await? {
            first_err.get_or_insert(e);
        }
    }
    progress.abort();
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(LoadReport {
        rows: n,
        elapsed: started.elapsed(),
    })
}
