//! The SSP's state in the database: registered views (`_00_query`) and their
//! edges in the per-user `_00_list_ref_user_<uid>` tables (and the shared
//! `_00_list_ref_anon`), the way a long-running SSP leaves them.
//!
//! The tables are defined with the SSP's own DDL
//! (`ssp_node::tables::user_list_ref_ddl`); edges are written with
//! `INSERT RELATION`, which writes the graph pointers `RELATE` does (checked:
//! `<record><-<table>` finds them), so the publication queries walk the same
//! structure as in production.
//!
//! Shape: the most active users (the top of the library Zipf) each hold many
//! views, most of them 50-row windows over their main collection at a few
//! popular offsets (so the top rows of a collection are in many views, which
//! is what makes `<record><-<table> WHERE in = $from` read many edges), some
//! game windows with their players as subquery children, some notification
//! lists, analysis panels, and a few views over a whole library (tens of
//! thousands of edges).

use std::collections::BTreeMap;

use crate::gen::{dt, key, q, rid, Gen, Rng, NOW_SECS};

#[derive(Clone, Debug)]
pub struct View {
    /// `_00_query` key.
    pub key: String,
    /// `None`: an anonymous view in `_00_list_ref_anon`.
    pub user: Option<u64>,
    pub surql: String,
    pub params: String,
    pub client_id: String,
    /// (record, child-of (record, alias))
    pub edges: Vec<(String, Option<(String, &'static str)>)>,
    pub last_active_secs_ago: i64,
}

impl View {
    pub fn table(&self) -> String {
        match self.user {
            Some(u) => list_ref_table(u),
            None => "_00_list_ref_anon".into(),
        }
    }
    pub fn auth_id(&self) -> String {
        match self.user {
            Some(u) => rid("user", u),
            None => "anon".into(),
        }
    }
    pub fn query_rid(&self) -> String {
        format!("_00_query:⟨{}⟩", self.key)
    }
}

pub fn list_ref_table(user: u64) -> String {
    format!("_00_list_ref_user_{}", key("user", user))
}

/// The users that hold views.
pub fn view_users(gen: &Gen) -> u64 {
    ((30.0 * gen.plan.scale).round() as u64).clamp(3, gen.n("user"))
}

/// Games of each viewing user, in collection display order (sort_index ASC).
pub fn games_of_view_users(gen: &Gen) -> BTreeMap<u64, Vec<u64>> {
    let users = view_users(gen);
    let mut out: BTreeMap<u64, Vec<(i64, u64)>> = BTreeMap::new();
    for g in 0..gen.n("game") {
        let owner = gen.game_owner(g);
        if owner < users {
            // the main collection only: what the windows page through
            if gen.game_database(g, owner) == owner {
                out.entry(owner).or_default().push((-gen.game_date(g), g));
            }
        }
    }
    out.into_iter()
        .map(|(u, mut v)| {
            v.sort();
            (u, v.into_iter().map(|(_, g)| g).collect())
        })
        .collect()
}

pub fn plan_views(gen: &Gen) -> Vec<View> {
    let users = view_users(gen);
    let games = games_of_view_users(gen);
    let n_notif = gen.n("notification").max(1);
    let n_analysis = gen.n("analysis");
    let mut views = Vec::new();
    for u in 0..users {
        let mine = games.get(&u).cloned().unwrap_or_default();
        let count = (300 / (u + 1)).max(8);
        let mut big = 0;
        for v in 0..count {
            let mut rng = Rng::of("view", u * 10_000 + v, "kind");
            let vkey = format!(
                "{:016x}{:016x}",
                crate::gen::mix(u * 1_000_003 + v),
                crate::gen::mix(v * 7 + u)
            );
            let client_id = format!(
                "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
                rng.next() as u32,
                rng.next() as u16,
                rng.next() & 0xfff,
                rng.next() & 0xfff,
                rng.next() & 0xffff_ffff_ffff
            );
            let r = rng.unit();
            let mut edges = Vec::new();
            let (surql, params);
            if r < 0.05 && !mine.is_empty() && big < 2 {
                big += 1;
                // the whole library ("All games" without a window)
                surql = "SELECT id, date, white, black, result, eco, sort_index FROM game WHERE owner = $auth.id".to_string();
                params = format!(
                    "{{ auth: {{ id: {} }}, access: 'account' }}",
                    rid("user", u)
                );
                for &g in mine.iter().take(40_000) {
                    edges.push((rid("game", g), None));
                }
            } else if r < 0.65 && !mine.is_empty() {
                // a 50-row window; popular offsets near the top
                let start = 50 * (rng.below(64).pow(2) / 64).min((mine.len() as u64) / 50);
                let with_players = rng.chance(0.5);
                surql = if with_players {
                    "SELECT id, date, result, sort_index, (SELECT id, name FROM player_name WHERE id = $parent.white) AS white, (SELECT id, name FROM player_name WHERE id = $parent.black) AS black FROM game WHERE database = $database ORDER BY sort_index ASC LIMIT 50 START $start".to_string()
                } else {
                    "SELECT id, date, white, black, result, sort_index FROM game WHERE database = $database ORDER BY sort_index ASC LIMIT 50 START $start".to_string()
                };
                params = format!(
                    "{{ auth: {{ id: {} }}, access: 'account', database: {}, start: {start} }}",
                    rid("user", u),
                    rid("game_database", u)
                );
                for &g in mine.iter().skip(start as usize).take(50) {
                    let game = rid("game", g);
                    edges.push((game.clone(), None));
                    if with_players {
                        let per_user = (gen.n("player_name") / gen.n("user").max(1)).max(1);
                        let mut prng = Rng::of("game", g, "players");
                        for alias in ["white", "black"] {
                            let p = (u + gen.n("user") * prng.below(per_user))
                                .min(gen.n("player_name") - 1);
                            edges.push((rid("player_name", p), Some((game.clone(), alias))));
                        }
                    }
                }
            } else if r < 0.85 {
                surql = "SELECT * FROM notification WHERE recipient = $auth.id ORDER BY created_ms DESC LIMIT 100".to_string();
                params = format!(
                    "{{ auth: {{ id: {} }}, access: 'account' }}",
                    rid("user", u)
                );
                let base = rng.below(n_notif);
                for i in 0..(20 + rng.below(80)) {
                    edges.push((rid("notification", (base + i * 997) % n_notif), None));
                }
            } else if n_analysis > 0 && !mine.is_empty() {
                let g = mine[rng.below(mine.len() as u64) as usize];
                surql =
                    "SELECT * FROM analysis WHERE game = $game AND owner = $auth.id".to_string();
                params = format!(
                    "{{ auth: {{ id: {} }}, access: 'account', game: {} }}",
                    rid("user", u),
                    rid("game", g)
                );
                let block = rng.below(n_analysis / 40);
                for i in 0..40 {
                    edges.push((rid("analysis", (block * 40 + i).min(n_analysis - 1)), None));
                }
            } else {
                continue;
            }
            views.push(View {
                key: vkey,
                user: Some(u),
                surql,
                params,
                client_id,
                edges,
                last_active_secs_ago: rng.range(0, 7200),
            });
        }
    }
    // anonymous viewers of public collections
    let anon = ((60.0 * gen.plan.scale).round() as u64).max(5);
    for a in 0..anon {
        let mut rng = Rng::of("anon_view", a, "kind");
        let owner = rng.below(users);
        let Some(mine) = games.get(&owner) else {
            continue;
        };
        let vkey = format!(
            "{:016x}{:016x}",
            crate::gen::mix(a ^ 0xA11),
            crate::gen::mix(a + 99)
        );
        let edges = mine
            .iter()
            .take(50)
            .map(|&g| (rid("game", g), None))
            .collect();
        views.push(View {
            key: vkey,
            user: None,
            surql: "SELECT id, date, white, black, result, sort_index FROM game WHERE database = $database ORDER BY sort_index ASC LIMIT 50".into(),
            params: format!("{{ database: {} }}", rid("game_database", owner)),
            client_id: format!("anon-{a:08x}-0000-4000-8000-000000000000"),
            edges,
            last_active_secs_ago: rng.range(0, 7200),
        });
    }
    views
}

/// `_00_query` row for a view.
pub fn query_row(v: &View) -> String {
    let last = NOW_SECS - v.last_active_secs_ago;
    format!(
        "{{id:{id},auth_id:{auth},clientId:{cid},createdAt:{created},errorCount:0,lastActiveAt:{last},params:{params},rowCount:{rows},state:'ready',subscribers:[{{id:{cid},seenAt:{last}}}],surql:{surql},ttl:1h,updateCount:{upd},registrationTime:12.5f}}",
        id = v.query_rid(),
        auth = q(&v.auth_id()),
        cid = q(&v.client_id),
        created = dt(last - 86_400),
        last = dt(last),
        params = v.params,
        rows = v.edges.iter().filter(|e| e.1.is_none()).count(),
        surql = q(&v.surql),
        upd = v.edges.len(),
    )
}

/// Edge rows of a view, as `INSERT RELATION` objects. Child edges point at
/// their parent row's edge.
pub fn edge_rows(v: &View) -> Vec<String> {
    let table = v.table();
    let last = NOW_SECS - v.last_active_secs_ago;
    let mut parent_edge: BTreeMap<String, String> = BTreeMap::new();
    let mut out = Vec::with_capacity(v.edges.len());
    for (i, (record, child_of)) in v.edges.iter().enumerate() {
        let eid = format!("{}{:06}", &v.key[..14], i);
        let id = format!("{table}:⟨{eid}⟩");
        let mut rng = Rng::of(&v.key, i as u64, "edge");
        let mut row = format!(
            "{{id:{id},in:{from},out:{record},clientId:{cid},auth_id:{auth},version:{ver},updatedAt:{at}",
            from = v.query_rid(),
            cid = q(&v.client_id),
            auth = q(&v.auth_id()),
            ver = 1 + rng.below(12),
            at = dt(last - rng.range(0, 86_400)),
        );
        match child_of {
            Some((parent, alias)) => {
                if let Some(p) = parent_edge.get(parent) {
                    row.push_str(&format!(",parent:{p},parent_rel:{}", q(alias)));
                }
            }
            None => {
                parent_edge.insert(record.clone(), id.clone());
            }
        }
        row.push('}');
        out.push(row);
    }
    out
}
