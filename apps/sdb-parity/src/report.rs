//! The Markdown report: one table per workload group with p50/p95 per server,
//! the 4.0.1/3.3.5 ratios, before and after the fixes, the plans of anything
//! more than 2x slower, the mixed load's CPU and the servers' slow logs.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use anyhow::{Context, Result};

use crate::workloads::{TargetResult, WorkloadResult};

const TARGETS: [(&str, &str); 3] = [
    ("335", "3.3.5"),
    ("401f", "4.0.1 fresh"),
    ("401c", "4.0.1 converted"),
];

fn ms(v: f64) -> String {
    if v >= 10_000.0 {
        format!("{:.1} s", v / 1000.0)
    } else if v >= 100.0 {
        format!("{v:.0}")
    } else if v >= 10.0 {
        format!("{v:.1}")
    } else {
        format!("{v:.2}")
    }
}

/// A workload id without its trailing row count (`sched.clone_page.x.1000`
/// pages 1,000 rows; the count adapts to how slow a server is), and the count.
fn split_rows(id: &str) -> (String, Option<f64>) {
    match id.rsplit_once('.') {
        Some((stem, n)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => {
            (stem.to_string(), n.parse().ok())
        }
        _ => (id.to_string(), None),
    }
}

fn cell(w: Option<&WorkloadResult>) -> String {
    match w {
        None => "-".into(),
        Some(w) if w.ms.is_empty() && w.p50 == 0.0 => "-".into(),
        Some(w) => {
            let err = if w.errors > 0 {
                format!(" ({} err)", w.errors)
            } else {
                String::new()
            };
            let rows = match split_rows(&w.id).1 {
                Some(n) => format!(" ({n} rows)"),
                None => String::new(),
            };
            format!("{} / {}{rows}{err}", ms(w.p50), ms(w.p95))
        }
    }
}

/// `b` over `a` by p50, per row when both page rows.
fn ratio(a: Option<&WorkloadResult>, b: Option<&WorkloadResult>) -> Option<f64> {
    let (a, b) = (a?, b?);
    let (ra, rb) = (split_rows(&a.id).1.unwrap_or(1.0), split_rows(&b.id).1.unwrap_or(1.0));
    (a.p50 > 0.0 && b.p50 > 0.0).then(|| (b.p50 / rb) / (a.p50 / ra))
}

fn fmt_ratio(r: Option<f64>) -> String {
    match r {
        None => "-".into(),
        Some(r) if r >= 2.0 => format!("**{r:.1}x**"),
        Some(r) if r < 0.095 => format!("{r:.2}x"),
        Some(r) => format!("{r:.1}x"),
    }
}

pub fn load(out: &Path, labels: &[String]) -> Result<Vec<TargetResult>> {
    let mut all: Vec<TargetResult> = Vec::new();
    for label in labels {
        let path = out.join("results").join(format!("{label}.json"));
        let text = std::fs::read_to_string(&path).with_context(|| format!("{}", path.display()))?;
        let rs: Vec<TargetResult> =
            serde_json::from_str(&text).with_context(|| format!("{}", path.display()))?;
        // a later label's results replace an earlier run's for the same server,
        // workload by workload and phase by phase (a `--only mixed` re-run
        // replaces just the mixed load)
        for r in rs {
            let Some(x) = all.iter_mut().find(|x| x.target == r.target) else {
                all.push(r);
                continue;
            };
            x.server = r.server;
            for w in r.workloads {
                x.workloads
                    .retain(|o| !(o.phase == w.phase && o.id == w.id));
                x.workloads.push(w);
            }
            for (phase, m) in r.mixed {
                x.mixed.retain(|(p, _)| p != &phase);
                x.mixed.push((phase, m));
            }
            if !r.slow_log.is_empty() && x.slow_log.is_empty() {
                x.slow_log = r.slow_log;
            }
        }
    }
    Ok(all)
}

pub fn render(out: &Path, labels: &[String]) -> Result<()> {
    let results = load(out, labels)?;
    let md = markdown(&results, labels);
    let path = out.join("sdb4-parity-results.md");
    std::fs::write(&path, md)?;
    eprintln!("report: {}", path.display());
    Ok(())
}

/// index: (phase, workload id) -> target key -> result
type Index<'a> = BTreeMap<(String, String), BTreeMap<String, &'a WorkloadResult>>;

pub fn markdown(results: &[TargetResult], labels: &[String]) -> String {
    let mut idx: Index = BTreeMap::new();
    let mut order: Vec<(String, String, String)> = Vec::new(); // group, id, source
    for t in results {
        for w in &t.workloads {
            let stem = split_rows(&w.id).0;
            let key = (w.phase.clone(), stem.clone());
            if !order.iter().any(|(_, id, _)| id == &stem) {
                order.push((w.group.clone(), stem, w.source.clone()));
            }
            idx.entry(key).or_default().insert(t.target.clone(), w);
        }
    }
    let get = |phase: &str, id: &str, t: &str| {
        idx.get(&(phase.to_string(), id.to_string()))
            .and_then(|m| m.get(t).copied())
    };
    let has_after = results
        .iter()
        .any(|t| t.workloads.iter().any(|w| w.phase == "after"));
    let mut s = String::new();
    let _ = writeln!(s, "# sdb-parity results\n");
    let _ = writeln!(
        s,
        "Generated by `sdb-parity report` from `results/{}.json`. Milliseconds, p50 / p95. \
         `before`: the dataset as deployed (WhitePawn's schema, sp00ky's production internal schema, \
         the legacy ledger). `after`: this branch's internal schema, the WhitePawn patch \
         (`whitepawn-events-4x.surql`) and the ledger migration applied. Ratios are 4.0.1 p50 over 3.3.5 p50 \
         in the same phase (per row for pages, whose row count adapts to the server's speed); \
         **bold** is 2x or more.\n",
        labels.join(", ")
    );
    let _ = writeln!(s, "| server | version reported |\n| --- | --- |");
    for t in results {
        let _ = writeln!(s, "| {} | {} |", t.label, t.server);
    }
    let mut groups: Vec<String> = order.iter().map(|o| o.0.clone()).collect();
    groups.dedup();
    let mut seen = std::collections::BTreeSet::new();
    groups.retain(|g| seen.insert(g.clone()));
    let phases: Vec<&str> = if has_after {
        vec!["before", "after"]
    } else {
        vec!["before"]
    };
    for g in &groups {
        let _ = writeln!(s, "\n## {g}\n");
        let mut head = String::from("| workload |");
        let mut rule = String::from("| --- |");
        for p in &phases {
            for (_, l) in TARGETS {
                let _ = write!(head, " {l} {p} |");
                rule.push_str(" ---: |");
            }
            let _ = write!(head, " fresh / 3.3.5 {p} | converted / 3.3.5 {p} |");
            rule.push_str(" ---: | ---: |");
        }
        let _ = writeln!(s, "{head}\n{rule}");
        for (_, id, _) in order.iter().filter(|o| &o.0 == g) {
            let mut row = format!("| `{id}` |");
            for p in &phases {
                for (k, _) in TARGETS {
                    let _ = write!(row, " {} |", cell(get(p, id, k)));
                }
                let _ = write!(
                    row,
                    " {} | {} |",
                    fmt_ratio(ratio(get(p, id, "335"), get(p, id, "401f"))),
                    fmt_ratio(ratio(get(p, id, "335"), get(p, id, "401c")))
                );
            }
            let _ = writeln!(s, "{row}");
        }
        let _ = writeln!(s, "\n<details><summary>sources and statements</summary>\n");
        for (_, id, source) in order.iter().filter(|o| &o.0 == g) {
            let sql = phases
                .iter()
                .flat_map(|p| TARGETS.iter().map(move |(k, _)| (p, k)))
                .find_map(|(p, k)| get(p, id, k))
                .map(|w| w.sql.clone())
                .unwrap_or_default();
            let _ = writeln!(
                s,
                "- `{id}`: {source}\n\n  ```surql\n  {}\n  ```",
                sql.replace('\n', "\n  ")
            );
        }
        let _ = writeln!(s, "\n</details>");
    }
    // plans of anything 2x slower on a 4.0.1 server
    let _ = writeln!(s, "\n## Plans of what is 2x or more slower on 4.0.1\n");
    let mut any = false;
    for (_, id, _) in &order {
        for p in &phases {
            for k in ["401f", "401c"] {
                let r = ratio(get(p, id, "335"), get(p, id, k));
                if r.map_or(false, |r| r >= 2.0) {
                    let w = get(p, id, k).unwrap();
                    let base = get(p, id, "335").unwrap();
                    any = true;
                    let _ = writeln!(
                        s,
                        "### `{id}` ({p}, {k}: {} against {} on 3.3.5)\n",
                        ms(w.p50),
                        ms(base.p50)
                    );
                    if let Some(e) = &w.explain {
                        let _ = writeln!(
                            s,
                            "4.0.1:\n\n```json\n{}\n```\n",
                            serde_json::to_string(e).unwrap_or_default()
                        );
                    }
                    if let Some(e) = &base.explain {
                        let _ = writeln!(
                            s,
                            "3.3.5:\n\n```json\n{}\n```\n",
                            serde_json::to_string(e).unwrap_or_default()
                        );
                    }
                    if w.explain.is_none() {
                        let _ = writeln!(s, "(no top-level plan: the statement runs inside a block, an event or a function)\n");
                    }
                    if let Some(e) = &w.first_error {
                        let _ = writeln!(s, "error: {e}\n");
                    }
                }
            }
        }
    }
    if !any {
        let _ = writeln!(s, "None.");
    }
    // mixed load
    let _ = writeln!(
        s,
        "\n## Mixed load: server CPU (100% = one core, cap 300%)\n"
    );
    let _ = writeln!(s, "| server | phase | seconds | requests | errors | CPU mean | CPU p95 | CPU max | memory max | busy after |\n| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for t in results {
        for (p, m) in &t.mixed {
            let _ = writeln!(
                s,
                "| {} | {p} | {} | {} | {} | {:.0}% | {:.0}% | {:.0}% | {:.0} MiB | {} |",
                t.label,
                m.secs,
                m.requests,
                m.errors,
                m.cpu_mean,
                m.cpu_p95,
                m.cpu_max,
                m.mem_max_mib,
                match m.drain_secs {
                    Some(d) if d >= 899.0 => "over 15 min".to_string(),
                    Some(d) => format!("{d:.0} s"),
                    None => "-".to_string(),
                }
            );
        }
    }
    let failed: Vec<String> = results
        .iter()
        .flat_map(|t| {
            t.mixed.iter().flat_map(move |(p, m)| {
                m.errors_by_task.iter().map(move |(task, (n, e))| {
                    format!(
                        "| {} | {p} | {task} | {n} | `{}` |",
                        t.label,
                        e.replace('|', "\\|").replace('`', "'")
                    )
                })
            })
        })
        .collect();
    if !failed.is_empty() {
        let _ = writeln!(s, "\nErrors:\n\n| server | phase | task | errors | first |\n| --- | --- | --- | ---: | --- |\n{}", failed.join("\n"));
    }
    let _ = writeln!(s, "\nPer task, p50 / p95 ms (count):\n");
    let mut tasks: Vec<String> = results
        .iter()
        .flat_map(|t| t.mixed.iter().flat_map(|(_, m)| m.per_task.keys().cloned()))
        .collect();
    tasks.sort();
    tasks.dedup();
    let mut head = String::from("| task |");
    let mut rule = String::from("| --- |");
    let cols: Vec<(String, String)> = results
        .iter()
        .flat_map(|t| {
            t.mixed
                .iter()
                .map(move |(p, _)| (t.target.clone(), p.clone()))
        })
        .collect();
    for (t, p) in &cols {
        let label = TARGETS.iter().find(|x| x.0 == t).map(|x| x.1).unwrap_or(t);
        let _ = write!(head, " {label} {p} |");
        rule.push_str(" ---: |");
    }
    let _ = writeln!(s, "{head}\n{rule}");
    for task in &tasks {
        let mut row = format!("| {task} |");
        for (t, p) in &cols {
            let m = results
                .iter()
                .find(|x| &x.target == t)
                .and_then(|x| x.mixed.iter().find(|(pp, _)| pp == p))
                .map(|x| &x.1);
            match m.and_then(|m| m.per_task.get(task)) {
                Some((n, p50, p95)) => {
                    let _ = write!(row, " {} / {} ({n}) |", ms(*p50), ms(*p95));
                }
                None => row.push_str(" - |"),
            }
        }
        let _ = writeln!(s, "{row}");
    }
    // slow logs
    let _ = writeln!(
        s,
        "\n## Slow query log (statements over 200 ms, by total time)\n"
    );
    for t in results {
        let _ = writeln!(
            s,
            "### {}\n\n| count | total | statement |\n| ---: | ---: | --- |",
            t.label
        );
        for (q, n, total) in t.slow_log.iter().take(15) {
            let _ = writeln!(
                s,
                "| {n} | {} | `{}` |",
                ms(*total),
                q.replace('|', "\\|").replace('`', "'")
            );
        }
        let _ = writeln!(s);
    }
    s
}
