//! `spky workflows` — inspect and drive workflow DAGs.
//!
//! `show` renders a run (or a definition) as a static diagram; `watch` renders the
//! same diagram live in a ratatui full-screen view. Both go through
//! [`crate::dag_render`], so what you see while watching is exactly what you get
//! when you paste `show` output into a ticket.
//!
//! Reads are plain SELECTs. The one mutation, `kill`, sets `kill_requested` on the
//! run and lets the engine do the work — run and step status stay engine-owned, so
//! the CLI can never leave a run in a state the engine disagrees with.

use std::io::{IsTerminal, Stdout};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use serde_json::Value;

use crate::dag_render::{self, DagNode, NodeStatus, RenderOpts};
use crate::surreal_client::{MigrationDB, SurrealClient};
use crate::{ConnectionArgs, WorkflowsCommands};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn run(
    conn: ConnectionArgs,
    config: Option<PathBuf>,
    action: WorkflowsCommands,
) -> Result<()> {
    let client = client_from(&conn, &config)?;
    match action {
        WorkflowsCommands::List { json } => list(&client, json),
        WorkflowsCommands::Show { target, ascii, json } => show(&client, &target, ascii, json),
        WorkflowsCommands::Watch { target } => watch(&client, &target),
        WorkflowsCommands::Runs { name, status, limit, json } => {
            runs(&client, name.as_deref(), status.as_deref(), limit, json)
        }
        WorkflowsCommands::Kill { run } => kill(&client, &run),
    }
}

fn client_from(conn: &ConnectionArgs, config: &Option<PathBuf>) -> Result<SurrealClient> {
    let c = conn.resolve(config)?;
    Ok(c.client())
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

fn result_rows(result: Option<&Value>) -> Vec<Value> {
    match result {
        Some(Value::Array(arr)) => arr.clone(),
        Some(other) => vec![other.clone()],
        None => vec![],
    }
}

fn query_rows(client: &SurrealClient, sql: &str) -> Result<Vec<Value>> {
    let responses = client.execute(sql).context("query failed")?;
    Ok(responses
        .into_iter()
        .next()
        .and_then(|r| r.result)
        .map(|r| result_rows(Some(&r)))
        .unwrap_or_default())
}

fn str_field(row: &Value, key: &str) -> String {
    row.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

// =============================================================
// Data model
// =============================================================

/// A workflow run plus its steps — everything both views need.
struct RunView {
    id: String,
    workflow_name: String,
    status: String,
    started_at: String,
    finished_at: Option<String>,
    error: Option<Value>,
    steps: Vec<StepView>,
}

struct StepView {
    step: String,
    depends_on: Vec<String>,
    status: String,
    job_id: Option<String>,
    output: Option<Value>,
    error: Option<Value>,
    finished_at: Option<String>,
}

impl RunView {
    fn nodes(&self) -> Vec<DagNode> {
        self.steps
            .iter()
            .map(|s| DagNode {
                name: s.step.clone(),
                depends_on: s.depends_on.clone(),
                status: NodeStatus::parse(&s.status),
                detail: s.detail(),
            })
            .collect()
    }

    fn elapsed(&self) -> String {
        let Ok(start) = chrono::DateTime::parse_from_rfc3339(&self.started_at) else {
            return String::new();
        };
        let end = self
            .finished_at
            .as_deref()
            .and_then(|f| chrono::DateTime::parse_from_rfc3339(f).ok())
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        fmt_duration((end - start.with_timezone(&chrono::Utc)).num_seconds())
    }
}

impl StepView {
    /// The line under a step's name: how long it took, or why it failed.
    fn detail(&self) -> Option<String> {
        if let Some(error) = &self.error {
            let reason = error.get("reason").and_then(Value::as_str).unwrap_or("failed");
            return Some(truncate(reason, 20));
        }
        match self.status.as_str() {
            "dispatched" => Some("running…".to_string()),
            "success" | "failed" => self.finished_at.as_ref().map(|_| "done".to_string()),
            _ => None,
        }
    }
}

fn load_run(client: &SurrealClient, run_id: &str) -> Result<RunView> {
    let rows = query_rows(
        client,
        &format!(
            "SELECT type::string(id) AS id, workflow_name, status, error, \
             type::string(created_at) AS created_at, type::string(finished_at) AS finished_at \
             FROM _00_workflow_run WHERE type::string(id) = '{}' LIMIT 1;",
            esc(run_id)
        ),
    )?;
    let run = rows.into_iter().next().ok_or_else(|| {
        anyhow!(
            "No workflow run '{run_id}'.\n\nA successful run leaves no rows behind on a \
             schedule with `history: failures-only` — check `spky schedules get <name>` for \
             the totals, which survive the rows."
        )
    })?;

    let step_rows = query_rows(
        client,
        &format!(
            "SELECT step, depends_on, status, job_id, output, error, \
             type::string(finished_at) AS finished_at \
             FROM _00_step_run WHERE type::string(workflow_run) = '{}';",
            esc(run_id)
        ),
    )?;

    let steps = step_rows
        .iter()
        .map(|row| StepView {
            step: str_field(row, "step"),
            depends_on: row
                .get("depends_on")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                .unwrap_or_default(),
            status: str_field(row, "status"),
            job_id: row.get("job_id").and_then(Value::as_str).map(str::to_string),
            output: row.get("output").cloned().filter(|v| !v.is_null()),
            error: row.get("error").cloned().filter(|v| !v.is_null()),
            finished_at: row.get("finished_at").and_then(Value::as_str).map(str::to_string),
        })
        .collect();

    Ok(RunView {
        id: str_field(&run, "id"),
        workflow_name: str_field(&run, "workflow_name"),
        status: str_field(&run, "status"),
        started_at: str_field(&run, "created_at"),
        finished_at: run.get("finished_at").and_then(Value::as_str).map(str::to_string),
        error: run.get("error").cloned().filter(|v| !v.is_null()),
        steps,
    })
}

/// Resolve `<name|run-id>` to a run: an explicit record id, or the newest run of
/// a named workflow.
fn resolve_run(client: &SurrealClient, target: &str) -> Result<RunView> {
    if target.starts_with("_00_workflow_run:") {
        return load_run(client, target);
    }
    let rows = query_rows(
        client,
        &format!(
            "SELECT type::string(id) AS id, type::string(created_at) AS created_at \
             FROM _00_workflow_run WHERE workflow_name = '{}' ORDER BY created_at DESC LIMIT 1;",
            esc(target)
        ),
    )?;
    let id = rows
        .into_iter()
        .next()
        .map(|r| str_field(&r, "id"))
        .ok_or_else(|| anyhow!("No runs yet for workflow '{target}'"))?;
    load_run(client, &id)
}

// =============================================================
// `spky workflows list`
// =============================================================

fn list(client: &SurrealClient, json: bool) -> Result<()> {
    let rows = query_rows(
        client,
        "SELECT name, cron, every_ms, paused, config_disabled, \
         type::string(next_fire_at) AS next_fire_at \
         FROM _00_schedule WHERE kind = 'workflow' ORDER BY name;",
    )?;
    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("{DIM}No workflows deployed. Declare them under `workflows:` in sp00ky.yml.{RESET}");
        return Ok(());
    }

    println!("{BOLD}{:<24} {:<14} {:<9} {:<20}{RESET}", "NAME", "CADENCE", "STATE", "NEXT FIRE");
    for row in &rows {
        let cadence = match (row.get("cron").and_then(Value::as_str), row.get("every_ms").and_then(Value::as_i64)) {
            (Some(cron), _) => cron.to_string(),
            (None, Some(ms)) => format!("every {}s", ms / 1000),
            _ => "trigger-only".to_string(),
        };
        let state = if row.get("paused").and_then(Value::as_bool) == Some(true) {
            "paused"
        } else if row.get("config_disabled").and_then(Value::as_bool) == Some(true) {
            "disabled"
        } else {
            "active"
        };
        println!(
            "{:<24} {:<14} {:<9} {:<20}",
            truncate(&str_field(row, "name"), 24),
            truncate(&cadence, 14),
            state,
            row.get("next_fire_at").and_then(Value::as_str).unwrap_or("-"),
        );
    }
    Ok(())
}

// =============================================================
// `spky workflows show`
// =============================================================

fn show(client: &SurrealClient, target: &str, ascii: bool, json: bool) -> Result<()> {
    let run = resolve_run(client, target)?;
    if json {
        let steps: Vec<Value> = run
            .steps
            .iter()
            .map(|s| {
                serde_json::json!({
                    "step": s.step,
                    "dependsOn": s.depends_on,
                    "status": s.status,
                    "jobId": s.job_id,
                    "output": s.output,
                    "error": s.error,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "id": run.id,
                "workflow": run.workflow_name,
                "status": run.status,
                "startedAt": run.started_at,
                "finishedAt": run.finished_at,
                "error": run.error,
                "steps": steps,
            }))?
        );
        return Ok(());
    }

    // Piped output gets plain ASCII by default — box-drawing characters mangle
    // in logs and tickets.
    let ascii = ascii || !std::io::stdout().is_terminal();
    let width = terminal_width();

    let (icon, color) = status_style(&run.status);
    println!(
        "{BOLD}{}{RESET}  {color}{icon} {}{RESET}  {DIM}{}{RESET}",
        run.workflow_name, run.status, run.id
    );
    println!("{DIM}elapsed {}{RESET}\n", run.elapsed());

    for line in dag_render::render(&run.nodes(), &RenderOpts { ascii, width, selected: None }) {
        println!("{line}");
    }
    println!("\n{DIM}{}{RESET}", dag_render::legend(ascii));

    if let Some(error) = &run.error {
        println!("\n{RED}error{RESET}: {}", serde_json::to_string(error)?);
    }
    let failed: Vec<&StepView> = run.steps.iter().filter(|s| s.status == "failed").collect();
    for step in failed {
        if let Some(error) = &step.error {
            println!("  {RED}{}{RESET}: {}", step.step, serde_json::to_string(error)?);
        }
    }
    Ok(())
}

// =============================================================
// `spky workflows runs`
// =============================================================

fn runs(
    client: &SurrealClient,
    name: Option<&str>,
    status: Option<&str>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let mut filters = Vec::new();
    if let Some(name) = name {
        filters.push(format!("workflow_name = '{}'", esc(name)));
    }
    if let Some(status) = status {
        filters.push(format!("status = '{}'", esc(status)));
    }
    let where_clause =
        if filters.is_empty() { String::new() } else { format!("WHERE {}", filters.join(" AND ")) };

    let rows = query_rows(
        client,
        &format!(
            "SELECT type::string(id) AS id, workflow_name, status, \
             type::string(created_at) AS created_at, type::string(finished_at) AS finished_at \
             FROM _00_workflow_run {where_clause} ORDER BY created_at DESC LIMIT {limit};"
        ),
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("{DIM}No workflow runs recorded.{RESET}");
        return Ok(());
    }

    println!("{BOLD}{:<22} {:<10} {:<20} {:<34}{RESET}", "WORKFLOW", "STATUS", "STARTED", "RUN");
    for row in &rows {
        let status = str_field(row, "status");
        let (icon, color) = status_style(&status);
        let started = str_field(row, "created_at");
        println!(
            "{:<22} {color}{} {:<8}{RESET} {:<20} {DIM}{}{RESET}",
            truncate(&str_field(row, "workflow_name"), 22),
            icon,
            status,
            &started[..19.min(started.len())],
            str_field(row, "id"),
        );
    }
    println!("\n{DIM}spky workflows show <run> for the DAG{RESET}");
    Ok(())
}

// =============================================================
// `spky workflows kill`
// =============================================================

fn kill(client: &SurrealClient, target: &str) -> Result<()> {
    let run = resolve_run(client, target)?;
    if run.status != "running" {
        bail!("Run {} is already {} — nothing to kill.", run.id, run.status);
    }
    // Only the flag: the engine skips what hasn't started, kills the jobs of what
    // has, and terminalizes the run. Writing status here would race it.
    client
        .execute(&format!("UPDATE {} SET kill_requested = true;", run.id))
        .with_context(|| format!("failed to request a kill for {}", run.id))?;
    println!("{YELLOW}Kill requested{RESET} for {BOLD}{}{RESET}", run.id);
    println!("{DIM}The engine stops in-flight steps and skips the rest within a few seconds.{RESET}");
    Ok(())
}

// =============================================================
// `spky workflows watch` — live DAG
// =============================================================

const POLL: Duration = Duration::from_millis(1000);

fn watch(client: &SurrealClient, target: &str) -> Result<()> {
    if !std::io::stdout().is_terminal() {
        // No TTY to draw on; the static view is the honest answer.
        return show(client, target, true, false);
    }
    let mut run = resolve_run(client, target)?;

    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = watch_loop(&mut terminal, client, target, &mut run);

    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    // Say what happened AFTER leaving the alternate screen, or the message is drawn on a
    // buffer that is about to be discarded.
    if matches!(result?, WatchOutcome::RunGone) {
        println!(
            "{DIM}Run finished and was discarded (`history: failures-only`). \
             `spky schedules get` still has the totals.{RESET}"
        );
    }
    Ok(())
}

struct WatchState {
    selected: usize,
    /// Sticky: keep the selection on the same STEP as the DAG changes shape,
    /// rather than on an index that may come to mean a different step.
    selected_name: Option<String>,
    message: Option<String>,
}

/// Why the watch stopped. The run disappearing is a normal ending, not an error: it is
/// what a success looks like on a `history: failures-only` schedule.
enum WatchOutcome {
    Quit,
    RunGone,
}

fn watch_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    client: &SurrealClient,
    target: &str,
    run: &mut RunView,
) -> Result<WatchOutcome> {
    let mut state = WatchState { selected: 0, selected_name: None, message: None };
    let mut last_poll = Instant::now();

    loop {
        terminal.draw(|frame| draw(frame, run, &state))?;

        if event::poll(Duration::from_millis(120))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(WatchOutcome::Quit),
                    KeyCode::Down | KeyCode::Char('j') => {
                        if !run.steps.is_empty() {
                            state.selected = (state.selected + 1) % run.steps.len();
                            state.selected_name = Some(run.steps[state.selected].step.clone());
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if !run.steps.is_empty() {
                            state.selected = state
                                .selected
                                .checked_sub(1)
                                .unwrap_or(run.steps.len() - 1);
                            state.selected_name = Some(run.steps[state.selected].step.clone());
                        }
                    }
                    KeyCode::Char('K') => {
                        // Uppercase on purpose: killing a run should not be one
                        // stray keystroke away while you are navigating with `k`.
                        match client
                            .execute(&format!("UPDATE {} SET kill_requested = true;", run.id))
                        {
                            Ok(_) => state.message = Some("kill requested".into()),
                            Err(e) => state.message = Some(format!("kill failed: {e}")),
                        }
                    }
                    KeyCode::Char('r') => last_poll = Instant::now() - POLL,
                    _ => {}
                }
            }
        }

        if last_poll.elapsed() >= POLL {
            last_poll = Instant::now();
            match resolve_run(client, target) {
                Ok(fresh) => *run = fresh,
                // The run is gone. On a `history: failures-only` schedule that is exactly
                // what SUCCESS looks like, so swallowing this froze the TUI on the last
                // pre-success frame forever and never rendered an outcome. Watching by
                // NAME was worse: `resolve_run` takes the newest surviving run, so it
                // silently re-anchored to an older, failed one under the operator.
                Err(_) => return Ok(WatchOutcome::RunGone),
            }
            // Re-anchor the selection to the remembered step name.
            if let Some(name) = &state.selected_name {
                if let Some(i) = run.steps.iter().position(|s| &s.step == name) {
                    state.selected = i;
                }
            }
            if state.selected >= run.steps.len() {
                state.selected = 0;
            }
        }
    }
}

fn draw(frame: &mut Frame, run: &RunView, state: &WatchState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(9),
            Constraint::Length(2),
        ])
        .split(frame.area());

    // Header.
    let (icon, color) = ratatui_status_style(&run.status);
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            run.workflow_name.clone(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(format!("{icon} {}", run.status), Style::default().fg(color)),
        Span::raw("  "),
        Span::styled(
            format!("elapsed {}", run.elapsed()),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw("  "),
        Span::styled(run.id.clone(), Style::default().fg(Color::DarkGray)),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, chunks[0]);

    // DAG — the same renderer `show` uses, so the two views can't diverge.
    let selected_name = run.steps.get(state.selected).map(|s| s.step.clone());
    let lines = dag_render::render(
        &run.nodes(),
        &RenderOpts {
            ascii: false,
            width: chunks[1].width.saturating_sub(2) as usize,
            selected: selected_name.clone(),
        },
    );
    let dag = Paragraph::new(lines.join("\n"))
        .block(Block::default().borders(Borders::ALL).title(" dag "));
    frame.render_widget(dag, chunks[1]);

    // Selected step detail.
    let detail = match run.steps.get(state.selected) {
        Some(step) => {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled(step.step.clone(), Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw("  "),
                    Span::styled(step.status.clone(), Style::default().fg(ratatui_status_style(&step.status).1)),
                ]),
                Line::from(format!("job     : {}", step.job_id.as_deref().unwrap_or("-"))),
                Line::from(format!(
                    "depends : {}",
                    if step.depends_on.is_empty() { "-".to_string() } else { step.depends_on.join(", ") }
                )),
            ];
            if let Some(output) = &step.output {
                lines.push(Line::from(format!(
                    "output  : {}",
                    truncate(&serde_json::to_string(output).unwrap_or_default(), 200)
                )));
            }
            if let Some(error) = &step.error {
                lines.push(Line::styled(
                    format!(
                        "error   : {}",
                        truncate(&serde_json::to_string(error).unwrap_or_default(), 200)
                    ),
                    Style::default().fg(Color::Red),
                ));
            }
            Paragraph::new(lines).wrap(Wrap { trim: true })
        }
        None => Paragraph::new("no steps"),
    }
    .block(Block::default().borders(Borders::ALL).title(" step "));
    frame.render_widget(detail, chunks[2]);

    // Footer: legend + keys, plus whatever the last action said.
    let mut footer = vec![Line::from(Span::styled(
        dag_render::legend(false),
        Style::default().fg(Color::DarkGray),
    ))];
    let keys = "↑↓/jk select   r refresh   K kill run   q quit";
    footer.push(Line::from(Span::styled(
        match &state.message {
            Some(msg) => format!("{keys}   —   {msg}"),
            None => keys.to_string(),
        },
        Style::default().fg(Color::DarkGray),
    )));
    frame.render_widget(Paragraph::new(footer), chunks[3]);
}

// =============================================================
// Formatting
// =============================================================

fn status_style(status: &str) -> (&'static str, &'static str) {
    match status {
        "success" => ("✔", GREEN),
        "failed" => ("✖", RED),
        "running" => ("◐", YELLOW),
        "killed" => ("■", RED),
        _ => ("·", DIM),
    }
}

fn ratatui_status_style(status: &str) -> (&'static str, Color) {
    match status {
        "success" => ("✔", Color::Green),
        "failed" => ("✖", Color::Red),
        "running" | "dispatched" => ("◐", Color::Yellow),
        "killed" => ("■", Color::Red),
        "skipped" => ("⊘", Color::Magenta),
        _ => ("·", Color::DarkGray),
    }
}

fn fmt_duration(secs: i64) -> String {
    let secs = secs.max(0);
    if secs >= 3600 {
        format!("{}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
    } else {
        format!("{}:{:02}", secs / 60, secs % 60)
    }
}

fn terminal_width() -> usize {
    crossterm::terminal::size().map(|(w, _)| w as usize).unwrap_or(100)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    format!("{}…", s.chars().take(max.saturating_sub(1)).collect::<String>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn step(name: &str, deps: &[&str], status: &str) -> StepView {
        StepView {
            step: name.to_string(),
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
            status: status.to_string(),
            job_id: Some(format!("job:wf_{name}")),
            output: None,
            error: None,
            finished_at: None,
        }
    }

    fn run() -> RunView {
        RunView {
            id: "_00_workflow_run:⟨report_1_abc⟩".into(),
            workflow_name: "monthly-report".into(),
            status: "running".into(),
            started_at: "2026-07-25T09:00:00Z".into(),
            finished_at: None,
            error: None,
            steps: vec![
                step("extract-orders", &[], "success"),
                step("extract-users", &[], "success"),
                step("transform", &["extract-orders", "extract-users"], "dispatched"),
                step("notify", &["transform"], "blocked"),
            ],
        }
    }

    #[test]
    fn nodes_carry_dependencies_and_status_into_the_renderer() {
        let nodes = run().nodes();
        assert_eq!(nodes.len(), 4);
        let transform = nodes.iter().find(|n| n.name == "transform").unwrap();
        assert_eq!(transform.status, NodeStatus::Dispatched);
        assert_eq!(transform.depends_on, vec!["extract-orders", "extract-users"]);
        assert_eq!(transform.detail.as_deref(), Some("running…"));
    }

    #[test]
    fn a_failed_step_shows_its_reason_rather_than_a_duration() {
        let mut s = step("transform", &[], "failed");
        s.error = Some(json!({ "code": 500, "reason": "backend exploded" }));
        assert_eq!(s.detail().as_deref(), Some("backend exploded"));
    }

    #[test]
    fn elapsed_counts_to_finish_when_finished_and_to_now_while_running() {
        let mut r = run();
        r.finished_at = Some("2026-07-25T09:02:41Z".into());
        assert_eq!(r.elapsed(), "2:41");

        // Still running: measured against now, so it must be far larger than the
        // fixed 2:41 above (the start date is in the past).
        r.finished_at = None;
        assert_ne!(r.elapsed(), "2:41");
    }

    #[test]
    fn durations_gain_an_hours_field_only_when_needed() {
        assert_eq!(fmt_duration(41), "0:41");
        assert_eq!(fmt_duration(161), "2:41");
        assert_eq!(fmt_duration(3_661), "1:01:01");
        assert_eq!(fmt_duration(-5), "0:00", "clock skew must not render nonsense");
    }

    /// The static and live views must agree; both go through one renderer.
    #[test]
    fn show_and_watch_render_the_same_diagram() {
        let nodes = run().nodes();
        let opts = RenderOpts { ascii: false, width: 120, selected: None };
        let text = dag_render::render(&nodes, &opts).join("\n");
        assert!(text.contains("transform"));
        assert!(text.contains('◐'), "the running step is visible in the diagram");
    }
}
