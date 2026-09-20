//! Shared terminal UI for the CLI.
//!
//! One place for spinners, step lines, warnings, boxes and the colour palette so
//! `spky dev`, `spky migrate` and friends render the same way. Everything here
//! degrades automatically:
//!
//! - **TTY**: steps are spinner lines updated in place; finished steps become
//!   permanent `✓ Label   detail` lines; concurrent output (child-process logs)
//!   is routed through the `MultiProgress` so spinners are never corrupted.
//! - **non-TTY / piped / CI**: no animation, no `\r`, no ANSI. A step prints one
//!   line when it finishes (`ok   Label   detail`), notes print immediately.
//! - `NO_COLOR` (and `console`'s CLICOLOR handling) disables colour; glyphs fall
//!   back to ASCII when the terminal doesn't want emoji or `SPKY_ASCII=1`.
//!
//! The global state is lazily initialised so any command (and `cargo test`) can
//! call into this module without an explicit `init`.

use std::cell::Cell;
use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use console::{strip_ansi_codes, Style, Term};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Column width for step labels so details line up.
const LABEL_WIDTH: usize = 22;
/// Indent for step lines.
const INDENT: &str = "  ";
/// Lines kept per infra stream before startup completes (shown on failure).
const RING_CAPACITY: usize = 50;

// ── Global state ────────────────────────────────────────────────────────────

pub struct Options {
    pub verbose: bool,
}

struct Ui {
    mp: MultiProgress,
    tty: bool,
    ascii: bool,
    verbose: AtomicBool,
    ready: AtomicBool,
}

static UI: OnceLock<Ui> = OnceLock::new();

thread_local! {
    static SILENCE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

fn build(verbose: bool) -> Ui {
    let tty = std::io::stdout().is_terminal();
    let ascii = std::env::var_os("SPKY_ASCII").is_some()
        || !tty
        || !Term::stdout().features().wants_emoji();
    let mp = if tty {
        MultiProgress::with_draw_target(ProgressDrawTarget::stdout())
    } else {
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    };
    Ui {
        mp,
        tty,
        ascii,
        verbose: AtomicBool::new(verbose),
        ready: AtomicBool::new(false),
    }
}

fn ui() -> &'static Ui {
    UI.get_or_init(|| build(std::env::var_os("SPKY_VERBOSE").is_some()))
}

/// Initialise once from `main`. Later calls only update the verbose flag.
pub fn init(opts: Options) {
    let u = UI.get_or_init(|| build(opts.verbose));
    u.verbose.store(opts.verbose, Ordering::Relaxed);
}

pub fn is_verbose() -> bool {
    ui().verbose.load(Ordering::Relaxed)
}

pub fn is_tty() -> bool {
    ui().tty
}

fn is_silenced() -> bool {
    SILENCE_DEPTH.with(|d| d.get() > 0)
}

// ── Confirmation ────────────────────────────────────────────────────────────
//
// Every yes/no question the CLI asks goes through here, so that one flag
// (`-y/--yes`, or `SPKY_YES=1`) answers all of them the same way.
//
// There used to be 32 separate `inquire::Confirm` calls and four per-command
// `--yes` flags, each with its own idea of what to do without a terminal: some
// hung, some failed with "Failed to read confirmation", and two - the PRODUCTION
// migration gate among them - silently skipped the question and proceeded. An
// agent driving the CLI hit the worst of both: a pseudo-terminal makes
// `is_terminal()` true, so the prompt renders and then waits forever for a
// keypress nothing can send.
//
// The two kinds of question need different answers, which is why there are two
// functions rather than one flag meaning "yes to everything":
//
// - [`consent`]: "may I do this consequential thing?" `--yes` means yes.
// - [`prefer`]: "which way do you want it?" `--yes` means the DEFAULT, because
//   answering yes to "Set up billing now?" is a decision, not a confirmation.
//
// Two operations are NOT covered by `--yes` at all: see [`consent_naming`].

static ASSUME_YES: AtomicBool = AtomicBool::new(false);

/// Record the global `-y/--yes` flag. Called once from `main`.
pub fn set_assume_yes(yes: bool) {
    ASSUME_YES.store(yes, Ordering::Relaxed);
}

/// Whether confirmations are pre-answered: `-y/--yes`, or `SPKY_YES` set to
/// anything but `0`/`false`/empty (so CI can turn it on without editing commands).
pub fn assume_yes() -> bool {
    ASSUME_YES.load(Ordering::Relaxed)
        || std::env::var("SPKY_YES")
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "" | "0" | "false" | "no"))
            .unwrap_or(false)
}

fn stdin_is_terminal() -> bool {
    std::io::stdin().is_terminal()
}

/// Whether nobody is there to answer: no terminal on stdin, or `--yes`.
///
/// For the prompts that offer a CHOICE rather than a yes/no (`spky dev` asking
/// what to do about schema drift or pending migrations). Each of those already
/// has an unattended path for a piped stdin; `--yes` takes the same one, so a
/// pseudo-terminal - which makes `is_terminal()` true - no longer lands an agent
/// on a menu it cannot operate.
pub fn is_unattended() -> bool {
    !stdin_is_terminal() || assume_yes()
}

/// Ask permission for something consequential. Defaults to No.
///
/// - `--yes`: answers yes, and says so, so a log shows what was agreed to.
/// - a terminal: asks.
/// - neither: **refuses**, naming the flag. Proceeding unasked is how a
///   production migration ran without anyone approving it; hanging or a bare
///   "Failed to read confirmation" is how everything else failed.
pub fn consent(prompt: &str) -> anyhow::Result<bool> {
    decide(prompt, false, true, assume_yes(), stdin_is_terminal())
}

/// Ask a yes/no preference that has a sensible default.
///
/// `--yes` and a missing terminal both take `default`: neither is a reason to
/// stop, and neither is licence to pick the non-default on the user's behalf.
pub fn prefer(prompt: &str, default: bool) -> anyhow::Result<bool> {
    decide(prompt, default, false, assume_yes(), stdin_is_terminal())
}

/// Ask permission for something IRREVERSIBLE, by making the caller name it.
///
/// `--yes` deliberately does not cover these. It is the flag an agent adds to
/// every command so nothing blocks, which makes it exactly the wrong thing to
/// arm "destroy this project" or "wipe this database" with. The caller has to
/// pass `--confirm <name>` and get the name right, which cannot happen by reflex.
/// A person at a terminal is still simply asked.
pub fn consent_naming(prompt: &str, target: &str, confirmed: Option<&str>) -> anyhow::Result<bool> {
    match confirmed {
        Some(named) if named == target => {
            note_auto_answer(prompt, &format!("yes (--confirm {target})"));
            Ok(true)
        }
        Some(named) => anyhow::bail!(
            "--confirm {named} does not match '{target}'. Nothing was changed."
        ),
        None if stdin_is_terminal() && !assume_yes() => decide(prompt, false, true, false, true),
        None => anyhow::bail!(
            "{prompt}\n  This cannot be undone, so --yes does not cover it. \
             Re-run with `--confirm {target}` to go ahead."
        ),
    }
}

/// The decision table, split from the prompt so it can be tested without a terminal.
fn decide(
    prompt: &str,
    default: bool,
    is_consent: bool,
    yes: bool,
    terminal: bool,
) -> anyhow::Result<bool> {
    if yes {
        let answer = if is_consent { true } else { default };
        note_auto_answer(prompt, if answer { "yes (--yes)" } else { "no (--yes, default)" });
        return Ok(answer);
    }
    if !terminal {
        if is_consent {
            anyhow::bail!(
                "{prompt}\n  No terminal to ask on. Re-run with `-y/--yes` (or SPKY_YES=1) to confirm."
            );
        }
        note_auto_answer(prompt, if default { "yes (default)" } else { "no (default)" });
        return Ok(default);
    }
    suspend(|| {
        inquire::Confirm::new(prompt)
            .with_default(default)
            .prompt()
            .map_err(|e| anyhow::anyhow!("Failed to read confirmation: {e}"))
    })
}

fn note_auto_answer(prompt: &str, answer: &str) {
    println(format!("{INDENT}{prompt} {answer}"));
}

/// Mark startup as finished: clears any live spinner region and lets infra log
/// sinks start printing (quiet mode buffers them until now).
pub fn done_startup() {
    let u = ui();
    u.ready.store(true, Ordering::SeqCst);
    let _ = u.mp.clear();
}

/// RAII guard: while alive on this thread, `step`/`info`/`hint`/`detail` and
/// step notes emit nothing. `warn`/`error` still print.
pub struct SilenceGuard(());

pub fn silenced() -> SilenceGuard {
    SILENCE_DEPTH.with(|d| d.set(d.get() + 1));
    SilenceGuard(())
}

impl Drop for SilenceGuard {
    fn drop(&mut self) {
        SILENCE_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Run `f` with the spinner region hidden. Wrap every interactive prompt
/// (`inquire`) in this so the prompt owns the terminal.
pub fn suspend<R>(f: impl FnOnce() -> R) -> R {
    let u = ui();
    if u.tty {
        u.mp.suspend(f)
    } else {
        f()
    }
}

// ── Glyphs & palette ────────────────────────────────────────────────────────

pub struct Glyphs {
    pub ok: &'static str,
    pub fail: &'static str,
    pub skip: &'static str,
    pub warn: &'static str,
    pub bullet: &'static str,
    pub arrow: &'static str,
    pub ghost: &'static str,
    pub pending: &'static str,
    pub sep: &'static str,
}

const UNICODE_GLYPHS: Glyphs = Glyphs {
    ok: "✓",
    fail: "✗",
    skip: "↷",
    warn: "!",
    bullet: "·",
    arrow: "→",
    ghost: "👻",
    pending: "…",
    sep: " · ",
};

const ASCII_GLYPHS: Glyphs = Glyphs {
    ok: "ok",
    fail: "x",
    skip: "-",
    warn: "!",
    bullet: "*",
    arrow: ">",
    ghost: "~",
    pending: "..",
    sep: " - ",
};

pub fn glyphs() -> &'static Glyphs {
    if ui().ascii {
        &ASCII_GLYPHS
    } else {
        &UNICODE_GLYPHS
    }
}

pub struct Palette {
    pub ok: Style,
    pub fail: Style,
    pub warn: Style,
    pub dim: Style,
    pub bold: Style,
    pub accent: Style,
    pub app: Style,
    app_cycle: Vec<Style>,
}

impl Palette {
    /// Colour for an infra service log prefix (`surrealdb` / `ssp` / `scheduler`).
    pub fn infra(&self, label: &str) -> Style {
        match label {
            "surrealdb" => Style::new().color256(208),
            "ssp" => Style::new().color256(75),
            "scheduler" => Style::new().color256(213),
            _ => Style::new().color256(245),
        }
    }

    /// Distinguishable colour for the i-th user backend app.
    pub fn app_cycle(&self, i: usize) -> Style {
        self.app_cycle[i % self.app_cycle.len()].clone()
    }
}

static PALETTE: OnceLock<Palette> = OnceLock::new();

pub fn style() -> &'static Palette {
    PALETTE.get_or_init(|| Palette {
        ok: Style::new().green(),
        fail: Style::new().red(),
        warn: Style::new().yellow(),
        dim: Style::new().dim(),
        bold: Style::new().bold(),
        accent: Style::new().cyan(),
        app: Style::new().white().bright(),
        app_cycle: vec![
            Style::new().cyan(),
            Style::new().yellow(),
            Style::new().magenta(),
            Style::new().green(),
            Style::new().blue(),
            Style::new().red().bright(),
            Style::new().cyan().bright(),
            Style::new().yellow().bright(),
            Style::new().magenta().bright(),
            Style::new().green().bright(),
        ],
    })
}

// ── Plain output ────────────────────────────────────────────────────────────

/// Write one newline-terminated line. On a TTY the spinner region is cleared
/// around the write (`MultiProgress::suspend`) rather than going through
/// indicatif's own `println`, which pads every line to the terminal width and
/// relies on wrap-around instead of `\n` (fine on screen, a mess when copied).
fn write_line(line: &str, to_stderr: bool) {
    let write = || {
        if to_stderr {
            let stderr = std::io::stderr();
            let mut lock = stderr.lock();
            let _ = writeln!(lock, "{}", line);
            let _ = lock.flush();
        } else {
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            let _ = writeln!(lock, "{}", line);
            let _ = lock.flush();
        }
    };
    let u = ui();
    if u.tty {
        u.mp.suspend(write);
    } else {
        write();
    }
}

fn write_stdout(line: &str) {
    write_line(line, false);
}

fn write_stderr(line: &str) {
    write_line(line, true);
}

/// Print a line. Safe to call from any thread while spinners are live.
pub fn println(line: impl AsRef<str>) {
    write_stdout(line.as_ref());
}

/// Dim informational line (`· message`).
pub fn info(msg: impl AsRef<str>) {
    if is_silenced() {
        return;
    }
    let g = glyphs();
    let s = style();
    write_stdout(&format!(
        "{}{} {}",
        INDENT,
        s.dim.apply_to(g.bullet),
        s.dim.apply_to(msg.as_ref())
    ));
}

/// Hint for the user (dim, indented one level deeper than steps).
pub fn hint(msg: impl AsRef<str>) {
    if is_silenced() {
        return;
    }
    let s = style();
    write_stdout(&format!("{}  {}", INDENT, s.dim.apply_to(msg.as_ref())));
}

pub fn warn(msg: impl AsRef<str>) {
    let g = glyphs();
    let s = style();
    write_stderr(&format!(
        "{}{} {}",
        INDENT,
        s.warn.apply_to(g.warn),
        s.warn.apply_to(msg.as_ref())
    ));
}

pub fn error(msg: impl AsRef<str>) {
    let g = glyphs();
    let s = style();
    write_stderr(&format!(
        "{}{} {}",
        INDENT,
        s.fail.apply_to(g.fail),
        s.fail.apply_to(msg.as_ref())
    ));
}

/// Verbose-only dim detail line, indented under the current step.
pub fn detail(msg: impl AsRef<str>) {
    if is_silenced() || !is_verbose() {
        return;
    }
    let s = style();
    write_stdout(&format!("{}    {}", INDENT, s.dim.apply_to(msg.as_ref())));
}

/// Section header: `👻 title  ·  meta  ·  meta`.
pub fn header(title: &str, meta: &[&str]) {
    if is_silenced() {
        return;
    }
    let g = glyphs();
    let s = style();
    let mut line = String::new();
    if !ui().ascii {
        line.push_str(g.ghost);
        line.push(' ');
    }
    line.push_str(&s.bold.apply_to(title).to_string());
    for m in meta {
        line.push_str(&s.dim.apply_to(format!(" {} ", g.sep)).to_string());
        line.push_str(&s.dim.apply_to(*m).to_string());
    }
    write_stdout(&line);
    write_stdout("");
}

fn box_chars() -> (&'static str, &'static str, &'static str, &'static str, &'static str, &'static str) {
    if ui().ascii {
        ("+", "+", "+", "+", "|", "-")
    } else {
        ("╭", "╮", "╰", "╯", "│", "─")
    }
}

/// One row of a [`kv_box`]: a key/value pair (optionally de-emphasised) or a
/// blank separator between groups.
pub enum BoxRow {
    Kv { key: String, value: String, dim: bool },
    Gap,
}

impl BoxRow {
    pub fn kv(key: impl Into<String>, value: impl Into<String>) -> Self {
        BoxRow::Kv { key: key.into(), value: value.into(), dim: false }
    }
    pub fn dim(key: impl Into<String>, value: impl Into<String>) -> Self {
        BoxRow::Kv { key: key.into(), value: value.into(), dim: true }
    }
}

/// Boxed key/value panel (the "ready" banner).
pub fn kv_box(title: &str, rows: &[BoxRow], footer: &[&str]) {
    if is_silenced() {
        return;
    }
    let s = style();
    let key_w = rows
        .iter()
        .map(|r| match r {
            BoxRow::Kv { key, .. } => key.len(),
            BoxRow::Gap => 0,
        })
        .max()
        .unwrap_or(0);
    if !is_tty() {
        write_stdout(title);
        for r in rows {
            if let BoxRow::Kv { key, value, .. } = r {
                write_stdout(&format!("{}{:<key_w$}   {}", INDENT, key, value, key_w = key_w));
            }
        }
        if !footer.is_empty() {
            write_stdout(&footer.join(". "));
        }
        return;
    }

    let mut body: Vec<String> = Vec::new();
    body.push(s.bold.apply_to(title).to_string());
    if !rows.is_empty() {
        body.push(String::new());
        for r in rows {
            match r {
                BoxRow::Gap => body.push(String::new()),
                BoxRow::Kv { key, value, dim } => {
                    // Pad the plain key first: width formatting doesn't see
                    // through ANSI escapes.
                    let k = format!("{:<key_w$}", key, key_w = key_w);
                    body.push(if *dim {
                        format!("{}   {}", s.dim.apply_to(k), s.dim.apply_to(value))
                    } else {
                        format!("{}   {}", s.dim.apply_to(k), s.accent.apply_to(value))
                    });
                }
            }
        }
    }
    if !footer.is_empty() {
        body.push(String::new());
        let joined = footer.join(&format!("  {}  ", glyphs().bullet));
        body.push(s.dim.apply_to(joined).to_string());
    }

    let inner_w = body
        .iter()
        .map(|l| console::measure_text_width(l))
        .max()
        .unwrap_or(0)
        + 4;
    let (tl, tr, bl, br, v, h) = box_chars();
    let hline = h.repeat(inner_w);
    write_stdout("");
    write_stdout(&format!("{}{}{}{}", INDENT, s.dim.apply_to(tl), s.dim.apply_to(&hline), s.dim.apply_to(tr)));
    for l in &body {
        let pad = inner_w - console::measure_text_width(l) - 2;
        write_stdout(&format!(
            "{}{}  {}{}{}",
            INDENT,
            s.dim.apply_to(v),
            l,
            " ".repeat(pad),
            s.dim.apply_to(v)
        ));
    }
    write_stdout(&format!("{}{}{}{}", INDENT, s.dim.apply_to(bl), s.dim.apply_to(&hline), s.dim.apply_to(br)));
    write_stdout("");
}

/// Dim, indented block with a title (container log excerpts).
pub fn block(title: &str, lines: impl Iterator<Item = String>) {
    let s = style();
    let (tl, _, bl, _, v, h) = box_chars();
    write_stdout("");
    write_stdout(&format!(
        "{}{}",
        INDENT,
        s.dim.apply_to(format!("{}{} {} {}", tl, h, title, h.repeat(3)))
    ));
    for l in lines {
        let clean = strip_ansi_codes(&l);
        write_stdout(&format!(
            "{}{} {}",
            INDENT,
            s.dim.apply_to(v),
            s.dim.apply_to(clean.trim_end())
        ));
    }
    write_stdout(&format!("{}{}", INDENT, s.dim.apply_to(format!("{}{}", bl, h.repeat(6)))));
    write_stdout("");
}

// ── Steps ───────────────────────────────────────────────────────────────────

pub fn fmt_elapsed(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let m = (secs / 60.0).floor() as u64;
        let s = secs - (m as f64) * 60.0;
        format!("{}m {:02.0}s", m, s)
    }
}

/// A single startup/migration step. Create with [`step`], finish with one of
/// `done` / `done_quiet` / `skip` / `warn` / `fail`. Dropping an unfinished
/// step renders it as failed (with the last live message, e.g. "failed while
/// pulling image …") so early `?` returns and Ctrl+C leave an honest trace.
pub struct Step {
    label: String,
    started: Instant,
    bar: Option<ProgressBar>,
    finished: bool,
    silent: bool,
    /// Last `set_message`, shown if the step is dropped unfinished so the
    /// failure line says what it was doing.
    last_msg: Mutex<Option<String>>,
}

pub fn step(label: impl Into<String>) -> Step {
    let label = label.into();
    let u = ui();
    let silent = is_silenced();
    let bar = if u.tty && !silent {
        let pb = u.mp.add(ProgressBar::new_spinner());
        let template = format!("{}{{spinner:.cyan}} {{msg}}", INDENT);
        let st = ProgressStyle::with_template(&template)
            .unwrap_or_else(|_| ProgressStyle::default_spinner())
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "]);
        pb.set_style(st);
        pb.set_message(format!("{:<w$}", label, w = LABEL_WIDTH));
        pb.enable_steady_tick(Duration::from_millis(80));
        Some(pb)
    } else {
        None
    };
    Step {
        label,
        started: Instant::now(),
        bar,
        finished: false,
        silent,
        last_msg: Mutex::new(None),
    }
}

impl Step {
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Live status next to the spinner (TTY only).
    pub fn set_message(&self, msg: impl AsRef<str>) {
        if let Ok(mut m) = self.last_msg.lock() {
            *m = Some(msg.as_ref().to_string());
        }
        if let Some(pb) = &self.bar {
            let s = style();
            pb.set_message(format!(
                "{:<w$}  {}",
                self.label,
                s.dim.apply_to(msg.as_ref()),
                w = LABEL_WIDTH
            ));
        }
    }

    /// A plain line under the step, in both TTY and non-TTY modes. Use for
    /// progress that must survive in a log (image pulls, long waits).
    pub fn note(&self, msg: impl AsRef<str>) {
        if self.silent {
            return;
        }
        let s = style();
        let g = glyphs();
        write_stdout(&format!(
            "{}{}  {:<w$}  {}",
            INDENT,
            s.dim.apply_to(g.pending),
            self.label,
            s.dim.apply_to(msg.as_ref()),
            w = LABEL_WIDTH
        ));
    }

    fn finish(&mut self, glyph: &str, glyph_style: &Style, detail: &str, detail_style: &Style) {
        self.finished = true;
        if self.silent {
            return;
        }
        if let Some(pb) = self.bar.take() {
            pb.finish_and_clear();
            ui().mp.remove(&pb);
        }
        let mut line = format!(
            "{}{} {:<w$}",
            INDENT,
            glyph_style.apply_to(glyph),
            self.label,
            w = LABEL_WIDTH
        );
        if !detail.is_empty() {
            line.push_str("  ");
            line.push_str(&detail_style.apply_to(detail).to_string());
        }
        write_stdout(&line);
    }

    fn with_elapsed(&self, detail: &str) -> String {
        let el = self.elapsed();
        if el < Duration::from_secs(1) {
            return detail.to_string();
        }
        if detail.is_empty() {
            fmt_elapsed(el)
        } else {
            format!("{}{}{}", detail, glyphs().sep, fmt_elapsed(el))
        }
    }

    /// `✓ label  detail  · 2.1s` (elapsed appended when >= 1s).
    pub fn done(mut self, detail: impl AsRef<str>) {
        let d = self.with_elapsed(detail.as_ref());
        let s = style();
        self.finish(glyphs().ok, &s.ok, &d, &s.dim);
    }

    /// `✓ label` with no detail and no elapsed.
    pub fn done_quiet(mut self) {
        let s = style();
        self.finish(glyphs().ok, &s.ok, "", &s.dim);
    }

    /// `↷ label  detail` (dim) for a step that had nothing to do.
    pub fn skip(mut self, detail: impl AsRef<str>) {
        let s = style();
        self.finish(glyphs().skip, &s.dim, detail.as_ref(), &s.dim);
    }

    /// `! label  detail` (yellow) for a step that needs attention but is not
    /// an error (pending migrations, schema drift).
    pub fn warn(mut self, detail: impl AsRef<str>) {
        let s = style();
        self.finish(glyphs().warn, &s.warn, detail.as_ref(), &s.warn);
    }

    /// `✗ label  detail` (red).
    pub fn fail(mut self, detail: impl AsRef<str>) {
        let s = style();
        self.finish(glyphs().fail, &s.fail, detail.as_ref(), &s.fail);
    }
}

impl Drop for Step {
    fn drop(&mut self) {
        if !self.finished {
            let s = style();
            let last = self.last_msg.lock().ok().and_then(|m| m.clone());
            let detail = match last {
                Some(m) => format!("failed while {}", m.trim_end_matches('…')),
                None => "failed".to_string(),
            };
            self.finish(glyphs().fail, &s.fail, &detail, &s.dim);
        }
    }
}

// ── Child-process log lines ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    fn parse(s: &str) -> Option<Level> {
        match s {
            "TRACE" => Some(Level::Trace),
            "DEBUG" => Some(Level::Debug),
            "INFO" => Some(Level::Info),
            "WARN" | "WARNING" => Some(Level::Warn),
            "ERROR" => Some(Level::Error),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }

    fn style(self) -> Style {
        let s = style();
        match self {
            Level::Trace | Level::Debug => s.dim.clone(),
            Level::Info => s.ok.clone(),
            Level::Warn => s.warn.clone(),
            Level::Error => s.fail.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLog {
    pub level: Option<Level>,
    pub target: Option<String>,
    pub msg: String,
}

fn looks_like_timestamp(tok: &str) -> bool {
    // 2026-09-01T09:12:01.123456Z, 2026-09-01T09:12:01Z, 2026-09-01 09:12:01
    let b = tok.as_bytes();
    b.len() >= 10
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[7] == b'-'
        && b[8..10].iter().all(u8::is_ascii_digit)
}

/// Parse a line from a tracing-style logger (SSP, scheduler, SurrealDB):
/// `[timestamp] LEVEL [target[{spans}]:] message`. ANSI escapes are stripped.
/// Anything unrecognised comes back with `level: None` and the whole (clean)
/// line as `msg`.
pub fn parse_log_line(raw: &str) -> ParsedLog {
    let clean = strip_ansi_codes(raw);
    let clean = clean.trim_end();
    let unparsed = || ParsedLog {
        level: None,
        target: None,
        msg: clean.to_string(),
    };

    let mut rest = clean.trim_start();
    // Optional timestamp (one or two tokens: `2026-09-01T..Z` or `2026-09-01 09:12:01`).
    if let Some((first, tail)) = rest.split_once(char::is_whitespace) {
        if looks_like_timestamp(first) {
            rest = tail.trim_start();
            if first.len() == 10 {
                if let Some((_, tail2)) = rest.split_once(char::is_whitespace) {
                    rest = tail2.trim_start();
                }
            }
        }
    }

    let (lvl_tok, tail) = match rest.split_once(char::is_whitespace) {
        Some(x) => x,
        None => return unparsed(),
    };
    let level = match Level::parse(lvl_tok) {
        Some(l) => l,
        None => return unparsed(),
    };
    let tail = tail.trim_start();

    // Optional `target:` / `target{span=..}:` segment. tracing's default and
    // compact formats end the target with `: `. Only accept it when the token
    // has no spaces before the colon and looks like a module path.
    let (target, msg) = match tail.split_once(": ") {
        Some((t, m)) if !t.contains(' ') && !t.is_empty() => {
            let t = t.split('{').next().unwrap_or(t);
            (Some(t.to_string()), m.to_string())
        }
        _ => match tail.strip_suffix(':') {
            Some(t) if !t.contains(' ') => (Some(t.to_string()), String::new()),
            _ => (None, tail.to_string()),
        },
    };

    ParsedLog {
        level: Some(level),
        target,
        msg,
    }
}

/// A line that belongs to the previous one: anyhow's "Caused by:" chains,
/// indented backtrace frames, node's trailing `}`.
fn looks_like_continuation(clean: &str) -> bool {
    clean.starts_with(' ')
        || clean == "}"
        || clean.starts_with('\t')
        || clean.starts_with("Caused by:")
        || clean.starts_with("Stack backtrace:")
}

fn looks_like_crash(clean: &str) -> bool {
    clean.contains("panicked at")
        || clean.starts_with("thread '")
        || clean.starts_with("Error:")
        || clean.contains("fatal runtime error")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    /// User app dev server (vite, backend, docker app). Quiet mode keeps only
    /// lines that look like warnings/errors (any log format); verbose shows all.
    App,
    /// Infra service (SurrealDB / SSP / scheduler): filtered + compacted.
    Infra,
}

/// Heuristic for arbitrary app output (vite, node, Go, Python…): does this
/// line look like something the user must see?
/// A dev server announcing where it listens (vite's `➜  Local:   http://…`,
/// "listening on http://…"). Shown in quiet mode because the CLI can't know
/// the port ahead of time. Request logs ("GET http://…") are excluded.
fn looks_like_url_announcement(clean: &str) -> bool {
    let lower = clean.to_ascii_lowercase();
    let has_url = lower.contains("http://localhost") || lower.contains("http://127.0.0.1");
    let request_log = [" get ", " post ", " put ", " delete ", " patch ", " head ", " options "]
        .iter()
        .any(|m| lower.contains(m))
        || lower.starts_with("get ")
        || lower.starts_with("post ");
    has_url && !request_log
}

/// First `http://localhost:…` / `http://127.0.0.1:…` URL in a line.
pub fn extract_local_url(clean: &str) -> Option<String> {
    for needle in ["http://localhost", "http://127.0.0.1"] {
        if let Some(i) = clean.find(needle) {
            let rest = &clean[i..];
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ')' || c == ',')
                .unwrap_or(rest.len());
            return Some(rest[..end].to_string());
        }
    }
    None
}

fn looks_like_app_problem(clean: &str) -> bool {
    let lower = clean.to_ascii_lowercase();
    lower.contains("error")
        || lower.contains("fatal")
        || lower.contains("panic")
        || lower.contains("exception")
        || lower.contains("unhandled")
        || lower.contains("failed")
        || lower.contains("eaddrinuse")
        || lower.contains("econnrefused")
        || lower.contains("traceback")
        || clean.contains("✘")
}

/// Per-process line sink: prefixes, filters and reformats child-process
/// output and prints it without tearing the spinner region.
pub struct LineSink {
    prefix: String,
    kind: StreamKind,
    ring: Mutex<VecDeque<String>>,
    /// Whether the previous line was printed, so multi-line errors
    /// ("Caused by:", indented backtrace frames) stay attached to their header
    /// in quiet mode.
    last_shown: AtomicBool,
    /// First URL the process announced (`➜ Local: http://…`), for the ready box.
    announced_url: Mutex<Option<String>>,
    /// Quiet mode prints URL announcements only when set (the ready box went
    /// out before the URL was known).
    print_urls: AtomicBool,
}

impl LineSink {
    pub fn new(label: &str, style: Style, kind: StreamKind) -> Arc<Self> {
        Arc::new(LineSink {
            prefix: style.apply_to(format!("[{}]", label)).to_string(),
            kind,
            ring: Mutex::new(VecDeque::with_capacity(RING_CAPACITY)),
            last_shown: AtomicBool::new(false),
            announced_url: Mutex::new(None),
            print_urls: AtomicBool::new(false),
        })
    }

    /// Decide what (if anything) to print for `raw`. Pure so it can be tested.
    fn render(&self, raw: &str, verbose: bool, ready: bool) -> Option<String> {
        match self.kind {
            StreamKind::App => {
                if verbose {
                    return Some(format!("{} {}", self.prefix, raw));
                }
                let clean = strip_ansi_codes(raw);
                let clean = clean.trim_end();
                if clean.is_empty() {
                    return None;
                }
                let parsed = parse_log_line(clean);
                let continuation = self.last_shown.load(Ordering::Relaxed)
                    && looks_like_continuation(clean);
                let problem = matches!(parsed.level, Some(l) if l >= Level::Error)
                    || looks_like_app_problem(clean)
                    || looks_like_crash(clean);
                let url_line = looks_like_url_announcement(clean)
                    && self.print_urls.load(Ordering::Relaxed);
                if problem || continuation || url_line {
                    Some(format!("{} {}", self.prefix, raw))
                } else {
                    None
                }
            }
            StreamKind::Infra => {
                if !ready && !verbose {
                    return None;
                }
                let parsed = parse_log_line(raw);
                if !verbose && parsed.msg.is_empty() && parsed.level.is_none() {
                    return None;
                }
                match parsed.level {
                    Some(level) => {
                        if !verbose && level < Level::Error {
                            return None;
                        }
                        let mut out = format!("{} {}", self.prefix, level.style().apply_to(level.label()));
                        if verbose {
                            if let Some(t) = &parsed.target {
                                out.push(' ');
                                out.push_str(&style().dim.apply_to(format!("{}:", t)).to_string());
                            }
                        }
                        if !parsed.msg.is_empty() {
                            out.push(' ');
                            out.push_str(&parsed.msg);
                        }
                        Some(out)
                    }
                    None => {
                        let continuation = self.last_shown.load(Ordering::Relaxed)
                            && looks_like_continuation(&parsed.msg);
                        if verbose || continuation || looks_like_crash(&parsed.msg) {
                            Some(format!("{} {}", self.prefix, parsed.msg))
                        } else {
                            None
                        }
                    }
                }
            }
        }
    }

    /// Our own "Starting: …" / "Building: …" lines: verbose only. Quiet mode
    /// gets a single summary of launched apps from the caller instead.
    pub fn push_verbose(&self, raw: &str) {
        if is_verbose() {
            write_stdout(&format!("{} {}", self.prefix, raw));
        }
    }

    /// URL this process announced, if any yet.
    pub fn announced_url(&self) -> Option<String> {
        self.announced_url.lock().ok().and_then(|u| u.clone())
    }

    /// Let URL announcements through the quiet filter from now on.
    pub fn print_urls(&self, on: bool) {
        self.print_urls.store(on, Ordering::Relaxed);
    }

    pub fn push(&self, raw: &str, is_stderr: bool) {
        let u = ui();
        if self.kind == StreamKind::App {
            let clean = strip_ansi_codes(raw);
            if looks_like_url_announcement(&clean) {
                if let Some(url) = extract_local_url(&clean) {
                    if let Ok(mut slot) = self.announced_url.lock() {
                        slot.get_or_insert(url);
                    }
                }
            }
        }
        let verbose = u.verbose.load(Ordering::Relaxed);
        let ready = u.ready.load(Ordering::SeqCst);
        if self.kind == StreamKind::Infra && !ready {
            let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
            if ring.len() == RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(strip_ansi_codes(raw).into_owned());
        }
        let rendered = self.render(raw, verbose, ready);
        self.last_shown.store(rendered.is_some(), Ordering::Relaxed);
        if let Some(line) = rendered {
            if is_stderr {
                write_stderr(&line);
            } else {
                write_stdout(&line);
            }
        }
    }

    /// Render the buffered pre-ready lines as a block (failure diagnostics).
    pub fn dump_ring(&self, title: &str) {
        let lines: Vec<String> = self
            .ring
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        if lines.is_empty() {
            return;
        }
        block(title, lines.into_iter());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tracing_default_format() {
        let p = parse_log_line("2026-09-01T09:12:01.123456Z  INFO ssp_server::main: listening on 0.0.0.0:8667");
        assert_eq!(p.level, Some(Level::Info));
        assert_eq!(p.target.as_deref(), Some("ssp_server::main"));
        assert_eq!(p.msg, "listening on 0.0.0.0:8667");
    }

    #[test]
    fn parses_tracing_with_span_context() {
        let p = parse_log_line("2026-09-01T09:12:01Z DEBUG scheduler::replica{id=3}: applied 12 events");
        assert_eq!(p.level, Some(Level::Debug));
        assert_eq!(p.target.as_deref(), Some("scheduler::replica"));
        assert_eq!(p.msg, "applied 12 events");
    }

    #[test]
    fn parses_compact_without_time_or_target() {
        let p = parse_log_line(" WARN registration retry 2/5");
        assert_eq!(p.level, Some(Level::Warn));
        assert_eq!(p.target, None);
        assert_eq!(p.msg, "registration retry 2/5");
    }

    #[test]
    fn parses_surrealdb_format_and_strips_ansi() {
        let p = parse_log_line("\x1b[2m2026-09-01T09:12:01.000Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2msurrealdb::net\x1b[0m\x1b[2m:\x1b[0m Started web server on 0.0.0.0:8000");
        assert_eq!(p.level, Some(Level::Info));
        assert_eq!(p.target.as_deref(), Some("surrealdb::net"));
        assert_eq!(p.msg, "Started web server on 0.0.0.0:8000");
    }

    #[test]
    fn parses_space_separated_timestamp() {
        let p = parse_log_line("2026-09-01 09:12:01 ERROR something broke");
        assert_eq!(p.level, Some(Level::Error));
        assert_eq!(p.target, None);
        assert_eq!(p.msg, "something broke");
    }

    #[test]
    fn unparseable_lines_keep_whole_text() {
        let p = parse_log_line("thread 'main' panicked at src/main.rs:10:5:");
        assert_eq!(p.level, None);
        assert_eq!(p.msg, "thread 'main' panicked at src/main.rs:10:5:");
        assert!(looks_like_crash(&p.msg));
    }

    #[test]
    fn message_with_colon_but_no_target() {
        // A message containing ": " with spaces before it must not be mistaken
        // for a target.
        let p = parse_log_line("INFO request took: 12ms");
        assert_eq!(p.target, None);
        assert_eq!(p.msg, "request took: 12ms");
    }

    fn sink(kind: StreamKind) -> Arc<LineSink> {
        LineSink::new("ssp", Style::new(), kind)
    }

    #[test]
    fn app_lines_filtered_in_quiet_mode() {
        let s = sink(StreamKind::App);
        assert_eq!(s.render("  VITE v6.0.1  ready in 412 ms", false, true), None);
        assert_eq!(s.render("gamesync-api listening on :3661", false, true), None);
        assert!(s.render("Error: listen EADDRINUSE: address already in use :::3661", false, true).is_some());
        assert_eq!(s.render("2026-09-02T08:59:10.132-0700  WARN  livekit something", false, true), None);
        assert_eq!(s.render("time=2026-09-02T16:22:57.924Z level=WARN msg=\"RESEND_API_KEY not set\"", false, true), None);
        assert!(s.render("time=2026-09-02T16:22:57.924Z level=ERROR msg=\"boom\"", false, true).is_some());
        assert_eq!(s.render("", false, true), None);
        assert_eq!(s.render("[vite] warning: unused import", false, true), None);
        assert!(s.render(" ELIFECYCLE  Command failed with exit code 1.", false, true).is_some());
        // Dev-server URL announcements are captured, and printed only once
        // the sink is told the ready box already went out.
        assert_eq!(s.render("  ➜  Local:   http://localhost:5166/", false, true), None);
        s.push("  ➜  Local:   http://localhost:5166/", false);
        assert_eq!(s.announced_url().as_deref(), Some("http://localhost:5166/"));
        s.print_urls(true);
        assert!(s.render("gamesync-api listening on http://localhost:3661", false, true).is_some());
        assert_eq!(s.render("GET http://localhost:3661/health 200 3ms", false, true), None);
        assert_eq!(extract_local_url("Local: http://127.0.0.1:3000/app, Network: …").as_deref(), Some("http://127.0.0.1:3000/app"));
        // Verbose shows everything.
        assert!(s.render("  VITE v6.0.1  ready in 412 ms", true, true).is_some());
        // Stack-trace continuation after a shown error stays visible.
        s.last_shown.store(true, Ordering::Relaxed);
        assert!(s.render("    at Server.listen (node:net:2102:7)", false, true).is_some());
    }

    #[test]
    fn infra_quiet_before_ready_renders_nothing() {
        let s = sink(StreamKind::Infra);
        assert_eq!(s.render("2026-01-01T00:00:00Z ERROR boom", false, false), None);
    }

    #[test]
    fn infra_quiet_after_ready_keeps_errors_only() {
        let s = sink(StreamKind::Infra);
        assert_eq!(s.render("2026-01-01T00:00:00Z INFO x: hi", false, true), None);
        assert_eq!(s.render("2026-01-01T00:00:00Z DEBUG x: hi", false, true), None);
        assert_eq!(s.render("2026-01-01T00:00:00Z WARN x: careful", false, true), None);
        let e = s.render("2026-01-01T00:00:00Z ERROR x: bad", false, true).unwrap();
        assert!(e.contains("ERROR") && e.contains("bad"));
        assert!(!e.contains("x:"), "target hidden in quiet mode: {e}");
        assert!(s.render("thread 'main' panicked at foo", false, true).is_some());
        assert_eq!(s.render("some unstructured noise", false, true), None);
    }

    #[test]
    fn infra_verbose_keeps_everything_compact() {
        let s = sink(StreamKind::Infra);
        let l = s
            .render("2026-01-01T00:00:00.123Z DEBUG ssp::x: hi", true, true)
            .unwrap();
        assert!(l.contains("DEBUG"));
        assert!(l.contains("ssp::x:"));
        assert!(l.contains("hi"));
        assert!(!l.contains("2026-01-01"), "timestamp dropped: {l}");
        assert!(s.render("unstructured", true, true).is_some());
        assert!(s.render("unstructured", true, false).is_some());
    }

    #[test]
    fn error_continuation_lines_stay_visible_in_quiet_mode() {
        let s = sink(StreamKind::Infra);
        // Real SSP crash output: an ERROR header then an anyhow chain.
        assert!(s.render("2026-09-02T16:00:02Z ERROR ssp_server: bootstrap failed", false, true).is_some());
        s.last_shown.store(true, Ordering::Relaxed);
        assert_eq!(s.render("", false, true), None);
        assert!(s.render("Caused by:", false, true).is_some());
        assert!(s.render("    0: Failed to open HTTP to http://surrealdb:8000", false, true).is_some());
        // After an unrelated hidden INFO line the chain is closed.
        s.last_shown.store(false, Ordering::Relaxed);
        assert_eq!(s.render("    stray indented noise", false, true), None);
    }

    #[test]
    fn ring_is_capped() {
        let s = sink(StreamKind::Infra);
        for i in 0..(RING_CAPACITY + 10) {
            s.push(&format!("line {i}"), false);
        }
        let ring = s.ring.lock().unwrap();
        assert_eq!(ring.len(), RING_CAPACITY);
        assert_eq!(ring.front().map(String::as_str), Some("line 10"));
    }

    #[test]
    fn elapsed_formatting() {
        assert_eq!(fmt_elapsed(Duration::from_millis(2100)), "2.1s");
        assert_eq!(fmt_elapsed(Duration::from_secs(64)), "1m 04s");
    }

    #[test]
    fn steps_do_not_panic_uninitialised() {
        // Under `cargo test` stdout is not a TTY: exercise every finish path.
        step("a").done("x");
        step("b").done_quiet();
        step("c").skip("nothing");
        step("d").warn("2 pending");
        step("e").fail("boom");
        let s = step("f");
        s.set_message("live");
        s.note("note");
        drop(s);
        {
            let _g = silenced();
            step("g").done("hidden");
            info("hidden");
            detail("hidden");
        }
        info("shown");
        warn("shown");
        error("shown");
        hint("shown");
        header("t", &["m"]);
        kv_box("title", &[BoxRow::kv("k", "v"), BoxRow::Gap, BoxRow::dim("d", "v")], &["footer"]);
        block("logs", vec!["l1".to_string()].into_iter());
        suspend(|| ());
    }
}

#[cfg(test)]
mod live_format_tests {
    use super::*;

    #[test]
    fn real_container_lines_are_quiet() {
        let s = LineSink::new("ssp", Style::new(), StreamKind::Infra);
        let ssp = "\x1b[2m2026-09-02T16:11:58.385431Z\x1b[0m \x1b[34mDEBUG\x1b[0m \x1b[2mssp_server\x1b[0m\x1b[2m:\x1b[0m Heartbeat sent successfully";
        let sched_info = "\x1b[2m2026-09-02T16:12:21.100202Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2mmaintenance::db\x1b[0m\x1b[2m:\x1b[0m Reconnected to SurrealDB with a fresh session";
        let sched_warn = "\x1b[2m2026-09-02T16:12:21.058598Z\x1b[0m \x1b[33m WARN\x1b[0m \x1b[2mmaintenance::db\x1b[0m\x1b[2m:\x1b[0m SurrealDB re-signin timed out \x1b[3mtimeout_secs\x1b[0m\x1b[2m=\x1b[0m10";
        let surreal_info = "2026-09-02T16:11:05.888147Z  INFO surrealdb::net: Listening for a system shutdown signal.";
        let surreal_warn = "2026-09-02T16:11:21.601517Z  WARN surrealdb_server::ntw::rpc: A connection was made without a specified protocol.";
        assert_eq!(s.render(ssp, false, true), None);
        assert_eq!(s.render(sched_info, false, true), None);
        assert_eq!(s.render(surreal_info, false, true), None);
        assert_eq!(s.render(sched_warn, false, true), None);
        assert_eq!(s.render(surreal_warn, false, true), None);
        assert!(s.render(sched_warn, true, true).is_some());
    }

    // ── confirmation decision table ─────────────────────────────────────────

    #[test]
    fn yes_answers_consent_with_yes_and_a_preference_with_its_default() {
        // (prompt, default, is_consent, yes, terminal)
        assert!(decide("apply to PRODUCTION?", false, true, true, false).unwrap());
        // "Set up billing now?" under --yes is a decision nobody made: default wins.
        assert!(!decide("set up billing now?", false, false, true, true).unwrap());
        assert!(decide("initialize git?", true, false, true, true).unwrap());
    }

    #[test]
    fn consent_without_a_terminal_or_yes_refuses_instead_of_proceeding() {
        // The production-migration gate used to be skipped entirely here.
        let err = decide("apply to PRODUCTION?", false, true, false, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("--yes"), "the refusal must name the way out: {msg}");
    }

    #[test]
    fn a_preference_without_a_terminal_takes_its_default_and_carries_on() {
        assert!(decide("initialize git?", true, false, false, false).unwrap());
        assert!(!decide("needs auth?", false, false, false, false).unwrap());
    }

    #[test]
    fn irreversible_consent_is_not_covered_by_yes() {
        set_assume_yes(true);
        let err = consent_naming("Destroy project 'acme'?", "acme", None).unwrap_err();
        set_assume_yes(false);
        let msg = format!("{err:#}");
        assert!(msg.contains("--confirm acme"), "{msg}");
        assert!(msg.contains("--yes does not cover"), "{msg}");
    }

    #[test]
    fn irreversible_consent_needs_the_name_spelled_right() {
        assert!(consent_naming("Destroy?", "acme", Some("acme")).unwrap());
        let err = consent_naming("Destroy?", "acme", Some("acme-staging")).unwrap_err();
        assert!(format!("{err:#}").contains("does not match"), "{err:#}");
    }
}
