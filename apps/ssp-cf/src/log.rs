//! `tracing` to the Workers console, one JSON object per event.
//!
//! Without a subscriber every `warn!`/`error!` in ssp-node is dropped on
//! Cloudflare (the threads title-fold failure of 2026-10-14 went unseen for
//! that reason). Workers Logs and `wrangler tail` keep each console line with
//! its level, so ERROR goes to `console.error`, WARN to `console.warn` and the
//! rest to `console.log`, and the OTLP export carries them on. No timestamp
//! field: the clock does not advance while code runs, and the platform stamps
//! every line anyway.

use std::io;
use std::sync::Once;

use tracing::{Level, Metadata};
use tracing_subscriber::fmt::MakeWriter;

/// One event's bytes, written out as a single console line on drop.
pub struct ConsoleLine {
    level: Level,
    buf: Vec<u8>,
}

impl io::Write for ConsoleLine {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ConsoleLine {
    fn drop(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let line = String::from_utf8_lossy(&self.buf);
        let line = line.trim_end();
        match self.level {
            Level::ERROR => worker::console_error!("{line}"),
            Level::WARN => worker::console_warn!("{line}"),
            _ => worker::console_log!("{line}"),
        }
    }
}

pub struct Console;

impl<'a> MakeWriter<'a> for Console {
    type Writer = ConsoleLine;

    fn make_writer(&'a self) -> Self::Writer {
        ConsoleLine { level: Level::INFO, buf: Vec::new() }
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        ConsoleLine { level: *meta.level(), buf: Vec::with_capacity(256) }
    }
}

/// Install the subscriber once per isolate. `SPKY_LOG` (error, warn, info,
/// debug, trace) sets the level; info by default.
pub fn init(level: Option<String>) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let level = match level.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("error") => Level::ERROR,
            Some("warn") => Level::WARN,
            Some("debug") => Level::DEBUG,
            Some("trace") => Level::TRACE,
            _ => Level::INFO,
        };
        let _ = tracing_subscriber::fmt()
            .json()
            .without_time()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_max_level(level)
            .with_writer(Console)
            .try_init();
    });
}
