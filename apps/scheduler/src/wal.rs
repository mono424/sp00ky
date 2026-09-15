use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use tracing::{debug, info, warn};

use crate::messages::BufferedEvent;

/// Write-Ahead Log for durable event buffering.
///
/// Each entry is a JSON line. On every ingest, the event is appended
/// before processing. On snapshot update, the WAL is truncated to only
/// keep events with seq > snapshot_seq.
pub struct EventWal {
    path: PathBuf,
    file: File,
}

impl EventWal {
    /// Open or create the WAL file
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create WAL directory: {:?}", parent))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to open WAL at {:?}", path))?;

        info!("Opened WAL at {:?}", path);
        Ok(Self { path, file })
    }

    /// Append a single event to the WAL (write-ahead)
    pub fn append(&mut self, event: &BufferedEvent) -> Result<()> {
        let json = serde_json::to_string(event)
            .context("Failed to serialize BufferedEvent")?;
        writeln!(self.file, "{}", json)
            .context("Failed to write to WAL")?;
        self.file.flush()
            .context("Failed to flush WAL")?;
        Ok(())
    }

    /// Truncate the WAL, keeping only events with seq > up_to_seq
    pub fn truncate(&mut self, up_to_seq: u64) -> Result<()> {
        let events = self.read_all()?;
        let remaining: Vec<&BufferedEvent> = events.iter()
            .filter(|e| e.seq > up_to_seq)
            .collect();

        let remaining_count = remaining.len();
        let removed_count = events.len() - remaining_count;

        // Rewrite the file with only remaining events
        let tmp_path = self.path.with_extension("tmp");
        {
            let mut tmp_file = File::create(&tmp_path)
                .with_context(|| format!("Failed to create temp WAL at {:?}", tmp_path))?;
            for event in &remaining {
                let json = serde_json::to_string(event)?;
                writeln!(tmp_file, "{}", json)?;
            }
            tmp_file.flush()?;
        }

        // Atomically replace
        fs::rename(&tmp_path, &self.path)
            .context("Failed to rename temp WAL")?;

        // Reopen file in append mode
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .context("Failed to reopen WAL after truncate")?;

        info!(
            removed = removed_count,
            remaining = remaining_count,
            up_to_seq,
            "WAL truncated"
        );
        Ok(())
    }

    /// Recover all events from the WAL file
    pub fn recover(&self) -> Result<Vec<BufferedEvent>> {
        self.read_all()
    }

    /// Read all events from the WAL
    fn read_all(&self) -> Result<Vec<BufferedEvent>> {
        let file = File::open(&self.path);
        let file = match file {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e).context("Failed to open WAL for reading"),
        };

        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("Failed to read WAL line {}", line_num))?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<BufferedEvent>(line) {
                Ok(event) => events.push(event),
                Err(e) => {
                    warn!(line_num, error = %e, "Skipping corrupt WAL entry");
                }
            }
        }

        debug!(count = events.len(), "Read events from WAL");
        Ok(events)
    }

    /// Get the highest sequence number in the WAL, or None if empty
    pub fn max_seq(&self) -> Result<Option<u64>> {
        let events = self.read_all()?;
        Ok(events.last().map(|e| e.seq))
    }

    /// Highest changefeed versionstamp any WAL entry carries (`0` when none
    /// does): where the tail resumes after a restart, together with the
    /// replica's persisted cursor.
    pub fn max_versionstamp(&self) -> Result<u64> {
        let events = self.read_all()?;
        Ok(events.iter().map(|e| e.versionstamp).max().unwrap_or(0))
    }
}
