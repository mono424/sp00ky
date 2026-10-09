//! Write-ahead log for durable event buffering.
//!
//! Every ingested event is appended here before it is buffered, and a drain
//! truncates what it applied. The log is a directory of append-only segment
//! files, so neither of those ever rewrites anything:
//!
//! - **Append** frames the event into one `write` on the active segment. A
//!   segment is rotated once it holds `SPKY_WAL_SEGMENT_MB` (default 16).
//! - **Truncate** closes the active segment and unlinks every closed segment
//!   whose highest seq the drain covered. A segment that also holds newer
//!   events survives whole; recovery filters by seq anyway.
//! - **Recover** scans the segments in order, CRC-checking every frame, and
//!   stops a segment at its first bad frame: the torn tail of the newest one
//!   is the normal crash shape, a hole in an older one is logged with the
//!   seq gap.
//!
//! The JSON Lines file this replaced was re-parsed and rewritten in full under
//! the WAL lock after every drain, while `/ingest` (and, on the HTTP
//! transport, the user's transaction) waited: a few milliseconds at a few
//! hundred events, hundreds at a 10k-event backlog. A file in that format is
//! migrated into segments once, on the first open that finds it.
//!
//! # Layout
//!
//! ```text
//! <wal dir>/event_wal.<first seq, 20 digits>.seg
//!   [8]  "SPKYWAL1"  [u64] first seq
//!   frame*: [u32 payload len][u32 crc32(payload)][payload]
//!   payload: [u64 seq][u64 received_at][u64 versionstamp][u8 op]
//!            [u16 len][table][u32 len][record id]
//!            [u8 has data][u32 len][record as JSON]
//! ```
//!
//! Little-endian throughout. The record stays JSON inside the frame: it is a
//! `serde_json::Value`, which no self-describing-free codec reads back, and
//! it is parsed once at recovery, never at append. `job_assignee` is always
//! `None` in the log and `version` always equals `seq`, so neither is stored.

use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

use crate::messages::{BufferedEvent, RecordOp, RecordUpdate};

const SEGMENT_MAGIC: &[u8; 8] = b"SPKYWAL1";
const SEGMENT_HEADER_LEN: usize = SEGMENT_MAGIC.len() + 8;
const FRAME_HEADER_LEN: usize = 8;
/// Largest frame recovery accepts, so a corrupt length cannot ask for a
/// multi-gigabyte read before the CRC gets to reject it.
const MAX_FRAME: u32 = 64 << 20;
const DEFAULT_SEGMENT_BYTES: u64 = 16 << 20;

/// Write-Ahead Log for durable event buffering. See the module docs.
pub struct EventWal {
    dir: PathBuf,
    segment_bytes: u64,
    /// Segments no longer appended to, oldest first.
    closed: Vec<Segment>,
    /// Where appends go, once one happened since the last rotation.
    active: Option<Active>,
    last_seq: u64,
    max_versionstamp: u64,
    /// Everything at or below this seq was truncated in this process.
    /// `recover` hides it even where its segment survived next to newer
    /// events; across a restart the boot recovery filters by the persisted
    /// snapshot seq, which is what that truncation described.
    retired_up_to: u64,
}

#[derive(Debug, Clone)]
struct Segment {
    path: PathBuf,
    max_seq: u64,
    events: u64,
}

struct Active {
    segment: Segment,
    file: File,
    bytes: u64,
}

/// What one segment held when it was scanned.
struct Scanned {
    first_seq: u64,
    events: Vec<BufferedEvent>,
    /// Where the scan stopped short of the file's end, if it did.
    torn_at: Option<u64>,
}

impl EventWal {
    /// Open the log whose segments live in `wal/` next to `path`. A JSON
    /// Lines log at `path` itself (the previous format) is migrated into
    /// segments and renamed `.migrated`.
    pub fn new(path: PathBuf) -> Result<Self> {
        let segment_bytes = std::env::var("SPKY_WAL_SEGMENT_MB")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|mb| *mb > 0)
            .map(|mb| mb << 20)
            .unwrap_or(DEFAULT_SEGMENT_BYTES);
        Self::open(path, segment_bytes)
    }

    /// [`Self::new`] with an explicit segment size.
    pub fn open(path: PathBuf, segment_bytes: u64) -> Result<Self> {
        let dir = path.parent().unwrap_or_else(|| Path::new(".")).join("wal");
        fs::create_dir_all(&dir).with_context(|| format!("Failed to create WAL directory: {:?}", dir))?;

        let mut wal = Self {
            dir,
            segment_bytes: segment_bytes.max(64 << 10),
            closed: Vec::new(),
            active: None,
            last_seq: 0,
            max_versionstamp: 0,
            retired_up_to: 0,
        };
        wal.scan_dir()?;
        wal.migrate_legacy(&path)?;
        info!(dir = ?wal.dir, segments = wal.closed.len(), last_seq = wal.last_seq, "Opened WAL");
        Ok(wal)
    }

    /// Append a single event to the WAL (write-ahead): one `write` of one
    /// frame onto the active segment, rotating first when it is full.
    pub fn append(&mut self, event: &BufferedEvent) -> Result<()> {
        let frame = encode_frame(event)?;
        let rotate = match &self.active {
            Some(active) => active.bytes >= self.segment_bytes,
            None => true,
        };
        if rotate {
            self.close_active();
            self.active = Some(self.open_segment(event.seq)?);
        }
        let active = self.active.as_mut().expect("rotated above");
        active
            .file
            .write_all(&frame)
            .with_context(|| format!("Failed to write to WAL segment {:?}", active.segment.path))?;
        active.bytes += frame.len() as u64;
        active.segment.events += 1;
        active.segment.max_seq = active.segment.max_seq.max(event.seq);
        self.last_seq = self.last_seq.max(event.seq);
        self.max_versionstamp = self.max_versionstamp.max(event.versionstamp);
        Ok(())
    }

    /// Forget every event with `seq <= up_to_seq`: close the active segment
    /// and unlink the segments the bound covers entirely. Nothing is
    /// rewritten; a segment holding a newer event keeps its older ones until
    /// a later truncation covers it.
    pub fn truncate(&mut self, up_to_seq: u64) -> Result<()> {
        self.close_active();
        let (gone, kept): (Vec<Segment>, Vec<Segment>) =
            self.closed.drain(..).partition(|s| s.max_seq <= up_to_seq);
        self.closed = kept;
        let mut removed_events = 0u64;
        for segment in &gone {
            match fs::remove_file(&segment.path) {
                Ok(()) => removed_events += segment.events,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => removed_events += segment.events,
                Err(e) => {
                    // Keep tracking it: the next truncation tries again.
                    warn!(path = ?segment.path, error = %e, "Could not remove a drained WAL segment");
                    self.closed.push(segment.clone());
                }
            }
        }
        self.closed.sort_by(|a, b| a.path.cmp(&b.path));
        self.retired_up_to = self.retired_up_to.max(up_to_seq);
        info!(
            removed = removed_events,
            segments_removed = gone.len(),
            remaining = self.closed.iter().map(|s| s.events).sum::<u64>(),
            up_to_seq,
            "WAL truncated"
        );
        Ok(())
    }

    /// Every event the log still holds past what this process truncated,
    /// in seq order.
    pub fn recover(&self) -> Result<Vec<BufferedEvent>> {
        let mut paths: Vec<&Path> = self.closed.iter().map(|s| s.path.as_path()).collect();
        if let Some(active) = &self.active {
            paths.push(active.segment.path.as_path());
        }
        let mut events = Vec::new();
        for (i, path) in paths.iter().enumerate() {
            let scanned = scan_segment(path)?;
            if let Some(at) = scanned.torn_at {
                if i + 1 == paths.len() {
                    debug!(path = ?path, at, "WAL segment ends in a torn frame; dropping the tail");
                } else {
                    warn!(
                        path = ?path,
                        at,
                        last_seq = scanned.events.last().map(|e| e.seq).unwrap_or(scanned.first_seq),
                        "WAL segment is damaged before its end; events after the damage are lost"
                    );
                }
            }
            events.extend(scanned.events.into_iter().filter(|e| e.seq > self.retired_up_to));
        }
        events.sort_by_key(|e| e.seq);
        debug!(count = events.len(), "Read events from WAL");
        Ok(events)
    }

    /// Highest changefeed versionstamp any logged entry carried (`0` when
    /// none did): where the tail resumes after a restart, together with the
    /// replica's persisted cursor.
    pub fn max_versionstamp(&self) -> Result<u64> {
        Ok(self.max_versionstamp)
    }

    /// Segment files currently on disk, for tests and diagnostics.
    pub fn segment_count(&self) -> usize {
        self.closed.len() + usize::from(self.active.is_some())
    }

    fn close_active(&mut self) {
        if let Some(active) = self.active.take() {
            if active.segment.events > 0 {
                self.closed.push(active.segment);
            } else {
                let _ = fs::remove_file(&active.segment.path);
            }
        }
    }

    fn open_segment(&self, first_seq: u64) -> Result<Active> {
        let path = self.dir.join(format!("event_wal.{first_seq:020}.seg"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to create WAL segment {:?}", path))?;
        let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
        header.extend_from_slice(SEGMENT_MAGIC);
        header.extend_from_slice(&first_seq.to_le_bytes());
        file.write_all(&header)?;
        Ok(Active {
            segment: Segment {
                path,
                max_seq: 0,
                events: 0,
            },
            file,
            bytes: header.len() as u64,
        })
    }

    /// Take stock of the segments already on disk.
    fn scan_dir(&mut self) -> Result<()> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)
            .with_context(|| format!("Failed to read WAL directory: {:?}", self.dir))?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
            .collect();
        paths.sort();
        for path in paths {
            let scanned = match scan_segment(&path) {
                Ok(s) => s,
                Err(e) => {
                    warn!(path = ?path, error = %e, "Skipping an unreadable WAL segment");
                    continue;
                }
            };
            if scanned.events.is_empty() {
                let _ = fs::remove_file(&path);
                continue;
            }
            let max_seq = scanned.events.iter().map(|e| e.seq).max().unwrap_or(0);
            self.last_seq = self.last_seq.max(max_seq);
            self.max_versionstamp = self
                .max_versionstamp
                .max(scanned.events.iter().map(|e| e.versionstamp).max().unwrap_or(0));
            self.closed.push(Segment {
                path,
                max_seq,
                events: scanned.events.len() as u64,
            });
        }
        Ok(())
    }

    /// Move a JSON Lines log at `path` into segments, once.
    fn migrate_legacy(&mut self, path: &Path) -> Result<()> {
        let text = match fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e).context("Failed to read the legacy WAL"),
        };
        let (mut migrated, mut skipped) = (0usize, 0usize);
        for (line_num, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<BufferedEvent>(line) {
                Ok(event) => {
                    self.append(&event)?;
                    migrated += 1;
                }
                Err(e) => {
                    warn!(line_num, error = %e, "Skipping corrupt legacy WAL entry");
                    skipped += 1;
                }
            }
        }
        self.close_active();
        let mut moved = path.as_os_str().to_owned();
        moved.push(".migrated");
        fs::rename(path, &moved).with_context(|| format!("Failed to retire the legacy WAL at {:?}", path))?;
        info!(migrated, skipped, retired = ?moved, "Migrated the legacy WAL into segments");
        Ok(())
    }
}

fn encode_frame(event: &BufferedEvent) -> Result<Vec<u8>> {
    let update = &event.update;
    let data = match &update.data {
        Some(value) => Some(serde_json::to_vec(value).context("Failed to serialize the event record")?),
        None => None,
    };
    let table = update.table.as_bytes();
    let id = update.record_id.as_bytes();
    let table_len = u16::try_from(table.len()).context("table name too long for the WAL")?;
    let id_len = u32::try_from(id.len()).context("record id too long for the WAL")?;
    let data_len = u32::try_from(data.as_ref().map_or(0, Vec::len)).context("record too large for the WAL")?;

    let payload_len = 8 + 8 + 8 + 1 + 2 + table.len() + 4 + id.len() + 1 + 4 + data_len as usize;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload_len);
    frame.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    frame.extend_from_slice(&event.seq.to_le_bytes());
    frame.extend_from_slice(&event.received_at.to_le_bytes());
    frame.extend_from_slice(&event.versionstamp.to_le_bytes());
    frame.push(match update.operation {
        RecordOp::Create => 0,
        RecordOp::Update => 1,
        RecordOp::Delete => 2,
    });
    frame.extend_from_slice(&table_len.to_le_bytes());
    frame.extend_from_slice(table);
    frame.extend_from_slice(&id_len.to_le_bytes());
    frame.extend_from_slice(id);
    frame.push(u8::from(data.is_some()));
    frame.extend_from_slice(&data_len.to_le_bytes());
    if let Some(data) = &data {
        frame.extend_from_slice(data);
    }

    let payload = &frame[FRAME_HEADER_LEN..];
    let len = u32::try_from(payload.len()).ok().filter(|l| *l <= MAX_FRAME).context("event too large for the WAL")?;
    let crc = crc32fast::hash(payload);
    frame[..4].copy_from_slice(&len.to_le_bytes());
    frame[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(frame)
}

/// Decode one frame's payload. `None` for anything that does not parse: a
/// CRC-checked payload that fails here was written by another layout.
fn decode_payload(payload: &[u8]) -> Option<BufferedEvent> {
    let mut at = 0usize;
    let mut take = |n: usize| -> Option<&[u8]> {
        let out = payload.get(at..at + n)?;
        at += n;
        Some(out)
    };
    let seq = u64::from_le_bytes(take(8)?.try_into().ok()?);
    let received_at = u64::from_le_bytes(take(8)?.try_into().ok()?);
    let versionstamp = u64::from_le_bytes(take(8)?.try_into().ok()?);
    let operation = match take(1)?[0] {
        0 => RecordOp::Create,
        1 => RecordOp::Update,
        2 => RecordOp::Delete,
        _ => return None,
    };
    let table_len = u16::from_le_bytes(take(2)?.try_into().ok()?) as usize;
    let table = std::str::from_utf8(take(table_len)?).ok()?.to_string();
    let id_len = u32::from_le_bytes(take(4)?.try_into().ok()?) as usize;
    let record_id = std::str::from_utf8(take(id_len)?).ok()?.to_string();
    let has_data = take(1)?[0];
    let data_len = u32::from_le_bytes(take(4)?.try_into().ok()?) as usize;
    let data = take(data_len)?;
    if at != payload.len() {
        return None;
    }
    let data = match has_data {
        0 => None,
        1 => Some(serde_json::from_slice(data).ok()?),
        _ => return None,
    };
    Some(BufferedEvent {
        seq,
        update: RecordUpdate {
            table,
            operation,
            record_id,
            data,
            version: seq,
            job_assignee: None,
        },
        received_at,
        versionstamp,
    })
}

/// Read one segment, frame by frame, stopping at the first frame that does
/// not check out.
fn scan_segment(path: &Path) -> Result<Scanned> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|mut f| f.read_to_end(&mut bytes))
        .with_context(|| format!("Failed to read WAL segment {:?}", path))?;
    if bytes.len() < SEGMENT_HEADER_LEN || &bytes[..SEGMENT_MAGIC.len()] != SEGMENT_MAGIC {
        anyhow::bail!("not a WAL segment: {:?}", path);
    }
    let first_seq = u64::from_le_bytes(bytes[SEGMENT_MAGIC.len()..SEGMENT_HEADER_LEN].try_into().expect("8 bytes"));
    let mut events = Vec::new();
    let mut at = SEGMENT_HEADER_LEN;
    let torn_at = loop {
        if at == bytes.len() {
            break None;
        }
        let Some(header) = bytes.get(at..at + FRAME_HEADER_LEN) else {
            break Some(at as u64);
        };
        let len = u32::from_le_bytes(header[..4].try_into().expect("4 bytes"));
        let crc = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));
        if len > MAX_FRAME {
            break Some(at as u64);
        }
        let start = at + FRAME_HEADER_LEN;
        let Some(payload) = bytes.get(start..start + len as usize) else {
            break Some(at as u64);
        };
        if crc32fast::hash(payload) != crc {
            break Some(at as u64);
        }
        match decode_payload(payload) {
            Some(event) => events.push(event),
            None => break Some(at as u64),
        }
        at = start + len as usize;
    };
    Ok(Scanned {
        first_seq,
        events,
        torn_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("spky-wal-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn event(seq: u64, op: RecordOp, data: Option<serde_json::Value>) -> BufferedEvent {
        BufferedEvent {
            seq,
            update: RecordUpdate {
                table: "game".to_string(),
                operation: op,
                record_id: format!("game:g{seq}"),
                data,
                version: seq,
                job_assignee: None,
            },
            received_at: 1_700_000_000 + seq,
            versionstamp: seq * 10,
        }
    }

    fn seqs(events: &[BufferedEvent]) -> Vec<u64> {
        events.iter().map(|e| e.seq).collect()
    }

    fn segments_on_disk(path: &Path) -> usize {
        fs::read_dir(path.parent().unwrap().join("wal"))
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("seg"))
            .count()
    }

    #[test]
    fn events_round_trip_through_frames() {
        let dir = tmpdir("roundtrip");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::new(path.clone()).unwrap();
        let events = vec![
            event(1, RecordOp::Create, Some(json!({ "title": "héllo ⟨odd⟩", "n": 1, "nested": { "a": [1, 2.5, null, true] } }))),
            event(2, RecordOp::Update, Some(json!({ "title": "b", "_00_rv": 7 }))),
            event(3, RecordOp::Delete, None),
        ];
        for e in &events {
            wal.append(e).unwrap();
        }
        let back = wal.recover().unwrap();
        assert_eq!(back.len(), 3);
        for (a, b) in events.iter().zip(&back) {
            assert_eq!(a.seq, b.seq);
            assert_eq!(a.received_at, b.received_at);
            assert_eq!(a.versionstamp, b.versionstamp);
            assert_eq!(a.update.table, b.update.table);
            assert_eq!(a.update.operation, b.update.operation);
            assert_eq!(a.update.record_id, b.update.record_id);
            assert_eq!(a.update.data, b.update.data);
            assert_eq!(b.update.version, b.seq);
            assert!(b.update.job_assignee.is_none());
        }
        assert_eq!(wal.max_versionstamp().unwrap(), 30);

        // A fresh open sees the same log.
        let again = EventWal::new(path).unwrap();
        assert_eq!(seqs(&again.recover().unwrap()), vec![1, 2, 3]);
        assert_eq!(again.max_versionstamp().unwrap(), 30);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncation_unlinks_covered_segments_and_rewrites_nothing() {
        let dir = tmpdir("truncate");
        let path = dir.join("event_wal.log");
        // Tiny segments: every append rotates.
        let mut wal = EventWal::open(path.clone(), 64 << 10).unwrap();
        for seq in 1..=4 {
            wal.append(&event(seq, RecordOp::Create, Some(json!({ "pad": "x".repeat(70_000) })))).unwrap();
        }
        assert_eq!(wal.segment_count(), 4);
        wal.truncate(2).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![3, 4]);
        assert_eq!(segments_on_disk(&path), 2);
        wal.truncate(10).unwrap();
        assert!(wal.recover().unwrap().is_empty());
        assert_eq!(segments_on_disk(&path), 0);

        // Appends after a truncation open a fresh segment.
        wal.append(&event(11, RecordOp::Create, Some(json!({})))).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![11]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A segment holding events past the bound survives whole; its covered
    /// events are hidden now and filtered by the boot recovery's snapshot
    /// seq after a restart.
    #[test]
    fn a_segment_with_newer_events_survives_a_truncation() {
        let dir = tmpdir("survive");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::new(path.clone()).unwrap();
        for seq in 1..=3 {
            wal.append(&event(seq, RecordOp::Create, Some(json!({})))).unwrap();
        }
        wal.truncate(2).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![3]);
        assert_eq!(segments_on_disk(&path), 1);
        let reopened = EventWal::new(path).unwrap();
        assert_eq!(seqs(&reopened.recover().unwrap()), vec![1, 2, 3], "nothing was rewritten");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn out_of_order_appends_recover_in_seq_order() {
        let dir = tmpdir("order");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::new(path).unwrap();
        wal.append(&event(3, RecordOp::Create, Some(json!({})))).unwrap();
        wal.append(&event(2, RecordOp::Create, Some(json!({})))).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![2, 3]);
        wal.truncate(2).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![3]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_torn_tail_is_dropped_and_appends_continue() {
        let dir = tmpdir("torn");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::new(path.clone()).unwrap();
        wal.append(&event(1, RecordOp::Create, Some(json!({ "a": 1 })))).unwrap();
        wal.append(&event(2, RecordOp::Create, Some(json!({ "a": 2 })))).unwrap();
        drop(wal);
        // The process died mid-write: the last frame is incomplete.
        let seg = fs::read_dir(dir.join("wal")).unwrap().flatten().next().unwrap().path();
        let bytes = fs::read(&seg).unwrap();
        fs::write(&seg, &bytes[..bytes.len() - 5]).unwrap();

        let mut wal = EventWal::new(path).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![1]);
        wal.append(&event(3, RecordOp::Create, Some(json!({ "a": 3 })))).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![1, 3]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_flipped_byte_stops_that_segment_only() {
        let dir = tmpdir("flipped");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::open(path.clone(), 64 << 10).unwrap();
        for seq in 1..=3 {
            wal.append(&event(seq, RecordOp::Create, Some(json!({ "pad": "x".repeat(70_000) })))).unwrap();
        }
        drop(wal);
        let mut segs: Vec<PathBuf> = fs::read_dir(dir.join("wal")).unwrap().flatten().map(|e| e.path()).collect();
        segs.sort();
        let mut bytes = fs::read(&segs[0]).unwrap();
        bytes[SEGMENT_HEADER_LEN + FRAME_HEADER_LEN + 20] ^= 0x01;
        fs::write(&segs[0], bytes).unwrap();

        let wal = EventWal::new(path).unwrap();
        assert_eq!(seqs(&wal.recover().unwrap()), vec![2, 3]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_legacy_json_lines_log_is_migrated_once() {
        let dir = tmpdir("legacy");
        let path = dir.join("event_wal.log");
        let legacy: String = [
            // Exactly what the previous writer put on a line, one without a
            // versionstamp (written before the field existed).
            r#"{"seq":5,"update":{"table":"game","operation":"Create","record_id":"game:a","data":{"title":"a"},"version":5},"received_at":100,"versionstamp":50}"#,
            r#"{"seq":6,"update":{"table":"game","operation":"Delete","record_id":"game:b","data":null,"version":6},"received_at":101}"#,
            "not json at all",
        ]
        .join("\n");
        fs::write(&path, legacy).unwrap();

        let wal = EventWal::new(path.clone()).unwrap();
        let events = wal.recover().unwrap();
        assert_eq!(seqs(&events), vec![5, 6]);
        assert_eq!(events[0].update.data, Some(json!({ "title": "a" })));
        assert_eq!(events[1].update.operation, RecordOp::Delete);
        assert_eq!(events[1].versionstamp, 0);
        assert_eq!(wal.max_versionstamp().unwrap(), 50);
        assert!(!path.exists(), "the legacy file is retired");
        assert!(dir.join("event_wal.log.migrated").exists());

        // Opening again finds the segments and no legacy file to migrate.
        let again = EventWal::new(path).unwrap();
        assert_eq!(seqs(&again.recover().unwrap()), vec![5, 6]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// What the segments buy: a drain's truncation no longer scales with the
    /// backlog, because nothing is parsed or rewritten.
    #[test]
    fn truncation_cost_does_not_grow_with_the_backlog() {
        let dir = tmpdir("cost");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::new(path).unwrap();
        let body = json!({ "pad": "z".repeat(2_000) });
        for seq in 1..=2_000 {
            wal.append(&event(seq, RecordOp::Create, Some(body.clone()))).unwrap();
        }
        let started = std::time::Instant::now();
        wal.truncate(2_000).unwrap();
        let elapsed = started.elapsed();
        assert!(elapsed < std::time::Duration::from_millis(250), "truncate took {elapsed:?}");
        assert!(wal.recover().unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn segments_rotate_at_the_configured_size() {
        let dir = tmpdir("rotate");
        let path = dir.join("event_wal.log");
        let mut wal = EventWal::open(path.clone(), 64 << 10).unwrap();
        for seq in 1..=10 {
            wal.append(&event(seq, RecordOp::Create, Some(json!({ "pad": "y".repeat(20_000) })))).unwrap();
        }
        assert!(wal.segment_count() >= 3, "{}", wal.segment_count());
        assert_eq!(seqs(&wal.recover().unwrap()), (1..=10).collect::<Vec<_>>());
        assert_eq!(wal.max_versionstamp().unwrap(), 100);
        let _ = fs::remove_dir_all(&dir);
    }
}
