//! Binary checkpoint of one collection's rows.
//!
//! What a cluster SSP keeps across a restart so it does not have to page the
//! whole database through the scheduler again. Rows only: views, operator
//! state and table metadata are rebuilt on boot from `_00_query` and the
//! upstream schema. That is also what keeps them right, since operator state
//! is a function of the rows and registering a view primes it from them.
//!
//! Records are written exactly as the row table holds them, next to the field
//! dictionary they were encoded against. Writing is a copy of bytes. Reading
//! does not copy them back: the file's bytes BECOME the table's arena, either
//! mapped read-only ([`map_image`]) or adopted as the heap buffer
//! ([`read_image`]), and the index is rebuilt by one walk over the records
//! that reads nothing but each record's id and its stored digest. That walk
//! is what turned a 629k-row load from seconds of per-row copies and
//! allocations into a hash insert per row.
//!
//! # Layout
//!
//! ```text
//! [8]   magic "SPKYROWS"
//! [u32] checkpoint format       FORMAT
//! [u32] record format           row_codec::RECORD_FORMAT
//! [u32 len][bytes]              table name
//! [32]  catch-up hash           the collection's XOR set-hash at write time
//! [u32] field count, then per field [u32 len][bytes]   dictionary, id order
//! [u64] row count, then per row [u32 len][bytes]       raw records
//! [32]  blake3 of every byte above
//! ```
//!
//! Integers are little-endian. `FORMAT` describes these bytes, not the reader:
//! the readers here changed from copying to mapping without the number moving,
//! so a file written by either build loads in either. Bump it only when the
//! layout changes.
//!
//! # Trust
//!
//! A read has two independent checks: the blake3 trailer, verified over the
//! whole file before anything else is parsed, catches torn or flipped bytes;
//! and the catch-up hash XOR-folded from the loaded records' digests has to
//! equal the one stored, which catches a writer whose accumulator had
//! drifted. Neither makes the rows *right*: the scheduler's per-table hash at
//! registration decides whether a loaded table is kept, repaired or paged. A
//! checkpoint is trusted for time, never for correctness.
//!
//! A mapped file must only ever be replaced by `rename`, never truncated or
//! written in place: that is the one way a read-only mapping can fault. The
//! writer renames a finished `.tmp` over the file, and a clean restart
//! unlinks, which keeps the mapped inode alive until the process exits.

use crate::circuit::arena::{Arena, HeapArena};
use crate::circuit::row_codec::{FieldDict, RECORD_FORMAT};
use crate::circuit::row_table::{IndexedBlock, RowTable};
use crate::circuit::store::Collection;
use std::io::{self, Write};
use std::ops::Range;

pub const MAGIC: &[u8; 8] = b"SPKYROWS";
pub const FORMAT: u32 = 1;

/// Upper bound on any one length field, so a corrupt length cannot ask for a
/// multi-gigabyte allocation before the trailer gets to reject the file.
const MAX_CHUNK: u32 = 256 << 20;

const XOR_LEN: usize = 32;
const TRAILER_LEN: usize = 32;

/// The smallest image: every fixed field, an empty name, no dictionary, no
/// rows. Anything shorter cannot be a checkpoint.
const MIN_IMAGE_LEN: usize = MAGIC.len() + 4 + 4 + 4 + XOR_LEN + 4 + 8 + TRAILER_LEN;

#[derive(Debug)]
pub enum CheckpointError {
    Io(io::Error),
    /// Readable, but not a checkpoint this build can trust: wrong magic, a
    /// format or record layout from another build, a bad checksum, or rows
    /// that do not add up to the stored hash.
    Invalid(String),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::Io(e) => write!(f, "checkpoint I/O: {e}"),
            CheckpointError::Invalid(why) => write!(f, "invalid checkpoint: {why}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl From<io::Error> for CheckpointError {
    fn from(e: io::Error) -> Self {
        CheckpointError::Io(e)
    }
}

fn invalid(why: impl Into<String>) -> CheckpointError {
    CheckpointError::Invalid(why.into())
}

/// What [`write_collection`] produced.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    pub rows: u64,
    /// Every byte of the file, trailer included.
    pub bytes: u64,
}

/// Write `coll`'s rows to `out`.
pub fn write_collection<W: Write>(coll: &Collection, out: W) -> io::Result<Written> {
    let mut w = Hashing::new(out);
    w.write_all(MAGIC)?;
    w.write_all(&FORMAT.to_le_bytes())?;
    w.write_all(&RECORD_FORMAT.to_le_bytes())?;
    write_chunk(&mut w, coll.name.as_bytes())?;
    w.write_all(&coll.catchup_xor)?;

    let names: Vec<&str> = coll.rows.dict().names().collect();
    w.write_all(&(names.len() as u32).to_le_bytes())?;
    for name in names {
        write_chunk(&mut w, name.as_bytes())?;
    }

    let rows = coll.rows.records().count() as u64;
    w.write_all(&rows.to_le_bytes())?;
    for record in coll.rows.records() {
        write_chunk(&mut w, record)?;
    }

    let digest = w.hasher.finalize();
    w.inner.write_all(digest.as_bytes())?;
    w.inner.flush()?;
    Ok(Written {
        rows,
        bytes: w.bytes + TRAILER_LEN as u64,
    })
}

/// The header of one image, borrowed from its bytes, and where its records
/// are. Produced by [`parse_image`] after the trailer has been verified.
#[derive(Debug)]
pub struct Image<'a> {
    pub name: &'a str,
    pub stored_xor: [u8; XOR_LEN],
    /// Dictionary names in id order.
    pub field_names: Vec<&'a str>,
    pub rows: u64,
    /// The record region: `rows` times `[u32 len][record]`.
    pub records: Range<usize>,
}

/// Verify an image's trailer and parse its header. Pure over a byte slice, so
/// every target has it; the records are not touched beyond locating them.
pub fn parse_image(bytes: &[u8]) -> Result<Image<'_>, CheckpointError> {
    if bytes.len() < MIN_IMAGE_LEN {
        return Err(invalid("too short to be a row checkpoint"));
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return Err(invalid("not a row checkpoint"));
    }
    // The trailer first: after this, every length below is as the writer left
    // it. The reads stay bounds-checked anyway.
    let (body, trailer) = bytes.split_at(bytes.len() - TRAILER_LEN);
    if blake3::hash(body).as_bytes() != trailer {
        return Err(invalid("checksum mismatch"));
    }

    let mut c = Cursor {
        bytes: body,
        at: MAGIC.len(),
    };
    let format = c.u32()?;
    if format != FORMAT {
        return Err(invalid(format!("checkpoint format {format}, this build reads {FORMAT}")));
    }
    let record_format = c.u32()?;
    if record_format != RECORD_FORMAT {
        return Err(invalid(format!(
            "record format {record_format}, this build encodes {RECORD_FORMAT}"
        )));
    }
    let name = c.str_chunk("table name")?;
    let stored_xor: [u8; XOR_LEN] = c
        .bytes(XOR_LEN)?
        .try_into()
        .expect("cursor returned the requested length");

    let fields = c.u32()?;
    let mut field_names = Vec::with_capacity(fields.min(4096) as usize);
    for _ in 0..fields {
        field_names.push(c.str_chunk("field name")?);
    }

    let rows = c.u64()?;
    Ok(Image {
        name,
        stored_xor,
        field_names,
        rows,
        records: c.at..body.len(),
    })
}

/// How a load went, for the log line that proves the gain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadStats {
    pub bytes: u64,
    /// Trailer verification: the sequential pass over the file.
    pub verify_ms: u64,
    /// The index walk over the records.
    pub index_ms: u64,
    /// The file is mapped rather than copied.
    pub mapped: bool,
}

/// Read an image whose bytes are already in memory. The buffer BECOMES the
/// table's arena: no record is copied, the slots point into it at the file
/// offsets. The arena is heap-backed whatever the process configured, which
/// is what the browser, the Durable Object and the Dart build need.
pub fn read_image(bytes: Vec<u8>) -> Result<(Collection, LoadStats), CheckpointError> {
    let (name, dict, block, stats) = index_image(&bytes)?;
    let arena = HeapArena::from_buf(bytes, block.live);
    Ok((assemble(name, dict, block, Box::new(arena)), stats))
}

/// Map the file at `path` read-only and make it the first segment of a
/// file-backed arena whose later appends go to fresh segments under `dir`
/// (see [`arena::MmapArena::from_image`]).
///
/// The mapping stays for the life of the collection, so the file must only
/// ever be replaced by `rename` (module docs). `populate` asks the kernel to
/// fault the pages in up front, which the verification pass needs anyway.
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
pub fn map_image(
    path: &std::path::Path,
    dir: &std::path::Path,
    segment_bytes: usize,
) -> Result<(Collection, LoadStats), CheckpointError> {
    let file = std::fs::File::open(path)?;
    // SAFETY: the file is opened read-only and the mapping is private. The
    // one way such a mapping faults is the file being truncated underneath
    // it, and every writer of these files replaces them by rename, which
    // leaves the mapped inode intact.
    let map = unsafe { memmap2::MmapOptions::new().populate().map(&file)? };
    #[cfg(unix)]
    let _ = map.advise(memmap2::Advice::Sequential);
    let (name, dict, block, stats) = index_image(&map)?;
    let arena = crate::circuit::arena::MmapArena::from_image(dir, &name, segment_bytes, map, block.live);
    Ok((
        assemble(name, dict, block, Box::new(arena)),
        LoadStats { mapped: true, ..stats },
    ))
}

/// Load the image at `path` the way this process stores rows: mapped when
/// the arena is file-backed, adopted into the heap otherwise.
#[cfg(not(target_arch = "wasm32"))]
pub fn load_file(path: &std::path::Path) -> Result<(Collection, LoadStats), CheckpointError> {
    match crate::circuit::arena::configured_backing() {
        #[cfg(feature = "mmap-store")]
        crate::circuit::arena::ArenaBacking::Files { dir, segment_bytes } => {
            map_image(path, dir, *segment_bytes)
        }
        _ => read_image(std::fs::read(path)?),
    }
}

/// Verify, parse and index one image. Shared by the two readers; only the
/// arena differs.
fn index_image(bytes: &[u8]) -> Result<(String, FieldDict, IndexedBlock, LoadStats), CheckpointError> {
    let started = web_time::Instant::now();
    let image = parse_image(bytes)?;
    let dict = FieldDict::from_names(image.field_names.iter().copied())
        .ok_or_else(|| invalid("field dictionary repeats a name"))?;
    let verify_ms = ms_since(started);

    let started = web_time::Instant::now();
    let block = RowTable::index_block(bytes, image.records.clone(), image.rows, 0)
        .map_err(|e| invalid(e.to_string()))?;
    if block.xor != image.stored_xor {
        return Err(invalid("rows do not add up to the stored catch-up hash"));
    }
    let index_ms = ms_since(started);

    let stats = LoadStats {
        bytes: bytes.len() as u64,
        verify_ms,
        index_ms,
        mapped: false,
    };
    Ok((image.name.to_string(), dict, block, stats))
}

fn assemble(name: String, dict: FieldDict, block: IndexedBlock, arena: Box<dyn Arena>) -> Collection {
    let rows = RowTable::from_parts(dict, block.index, arena);
    Collection::from_rows(name, rows, block.xor)
}

fn ms_since(started: web_time::Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

/// A writer that feeds every byte through blake3 on the way and counts them.
struct Hashing<W> {
    inner: W,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl<W> Hashing<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
            bytes: 0,
        }
    }
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn write_chunk<W: Write>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len())
        .ok()
        .filter(|len| *len <= MAX_CHUNK)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "checkpoint chunk too large"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(bytes)
}

/// Bounds-checked reads over the verified body.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], CheckpointError> {
        let end = self.at.checked_add(n).ok_or_else(|| invalid("length overflows the file"))?;
        let out = self.bytes.get(self.at..end).ok_or_else(|| invalid("truncated header"))?;
        self.at = end;
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32, CheckpointError> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().expect("4 bytes")))
    }

    fn u64(&mut self) -> Result<u64, CheckpointError> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().expect("8 bytes")))
    }

    fn chunk(&mut self) -> Result<&'a [u8], CheckpointError> {
        let len = self.u32()?;
        if len > MAX_CHUNK {
            return Err(invalid(format!("chunk of {len} bytes")));
        }
        self.bytes(len as usize)
    }

    fn str_chunk(&mut self, what: &str) -> Result<&'a str, CheckpointError> {
        std::str::from_utf8(self.chunk()?).map_err(|_| invalid(format!("{what} is not UTF-8")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit::store::Operation;
    use crate::types::Sp00kyValue;
    use serde_json::json;

    fn collection(rows: &[(&str, serde_json::Value)]) -> Collection {
        let mut coll = Collection::new("game".to_string());
        for (id, body) in rows {
            coll.apply(Operation::Create, id, Sp00kyValue::from(body.clone()));
        }
        coll
    }

    fn bytes_of(coll: &Collection) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_collection(coll, &mut bytes).unwrap();
        bytes
    }

    fn round_trip(coll: &Collection) -> Result<Collection, CheckpointError> {
        read_image(bytes_of(coll)).map(|(c, _)| c)
    }

    /// Recompute the trailer after the test edited the body, so the edit is
    /// what gets rejected rather than the checksum.
    fn reseal(bytes: &mut [u8]) {
        let (body, trailer) = bytes.split_at_mut(bytes.len() - TRAILER_LEN);
        trailer.copy_from_slice(blake3::hash(body).as_bytes());
    }

    /// Offset of the `[u64 rows]` field in an image of `coll`.
    fn rows_offset(coll: &Collection) -> usize {
        let dict: usize = coll.rows.dict().names().map(|n| 4 + n.len()).sum();
        MAGIC.len() + 4 + 4 + 4 + coll.name.len() + XOR_LEN + 4 + dict
    }

    #[test]
    fn rows_survive_byte_for_byte() {
        let coll = collection(&[
            ("a", json!({ "id": "game:a", "white": "x", "moves": [1, 2, 3], "_00_rv": 4 })),
            ("b", json!({ "id": "game:b", "meta": { "site": "lichess", "rated": true }, "elo": 1.5 })),
            ("⟨odd id⟩", json!({ "id": "game:⟨odd id⟩", "nil": null })),
        ]);
        let back = round_trip(&coll).unwrap();

        assert_eq!(back.name, "game");
        assert_eq!(back.rows.len(), 3);
        assert_eq!(back.catchup_xor, coll.catchup_xor);
        for id in ["a", "b", "⟨odd id⟩"] {
            assert_eq!(back.get_row(id).to_owned_value(), coll.get_row(id).to_owned_value(), "{id}");
            assert_eq!(back.rows.digest_of(id), coll.rows.digest_of(id), "{id}");
            assert_eq!(back.rows.rv_of(id), coll.rows.rv_of(id), "{id}");
        }
        assert!(!back.membership_built(), "a loaded table builds no z-set until a view scans it");
        let mut keys: Vec<_> = back.membership().keys().map(|k| k.to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["game:a", "game:b", "game:⟨odd id⟩"]);
        assert!(back.membership().values().all(|w| *w == 1));
    }

    #[test]
    fn written_reports_rows_and_every_byte() {
        let coll = collection(&[("a", json!({ "n": 1 })), ("b", json!({ "n": 2 }))]);
        let mut bytes = Vec::new();
        let written = write_collection(&coll, &mut bytes).unwrap();
        assert_eq!(written.rows, 2);
        assert_eq!(written.bytes, bytes.len() as u64);
    }

    /// The adopted buffer is the arena: nothing was copied, and the bytes
    /// that are not records (header, prefixes, trailer) are accounted dead.
    #[test]
    fn the_heap_image_is_the_arena() {
        let coll = collection(&[("a", json!({ "n": 1 })), ("b", json!({ "s": "two" }))]);
        let bytes = bytes_of(&coll);
        let records: u64 = coll.rows.records().map(|r| r.len() as u64).sum();
        let (back, stats) = read_image(bytes.clone()).unwrap();
        assert_eq!(back.rows.live_bytes(), records);
        assert_eq!(back.rows.dead_bytes(), bytes.len() as u64 - records);
        assert_eq!(stats.bytes, bytes.len() as u64);
        assert!(!stats.mapped);
        // The table keeps working as a table: a later write appends, an
        // update retires the image's copy, and a second checkpoint of the
        // mixed table loads again.
        let mut back = back;
        back.apply(Operation::Create, "c", Sp00kyValue::from(json!({ "n": 3 })));
        back.apply(Operation::Update, "a", Sp00kyValue::from(json!({ "n": 10 })));
        assert_eq!(back.get_row("c").get("n").as_i64(), Some(3));
        assert_eq!(back.get_row("a").get("n").as_i64(), Some(10));
        assert_eq!(back.get_row("b").get("s").as_str(), Some("two"));
        let again = round_trip(&back).unwrap();
        assert_eq!(again.rows.len(), 3);
        assert_eq!(again.catchup_xor, back.catchup_xor);
        assert_eq!(again.get_row("a").get("n").as_i64(), Some(10));
    }

    #[test]
    fn updated_and_deleted_rows_are_not_resurrected() {
        let mut coll = collection(&[("a", json!({ "v": 1 })), ("b", json!({ "v": 1 }))]);
        coll.apply(Operation::Update, "a", Sp00kyValue::from(json!({ "v": 2 })));
        coll.apply(Operation::Delete, "b", Sp00kyValue::Null);
        let back = round_trip(&coll).unwrap();
        assert_eq!(back.rows.len(), 1);
        assert_eq!(back.get_row("a").get("v").as_i64(), Some(2));
        assert!(!back.has_row("b"));
        assert_eq!(back.catchup_xor, coll.catchup_xor);
    }

    #[test]
    fn an_empty_table_round_trips() {
        let coll = Collection::new("empty".to_string());
        let back = round_trip(&coll).unwrap();
        assert_eq!(back.rows.len(), 0);
        assert_eq!(back.catchup_xor, coll.catchup_xor);
    }

    #[test]
    fn a_flipped_byte_anywhere_is_rejected() {
        let coll = collection(&[
            ("a", json!({ "title": "hello" })),
            ("b", json!({ "title": "world", "n": 2 })),
        ]);
        let bytes = bytes_of(&coll);
        // One byte in each region: magic, name, dictionary, a record, the trailer.
        let rows_at = rows_offset(&coll);
        for at in [0, MAGIC.len() + 9, rows_at - 2, rows_at + 12, rows_at + 60, bytes.len() - 1] {
            let mut bad = bytes.clone();
            bad[at] ^= 0x40;
            assert!(read_image(bad).is_err(), "flipped byte at {at} was accepted");
        }
    }

    #[test]
    fn a_truncated_file_is_rejected() {
        let coll = collection(&[("a", json!({ "title": "hello" })), ("b", json!({ "n": 2 }))]);
        let mut bytes = bytes_of(&coll);
        bytes.truncate(bytes.len() - 40);
        assert!(read_image(bytes).is_err());
        assert!(read_image(Vec::new()).is_err());
        assert!(read_image(b"SPKYROWS".to_vec()).is_err());
    }

    #[test]
    fn another_record_format_is_refused() {
        let coll = collection(&[("a", json!({ "n": 1 }))]);
        let mut bytes = bytes_of(&coll);
        bytes[12..16].copy_from_slice(&(RECORD_FORMAT + 1).to_le_bytes());
        reseal(&mut bytes);
        let err = read_image(bytes).unwrap_err();
        assert!(err.to_string().contains("record format"), "{err}");
    }

    #[test]
    fn another_checkpoint_format_is_refused() {
        let coll = collection(&[("a", json!({ "n": 1 }))]);
        let mut bytes = bytes_of(&coll);
        bytes[8..12].copy_from_slice(&(FORMAT + 1).to_le_bytes());
        reseal(&mut bytes);
        let err = read_image(bytes).unwrap_err();
        assert!(err.to_string().contains("checkpoint format"), "{err}");
    }

    #[test]
    fn a_stale_stored_hash_is_rejected() {
        let mut coll = collection(&[("a", json!({ "n": 1 }))]);
        // A hash that does not describe the rows, as a checkpoint written from
        // a collection whose accumulator had drifted would carry.
        coll.catchup_xor[0] ^= 1;
        let err = round_trip(&coll).unwrap_err();
        assert!(err.to_string().contains("catch-up hash"), "{err}");
    }

    /// The row count has to describe the region exactly: more declared than
    /// present is a truncated walk, fewer leaves bytes nobody indexed.
    #[test]
    fn a_row_count_that_does_not_match_the_region_is_rejected() {
        let coll = collection(&[("a", json!({ "n": 1 })), ("b", json!({ "n": 2 }))]);
        let bytes = bytes_of(&coll);
        let at = rows_offset(&coll);
        for declared in [1u64, 3, u64::MAX] {
            let mut bad = bytes.clone();
            bad[at..at + 8].copy_from_slice(&declared.to_le_bytes());
            reseal(&mut bad);
            assert!(read_image(bad).is_err(), "row count {declared} was accepted");
        }
    }

    #[test]
    fn a_duplicated_record_is_rejected() {
        let coll = collection(&[("a", json!({ "n": 1 }))]);
        let bytes = bytes_of(&coll);
        let at = rows_offset(&coll);
        let record = bytes[at + 8..bytes.len() - TRAILER_LEN].to_vec();
        let mut bad = bytes[..at].to_vec();
        bad.extend_from_slice(&2u64.to_le_bytes());
        bad.extend_from_slice(&record);
        bad.extend_from_slice(&record);
        bad.extend_from_slice(&[0u8; TRAILER_LEN]);
        reseal(&mut bad);
        let err = read_image(bad).unwrap_err();
        assert!(err.to_string().contains("twice"), "{err}");
    }

    #[test]
    fn a_dictionary_that_repeats_a_name_is_rejected() {
        let coll = collection(&[("a", json!({ "n": 1 }))]);
        let bytes = bytes_of(&coll);
        let dict_at = MAGIC.len() + 4 + 4 + 4 + coll.name.len() + XOR_LEN;
        let mut bad = bytes[..dict_at].to_vec();
        bad.extend_from_slice(&2u32.to_le_bytes());
        for _ in 0..2 {
            bad.extend_from_slice(&1u32.to_le_bytes());
            bad.extend_from_slice(b"n");
        }
        bad.extend_from_slice(&bytes[rows_offset(&coll)..]);
        reseal(&mut bad);
        let err = read_image(bad).unwrap_err();
        assert!(err.to_string().contains("dictionary"), "{err}");
    }

    #[test]
    fn parse_image_reports_the_header() {
        let coll = collection(&[("a", json!({ "x": 1, "y": "z" }))]);
        let bytes = bytes_of(&coll);
        let image = parse_image(&bytes).unwrap();
        assert_eq!(image.name, "game");
        assert_eq!(image.rows, 1);
        assert_eq!(image.stored_xor, coll.catchup_xor);
        let mut names = image.field_names.clone();
        names.sort();
        assert_eq!(names, vec!["x", "y"]);
        assert_eq!(image.records.end, bytes.len() - TRAILER_LEN);
    }

    #[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
    mod mapped {
        use super::*;

        fn tmpdir(name: &str) -> std::path::PathBuf {
            let d = std::env::temp_dir().join(format!("ssp-checkpoint-test-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d
        }

        #[test]
        fn a_mapped_image_reads_back_and_accepts_appends() {
            let dir = tmpdir("map");
            let coll = collection(&[
                ("a", json!({ "n": 1, "s": "one" })),
                ("b", json!({ "n": 2, "nested": { "k": [true, null] } })),
            ]);
            let path = dir.join("game.rows");
            std::fs::write(&path, bytes_of(&coll)).unwrap();

            let (mut back, stats) = map_image(&path, &dir.join("arena"), 64 * 1024).unwrap();
            assert!(stats.mapped);
            assert_eq!(stats.bytes, std::fs::metadata(&path).unwrap().len());
            assert_eq!(back.rows.len(), 2);
            assert_eq!(back.catchup_xor, coll.catchup_xor);
            for id in ["a", "b"] {
                assert_eq!(back.get_row(id).to_owned_value(), coll.get_row(id).to_owned_value());
                assert_eq!(back.rows.digest_of(id), coll.rows.digest_of(id));
            }

            // Appends land in a fresh writable segment; the image's rows stay.
            back.apply(Operation::Create, "c", Sp00kyValue::from(json!({ "n": 3 })));
            back.apply(Operation::Update, "a", Sp00kyValue::from(json!({ "n": 11 })));
            assert_eq!(back.get_row("c").get("n").as_i64(), Some(3));
            assert_eq!(back.get_row("a").get("n").as_i64(), Some(11));
            assert_eq!(back.get_row("b").get("n").as_i64(), Some(2));

            // The file is replaced by rename while mapped, as the writer does;
            // the mapping keeps serving the old inode.
            let tmp = dir.join("game.rows.tmp");
            let mut out = std::fs::File::create(&tmp).unwrap();
            write_collection(&back, &mut out).unwrap();
            std::fs::rename(&tmp, &path).unwrap();
            assert_eq!(back.get_row("b").get("n").as_i64(), Some(2));

            let (again, _) = map_image(&path, &dir.join("arena"), 64 * 1024).unwrap();
            assert_eq!(again.rows.len(), 3);
            assert_eq!(again.catchup_xor, back.catchup_xor);
            assert_eq!(again.get_row("a").get("n").as_i64(), Some(11));
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn a_corrupt_file_is_refused_when_mapped() {
            let dir = tmpdir("corrupt");
            let coll = collection(&[("a", json!({ "n": 1 }))]);
            let mut bytes = bytes_of(&coll);
            let mid = bytes.len() / 2;
            bytes[mid] ^= 1;
            let path = dir.join("game.rows");
            std::fs::write(&path, bytes).unwrap();
            assert!(map_image(&path, &dir.join("arena"), 64 * 1024).is_err());
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
