//! Binary checkpoint of one collection's rows.
//!
//! What a cluster SSP keeps across a restart so it does not have to page the
//! whole database through the scheduler again. Rows only: views, operator
//! state and table metadata are rebuilt on boot from `_00_query` and the
//! upstream schema. That is also what keeps them right, since operator state
//! is a function of the rows and registering a view primes it from them.
//!
//! Records are written exactly as the row table holds them, next to the field
//! dictionary they were encoded against. Writing is a copy of bytes, and
//! reading appends them to a fresh arena without decoding a single value.
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
//! Integers are little-endian. A read has two independent checks: the blake3
//! trailer catches torn or flipped bytes, and the catch-up hash re-seeded from
//! the loaded rows' digests has to equal the one stored.

use crate::circuit::row_codec::{self as codec, RECORD_FORMAT};
use crate::circuit::store::Collection;
use crate::types::make_key;
use std::io::{self, Read, Write};

pub const MAGIC: &[u8; 8] = b"SPKYROWS";
pub const FORMAT: u32 = 1;

/// Upper bound on any one length field, so a corrupt length cannot ask for a
/// multi-gigabyte allocation before the trailer gets to reject the file.
const MAX_CHUNK: u32 = 256 << 20;

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

/// Write `coll`'s rows to `out`. Returns the number of rows written.
pub fn write_collection<W: Write>(coll: &Collection, out: W) -> io::Result<u64> {
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
    Ok(rows)
}

/// Read a collection written by [`write_collection`]. The rows land in a
/// fresh arena of the configured kind (file-backed where the shell set one
/// up), with the z-set and the catch-up hash rebuilt from them.
pub fn read_collection<R: Read>(input: R) -> Result<Collection, CheckpointError> {
    let mut r = Hashing::new(input);

    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid("not a row checkpoint"));
    }
    let format = read_u32(&mut r)?;
    if format != FORMAT {
        return Err(invalid(format!("checkpoint format {format}, this build reads {FORMAT}")));
    }
    let record_format = read_u32(&mut r)?;
    if record_format != RECORD_FORMAT {
        return Err(invalid(format!(
            "record format {record_format}, this build encodes {RECORD_FORMAT}"
        )));
    }
    let name = String::from_utf8(read_chunk(&mut r)?).map_err(|_| invalid("table name is not UTF-8"))?;
    let mut stored_hash = [0u8; 32];
    r.read_exact(&mut stored_hash)?;

    let fields = read_u32(&mut r)?;
    let mut names = Vec::with_capacity(fields.min(4096) as usize);
    for _ in 0..fields {
        names.push(String::from_utf8(read_chunk(&mut r)?).map_err(|_| invalid("field name is not UTF-8"))?);
    }

    let mut coll = Collection::new(name.clone());
    if !coll.rows.restore_dict(names.iter().map(String::as_str)) {
        return Err(invalid("field dictionary repeats a name"));
    }

    let rows = read_u64(&mut r)?;
    let mut record = Vec::new();
    for _ in 0..rows {
        read_chunk_into(&mut r, &mut record)?;
        let Some(id) = codec::record_id(&record) else {
            return Err(invalid("record without a readable id"));
        };
        coll.zset.insert(make_key(&name, id), 1);
        coll.rows.insert_encoded(&record);
    }

    let computed = r.hasher.finalize();
    let mut trailer = [0u8; 32];
    r.inner.read_exact(&mut trailer)?;
    if computed.as_bytes() != &trailer {
        return Err(invalid("checksum mismatch"));
    }

    coll.reseed_catchup_xor();
    if coll.catchup_xor != stored_hash {
        return Err(invalid("rows do not add up to the stored catch-up hash"));
    }
    Ok(coll)
}

/// A reader or writer that feeds every byte through blake3 on the way.
struct Hashing<T> {
    inner: T,
    hasher: blake3::Hasher,
}

impl<T> Hashing<T> {
    fn new(inner: T) -> Self {
        Self { inner, hasher: blake3::Hasher::new() }
    }
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
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

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_chunk<R: Read>(r: &mut R) -> Result<Vec<u8>, CheckpointError> {
    let mut out = Vec::new();
    read_chunk_into(r, &mut out)?;
    Ok(out)
}

fn read_chunk_into<R: Read>(r: &mut R, out: &mut Vec<u8>) -> Result<(), CheckpointError> {
    let len = read_u32(r)?;
    if len > MAX_CHUNK {
        return Err(invalid(format!("chunk of {len} bytes")));
    }
    out.clear();
    out.resize(len as usize, 0);
    r.read_exact(out)?;
    Ok(())
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

    fn round_trip(coll: &Collection) -> Result<Collection, CheckpointError> {
        let mut bytes = Vec::new();
        write_collection(coll, &mut bytes).unwrap();
        read_collection(bytes.as_slice())
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
        let mut keys: Vec<_> = back.zset.keys().map(|k| k.to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["game:a", "game:b", "game:⟨odd id⟩"]);
        assert!(back.zset.values().all(|w| *w == 1));
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
    fn a_flipped_byte_is_rejected() {
        let coll = collection(&[("a", json!({ "title": "hello" }))]);
        let mut bytes = Vec::new();
        write_collection(&coll, &mut bytes).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x40;
        assert!(read_collection(bytes.as_slice()).is_err());
    }

    #[test]
    fn a_truncated_file_is_rejected() {
        let coll = collection(&[("a", json!({ "title": "hello" })), ("b", json!({ "n": 2 }))]);
        let mut bytes = Vec::new();
        write_collection(&coll, &mut bytes).unwrap();
        bytes.truncate(bytes.len() - 40);
        assert!(read_collection(bytes.as_slice()).is_err());
    }

    #[test]
    fn another_record_format_is_refused() {
        let coll = collection(&[("a", json!({ "n": 1 }))]);
        let mut bytes = Vec::new();
        write_collection(&coll, &mut bytes).unwrap();
        bytes[12..16].copy_from_slice(&(RECORD_FORMAT + 1).to_le_bytes());
        let err = read_collection(bytes.as_slice()).unwrap_err();
        assert!(err.to_string().contains("record format"), "{err}");
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
}
