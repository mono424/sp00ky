//! Timing harness for the checkpoint readers.
//!
//! Ignored by default: it builds a 600k-row table. Run it in release mode and
//! read the printed numbers:
//!
//! ```text
//! cargo test -p ssp --release --test checkpoint_timing -- --ignored --nocapture
//! ```
//!
//! It measures a warm page cache. The cold number is the file size over the
//! volume's throughput on top.

use ssp::circuit::checkpoint::{read_image, write_collection, FORMAT, MAGIC};
use ssp::circuit::row_codec::{self as codec, RECORD_FORMAT};
use ssp::circuit::store::{Collection, Operation};
use ssp::types::{make_key, Sp00kyValue};
use std::io::{self, BufReader, BufWriter, Read};
use std::time::Instant;

const ROWS: usize = 600_000;

fn row(i: usize) -> serde_json::Value {
    let result = ["1-0", "0-1", "1/2-1/2"][i % 3];
    serde_json::json!({
        "id": format!("game:g{i:07}"),
        "white": format!("player{}", i % 977),
        "black": format!("player{}", (i * 7) % 977),
        "result": result,
        "moves": "e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3 d6 c3 O-O h3 Nb8 d4 Nbd7 c4 c6 cxb5 axb5",
        "rating": 1500 + (i % 800) as i64,
        "_00_rv": i as i64,
        "tags": ["blitz", "rated"],
        "meta": { "site": "lichess", "rated": true, "clock": { "initial": 300, "increment": 3 } }
    })
}

fn build() -> Collection {
    let mut coll = Collection::new("game".to_string());
    for i in 0..ROWS {
        coll.apply(Operation::Create, &format!("g{i:07}"), Sp00kyValue::from(row(i)));
    }
    coll
}

/// The reader this harness replaced: the streaming `read_collection` from
/// before the mapped loader, kept here as the baseline. One copy per record,
/// a key allocation per row, and a reseed walk at the end.
fn legacy_read<R: Read>(input: R) -> io::Result<Collection> {
    struct Hashing<R> {
        inner: R,
        hasher: blake3::Hasher,
    }
    impl<R: Read> Read for Hashing<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.hasher.update(&buf[..n]);
            Ok(n)
        }
    }
    fn u32<R: Read>(r: &mut R) -> io::Result<u32> {
        let mut b = [0u8; 4];
        r.read_exact(&mut b)?;
        Ok(u32::from_le_bytes(b))
    }
    fn u64<R: Read>(r: &mut R) -> io::Result<u64> {
        let mut b = [0u8; 8];
        r.read_exact(&mut b)?;
        Ok(u64::from_le_bytes(b))
    }
    fn chunk_into<R: Read>(r: &mut R, out: &mut Vec<u8>) -> io::Result<()> {
        let len = u32(r)?;
        out.clear();
        out.resize(len as usize, 0);
        r.read_exact(out)
    }

    let mut r = Hashing { inner: input, hasher: blake3::Hasher::new() };
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    assert_eq!(&magic, MAGIC);
    assert_eq!(u32(&mut r)?, FORMAT);
    assert_eq!(u32(&mut r)?, RECORD_FORMAT);
    let mut name = Vec::new();
    chunk_into(&mut r, &mut name)?;
    let name = String::from_utf8(name).unwrap();
    let mut stored_hash = [0u8; 32];
    r.read_exact(&mut stored_hash)?;
    let fields = u32(&mut r)?;
    let mut names = Vec::new();
    for _ in 0..fields {
        let mut n = Vec::new();
        chunk_into(&mut r, &mut n)?;
        names.push(String::from_utf8(n).unwrap());
    }
    let mut coll = Collection::new(name.clone());
    assert!(coll.rows.restore_dict(names.iter().map(String::as_str)));
    let rows = u64(&mut r)?;
    let mut record = Vec::new();
    for _ in 0..rows {
        chunk_into(&mut r, &mut record)?;
        let id = codec::record_id(&record).unwrap();
        coll.zset.insert(make_key(&name, id), 1);
        coll.rows.insert_encoded(&record);
    }
    let computed = r.hasher.finalize();
    let mut trailer = [0u8; 32];
    r.inner.read_exact(&mut trailer)?;
    assert_eq!(computed.as_bytes(), &trailer);
    coll.reseed_catchup_xor();
    assert_eq!(coll.catchup_xor, stored_hash);
    Ok(coll)
}

/// A few hundred ids spread over the table, the same for every reader.
fn sample_ids() -> Vec<String> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..1000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            format!("g{:07}", (x % ROWS as u64) as usize)
        })
        .collect()
}

fn check(coll: &Collection, reference: &Collection, ids: &[String]) {
    assert_eq!(coll.rows.len(), ROWS);
    assert_eq!(coll.catchup_xor, reference.catchup_xor);
    for id in ids {
        assert_eq!(coll.get_row(id).to_owned_value(), reference.get_row(id).to_owned_value(), "{id}");
        assert_eq!(coll.rows.digest_of(id), reference.rows.digest_of(id), "{id}");
    }
}

fn report(what: &str, started: Instant, bytes: u64) {
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    println!(
        "{what:<28} {ms:>9.1} ms  {:>8.0} rows/s  {:>6.1} MB/s",
        ROWS as f64 / (ms / 1000.0),
        bytes as f64 / 1e6 / (ms / 1000.0)
    );
}

#[test]
#[ignore = "builds a 600k-row table; run in release with --ignored --nocapture"]
fn checkpoint_readers() {
    let dir = std::env::temp_dir().join(format!("ssp-checkpoint-timing-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("game.rows");
    let ids = sample_ids();

    let started = Instant::now();
    let reference = build();
    println!("built {ROWS} rows in {:.1} ms", started.elapsed().as_secs_f64() * 1000.0);

    let started = Instant::now();
    let written = {
        let mut out = BufWriter::with_capacity(1 << 20, std::fs::File::create(&path).unwrap());
        write_collection(&reference, &mut out).unwrap()
    };
    report("write", started, written.bytes);
    println!("file: {} bytes, {:.0} bytes/row", written.bytes, written.bytes as f64 / ROWS as f64);

    let started = Instant::now();
    let legacy = legacy_read(BufReader::with_capacity(1 << 20, std::fs::File::open(&path).unwrap())).unwrap();
    report("legacy streaming read", started, written.bytes);
    check(&legacy, &reference, &ids);
    drop(legacy);

    let started = Instant::now();
    let (heap, stats) = read_image(std::fs::read(&path).unwrap()).unwrap();
    report("read_image (heap)", started, written.bytes);
    println!("    verify {} ms, index {} ms", stats.verify_ms, stats.index_ms);
    check(&heap, &reference, &ids);
    drop(heap);

    #[cfg(feature = "mmap-store")]
    {
        let started = Instant::now();
        let (mapped, stats) =
            ssp::circuit::checkpoint::map_image(&path, &dir.join("arena"), 64 << 20).unwrap();
        report("map_image (mmap)", started, written.bytes);
        println!("    verify {} ms, index {} ms", stats.verify_ms, stats.index_ms);
        check(&mapped, &reference, &ids);
        // Writes after a mapped load go to a fresh segment and the table
        // still checkpoints.
        let mut mapped = mapped;
        mapped.apply(Operation::Update, "g0000001", Sp00kyValue::from(row(1_000_001)));
        let mut out = Vec::new();
        write_collection(&mapped, &mut out).unwrap();
        let (again, _) = read_image(out).unwrap();
        assert_eq!(again.rows.len(), ROWS);
        assert_eq!(again.catchup_xor, mapped.catchup_xor);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
