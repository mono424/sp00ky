//! Timing harness for the checkpoint readers.
//!
//! Ignored by default: it builds a 600k-row table. Run it in release mode and
//! read the printed numbers:
//!
//! ```text
//! cargo test -p ssp --release --test checkpoint_timing -- --ignored --nocapture
//! ```
//!
//! It measures a warm page cache. The cold number is the bytes a step reads
//! over the volume's throughput on top: the heads for a mapped load, the
//! whole file for a verification.

use ssp::circuit::checkpoint::{fresh_image_id, read_image, write_collection};
use ssp::circuit::store::{Collection, Operation};
use ssp::types::Sp00kyValue;
use std::io::BufWriter;
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
        "{what:<30} {ms:>9.1} ms  {:>8.0} rows/s  {:>7.1} MB/s",
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
        write_collection(&reference, fresh_image_id(), &mut out).unwrap()
    };
    report("write", started, written.bytes);
    println!(
        "file: {} bytes, {:.0} bytes/row, heads {} bytes ({:.0}/row), bodies {} bytes",
        written.bytes,
        written.bytes as f64 / ROWS as f64,
        written.heads_bytes,
        written.heads_bytes as f64 / ROWS as f64,
        written.bodies_bytes
    );

    let started = Instant::now();
    let (heap, stats) = read_image(std::fs::read(&path).unwrap()).unwrap();
    report("read_image (heap, verified)", started, written.bytes);
    println!("    heads {} ms, bodies {} ms", stats.heads_ms, stats.verify_ms);
    check(&heap, &reference, &ids);
    drop(heap);

    #[cfg(feature = "mmap-store")]
    {
        use ssp::circuit::checkpoint::{map_image, verify_bodies, BodyVerify, Throttle};
        let started = Instant::now();
        let mapped = map_image(&path, &dir.join("arena"), 64 << 20, BodyVerify::Deferred).unwrap();
        report("map_image (heads only)", started, written.bytes - written.bodies_bytes);
        println!("    heads {} ms", mapped.stats.heads_ms);
        check(&mapped.collection, &reference, &ids);

        let pending = mapped.pending.unwrap();
        let started = Instant::now();
        assert!(verify_bodies(&pending.image, Throttle::none()).unwrap());
        report("verify_bodies", started, written.bodies_bytes);

        let started = Instant::now();
        let at_load = map_image(&path, &dir.join("arena"), 64 << 20, BodyVerify::AtLoad).unwrap();
        report("map_image (verified at load)", started, written.bytes);
        println!("    heads {} ms, bodies {} ms", at_load.stats.heads_ms, at_load.stats.verify_ms);
        drop(at_load);

        // Writes after a mapped load go to a fresh segment and the table
        // still checkpoints.
        let mut mapped = mapped.collection;
        mapped.apply(Operation::Update, "g0000001", Sp00kyValue::from(row(1_000_001)));
        let mut out = Vec::new();
        write_collection(&mapped, fresh_image_id(), &mut out).unwrap();
        let (again, _) = read_image(out).unwrap();
        assert_eq!(again.rows.len(), ROWS);
        assert_eq!(again.catchup_xor, mapped.catchup_xor);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
