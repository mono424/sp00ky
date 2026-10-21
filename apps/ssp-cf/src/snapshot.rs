//! [`SnapshotStore`] over the Durable Object's SQLite: one FORMAT 2 chain per
//! table (a base and its deltas, each file in 1 MiB parts: a row holds at most
//! 2 MB), the snapshot meta in
//! `ssp_meta`, and the change journal, which is the inbox (`ssp_inbox`) with
//! its applied rows kept until every image covers them.
//!
//! All of it is synchronous SQLite, so a file's parts, its manifest row and
//! the removal of a replaced chain are written in one event with no await
//! between them, and the Durable Object commits them together.

use std::io;
use std::rc::Rc;

use ssp_node::ports::{SnapshotMeta, SnapshotStore, TableImage};
use ssp_protocol::IngestRequest;
use wasm_bindgen::JsCast;
use worker::{SqlStorage, Storage};

/// Bytes per stored part.
const PART: usize = 1 << 20;

pub const TABLES: &str = "
    CREATE TABLE IF NOT EXISTS snap_table (
        tbl TEXT PRIMARY KEY, image_id TEXT NOT NULL, seq INTEGER NOT NULL,
        covered_seq INTEGER NOT NULL, xor TEXT NOT NULL,
        base_bytes INTEGER NOT NULL, delta_bytes INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS snap_part (
        tbl TEXT NOT NULL, image_id TEXT NOT NULL, seq INTEGER NOT NULL, part INTEGER NOT NULL,
        data BLOB NOT NULL, PRIMARY KEY (tbl, image_id, seq, part));";

const META_KEY: &str = "snapshot";

pub struct CfSnapshot {
    pub storage: Rc<Storage>,
}

fn err(e: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

fn rows(sql: &SqlStorage, query: &str, binds: Vec<worker::SqlStorageValue>) -> anyhow::Result<Vec<serde_json::Value>> {
    sql.exec(query, binds).and_then(|c| c.to_array()).map_err(err)
}

fn int(row: &serde_json::Value, key: &str) -> i64 {
    row.get(key).and_then(serde_json::Value::as_f64).map(|v| v as i64).unwrap_or(0)
}

fn text(row: &serde_json::Value, key: &str) -> String {
    row.get(key).and_then(serde_json::Value::as_str).unwrap_or_default().to_string()
}

/// `Write` that cuts the stream into `PART`-sized rows of one file.
struct PartWriter<'a> {
    sql: &'a SqlStorage,
    tbl: &'a str,
    image_id: &'a str,
    seq: u32,
    part: i64,
    buf: Vec<u8>,
    total: u64,
}

impl PartWriter<'_> {
    fn put(&mut self, upto: usize) -> io::Result<()> {
        let view = js_sys::Uint8Array::from(&self.buf[..upto]);
        self.sql
            .exec_raw(
                "INSERT OR REPLACE INTO snap_part (tbl, image_id, seq, part, data) VALUES (?, ?, ?, ?, ?)",
                vec![
                    self.tbl.into(),
                    self.image_id.into(),
                    (self.seq as f64).into(),
                    (self.part as f64).into(),
                    view.into(),
                ],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.part += 1;
        self.total += upto as u64;
        self.buf.drain(..upto);
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let n = self.buf.len();
            self.put(n)?;
        }
        Ok(())
    }
}

impl io::Write for PartWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(bytes);
        while self.buf.len() >= PART {
            self.put(PART)?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SnapshotStore for CfSnapshot {
    fn meta(&self) -> Option<SnapshotMeta> {
        let rows = rows(&self.storage.sql(), "SELECT v FROM ssp_meta WHERE k = ?", vec![META_KEY.into()]).ok()?;
        serde_json::from_str(rows.first()?.get("v")?.as_str()?).ok()
    }

    fn set_meta(&self, meta: &SnapshotMeta) -> anyhow::Result<()> {
        let v = serde_json::to_string(meta)?;
        self.storage
            .sql()
            .exec("INSERT OR REPLACE INTO ssp_meta (k, v) VALUES (?, ?)", vec![META_KEY.into(), v.into()])
            .map_err(err)?;
        Ok(())
    }

    fn images(&self) -> anyhow::Result<Vec<TableImage>> {
        let sql = self.storage.sql();
        Ok(rows(&sql, "SELECT tbl, image_id, seq, covered_seq, xor, base_bytes, delta_bytes FROM snap_table", vec![])?
            .iter()
            .filter_map(|r| {
                Some(TableImage {
                    table: text(r, "tbl"),
                    image_id: text(r, "image_id").parse().ok()?,
                    seq: int(r, "seq") as u32,
                    covered_seq: int(r, "covered_seq"),
                    xor: text(r, "xor"),
                    base_bytes: int(r, "base_bytes") as u64,
                    delta_bytes: int(r, "delta_bytes") as u64,
                })
            })
            .collect())
    }

    /// Each file's parts copied straight into one buffer sized from its
    /// parts: one copy out of SQLite, no regrowth, and the base buffer becomes
    /// the table's arena (spike S1).
    fn read_files(&self, image: &TableImage) -> anyhow::Result<(Vec<u8>, Vec<Vec<u8>>)> {
        let sql = self.storage.sql();
        let image_id = image.image_id.to_string();
        let sizes = rows(
            &sql,
            "SELECT seq, SUM(length(data)) AS n FROM snap_part WHERE tbl = ? AND image_id = ? GROUP BY seq ORDER BY seq",
            vec![image.table.clone().into(), image_id.clone().into()],
        )?;
        let mut files = Vec::with_capacity(sizes.len());
        for (expected, row) in sizes.iter().enumerate() {
            let seq = int(row, "seq");
            if seq != expected as i64 {
                anyhow::bail!("chain of {} is missing file {expected}", image.table);
            }
            files.push(read_file(&sql, &image.table, &image_id, seq, int(row, "n") as usize)?);
        }
        if files.len() != image.seq as usize + 1 {
            anyhow::bail!("chain of {} has {} files, the manifest {}", image.table, files.len(), image.seq + 1);
        }
        let base = files.remove(0);
        Ok((base, files))
    }

    fn write_file(
        &self,
        image: &TableImage,
        write: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<()>,
    ) -> anyhow::Result<u64> {
        let sql = self.storage.sql();
        let image_id = image.image_id.to_string();
        if image.seq > 0 {
            let current = self.images()?.into_iter().find(|i| i.table == image.table);
            if !current.is_some_and(|c| c.image_id == image.image_id && c.seq + 1 == image.seq) {
                anyhow::bail!("delta {} does not extend {}'s chain", image.seq, image.table);
            }
        }
        let mut out = PartWriter {
            sql: &sql,
            tbl: &image.table,
            image_id: &image_id,
            seq: image.seq,
            part: 0,
            buf: Vec::with_capacity(PART + 64 * 1024),
            total: 0,
        };
        let written = write(&mut out).and_then(|_| out.finish());
        if let Err(e) = written {
            let _ = sql.exec(
                "DELETE FROM snap_part WHERE tbl = ? AND image_id = ? AND seq = ?",
                vec![image.table.clone().into(), image_id.clone().into(), (image.seq as i64).into()],
            );
            return Err(err(e));
        }
        let total = out.total as i64;
        if image.seq == 0 {
            sql.exec(
                "INSERT OR REPLACE INTO snap_table (tbl, image_id, seq, covered_seq, xor, base_bytes, delta_bytes) \
                 VALUES (?, ?, 0, ?, ?, ?, 0)",
                vec![
                    image.table.clone().into(),
                    image_id.clone().into(),
                    image.covered_seq.into(),
                    image.xor.clone().into(),
                    total.into(),
                ],
            )
            .map_err(err)?;
            sql.exec(
                "DELETE FROM snap_part WHERE tbl = ? AND image_id != ?",
                vec![image.table.clone().into(), image_id.into()],
            )
            .map_err(err)?;
        } else {
            sql.exec(
                "UPDATE snap_table SET seq = ?, covered_seq = ?, xor = ?, delta_bytes = delta_bytes + ? WHERE tbl = ?",
                vec![
                    (image.seq as i64).into(),
                    image.covered_seq.into(),
                    image.xor.clone().into(),
                    total.into(),
                    image.table.clone().into(),
                ],
            )
            .map_err(err)?;
        }
        Ok(total as u64)
    }

    fn set_covered(&self, table: &str, covered_seq: i64) -> anyhow::Result<()> {
        self.storage
            .sql()
            .exec("UPDATE snap_table SET covered_seq = ? WHERE tbl = ?", vec![covered_seq.into(), table.into()])
            .map_err(err)?;
        Ok(())
    }

    fn drop_image(&self, table: &str) -> anyhow::Result<()> {
        let sql = self.storage.sql();
        sql.exec("DELETE FROM snap_part WHERE tbl = ?", vec![table.into()]).map_err(err)?;
        sql.exec("DELETE FROM snap_table WHERE tbl = ?", vec![table.into()]).map_err(err)?;
        Ok(())
    }

    fn clear(&self) -> anyhow::Result<()> {
        let sql = self.storage.sql();
        sql.exec("DELETE FROM snap_part", None).map_err(err)?;
        sql.exec("DELETE FROM snap_table", None).map_err(err)?;
        sql.exec("DELETE FROM ssp_meta WHERE k = ?", vec![META_KEY.into()]).map_err(err)?;
        Ok(())
    }

    fn journal_since(&self, after: i64) -> anyhow::Result<Vec<(i64, Vec<IngestRequest>)>> {
        let rows = rows(
            &self.storage.sql(),
            "SELECT seq, body FROM ssp_inbox WHERE kind = 'ingest' AND seq > ? ORDER BY seq",
            vec![after.into()],
        )?;
        Ok(rows
            .iter()
            .filter_map(|r| Some((int(r, "seq"), serde_json::from_str::<Vec<IngestRequest>>(r.get("body")?.as_str()?).ok()?)))
            .collect())
    }

    fn applied_through(&self) -> i64 {
        let sql = self.storage.sql();
        let first_pending = rows(&sql, "SELECT MIN(seq) AS s FROM ssp_inbox WHERE applied = 0", vec![])
            .ok()
            .and_then(|r| r.first().and_then(|r| r.get("s")).and_then(serde_json::Value::as_f64));
        match first_pending {
            Some(seq) => seq as i64 - 1,
            None => rows(&sql, "SELECT MAX(seq) AS s FROM ssp_inbox", vec![])
                .ok()
                .and_then(|r| r.first().and_then(|r| r.get("s")).and_then(serde_json::Value::as_f64))
                .map(|s| s as i64)
                .unwrap_or(0),
        }
    }

    fn trim_journal(&self, through: i64) -> anyhow::Result<()> {
        self.storage
            .sql()
            .exec("DELETE FROM ssp_inbox WHERE seq <= ? AND applied = 1", vec![through.into()])
            .map_err(err)?;
        Ok(())
    }
}

/// One file of a chain, into a buffer of exactly `bytes`.
fn read_file(sql: &SqlStorage, tbl: &str, image_id: &str, seq: i64, bytes: usize) -> anyhow::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(bytes);
    let cursor = sql
        .exec(
            "SELECT data FROM snap_part WHERE tbl = ? AND image_id = ? AND seq = ? ORDER BY part",
            vec![tbl.into(), image_id.into(), seq.into()],
        )
        .map_err(err)?;
    let key = wasm_bindgen::JsValue::from_str("data");
    for row in cursor {
        let row = row.map_err(err)?;
        let data = js_sys::Reflect::get(&row, &key).map_err(|e| err(format!("{e:?}")))?;
        let part = js_sys::Uint8Array::new(&data.unchecked_into::<js_sys::ArrayBuffer>());
        let n = part.length() as usize;
        if buf.len() + n > buf.capacity() {
            anyhow::bail!("parts of {tbl} file {seq} exceed {bytes} bytes");
        }
        // Safety: exactly `n` bytes go into spare capacity checked above, and
        // the length moves only after the copy.
        unsafe {
            part.raw_copy_to_ptr(buf.as_mut_ptr().add(buf.len()));
            buf.set_len(buf.len() + n);
        }
    }
    if buf.len() != bytes {
        anyhow::bail!("{tbl} file {seq} has {} of {bytes} bytes", buf.len());
    }
    Ok(buf)
}
