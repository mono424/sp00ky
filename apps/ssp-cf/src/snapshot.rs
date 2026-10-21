//! [`SnapshotStore`] over the Durable Object's SQLite: one FORMAT 2 image per
//! table in 1 MiB parts (a row holds at most 2 MB), the snapshot meta in
//! `ssp_meta`, and the change journal, which is the inbox (`ssp_inbox`) with
//! its applied rows kept until every image covers them.
//!
//! All of it is synchronous SQLite, so one table's parts, its manifest row and
//! the removal of its previous image are written in one event with no await
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
        tbl TEXT PRIMARY KEY, image_id TEXT NOT NULL, covered_seq INTEGER NOT NULL,
        xor TEXT NOT NULL, bytes INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS snap_part (
        tbl TEXT NOT NULL, image_id TEXT NOT NULL, part INTEGER NOT NULL, data BLOB NOT NULL,
        PRIMARY KEY (tbl, image_id, part));";

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

/// `Write` that cuts the stream into `PART`-sized rows of one image.
struct PartWriter<'a> {
    sql: &'a SqlStorage,
    tbl: &'a str,
    image_id: &'a str,
    part: i64,
    buf: Vec<u8>,
    total: u64,
}

impl PartWriter<'_> {
    fn put(&mut self, upto: usize) -> io::Result<()> {
        let view = js_sys::Uint8Array::from(&self.buf[..upto]);
        self.sql
            .exec_raw(
                "INSERT OR REPLACE INTO snap_part (tbl, image_id, part, data) VALUES (?, ?, ?, ?)",
                vec![self.tbl.into(), self.image_id.into(), (self.part as f64).into(), view.into()],
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
        Ok(rows(&sql, "SELECT tbl, image_id, covered_seq, xor, bytes FROM snap_table", vec![])?
            .iter()
            .filter_map(|r| {
                Some(TableImage {
                    table: text(r, "tbl"),
                    image_id: text(r, "image_id").parse().ok()?,
                    covered_seq: int(r, "covered_seq"),
                    xor: text(r, "xor"),
                    bytes: int(r, "bytes") as u64,
                })
            })
            .collect())
    }

    /// Parts copied straight into one buffer sized from the manifest: one copy
    /// out of SQLite, no regrowth, and `read_image` adopts the buffer as the
    /// table's arena (spike S1).
    fn read_image(&self, image: &TableImage) -> anyhow::Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::with_capacity(image.bytes as usize);
        let cursor = self
            .storage
            .sql()
            .exec(
                "SELECT data FROM snap_part WHERE tbl = ? AND image_id = ? ORDER BY part",
                vec![image.table.clone().into(), image.image_id.to_string().into()],
            )
            .map_err(err)?;
        let key = wasm_bindgen::JsValue::from_str("data");
        for row in cursor {
            let row = row.map_err(err)?;
            let data = js_sys::Reflect::get(&row, &key).map_err(|e| err(format!("{e:?}")))?;
            let part = js_sys::Uint8Array::new(&data.unchecked_into::<js_sys::ArrayBuffer>());
            let n = part.length() as usize;
            if buf.len() + n > buf.capacity() {
                anyhow::bail!("image parts of {} exceed the manifest's {} bytes", image.table, image.bytes);
            }
            // Safety: exactly `n` bytes go into spare capacity checked above,
            // and the length moves only after the copy.
            unsafe {
                part.raw_copy_to_ptr(buf.as_mut_ptr().add(buf.len()));
                buf.set_len(buf.len() + n);
            }
        }
        if buf.len() as u64 != image.bytes {
            anyhow::bail!("image of {} has {} of {} bytes", image.table, buf.len(), image.bytes);
        }
        Ok(buf)
    }

    fn write_image(
        &self,
        image: &TableImage,
        write: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<()>,
    ) -> anyhow::Result<u64> {
        let sql = self.storage.sql();
        let image_id = image.image_id.to_string();
        let mut out = PartWriter {
            sql: &sql,
            tbl: &image.table,
            image_id: &image_id,
            part: 0,
            buf: Vec::with_capacity(PART + 64 * 1024),
            total: 0,
        };
        let written = write(&mut out).and_then(|_| out.finish());
        if let Err(e) = written {
            let _ = sql.exec(
                "DELETE FROM snap_part WHERE tbl = ? AND image_id = ?",
                vec![image.table.clone().into(), image_id.clone().into()],
            );
            return Err(err(e));
        }
        let total = out.total;
        sql.exec(
            "INSERT OR REPLACE INTO snap_table (tbl, image_id, covered_seq, xor, bytes) VALUES (?, ?, ?, ?, ?)",
            vec![
                image.table.clone().into(),
                image_id.clone().into(),
                image.covered_seq.into(),
                image.xor.clone().into(),
                (total as i64).into(),
            ],
        )
        .map_err(err)?;
        sql.exec(
            "DELETE FROM snap_part WHERE tbl = ? AND image_id != ?",
            vec![image.table.clone().into(), image_id.into()],
        )
        .map_err(err)?;
        Ok(total)
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
