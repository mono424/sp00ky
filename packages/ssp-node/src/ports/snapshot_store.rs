//! Durable row images plus the change journal they are read back with (see
//! [`crate::snapshot`]).
//!
//! A host keeps one FORMAT 2 image per synced table
//! (`ssp::circuit::checkpoint`) and the journal of change notices it accepted.
//! Each image records the journal position it covers, so a warm start reads
//! the images and re-reads from the database only the records noticed after
//! that position. The Cloudflare adapter keeps both in the Durable Object's
//! SQLite (`apps/ssp-cf`); the methods are synchronous because that storage
//! is.

use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use ssp_protocol::IngestRequest;

use super::MaybeSendSync;

/// One table's current image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableImage {
    pub table: String,
    pub image_id: u64,
    /// The image holds every journal notice up to and including this seq.
    pub covered_seq: i64,
    /// The table's catch-up hash when it was written (`xor_acc_to_hex`): a
    /// checkpoint rewrites only tables whose hash moved.
    pub xor: String,
    pub bytes: u64,
}

/// What a set of images is valid for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// [`crate::snapshot::SNAPSHOT_FORMAT`].
    pub format: u32,
    /// The ssp-node version that wrote the images. Another version rebuilds.
    pub node_version: String,
    /// Upstream's schema fingerprint the circuit held.
    pub fingerprint: String,
    /// Whether row versions came from the `_00_version` ledger.
    pub has_versions: bool,
    pub saved_at_epoch_ms: u64,
}

pub trait SnapshotStore: MaybeSendSync {
    fn meta(&self) -> Option<SnapshotMeta>;
    fn set_meta(&self, meta: &SnapshotMeta) -> anyhow::Result<()>;
    fn images(&self) -> anyhow::Result<Vec<TableImage>>;
    /// The image's bytes, in one buffer.
    fn read_image(&self, image: &TableImage) -> anyhow::Result<Vec<u8>>;
    /// Write a table's image with `write` and make it the table's current one,
    /// atomically: a failure leaves the previous image in place.
    fn write_image(
        &self,
        image: &TableImage,
        write: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<()>,
    ) -> anyhow::Result<u64>;
    /// Move an unchanged image's coverage forward.
    fn set_covered(&self, table: &str, covered_seq: i64) -> anyhow::Result<()>;
    fn drop_image(&self, table: &str) -> anyhow::Result<()>;
    /// Forget every image and the meta. The journal stays.
    fn clear(&self) -> anyhow::Result<()>;

    /// Journal rows after `after`, in order: `(seq, notices)`.
    fn journal_since(&self, after: i64) -> anyhow::Result<Vec<(i64, Vec<IngestRequest>)>>;
    /// The highest seq such that it and every notice before it is applied to
    /// the circuit (0 when nothing is).
    fn applied_through(&self) -> i64;
    /// Drop journal rows up to and including `seq`.
    fn trim_journal(&self, through: i64) -> anyhow::Result<()>;
}

/// In-memory [`SnapshotStore`], for tests.
#[derive(Default)]
pub struct MemorySnapshotStore {
    inner: Mutex<MemoryInner>,
}

#[derive(Default)]
struct MemoryInner {
    meta: Option<SnapshotMeta>,
    images: BTreeMap<String, (TableImage, Vec<u8>)>,
    journal: Vec<(i64, Vec<IngestRequest>, bool)>,
    last_seq: i64,
}

impl MemorySnapshotStore {
    /// Append a journal row; `applied` as the host would mark it.
    pub fn journal(&self, notices: Vec<IngestRequest>, applied: bool) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        inner.last_seq += 1;
        let seq = inner.last_seq;
        inner.journal.push((seq, notices, applied));
        seq
    }

    /// Mark every journal row up to `seq` applied, as the host's drain does.
    pub fn mark_applied(&self, through: i64) {
        for row in self.inner.lock().unwrap().journal.iter_mut().filter(|r| r.0 <= through) {
            row.2 = true;
        }
    }

    pub fn journal_len(&self) -> usize {
        self.inner.lock().unwrap().journal.len()
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn meta(&self) -> Option<SnapshotMeta> {
        self.inner.lock().unwrap().meta.clone()
    }

    fn set_meta(&self, meta: &SnapshotMeta) -> anyhow::Result<()> {
        self.inner.lock().unwrap().meta = Some(meta.clone());
        Ok(())
    }

    fn images(&self) -> anyhow::Result<Vec<TableImage>> {
        Ok(self.inner.lock().unwrap().images.values().map(|(m, _)| m.clone()).collect())
    }

    fn read_image(&self, image: &TableImage) -> anyhow::Result<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        match inner.images.get(&image.table) {
            Some((m, bytes)) if m.image_id == image.image_id => Ok(bytes.clone()),
            _ => Err(anyhow::anyhow!("no image {} for {}", image.image_id, image.table)),
        }
    }

    fn write_image(
        &self,
        image: &TableImage,
        write: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<()>,
    ) -> anyhow::Result<u64> {
        let mut bytes = Vec::new();
        write(&mut bytes)?;
        let n = bytes.len() as u64;
        let meta = TableImage { bytes: n, ..image.clone() };
        self.inner.lock().unwrap().images.insert(image.table.clone(), (meta, bytes));
        Ok(n)
    }

    fn set_covered(&self, table: &str, covered_seq: i64) -> anyhow::Result<()> {
        if let Some((m, _)) = self.inner.lock().unwrap().images.get_mut(table) {
            m.covered_seq = covered_seq;
        }
        Ok(())
    }

    fn drop_image(&self, table: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().images.remove(table);
        Ok(())
    }

    fn clear(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.images.clear();
        inner.meta = None;
        Ok(())
    }

    fn journal_since(&self, after: i64) -> anyhow::Result<Vec<(i64, Vec<IngestRequest>)>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .journal
            .iter()
            .filter(|r| r.0 > after)
            .map(|r| (r.0, r.1.clone()))
            .collect())
    }

    fn applied_through(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        match inner.journal.iter().find(|r| !r.2) {
            Some(first_pending) => first_pending.0 - 1,
            None => inner.last_seq,
        }
    }

    fn trim_journal(&self, through: i64) -> anyhow::Result<()> {
        self.inner.lock().unwrap().journal.retain(|r| r.0 > through);
        Ok(())
    }
}
