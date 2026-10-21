//! Warm start from durable row images instead of a full rebuild.
//!
//! A cold node pages every synced table out of the database. With a
//! [`SnapshotStore`] it instead reads each table's FORMAT 2 image
//! (`ssp::circuit::checkpoint`), then re-reads only the records the journal
//! noticed after the position the image covers, and registers the persisted
//! views as a rebuild does. Re-reading by id is the same rule replay follows
//! ([`crate::replay`]): a notice says "this record changed", the database
//! says what it is now, so an image of any age is caught up exactly.
//!
//! A snapshot is only used when it was written by this node version under the
//! schema upstream has now; anything else rebuilds. Writes that bypass the
//! journal (an import straight into SurrealDB) are not seen until the
//! snapshot's age limit forces a rebuild, or an `/admin/reload`.
//!
//! [`SspNode::checkpoint_step`] keeps the images current: for each table whose
//! catch-up hash moved it appends a delta (the rows changed and removed since
//! the table's watermark) to the table's chain, or writes a new base once the
//! deltas outweigh it, a byte budget at a time (each step holds the host's
//! storage for its writes), and trims the journal once every table's chain
//! covers a position.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Context;
use serde_json::Value;
use ssp::circuit::checkpoint;
use ssp::circuit::store::Collection;
use ssp::circuit::{Operation, Record};
use ssp_protocol::snapshot_hash::xor_acc_to_hex;

use crate::ports::{SnapshotMeta, SnapshotStore, TableImage};
use crate::status::SspStatus;
use crate::SspNode;

/// Bumped when what a snapshot holds changes meaning.
pub const SNAPSHOT_FORMAT: u32 = 1;

/// Images are only read back by the node version that wrote them.
pub const NODE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Writes one checkpoint file of a table.
type WriteFile<'a> = dyn FnMut(&mut dyn std::io::Write) -> std::io::Result<checkpoint::Written> + 'a;

/// Records re-read per query during the catch-up.
const CATCH_UP_CHUNK: usize = 500;

/// Deltas on one base before the next write is a new base (each delta is a
/// file to read back at the next warm start).
pub const MAX_DELTAS: u32 = 16;

/// The rule `apps/ssp/src/warm.rs` follows: rebase once the deltas would make
/// up more than a third of what a warm start reads.
fn deltas_outweigh(base_bytes: u64, delta_bytes: u64) -> bool {
    delta_bytes * 2 > base_bytes
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WarmReport {
    pub tables_from_images: usize,
    pub tables_from_db: usize,
    /// Journal notices after the images, by distinct record.
    pub noticed: usize,
    pub caught_up: usize,
    pub deleted: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CheckpointReport {
    /// Tables whose chain got a file.
    pub written: Vec<String>,
    /// Of those, the ones that got a delta rather than a new base.
    pub deltas: usize,
    pub bytes: u64,
    /// Tables still to write (the byte budget ran out).
    pub remaining: usize,
    /// Why nothing was written, when nothing could be.
    pub skipped: Option<&'static str>,
    /// Journal rows up to this seq were dropped.
    pub trimmed_through: Option<i64>,
}

impl SspNode {
    /// Load the circuit from `store` instead of rebuilding it. `Ok(None)`
    /// means the snapshot is unusable (missing, another version or schema,
    /// too old) and the caller should rebuild; the circuit is untouched then.
    /// An error means the database failed mid-way: the circuit is partial and
    /// the caller must reset it.
    pub async fn warm_bootstrap(
        &self,
        store: &dyn SnapshotStore,
        max_age_secs: u64,
    ) -> anyhow::Result<Option<WarmReport>> {
        let Some(meta) = store.meta() else {
            tracing::info!("No snapshot — full rebuild");
            return Ok(None);
        };
        if meta.format != SNAPSHOT_FORMAT || meta.node_version != NODE_VERSION {
            tracing::info!(format = meta.format, version = %meta.node_version, "Snapshot from another node version — full rebuild");
            return Ok(None);
        }
        let age_ms = crate::now_epoch_ms().saturating_sub(meta.saved_at_epoch_ms);
        if age_ms > max_age_secs.saturating_mul(1000) {
            tracing::info!(age_ms, "Snapshot too old — full rebuild");
            return Ok(None);
        }
        let db = self.platform.db.as_ref();
        let schema = crate::bootstrap::load_schema(db).await?;
        let Some(probe) = schema.probe.as_ref() else {
            return Ok(None);
        };
        if probe.fingerprint != meta.fingerprint || schema.has_versions != meta.has_versions {
            tracing::info!(snapshot = %meta.fingerprint, upstream = %probe.fingerprint, "Schema moved since the snapshot — full rebuild");
            return Ok(None);
        }

        let images: HashMap<String, TableImage> =
            store.images()?.into_iter().map(|i| (i.table.clone(), i)).collect();
        crate::bootstrap::apply_schema(&mut *self.processor.write().await, &schema.tables);
        let mut report = WarmReport::default();
        let mut covered: HashMap<String, i64> = HashMap::new();
        for (table, table_meta) in &schema.tables {
            let loaded = images.get(table).and_then(|image| {
                let (base, deltas) = store
                    .read_files(image)
                    .map_err(|e| tracing::warn!(table = %table, error = %format!("{e:#}"), "reading a table image failed"))
                    .ok()?;
                // Sets the table's watermark at the chain's end, so the next
                // checkpoint of it can be a delta.
                let (coll, end) = checkpoint::read_table_bytes(base, deltas)
                    .map_err(|e| tracing::warn!(table = %table, error = %e, "unreadable table image"))
                    .ok()?;
                let sound = coll.name == *table
                    && end.image_id == image.image_id
                    && end.seq == image.seq
                    && xor_acc_to_hex(&coll.catchup_xor) == image.xor;
                sound.then_some((coll, image.covered_seq))
            });
            match loaded {
                Some((coll, seq)) => {
                    self.processor.write().await.store.collections.insert(table.clone(), coll);
                    covered.insert(table.clone(), seq);
                    report.tables_from_images += 1;
                }
                None => {
                    crate::bootstrap::load_table_rows(
                        db,
                        &self.processor,
                        table,
                        table_meta,
                        self.bootstrap_page_size,
                        schema.has_versions,
                    )
                    .await?;
                    report.tables_from_db += 1;
                }
            }
        }

        // Everything noticed after an image was written, re-read by id.
        if let Some(floor) = covered.values().copied().min() {
            let mut seen = HashSet::new();
            let mut noticed: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (seq, notices) in store.journal_since(floor)? {
                for notice in notices {
                    let after_image = covered.get(&notice.table).is_some_and(|c| seq > *c);
                    if after_image && seen.insert((notice.table.clone(), notice.id.clone())) {
                        noticed.entry(notice.table).or_default().push(notice.id);
                    }
                }
            }
            report.noticed = seen.len();
            for (table, ids) in noticed {
                let omit = ssp_protocol::omit_clause(&schema.tables[&table].opaque);
                for chunk in ids.chunks(CATCH_UP_CHUNK) {
                    let mut current = crate::replay::fetch_current(db, chunk, &omit)
                        .await
                        .context("snapshot catch-up")?;
                    let mut circuit = self.processor.write().await;
                    for id in chunk {
                        match current.remove(id) {
                            Some(row) => {
                                let rv = row.get("_00_rv").and_then(Value::as_i64);
                                if rv.is_some() && circuit.contains(&table, id) && circuit.stored_rv(&table, id) >= rv {
                                    continue;
                                }
                                circuit.load([Record::new(&table, id, row)]);
                                report.caught_up += 1;
                            }
                            None if circuit.contains(&table, id) => {
                                circuit.store.ensure_collection(&table).apply(Operation::Delete, id, Value::Null.into());
                                report.deleted += 1;
                            }
                            None => {}
                        }
                    }
                }
            }
        }

        crate::bootstrap::register_views_from_db(db, &self.processor).await;
        tracing::info!(?report, "Warm start from table images");
        Ok(Some(report))
    }

    /// Bring the images in `store` up to date with the circuit: rewrite the
    /// tables whose catch-up hash moved, until `budget_bytes` were written
    /// (at least one table per step), move the coverage of unchanged ones,
    /// and trim the journal once every table has an image. The caller runs
    /// another step while `remaining` is non-zero.
    ///
    /// Only while Ready, and only when the circuit holds upstream's current
    /// schema (two small reads): an image of rows shaped by an older schema
    /// must not be stamped with the new fingerprint.
    pub async fn checkpoint_step(
        &self,
        store: &dyn SnapshotStore,
        budget_bytes: u64,
    ) -> anyhow::Result<CheckpointReport> {
        let mut report = CheckpointReport::default();
        if *self.status.read().await != SspStatus::Ready {
            report.skipped = Some("not ready");
            return Ok(report);
        }
        let images: HashMap<String, TableImage> =
            store.images()?.into_iter().map(|i| (i.table.clone(), i)).collect();

        // Nothing moved since the images (a view unregistered, a write that
        // changed nothing): advance their coverage, no database read.
        {
            let circuit = self.processor.read().await;
            let held = circuit.permissions();
            let unmoved = held.len() == images.len()
                && held.keys().all(|t| images.get(t).is_some_and(|i| i.xor == table_xor(&circuit, t)));
            if unmoved {
                let covered = store.applied_through();
                for image in images.values().filter(|i| i.covered_seq < covered) {
                    store.set_covered(&image.table, covered)?;
                }
                drop(circuit);
                if let Some(floor) = store.images()?.iter().map(|i| i.covered_seq).min() {
                    store.trim_journal(floor)?;
                    report.trimmed_through = Some(floor);
                }
                return Ok(report);
            }
        }

        let schema = crate::bootstrap::load_schema(self.platform.db.as_ref()).await?;
        let Some(probe) = schema.probe.as_ref() else {
            report.skipped = Some("no schema upstream");
            return Ok(report);
        };

        // From here to the end of the writes nothing awaits, so the circuit
        // and the journal position are read at one instant.
        let circuit = self.processor.read().await;
        let held: HashSet<&String> = circuit.permissions().keys().collect();
        let current = held.len() == schema.tables.len()
            && schema.tables.iter().all(|(t, m)| circuit.table_meta(t).as_ref() == Some(m));
        if !current {
            report.skipped = Some("circuit schema behind upstream");
            return Ok(report);
        }
        let covered = store.applied_through();
        let mut dirty = Vec::new();
        for table in schema.tables.keys() {
            let xor = table_xor(&circuit, table);
            match images.get(table) {
                Some(image) if image.xor == xor => {
                    if image.covered_seq < covered {
                        store.set_covered(table, covered)?;
                    }
                }
                _ => dirty.push((table, xor)),
            }
        }
        let mut marks = Vec::new();
        let mut iter = dirty.into_iter();
        for (table, xor) in iter.by_ref() {
            let fresh;
            let coll = match circuit.store.collections.get(table) {
                Some(coll) => coll,
                None => {
                    fresh = Collection::new(table.clone());
                    &fresh
                }
            };
            // A delta when the table's watermark is this chain's end: the
            // table was read from it or last written into it. Every write is
            // marked below, and a cleared or replaced table has no watermark.
            let tip = images.get(table).filter(|image| {
                coll.rows.persisted().is_some()
                    && image.seq < MAX_DELTAS
                    && !deltas_outweigh(image.base_bytes, image.delta_bytes)
            });
            let prev_xor = tip.and_then(|image| ssp_protocol::snapshot_hash::xor_acc_from_hex(&image.xor));
            let (image, mut write): (TableImage, Box<WriteFile<'_>>) =
                match (tip, prev_xor) {
                    (Some(tip), Some(prev_xor)) => {
                        let (base, seq) = (tip.image_id, tip.seq + 1);
                        let image = TableImage { seq, covered_seq: covered, xor, ..tip.clone() };
                        (image, Box::new(move |out| checkpoint::write_delta(coll, base, seq, prev_xor, out)))
                    }
                    _ => {
                        let image_id = checkpoint::fresh_image_id();
                        let image = TableImage { table: table.clone(), image_id, seq: 0, covered_seq: covered, xor, ..TableImage::default() };
                        (image, Box::new(move |out| checkpoint::write_collection(coll, image_id, out)))
                    }
                };
            let mut mark = None;
            let bytes = store.write_file(&image, &mut |out| {
                mark = Some(write(out)?.mark);
                Ok(())
            })?;
            if let (Some(mark), true) = (mark, circuit.store.collections.contains_key(table)) {
                marks.push((table.clone(), mark));
            }
            report.deltas += usize::from(image.seq > 0);
            report.written.push(table.clone());
            report.bytes += bytes;
            if report.bytes >= budget_bytes {
                break;
            }
        }
        report.remaining = iter.count();
        drop(circuit);
        // The files are in place: what they hold is persisted. A mark of a
        // table cleared since is refused, and that table's next file is a base.
        if !marks.is_empty() {
            let mut circuit = self.processor.write().await;
            for (table, mark) in marks {
                if let Some(coll) = circuit.store.collections.get_mut(&table) {
                    coll.rows.mark_persisted(mark);
                }
            }
        }

        for table in images.keys().filter(|t| !schema.tables.contains_key(*t)) {
            store.drop_image(table)?;
        }
        let same_schema = store
            .meta()
            .is_some_and(|m| m.fingerprint == probe.fingerprint && m.has_versions == schema.has_versions && m.node_version == NODE_VERSION);
        // A new schema is only stamped once every table's image is from it.
        if same_schema || report.remaining == 0 {
            store.set_meta(&SnapshotMeta {
                format: SNAPSHOT_FORMAT,
                node_version: NODE_VERSION.to_string(),
                fingerprint: probe.fingerprint.clone(),
                has_versions: schema.has_versions,
                saved_at_epoch_ms: crate::now_epoch_ms(),
            })?;
        }
        if report.remaining == 0 {
            let images = store.images()?;
            let all_imaged = schema.tables.keys().all(|t| images.iter().any(|i| &i.table == t));
            if let Some(floor) = images.iter().map(|i| i.covered_seq).min().filter(|_| all_imaged) {
                store.trim_journal(floor)?;
                report.trimmed_through = Some(floor);
            }
        }
        Ok(report)
    }
}

/// A table's catch-up hash as images record it; an empty table has no
/// collection yet.
fn table_xor(circuit: &ssp::circuit::Circuit, table: &str) -> String {
    match circuit.store.collections.get(table) {
        Some(coll) => xor_acc_to_hex(&coll.catchup_xor),
        None => xor_acc_to_hex(&ssp_protocol::snapshot_hash::xor_empty()),
    }
}
