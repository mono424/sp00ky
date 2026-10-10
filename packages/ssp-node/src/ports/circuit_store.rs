use super::MaybeSendSync;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum CircuitStoreError {
    /// No snapshot persisted yet — a truly cold start (rebuild from the DB).
    #[error("not found")]
    NotFound,
    #[error("transport: {0}")]
    Transport(String),
    /// Stored blob failed to parse / restore — treat as cold and rebuild.
    #[error("corrupt: {0}")]
    Corrupt(String),
}

/// Where a restored circuit snapshot sits relative to the current database, so
/// cold-start can bound catch-up and decide restore-vs-rebuild.
///
/// There is NO global mutation sequence for a standalone node (the scheduler's
/// `snapshot_seq` is cluster-only; SurrealDB's `_00_rv`/`_00_version` are
/// per-record). So the resume point is content-oriented, not a stream cursor:
/// staleness + per-table content hashes + the highest per-record version
/// folded into the snapshot. Catch-up selects rows newer than
/// `max_row_version` and a content-hash verify (falling back to a full
/// re-scan) guarantees convergence — a lossy/absent `_00_rv` degrades to
/// rebuild, never silent divergence.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResumePoint {
    /// Epoch-ms the snapshot was checkpointed. Gates staleness.
    pub saved_at_epoch_ms: u64,
    /// Per-table content hash at checkpoint (`Circuit::compute_table_hashes`).
    pub table_hashes: BTreeMap<String, String>,
    /// Highest `_00_rv` folded into the snapshot, per table. `None`/absent for
    /// a table ⇒ catch-up re-scans that table in full.
    pub max_row_version: BTreeMap<String, i64>,
}

/// Persist / restore the DBSP circuit across process (or Durable Object)
/// lifetimes. Generalizes the cold-start / eviction problem into a port: a
/// long-lived host (VM) supplies a noop (the process holds the circuit; a
/// cluster SSP's warm restart is its own mechanism, row checkpoints in
/// `apps/ssp/src/warm.rs`); an ephemeral host (edge/DO/serverless, the
/// portable host) supplies a durable blob store.
///
/// The blob is the output of `ssp::circuit::Circuit::save()` (a JSON string;
/// the operator DAG is rebuilt from plans on `restore`, not serialized).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait CircuitStore: MaybeSendSync {
    /// Persist blob + resume point atomically, overwriting the previous
    /// snapshot (write-temp-then-rename on disk; single put on a KV store).
    async fn save(&self, blob: &str, point: &ResumePoint) -> Result<(), CircuitStoreError>;

    /// Load the latest snapshot. `NotFound` = cold start (caller rebuilds).
    async fn load(&self) -> Result<(String, ResumePoint), CircuitStoreError>;

    /// Drop the snapshot, for a host's clean-restart wipe.
    async fn clear(&self) -> Result<(), CircuitStoreError>;
}

/// A `CircuitStore` that never persists — every `load` is a cold start. Used by
/// long-lived hosts (the VM) whose process already holds the circuit in memory,
/// so `bootstrap()` always takes the rebuild-from-DB branch.
pub struct NoopCircuitStore;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl CircuitStore for NoopCircuitStore {
    async fn save(&self, _blob: &str, _point: &ResumePoint) -> Result<(), CircuitStoreError> {
        Ok(())
    }
    async fn load(&self) -> Result<(String, ResumePoint), CircuitStoreError> {
        Err(CircuitStoreError::NotFound)
    }
    async fn clear(&self) -> Result<(), CircuitStoreError> {
        Ok(())
    }
}

/// `CircuitStore` backed by a single JSON file, written atomically
/// (temp file + rename) so a crash mid-write leaves the previous snapshot
/// intact rather than a truncated one.
///
/// Lives here rather than in a shell so the server and the portable host share
/// one implementation; the Durable Object shell fills the same seam with its
/// own storage API.
///
/// The snapshot is a cache, not a source of truth: SurrealDB is. A snapshot
/// that is missing, stale or unreadable simply costs a full rebuild, which is
/// what every caller already handles.
#[cfg(not(target_arch = "wasm32"))]
pub struct DiskCircuitStore {
    path: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(serde::Serialize, serde::Deserialize)]
struct DiskSnapshot {
    blob: String,
    point: ResumePoint,
}

#[cfg(not(target_arch = "wasm32"))]
impl DiskCircuitStore {
    pub fn new(dir: impl AsRef<std::path::Path>) -> Self {
        Self {
            path: dir.as_ref().join("snapshot.json"),
        }
    }

    /// Whether `dir` is usable for snapshots.
    ///
    /// Checked rather than assumed because a configured-but-unwritable
    /// directory is a real deployment state (a read-only root, a missing
    /// volume), and the right response is to run without snapshots rather
    /// than to fail startup.
    pub fn probe_writable(dir: &std::path::Path) -> bool {
        if std::fs::create_dir_all(dir).is_err() {
            return false;
        }
        let probe = dir.join(".snapshot-probe");
        match std::fs::write(&probe, b"1") {
            Ok(()) => {
                let _ = std::fs::remove_file(&probe);
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl CircuitStore for DiskCircuitStore {
    async fn save(&self, blob: &str, point: &ResumePoint) -> Result<(), CircuitStoreError> {
        let snap = DiskSnapshot {
            blob: blob.to_string(),
            point: point.clone(),
        };
        let bytes =
            serde_json::to_vec(&snap).map_err(|e| CircuitStoreError::Transport(e.to_string()))?;
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, &bytes).map_err(|e| CircuitStoreError::Transport(e.to_string()))?;
        std::fs::rename(&tmp, &self.path)
            .map_err(|e| CircuitStoreError::Transport(e.to_string()))?;
        Ok(())
    }

    async fn load(&self) -> Result<(String, ResumePoint), CircuitStoreError> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(CircuitStoreError::NotFound)
            }
            Err(e) => return Err(CircuitStoreError::Transport(e.to_string())),
        };
        let snap: DiskSnapshot = serde_json::from_slice(&bytes)
            .map_err(|e| CircuitStoreError::Corrupt(e.to_string()))?;
        Ok((snap.blob, snap.point))
    }

    async fn clear(&self) -> Result<(), CircuitStoreError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CircuitStoreError::Transport(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Minimal in-memory store for exercising the round-trip contract.
    #[derive(Default)]
    struct MemStore(Mutex<Option<(String, ResumePoint)>>);

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl CircuitStore for MemStore {
        async fn save(&self, blob: &str, point: &ResumePoint) -> Result<(), CircuitStoreError> {
            *self.0.lock().unwrap() = Some((blob.to_string(), point.clone()));
            Ok(())
        }
        async fn load(&self) -> Result<(String, ResumePoint), CircuitStoreError> {
            self.0.lock().unwrap().clone().ok_or(CircuitStoreError::NotFound)
        }
        async fn clear(&self) -> Result<(), CircuitStoreError> {
            *self.0.lock().unwrap() = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn noop_store_is_always_cold() {
        let s = NoopCircuitStore;
        s.save("blob", &ResumePoint::default()).await.unwrap();
        assert!(matches!(s.load().await, Err(CircuitStoreError::NotFound)));
    }

    #[tokio::test]
    async fn mem_store_round_trips_blob_and_point() {
        let s = MemStore::default();
        assert!(matches!(s.load().await, Err(CircuitStoreError::NotFound)));

        let mut point = ResumePoint { saved_at_epoch_ms: 123, ..Default::default() };
        point.table_hashes.insert("thread".into(), "b3:abc".into());
        point.max_row_version.insert("thread".into(), 7);
        s.save("SNAPSHOT", &point).await.unwrap();

        let (blob, got) = s.load().await.unwrap();
        assert_eq!(blob, "SNAPSHOT");
        assert_eq!(got.saved_at_epoch_ms, 123);
        assert_eq!(got.table_hashes.get("thread").map(String::as_str), Some("b3:abc"));
        assert_eq!(got.max_row_version.get("thread"), Some(&7));

        s.clear().await.unwrap();
        assert!(matches!(s.load().await, Err(CircuitStoreError::NotFound)));
    }
}
