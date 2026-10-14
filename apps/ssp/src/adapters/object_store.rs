use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use axum::body::Bytes;
use ssp_node::ports::{ObjectMeta, ObjectStore, ObjectStoreError, StoredObject};

/// `r2` bucket files in a directory (`SPKY_BUCKET_DIR`): what `spky dev` and a
/// VM deployment use where Cloudflare uses R2. Keys arrive validated by
/// `ssp_node::bucket` (no empty, `.` or `..` segments), so joining them under
/// the root cannot escape it.
pub struct DirObjectStore {
    root: PathBuf,
}

impl DirObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

fn transport(e: std::io::Error) -> ObjectStoreError {
    ObjectStoreError::Transport(e.to_string())
}

async fn meta_of(path: &Path) -> Result<Option<ObjectMeta>, ObjectStoreError> {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.is_file() => Ok(Some(ObjectMeta {
            size: meta.len(),
            content_type: None,
            updated_epoch_ms: meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64),
        })),
        Ok(_) => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(transport(e)),
    }
}

#[async_trait::async_trait]
impl ObjectStore for DirObjectStore {
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, ObjectStoreError> {
        let path = self.path(key);
        let Some(meta) = meta_of(&path).await? else { return Ok(None) };
        match tokio::fs::read(&path).await {
            Ok(body) => Ok(Some(StoredObject { meta, body: Bytes::from(body) })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(transport(e)),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        meta_of(&self.path(key)).await
    }

    async fn put(&self, key: &str, body: Bytes, _content_type: &str) -> Result<(), ObjectStoreError> {
        let path = self.path(key);
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await.map_err(transport)?;
        }
        // Write then rename, so a reader never sees half a file.
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
        tokio::fs::write(&tmp, &body).await.map_err(transport)?;
        tokio::fs::rename(&tmp, &path).await.map_err(transport)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match tokio::fs::remove_file(self.path(key)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(transport(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_and_missing_keys() {
        let dir = std::env::temp_dir().join(format!("spky-dir-store-{}", uuid::Uuid::new_v4().simple()));
        let store = DirObjectStore::new(&dir);
        assert!(store.get("pics/a/b.png").await.unwrap().is_none());
        store.put("pics/a/b.png", Bytes::from_static(b"png"), "image/png").await.unwrap();
        let object = store.get("pics/a/b.png").await.unwrap().unwrap();
        assert_eq!(&object.body[..], b"png");
        assert_eq!(store.head("pics/a/b.png").await.unwrap().unwrap().size, 3);
        store.delete("pics/a/b.png").await.unwrap();
        store.delete("pics/a/b.png").await.unwrap();
        assert!(store.head("pics/a/b.png").await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }
}
