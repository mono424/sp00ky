use super::MaybeSendSync;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("object store: {0}")]
    Transport(String),
}

/// What a store knows about an object without reading its bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    pub size: u64,
    /// As stored with the object; `None` when the store keeps none (a plain
    /// directory), and the caller falls back to the key's extension.
    pub content_type: Option<String>,
    pub updated_epoch_ms: Option<u64>,
}

pub struct StoredObject {
    pub meta: ObjectMeta,
    pub body: Bytes,
}

/// Bytes of the buckets the SSP serves itself (`BACKEND "r2"`, see
/// [`crate::bucket`]), for databases that cannot keep files: Surreal Cloud
/// allows only memory buckets, which lose every file on a restart. Keys are
/// `<bucket>/<key without its leading slash>`, already validated by the
/// caller. Cloudflare: an R2 binding (`apps/ssp-cf`); VM and `spky dev`: a
/// directory (`apps/ssp`).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait ObjectStore: MaybeSendSync {
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, ObjectStoreError>;
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectStoreError>;
    async fn put(&self, key: &str, body: Bytes, content_type: &str) -> Result<(), ObjectStoreError>;
    /// Deleting a missing key is not an error.
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
}

/// In process memory, for tests.
#[derive(Default)]
pub struct MemoryObjectStore {
    objects: Mutex<BTreeMap<String, (Bytes, String)>>,
}

impl MemoryObjectStore {
    pub fn keys(&self) -> Vec<String> {
        self.objects.lock().expect("store lock").keys().cloned().collect()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl ObjectStore for MemoryObjectStore {
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, ObjectStoreError> {
        Ok(self.objects.lock().expect("store lock").get(key).map(|(body, content_type)| StoredObject {
            meta: ObjectMeta {
                size: body.len() as u64,
                content_type: Some(content_type.clone()),
                updated_epoch_ms: None,
            },
            body: body.clone(),
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        Ok(self.get(key).await?.map(|object| object.meta))
    }

    async fn put(&self, key: &str, body: Bytes, content_type: &str) -> Result<(), ObjectStoreError> {
        self.objects
            .lock()
            .expect("store lock")
            .insert(key.to_string(), (body, content_type.to_string()));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.objects.lock().expect("store lock").remove(key);
        Ok(())
    }
}
