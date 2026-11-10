//! A handle to a SurrealDB storage bucket (TS `BucketHandle`).
//!
//! PORT: `packages/spooky_core/lib/src/modules/bucket.dart`. Operations are
//! issued as `f"<bucket>:/<path>"....` file expressions through an injected
//! remote query. [`BucketHandle::read`] goes through the local
//! [`BlobCache`] when the client has one, so a warm file never touches the
//! serialized remote queue; [`BucketHandle::get`] always goes remote.
//!
//! Return-value parsing is tolerant, so a backend that returns no body for an
//! op does not fail.

use std::rc::Rc;
use std::sync::LazyLock;

use futures::future::{FutureExt, LocalBoxFuture};
use regex::Regex;

use crate::error::{Error, Result};
use crate::modules::blobs::{Blob, BlobCache, BlobKey, FetchRemote};
use crate::value::{Object, Value};

/// Runs SurrealQL on the remote and answers one value per statement (failing
/// when any statement failed).
pub type RemoteQuery =
    Rc<dyn Fn(String, Option<Object>) -> LocalBoxFuture<'static, Result<Vec<Value>>>>;

/// The cache namespace for the signed-in user, resolved per call so the handle
/// follows an auth flip without subscribing to it. May wait: a read issued
/// before the client has restored its session must not land in the anonymous
/// namespace.
pub type NamespaceResolver = Rc<dyn Fn() -> LocalBoxFuture<'static, String>>;

/// Bucket keys are restricted to this charset BY THE QUERY LEXER: the path is
/// interpolated into `f"<bucket>:/<path>"`, which is syntax and cannot take a
/// bound parameter. A space is a parse error and a `:` silently TRUNCATES the
/// key, so an unvalidated path is a broken query at best and the wrong object
/// at worst.
static SAFE_PATH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_\-./]+$").unwrap());

/// Blurhash sidecar written by the TS client's `bucket.put()` next to an image
/// (TS `blurhashSidecarPath`).
pub fn blurhash_sidecar_path(path: &str) -> String {
    format!("{path}.bh")
}

/// Coerce whatever the `.get()` RPC hands back into bytes (TS
/// `bucketContentToBlob`). Text files arrive as a string; binary as a CBOR
/// byte string.
pub fn bucket_content_to_bytes(content: &Value) -> Option<Vec<u8>> {
    match content {
        Value::Bytes(b) => Some(b.clone()),
        // Truncated to a byte, as Dart's `Uint8List.fromList`.
        Value::Array(items) => items.iter().map(|v| v.as_i64().map(|i| i as u8)).collect(),
        Value::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

/// A handle to one bucket. Cheap to clone.
#[derive(Clone)]
pub struct BucketHandle {
    name: String,
    query: RemoteQuery,
    blobs: Option<BlobCache>,
    namespace: Option<NamespaceResolver>,
}

impl BucketHandle {
    /// An uncached handle issuing its file expressions through `query`.
    pub fn new(name: impl Into<String>, query: RemoteQuery) -> Self {
        BucketHandle {
            name: name.into(),
            query,
            blobs: None,
            namespace: None,
        }
    }

    /// Read through `blobs`.
    pub fn with_blobs(mut self, blobs: BlobCache) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Point the cache at the namespace `resolve` names before every use.
    pub fn with_namespace(mut self, resolve: NamespaceResolver) -> Self {
        self.namespace = Some(resolve);
        self
    }

    /// The [`FetchRemote`] a [`BlobCache`] fills its misses with: a raw
    /// `.get()` of the key's bucket and path through `query`.
    pub fn fetcher(query: RemoteQuery) -> FetchRemote {
        Rc::new(move |key: BlobKey| {
            let handle = BucketHandle::new(key.bucket, query.clone());
            async move { Ok(bucket_content_to_bytes(&handle.get(&key.path).await?)) }.boxed_local()
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn file_ref(&self, path: &str) -> Result<String> {
        if !SAFE_PATH.is_match(path) {
            return Err(Error::InvalidArgument(format!(
                "bucket paths may only contain [A-Za-z0-9_-./]: {path:?}"
            )));
        }
        Ok(format!("f\"{}:/{path}\"", self.name))
    }

    fn key(&self, path: &str) -> BlobKey {
        BlobKey::new(self.name.clone(), path)
    }

    /// The cache, pointed at the current user's namespace.
    async fn cache(&self) -> Option<BlobCache> {
        let blobs = self.blobs.clone()?;
        if let Some(resolve) = &self.namespace {
            let ns = resolve().await;
            blobs.set_namespace(&ns).await;
        }
        Some(blobs)
    }

    async fn run(&self, sql: String, vars: Option<Object>) -> Result<Vec<Value>> {
        (self.query)(sql, vars).await
    }

    async fn first(&self, sql: String, vars: Option<Object>) -> Result<Value> {
        Ok(self
            .run(sql, vars)
            .await?
            .into_iter()
            .next()
            .unwrap_or_default())
    }

    /// Upload `content` (a string or bytes) to `path`.
    pub async fn put(&self, path: &str, content: impl Into<Value>) -> Result<()> {
        let file = self.file_ref(path)?;
        let mut vars = Object::new();
        vars.insert("content".into(), content.into());
        self.run(format!("RETURN {file}.put($content);"), Some(vars))
            .await?;
        // A path can be overwritten, so anything cached under it is now wrong.
        if let Some(cache) = self.cache().await {
            cache.invalidate(&self.key(path)).await;
        }
        Ok(())
    }

    /// Raw remote read, uncached. Prefer [`BucketHandle::read`].
    pub async fn get(&self, path: &str) -> Result<Value> {
        let file = self.file_ref(path)?;
        self.first(format!("RETURN {file}.get();"), None).await
    }

    /// Read through the local blob cache: disk first, the bucket second.
    /// `None` when the file exists in neither place.
    pub async fn read(&self, path: &str) -> Result<Option<Blob>> {
        self.read_with(path, false).await
    }

    /// [`BucketHandle::read`]; `reload` skips the cache and refills it.
    pub async fn read_with(&self, path: &str, reload: bool) -> Result<Option<Blob>> {
        self.file_ref(path)?;
        match self.cache().await {
            None => Ok(bucket_content_to_bytes(&self.get(path).await?).map(Blob::from)),
            Some(cache) => cache.read_with(&self.key(path), reload, true).await,
        }
    }

    /// The blurhash stored alongside an uploaded image, `None` when there is
    /// none. Reads through the cache, so a missing sidecar costs one remote
    /// read per process, not one per mount.
    pub async fn blurhash(&self, path: &str) -> Result<Option<String>> {
        let Some(bytes) = self.read(&blurhash_sidecar_path(path)).await? else {
            return Ok(None);
        };
        let hash = String::from_utf8_lossy(&bytes).trim().to_string();
        Ok((hash.len() >= 6).then_some(hash))
    }

    /// Drop `path` from the local cache without touching the remote file.
    pub async fn evict(&self, path: &str) -> Result<()> {
        if let Some(cache) = self.cache().await {
            cache.invalidate(&self.key(path)).await;
        }
        Ok(())
    }

    /// Warm the cache. Already-cached paths are skipped.
    pub async fn prefetch(&self, paths: &[&str]) -> Result<()> {
        for p in paths {
            self.file_ref(p)?;
        }
        if let Some(cache) = self.cache().await {
            let keys: Vec<BlobKey> = paths.iter().map(|p| self.key(p)).collect();
            cache.prefetch(&keys).await;
        }
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let file = self.file_ref(path)?;
        self.run(format!("RETURN {file}.delete();"), None).await?;
        if let Some(cache) = self.cache().await {
            cache.invalidate(&self.key(path)).await;
            cache
                .invalidate(&self.key(&blurhash_sidecar_path(path)))
                .await;
        }
        Ok(())
    }

    pub async fn exists(&self, path: &str) -> Result<bool> {
        let file = self.file_ref(path)?;
        Ok(self.first(format!("RETURN {file}.exists();"), None).await? == Value::Bool(true))
    }

    /// The file's metadata (`size`, `updated`, ...), empty when unknown.
    pub async fn head(&self, path: &str) -> Result<Object> {
        let file = self.file_ref(path)?;
        Ok(self
            .first(format!("RETURN {file}.head();"), None)
            .await?
            .into_object()
            .unwrap_or_default())
    }

    pub async fn copy(&self, source: &str, target: &str) -> Result<()> {
        self.file_ref(target)?;
        let file = self.file_ref(source)?;
        let mut vars = Object::new();
        vars.insert("target".into(), target.into());
        self.run(format!("RETURN {file}.copy($target);"), Some(vars))
            .await?;
        Ok(())
    }

    pub async fn rename(&self, source: &str, target: &str) -> Result<()> {
        self.file_ref(target)?;
        let file = self.file_ref(source)?;
        let mut vars = Object::new();
        vars.insert("target".into(), target.into());
        self.run(format!("RETURN {file}.rename($target);"), Some(vars))
            .await?;
        if let Some(cache) = self.cache().await {
            cache.invalidate(&self.key(source)).await;
        }
        Ok(())
    }

    /// The paths under `prefix` (the whole bucket for `None` or `""`).
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let file = match prefix.unwrap_or("") {
            "" => format!("f\"{}:/\"", self.name),
            p => self.file_ref(p)?,
        };
        Ok(
            match self.first(format!("RETURN {file}.list();"), None).await? {
                Value::Array(items) => items
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
                _ => Vec::new(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::modules::blobs::{FileBlobStore, MemoryBlobStore};

    type Calls = Rc<RefCell<Vec<String>>>;

    /// A remote that records every statement and answers `answer`.
    fn recording(calls: &Calls, answer: Vec<Value>) -> RemoteQuery {
        let calls = calls.clone();
        Rc::new(move |sql, _vars| {
            calls.borrow_mut().push(sql);
            let answer = answer.clone();
            async move { Ok(answer) }.boxed_local()
        })
    }

    #[tokio::test]
    async fn read_goes_through_the_cache_and_get_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let calls: Calls = Rc::default();
        let c2 = calls.clone();
        let cache = BlobCache::new(
            Rc::new(FileBlobStore::new(dir.path())),
            Rc::new(move |key: BlobKey| {
                c2.borrow_mut().push(format!("remote:{}", key.id()));
                async { Ok(Some(vec![1, 2, 3])) }.boxed_local()
            }),
            1 << 20,
        );
        let handle = BucketHandle::new(
            "covers",
            recording(&calls, vec![Value::Bytes(vec![1, 2, 3])]),
        )
        .with_blobs(cache.clone())
        .with_namespace(Rc::new(|| async { "u1".to_string() }.boxed_local()));
        assert_eq!(&*handle.read("a.webp").await.unwrap().unwrap(), &[1, 2, 3]);
        assert_eq!(&*handle.read("a.webp").await.unwrap().unwrap(), &[1, 2, 3]);
        assert_eq!(*calls.borrow(), vec!["remote:covers/a.webp"]);
        assert_eq!(cache.namespace(), "u1");
        handle.get("a.webp").await.unwrap();
        assert_eq!(
            calls.borrow().last().unwrap(),
            "RETURN f\"covers:/a.webp\".get();"
        );
    }

    #[tokio::test]
    async fn put_and_delete_invalidate_the_cached_copy() {
        let calls: Calls = Rc::default();
        let cache = BlobCache::new(
            Rc::new(MemoryBlobStore::default()),
            Rc::new(|_| async { Ok(Some(vec![1])) }.boxed_local()),
            1 << 20,
        );
        let handle =
            BucketHandle::new("covers", recording(&calls, vec![])).with_blobs(cache.clone());
        handle.read("a").await.unwrap();
        assert_eq!(cache.stats().entries, 1);
        handle.put("a", "x").await.unwrap();
        assert_eq!(cache.stats().entries, 0);
        handle.read("a").await.unwrap();
        handle.delete("a").await.unwrap();
        assert_eq!(cache.stats().entries, 0);
    }

    #[tokio::test]
    async fn rejects_paths_the_query_lexer_would_mangle() {
        let calls: Calls = Rc::default();
        let handle = BucketHandle::new("covers", recording(&calls, vec![]));
        assert!(matches!(
            handle.read("a:b").await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            handle.get("has space").await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            handle.put("x\"y", "z").await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(handle.copy("a", "b c").await.is_err());
        assert!(calls.borrow().is_empty());
    }

    #[test]
    fn bucket_content_to_bytes_coerces_every_wire_shape() {
        assert_eq!(bucket_content_to_bytes(&Value::Null), None);
        assert_eq!(
            bucket_content_to_bytes(&Value::Bytes(vec![1])),
            Some(vec![1])
        );
        assert_eq!(
            bucket_content_to_bytes(&Value::Array(vec![1.into(), 2.into()])),
            Some(vec![1, 2])
        );
        assert_eq!(bucket_content_to_bytes(&"hi".into()), Some(vec![104, 105]));
        assert_eq!(bucket_content_to_bytes(&42.into()), None);
    }

    /// Dart `run_bucket_test.dart`: the file SURQL every operation issues.
    #[tokio::test]
    async fn every_operation_issues_its_file_surql() {
        let calls: Calls = Rc::default();
        let bucket = BucketHandle::new("avatars", recording(&calls, vec![Value::Bool(true)]));
        bucket.put("a.png", "data").await.unwrap();
        bucket.get("a.png").await.unwrap();
        assert!(bucket.exists("a.png").await.unwrap());
        bucket.delete("a.png").await.unwrap();
        bucket.copy("a.png", "b.png").await.unwrap();
        bucket.rename("a.png", "c.png").await.unwrap();
        bucket.list(Some("sub/")).await.unwrap();
        bucket.list(None).await.unwrap();
        bucket.head("a.png").await.unwrap();
        let q = calls.borrow().join("\n");
        for expected in [
            "f\"avatars:/a.png\".put($content)",
            "f\"avatars:/a.png\".get()",
            "f\"avatars:/a.png\".exists()",
            "f\"avatars:/a.png\".delete()",
            "f\"avatars:/a.png\".copy($target)",
            "f\"avatars:/a.png\".rename($target)",
            "f\"avatars:/sub/\".list()",
            "RETURN f\"avatars:/\".list();",
            "f\"avatars:/a.png\".head()",
        ] {
            assert!(q.contains(expected), "missing {expected} in\n{q}");
        }
    }

    #[tokio::test]
    async fn tolerant_return_parsing() {
        let calls: Calls = Rc::default();
        let bucket = BucketHandle::new("b", recording(&calls, vec![]));
        assert!(!bucket.exists("a").await.unwrap());
        assert!(bucket.head("a").await.unwrap().is_empty());
        assert!(bucket.list(None).await.unwrap().is_empty());
        assert!(bucket.read("a").await.unwrap().is_none());

        let listed = BucketHandle::new(
            "b",
            recording(
                &calls,
                vec![Value::Array(vec!["x".into(), 1.into(), "y".into()])],
            ),
        );
        assert_eq!(listed.list(None).await.unwrap(), ["x", "y"]);
    }

    #[tokio::test]
    async fn blurhash_reads_the_sidecar_and_fetcher_reads_through_get() {
        let calls: Calls = Rc::default();
        let query = recording(&calls, vec![Value::String("  LEHV6nWB2yk8  ".into())]);
        let cache = BlobCache::new(
            Rc::new(MemoryBlobStore::default()),
            BucketHandle::fetcher(query.clone()),
            1 << 20,
        );
        let bucket = BucketHandle::new("covers", query).with_blobs(cache);
        assert_eq!(
            bucket.blurhash("a.webp").await.unwrap().as_deref(),
            Some("LEHV6nWB2yk8")
        );
        assert_eq!(
            *calls.borrow(),
            vec!["RETURN f\"covers:/a.webp.bh\".get();"]
        );

        let short = BucketHandle::new(
            "covers",
            recording(&calls, vec![Value::String("abc".into())]),
        );
        assert_eq!(short.blurhash("a").await.unwrap(), None);
    }
}
