//! Cache for bucket files (the Rust twin of `packages/core/src/services/blobs`).
//!
//! PORT: `packages/spooky_core/lib/src/services/blobs/blob_cache.dart`.
//!
//! ```text
//!   L1  bytes on disk, durable                  (BlobStore)
//!   L2  the remote bucket over the sync socket  (BlobCache fetch_remote)
//! ```
//!
//! There is no in-memory byte layer: the render side keeps decoded bitmaps
//! hot, and a disk read is cheap next to the serialized remote queue a miss
//! would join. What the cache does hold in memory is the manifest (sizes and
//! access times for eviction), the in-flight map (two views mounting the same
//! cover issue one query), and the negative set (a missing cover is normal and
//! must not be re-asked on every rebuild).
//!
//! Nothing is dropped because it got old: the row referencing an image is in
//! the local store indefinitely, so the image has to be too. Bytes only go away
//! when the app invalidated them (`put`/`delete` on that path), when a read
//! found a torn file, or when the cache is over budget and this is the least
//! recently used entry.
//!
//! File I/O runs on tokio's blocking pool, so the engine thread never waits on
//! the disk.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::{join_all, FutureExt, LocalBoxFuture, Shared};

use crate::error::{Error, Result};

/// The bytes of one cached file, shared between everyone who asked for it.
pub type Blob = Rc<[u8]>;

/// Reads one file from the remote bucket; `None` when it does not exist.
pub type FetchRemote = Rc<dyn Fn(BlobKey) -> LocalBoxFuture<'static, Result<Option<Vec<u8>>>>>;

/// The cache's clock.
pub type Clock = Rc<dyn Fn() -> DateTime<Utc>>;

/// Identifies one cached file: the bucket it lives in and its path within.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlobKey {
    pub bucket: String,
    pub path: String,
}

impl BlobKey {
    pub fn new(bucket: impl Into<String>, path: impl Into<String>) -> Self {
        BlobKey {
            bucket: bucket.into(),
            path: path.into(),
        }
    }

    /// Path segments, with `..`, `.` and empty ones stripped so a crafted path
    /// cannot escape the cache root.
    pub fn segments(&self) -> Vec<&str> {
        self.path
            .split('/')
            .filter(|s| !s.is_empty() && *s != "." && *s != "..")
            .collect()
    }

    /// Stable manifest id. Mirrors the on-disk layout, decoded.
    pub fn id(&self) -> String {
        format!("{}/{}", self.bucket, self.segments().join("/"))
    }
}

impl std::fmt::Display for BlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.id())
    }
}

/// What a directory walk can tell about a stored file, with no manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStat {
    pub key: BlobKey,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

/// Marks a half-written file. Committed names can never contain a literal `.`
/// (see [`encode_segment`]), so this suffix is unambiguous: anything wearing it
/// at walk time is a write that died before commit, and is swept.
const PART_MARKER: &str = ".part-";

/// Filesystem names are capped around 255 bytes; stay well clear.
const MAX_SEGMENT_LENGTH: usize = 200;

/// Evict down to this fraction of the budget, so eviction is not per write.
const LOW_WATER: f64 = 0.8;

/// Parallel remote reads during [`BlobCache::prefetch`]. Shares the serialized
/// remote queue, so more would only queue.
const PREFETCH_CONCURRENCY: usize = 3;

/// A hit re-stamps the file mtime at most this often. Every hit moving the
/// clock would turn scrolling a shelf of covers into a write storm.
const TOUCH_INTERVAL: Duration = Duration::from_secs(60);

/// A path the cache refuses to store: the caller degrades to no persistence.
fn key_error(message: String) -> Error {
    Error::InvalidArgument(message)
}

fn is_key_error(e: &Error) -> bool {
    matches!(e, Error::InvalidArgument(_))
}

/// Percent-encode a path segment, and escape `.` on top of that. Escaping the
/// dot is what makes the `.part-` marker safe. Same encoding as the TS OPFS
/// store (`encodeURIComponent`), so the on-disk layout is one layout across
/// clients.
pub fn encode_segment(segment: &str) -> Result<String> {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || b"-_!~*'()".contains(&b) {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    if out.len() > MAX_SEGMENT_LENGTH {
        return Err(key_error(format!(
            "path segment too long to store ({} > {MAX_SEGMENT_LENGTH})",
            out.len()
        )));
    }
    Ok(out)
}

/// Undo [`encode_segment`]. A malformed escape is kept literally.
pub fn decode_segment(segment: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        (b as char).to_digit(16).map(|d| d as u8)
    }
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `<bucket>/<...path>`, encoded: the directory chain plus filename for a key.
pub fn key_segments(key: &BlobKey) -> Result<Vec<String>> {
    let parts = key.segments();
    if parts.is_empty() {
        return Err(key_error(format!(
            "empty file path for bucket \"{}\"",
            key.bucket
        )));
    }
    std::iter::once(key.bucket.as_str())
        .chain(parts)
        .map(encode_segment)
        .collect()
}

/// Byte storage for cached bucket files, keyed under a namespace (the local
/// bucket id of the signed-in user, so two accounts on one device never see
/// each other's files). A key that cannot be stored fails with
/// [`Error::InvalidArgument`].
#[async_trait(?Send)]
pub trait BlobStore {
    /// False for [`MemoryBlobStore`]: the cache still dedupes within the
    /// process, but nothing survives a restart.
    fn persistent(&self) -> bool;
    async fn read(&self, namespace: &str, key: &BlobKey) -> Result<Option<Vec<u8>>>;
    /// Returns the number of bytes written.
    async fn write(&self, namespace: &str, key: &BlobKey, bytes: &[u8]) -> Result<u64>;
    async fn remove(&self, namespace: &str, key: &BlobKey) -> Result<()>;
    /// Record an access so a rebuilt manifest orders eviction correctly.
    async fn touch(&self, namespace: &str, key: &BlobKey, at: DateTime<Utc>) -> Result<()>;
    /// Every committed file under the namespace. Sweeps torn writes.
    async fn list(&self, namespace: &str) -> Result<Vec<BlobStat>>;
    /// Drop the whole namespace.
    async fn clear(&self, namespace: &str) -> Result<()>;
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(Error::other)?
        .map_err(Error::other)
}

/// Files on disk under `<root>/<namespace>/<bucket>/<...segments>`. Writes go
/// to a `.part-` temp name and commit by rename, so a crash mid-write leaves a
/// sweepable file rather than a truncated real one.
pub struct FileBlobStore {
    root: PathBuf,
    temp_counter: Cell<u64>,
    temp_token: String,
}

fn base36(mut n: u128) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".into();
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

impl FileBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or_default();
        FileBlobStore {
            root: root.into(),
            temp_counter: Cell::new(0),
            temp_token: base36(micros),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn ns_dir(&self, namespace: &str) -> Result<PathBuf> {
        Ok(self.root.join(encode_segment(namespace)?))
    }

    fn file(&self, namespace: &str, key: &BlobKey) -> Result<PathBuf> {
        let mut path = self.ns_dir(namespace)?;
        for s in key_segments(key)? {
            path.push(s);
        }
        Ok(path)
    }
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // `file_type` does not follow links.
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(&entry.path(), out)?;
        } else if kind.is_file() {
            out.push(entry.path());
        }
    }
    Ok(())
}

#[async_trait(?Send)]
impl BlobStore for FileBlobStore {
    fn persistent(&self) -> bool {
        true
    }

    async fn read(&self, namespace: &str, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        let path = self.file(namespace, key)?;
        Ok(blocking(move || Ok(std::fs::read(path).ok())).await?)
    }

    async fn write(&self, namespace: &str, key: &BlobKey, bytes: &[u8]) -> Result<u64> {
        let target = self.file(namespace, key)?;
        let n = self.temp_counter.get();
        self.temp_counter.set(n + 1);
        let mut temp = target.clone().into_os_string();
        temp.push(format!("{PART_MARKER}{}-{n}", self.temp_token));
        let temp = PathBuf::from(temp);
        let bytes = bytes.to_vec();
        blocking(move || {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let written = (|| {
                use std::io::Write;
                let mut f = std::fs::File::create(&temp)?;
                f.write_all(&bytes)?;
                f.sync_all()?;
                std::fs::rename(&temp, &target)
            })();
            if let Err(e) = written {
                let _ = std::fs::remove_file(&temp);
                return Err(e);
            }
            Ok(bytes.len() as u64)
        })
        .await
    }

    async fn remove(&self, namespace: &str, key: &BlobKey) -> Result<()> {
        let path = self.file(namespace, key)?;
        // Already gone is fine. Directories are left behind deliberately:
        // pruning them would race a concurrent write into the same folder for
        // no measurable space saving.
        blocking(move || Ok(std::fs::remove_file(path).ok())).await?;
        Ok(())
    }

    async fn touch(&self, namespace: &str, key: &BlobKey, at: DateTime<Utc>) -> Result<()> {
        let path = self.file(namespace, key)?;
        let at: SystemTime = at.into();
        // Vanished under us: the next read refetches.
        blocking(move || {
            Ok(std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .and_then(|f| f.set_modified(at))
                .ok())
        })
        .await?;
        Ok(())
    }

    async fn list(&self, namespace: &str) -> Result<Vec<BlobStat>> {
        let ns = self.ns_dir(namespace)?;
        blocking(move || {
            if !ns.is_dir() {
                return Ok(Vec::new());
            }
            let mut files = Vec::new();
            walk(&ns, &mut files)?;
            let mut out = Vec::new();
            for file in files {
                let Ok(rel) = file.strip_prefix(&ns) else {
                    continue;
                };
                let rel: Vec<String> = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect();
                if rel.last().is_some_and(|name| name.contains(PART_MARKER)) {
                    let _ = std::fs::remove_file(&file);
                    continue;
                }
                // A file directly under the namespace root has no bucket.
                if rel.len() < 2 {
                    continue;
                }
                // Vanished mid-walk: skip. Reconcile is best-effort.
                let Ok(meta) = std::fs::metadata(&file) else {
                    continue;
                };
                let mtime = meta
                    .modified()
                    .map(DateTime::<Utc>::from)
                    .unwrap_or_else(|_| Utc::now());
                let path: Vec<String> = rel[1..].iter().map(|s| decode_segment(s)).collect();
                out.push(BlobStat {
                    key: BlobKey::new(decode_segment(&rel[0]), path.join("/")),
                    size: meta.len(),
                    mtime,
                });
            }
            Ok(out)
        })
        .await
    }

    async fn clear(&self, namespace: &str) -> Result<()> {
        let ns = self.ns_dir(namespace)?;
        blocking(move || {
            if ns.exists() {
                std::fs::remove_dir_all(ns)?;
            }
            Ok(())
        })
        .await
    }
}

/// In-memory byte store. Used when no cache directory is configured (the
/// cache still dedupes within the process) and as the test double.
#[derive(Default)]
pub struct MemoryBlobStore {
    files: RefCell<HashMap<String, MemoryNamespace>>,
}

/// One namespace's files by key id: bytes and last access.
type MemoryNamespace = HashMap<String, (Vec<u8>, DateTime<Utc>)>;

#[async_trait(?Send)]
impl BlobStore for MemoryBlobStore {
    fn persistent(&self) -> bool {
        false
    }

    async fn read(&self, namespace: &str, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        key_segments(key)?;
        Ok(self
            .files
            .borrow()
            .get(namespace)
            .and_then(|ns| ns.get(&key.id()))
            .map(|(b, _)| b.clone()))
    }

    async fn write(&self, namespace: &str, key: &BlobKey, bytes: &[u8]) -> Result<u64> {
        key_segments(key)?;
        self.files
            .borrow_mut()
            .entry(namespace.to_string())
            .or_default()
            .insert(key.id(), (bytes.to_vec(), Utc::now()));
        Ok(bytes.len() as u64)
    }

    async fn remove(&self, namespace: &str, key: &BlobKey) -> Result<()> {
        if let Some(ns) = self.files.borrow_mut().get_mut(namespace) {
            ns.remove(&key.id());
        }
        Ok(())
    }

    async fn touch(&self, namespace: &str, key: &BlobKey, at: DateTime<Utc>) -> Result<()> {
        if let Some(entry) = self
            .files
            .borrow_mut()
            .get_mut(namespace)
            .and_then(|ns| ns.get_mut(&key.id()))
        {
            entry.1 = at;
        }
        Ok(())
    }

    async fn list(&self, namespace: &str) -> Result<Vec<BlobStat>> {
        Ok(self
            .files
            .borrow()
            .get(namespace)
            .map(|ns| {
                ns.iter()
                    .map(|(id, (bytes, at))| {
                        let (bucket, path) = id.split_once('/').unwrap_or((id.as_str(), ""));
                        BlobStat {
                            key: BlobKey::new(bucket, path),
                            size: bytes.len() as u64,
                            mtime: *at,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn clear(&self, namespace: &str) -> Result<()> {
        self.files.borrow_mut().remove(namespace);
        Ok(())
    }
}

/// Counters for diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobCacheStats {
    pub namespace: String,
    pub entries: usize,
    pub total_bytes: u64,
    pub budget_bytes: u64,
    pub evicted_entries: u64,
    pub evicted_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub persistent: bool,
}

#[derive(Clone)]
struct Entry {
    key: BlobKey,
    size: u64,
    last_access: DateTime<Utc>,
}

type ReadyFuture = Shared<LocalBoxFuture<'static, ()>>;
type InflightFuture = Shared<LocalBoxFuture<'static, Result<Option<Blob>>>>;

struct Inner {
    store: Rc<dyn BlobStore>,
    fetch_remote: FetchRemote,
    now: RefCell<Clock>,
    max_bytes: Cell<u64>,
    namespace: RefCell<String>,
    manifest: RefCell<HashMap<String, Entry>>,
    inflight: RefCell<HashMap<String, (u64, InflightFuture)>>,
    /// Paths the remote said do not exist. Per namespace, per process; cleared
    /// by [`BlobCache::invalidate`] (a `put` may have created the file since).
    missing: RefCell<HashSet<String>>,
    /// Resolves once the manifest has been rebuilt from disk; reads await it,
    /// so nothing refetches a file that is already there. Created on first use.
    ready: RefCell<Option<ReadyFuture>>,
    seq: Cell<u64>,
    hits: Cell<u64>,
    misses: Cell<u64>,
    evicted_entries: Cell<u64>,
    evicted_bytes: Cell<u64>,
}

/// The blob cache. Cheap to clone; clones share one cache.
#[derive(Clone)]
pub struct BlobCache(Rc<Inner>);

fn bump(c: &Cell<u64>, by: u64) {
    c.set(c.get() + by);
}

impl BlobCache {
    /// A cache over `store`, filling misses with `fetch_remote`, in the
    /// anonymous namespace.
    pub fn new(store: Rc<dyn BlobStore>, fetch_remote: FetchRemote, max_bytes: u64) -> Self {
        BlobCache(Rc::new(Inner {
            store,
            fetch_remote,
            now: RefCell::new(Rc::new(Utc::now)),
            max_bytes: Cell::new(max_bytes),
            namespace: RefCell::new(crate::modules::ref_tables::ANON_USER_ID.to_string()),
            manifest: RefCell::default(),
            inflight: RefCell::default(),
            missing: RefCell::default(),
            ready: RefCell::new(None),
            seq: Cell::new(0),
            hits: Cell::new(0),
            misses: Cell::new(0),
            evicted_entries: Cell::new(0),
            evicted_bytes: Cell::new(0),
        }))
    }

    /// Start in `namespace` instead of the anonymous one (before first use).
    pub fn with_namespace(self, namespace: impl Into<String>) -> Self {
        *self.0.namespace.borrow_mut() = namespace.into();
        self
    }

    /// Use `now` as the clock (tests).
    pub fn with_clock(self, now: Clock) -> Self {
        *self.0.now.borrow_mut() = now;
        self
    }

    pub fn namespace(&self) -> String {
        self.0.namespace.borrow().clone()
    }

    pub fn set_max_bytes(&self, max_bytes: u64) {
        self.0.max_bytes.set(max_bytes);
    }

    fn now(&self) -> DateTime<Utc> {
        let clock = self.0.now.borrow().clone();
        clock()
    }

    fn ready(&self) -> ReadyFuture {
        let mut ready = self.0.ready.borrow_mut();
        if let Some(f) = ready.as_ref() {
            return f.clone();
        }
        let weak = Rc::downgrade(&self.0);
        let f = async move {
            if let Some(inner) = weak.upgrade() {
                BlobCache(inner).reconcile().await;
            }
        }
        .boxed_local()
        .shared();
        *ready = Some(f.clone());
        f
    }

    // ---- reads ---------------------------------------------------------------------

    /// The bytes for `key`: disk first, the bucket second, filling disk on the
    /// way. `None` when the file exists in neither place.
    pub async fn read(&self, key: &BlobKey) -> Result<Option<Blob>> {
        self.read_with(key, false, true).await
    }

    /// [`BlobCache::read`] that skips the cache and refills it from remote.
    pub async fn reload(&self, key: &BlobKey) -> Result<Option<Blob>> {
        self.read_with(key, true, true).await
    }

    /// `reload` skips the cache; `persist` false serves a miss without
    /// writing it to disk.
    pub async fn read_with(
        &self,
        key: &BlobKey,
        reload: bool,
        persist: bool,
    ) -> Result<Option<Blob>> {
        self.ready().await;
        let id = key.id();
        if !reload {
            if self.0.missing.borrow().contains(&id) {
                return Ok(None);
            }
            if let Some(running) = self.inflight(&id) {
                return running.await;
            }
            if let Some(cached) = self.read_local(key, &id).await {
                bump(&self.0.hits, 1);
                return Ok(Some(cached));
            }
            // The local read yielded; a sibling may have started the same miss.
            if let Some(started) = self.inflight(&id) {
                return started.await;
            }
        }
        bump(&self.0.misses, 1);
        // The whole miss (fetch AND write-back) is what siblings join, so a
        // second view mounting mid-persist neither refetches nor reads a torn
        // file.
        let seq = self.0.seq.get() + 1;
        self.0.seq.set(seq);
        let weak: Weak<Inner> = Rc::downgrade(&self.0);
        let (key, id2) = (key.clone(), id.clone());
        let fut: InflightFuture = async move {
            let cache = BlobCache(weak.upgrade().ok_or(Error::Closed)?);
            let out = cache.miss(&key, &id2, persist).await;
            let mut inflight = cache.0.inflight.borrow_mut();
            if inflight.get(&id2).is_some_and(|(s, _)| *s == seq) {
                inflight.remove(&id2);
            }
            out
        }
        .boxed_local()
        .shared();
        self.0.inflight.borrow_mut().insert(id, (seq, fut.clone()));
        fut.await
    }

    fn inflight(&self, id: &str) -> Option<InflightFuture> {
        self.0.inflight.borrow().get(id).map(|(_, f)| f.clone())
    }

    async fn miss(&self, key: &BlobKey, id: &str, persist: bool) -> Result<Option<Blob>> {
        match (self.0.fetch_remote)(key.clone()).await? {
            None => {
                // Gone remotely: drop any stale copy and remember the miss.
                self.drop_local(key, id).await;
                self.0.missing.borrow_mut().insert(id.to_string());
                Ok(None)
            }
            Some(bytes) => {
                self.0.missing.borrow_mut().remove(id);
                if persist {
                    self.persist(key, id, &bytes).await;
                }
                Ok(Some(Blob::from(bytes)))
            }
        }
    }

    async fn read_local(&self, key: &BlobKey, id: &str) -> Option<Blob> {
        let ns = self.namespace();
        let bytes = match self.0.store.read(&ns, key).await {
            Ok(b) => b,
            Err(e) if is_key_error(&e) => return None,
            Err(e) => {
                tracing::warn!(target: "spooky", "blob read from local store failed ({id}): {e}");
                return None;
            }
        };
        let Some(bytes) = bytes else {
            self.0.manifest.borrow_mut().remove(id);
            return None;
        };
        let known = self.0.manifest.borrow().get(id).map(|e| e.size);
        if known.is_some_and(|size| size != bytes.len() as u64) {
            // Half-written, or overwritten while we ran: refill from remote.
            self.drop_local(key, id).await;
            return None;
        }
        let now = self.now();
        let touch = {
            let mut manifest = self.0.manifest.borrow_mut();
            match manifest.get_mut(id) {
                // A file on disk with no row: adopt it rather than download it
                // again.
                None => {
                    manifest.insert(
                        id.to_string(),
                        Entry {
                            key: key.clone(),
                            size: bytes.len() as u64,
                            last_access: now,
                        },
                    );
                    false
                }
                Some(e)
                    if (now - e.last_access)
                        .to_std()
                        .is_ok_and(|d| d >= TOUCH_INTERVAL) =>
                {
                    e.last_access = now;
                    true
                }
                Some(_) => false,
            }
        };
        if touch {
            let _ = self.0.store.touch(&ns, key, now).await;
        }
        Some(Blob::from(bytes))
    }

    // ---- writes ----------------------------------------------------------------------

    async fn persist(&self, key: &BlobKey, id: &str, bytes: &[u8]) {
        let ns = self.namespace();
        match self.0.store.write(&ns, key, bytes).await {
            Ok(size) => {
                let now = self.now();
                self.0.manifest.borrow_mut().insert(
                    id.to_string(),
                    Entry {
                        key: key.clone(),
                        size,
                        last_access: now,
                    },
                );
                self.enforce_budget().await;
            }
            // An unstorable path: served from remote, never persisted.
            Err(e) if is_key_error(&e) => {}
            Err(e) => tracing::warn!(target: "spooky", "blob write failed ({id}): {e}"),
        }
    }

    /// Forget one path everywhere. Called on `put`/`delete` of that path.
    pub async fn invalidate(&self, key: &BlobKey) {
        self.ready().await;
        let id = key.id();
        self.0.missing.borrow_mut().remove(&id);
        self.drop_local(key, &id).await;
    }

    async fn drop_local(&self, key: &BlobKey, id: &str) {
        self.0.manifest.borrow_mut().remove(id);
        let ns = self.namespace();
        match self.0.store.remove(&ns, key).await {
            Ok(()) => {}
            // Nothing could have been stored under it.
            Err(e) if is_key_error(&e) => {}
            Err(e) => tracing::warn!(target: "spooky", "blob delete failed ({id}): {e}"),
        }
    }

    fn total_bytes(&self) -> u64 {
        self.0.manifest.borrow().values().map(|e| e.size).sum()
    }

    /// Bring total bytes under budget by dropping the least recently used
    /// entries. Decoded bitmaps a view is showing live in the UI's own cache,
    /// so evicting a file never blanks anything on screen.
    async fn enforce_budget(&self) {
        let max = self.0.max_bytes.get();
        let mut total = self.total_bytes();
        if total <= max {
            return;
        }
        let target = (max as f64 * LOW_WATER).floor() as u64;
        let mut candidates: Vec<Entry> = self.0.manifest.borrow().values().cloned().collect();
        candidates.sort_by_key(|e| e.last_access);
        let mut dropped = 0;
        for entry in candidates {
            if total <= target {
                break;
            }
            self.drop_local(&entry.key, &entry.key.id()).await;
            total = total.saturating_sub(entry.size);
            bump(&self.0.evicted_entries, 1);
            bump(&self.0.evicted_bytes, entry.size);
            dropped += 1;
        }
        if dropped > 0 {
            tracing::info!(
                target: "spooky",
                "blob cache evicted {dropped} least-recently-used entries ({total}/{max} bytes)"
            );
        }
    }

    // ---- lifecycle -------------------------------------------------------------------

    /// Rebuild the manifest from what is on disk. Disk wins on existence:
    /// files with no row get a row seeded from mtime, and torn writes are swept
    /// by the walk itself.
    async fn reconcile(&self) {
        self.0.manifest.borrow_mut().clear();
        self.0.missing.borrow_mut().clear();
        let ns = self.namespace();
        let stats = match self.0.store.list(&ns).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "spooky", "blob cache reconcile failed to list local store: {e}");
                return;
            }
        };
        {
            let mut manifest = self.0.manifest.borrow_mut();
            for stat in stats {
                manifest.insert(
                    stat.key.id(),
                    Entry {
                        key: stat.key,
                        size: stat.size,
                        last_access: stat.mtime,
                    },
                );
            }
        }
        self.enforce_budget().await;
    }

    /// Repoint at another user's files, at once; the returned future resolves
    /// once their manifest is loaded. The old namespace's bytes stay on disk,
    /// so signing back in is still warm. Cheap when unchanged, so callers
    /// resolve the namespace per read rather than tracking auth themselves.
    pub fn set_namespace(
        &self,
        namespace: &str,
    ) -> impl std::future::Future<Output = ()> + 'static {
        if *self.0.namespace.borrow() == namespace {
            return self.ready();
        }
        let previous = self.ready();
        *self.0.namespace.borrow_mut() = namespace.to_string();
        self.0.inflight.borrow_mut().clear();
        let weak = Rc::downgrade(&self.0);
        let next = async move {
            previous.await;
            if let Some(inner) = weak.upgrade() {
                BlobCache(inner).reconcile().await;
            }
        }
        .boxed_local()
        .shared();
        *self.0.ready.borrow_mut() = Some(next.clone());
        next
    }

    /// Delete every cached byte in `namespace` (default: the current one).
    pub async fn clear(&self, namespace: Option<&str>) {
        self.ready().await;
        let current = self.namespace();
        let ns = namespace.unwrap_or(&current).to_string();
        if let Err(e) = self.0.store.clear(&ns).await {
            tracing::warn!(target: "spooky", "blob cache clear failed ({ns}): {e}");
        }
        if ns == current {
            self.0.manifest.borrow_mut().clear();
            self.0.missing.borrow_mut().clear();
        }
    }

    /// Warm the cache. Already-cached keys are skipped.
    pub async fn prefetch(&self, keys: &[BlobKey]) {
        self.ready().await;
        let queue: Vec<BlobKey> = {
            let manifest = self.0.manifest.borrow();
            keys.iter()
                .filter(|k| !manifest.contains_key(&k.id()))
                .cloned()
                .collect()
        };
        let cursor = Cell::new(0usize);
        let worker = || async {
            loop {
                let i = cursor.get();
                let Some(key) = queue.get(i) else { break };
                cursor.set(i + 1);
                if let Err(e) = self.read(key).await {
                    tracing::warn!(target: "spooky", "blob prefetch failed ({key}): {e}");
                }
            }
        };
        let lanes = queue.len().min(PREFETCH_CONCURRENCY);
        join_all((0..lanes).map(|_| worker())).await;
    }

    pub fn stats(&self) -> BlobCacheStats {
        BlobCacheStats {
            namespace: self.namespace(),
            entries: self.0.manifest.borrow().len(),
            total_bytes: self.total_bytes(),
            budget_bytes: self.0.max_bytes.get(),
            evicted_entries: self.0.evicted_entries.get(),
            evicted_bytes: self.0.evicted_bytes.get(),
            hits: self.0.hits.get(),
            misses: self.0.misses.get(),
            persistent: self.0.store.persistent(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize, fill: u8) -> Vec<u8> {
        vec![fill; n]
    }

    /// A fake bucket: `files` is the remote, `calls` counts remote reads.
    #[derive(Default)]
    struct Remote {
        files: RefCell<HashMap<String, Vec<u8>>>,
        calls: RefCell<Vec<String>>,
    }

    impl Remote {
        fn put(&self, id: &str, b: Vec<u8>) {
            self.files.borrow_mut().insert(id.to_string(), b);
        }

        fn calls(&self) -> usize {
            self.calls.borrow().len()
        }

        fn fetcher(self: &Rc<Self>) -> FetchRemote {
            let me = self.clone();
            Rc::new(move |key: BlobKey| {
                me.calls.borrow_mut().push(key.id());
                let found = me.files.borrow().get(&key.id()).cloned();
                async move { Ok(found) }.boxed_local()
            })
        }
    }

    struct Rig {
        dir: tempfile::TempDir,
        remote: Rc<Remote>,
    }

    impl Rig {
        fn new() -> Self {
            Rig {
                dir: tempfile::tempdir().unwrap(),
                remote: Rc::default(),
            }
        }

        fn cache(&self) -> BlobCache {
            BlobCache::new(
                Rc::new(FileBlobStore::new(self.dir.path())),
                self.remote.fetcher(),
                1 << 20,
            )
        }

        fn path(&self, rel: &str) -> PathBuf {
            self.dir.path().join(rel)
        }
    }

    fn key(bucket: &str, path: &str) -> BlobKey {
        BlobKey::new(bucket, path)
    }

    #[test]
    fn segments_encode_like_encode_uri_component_plus_dot() {
        assert_eq!(encode_segment("a_t.webp").unwrap(), "a_t%2Ewebp");
        assert_eq!(encode_segment("a b/é").unwrap(), "a%20b%2F%C3%A9");
        assert_eq!(encode_segment("x-_!~*'()").unwrap(), "x-_!~*'()");
        assert_eq!(decode_segment("a%20b%2F%C3%A9%2Ewebp"), "a b/é.webp");
        assert_eq!(decode_segment("100%"), "100%");
        assert!(encode_segment(&"x".repeat(201)).is_err());
        assert_eq!(key("b", "/../a/./b//c").id(), "b/a/b/c");
        assert!(key_segments(&key("b", "..")).is_err());
    }

    #[tokio::test]
    async fn miss_fetches_remote_and_writes_the_file_then_hit_reads_disk_only() {
        let rig = Rig::new();
        rig.remote.put("covers/a_t.webp", bytes(10, 7));
        let c = rig.cache();
        let k = key("covers", "a_t.webp");
        assert_eq!(&*c.read(&k).await.unwrap().unwrap(), &bytes(10, 7)[..]);
        assert_eq!(*rig.remote.calls.borrow(), vec!["covers/a_t.webp"]);
        assert_eq!(&*c.read(&k).await.unwrap().unwrap(), &bytes(10, 7)[..]);
        assert_eq!(rig.remote.calls(), 1, "second read is a hit");
        assert_eq!(c.stats().hits, 1);
        assert_eq!(c.stats().misses, 1);
        assert!(rig.path("anon/covers/a_t%2Ewebp").exists());
    }

    #[tokio::test]
    async fn a_new_cache_over_the_same_directory_serves_from_disk() {
        let rig = Rig::new();
        rig.remote.put("covers/a_t.webp", bytes(10, 7));
        rig.cache().read(&key("covers", "a_t.webp")).await.unwrap();
        rig.remote.calls.borrow_mut().clear();
        let warm = rig.cache();
        assert_eq!(
            &*warm
                .read(&key("covers", "a_t.webp"))
                .await
                .unwrap()
                .unwrap(),
            &bytes(10, 7)[..]
        );
        assert_eq!(rig.remote.calls(), 0);
        assert_eq!(warm.stats().entries, 1);
        assert_eq!(warm.stats().total_bytes, 10);
    }

    #[tokio::test]
    async fn nested_paths_round_trip_through_the_encoded_layout() {
        let rig = Rig::new();
        rig.remote.put("b/dir/sub/f.png", bytes(3, 1));
        let k = key("b", "dir/sub/f.png");
        rig.cache().read(&k).await.unwrap();
        let warm = rig.cache();
        warm.read(&k).await.unwrap();
        assert_eq!(rig.remote.calls(), 1);
        assert_eq!(warm.stats().entries, 1);
    }

    #[tokio::test]
    async fn torn_part_files_are_swept_at_start() {
        let rig = Rig::new();
        let dir = rig.path("anon/covers");
        std::fs::create_dir_all(&dir).unwrap();
        let torn = dir.join("x%2Ewebp.part-abc-0");
        std::fs::write(&torn, bytes(4, 1)).unwrap();
        let c = rig.cache();
        c.read(&key("covers", "nothing")).await.unwrap();
        assert!(!torn.exists());
        assert_eq!(c.stats().entries, 0);
    }

    #[tokio::test]
    async fn a_size_mismatch_against_the_manifest_refetches() {
        let rig = Rig::new();
        rig.remote.put("covers/a", bytes(10, 7));
        let c = rig.cache();
        let k = key("covers", "a");
        c.read(&k).await.unwrap();
        // Another writer truncates the file behind our back.
        std::fs::write(rig.path("anon/covers/a"), bytes(3, 7)).unwrap();
        assert_eq!(&*c.read(&k).await.unwrap().unwrap(), &bytes(10, 7)[..]);
        assert_eq!(rig.remote.calls(), 2);
    }

    #[tokio::test]
    async fn lru_eviction_keeps_the_cache_under_budget() {
        let rig = Rig::new();
        for i in 0..5 {
            rig.remote.put(&format!("covers/{i}"), bytes(100, 1));
        }
        let tick = Rc::new(Cell::new(0i64));
        let t = tick.clone();
        let c = BlobCache::new(
            Rc::new(FileBlobStore::new(rig.dir.path())),
            rig.remote.fetcher(),
            350,
        )
        .with_clock(Rc::new(move || {
            t.set(t.get() + 1);
            DateTime::from_timestamp_millis(1000 * t.get()).unwrap()
        }));
        for i in 0..5 {
            c.read(&key("covers", &i.to_string())).await.unwrap();
        }
        assert!(c.stats().total_bytes <= 350);
        assert!(c.stats().evicted_entries > 0);
        // The most recent entry survives, the oldest is gone.
        assert!(rig.path("anon/covers/4").exists());
        assert!(!rig.path("anon/covers/0").exists());
    }

    #[tokio::test]
    async fn a_missing_remote_file_is_remembered_until_invalidated() {
        let rig = Rig::new();
        let c = rig.cache();
        let k = key("covers", "gone");
        assert!(c.read(&k).await.unwrap().is_none());
        assert!(c.read(&k).await.unwrap().is_none());
        assert_eq!(rig.remote.calls(), 1);
        rig.remote.put("covers/gone", bytes(1, 1));
        c.invalidate(&k).await;
        assert_eq!(&*c.read(&k).await.unwrap().unwrap(), &[1u8][..]);
    }

    #[tokio::test]
    async fn concurrent_reads_of_one_key_issue_one_remote_read() {
        let rig = Rig::new();
        rig.remote.put("covers/a", bytes(1, 1));
        let c = rig.cache();
        let k = key("covers", "a");
        let all = join_all([c.read(&k), c.read(&k), c.read(&k)]).await;
        assert!(all.iter().all(|r| r.as_ref().unwrap().is_some()));
        assert_eq!(rig.remote.calls(), 1);
    }

    #[tokio::test]
    async fn namespaces_are_isolated_and_clear_wipes_one_of_them() {
        let rig = Rig::new();
        rig.remote.put("covers/a", bytes(2, 1));
        let c = rig.cache();
        let k = key("covers", "a");
        c.read(&k).await.unwrap();
        c.set_namespace("user1").await;
        assert_eq!(c.stats().entries, 0);
        c.read(&k).await.unwrap();
        assert_eq!(rig.remote.calls(), 2);
        c.clear(Some("anon")).await;
        assert!(!rig.path("anon").exists());
        assert!(rig.path("user1").exists());
        c.set_namespace("anon").await;
        c.read(&k).await.unwrap();
        assert_eq!(rig.remote.calls(), 3);
    }

    #[tokio::test]
    async fn reload_bypasses_the_cache_and_refills_it() {
        let rig = Rig::new();
        rig.remote.put("covers/a", bytes(1, 1));
        let c = rig.cache();
        let k = key("covers", "a");
        c.read(&k).await.unwrap();
        rig.remote.put("covers/a", bytes(9, 2));
        assert_eq!(&*c.reload(&k).await.unwrap().unwrap(), &bytes(9, 2)[..]);
        assert_eq!(&*c.read(&k).await.unwrap().unwrap(), &bytes(9, 2)[..]);
        assert_eq!(rig.remote.calls(), 2);
    }

    #[tokio::test]
    async fn a_memory_store_dedupes_within_the_process_only() {
        let rig = Rig::new();
        rig.remote.put("covers/a", bytes(1, 1));
        let c = BlobCache::new(
            Rc::new(MemoryBlobStore::default()),
            rig.remote.fetcher(),
            1 << 20,
        );
        c.read(&key("covers", "a")).await.unwrap();
        c.read(&key("covers", "a")).await.unwrap();
        assert_eq!(rig.remote.calls(), 1);
        assert!(!c.stats().persistent);
    }

    #[tokio::test]
    async fn prefetch_skips_cached_keys_and_unpersisted_reads_skip_disk() {
        let rig = Rig::new();
        for p in ["a", "b", "c", "d"] {
            rig.remote.put(&format!("covers/{p}"), bytes(2, 1));
        }
        let c = rig.cache();
        c.read(&key("covers", "a")).await.unwrap();
        let keys: Vec<BlobKey> = ["a", "b", "c", "d"]
            .iter()
            .map(|p| key("covers", p))
            .collect();
        c.prefetch(&keys).await;
        assert_eq!(rig.remote.calls(), 4);
        assert_eq!(c.stats().entries, 4);

        rig.remote.put("covers/e", bytes(2, 1));
        c.read_with(&key("covers", "e"), false, false)
            .await
            .unwrap();
        assert!(!rig.path("anon/covers/e").exists());
    }

    #[tokio::test]
    async fn an_unstorable_path_is_served_but_not_persisted() {
        let rig = Rig::new();
        let long = "x".repeat(250);
        rig.remote.put(&format!("covers/{long}"), bytes(2, 1));
        let c = rig.cache();
        assert!(c.read(&key("covers", &long)).await.unwrap().is_some());
        assert_eq!(c.stats().entries, 0);
    }
}
