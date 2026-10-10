//! Backing store for encoded row bytes.
//!
//! Rows are appended and never moved, so a stored [`RowSlot`] stays valid until
//! the slot is rewritten or compaction runs. Updates append a fresh copy and
//! repoint the slot; deletes drop the slot. Both are O(1), and the bytes they
//! orphan are tracked as `dead_bytes` for a later compaction pass.
//!
//! The trait exists so the *backing* can differ per platform while the encoding
//! stays shared. [`HeapArena`] is always compiled, which is what lets wasm32 —
//! the browser build, the Cloudflare Durable Object with its hard 128 MB
//! ceiling, and the Dart FFI dylib — get the encoding win even though none of
//! them can mmap a file. A file-backed implementation slots in behind the same
//! trait without any operator noticing.
//!
//! [`RowSlot`]: crate::circuit::row_table::RowSlot

/// Where a record lives: which segment, and the byte range within it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub seg: u16,
    pub off: u32,
    pub len: u32,
}

impl Span {
    /// Where this span starts, in the arena's append order.
    pub fn start(&self) -> Cursor {
        Cursor {
            seg: self.seg,
            off: self.off,
        }
    }
}

/// A point in an arena's append order: every slot appended at or after it
/// starts at or past it. Segments are never reused until `clear`, which is
/// what makes the order total; a checkpoint mark's epoch guards against
/// `clear` (see `row_table`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    pub seg: u16,
    pub off: u32,
}

/// Append-only byte storage addressed by [`Span`].
pub trait Arena: std::fmt::Debug + Send + Sync {
    /// Append `bytes`, returning where they landed.
    fn append(&mut self, bytes: &[u8]) -> Span;

    /// Read back a span. Returns an empty slice if the span is out of range,
    /// so a corrupt index degrades to a missing row rather than a panic.
    fn get(&self, span: Span) -> &[u8];

    /// Note that a span is no longer referenced. Accounting only — the bytes
    /// stay until compaction.
    fn free(&mut self, span: Span);

    /// Count `bytes` of the arena's existing contents as referenced: what a
    /// loader does for the records of an image it adopted, which came in
    /// accounted dead.
    fn adopt(&mut self, bytes: u64);

    /// The checkpoint image behind segment `seg`, if that is what it is.
    #[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
    fn image(&self, _seg: u16) -> Option<&ImageRef> {
        None
    }

    /// Bytes currently referenced by a live slot.
    fn live_bytes(&self) -> u64;

    /// Bytes orphaned by updates and deletes.
    fn dead_bytes(&self) -> u64;

    /// Total allocated capacity.
    fn capacity_bytes(&self) -> u64;

    /// Where the next append lands. Monotone until `clear`.
    fn end_cursor(&self) -> Cursor;

    /// Drop everything.
    fn clear(&mut self);
}

/// In-memory arena: one growable byte buffer.
///
/// A single segment is enough here because nothing needs to unmap or reclaim
/// address space; segmentation only earns its keep against a file-backed
/// implementation, which can then drop a whole segment at once.
#[derive(Debug, Default)]
pub struct HeapArena {
    buf: Vec<u8>,
    live: u64,
    dead: u64,
}

impl HeapArena {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bytes),
            live: 0,
            dead: 0,
        }
    }

    /// An arena over bytes that already hold records: a checkpoint image
    /// adopted as is. `live` is the part a slot references; the rest (the
    /// image's framing) is accounted dead from the start, and the next
    /// compaction drops it.
    pub fn from_buf(buf: Vec<u8>, live: u64) -> Self {
        let dead = (buf.len() as u64).saturating_sub(live);
        Self { buf, live, dead }
    }
}

impl Arena for HeapArena {
    fn append(&mut self, bytes: &[u8]) -> Span {
        let off = self.buf.len() as u32;
        self.buf.extend_from_slice(bytes);
        self.live += bytes.len() as u64;
        Span {
            seg: 0,
            off,
            len: bytes.len() as u32,
        }
    }

    fn get(&self, span: Span) -> &[u8] {
        let start = span.off as usize;
        let end = start.saturating_add(span.len as usize);
        self.buf.get(start..end).unwrap_or(&[])
    }

    fn free(&mut self, span: Span) {
        let len = span.len as u64;
        self.live = self.live.saturating_sub(len);
        self.dead += len;
    }

    fn adopt(&mut self, bytes: u64) {
        self.live += bytes;
        self.dead = self.dead.saturating_sub(bytes);
    }

    fn live_bytes(&self) -> u64 {
        self.live
    }

    fn dead_bytes(&self) -> u64 {
        self.dead
    }

    fn capacity_bytes(&self) -> u64 {
        self.buf.capacity() as u64
    }

    fn end_cursor(&self) -> Cursor {
        Cursor {
            seg: 0,
            off: self.buf.len() as u32,
        }
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.live = 0;
        self.dead = 0;
    }
}

/// File-backed arena: a chain of sparse files, each mapped once.
///
/// The point is not to save bytes — the encoding already did that — but to
/// move them out of anonymous memory. Pages of a file mapping are page cache:
/// under pressure the kernel reclaims the clean ones instead of the OOM killer
/// taking the process. Anonymous pages have no such option.
///
/// # Why segments, and why each is mapped once and never remapped
///
/// Appends only ever write inside an already-mapped segment, so no live
/// `&[u8]` is ever invalidated. Growing a single mapping would mean remapping,
/// which can move the base address and dangle every outstanding borrow — so
/// instead, a full segment is retired and a new one is mapped alongside it.
/// Retired segments stay mapped, which is what keeps their spans readable.
///
/// The alternative considered and rejected was refusing an append once the
/// mapping filled. That loses a row silently, and a row missing from the store
/// is not a visible failure: it is wrong query results, then a table-hash
/// mismatch against the scheduler, then `exit(2)` and a crash loop. Growing is
/// the only safe answer.
///
/// Files are sparse, so a segment's declared size costs nothing until written.
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
#[derive(Debug)]
pub struct MmapArena {
    segments: Vec<Seg>,
    segment_bytes: usize,
    dir: std::path::PathBuf,
    name: String,
    /// Distinguishes arenas for the same table within and across processes.
    instance: String,
    live: u64,
    dead: u64,
}

/// One segment, plus how much of it has actually been written.
///
/// The written length is what makes a read past the data return empty rather
/// than the mapping's zero fill — matching `HeapArena`, where a shorter buffer
/// simply has no such offset.
///
/// A segment is normally a file mapping. It falls back to plain memory when
/// one cannot be created — a full or read-only filesystem — because the
/// alternative is refusing the write, and a refused write is a row that
/// silently vanishes from the store. That is not a visible failure: it is
/// wrong query results, then a table-hash mismatch against the scheduler,
/// then `exit(2)`. Trading the page-cache benefit for correctness is the only
/// acceptable direction.
///
/// An image segment is a checkpoint file mapped read-only: full from the
/// start, never appended into, its pages clean page cache the kernel can drop
/// and re-read at will. See [`MmapArena::from_images`].
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct Seg {
    store: SegStore,
    written: usize,
}

/// A checkpoint image as the arena holds it: the read-only mapping, the open
/// file it was mapped from (the same inode whatever the path holds later),
/// and what a verification of its bodies needs. Cloneable, so a verifier can
/// hold the mapping without the table's lock; the arena never writes into it
/// and nothing writes the file in place (see `checkpoint`), so sharing the
/// bytes is sound.
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
#[derive(Debug, Clone)]
pub struct ImageRef {
    pub map: std::sync::Arc<memmap2::Mmap>,
    pub file: std::sync::Arc<std::fs::File>,
    /// The bodies region, which the mapping's heads point into.
    pub bodies: std::ops::Range<usize>,
    /// blake3 of the bodies region, as the file's trailer stores it.
    pub bodies_hash: [u8; 32],
}

#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
impl ImageRef {
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Whether both refer to one mapping (not merely one file).
    pub fn same_mapping(&self, other: &ImageRef) -> bool {
        std::sync::Arc::ptr_eq(&self.map, &other.map)
    }
}

#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
#[derive(Debug)]
enum SegStore {
    Mapped(memmap2::MmapMut),
    /// Fallback when the filesystem will not give us a mapping.
    Memory(Vec<u8>),
    /// A checkpoint image, mapped read-only.
    Image(ImageRef),
}

#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
impl Seg {
    fn bytes(&self) -> &[u8] {
        match &self.store {
            SegStore::Mapped(m) => m,
            SegStore::Memory(v) => v,
            SegStore::Image(i) => &i.map,
        }
    }

    /// The writable bytes; `None` for an image, which is never written.
    fn bytes_mut(&mut self) -> Option<&mut [u8]> {
        match &mut self.store {
            SegStore::Mapped(m) => Some(m),
            SegStore::Memory(v) => Some(v),
            SegStore::Image(_) => None,
        }
    }

    fn writable(&self) -> bool {
        !matches!(self.store, SegStore::Image(_))
    }

    fn capacity(&self) -> usize {
        self.bytes().len()
    }
}

#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
impl MmapArena {
    /// Create a file-backed arena under `dir`, growing in `segment_bytes`
    /// chunks.
    pub fn create(
        dir: &std::path::Path,
        name: &str,
        segment_bytes: usize,
    ) -> std::io::Result<Self> {
        let mut arena = Self {
            segments: Vec::new(),
            segment_bytes: segment_bytes.max(64 * 1024),
            dir: dir.to_path_buf(),
            name: name.to_string(),
            instance: next_instance_id(),
            live: 0,
            dead: 0,
        };
        arena.map_segment(arena.segment_bytes)?;
        Ok(arena)
    }

    /// An arena whose first segments are checkpoint images mapped read-only,
    /// in the order given: segment `i` is `images[i]`. Every byte of them is
    /// accounted dead until a loader adopts the records it indexed
    /// ([`Arena::adopt`]). Nothing is appended into an image: the first
    /// append maps a fresh writable segment under `dir`, exactly as a full
    /// segment does, so the images' spans stay valid for as long as the
    /// arena lives.
    pub fn from_images(
        dir: &std::path::Path,
        name: &str,
        segment_bytes: usize,
        images: Vec<ImageRef>,
    ) -> Self {
        let dead: u64 = images.iter().map(|i| i.len() as u64).sum();
        Self {
            segments: images
                .into_iter()
                .map(|image| Seg {
                    written: image.len(),
                    store: SegStore::Image(image),
                })
                .collect(),
            segment_bytes: segment_bytes.max(64 * 1024),
            dir: dir.to_path_buf(),
            name: sanitize_table_name(name),
            instance: next_instance_id(),
            live: 0,
            dead,
        }
    }

    /// Map one more segment of at least `min_bytes`.
    ///
    /// The backing file is unlinked immediately after mapping, so it cannot
    /// outlive the process or leak on a crash — the mapping holds the inode
    /// alive. This arena is a cache, not a durable store: SurrealDB remains
    /// the source of truth and nothing here survives a restart.
    fn map_segment(&mut self, min_bytes: usize) -> std::io::Result<()> {
        use std::fs::OpenOptions;
        std::fs::create_dir_all(&self.dir)?;
        let size = min_bytes.max(self.segment_bytes);
        // The path has to be unique per *arena instance*, not per table. Two
        // collections for the same table can coexist — a rebuilt circuit
        // alongside the outgoing one, or two processes sharing a directory —
        // and a second creator opening the same path with `truncate` would
        // truncate a file the first still has mapped. That is the one way a
        // file mapping faults on access, and it showed up immediately as
        // corrupted rows when the test suite ran against this backend.
        let path = self.dir.join(format!(
            "{}.{}.{}.arena",
            self.name,
            self.instance,
            self.segments.len()
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            // Fail rather than reuse: a collision here means the uniqueness
            // argument above is wrong, and silently sharing would corrupt.
            .create_new(true)
            .open(&path)?;
        file.set_len(size as u64)?;
        // SAFETY: the file was just created, is exclusively ours, and is
        // unlinked below so nothing else can truncate it underneath the
        // mapping — the one way a file mapping can fault on access.
        let map = unsafe { memmap2::MmapMut::map_mut(&file)? };
        let _ = std::fs::remove_file(&path);
        self.segments.push(Seg {
            store: SegStore::Mapped(map),
            written: 0,
        });
        Ok(())
    }

    /// Add a plain-memory segment. Used only when a mapping cannot be made.
    fn push_memory_segment(&mut self, min_bytes: usize) {
        let size = min_bytes.max(self.segment_bytes);
        self.segments.push(Seg {
            store: SegStore::Memory(vec![0u8; size]),
            written: 0,
        });
    }

    /// Whether `dir` can actually be written to.
    ///
    /// Probes by creating and removing a file rather than trusting metadata:
    /// a read-only mount, a full filesystem and a missing directory all have
    /// to end in the same answer, and the caller has to be able to fall back
    /// rather than fail. The Firecracker substrate boots with a read-only root
    /// and no data drive, so this returning false is a normal outcome, not an
    /// error.
    pub fn probe_writable(dir: &std::path::Path) -> bool {
        if std::fs::create_dir_all(dir).is_err() {
            return false;
        }
        let probe = dir.join(".arena-probe");
        match std::fs::write(&probe, b"1") {
            Ok(()) => {
                let _ = std::fs::remove_file(&probe);
                true
            }
            Err(_) => false,
        }
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
impl Arena for MmapArena {
    fn append(&mut self, bytes: &[u8]) -> Span {
        // A read-only image, or a segment without room: retire it and add
        // another. A mapping is preferred; memory is the fallback, because the
        // one thing this must never do is drop the write. See `Seg`.
        let needs_segment = match self.segments.last() {
            Some(s) => !s.writable() || bytes.len() > s.capacity().saturating_sub(s.written),
            None => true,
        };
        if needs_segment {
            if let Err(e) = self.map_segment(bytes.len()) {
                tracing::warn!(
                    error = %e,
                    "Could not extend the file-backed row arena — continuing in memory"
                );
                self.push_memory_segment(bytes.len());
            }
        }
        let seg = self.segments.len() - 1;
        let s = &mut self.segments[seg];
        let off = s.written;
        if let Some(dst) = s.bytes_mut() {
            dst[off..off + bytes.len()].copy_from_slice(bytes);
        }
        s.written += bytes.len();
        self.live += bytes.len() as u64;
        Span {
            seg: seg as u16,
            off: off as u32,
            len: bytes.len() as u32,
        }
    }

    fn get(&self, span: Span) -> &[u8] {
        if span.len == 0 {
            return &[];
        }
        let Some(seg) = self.segments.get(span.seg as usize) else {
            return &[];
        };
        let start = span.off as usize;
        let end = start.saturating_add(span.len as usize);
        // Bounded by what was written, not by the segment's capacity: a stale
        // span into a re-created segment must read empty, not its zero fill.
        if end > seg.written {
            return &[];
        }
        seg.bytes().get(start..end).unwrap_or(&[])
    }

    fn free(&mut self, span: Span) {
        let len = span.len as u64;
        self.live = self.live.saturating_sub(len);
        self.dead += len;
    }

    fn adopt(&mut self, bytes: u64) {
        self.live += bytes;
        self.dead = self.dead.saturating_sub(bytes);
    }

    fn image(&self, seg: u16) -> Option<&ImageRef> {
        match &self.segments.get(seg as usize)?.store {
            SegStore::Image(image) => Some(image),
            _ => None,
        }
    }

    fn live_bytes(&self) -> u64 {
        self.live
    }

    fn dead_bytes(&self) -> u64 {
        self.dead
    }

    fn capacity_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.capacity() as u64).sum()
    }

    fn end_cursor(&self) -> Cursor {
        // An image, or no segment at all, puts the next append in a segment
        // that does not exist yet.
        match self.segments.last() {
            Some(s) if s.writable() => Cursor {
                seg: (self.segments.len() - 1) as u16,
                off: s.written as u32,
            },
            _ => Cursor {
                seg: self.segments.len() as u16,
                off: 0,
            },
        }
    }

    fn clear(&mut self) {
        // Drop every mapping rather than just resetting the write cursor. The
        // bytes stay in a mapping until it is unmapped, so keeping one around
        // would let a stale span read back cleared data — where `HeapArena`
        // reads empty. Dropping them makes a stale span reference a segment
        // that no longer exists, which `get` reports as absent.
        self.segments.clear();
        self.live = 0;
        self.dead = 0;
        // A failure here leaves the arena with no segments, which is
        // self-healing: the next append maps one.
        let _ = self.map_segment(self.segment_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_back() {
        let mut a = HeapArena::new();
        let one = a.append(b"hello");
        let two = a.append(b"world!");
        assert_eq!(a.get(one), b"hello");
        assert_eq!(a.get(two), b"world!");
        assert_eq!(a.live_bytes(), 11);
        assert_eq!(a.dead_bytes(), 0);
    }

    #[test]
    fn appending_does_not_move_earlier_spans() {
        // Slots must stay valid across later appends; growth reallocating the
        // buffer is fine because spans are offsets, not pointers.
        let mut a = HeapArena::new();
        let first = a.append(b"first");
        for i in 0..1000u32 {
            a.append(&i.to_le_bytes());
        }
        assert_eq!(a.get(first), b"first");
    }

    #[test]
    fn end_cursor_advances_with_appends_and_resets_on_clear() {
        let mut a = HeapArena::new();
        assert_eq!(a.end_cursor(), Cursor { seg: 0, off: 0 });
        let first = a.append(b"12345");
        assert_eq!(first.start(), Cursor { seg: 0, off: 0 });
        assert_eq!(a.end_cursor(), Cursor { seg: 0, off: 5 });
        let second = a.append(b"67");
        assert!(second.start() >= Cursor { seg: 0, off: 5 });
        assert!(first.start() < Cursor { seg: 0, off: 5 });
        a.clear();
        assert_eq!(a.end_cursor(), Cursor { seg: 0, off: 0 });
    }

    #[test]
    fn free_moves_bytes_from_live_to_dead() {
        let mut a = HeapArena::new();
        let s = a.append(b"1234");
        a.append(b"56");
        a.free(s);
        assert_eq!(a.live_bytes(), 2);
        assert_eq!(a.dead_bytes(), 4);
        // Freed bytes are still readable — nothing has moved yet.
        assert_eq!(a.get(s), b"1234");
    }

    #[test]
    fn out_of_range_span_reads_empty_rather_than_panicking() {
        let a = HeapArena::new();
        let bogus = Span {
            seg: 0,
            off: 9_999,
            len: 10,
        };
        assert!(a.get(bogus).is_empty());

        let mut a = HeapArena::new();
        a.append(b"short");
        // A span that starts inside but runs past the end.
        assert!(a
            .get(Span {
                seg: 0,
                off: 3,
                len: 100
            })
            .is_empty());
        // A length that would overflow the addition.
        assert!(a
            .get(Span {
                seg: 0,
                off: u32::MAX,
                len: u32::MAX
            })
            .is_empty());
    }

    #[test]
    fn clear_resets_accounting() {
        let mut a = HeapArena::new();
        let s = a.append(b"data");
        a.free(s);
        a.clear();
        assert_eq!(a.live_bytes(), 0);
        assert_eq!(a.dead_bytes(), 0);
        assert!(a.get(s).is_empty());
    }
}

#[cfg(all(test, feature = "mmap-store", not(target_arch = "wasm32")))]
mod mmap_tests {
    use super::*;

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("ssp-arena-test-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn append_and_read_back() {
        let dir = tmpdir("basic");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        let one = a.append(b"hello");
        let two = a.append(b"world!");
        assert_eq!(a.get(one), b"hello");
        assert_eq!(a.get(two), b"world!");
        assert_eq!(a.live_bytes(), 11);
        assert_eq!(a.capacity_bytes(), 64 * 1024);
    }

    /// The whole reason the file is created at full size and mapped once:
    /// a borrow taken before later appends must stay valid.
    #[test]
    fn earlier_spans_survive_later_appends() {
        let dir = tmpdir("stable");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        let first = a.append(b"first");
        for i in 0..1000u32 {
            a.append(&i.to_le_bytes());
        }
        assert_eq!(a.get(first), b"first");
    }

    /// Filling a segment must map another rather than dropping the write.
    /// A silently lost row is not a visible failure — it is wrong results,
    /// then a hash mismatch, then a crash loop.
    #[test]
    fn a_full_segment_grows_instead_of_losing_the_row() {
        let dir = tmpdir("grow");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        assert_eq!(a.segment_count(), 1);

        // Write well past one segment and keep every span.
        let mut spans = Vec::new();
        for i in 0..400u32 {
            let payload = vec![(i % 251) as u8; 1000];
            spans.push((a.append(&payload), payload));
        }
        assert!(a.segment_count() > 1, "must have grown");

        // Every span, including ones in retired segments, still reads back.
        for (span, expected) in &spans {
            assert_eq!(a.get(*span), expected.as_slice(), "span {span:?} lost");
            assert_ne!(span.len, 0, "no append may be dropped");
        }
    }

    /// A single value larger than the configured segment size still has to fit.
    #[test]
    fn an_oversized_value_gets_its_own_segment() {
        let dir = tmpdir("oversized");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        let big = vec![7u8; 300_000];
        let span = a.append(&big);
        assert_eq!(a.get(span), big.as_slice());
    }

    #[test]
    fn out_of_range_reads_are_empty_rather_than_a_panic() {
        let dir = tmpdir("bounds");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        a.append(b"abc");
        // Past the end of the mapping.
        assert!(a
            .get(Span { seg: 0, off: u32::MAX, len: u32::MAX })
            .is_empty());
        // A segment that does not exist.
        assert!(a.get(Span { seg: 99, off: 0, len: 4 }).is_empty());
        // A zero-length span.
        assert!(a.get(Span { seg: 0, off: 0, len: 0 }).is_empty());
    }

    /// The backing file is unlinked right after mapping, so a crash cannot
    /// leave arena files behind.
    #[test]
    fn backing_file_is_unlinked_immediately() {
        let dir = tmpdir("unlink");
        let a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".arena"))
            .collect();
        assert!(leftovers.is_empty(), "arena files must not persist: {leftovers:?}");
        drop(a);
    }

    /// Two arenas for the same table in the same directory must not share a
    /// file. They previously did, and the second one's `truncate` corrupted
    /// the first one's live mapping — which surfaced as unrelated circuit
    /// tests failing once the suite ran against this backend.
    #[test]
    fn concurrent_arenas_for_one_table_do_not_share_a_file() {
        let dir = tmpdir("collide");
        let mut a = MmapArena::create(&dir, "thread", 64 * 1024).unwrap();
        let span_a = a.append(b"written by A");

        let mut b = MmapArena::create(&dir, "thread", 64 * 1024).unwrap();
        let span_b = b.append(b"written by B, longer");

        assert_eq!(a.get(span_a), b"written by A", "A's mapping was clobbered");
        assert_eq!(b.get(span_b), b"written by B, longer");
    }

    /// A row must never be dropped because the filesystem is full or
    /// read-only: the arena falls back to memory rather than refusing the
    /// write. A refused write is not a visible failure — it is a row silently
    /// missing from the store.
    #[test]
    fn an_unusable_directory_falls_back_to_memory_rather_than_dropping_writes() {
        let dir = tmpdir("nofs");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        // Make further segment creation impossible by removing the directory
        // and putting a regular file in its place.
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();

        // Write far past the first segment. Every span must still read back.
        let mut spans = Vec::new();
        for i in 0..200u32 {
            let payload = vec![(i % 251) as u8; 1000];
            spans.push((a.append(&payload), payload));
        }
        for (span, expected) in &spans {
            assert_ne!(span.len, 0, "no append may be dropped");
            assert_eq!(a.get(*span), expected.as_slice(), "span {span:?} lost");
        }
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn probe_detects_writability() {
        let dir = tmpdir("probe");
        assert!(MmapArena::probe_writable(&dir));
        assert!(!dir.join(".arena-probe").exists(), "probe must clean up");
        // A path under a file rather than a directory cannot be created.
        let f = dir.join("afile");
        std::fs::write(&f, b"x").unwrap();
        assert!(!MmapArena::probe_writable(&f.join("nested")));
    }

    fn map_path(path: &std::path::Path) -> ImageRef {
        let file = std::fs::File::open(path).unwrap();
        let map = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let len = map.len();
        ImageRef {
            map: std::sync::Arc::new(map),
            file: std::sync::Arc::new(file),
            bodies: 0..len,
            bodies_hash: [0; 32],
        }
    }

    fn image_file(dir: &std::path::Path, content: &[u8]) -> ImageRef {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join("image.rows");
        std::fs::write(&path, content).unwrap();
        map_path(&path)
    }

    /// The image is segment 0 as it lies on disk; the first append goes to a
    /// fresh writable segment and the image's spans keep reading.
    #[test]
    fn an_image_segment_is_read_only_and_appends_go_elsewhere() {
        let dir = tmpdir("image");
        let image = image_file(&dir, b"header[record-a][record-b]trailer");
        let mut a = MmapArena::from_images(&dir, "t", 64 * 1024, vec![image]);
        assert_eq!(a.segment_count(), 1);
        assert_eq!(a.live_bytes(), 0);
        assert_eq!(a.dead_bytes(), 33, "an image is dead until its records are adopted");
        a.adopt(20);
        assert_eq!(a.live_bytes(), 20);
        assert_eq!(a.dead_bytes(), 33 - 20);
        let in_image = Span { seg: 0, off: 7, len: 8 };
        assert_eq!(a.get(in_image), b"record-a");
        // Past the file's end reads empty, never the mapping's zero fill.
        assert!(a.get(Span { seg: 0, off: 30, len: 10 }).is_empty());
        assert!(a.image(0).is_some());
        assert!(a.image(1).is_none());

        let appended = a.append(b"record-c");
        assert_eq!(a.segment_count(), 2, "the image must not be written into");
        assert_eq!(appended.seg, 1);
        assert_eq!(a.get(appended), b"record-c");
        assert_eq!(a.get(in_image), b"record-a");
        assert_eq!(a.live_bytes(), 28);
        assert!(a.image(1).is_none(), "a writable segment is not an image");

        a.free(in_image);
        assert_eq!(a.dead_bytes(), 33 - 20 + 8);
        assert_eq!(a.get(in_image), b"record-a", "freed bytes stay until compaction");
    }

    #[test]
    fn clear_drops_the_image() {
        let dir = tmpdir("image-clear");
        let image = image_file(&dir, b"0123456789");
        let mut a = MmapArena::from_images(&dir, "t", 64 * 1024, vec![image]);
        a.adopt(10);
        let s = Span { seg: 0, off: 2, len: 3 };
        assert_eq!(a.get(s), b"234");
        a.clear();
        assert!(a.get(s).is_empty(), "cleared image must not read back");
        assert!(a.image(0).is_none(), "the image is gone with the clear");
        assert_eq!(a.live_bytes(), 0);
        let again = a.append(b"new");
        assert_eq!(a.get(again), b"new");
    }

    /// After an image the next append opens a new segment, and the cursor
    /// says so before it exists.
    #[test]
    fn end_cursor_after_an_image_points_at_the_next_segment() {
        let dir = tmpdir("image-cursor");
        let image = image_file(&dir, b"0123456789");
        let mut a = MmapArena::from_images(&dir, "t", 64 * 1024, vec![image]);
        assert_eq!(a.end_cursor(), Cursor { seg: 1, off: 0 });
        let s = a.append(b"new");
        assert_eq!(s.start(), Cursor { seg: 1, off: 0 });
        assert_eq!(a.end_cursor(), Cursor { seg: 1, off: 3 });
        assert!(Span { seg: 0, off: 9, len: 1 }.start() < Cursor { seg: 1, off: 0 });
    }

    /// Images are segments in the order given; appends land after them.
    #[test]
    fn from_images_orders_segments_as_given() {
        let dir = tmpdir("images");
        let one = image_file(&dir.join("a"), b"first-image");
        let two = image_file(&dir.join("b"), b"second-image");
        let mut a = MmapArena::from_images(&dir, "t", 64 * 1024, vec![one, two]);
        assert_eq!(a.segment_count(), 2);
        assert_eq!(a.get(Span { seg: 0, off: 0, len: 5 }), b"first");
        assert_eq!(a.get(Span { seg: 1, off: 0, len: 6 }), b"second");
        assert_eq!(a.dead_bytes(), 11 + 12);
        let appended = a.append(b"third");
        assert_eq!(appended.seg, 2);
        assert!(a.image(1).is_some());
        assert!(a.image(2).is_none());
    }

    /// Two arenas over one image file (a rebuilt circuit beside the old one,
    /// or two processes) share the page cache and nothing else.
    #[test]
    fn two_arenas_may_map_the_same_image() {
        let dir = tmpdir("image-shared");
        let one = image_file(&dir, b"shared-bytes");
        let two = map_path(&dir.join("image.rows"));
        assert!(!one.same_mapping(&two));
        let mut a = MmapArena::from_images(&dir, "t", 64 * 1024, vec![one]);
        let mut b = MmapArena::from_images(&dir, "t", 64 * 1024, vec![two]);
        a.adopt(12);
        b.adopt(12);
        let sa = a.append(b"A");
        let sb = b.append(b"B");
        assert_eq!(a.get(Span { seg: 0, off: 0, len: 6 }), b"shared");
        assert_eq!(b.get(Span { seg: 0, off: 0, len: 6 }), b"shared");
        assert_eq!(a.get(sa), b"A");
        assert_eq!(b.get(sb), b"B");
    }

    #[test]
    fn clear_resets_and_reuses_the_mapping() {
        let dir = tmpdir("clear");
        let mut a = MmapArena::create(&dir, "t", 64 * 1024).unwrap();
        let s = a.append(b"data");
        a.free(s);
        a.clear();
        assert_eq!(a.live_bytes(), 0);
        assert_eq!(a.dead_bytes(), 0);
        assert!(a.get(s).is_empty(), "cleared bytes must not read back");
        let again = a.append(b"new");
        assert_eq!(a.get(again), b"new");
    }
}

// --- runtime backing selection ---

/// How row bytes should be backed for this process.
#[derive(Debug, Clone)]
pub enum ArenaBacking {
    /// Anonymous memory. The only option on wasm32, and the fallback anywhere
    /// a writable directory is unavailable.
    Heap,
    /// A directory of sparse files. Pages become reclaimable page cache rather
    /// than anonymous memory.
    Files {
        dir: std::path::PathBuf,
        segment_bytes: usize,
    },
}

/// Default segment size. Sparse, so this is address space rather than disk
/// until written.
#[cfg_attr(not(all(feature = "mmap-store", not(target_arch = "wasm32"))), allow(dead_code))]
const DEFAULT_SEGMENT_BYTES: usize = 64 * 1024 * 1024;

/// Backing chosen for this process, resolved once.
///
/// Opt-in via `SPKY_SSP_ARENA_DIR`, and only honoured if that directory is
/// genuinely writable. The probe is not defensive programming for its own
/// sake: the Firecracker substrate boots read-only with no data drive, so
/// "configured but unusable" is a state that really occurs, and failing hard
/// there would take down every SSP on that substrate. Falling back costs the
/// page-cache benefit and keeps everything else.
pub fn configured_backing() -> &'static ArenaBacking {
    static BACKING: std::sync::OnceLock<ArenaBacking> = std::sync::OnceLock::new();
    BACKING.get_or_init(resolve_backing)
}

fn resolve_backing() -> ArenaBacking {
    #[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
    {
        let Some(dir) = std::env::var_os("SPKY_SSP_ARENA_DIR") else {
            return ArenaBacking::Heap;
        };
        let dir = std::path::PathBuf::from(dir);
        if !MmapArena::probe_writable(&dir) {
            tracing::warn!(
                dir = %dir.display(),
                "SPKY_SSP_ARENA_DIR is not writable — falling back to in-memory row storage"
            );
            return ArenaBacking::Heap;
        }
        let segment_bytes = std::env::var("SPKY_SSP_ARENA_SEGMENT_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|mb| *mb > 0)
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(DEFAULT_SEGMENT_BYTES);
        tracing::info!(
            dir = %dir.display(),
            segment_bytes,
            "Row arena is file-backed"
        );
        ArenaBacking::Files { dir, segment_bytes }
    }
    #[cfg(not(all(feature = "mmap-store", not(target_arch = "wasm32"))))]
    {
        ArenaBacking::Heap
    }
}

/// Build an arena for one table, honouring [`configured_backing`].
///
/// Falls back to the heap if the mapping cannot be created, so a disk that
/// fills or goes read-only mid-run degrades to the previous behaviour rather
/// than failing the table.
#[cfg_attr(not(all(feature = "mmap-store", not(target_arch = "wasm32"))), allow(unused_variables))]
pub fn new_arena(table: &str) -> Box<dyn Arena> {
    match configured_backing() {
        ArenaBacking::Heap => Box::new(HeapArena::new()),
        #[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
        ArenaBacking::Files { dir, segment_bytes } => {
            match MmapArena::create(dir, &sanitize_table_name(table), *segment_bytes) {
                Ok(a) => Box::new(a),
                Err(e) => {
                    tracing::warn!(
                        table, error = %e,
                        "Could not create a file-backed arena — using memory for this table"
                    );
                    Box::new(HeapArena::new())
                }
            }
        }
        #[cfg(not(all(feature = "mmap-store", not(target_arch = "wasm32"))))]
        ArenaBacking::Files { .. } => Box::new(HeapArena::new()),
    }
}

/// A process- and instance-unique token for arena filenames.
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
fn next_instance_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Make a table name safe to embed in a filename. Table names reach us from
/// the database schema, so they are not assumed to be path-safe.
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
fn sanitize_table_name(table: &str) -> String {
    let cleaned: String = table
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();
    // Distinguish names that collide after cleaning, and give an empty name
    // something to be.
    format!("{cleaned}-{:016x}", fnv1a(table))
}

#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

#[cfg(all(test, feature = "mmap-store", not(target_arch = "wasm32")))]
mod backing_tests {
    use super::*;

    #[test]
    fn table_names_are_made_path_safe_without_colliding() {
        let a = sanitize_table_name("_00_list_ref_user_abc");
        let b = sanitize_table_name("thread");
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'));
        assert_ne!(a, b);
        // Names that clean to the same string must stay distinct.
        assert_ne!(sanitize_table_name("a/b"), sanitize_table_name("a:b"));
        // And an empty name still produces something usable.
        assert!(!sanitize_table_name("").is_empty());
    }

    #[test]
    fn an_unwritable_dir_falls_back_to_heap() {
        // `new_arena` must never fail: point it at a path that cannot exist.
        let dir = std::env::temp_dir().join("ssp-arena-nope/deep");
        let file = std::env::temp_dir().join("ssp-arena-nope");
        let _ = std::fs::remove_dir_all(&file);
        std::fs::write(&file, b"not a directory").unwrap();
        assert!(!MmapArena::probe_writable(&dir));
        let _ = std::fs::remove_file(&file);
    }
}
