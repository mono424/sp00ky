import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../logger/logger.dart';

/// Cache for bucket files (the Dart twin of `packages/core/src/services/blobs`).
///
///   L1  bytes on disk, durable                 ([BlobStore])
///   L2  the remote bucket over the sync socket ([BlobCache.fetchRemote])
///
/// There is no in-memory byte layer: on the render side Flutter's `ImageCache`
/// already keeps the decoded bitmaps hot, and a disk read is cheap next to the
/// serialized remote queue a miss would join. What the cache does hold in
/// memory is the manifest (sizes and access times for eviction), the in-flight
/// map (two widgets mounting the same cover issue one query), and the negative
/// set (a missing cover is normal and must not be re-asked on every rebuild).
///
/// Nothing is dropped because it got old: the row referencing an image is in
/// the local store indefinitely, so the image has to be too. Bytes only go
/// away when the app invalidated them (`put`/`delete` on that path), when a
/// read found a torn file, or when the cache is over budget and this is the
/// least recently used entry.

/// Identifies one cached file: the bucket it lives in and its path within.
class BlobKey {
  const BlobKey(this.bucket, this.path);
  final String bucket;
  final String path;

  /// Path segments, `..`/`.`/empty stripped so a crafted path can't escape
  /// the cache root.
  List<String> get segments => path
      .split('/')
      .where((s) => s.isNotEmpty && s != '.' && s != '..')
      .toList(growable: false);

  /// Stable manifest id. Mirrors the on-disk layout, decoded.
  String get id => '$bucket/${segments.join('/')}';

  @override
  bool operator ==(Object other) =>
      other is BlobKey && other.bucket == bucket && other.path == path;
  @override
  int get hashCode => Object.hash(bucket, path);
  @override
  String toString() => id;
}

/// What a directory walk can tell us about a stored file, with no manifest.
class BlobStat {
  const BlobStat(this.key, this.size, this.mtime);
  final BlobKey key;
  final int size;
  final DateTime mtime;
}

/// A path segment we refuse to store — the caller degrades to no persistence.
class BlobKeyError extends ArgumentError {
  BlobKeyError(String message) : super(message);
}

/// Byte storage for cached bucket files, keyed under a namespace (the local
/// bucket id of the signed-in user, so two accounts on one device never see
/// each other's files).
abstract class BlobStore {
  /// False for [MemoryBlobStore]: the cache still dedupes within the process,
  /// but nothing survives a restart.
  bool get persistent;
  Future<Uint8List?> read(String namespace, BlobKey key);

  /// Returns the number of bytes written.
  Future<int> write(String namespace, BlobKey key, Uint8List bytes);
  Future<void> remove(String namespace, BlobKey key);

  /// Record an access so a rebuilt manifest orders eviction correctly.
  Future<void> touch(String namespace, BlobKey key, DateTime at);

  /// Every committed file under the namespace. Sweeps torn writes.
  Future<List<BlobStat>> list(String namespace);

  /// Drop the whole namespace.
  Future<void> clear(String namespace);
}

/// Marks a half-written file. Committed names can never contain a literal `.`
/// (see [encodeSegment]), so this suffix is unambiguous: anything wearing it at
/// walk time is a write that died before commit, and is swept.
const String _partMarker = '.part-';

/// Filesystem names are capped around 255 bytes; stay well clear.
const int _maxSegmentLength = 200;

/// Percent-encode a path segment, and escape `.` on top of that. Escaping the
/// dot is what makes [_partMarker] safe. Same encoding as the TS OPFS store, so
/// the on-disk layout is one layout across clients.
String encodeSegment(String segment) {
  final encoded = Uri.encodeComponent(segment).replaceAll('.', '%2E');
  if (encoded.length > _maxSegmentLength) {
    throw BlobKeyError(
        'path segment too long to store (${encoded.length} > $_maxSegmentLength)');
  }
  return encoded;
}

String decodeSegment(String segment) => Uri.decodeComponent(segment);

/// `<bucket>/<...path>`, encoded: the directory chain plus filename for a key.
List<String> keySegments(BlobKey key) {
  final parts = key.segments;
  if (parts.isEmpty) {
    throw BlobKeyError('empty file path for bucket "${key.bucket}"');
  }
  return [key.bucket, ...parts].map(encodeSegment).toList(growable: false);
}

/// Files on disk under `<root>/<namespace>/<bucket>/<...segments>`. Writes go
/// to a `.part-` temp name and commit by rename, so a crash mid-write leaves a
/// sweepable file rather than a truncated real one.
class FileBlobStore implements BlobStore {
  FileBlobStore(this.root);
  final Directory root;
  int _tempCounter = 0;
  final String _tempToken =
      DateTime.now().microsecondsSinceEpoch.toRadixString(36);

  @override
  bool get persistent => true;

  Directory _nsDir(String namespace) =>
      Directory('${root.path}/${encodeSegment(namespace)}');

  File _file(String namespace, BlobKey key) =>
      File('${_nsDir(namespace).path}/${keySegments(key).join('/')}');

  @override
  Future<Uint8List?> read(String namespace, BlobKey key) async {
    final file = _file(namespace, key);
    try {
      return await file.readAsBytes();
    } on FileSystemException {
      return null;
    }
  }

  @override
  Future<int> write(String namespace, BlobKey key, Uint8List bytes) async {
    final target = _file(namespace, key);
    await target.parent.create(recursive: true);
    final temp =
        File('${target.path}$_partMarker$_tempToken-${_tempCounter++}');
    try {
      await temp.writeAsBytes(bytes, flush: true);
      await temp.rename(target.path);
    } catch (_) {
      try {
        await temp.delete();
      } catch (_) {}
      rethrow;
    }
    return bytes.length;
  }

  @override
  Future<void> remove(String namespace, BlobKey key) async {
    try {
      await _file(namespace, key).delete();
    } on FileSystemException {
      // Already gone. Directories are left behind deliberately: pruning them
      // would race a concurrent write into the same folder for no measurable
      // space saving.
    }
  }

  @override
  Future<void> touch(String namespace, BlobKey key, DateTime at) async {
    try {
      await _file(namespace, key).setLastModified(at);
    } on FileSystemException {
      // Vanished under us; the next read refetches.
    }
  }

  @override
  Future<List<BlobStat>> list(String namespace) async {
    final ns = _nsDir(namespace);
    if (!await ns.exists()) return const [];
    final out = <BlobStat>[];
    final prefix = ns.path.length + 1;
    await for (final entity in ns.list(recursive: true, followLinks: false)) {
      if (entity is! File) continue;
      final rel = entity.path.substring(prefix).split(Platform.pathSeparator);
      if (rel.last.contains(_partMarker)) {
        try {
          await entity.delete();
        } catch (_) {}
        continue;
      }
      // A file directly under the namespace root has no bucket segment.
      if (rel.length < 2) continue;
      try {
        final stat = await entity.stat();
        out.add(BlobStat(
          BlobKey(decodeSegment(rel.first),
              rel.skip(1).map(decodeSegment).join('/')),
          stat.size,
          stat.modified,
        ));
      } catch (_) {
        // Vanished mid-walk: skip. Reconcile is best-effort.
      }
    }
    return out;
  }

  @override
  Future<void> clear(String namespace) async {
    final ns = _nsDir(namespace);
    if (await ns.exists()) await ns.delete(recursive: true);
  }
}

/// In-memory byte store. Used when no cache directory is configured (the cache
/// still dedupes within the process) and as the test double.
class MemoryBlobStore implements BlobStore {
  final _files = <String, Map<String, (Uint8List, DateTime)>>{};

  @override
  bool get persistent => false;

  Map<String, (Uint8List, DateTime)> _ns(String namespace) =>
      _files.putIfAbsent(namespace, () => {});

  @override
  Future<Uint8List?> read(String namespace, BlobKey key) async {
    keySegments(key);
    return _ns(namespace)[key.id]?.$1;
  }

  @override
  Future<int> write(String namespace, BlobKey key, Uint8List bytes) async {
    keySegments(key);
    _ns(namespace)[key.id] = (bytes, DateTime.now());
    return bytes.length;
  }

  @override
  Future<void> remove(String namespace, BlobKey key) async {
    _ns(namespace).remove(key.id);
  }

  @override
  Future<void> touch(String namespace, BlobKey key, DateTime at) async {
    final entry = _ns(namespace)[key.id];
    if (entry != null) _ns(namespace)[key.id] = (entry.$1, at);
  }

  @override
  Future<List<BlobStat>> list(String namespace) async => [
        for (final e in _ns(namespace).entries)
          BlobStat(
            BlobKey(e.key.substring(0, e.key.indexOf('/')),
                e.key.substring(e.key.indexOf('/') + 1)),
            e.value.$1.length,
            e.value.$2,
          ),
      ];

  @override
  Future<void> clear(String namespace) async {
    _files.remove(namespace);
  }
}

class BlobCacheStats {
  const BlobCacheStats({
    required this.namespace,
    required this.entries,
    required this.totalBytes,
    required this.budgetBytes,
    required this.evictedEntries,
    required this.evictedBytes,
    required this.hits,
    required this.misses,
    required this.persistent,
  });
  final String namespace;
  final int entries;
  final int totalBytes;
  final int budgetBytes;
  final int evictedEntries;
  final int evictedBytes;
  final int hits;
  final int misses;
  final bool persistent;
}

class _Entry {
  _Entry(this.key, this.size, this.lastAccess);
  final BlobKey key;
  int size;
  DateTime lastAccess;
}

/// Evict down to this fraction of the budget, so eviction is not per-write.
const double _lowWater = 0.8;

/// Parallel remote reads during [BlobCache.prefetch]. Shares the serialized
/// remote queue, so more would only queue.
const int _prefetchConcurrency = 3;

/// A hit re-stamps the file mtime at most this often. Every hit moving the
/// clock would turn scrolling a shelf of covers into a write storm.
const Duration _touchInterval = Duration(seconds: 60);

class BlobCache {
  BlobCache({
    required BlobStore store,
    required Future<Uint8List?> Function(BlobKey key) fetchRemote,
    required SpookyLogger logger,
    required int maxBytes,
    String namespace = 'anon',
    DateTime Function()? now,
  })  : _store = store,
        _fetchRemote = fetchRemote,
        _logger = logger.child('BlobCache'),
        _maxBytes = maxBytes,
        _namespace = namespace,
        _now = now ?? DateTime.now {
    _ready = _reconcile();
  }

  final BlobStore _store;
  final Future<Uint8List?> Function(BlobKey key) _fetchRemote;
  final SpookyLogger _logger;
  final DateTime Function() _now;
  int _maxBytes;
  String _namespace;

  final _manifest = <String, _Entry>{};
  final _inflight = <String, Future<Uint8List?>>{};

  /// Paths the remote said do not exist. Per namespace, per process; cleared
  /// by [invalidate] (a `put` may have created the file since).
  final _missing = <String>{};

  /// Resolves once the manifest has been rebuilt from disk; reads await it, so
  /// nothing refetches a file that is already there.
  late Future<void> _ready;

  int _hits = 0;
  int _misses = 0;
  int _evictedEntries = 0;
  int _evictedBytes = 0;

  String get namespace => _namespace;

  // ---- Reads -------------------------------------------------------------

  /// Resolve the bytes for [key], filling disk on the way when [persist] is on.
  /// Returns null when the file does not exist remotely and is not cached.
  Future<Uint8List?> read(BlobKey key,
      {bool reload = false, bool persist = true}) async {
    await _ready;
    final id = key.id;
    if (!reload) {
      if (_missing.contains(id)) return null;
      final running = _inflight[id];
      if (running != null) return running;
      final cached = await _readLocal(key, id);
      if (cached != null) {
        _hits++;
        return cached;
      }
      // The local read yielded; a sibling may have started the same miss.
      final started = _inflight[id];
      if (started != null) return started;
    }
    _misses++;
    // The whole miss (fetch AND write-back) is what siblings join, so a second
    // widget mounting mid-persist neither refetches nor reads a torn file.
    // Block body on purpose: `Map.remove` returns the stored future, and a
    // `whenComplete` callback that returns a future waits for it — for itself.
    final future = _miss(key, id, persist).whenComplete(() {
      _inflight.remove(id);
    });
    _inflight[id] = future;
    return future;
  }

  Future<Uint8List?> _miss(BlobKey key, String id, bool persist) async {
    final bytes = await _fetchRemote(key);
    if (bytes == null) {
      // Gone remotely: drop any stale copy and remember the miss.
      await _dropLocal(key, id);
      _missing.add(id);
      return null;
    }
    _missing.remove(id);
    if (persist) await _persist(key, id, bytes);
    return bytes;
  }

  Future<Uint8List?> _readLocal(BlobKey key, String id) async {
    Uint8List? bytes;
    try {
      bytes = await _store.read(_namespace, key);
    } on BlobKeyError {
      return null;
    } catch (err) {
      _logger.warn('blob read from local store failed ($id): $err');
      return null;
    }
    final entry = _manifest[id];
    if (bytes == null) {
      _manifest.remove(id);
      return null;
    }
    if (entry != null && entry.size != bytes.length) {
      // Half-written, or overwritten while we ran: refill from remote.
      await _dropLocal(key, id);
      return null;
    }
    final now = _now();
    if (entry == null) {
      // File on disk with no row: adopt it rather than re-download it.
      _manifest[id] = _Entry(key, bytes.length, now);
    } else if (now.difference(entry.lastAccess) >= _touchInterval) {
      entry.lastAccess = now;
      unawaited(_store.touch(_namespace, key, now));
    }
    return bytes;
  }

  // ---- Writes ------------------------------------------------------------

  Future<void> _persist(BlobKey key, String id, Uint8List bytes) async {
    try {
      final size = await _store.write(_namespace, key, bytes);
      _manifest[id] = _Entry(key, size, _now());
      await _enforceBudget();
    } on BlobKeyError {
      // Unstorable path: served from remote, never persisted.
    } catch (err) {
      _logger.warn('blob write failed ($id): $err');
    }
  }

  /// Forget one path everywhere. Called on `put`/`delete` of that path.
  Future<void> invalidate(BlobKey key) async {
    await _ready;
    _missing.remove(key.id);
    await _dropLocal(key, key.id);
  }

  Future<void> _dropLocal(BlobKey key, String id) async {
    _manifest.remove(id);
    try {
      await _store.remove(_namespace, key);
    } on BlobKeyError {
      // Nothing could have been stored under it.
    } catch (err) {
      _logger.warn('blob delete failed ($id): $err');
    }
  }

  int _totalBytes() => _manifest.values.fold(0, (sum, e) => sum + e.size);

  /// Bring total bytes under budget by dropping the least recently used
  /// entries. The decoded bitmaps a widget is showing live in Flutter's own
  /// cache, so evicting a file never blanks anything on screen.
  Future<void> _enforceBudget() async {
    var total = _totalBytes();
    if (total <= _maxBytes) return;
    final target = (_maxBytes * _lowWater).floor();
    final candidates = _manifest.values.toList()
      ..sort((a, b) => a.lastAccess.compareTo(b.lastAccess));
    var dropped = 0;
    for (final entry in candidates) {
      if (total <= target) break;
      await _dropLocal(entry.key, entry.key.id);
      total -= entry.size;
      _evictedEntries++;
      _evictedBytes += entry.size;
      dropped++;
    }
    if (dropped > 0) {
      _logger.info(
          'blob cache evicted $dropped least-recently-used entries ($total/$_maxBytes bytes)');
    }
  }

  // ---- Lifecycle ---------------------------------------------------------

  /// Rebuild the manifest from what is on disk. Disk wins on existence: files
  /// with no row get a row seeded from mtime, and torn writes are swept by the
  /// walk itself.
  Future<void> _reconcile() async {
    _manifest.clear();
    _missing.clear();
    List<BlobStat> stats;
    try {
      stats = await _store.list(_namespace);
    } catch (err) {
      _logger.warn('blob cache reconcile failed to list local store: $err');
      return;
    }
    for (final stat in stats) {
      _manifest[stat.key.id] = _Entry(stat.key, stat.size, stat.mtime);
    }
    await _enforceBudget();
  }

  /// Repoint at another user's files. The bytes of the old namespace stay on
  /// disk so signing back in is still warm. Cheap when unchanged, so callers
  /// resolve the namespace per read rather than tracking auth themselves.
  Future<void> setNamespace(String namespace) {
    if (namespace == _namespace) return _ready;
    final previous = _ready;
    _namespace = namespace;
    _inflight.clear();
    return _ready = previous.then((_) => _reconcile());
  }

  /// Delete every cached byte in [namespace] (default: the current one).
  Future<void> clear([String? namespace]) async {
    await _ready;
    final ns = namespace ?? _namespace;
    try {
      await _store.clear(ns);
    } catch (err) {
      _logger.warn('blob cache clear failed ($ns): $err');
    }
    if (ns == _namespace) {
      _manifest.clear();
      _missing.clear();
    }
  }

  /// Warm the cache. Already-cached paths are skipped.
  Future<void> prefetch(List<BlobKey> keys) async {
    await _ready;
    final queue = keys.where((k) => !_manifest.containsKey(k.id)).toList();
    var cursor = 0;
    Future<void> worker() async {
      while (cursor < queue.length) {
        final key = queue[cursor++];
        try {
          await read(key);
        } catch (err) {
          _logger.warn('blob prefetch failed (${key.id}): $err');
        }
      }
    }

    final lanes = queue.length < _prefetchConcurrency
        ? queue.length
        : _prefetchConcurrency;
    await Future.wait([for (var i = 0; i < lanes; i++) worker()]);
  }

  void setMaxBytes(int maxBytes) {
    _maxBytes = maxBytes;
  }

  BlobCacheStats get stats => BlobCacheStats(
        namespace: _namespace,
        entries: _manifest.length,
        totalBytes: _totalBytes(),
        budgetBytes: _maxBytes,
        evictedEntries: _evictedEntries,
        evictedBytes: _evictedBytes,
        hits: _hits,
        misses: _misses,
        persistent: _store.persistent,
      );
}
