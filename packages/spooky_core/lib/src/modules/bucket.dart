import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import '../services/blobs/blob_cache.dart';
import '../services/database/remote_database_service.dart';

/// Coerce whatever the `.get()` RPC hands back into bytes (TS
/// `bucketContentToBlob`). Text files arrive as a string; binary as a CBOR
/// byte string, which the codec already decodes to [Uint8List].
Uint8List? bucketContentToBytes(dynamic content) {
  if (content == null) return null;
  if (content is Uint8List) return content;
  if (content is List<int>) return Uint8List.fromList(content);
  if (content is List) return Uint8List.fromList(content.cast<int>());
  if (content is String) return Uint8List.fromList(utf8.encode(content));
  return null;
}

/// Bucket keys are restricted to this charset BY THE QUERY LEXER: the path is
/// interpolated into `f"<bucket>:/<path>"`, which is syntax and cannot take a
/// bound parameter. A space is a parse error and a `:` silently TRUNCATES the
/// key, so an unvalidated path is a broken query at best and the wrong object
/// at worst.
final RegExp _safePath = RegExp(r'^[A-Za-z0-9_\-./]+$');

/// Blurhash sidecar written by the TS client's `bucket.put()` next to an
/// image (TS `blurhashSidecarPath`).
String blurhashSidecarPath(String path) => '$path.bh';

/// A handle to a SurrealDB storage bucket (TS `BucketHandle`). Operations are
/// issued as `f"<bucket>:/<path>"....` file expressions over the remote.
///
/// [read] goes through the local blob cache when the client has one, so a warm
/// file never touches the serialized remote queue. [get] always does.
///
/// Return-value parsing is tolerant (null-safe) so a backend that returns no
/// body for an op doesn't throw.
class BucketHandle {
  BucketHandle(this._bucketName, RemoteDatabaseService remote,
      {BlobCache? blobs, FutureOr<String> Function()? namespace})
      : _query = remote.query,
        _blobs = blobs,
        _namespace = namespace;
  BucketHandle.withQuery(this._bucketName, this._query,
      {BlobCache? blobs, FutureOr<String> Function()? namespace})
      : _blobs = blobs,
        _namespace = namespace;

  final String _bucketName;
  final Future<List<dynamic>> Function(String, [Map<String, dynamic>?]) _query;
  final BlobCache? _blobs;

  /// The cache namespace for the signed-in user, resolved per call so the
  /// handle follows an auth flip without subscribing to it. May wait: a read
  /// issued before the client has restored its session must not land in the
  /// anonymous namespace.
  final FutureOr<String> Function()? _namespace;

  String get name => _bucketName;

  String _ref(String path) {
    if (!_safePath.hasMatch(path)) {
      throw ArgumentError.value(
          path, 'path', 'bucket paths may only contain [A-Za-z0-9_-./]');
    }
    return 'f"$_bucketName:/$path"';
  }

  BlobKey _key(String path) => BlobKey(_bucketName, path);

  /// The cache, pointed at the current user's namespace.
  Future<BlobCache>? _cache() {
    final blobs = _blobs;
    if (blobs == null) return null;
    final resolve = _namespace;
    if (resolve == null) return Future.value(blobs);
    return Future.sync(resolve).then(blobs.setNamespace).then((_) => blobs);
  }

  Future<void> put(String path, Object content) async {
    final ref = _ref(path);
    await _query('RETURN $ref.put(\$content);', {'content': content});
    // A path can be overwritten, so anything cached under it is now wrong.
    await (await _cache())?.invalidate(_key(path));
  }

  /// Raw remote read, uncached. Prefer [read].
  Future<dynamic> get(String path) async {
    final result = await _query('RETURN ${_ref(path)}.get();');
    return result.isNotEmpty ? result.first : null;
  }

  /// Read through the local blob cache: disk first, the bucket second. Returns
  /// null when the file exists in neither place. [reload] skips the cache and
  /// refills it from remote.
  Future<Uint8List?> read(String path, {bool reload = false}) async {
    _ref(path);
    final cache = await _cache();
    if (cache == null) return bucketContentToBytes(await get(path));
    return cache.read(_key(path), reload: reload);
  }

  /// The blurhash stored alongside an uploaded image, or null when there is
  /// none. Reads through the cache, so a missing sidecar costs one remote read
  /// per process, not one per mount.
  Future<String?> blurhash(String path) async {
    final bytes = await read(blurhashSidecarPath(path));
    if (bytes == null) return null;
    final hash = utf8.decode(bytes, allowMalformed: true).trim();
    return hash.length >= 6 ? hash : null;
  }

  /// Drop [path] from the local cache without touching the remote file.
  Future<void> evict(String path) async {
    await (await _cache())?.invalidate(_key(path));
  }

  /// Warm the cache. Already-cached paths are skipped.
  Future<void> prefetch(List<String> paths) async {
    for (final p in paths) {
      _ref(p);
    }
    await (await _cache())?.prefetch([for (final p in paths) _key(p)]);
  }

  Future<void> delete(String path) async {
    await _query('RETURN ${_ref(path)}.delete();');
    final cache = await _cache();
    await cache?.invalidate(_key(path));
    await cache?.invalidate(_key(blurhashSidecarPath(path)));
  }

  Future<bool> exists(String path) async {
    final result = await _query('RETURN ${_ref(path)}.exists();');
    return result.isNotEmpty && result.first == true;
  }

  Future<Map<String, dynamic>> head(String path) async {
    final result = await _query('RETURN ${_ref(path)}.head();');
    final first = result.isNotEmpty ? result.first : null;
    return (first as Map?)?.cast<String, dynamic>() ?? {};
  }

  Future<void> copy(String sourcePath, String targetPath) async {
    _ref(targetPath);
    await _query(
        'RETURN ${_ref(sourcePath)}.copy(\$target);', {'target': targetPath});
  }

  Future<void> rename(String sourcePath, String targetPath) async {
    _ref(targetPath);
    await _query(
        'RETURN ${_ref(sourcePath)}.rename(\$target);', {'target': targetPath});
    await (await _cache())?.invalidate(_key(sourcePath));
  }

  Future<List<String>> list([String? prefix]) async {
    final p = prefix ?? '';
    final ref = p.isEmpty ? 'f"$_bucketName:/"' : _ref(p);
    final result = await _query('RETURN $ref.list();');
    final first = result.isNotEmpty ? result.first : null;
    return (first as List?)?.cast<String>() ?? [];
  }
}
