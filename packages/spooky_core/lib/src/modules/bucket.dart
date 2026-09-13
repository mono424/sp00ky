import '../services/database/remote_database_service.dart';

/// A handle to a SurrealDB storage bucket (TS `BucketHandle`). Operations are
/// issued as `f"<bucket>:/<path>"....` file expressions over the remote.
///
/// Return-value parsing is tolerant (null-safe) so a backend that returns no
/// body for an op doesn't throw.
class BucketHandle {
  BucketHandle(this._bucketName, RemoteDatabaseService remote)
      : _query = remote.query;
  BucketHandle.withQuery(this._bucketName, this._query);

  final String _bucketName;
  final Future<List<dynamic>> Function(String, [Map<String, dynamic>?]) _query;

  String _ref(String path) => 'f"$_bucketName:/$path"';

  Future<void> put(String path, Object content) async {
    await _query('RETURN ${_ref(path)}.put(\$content);', {'content': content});
  }

  Future<dynamic> get(String path) async {
    final result = await _query('RETURN ${_ref(path)}.get();');
    return result.isNotEmpty ? result.first : null;
  }

  Future<void> delete(String path) async {
    await _query('RETURN ${_ref(path)}.delete();');
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
    await _query(
        'RETURN ${_ref(sourcePath)}.copy(\$target);', {'target': targetPath});
  }

  Future<void> rename(String sourcePath, String targetPath) async {
    await _query(
        'RETURN ${_ref(sourcePath)}.rename(\$target);', {'target': targetPath});
  }

  Future<List<String>> list([String? prefix]) async {
    final p = prefix ?? '';
    final result = await _query('RETURN ${_ref(p)}.list();');
    final first = result.isNotEmpty ? result.first : null;
    return (first as List?)?.cast<String>() ?? [];
  }
}
