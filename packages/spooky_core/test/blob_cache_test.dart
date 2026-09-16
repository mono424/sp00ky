import 'dart:io';
import 'dart:typed_data';

import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:test/test.dart';

Uint8List _bytes(int n, [int fill = 7]) => Uint8List(n)..fillRange(0, n, fill);

/// A fake bucket: `files` is the remote, `calls` counts remote reads.
class _Remote {
  final files = <String, Uint8List>{};
  final calls = <String>[];
  Future<Uint8List?> fetch(BlobKey key) async {
    calls.add(key.id);
    return files[key.id];
  }
}

void main() {
  late Directory tmp;
  late _Remote remote;

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('spooky-blobs-');
    remote = _Remote();
  });
  tearDown(() => tmp.delete(recursive: true));

  BlobCache cache(
          {BlobStore? store, int maxBytes = 1 << 20, String ns = 'anon'}) =>
      BlobCache(
        store: store ?? FileBlobStore(tmp),
        fetchRemote: remote.fetch,
        logger: SpookyLogger.root('test'),
        maxBytes: maxBytes,
        namespace: ns,
      );

  group('BlobCache on disk', () {
    test('miss fetches remote and writes the file; hit reads disk only',
        () async {
      remote.files['covers/a_t.webp'] = _bytes(10);
      final c = cache();
      final key = const BlobKey('covers', 'a_t.webp');
      expect(await c.read(key), _bytes(10));
      expect(remote.calls, ['covers/a_t.webp']);
      expect(await c.read(key), _bytes(10));
      expect(remote.calls, hasLength(1), reason: 'second read is a hit');
      expect(c.stats.hits, 1);
      expect(c.stats.misses, 1);
      expect(File('${tmp.path}/anon/covers/a_t%2Ewebp').existsSync(), isTrue);
    });

    test('a new cache over the same directory serves from disk', () async {
      remote.files['covers/a_t.webp'] = _bytes(10);
      await cache().read(const BlobKey('covers', 'a_t.webp'));
      remote.calls.clear();
      final warm = cache();
      expect(await warm.read(const BlobKey('covers', 'a_t.webp')), _bytes(10));
      expect(remote.calls, isEmpty);
      expect(warm.stats.entries, 1);
      expect(warm.stats.totalBytes, 10);
    });

    test('nested paths round-trip through the encoded layout', () async {
      remote.files['b/dir/sub/f.png'] = _bytes(3);
      final key = const BlobKey('b', 'dir/sub/f.png');
      await cache().read(key);
      final warm = cache();
      await warm.read(key);
      expect(remote.calls, hasLength(1));
      expect(warm.stats.entries, 1);
    });

    test('torn .part- files are swept at start', () async {
      final dir = Directory('${tmp.path}/anon/covers')
        ..createSync(recursive: true);
      final torn = File('${dir.path}/x%2Ewebp.part-abc-0')
        ..writeAsBytesSync(_bytes(4));
      final c = cache();
      await c.read(const BlobKey('covers', 'nothing'));
      expect(torn.existsSync(), isFalse);
      expect(c.stats.entries, 0);
    });

    test('a size mismatch against the manifest refetches', () async {
      remote.files['covers/a'] = _bytes(10);
      final c = cache();
      final key = const BlobKey('covers', 'a');
      await c.read(key);
      // Another writer truncates the file behind our back.
      File('${tmp.path}/anon/covers/a').writeAsBytesSync(_bytes(3));
      expect(await c.read(key), _bytes(10));
      expect(remote.calls, hasLength(2));
    });

    test('LRU eviction keeps the cache under budget', () async {
      for (var i = 0; i < 5; i++) {
        remote.files['covers/$i'] = _bytes(100);
      }
      var tick = 0;
      final c = BlobCache(
        store: FileBlobStore(tmp),
        fetchRemote: remote.fetch,
        logger: SpookyLogger.root('test'),
        maxBytes: 350,
        now: () => DateTime.fromMillisecondsSinceEpoch(1000 * ++tick),
      );
      for (var i = 0; i < 5; i++) {
        await c.read(BlobKey('covers', '$i'));
      }
      expect(c.stats.totalBytes, lessThanOrEqualTo(350));
      expect(c.stats.evictedEntries, greaterThan(0));
      // The most recent entry survives, the oldest is gone.
      expect(File('${tmp.path}/anon/covers/4').existsSync(), isTrue);
      expect(File('${tmp.path}/anon/covers/0').existsSync(), isFalse);
    });

    test('a missing remote file is remembered until invalidated', () async {
      final c = cache();
      final key = const BlobKey('covers', 'gone');
      expect(await c.read(key), isNull);
      expect(await c.read(key), isNull);
      expect(remote.calls, hasLength(1));
      remote.files['covers/gone'] = _bytes(1);
      await c.invalidate(key);
      expect(await c.read(key), _bytes(1));
    });

    test('concurrent reads of one key issue one remote read', () async {
      remote.files['covers/a'] = _bytes(1);
      final c = cache();
      await Future.wait([
        c.read(const BlobKey('covers', 'a')),
        c.read(const BlobKey('covers', 'a')),
        c.read(const BlobKey('covers', 'a')),
      ]);
      expect(remote.calls, hasLength(1));
    });

    test('namespaces are isolated and clear() wipes one of them', () async {
      remote.files['covers/a'] = _bytes(2);
      final c = cache();
      await c.read(const BlobKey('covers', 'a'));
      await c.setNamespace('user1');
      expect(c.stats.entries, 0);
      await c.read(const BlobKey('covers', 'a'));
      expect(remote.calls, hasLength(2));
      await c.clear('anon');
      expect(Directory('${tmp.path}/anon').existsSync(), isFalse);
      expect(Directory('${tmp.path}/user1').existsSync(), isTrue);
      await c.setNamespace('anon');
      await c.read(const BlobKey('covers', 'a'));
      expect(remote.calls, hasLength(3));
    });

    test('reload bypasses the cache and refills it', () async {
      remote.files['covers/a'] = _bytes(1);
      final c = cache();
      await c.read(const BlobKey('covers', 'a'));
      remote.files['covers/a'] = _bytes(9);
      expect(
          await c.read(const BlobKey('covers', 'a'), reload: true), _bytes(9));
      expect(await c.read(const BlobKey('covers', 'a')), _bytes(9));
      expect(remote.calls, hasLength(2));
    });

    test('a memory store dedupes within the process only', () async {
      remote.files['covers/a'] = _bytes(1);
      final store = MemoryBlobStore();
      final c = cache(store: store);
      await c.read(const BlobKey('covers', 'a'));
      await c.read(const BlobKey('covers', 'a'));
      expect(remote.calls, hasLength(1));
      expect(c.stats.persistent, isFalse);
    });
  });

  group('BucketHandle', () {
    test('read() goes through the cache and get() does not', () async {
      final calls = <String>[];
      Future<List<dynamic>> query(String sql,
          [Map<String, dynamic>? vars]) async {
        calls.add(sql);
        return [
          Uint8List.fromList([1, 2, 3])
        ];
      }

      final c = BlobCache(
        store: FileBlobStore(tmp),
        fetchRemote: (key) async {
          calls.add('remote:${key.id}');
          return Uint8List.fromList([1, 2, 3]);
        },
        logger: SpookyLogger.root('test'),
        maxBytes: 1 << 20,
      );
      final handle = BucketHandle.withQuery('covers', query,
          blobs: c, namespace: () => 'u1');
      expect(await handle.read('a.webp'), [1, 2, 3]);
      expect(await handle.read('a.webp'), [1, 2, 3]);
      expect(calls, ['remote:covers/a.webp']);
      expect(c.namespace, 'u1');
      await handle.get('a.webp');
      expect(calls.last, 'RETURN f"covers:/a.webp".get();');
    });

    test('put and delete invalidate the cached copy', () async {
      final store = MemoryBlobStore();
      final c = BlobCache(
        store: store,
        fetchRemote: (key) async => Uint8List.fromList([1]),
        logger: SpookyLogger.root('test'),
        maxBytes: 1 << 20,
      );
      Future<List<dynamic>> query(String sql,
              [Map<String, dynamic>? vars]) async =>
          [];
      final handle = BucketHandle.withQuery('covers', query, blobs: c);
      await handle.read('a');
      expect(c.stats.entries, 1);
      await handle.put('a', 'x');
      expect(c.stats.entries, 0);
      await handle.read('a');
      await handle.delete('a');
      expect(c.stats.entries, 0);
    });

    test('rejects paths the query lexer would mangle', () async {
      Future<List<dynamic>> query(String sql,
              [Map<String, dynamic>? vars]) async =>
          [];
      final handle = BucketHandle.withQuery('covers', query);
      expect(() => handle.read('a:b'), throwsArgumentError);
      expect(() => handle.get('has space'), throwsArgumentError);
      expect(() => handle.put('x"y', 'z'), throwsArgumentError);
    });

    test('bucketContentToBytes coerces every wire shape', () {
      expect(bucketContentToBytes(null), isNull);
      expect(bucketContentToBytes(Uint8List.fromList([1])), [1]);
      expect(bucketContentToBytes(<int>[1, 2]), [1, 2]);
      expect(bucketContentToBytes('hi'), [104, 105]);
      expect(bucketContentToBytes(42), isNull);
    });
  });
}
