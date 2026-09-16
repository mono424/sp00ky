import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'dart:ui' as ui;

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_flutter/spooky_flutter.dart';

/// A 2x2 opaque PNG, encoded by the engine so the bytes are exactly what the
/// decoder expects on this platform.
Future<Uint8List> _png() async {
  final recorder = ui.PictureRecorder();
  ui.Canvas(recorder).drawRect(const ui.Rect.fromLTWH(0, 0, 2, 2),
      ui.Paint()..color = const ui.Color(0xFF336699));
  final image = await recorder.endRecording().toImage(2, 2);
  final data = await image.toByteData(format: ui.ImageByteFormat.png);
  return data!.buffer.asUint8List();
}

Future<List<dynamic>> _noQuery(String sql,
        [Map<String, dynamic>? vars]) async =>
    [];

void main() {
  late Directory tmp;
  late Uint8List png;
  final remoteCalls = <String>[];

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('spooky-flutter-');
    remoteCalls.clear();
    // Decoded bitmaps are keyed by bucket name + path, so they would leak
    // across tests that reuse the same path.
    imageCache.clear();
  });
  tearDown(() => tmp.delete(recursive: true));

  BucketHandle handle({bool missing = false}) {
    final cache = BlobCache(
      store: FileBlobStore(tmp),
      fetchRemote: (key) async {
        remoteCalls.add(key.id);
        return missing ? null : png;
      },
      logger: SpookyLogger.root('test'),
      maxBytes: 1 << 20,
    );
    return BucketHandle.withQuery('covers', _noQuery, blobs: cache);
  }

  Widget app(BucketHandle bucket, {String? path = 'a.png', Widget? fallback}) =>
      Directionality(
        textDirection: TextDirection.ltr,
        child: Center(
          child: SizedBox(
            width: 20,
            height: 20,
            child: BucketImage(bucket: bucket, path: path, fallback: fallback),
          ),
        ),
      );

  /// Real file and decode I/O only progress outside the fake-async zone, so
  /// every test body runs inside [WidgetTester.runAsync], and this waits for
  /// the image stream to notify and the fade to finish.
  Future<void> settle(WidgetTester tester) async {
    for (var i = 0; i < 10; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 30));
      await tester.pump();
    }
    await tester.pump(const Duration(milliseconds: 300));
  }

  bool painted(WidgetTester tester) =>
      tester.widget<RawImage>(find.byType(RawImage)).image != null;

  testWidgets(
      'first mount fetches once; the same path never refetches',
      (tester) => tester.runAsync(() async {
            png = await _png();
            final bucket = handle();
            await tester.pumpWidget(app(bucket));
            await settle(tester);
            expect(painted(tester), isTrue);
            expect(remoteCalls, ['covers/a.png']);

            await tester.pumpWidget(const SizedBox());
            await tester.pumpWidget(app(bucket));
            await settle(tester);
            expect(painted(tester), isTrue);
            expect(remoteCalls, hasLength(1));
          }));

  testWidgets(
      'a fresh handle over the same directory paints from disk',
      (tester) => tester.runAsync(() async {
            png = await _png();
            await tester.pumpWidget(app(handle()));
            await settle(tester);
            expect(remoteCalls, hasLength(1));

            imageCache.clear();
            await tester.pumpWidget(const SizedBox());
            await tester.pumpWidget(app(handle()));
            await settle(tester);
            expect(painted(tester), isTrue);
            expect(remoteCalls, hasLength(1),
                reason: 'served from the cache dir');
          }));

  testWidgets(
      'a missing file shows the fallback',
      (tester) => tester.runAsync(() async {
            png = await _png();
            await tester.pumpWidget(
                app(handle(missing: true), fallback: const Text('fallback')));
            await settle(tester);
            expect(find.text('fallback'), findsOneWidget);
            expect(find.byType(RawImage), findsNothing);
          }));

  testWidgets(
      'a slow load shows the fallback, then the image',
      (tester) => tester.runAsync(() async {
            png = await _png();
            final gate = Completer<void>();
            final cache = BlobCache(
              store: FileBlobStore(tmp),
              fetchRemote: (key) async {
                await gate.future;
                remoteCalls.add(key.id);
                return png;
              },
              logger: SpookyLogger.root('test'),
              maxBytes: 1 << 20,
            );
            final bucket =
                BucketHandle.withQuery('covers', _noQuery, blobs: cache);
            await tester
                .pumpWidget(app(bucket, fallback: const Text('fallback')));
            await tester.pump();
            expect(find.text('fallback'), findsNothing,
                reason: 'inside the instant window nothing flashes');
            await Future<void>.delayed(const Duration(milliseconds: 200));
            await tester.pump();
            expect(find.text('fallback'), findsOneWidget);
            gate.complete();
            await settle(tester);
            expect(painted(tester), isTrue);
            expect(find.text('fallback'), findsOneWidget,
                reason: 'the fallback stays under the image');
          }));

  testWidgets(
      'an image that lands inside the window never shows the fallback',
      (tester) => tester.runAsync(() async {
            png = await _png();
            await tester.pumpWidget(app(handle()));
            await settle(tester); // warm the disk
            imageCache.clear();
            await tester.pumpWidget(const SizedBox());
            await tester
                .pumpWidget(app(handle(), fallback: const Text('fallback')));
            for (var i = 0; i < 4; i++) {
              await Future<void>.delayed(const Duration(milliseconds: 20));
              await tester.pump();
            }
            expect(painted(tester), isTrue);
            expect(find.text('fallback'), findsNothing);
          }));

  testWidgets('a null path paints only the fallback and never fetches',
      (tester) async {
    await tester.pumpWidget(
        app(handle(), path: null, fallback: const Text('fallback')));
    await tester.pump();
    expect(find.text('fallback'), findsOneWidget);
    expect(remoteCalls, isEmpty);
  });
}
