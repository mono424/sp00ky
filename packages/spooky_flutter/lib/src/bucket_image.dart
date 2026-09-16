import 'dart:ui' as ui;

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:spooky_core/spooky_core.dart';

/// Thrown by [BucketImageProvider] when the bucket has no file at the path.
/// [BucketImage] turns it into the fallback; a bare `Image` sees it through
/// `errorBuilder`.
class BucketFileMissing implements Exception {
  const BucketFileMissing(this.bucket, this.path);
  final String bucket;
  final String path;
  @override
  String toString() => 'BucketFileMissing: $bucket:/$path';
}

/// An [ImageProvider] over `bucket.read(path)`.
///
/// Reads go through the client's blob cache, so the bytes come off disk after
/// the first load. Equality is by bucket name and path, which is what lets
/// Flutter's [ImageCache] hand a remounting widget the decoded bitmap
/// synchronously: a shelf scrolled back into view paints on its first frame.
class BucketImageProvider extends ImageProvider<BucketImageProvider> {
  const BucketImageProvider(this.bucket, this.path, {this.scale = 1.0});

  final BucketHandle bucket;
  final String path;
  final double scale;

  @override
  Future<BucketImageProvider> obtainKey(ImageConfiguration configuration) =>
      SynchronousFuture<BucketImageProvider>(this);

  @override
  ImageStreamCompleter loadImage(
          BucketImageProvider key, ImageDecoderCallback decode) =>
      MultiFrameImageStreamCompleter(
        codec: _load(key, decode),
        scale: key.scale,
        debugLabel: '${bucket.name}:/$path',
        informationCollector: () => [
          DiagnosticsProperty<ImageProvider>('Image provider', this),
          DiagnosticsProperty<BucketImageProvider>('Image key', key),
        ],
      );

  Future<ui.Codec> _load(
      BucketImageProvider key, ImageDecoderCallback decode) async {
    final bytes = await bucket.read(path);
    if (bytes == null) {
      // Don't leave a failed completer in the cache: the file may be uploaded
      // later, and the next mount should ask again (the blob cache remembers
      // the miss itself, cheaply).
      PaintingBinding.instance.imageCache.evict(key);
      throw BucketFileMissing(bucket.name, path);
    }
    return decode(await ui.ImmutableBuffer.fromUint8List(bytes));
  }

  @override
  bool operator ==(Object other) =>
      other is BucketImageProvider &&
      other.bucket.name == bucket.name &&
      other.path == path &&
      other.scale == scale;

  @override
  int get hashCode => Object.hash(bucket.name, path, scale);

  @override
  String toString() =>
      '${objectRuntimeType(this, 'BucketImageProvider')}("${bucket.name}:/$path", scale: $scale)';
}

/// An image out of a bucket, with a fallback that stays painted underneath.
///
/// The fallback is not swapped out for the image: it stays under it so a
/// transparent cover never shows the page through, and so a late download
/// fades in over the placeholder rather than popping. A bitmap Flutter already
/// has decoded paints synchronously with no fade at all.
class BucketImage extends StatelessWidget {
  const BucketImage({
    super.key,
    required this.bucket,
    required this.path,
    this.fallback,
    this.fit = BoxFit.cover,
    this.alignment = Alignment.center,
    this.width,
    this.height,
    this.fadeDuration = const Duration(milliseconds: 200),
    this.semanticLabel,
  });

  final BucketHandle bucket;

  /// Null or empty paints only the fallback and never touches the bucket.
  final String? path;

  /// Painted while loading, under the image once it arrives, and alone when
  /// the file does not exist.
  final Widget? fallback;
  final BoxFit fit;
  final AlignmentGeometry alignment;
  final double? width;
  final double? height;

  /// Fade of a freshly decoded image over the fallback. Zero disables it.
  final Duration fadeDuration;
  final String? semanticLabel;

  @override
  Widget build(BuildContext context) {
    final path = this.path;
    final placeholder = fallback ?? const SizedBox.shrink();
    if (path == null || path.isEmpty) return placeholder;
    return Image(
      image: BucketImageProvider(bucket, path),
      fit: fit,
      alignment: alignment,
      width: width,
      height: height,
      gaplessPlayback: true,
      semanticLabel: semanticLabel,
      excludeFromSemantics: semanticLabel == null,
      errorBuilder: (_, __, ___) => placeholder,
      frameBuilder: (context, child, frame, wasSynchronouslyLoaded) {
        if (wasSynchronouslyLoaded || fadeDuration == Duration.zero) {
          return child;
        }
        return Stack(
          fit: StackFit.passthrough,
          children: [
            if (fallback != null) fallback!,
            AnimatedOpacity(
              opacity: frame == null ? 0 : 1,
              duration: fadeDuration,
              curve: Curves.easeOut,
              child: child,
            ),
          ],
        );
      },
    );
  }
}
