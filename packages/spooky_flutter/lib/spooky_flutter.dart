/// Flutter bindings for `spooky_core`.
///
/// Today: rendering bucket files. [BucketImage] paints an image out of a
/// SurrealDB bucket through the client's blob cache, so after the first load
/// it comes off disk (and, once decoded, out of Flutter's own image cache)
/// instead of the serialized remote queue.
library;

export 'src/bucket_image.dart'
    show BucketImage, BucketImageProvider, BucketFileMissing;
