# spooky_flutter

Flutter bindings for [`spooky_core`](../spooky_core). Today that is rendering
bucket files:

```dart
final covers = db.client.bucket('puzzle_covers');

BucketImage(
  bucket: covers,
  path: '${coverKey}_t.webp',
  fallback: const CoverFallback(),
  fit: BoxFit.cover,
)
```

`BucketImage` paints through `BucketImageProvider`, which reads
`bucket.read(path)`: the client's blob cache serves the bytes off disk after the
first load, and Flutter's own `ImageCache` keeps the decoded bitmap, so a shelf
scrolled back into view paints on its first frame. Point the cache at a
directory in `Sp00kyConfig.blobCache` or it stays in memory:

```dart
Sp00kyConfig(
  ...,
  blobCache: BlobCacheConfig(
    directory: '${(await getApplicationSupportDirectory()).path}/sp00ky-blobs',
  ),
)
```
