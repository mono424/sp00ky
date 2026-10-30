# spooky_flutter

Flutter bindings for [`spooky_core`](../spooky_core): rendering bucket files,
and WebRTC for peer to peer channels.

## Bucket images

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

## Peer to peer channels

`flutterWebRtc` gives `-- @channel` tables their WebRTC connections, over
`flutter_webrtc` (same data channel and ICE shape as a browser tab, so web and
Flutter peers connect to each other):

```dart
Sp00kyConfig(
  ...,
  channels: ChannelsConfig(rtc: flutterWebRtc),
)

final ch = client.channel(client.query('call').where({'id': callId}), [peerId]);
```

See `/docs/client/channels` for the filter rules, `accept` and ephemeral fields.
