/// Flutter bindings for `spooky_core`.
///
/// [BucketImage] paints an image out of a SurrealDB bucket through the
/// client's blob cache, so after the first load it comes off disk (and, once
/// decoded, out of Flutter's own image cache) instead of the serialized remote
/// queue.
///
/// [flutterWebRtc] gives peer to peer channels (`-- @channel` tables) their
/// WebRTC connections: `Sp00kyConfig(channels: ChannelsConfig(rtc:
/// flutterWebRtc))`.
library;

export 'src/bucket_image.dart'
    show BucketImage, BucketImageProvider, BucketFileMissing;
export 'src/webrtc.dart' show flutterWebRtc;
