/// Pure-Dart core for Spooky local-first sync (framework-agnostic).
///
/// Mirrors `@spooky-sync/core`. Query subscriptions are exposed as Dart
/// `Stream`s so a Flutter app can consume them with `StreamBuilder`.
library;

export 'src/sp00ky_client.dart';
export 'src/types.dart';
export 'src/surreal/value.dart'
    show RecordId, SurrealDuration, generateId, generateNewTableId;
export 'src/events/event_system.dart'
    show EventSystem, SpookyEvent, PushEventOptions, EventSubscriptionOptions;
export 'src/ffi/stream_update.dart'
    show
        StreamUpdate,
        ViewDelta,
        DeltaRecord,
        RecordVersion,
        RecordVersionArray,
        SspException;
export 'src/kernel/events.dart'
    show RuntimeEvent, OutEvent, MutationEmittedEvent, LogLevel;
export 'src/mutation/rows.dart'
    show FailedMutationRow, FailedMutationError, FailedErrorKind, RevertKind;
export 'src/boot/preload_saga.dart' show PreloadFailedError;
export 'src/modules/query_host.dart' show QueryHost;
export 'src/state/client_state.dart' show ClientState, QueryEntry, TabRole;
export 'src/state/lifecycle.dart' show QueryPhase, RemotePhase;
export 'src/utils/duration_utils.dart'
    show QueryTimeToLive, defaultTtl, parseDuration;
export 'src/utils/parser.dart' show ColumnSchema;
export 'src/utils/semver.dart' show semverGt;
export 'src/surreal/remote_client.dart'
    show RemoteSurrealClient, WebSocketSurrealClient, LiveMessage;
export 'src/modules/auth/auth_service.dart' show AuthService, AuthEventTypes;
export 'src/modules/bucket.dart' show BucketHandle;
export 'src/modules/feature_flag/feature_flag.dart'
    show FeatureFlagModule, FeatureFlagHandle, FeatureFlagSnapshot;
export 'src/modules/app_release/app_release.dart'
    show AppReleaseModule, AppReleaseHandle, AppReleaseSnapshot;
export 'src/modules/query_builder.dart'
    show QueryBuilder, QueryOp, RelationPlan;
export 'src/modules/relationships.dart' show SchemaRelationship;
