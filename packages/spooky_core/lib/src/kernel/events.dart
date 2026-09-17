import '../ffi/stream_update.dart' show StreamUpdate;
import '../types.dart';

/// Inbound events. Everything that can start a saga arrives as one of these
/// through the runtime router: public API calls, adapter callbacks, timers, and
/// `dispatch` effects from other sagas.
sealed class RuntimeEvent {
  const RuntimeEvent();

  /// Stable discriminator, used by the router and by test assertions.
  String get type;
}

class EnsureRegistered extends RuntimeEvent {
  const EnsureRegistered({this.requireAuth = false, this.attempt = 0});
  final bool requireAuth;
  final int attempt;
  @override
  String get type => 'EnsureRegistered';
}

class RegisterRemote extends RuntimeEvent {
  const RegisterRemote(this.hash, {this.retry = false});
  final QueryHash hash;
  final bool retry;
  @override
  String get type => 'RegisterRemote';
}

class SyncOutcome extends RuntimeEvent {
  const SyncOutcome(this.ok, [this.error]);
  final bool ok;
  final Object? error;
  @override
  String get type => 'SyncOutcome';
}

class AckPrune extends RuntimeEvent {
  const AckPrune();
  @override
  String get type => 'AckPrune';
}

class RecoverLostView extends RuntimeEvent {
  const RecoverLostView(this.hash);
  final QueryHash hash;
  @override
  String get type => 'RecoverLostView';
}

class ReadDirtyMembership extends RuntimeEvent {
  const ReadDirtyMembership();
  @override
  String get type => 'ReadDirtyMembership';
}

class ReadMembership extends RuntimeEvent {
  const ReadMembership(this.hashes);
  final List<QueryHash> hashes;
  @override
  String get type => 'ReadMembership';
}

class FetchRows extends RuntimeEvent {
  const FetchRows();
  @override
  String get type => 'FetchRows';
}

class Materialize extends RuntimeEvent {
  const Materialize(this.hash);
  final QueryHash hash;
  @override
  String get type => 'Materialize';
}

class MaterializeDirty extends RuntimeEvent {
  const MaterializeDirty();
  @override
  String get type => 'MaterializeDirty';
}

class LifecycleTick extends RuntimeEvent {
  const LifecycleTick();
  @override
  String get type => 'LifecycleTick';
}

class GcTick extends RuntimeEvent {
  const GcTick();
  @override
  String get type => 'GcTick';
}

class Drain extends RuntimeEvent {
  const Drain();
  @override
  String get type => 'Drain';
}

class FlushWrite extends RuntimeEvent {
  const FlushWrite(this.key);
  final String key;
  @override
  String get type => 'FlushWrite';
}

class PollTick extends RuntimeEvent {
  const PollTick();
  @override
  String get type => 'PollTick';
}

class SelfHealTick extends RuntimeEvent {
  const SelfHealTick();
  @override
  String get type => 'SelfHealTick';
}

class HeartbeatNow extends RuntimeEvent {
  const HeartbeatNow();
  @override
  String get type => 'HeartbeatNow';
}

class StartRemote extends RuntimeEvent {
  const StartRemote();
  @override
  String get type => 'StartRemote';
}

class PrimeCircuit extends RuntimeEvent {
  const PrimeCircuit();
  @override
  String get type => 'PrimeCircuit';
}

class VersionsPrimed extends RuntimeEvent {
  const VersionsPrimed(this.entries);
  final List<(String, int)> entries;
  @override
  String get type => 'VersionsPrimed';
}

class AuthFlip extends RuntimeEvent {
  const AuthFlip(this.userId);
  final String? userId;
  @override
  String get type => 'AuthFlip';
}

class BucketSwitch extends RuntimeEvent {
  const BucketSwitch(this.target);
  final String target;
  @override
  String get type => 'BucketSwitch';
}

class AppDetached extends RuntimeEvent {
  const AppDetached();
  @override
  String get type => 'AppDetached';
}

class ConnectionChanged extends RuntimeEvent {
  const ConnectionChanged(this.state);
  final ConnectionState state;
  @override
  String get type => 'ConnectionChanged';
}

class StreamUpdateEvent extends RuntimeEvent {
  const StreamUpdateEvent(this.update);
  final StreamUpdate update;
  @override
  String get type => 'StreamUpdate';
}

class LiveStart extends RuntimeEvent {
  const LiveStart();
  @override
  String get type => 'LiveStart';
}

class LiveChange extends RuntimeEvent {
  const LiveChange(this.hashes, {this.rows});
  final List<QueryHash> hashes;
  final List<InlineRow>? rows;
  @override
  String get type => 'LiveChange';
}

/// Outbound events: everything the runtime hands to subscribers, the logger and
/// the host app.
sealed class OutEvent {
  const OutEvent();
  String get type;
}

class QueryRecordsEvent extends OutEvent {
  const QueryRecordsEvent(this.hash, this.records);
  final QueryHash hash;
  final List<Map<String, dynamic>> records;
  @override
  String get type => 'query:records';
}

class QueryStatusEvent extends OutEvent {
  const QueryStatusEvent(this.hash, this.status);
  final QueryHash hash;
  final QueryStatus status;
  @override
  String get type => 'query:status';
}

class QueryAuthorityEvent extends OutEvent {
  const QueryAuthorityEvent(this.hash, this.known);
  final QueryHash hash;
  final bool known;
  @override
  String get type => 'query:authority';
}

class QueryViewLostEvent extends OutEvent {
  const QueryViewLostEvent(this.hash);
  final QueryHash hash;
  @override
  String get type => 'query:view-lost';
}

class QueryEvictedEvent extends OutEvent {
  const QueryEvictedEvent(this.hash);
  final QueryHash hash;
  @override
  String get type => 'query:evicted';
}

/// One optimistic write, as the host app observes it. The outbox is what
/// actually drives the push; this is the observation hook.
class MutationEmittedEvent extends OutEvent {
  const MutationEmittedEvent(this.event);
  final MutationEvent event;
  @override
  String get type => 'mutation:event';
}

class MutationSettledEvent extends OutEvent {
  const MutationSettledEvent({
    required this.mutationId,
    required this.recordId,
    required this.eventType,
  });
  final String mutationId;
  final String recordId;
  final MutationEventType eventType;
  @override
  String get type => 'mutation:settled';
}

class MutationRolledBackEvent extends OutEvent {
  const MutationRolledBackEvent({
    required this.mutationId,
    required this.recordId,
    required this.eventType,
    required this.error,
  });
  final String mutationId;
  final String recordId;
  final MutationEventType eventType;
  final String error;
  @override
  String get type => 'mutation:rolled-back';
}

class TrayChangedEvent extends OutEvent {
  const TrayChangedEvent(this.count);
  final int count;
  @override
  String get type => 'tray:changed';
}

class HealthChangedEvent extends OutEvent {
  const HealthChangedEvent(this.health);
  final SyncHealth health;
  @override
  String get type => 'health:changed';
}

class ActivityChangedEvent extends OutEvent {
  const ActivityChangedEvent({required this.fetching, required this.pending});
  final int fetching;
  final int pending;
  @override
  String get type => 'activity:changed';
}

/// The set of record ids with unacknowledged local writes changed. Separate
/// from [ActivityChangedEvent] because the count can stay equal while the set
/// moves (one write acked as another is queued).
class UnsyncedChangedEvent extends OutEvent {
  const UnsyncedChangedEvent(this.recordIds);
  final Set<String> recordIds;
  @override
  String get type => 'unsynced:changed';
}

enum LogLevel { debug, info, warn, error }

class LogEvent extends OutEvent {
  const LogEvent(this.level, this.message, [this.data]);
  final LogLevel level;
  final String message;
  final Map<String, Object?>? data;
  @override
  String get type => 'log';
}
