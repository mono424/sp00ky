import '../types.dart' show QueryStatus;

/// The query lifecycle as one explicit machine.
///
/// [phase] answers "where do this query's rows come from":
///
/// - `cold`      never resolved on this device: rows come from the SSP's local
///               window, bindings show a loader.
/// - `cached`    a durable `_00_view` row was found: rows come from that id-set.
/// - `live`      a server membership set was accepted this session.
/// - `viewLost`  the server's `_00_query` row vanished while we held
///               membership; rows are kept, a re-registration is under way.
///
/// [remote] tracks the server-side registration, [fetchDepth] is the refcount
/// behind `status: fetching`, and [notified] says whether subscribers received
/// at least one materialization this registration.
enum QueryPhase { cold, cached, live, viewLost }

enum RemotePhase { unregistered, registering, registered, failed }

class QueryLifecycle {
  const QueryLifecycle({
    required this.phase,
    required this.remote,
    required this.fetchDepth,
    required this.notified,
  });

  final QueryPhase phase;
  final RemotePhase remote;
  final int fetchDepth;
  final bool notified;

  QueryLifecycle copyWith({
    QueryPhase? phase,
    RemotePhase? remote,
    int? fetchDepth,
    bool? notified,
  }) =>
      QueryLifecycle(
        phase: phase ?? this.phase,
        remote: remote ?? this.remote,
        fetchDepth: fetchDepth ?? this.fetchDepth,
        notified: notified ?? this.notified,
      );

  @override
  String toString() =>
      'QueryLifecycle(${phase.name}/${remote.name}, fetchDepth: $fetchDepth, notified: $notified)';
}

sealed class LifecycleEvent {
  const LifecycleEvent();
  String get type;
}

class SeedEvent extends LifecycleEvent {
  const SeedEvent(this.resolvedBefore);
  final bool resolvedBefore;
  @override
  String get type => 'seed';
}

class MembershipAppliedEvent extends LifecycleEvent {
  const MembershipAppliedEvent(this.present);
  final bool present;
  @override
  String get type => 'membership-applied';
}

class RowMissingEvent extends LifecycleEvent {
  const RowMissingEvent();
  @override
  String get type => 'row-missing';
}

class RemoteRegisteringEvent extends LifecycleEvent {
  const RemoteRegisteringEvent();
  @override
  String get type => 'remote-registering';
}

class RemoteRegisteredEvent extends LifecycleEvent {
  const RemoteRegisteredEvent();
  @override
  String get type => 'remote-registered';
}

class RemoteFailedEvent extends LifecycleEvent {
  const RemoteFailedEvent();
  @override
  String get type => 'remote-failed';
}

class RemoteDroppedEvent extends LifecycleEvent {
  const RemoteDroppedEvent();
  @override
  String get type => 'remote-dropped';
}

class FetchBeginEvent extends LifecycleEvent {
  const FetchBeginEvent();
  @override
  String get type => 'fetch-begin';
}

class FetchEndEvent extends LifecycleEvent {
  const FetchEndEvent();
  @override
  String get type => 'fetch-end';
}

class NotifiedEvent extends LifecycleEvent {
  const NotifiedEvent();
  @override
  String get type => 'notified';
}

class BucketSwitchEvent extends LifecycleEvent {
  const BucketSwitchEvent(this.resolvedBefore);
  final bool resolvedBefore;
  @override
  String get type => 'bucket-switch';
}

class LifecycleError extends Error {
  LifecycleError(this.lifecycle, this.event);
  final QueryLifecycle lifecycle;
  final LifecycleEvent event;

  @override
  String toString() =>
      'LifecycleError: impossible lifecycle transition: '
      '${lifecycle.phase.name}/${lifecycle.remote.name} + ${event.type}';
}

QueryLifecycle seedLifecycle(bool resolvedBefore) => QueryLifecycle(
      phase: resolvedBefore ? QueryPhase.cached : QueryPhase.cold,
      remote: RemotePhase.unregistered,
      fetchDepth: 0,
      notified: false,
    );

/// Total on every (lifecycle, event) pair the sagas can produce; throws on the
/// rest.
QueryLifecycle transition(QueryLifecycle l, LifecycleEvent ev) {
  switch (ev) {
    case SeedEvent(:final resolvedBefore):
      return seedLifecycle(resolvedBefore);
    case MembershipAppliedEvent(:final present):
      if (l.phase == QueryPhase.viewLost && !present) {
        throw LifecycleError(l, ev);
      }
      return l.copyWith(phase: QueryPhase.live);
    case RowMissingEvent():
      // A cold query holds nothing the server could have lost: the outcome is
      // `ignored` upstream and the phase does not move.
      return l.phase == QueryPhase.cold
          ? l
          : l.copyWith(phase: QueryPhase.viewLost);
    case RemoteRegisteringEvent():
      return l.copyWith(remote: RemotePhase.registering);
    case RemoteRegisteredEvent():
      return l.copyWith(remote: RemotePhase.registered);
    case RemoteFailedEvent():
      return l.copyWith(remote: RemotePhase.failed);
    case RemoteDroppedEvent():
      return l.copyWith(remote: RemotePhase.unregistered, notified: false);
    case FetchBeginEvent():
      return l.copyWith(fetchDepth: l.fetchDepth + 1);
    case FetchEndEvent():
      return l.fetchDepth == 0 ? l : l.copyWith(fetchDepth: l.fetchDepth - 1);
    case NotifiedEvent():
      return l.notified ? l : l.copyWith(notified: true);
    case BucketSwitchEvent(:final resolvedBefore):
      return seedLifecycle(resolvedBefore).copyWith(fetchDepth: 0);
  }
}

bool isAuthoritative(QueryLifecycle l) => l.phase != QueryPhase.cold;

bool hasServerMembership(QueryLifecycle l) =>
    l.phase == QueryPhase.live || l.phase == QueryPhase.viewLost;

QueryStatus deriveStatus(QueryLifecycle l) =>
    l.fetchDepth > 0 ? QueryStatus.fetching : QueryStatus.idle;
