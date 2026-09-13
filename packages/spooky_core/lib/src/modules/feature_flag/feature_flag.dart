import '../../services/logger/logger.dart';
import '../../utils/duration_utils.dart';
import '../auth/auth_service.dart';
import '../query_host.dart';

/// One shared LIVE query over ALL of the signed-in user's assignments — the
/// `_00_user_feature` select permission scopes it to `user = $auth.id`, so no
/// per-key `WHERE key = $key` param is needed. A single registration means every
/// flag the user is (or becomes) assigned is observed at once: new assignments
/// stream in live, and a handle for an unassigned key simply resolves to its
/// fallback. Avoids one-registration-per-flag and the param-filtered live query.
///
/// `_00_user_feature` is a server-provisioned meta table, absent from the app
/// `schemaSurql`; the native circuit permission is seeded as a built-in in
/// [StreamProcessorService] (without it the view is default-deny and empty).
const String featureQuery =
    'SELECT key, variant, payload FROM _00_user_feature';

/// Immutable view of a flag's current assignment.
class FeatureFlagSnapshot {
  const FeatureFlagSnapshot({this.variant, this.payload});
  final String? variant;
  final Object? payload;
}

/// A per-key handle (TS `FeatureFlagHandle`). Holds the latest snapshot for one
/// flag key, falling back to [fallback] when the key has no assignment.
class FeatureFlagHandle {
  FeatureFlagHandle(this.key, this.fallback);

  final String key;
  final String? fallback;

  FeatureFlagSnapshot _latest = const FeatureFlagSnapshot();
  final Set<void Function(FeatureFlagSnapshot)> _listeners = {};
  bool _closed = false;
  void Function()? _onClose;

  /// Push a fresh snapshot to this handle and notify its listeners.
  void set(FeatureFlagSnapshot snapshot) {
    if (_closed) return;
    _latest = snapshot;
    for (final cb in _listeners.toList()) {
      cb(snapshot);
    }
  }

  /// Current variant, or [fallback] when unassigned.
  String? variant() => _latest.variant ?? fallback;

  /// Current payload (typed), or null when unassigned.
  T? payload<T>() => _latest.payload as T?;

  /// True when a real variant exists and isn't the `'off'` sentinel.
  bool enabled() {
    final v = variant();
    return v != null && v != 'off';
  }

  /// Subscribe to snapshot changes. Fires immediately with the current value and
  /// returns an unsubscribe fn.
  void Function() subscribe(void Function(FeatureFlagSnapshot) cb) {
    _listeners.add(cb);
    cb(FeatureFlagSnapshot(variant: variant(), payload: _latest.payload));
    return () => _listeners.remove(cb);
  }

  void _attachOnClose(void Function() cb) => _onClose = cb;

  /// Tear down this handle (drops listeners and detaches from the module).
  void close() {
    if (_closed) return;
    _closed = true;
    _listeners.clear();
    _onClose?.call();
  }
}

/// Reactive feature-flag module (faithful port of the TS `FeatureFlagModule`).
///
/// Runs a single shared live query over the user's `_00_user_feature` rows and
/// fans per-key snapshots out to [FeatureFlagHandle]s. Auth-aware: a user change
/// tears down the query, clears snapshots, and re-observes.
class FeatureFlagModule {
  FeatureFlagModule({
    required QueryHost host,
    required AuthService auth,
    required SpookyLogger logger,
  })  : _host = host,
        _auth = auth,
        _logger = logger.child('FeatureFlagModule');

  final QueryHost _host;
  final AuthService _auth;
  final SpookyLogger _logger;

  final Set<FeatureFlagHandle> _handles = {};
  void Function()? _authUnsubscribe;
  String? _lastUserId;
  bool _authInitialized = false;

  // The single shared live query over the user's assignments.
  void Function()? _querySubscription;
  bool _starting = false;
  // Longest TTL any caller asked for (the query is shared across all flags).
  QueryTimeToLive _ttl = defaultTtl;
  // Latest assignment per key, plus whether the query has resolved at least once
  // (so a handle created before the first result knows to wait vs. fall back).
  // `_snapshots` only holds ASSIGNED keys; an absent key -> fallback.
  final Map<String, FeatureFlagSnapshot> _snapshots = {};
  bool _loaded = false;

  /// Subscribe to auth changes so the shared query follows sign-in/out.
  void init() {
    if (_authInitialized) return;
    _authInitialized = true;
    _authUnsubscribe = _auth.subscribe((userId) {
      if (userId == _lastUserId) return;
      _lastUserId = userId;
      _refresh();
    });
  }

  /// Get a reactive handle for [key]. Late callers are seeded immediately from
  /// the already-resolved shared query so an assigned key doesn't flash its
  /// fallback.
  FeatureFlagHandle feature(String key, {String? fallback, QueryTimeToLive? ttl}) {
    final handle = FeatureFlagHandle(key, fallback);
    _handles.add(handle);
    handle._attachOnClose(() => _handles.remove(handle));
    if (ttl != null) _ttl = ttl;
    if (_loaded) {
      handle.set(_snapshots[key] ?? const FeatureFlagSnapshot());
    }
    _ensureStarted();
    return handle;
  }

  /// Tear down everything (call from the client's close()).
  void closeAll() {
    _authUnsubscribe?.call();
    _authUnsubscribe = null;
    _teardownQuery();
    for (final handle in _handles.toList()) {
      handle.close();
    }
  }

  /// Auth changed: drop the old user's query/snapshots and re-observe.
  void _refresh() {
    _teardownQuery();
    _loaded = false;
    _snapshots.clear();
    // Clear handles immediately so a sign-out hides flag-gated UI without lag.
    for (final handle in _handles) {
      handle.set(const FeatureFlagSnapshot());
    }
    _ensureStarted();
  }

  void _teardownQuery() {
    _querySubscription?.call();
    _querySubscription = null;
  }

  /// Start the single shared live query (idempotent; no-op with no handles).
  void _ensureStarted() {
    if (_querySubscription != null || _starting || _handles.isEmpty) return;
    _starting = true;
    // Register the shared query and subscribe to its rows. Unawaited: a handle
    // reads its fallback until the first result lands.
    () async {
      try {
        final hash = await _host.registerQuery(
            '_00_user_feature', featureQuery, const {}, _ttl);
        _querySubscription = _host.subscribe(
          hash,
          _applyRecords,
          immediate: true,
        );
      } catch (err) {
        _logger.warn('Failed to register feature flag query: $err');
      } finally {
        _starting = false;
      }
    }();
  }

  /// Live query result -> per-key snapshots -> push to every active handle.
  void _applyRecords(List<Map<String, dynamic>> records) {
    _snapshots.clear();
    for (final row in records) {
      final key = row['key'];
      if (key is String) {
        _snapshots[key] = FeatureFlagSnapshot(
          variant: row['variant'] as String?,
          payload: row['payload'],
        );
      }
    }
    _loaded = true;
    for (final handle in _handles) {
      handle.set(_snapshots[handle.key] ?? const FeatureFlagSnapshot());
    }
  }
}
