import '../../services/logger/logger.dart';
import '../../utils/duration_utils.dart';
import '../../utils/semver.dart';
import '../auth/auth_service.dart';
import '../query_host.dart';

/// One shared LIVE query over every app's release row (TS `RELEASE_QUERY`).
///
/// `_00_app_release` is world-readable (root-only writes), one row per app keyed
/// by name, written by `spky deploy` / `spky release` / the git-linked builder. A
/// single registration observes every app at once; a handle for an app with no
/// row simply reports no update. Mirrors [FeatureFlagModule].
///
/// Like `_00_user_feature`, this is a server-provisioned meta table absent from
/// the app `schemaSurql`, so its native-circuit permission is seeded as a
/// built-in in `StreamProcessorService` (without it the view is default-deny and
/// stays empty).
const String releaseQuery = 'SELECT * FROM _00_app_release';

/// Immutable view of an app's latest announced release (TS `AppReleaseSnapshot`).
class AppReleaseSnapshot {
  const AppReleaseSnapshot({
    this.version,
    this.cacheBust = false,
    this.mandatory = false,
    this.releasedAt,
  });

  /// Latest announced version, or null when no row exists for the app.
  final String? version;

  /// Clients should clear caches when reloading onto this version.
  final bool cacheBust;

  /// Clients should update immediately instead of asking.
  final bool mandatory;

  final String? releasedAt;
}

const AppReleaseSnapshot _emptySnapshot = AppReleaseSnapshot();

/// A per-app handle (TS `AppReleaseHandle`).
class AppReleaseHandle {
  AppReleaseHandle(this.app);

  final String app;

  AppReleaseSnapshot _latest = _emptySnapshot;
  final Set<void Function(AppReleaseSnapshot)> _listeners = {};
  bool _closed = false;
  void Function()? _onClose;

  /// Push a fresh snapshot to this handle and notify its listeners.
  void set(AppReleaseSnapshot snapshot) {
    if (_closed) return;
    _latest = snapshot;
    for (final cb in _listeners.toList()) {
      cb(snapshot);
    }
  }

  AppReleaseSnapshot snapshot() => _latest;

  /// Latest announced version, or null when none has been announced.
  String? version() => _latest.version;

  /// Whether clients should clear caches when reloading onto this version.
  bool get cacheBust => _latest.cacheBust;

  /// Whether clients should update immediately instead of asking.
  bool get mandatory => _latest.mandatory;

  /// True when the announced version is semver-newer than [currentVersion]. A
  /// malformed version on either side is never newer, so a bad release row can't
  /// nag every client.
  bool updateAvailable(String currentVersion) =>
      semverGt(_latest.version, currentVersion);

  /// Subscribe to snapshot changes. Fires immediately with the current value and
  /// returns an unsubscribe fn.
  void Function() subscribe(void Function(AppReleaseSnapshot) cb) {
    _listeners.add(cb);
    cb(_latest);
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

/// Reactive app-release module (faithful port of the TS `AppReleaseModule`).
///
/// Runs a single shared live query over `_00_app_release` and fans per-app
/// snapshots out to [AppReleaseHandle]s, so an app can prompt (or force) an
/// update when the deployed version moves past the running build.
class AppReleaseModule {
  AppReleaseModule({
    required QueryHost host,
    required AuthService auth,
    required SpookyLogger logger,
  })  : _host = host,
        _auth = auth,
        _logger = logger.child('AppReleaseModule');

  final QueryHost _host;
  final AuthService _auth;
  final SpookyLogger _logger;

  final Set<AppReleaseHandle> _handles = {};
  void Function()? _authUnsubscribe;
  String? _lastUserId;
  bool _authInitialized = false;

  void Function()? _querySubscription;
  bool _starting = false;
  QueryTimeToLive _ttl = defaultTtl;
  final Map<String, AppReleaseSnapshot> _snapshots = {};
  bool _loaded = false;

  /// Subscribe to auth changes so the shared query follows sign-in/out. The
  /// table is world-readable, so the data is the same for every user — this is
  /// purely plumbing hygiene: a new session invalidates the old circuit plan.
  void init() {
    if (_authInitialized) return;
    _authInitialized = true;
    _authUnsubscribe = _auth.subscribe((userId) {
      if (userId == _lastUserId) return;
      _lastUserId = userId;
      _refresh();
    });
  }

  /// Get a reactive handle for [app]. Late callers are seeded immediately from
  /// the already-resolved shared query, so a known release doesn't flash empty.
  AppReleaseHandle release(String app, {QueryTimeToLive? ttl}) {
    final handle = AppReleaseHandle(app);
    _handles.add(handle);
    handle._attachOnClose(() => _handles.remove(handle));
    if (ttl != null) _ttl = ttl;
    if (_loaded) {
      handle.set(_snapshots[app] ?? _emptySnapshot);
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

  /// Auth changed: drop the old query/snapshots and re-observe.
  void _refresh() {
    _teardownQuery();
    _loaded = false;
    _snapshots.clear();
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
    // reports no update until the first result lands.
    () async {
      try {
        final hash = await _host.registerQuery(
            '_00_app_release', releaseQuery, const {}, _ttl);
        _querySubscription = _host.subscribe(
          hash,
          _applyRecords,
          immediate: true,
        );
      } catch (err) {
        _logger.warn('Failed to register app release query: $err');
      } finally {
        _starting = false;
      }
    }();
  }

  /// Live query result -> per-app snapshots -> push to every active handle.
  void _applyRecords(List<Map<String, dynamic>> records) {
    _snapshots.clear();
    for (final row in records) {
      final app = row['app'];
      final version = row['version'];
      if (app is String && version is String) {
        _snapshots[app] = AppReleaseSnapshot(
          version: version,
          cacheBust: row['cache_bust'] == true,
          mandatory: row['mandatory'] == true,
          releasedAt: row['released_at']?.toString(),
        );
      }
    }
    _loaded = true;
    for (final handle in _handles) {
      handle.set(_snapshots[handle.app] ?? _emptySnapshot);
    }
  }
}
