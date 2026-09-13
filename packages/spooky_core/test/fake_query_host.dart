import 'package:spooky_core/spooky_core.dart';

/// One registration a module asked for.
typedef Registration = ({String table, String surql, QueryTimeToLive ttl});

/// A [QueryHost] that records what a module registered and lets a test push the
/// rows the shared query would have delivered.
class FakeQueryHost implements QueryHost {
  final List<Registration> registrations = [];
  final Map<String, QueryUpdateCallback> _subscribers = {};
  var _next = 0;

  /// How many subscriptions are currently live.
  int get liveSubscriptions => _subscribers.length;

  @override
  Future<String> registerQuery(String table, String surql,
      Map<String, dynamic> params, QueryTimeToLive ttl) async {
    registrations.add((table: table, surql: surql, ttl: ttl));
    return 'hash-${_next++}';
  }

  @override
  void Function() subscribe(String hash, QueryUpdateCallback cb,
      {bool immediate = false}) {
    _subscribers[hash] = cb;
    if (immediate) cb(const []);
    return () => _subscribers.remove(hash);
  }

  /// Deliver [rows] to every live subscriber, as a materialization would.
  void emit(List<Map<String, dynamic>> rows) {
    for (final cb in [..._subscribers.values]) {
      cb(rows);
    }
  }
}

/// Minimal [AuthService] stand-in exposing only the `subscribe` a module uses.
class FakeAuth implements AuthService {
  @override
  AuthVerificationError? get verificationError => null;
  final List<void Function(String?)> _listeners = [];
  String? userId;

  void emit(String? next) {
    userId = next;
    for (final cb in [..._listeners]) {
      cb(next);
    }
  }

  @override
  void Function() subscribe(void Function(String? userId) cb) {
    _listeners.add(cb);
    cb(userId);
    return () => _listeners.remove(cb);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
