import '../../services/logger/logger.dart';

/// The access name the platform issues impersonation sessions under (TS
/// `IMPERSONATION_ACCESS`). Per-device state (push registrations) must never
/// be attached to a user while an operator is looking through their eyes.
const impersonationAccess = '_00_impersonate';

/// Who is signing out, captured before anything is cleared.
class SignOutContext {
  const SignOutContext({this.userId, this.token, this.impersonating = false});
  final String? userId;
  final String? token;
  final bool impersonating;
}

/// Runs while the session is still valid, so the hook can reach the server
/// as the leaving user. Each hook bounds itself; one failing never blocks the
/// others or the sign-out.
typedef SignOutHook = Future<void> Function(SignOutContext context);

/// A set of [SignOutHook]s, run in parallel.
class SignOutHooks {
  SignOutHooks(this._logger);
  final SpookyLogger _logger;
  final _hooks = <SignOutHook>[];

  /// Returns the remover.
  void Function() add(SignOutHook hook) {
    _hooks.add(hook);
    return () => _hooks.remove(hook);
  }

  void clear() => _hooks.clear();

  Future<void> run(SignOutContext context) => Future.wait([
        for (final hook in _hooks.toList())
          Future.sync(() => hook(context)).catchError(
              (Object e) => _logger.debug('A sign-out hook failed: $e')),
      ]);
}

/// Public auth state is a snapshot. Profile edits go through local mutations.
abstract interface class Sp00kyAuth {
  String? get token;
  AuthVerificationError? get verificationError;
  Map<String, dynamic>? get currentUser;
  bool get isAuthenticated;
  bool get isLoading;
  String? get access;
  void Function() subscribe(void Function(String?) callback);
  Future<void> signIn(String accessName, Map<String, dynamic> params);
  Future<void> signUp(String accessName, Map<String, dynamic> params);

  /// Validate a token with the server and open the session as its account
  /// (TS `auth.check(token)`). Without [token] it re-verifies the stored one.
  /// Use it for a token minted outside the client, such as an OAuth exchange:
  /// unlike a raw `authenticate`, the session is persisted and listeners fire.
  Future<void> check([String? token]);
  Future<void> signOut();

  /// Register work that must happen before the session is dropped (drain
  /// the outbox, unregister this device from push). Returns the remover.
  void Function() onBeforeSignOut(SignOutHook hook);
}

extension Sp00kyAuthImpersonation on Sp00kyAuth {
  /// The session was opened through the platform's impersonation access.
  bool get isImpersonating => isAuthenticated && access == impersonationAccess;
}

/// A deferred verification failure. The restored session remains usable.
class AuthVerificationError implements Exception {
  const AuthVerificationError(this.category, this.message, this.stack);
  final String category;
  final String message;
  final String stack;
  @override
  String toString() => '$category: $message';
}
