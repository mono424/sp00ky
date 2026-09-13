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
  Future<void> signOut();
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
