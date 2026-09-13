import '../modules/auth/sp00ky_auth.dart';
import '../types.dart';
import '../modules/query_builder.dart';

/// Protocol stays within an isolate group. No callbacks, handles or services.
enum WorkerOp {
  query,
  remote,
  preload,
  create,
  update,
  delete,
  run,
  signIn,
  signUp,
  signOut,
  authenticate,
  deauthenticate,
  subscribe,
  unsubscribe,
  inspect,
  checkpoint,
  wake,
  detach,
  deregister,
  timing,
  failed,
  retry,
  discard,
  close
}

sealed class WorkerCommand {
  const WorkerCommand();
  WorkerOp get op;
}

final class QueryCommand extends WorkerCommand {
  const QueryCommand(this.sql, this.params, this.ttl, this.relations);
  final String sql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;
  final List<RelationPlan> relations;
  @override
  WorkerOp get op => WorkerOp.query;
}

final class RemoteCommand extends WorkerCommand {
  const RemoteCommand(this.sql, this.vars);
  final String sql;
  final Map<String, dynamic>? vars;
  @override
  WorkerOp get op => WorkerOp.remote;
}

final class PreloadCommand extends WorkerCommand {
  const PreloadCommand(this.sql, this.params, this.ttl);
  final String sql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;
  @override
  WorkerOp get op => WorkerOp.preload;
}

final class CreateCommand extends WorkerCommand {
  const CreateCommand(this.recordId, this.data);
  final String recordId;
  final Map<String, dynamic> data;
  @override
  WorkerOp get op => WorkerOp.create;
}

final class UpdateCommand extends WorkerCommand {
  const UpdateCommand(this.table, this.recordId, this.data, this.options);
  final String table;
  final String recordId;
  final Map<String, dynamic> data;
  final UpdateOptions? options;
  @override
  WorkerOp get op => WorkerOp.update;
}

final class DeleteCommand extends WorkerCommand {
  const DeleteCommand(this.table, this.recordId);
  final String table;
  final String recordId;
  @override
  WorkerOp get op => WorkerOp.delete;
}

final class RunCommand extends WorkerCommand {
  const RunCommand(this.backend, this.path, this.payload, this.options);
  final String backend;
  final String path;
  final Map<String, dynamic> payload;
  final RunOptions? options;
  @override
  WorkerOp get op => WorkerOp.run;
}

final class SignInCommand extends WorkerCommand {
  const SignInCommand(this.access, this.params);
  final String access;
  final Map<String, dynamic> params;
  @override
  WorkerOp get op => WorkerOp.signIn;
}

final class SignUpCommand extends WorkerCommand {
  const SignUpCommand(this.access, this.params);
  final String access;
  final Map<String, dynamic> params;
  @override
  WorkerOp get op => WorkerOp.signUp;
}

final class SignOutCommand extends WorkerCommand {
  const SignOutCommand();
  @override
  WorkerOp get op => WorkerOp.signOut;
}

final class AuthenticateCommand extends WorkerCommand {
  const AuthenticateCommand(this.token);
  final String token;
  @override
  WorkerOp get op => WorkerOp.authenticate;
}

final class DeauthenticateCommand extends WorkerCommand {
  const DeauthenticateCommand();
  @override
  WorkerOp get op => WorkerOp.deauthenticate;
}

final class SubscribeCommand extends WorkerCommand {
  const SubscribeCommand(this.subscriptionId, this.hash, this.immediate);
  final int subscriptionId;
  final String hash;
  final bool immediate;
  @override
  WorkerOp get op => WorkerOp.subscribe;
}

final class UnsubscribeCommand extends WorkerCommand {
  const UnsubscribeCommand(this.subscriptionId);
  final int subscriptionId;
  @override
  WorkerOp get op => WorkerOp.unsubscribe;
}

final class InspectCommand extends WorkerCommand {
  const InspectCommand();
  @override
  WorkerOp get op => WorkerOp.inspect;
}

final class CheckpointCommand extends WorkerCommand {
  const CheckpointCommand();
  @override
  WorkerOp get op => WorkerOp.checkpoint;
}

final class WakeCommand extends WorkerCommand {
  const WakeCommand();
  @override
  WorkerOp get op => WorkerOp.wake;
}

final class DetachCommand extends WorkerCommand {
  const DetachCommand();
  @override
  WorkerOp get op => WorkerOp.detach;
}

final class DeregisterCommand extends WorkerCommand {
  const DeregisterCommand(this.hash);
  final String hash;
  @override
  WorkerOp get op => WorkerOp.deregister;
}

final class TimingCommand extends WorkerCommand {
  const TimingCommand(this.hash, this.milliseconds);
  final String hash;
  final double milliseconds;
  @override
  WorkerOp get op => WorkerOp.timing;
}

final class FailedCommand extends WorkerCommand {
  const FailedCommand();
  @override
  WorkerOp get op => WorkerOp.failed;
}

final class RetryCommand extends WorkerCommand {
  const RetryCommand(this.mutationId);
  final String mutationId;
  @override
  WorkerOp get op => WorkerOp.retry;
}

final class DiscardCommand extends WorkerCommand {
  const DiscardCommand(this.mutationId);
  final String mutationId;
  @override
  WorkerOp get op => WorkerOp.discard;
}

final class CloseCommand extends WorkerCommand {
  const CloseCommand();
  @override
  WorkerOp get op => WorkerOp.close;
}

class WorkerRequest {
  const WorkerRequest(this.id, this.command);
  final int id;
  final WorkerCommand command;
  WorkerOp get op => command.op;
}

class WorkerResult<T> {
  const WorkerResult(this.id, this.value, [this.error]);
  final int id;
  final T? value;
  final WorkerFailure? error;
}

/// Stable failure category across the worker boundary, with the original stack.
class WorkerFailure implements Exception {
  const WorkerFailure(this.category, this.message, this.stack);
  final String category;
  final String message;
  final String stack;
  @override
  String toString() => '$category: $message';
}

class AuthSnapshot {
  AuthSnapshot(Sp00kyAuth auth)
      : token = auth.token,
        verificationError = auth.verificationError,
        user = auth.currentUser,
        access = auth.access,
        authenticated = auth.isAuthenticated,
        loading = auth.isLoading;
  final String? token;
  final AuthVerificationError? verificationError;
  final Map<String, dynamic>? user;
  final String? access;
  final bool authenticated;
  final bool loading;
}

class WorkerState {
  const WorkerState(this.epoch, this.auth, this.pending, this.fetching,
      this.failed, this.unsynced, this.health);
  final int epoch;
  final AuthSnapshot? auth;
  final int pending;
  final int fetching;
  final int failed;
  final Set<String> unsynced;
  final SyncHealth health;
}

class WorkerQueryState {
  const WorkerQueryState(this.epoch, this.hash, this.authoritative,
      this.settled, this.status, this.timings);
  final int epoch;
  final String hash;
  final bool authoritative;
  final bool settled;
  final QueryStatus status;
  final QueryTimings? timings;
}

class WorkerRows {
  const WorkerRows(this.epoch, this.id, this.rows, [this.error]);
  final int epoch;
  final int id;
  final List<Map<String, dynamic>> rows;
  final WorkerFailure? error;
}
