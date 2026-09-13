import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../modules/ref_tables.dart' show bucketIdForUser;
import '../query/env.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestOp, IngestRecord;
import '../state/reducers.dart' as r;
import '../utils/parser.dart' show cleanRecord;
import '../utils/record_id_utils.dart';
import 'bucket_switch_saga.dart';

/// The signed-in principal changed (sign-in, sign-out, boot verification).
/// Identity first (routing and `$auth` for the local SSP), then the bucket,
/// then the salt if the principal really changed, then the verified user row.
Future<void> authFlip(Ctx ctx, SagaEnv env, String? userId) async {
  final authId = await ctx(Fx.service<String?>(ServiceName.authSessionAuthId));
  final access = await ctx(Fx.service<String?>(ServiceName.authAccess));
  await ctx(Fx.stateUpdate(r.setIdentity(userId: userId)));
  await ctx(
      Fx.service<void>(ServiceName.sspSetSessionAuth, [authId, access]));
  final target = bucketIdForUser(userId);
  await ctx(Fx.service<void>(ServiceName.hintWrite, [target]));
  await ctx(Fx.stateUpdate(r.setIdentity(pendingBucket: target)));
  await bucketSwitch(ctx, env, target);
  final saltUserId = await ctx(Fx.stateRead((s) => s.saltUserId));
  if (authId != saltUserId) {
    final salt = await ctx(Fx.id(IdScope.salt));
    await ctx(Fx.stateUpdate(
        r.setIdentity(sessionId: salt, saltUserId: authId)));
    await ctx(Fx.service<void>(ServiceName.crdtSetSessionId, [salt]));
  }
  await persistVerifiedUser(ctx, env);
}

/// Write the user's own row, as the server just returned it, into the local
/// store so `user` queries see `email_verified` and friends without waiting for
/// membership. A field refresh, not a version claim.
Future<void> persistVerifiedUser(Ctx ctx, SagaEnv env) async {
  final row = await ctx(Fx.service<Map<String, dynamic>?>(
      ServiceName.authCurrentUser));
  if (row == null || row['id'] == null || row.length <= 1) return;
  final id = row['id'].toString();
  final table = extractTablePart(id);
  final columns = columnsFor(env, table);
  if (columns == null) return;
  final version = await ctx(Fx.stateRead((s) => s.versions[id])) ?? 1;
  final cleaned = columns.isEmpty ? row : cleanRecord(columns, row);
  final body = {...cleaned, 'id': id, '_00_rv': version};
  try {
    await ctx(Fx.localTx([PutOp(table, id, body, mode: WriteMode.merge)]));
    await ctx(Fx.sspIngest([
      IngestRecord(
          table: table, op: IngestOp.update, id: id, record: body)
    ]));
    await ctx(Fx.stateUpdate(r.compose([
      r.setVersions([(id, version)]),
      r.markTableDirty(table),
    ])));
  } catch (error) {
    await ctx(Fx.log(LogLevel.warn,
        'could not persist the verified user row', {'error': error}));
  }
}
