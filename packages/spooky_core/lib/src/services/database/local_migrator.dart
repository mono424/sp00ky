import 'dart:convert';

import 'package:crypto/crypto.dart';

import '../logger/logger.dart';
import 'local_database_service.dart';

/// SHA-1 hex of the schema text (TS `sha1`). Used to detect schema changes.
String schemaSha1(String schemaSurql) =>
    sha1.convert(utf8.encode(schemaSurql)).toString();

/// Provisions the local store against a schema and migrates on change
/// (TS `LocalMigrator`).
///
/// SQLite stores documents, so application schema changes need no destructive
/// table migration. Keep cached rows, memberships and queued writes. Rebuild
/// the circuit using the new schema and reconcile rows through normal sync.
class LocalMigrator {
  LocalMigrator(this._local, SpookyLogger logger)
      : _logger = logger.child('LocalMigrator');

  final LocalDatabaseService _local;
  final SpookyLogger _logger;

  Future<void> provision(String schemaSurql) async {
    final hash = schemaSha1(schemaSurql);
    // The full circuit used to be persisted here as JSON after every ingest.
    // Only the store snapshot is read now; drop the dead row (megabytes on a
    // real store). A no-op once it is gone.
    _local.dropLegacyStreamState();

    if (_local.latestSchemaHash() == hash) {
      _logger.info('[Provisioning] Schema up to date, skipping migration');
      return;
    }

    _logger.info('[Provisioning] Schema changed, rebuilding circuit');
    _local.tx(() {
      _local.clearSnapshot();
      _local.recordSchemaHash(hash, DateTime.now().toUtc().toIso8601String());
    });
  }
}
