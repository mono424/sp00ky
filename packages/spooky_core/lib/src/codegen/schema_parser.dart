/// A parsed field from a `DEFINE FIELD` statement.
class FieldDef {
  FieldDef({
    required this.name,
    required this.type,
    required this.optional,
    required this.isRecord,
    required this.isDateTime,
    this.recordTable,
    this.opaque = false,
    this.ephemeral = false,
  });

  /// Field name as declared (kept as-is, e.g. snake_case).
  final String name;

  /// Normalized type expression with any `option<>` stripped
  /// (e.g. `string`, `datetime`, `record`, `array<string>`).
  final String type;
  final bool optional;
  final bool isRecord;
  final bool isDateTime;

  /// For `record<x>`, the referenced table (`x`); null for bare `record`.
  final String? recordTable;

  /// `-- @opaque`: synced to the client and readable, but the sync engine never
  /// stores the value, so it cannot be used for server-side filtering or
  /// ordering. Contrast `-- @nosync`, which removes the field entirely.
  final bool opaque;

  /// `-- @ephemeral` (on a `-- @channel` table): the value only travels between
  /// clients over peer to peer channels and is never written to the database.
  final bool ephemeral;
}

/// A parsed table from a `DEFINE TABLE` statement, with its fields.
class TableDef {
  TableDef(this.name, {this.channel = false}) : fields = [];
  final String name;
  final List<FieldDef> fields;

  /// `-- @channel`: clients may open peer to peer channels on its rows
  /// (`client.channel(query, peers)`).
  bool channel;
}

final _defineTable = RegExp(
    r'^DEFINE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+|OVERWRITE\s+)?([A-Za-z_][A-Za-z0-9_]*)',
    caseSensitive: false);

// `@nosync` marker. Source schemas carry it as a `-- @nosync` comment line
// preceding the DEFINE TABLE (or DEFINE FIELD); once the CLI materializes it,
// the server DEFINE TABLE carries a `COMMENT 'sp00ky:nosync'` clause instead.
final _nosyncComment = RegExp(r'--\s*@nosync\b', caseSensitive: false);
const _nosyncTableComment = 'sp00ky:nosync';

// `@opaque`: the field IS synced to the client (unlike `@nosync`), but the sync
// engine never stores its value, so it cannot be filtered or ordered on.
final _opaqueComment = RegExp(r'--\s*@opaque\b', caseSensitive: false);

// `@channel` (table) / `@ephemeral` (field): peer to peer channels.
final _channelComment = RegExp(r'--\s*@channel\b', caseSensitive: false);
final _ephemeralComment = RegExp(r'--\s*@ephemeral\b', caseSensitive: false);

final _defineField = RegExp(
    r'^DEFINE\s+FIELD\s+(?:IF\s+NOT\s+EXISTS\s+|OVERWRITE\s+)?([A-Za-z_][A-Za-z0-9_.*\[\]]*)\s+ON\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_]*)\s+TYPE\s+(.*)$',
    caseSensitive: false);

// Line-anchored variants used when scanning the RAW schema for annotation
// comments, where a statement has not yet been isolated by splitting on `;`.
final _defineFieldLine = RegExp(
    r'^DEFINE\s+FIELD\s+(?:IF\s+NOT\s+EXISTS\s+|OVERWRITE\s+)?([A-Za-z_][A-Za-z0-9_.*\[\]]*)\s+ON\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_]*)',
    caseSensitive: false);

// Clause keywords that can follow the TYPE expression in a DEFINE FIELD.
final _typeTerminator = RegExp(
    r'\s+(ASSERT|DEFAULT|VALUE|PERMISSIONS|READONLY|COMMENT|REFERENCE|FLEXIBLE)\b',
    caseSensitive: false);

/// A `DEFINE ACCESS` definition's signup/signin variable names (the `$vars`
/// referenced in its SIGNUP/SIGNIN bodies). SurrealQL declares no types for
/// these, so the generator emits them as `String`.
class AccessDef {
  AccessDef({
    required this.name,
    required this.signupParams,
    required this.signinParams,
  });
  final String name;
  final List<String> signupParams;
  final List<String> signinParams;
}

/// Tables + access methods parsed from a schema (backends come from OpenAPI).
class ParsedSchema {
  ParsedSchema({required this.tables, required this.accesses});
  final List<TableDef> tables;
  final List<AccessDef> accesses;
}

/// Parse tables and `DEFINE ACCESS` methods from a schema.
ParsedSchema parseProject(String surql) =>
    ParsedSchema(tables: parseSchema(surql), accesses: parseAccesses(surql));

// SurrealQL built-in vars that are never user-supplied signup/signin params.
const _builtinVars = {
  'auth',
  'value',
  'access',
  'token',
  'session',
  'scope',
  'before',
  'after',
  'this',
  'parent',
  'input',
  'event',
};

final _defineAccess =
    RegExp(r'DEFINE\s+ACCESS\s+([A-Za-z_][A-Za-z0-9_]*)', caseSensitive: false);

/// Extract [AccessDef]s by scanning the full source (access blocks span
/// braces/parens with internal `;`, so they survive the statement splitter).
List<AccessDef> parseAccesses(String surql) {
  final src = _stripComments(surql);
  final out = <AccessDef>[];
  for (final m in _defineAccess.allMatches(src)) {
    final name = m.group(1)!;
    final rest = src.substring(m.end);
    out.add(AccessDef(
      name: name,
      signupParams: _varsInSection(rest, 'SIGNUP'),
      signinParams: _varsInSection(rest, 'SIGNIN'),
    ));
  }
  return out;
}

/// `$var` names referenced inside the balanced `{...}`/`(...)` body following
/// [keyword], minus SurrealQL built-ins, de-duplicated in first-seen order.
List<String> _varsInSection(String block, String keyword) {
  final kw = RegExp('\\b$keyword\\b', caseSensitive: false).firstMatch(block);
  if (kw == null) return const [];
  final body = _balancedSpan(block, kw.end);
  if (body == null) return const [];
  final seen = <String>{};
  final result = <String>[];
  for (final v in RegExp(r'\$([A-Za-z_][A-Za-z0-9_]*)').allMatches(body)) {
    final name = v.group(1)!;
    if (_builtinVars.contains(name)) continue;
    if (seen.add(name)) result.add(name);
  }
  return result;
}

/// Starting at [from], skip whitespace to the first `{` or `(`, then return the
/// substring up to its matching close (handling nesting). Null if none.
String? _balancedSpan(String s, int from) {
  var i = from;
  while (i < s.length && s[i].trim().isEmpty) {
    i++;
  }
  if (i >= s.length) return null;
  final open = s[i];
  final close = open == '{' ? '}' : (open == '(' ? ')' : null);
  if (close == null) return null;
  var depth = 0;
  final start = i;
  for (; i < s.length; i++) {
    if (s[i] == open) depth++;
    if (s[i] == close) {
      depth--;
      if (depth == 0) return s.substring(start + 1, i);
    }
  }
  return null;
}

/// Parse a SurrealQL schema into ordered [TableDef]s. Tables are emitted in the
/// order their `DEFINE TABLE` appears; fields in declaration order. Statements
/// other than DEFINE TABLE/FIELD are ignored.
List<TableDef> parseSchema(String surql) {
  final tables = <String, TableDef>{};
  final order = <String>[];
  // Tables marked `@nosync` are server-only and excluded from the generated
  // client schema (mirrors the TS CLI, which strips them too). `_00_`-prefixed
  // system/meta tables (e.g. `_00_user_feature`) are excluded the same way:
  // they are synced and read at runtime but must never appear in generated
  // types. Their fields are skipped so a trailing DEFINE FIELD can't resurrect
  // the table.
  final nosync = <String>{};

  TableDef tableFor(String name) {
    final existing = tables[name];
    if (existing != null) return existing;
    final t = TableDef(name);
    tables[name] = t;
    order.add(name);
    return t;
  }

  // Collect `-- @nosync` / `-- @opaque` annotations from the original text
  // first: they are comment lines preceding their statement, so they must be
  // read before comments are stripped. (The materialized
  // `COMMENT 'sp00ky:nosync'` form survives stripping and is handled in the main
  // loop below.)
  final annotations = _annotationsFromComments(surql);
  nosync.addAll(annotations.nosyncTables);

  // Strip line comments BEFORE splitting on `;`. A `;` inside a `--` comment
  // (e.g. "owner of this broadcast (the publisher; = relay stream id)") would
  // otherwise split mid-statement, leaving the following `DEFINE FIELD` with a
  // junk prefix so its `^`-anchored regex never matches — silently dropping the
  // field (this is exactly how `broadcast.owner`/`restream_providers` vanished).
  final clean = _stripComments(surql);

  for (final raw in clean.split(';')) {
    final stmt = raw.trim();
    if (stmt.isEmpty) continue;

    final tableMatch = _defineTable.firstMatch(stmt);
    if (tableMatch != null) {
      final name = tableMatch.group(1)!;
      if (name.startsWith('_00_') ||
          nosync.contains(name) ||
          stmt.contains(_nosyncTableComment)) {
        nosync.add(name);
        continue;
      }
      final table = tableFor(name);
      if (annotations.channelTables.contains(name)) table.channel = true;
      continue;
    }

    final fieldMatch = _defineField.firstMatch(stmt);
    if (fieldMatch != null) {
      final fieldName = fieldMatch.group(1)!;
      final tableName = fieldMatch.group(2)!;
      if (nosync.contains(tableName)) continue;
      final key = '$tableName.$fieldName';
      // A `-- @nosync` FIELD is server-only: drop it, but keep the table. The
      // materialized server form carries `COMMENT 'sp00ky:opaque'`, which the
      // Rust CLI also stamps on `@crdt`/`@opaque` fields — so that marker alone
      // cannot be used to decide this, and only the comment form is honored.
      if (annotations.nosyncFields.contains(key)) continue;
      final typeExpr = _extractType(fieldMatch.group(3)!);
      tableFor(tableName).fields.add(_buildField(
            fieldName,
            typeExpr,
            opaque: annotations.opaqueFields.contains(key),
            ephemeral: annotations.ephemeralFields.contains(key),
          ));
    }
  }

  final result = [for (final name in order) tables[name]!];
  _validateChannels(result, nosync);
  return result;
}

/// `-- @channel` / `-- @ephemeral` rules, as the Rust CLI enforces them: an
/// ephemeral field is never written to the database, so its table must be a
/// channel table (the only way the value can travel) and the field must be
/// optional (a SCHEMAFULL create would otherwise demand a value that never
/// comes).
void _validateChannels(List<TableDef> tables, Set<String> nosync) {
  for (final table in tables) {
    for (final field in table.fields) {
      if (!field.ephemeral) continue;
      final where = '${table.name}.${field.name}';
      if (!table.channel) {
        throw FormatException("Field '$where' is `-- @ephemeral` but table "
            "'${table.name}' is not `-- @channel`. Ephemeral values only travel "
            "over channels: add `-- @channel` above `DEFINE TABLE ${table.name}`.");
      }
      if (field.opaque) {
        throw FormatException(
            "Field '$where' cannot be both `-- @ephemeral` and `-- @opaque`.");
      }
      if (!field.optional) {
        throw FormatException("Field '$where' is `-- @ephemeral`, so the "
            'database never receives a value for it. Declare it optional '
            '(`TYPE option<...>`).');
      }
    }
  }
}

/// `-- @nosync` / `-- @opaque` annotations, resolved to the statement each one
/// precedes. Scanned on the raw schema, before comment stripping.
class _CommentAnnotations {
  final Set<String> nosyncTables = {};
  /// `"<table>.<field>"` keys, matching how the main loop looks them up.
  final Set<String> nosyncFields = {};
  final Set<String> opaqueFields = {};
  final Set<String> channelTables = {};
  final Set<String> ephemeralFields = {};
}

/// Resolve annotation comments to the `DEFINE TABLE` / `DEFINE FIELD` that
/// follows them.
///
/// The pending flags MUST be cleared by any statement, not just a matching one.
/// Clearing only on `DEFINE TABLE` (as this did) let a field-level `-- @nosync`
/// leak forward to the next `DEFINE TABLE` in the file, which was then dropped
/// from the generated types entirely — a whole table silently missing from the
/// Dart client, with no warning. A blank line clears too, mirroring
/// `annotations.rs` on the Rust side so both parsers agree on attachment.
_CommentAnnotations _annotationsFromComments(String surql) {
  final result = _CommentAnnotations();
  var pendingNosync = false;
  var pendingOpaque = false;
  var pendingChannel = false;
  var pendingEphemeral = false;

  void clear() {
    pendingNosync = false;
    pendingOpaque = false;
    pendingChannel = false;
    pendingEphemeral = false;
  }

  for (final line in surql.split('\n')) {
    final trimmed = line.trim();

    if (trimmed.startsWith('--')) {
      if (_nosyncComment.hasMatch(trimmed)) pendingNosync = true;
      if (_opaqueComment.hasMatch(trimmed)) pendingOpaque = true;
      if (_channelComment.hasMatch(trimmed)) pendingChannel = true;
      if (_ephemeralComment.hasMatch(trimmed)) pendingEphemeral = true;
      // A non-annotation comment does not clear pending, so prose between the
      // marker and the statement is allowed.
      continue;
    }

    if (trimmed.isEmpty) {
      clear();
      continue;
    }

    final table = _defineTable.firstMatch(trimmed);
    if (table != null) {
      if (pendingNosync) result.nosyncTables.add(table.group(1)!);
      if (pendingChannel) result.channelTables.add(table.group(1)!);
      clear();
      continue;
    }

    final field = _defineFieldLine.firstMatch(trimmed);
    if (field != null) {
      final key = '${field.group(2)!}.${field.group(1)!}';
      if (pendingNosync) result.nosyncFields.add(key);
      if (pendingOpaque) result.opaqueFields.add(key);
      if (pendingEphemeral) result.ephemeralFields.add(key);
      clear();
      continue;
    }

    // Any other statement orphans the annotation.
    clear();
  }
  return result;
}

String _stripComments(String s) =>
    s.replaceAll(RegExp(r'--.*'), '').replaceAll(RegExp(r'#.*'), '');

/// Remove `-- @nosync` fields (and the annotation comment that marked them) from
/// a schema, for embedding as the client's local cache DDL.
///
/// Only single-line `DEFINE FIELD ...;` statements are handled, which is the form
/// the marker attaches to; a multi-line definition is left in place rather than
/// risking a mangled statement. The annotation comment must go along with the
/// field: an orphan `-- @nosync` re-attaches to the next `DEFINE TABLE` and would
/// drop that whole table on the next parse.
String stripServerOnlyFields(String surql) {
  final lines = surql.split('\n');
  final out = <String>[];
  final annotations = _annotationsFromComments(surql);
  if (annotations.nosyncFields.isEmpty) return surql;

  for (final line in lines) {
    final trimmed = line.trim();
    final field = _defineFieldLine.firstMatch(trimmed);
    if (field != null && trimmed.endsWith(';')) {
      final key = '${field.group(2)!}.${field.group(1)!}';
      if (annotations.nosyncFields.contains(key)) {
        // Drop the contiguous comment block that introduced this field, but only
        // if it holds an annotation — otherwise ordinary prose above an
        // unrelated statement could be eaten. Dropping the whole block (not just
        // the annotation lines) is what handles
        // `-- @nosync` / `-- prose` / `DEFINE FIELD`, where stopping at the prose
        // would strand the marker and mis-mark the next statement.
        var blockStart = out.length;
        while (blockStart > 0 && out[blockStart - 1].trim().startsWith('--')) {
          blockStart--;
        }
        final hasAnnotation = out
            .sublist(blockStart)
            .any((l) => RegExp(r'^--\s*@[a-z]').hasMatch(l.trim()));
        if (hasAnnotation) out.removeRange(blockStart, out.length);
        continue;
      }
    }
    out.add(line);
  }
  return out.join('\n');
}

/// Trim trailing clause keywords (ASSERT/DEFAULT/...) off the TYPE expression.
String _extractType(String afterType) {
  final term = _typeTerminator.firstMatch(afterType);
  final typePart =
      term != null ? afterType.substring(0, term.start) : afterType;
  return typePart.trim();
}

FieldDef _buildField(String name, String typeExpr,
    {bool opaque = false, bool ephemeral = false}) {
  var type = typeExpr.trim();
  var optional = false;

  // option<T> -> optional, inner T
  final opt =
      RegExp(r'^option\s*<\s*(.*)\s*>$', caseSensitive: false).firstMatch(type);
  if (opt != null) {
    optional = true;
    type = opt.group(1)!.trim();
  }

  final lower = type.toLowerCase();
  final isDateTime = lower == 'datetime';
  final isRecord = lower == 'record' || lower.startsWith('record<');

  String? recordTable;
  if (isRecord) {
    final rec =
        RegExp(r'^record\s*<\s*([A-Za-z_][A-Za-z0-9_]*)', caseSensitive: false)
            .firstMatch(type);
    recordTable = rec?.group(1);
    type = 'record';
  }

  return FieldDef(
    name: name,
    type: type,
    optional: optional,
    isRecord: isRecord,
    isDateTime: isDateTime,
    recordTable: recordTable,
    opaque: opaque,
    ephemeral: ephemeral,
  );
}
