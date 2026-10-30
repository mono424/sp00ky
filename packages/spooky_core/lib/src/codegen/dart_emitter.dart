import '../modules/relationships.dart';
import 'openapi_parser.dart';
import 'schema_parser.dart';

/// Emit the `ColumnSchema` map literal consumed by `Sp00kyConfig.schema`.
///
/// Also emits an `'access'` entry per `DEFINE ACCESS`, which `AuthService`
/// reads (`schema['access'][name][method]['params']`) to validate signin/signup
/// params before issuing the RPC. Without it, auth calls throw
/// "Access definition '<name>' not found".
String emitSchemaMap(List<TableDef> tables, [List<AccessDef> accesses = const []]) {
  final buf = StringBuffer('final spookySchema = <String, dynamic>{\n');
  for (final table in tables) {
    buf.writeln("  '${table.name}': {");
    buf.writeln('    \'columns\': {');
    for (final field in table.fields) {
      buf.writeln("      '${field.name}': ${_columnSchema(field)},");
    }
    buf.writeln('    },');
    // `-- @channel`: `client.channel(query, peers)` only accepts these tables.
    if (table.channel) buf.writeln("    'channel': true,");
    buf.writeln('  },');
  }
  final relationships = deriveRelationships(tables);
  if (relationships.isNotEmpty) {
    buf.writeln("  'relationships': [");
    for (final rel in relationships) {
      buf.writeln("    {'from': '${rel.from}', 'field': '${rel.field}', "
          "'to': '${rel.to}', 'cardinality': '${rel.cardinality}'},");
    }
    buf.writeln('  ],');
  }
  if (accesses.isNotEmpty) {
    buf.writeln("  'access': {");
    for (final a in accesses) {
      buf.writeln("    '${a.name}': {");
      // Keys match AuthService._validateAccessParams method strings.
      buf.writeln("      'signIn': {'params': {${_accessParams(a.signinParams)}}},");
      buf.writeln("      'signup': {'params': {${_accessParams(a.signupParams)}}},");
      buf.writeln('    },');
    }
    buf.writeln('  },');
  }
  buf.writeln('};');
  return buf.toString();
}

/// Derive the schema relationships from parsed tables, matching
/// `apps/cli/src/json_schema.rs` so a Dart client and a JS client generated from
/// the same schema expose the same relation names.
///
/// Forward: every `record<x>` field whose target is a known table becomes a
/// `one` relationship under its own field name. Reverse: each of those adds a
/// `many` relationship on the target, named by pluralizing the source table
/// (skipped when the target already declares a field of that name, so an
/// explicit field always wins).
List<SchemaRelationship> deriveRelationships(List<TableDef> tables) {
  final known = {for (final t in tables) t.name};
  final fieldNames = {
    for (final t in tables) t.name: {for (final f in t.fields) f.name},
  };
  final out = <SchemaRelationship>[];
  final seen = <String>{};

  void add(SchemaRelationship rel) {
    if (seen.add('${rel.from}.${rel.field}')) out.add(rel);
  }

  for (final table in tables) {
    for (final field in table.fields) {
      final target = field.recordTable;
      if (!field.isRecord || target == null || !known.contains(target)) continue;
      add(SchemaRelationship(
        from: table.name,
        field: field.name,
        to: target,
        cardinality: 'one',
      ));
    }
  }

  for (final table in tables) {
    for (final field in table.fields) {
      final target = field.recordTable;
      if (!field.isRecord || target == null || !known.contains(target)) continue;
      final reverseField = pluralizeTableName(table.name);
      if (fieldNames[target]?.contains(reverseField) ?? false) continue;
      add(SchemaRelationship(
        from: target,
        field: reverseField,
        to: table.name,
        cardinality: 'many',
      ));
    }
  }

  return out;
}

String _accessParams(List<String> params) =>
    params.map((p) => "'$p': ColumnSchema(type: 'string')").join(', ');

String _columnSchema(FieldDef f) {
  final args = <String>["type: '${f.type}'"];
  if (f.isRecord) args.add('recordId: true');
  if (f.isDateTime) args.add('dateTime: true');
  if (f.optional) args.add('optional: true');
  // `@ephemeral`: travels only over peer to peer channels, never to the
  // database. The client keeps it out of writes and the query builder rejects
  // it in where/orderBy.
  if (f.ephemeral) args.add('ephemeral: true');
  return 'ColumnSchema(${args.join(', ')})';
}

/// Emit a typed model class per table (fields kept as declared; record ids and
/// datetimes mapped to `String` / `DateTime`).
String emitModels(List<TableDef> tables) {
  final buf = StringBuffer();
  for (final table in tables) {
    final className = _pascal(table.name);
    final fields = [
      FieldDef(
          name: 'id',
          type: 'string',
          optional: false,
          isRecord: false,
          isDateTime: false),
      ...table.fields.where((f) => f.name != 'id'),
    ];

    buf.writeln('class $className {');
    // Constructor.
    buf.writeln('  $className({');
    for (final f in fields) {
      final req = f.optional ? '' : 'required ';
      buf.writeln('    ${req}this.${f.name},');
    }
    buf.writeln('  });');
    buf.writeln();
    // Fields.
    for (final f in fields) {
      buf.writeln('  final ${_dartType(f)} ${f.name};');
    }
    buf.writeln();
    // fromJson.
    buf.writeln('  factory $className.fromJson(Map<String, dynamic> json) =>');
    buf.writeln('      $className(');
    for (final f in fields) {
      buf.writeln('        ${f.name}: ${_fromJson(f)},');
    }
    buf.writeln('      );');
    buf.writeln();
    // toJson.
    buf.writeln('  Map<String, dynamic> toJson() => {');
    for (final f in fields) {
      buf.writeln("        '${f.name}': ${_toJson(f)},");
    }
    buf.writeln('      };');
    buf.writeln('}');
    buf.writeln();
  }
  return buf.toString().trimRight();
}

String _dartType(FieldDef f) {
  final base = _baseDartType(f);
  return f.optional ? '$base?' : base;
}

String _baseDartType(FieldDef f) {
  if (f.isRecord) return 'String'; // record id encoded as `table:id`
  if (f.isDateTime) return 'DateTime';
  final t = f.type.toLowerCase();
  if (t == 'string' || t == 'uuid') return 'String';
  if (t == 'int') return 'int';
  if (t == 'number' || t == 'float' || t == 'decimal') return 'num';
  if (t == 'bool') return 'bool';
  if (t == 'object') return 'Map<String, dynamic>';
  final arr = RegExp(r'^array\s*<\s*(.*)\s*>$').firstMatch(t);
  if (arr != null) {
    final inner = _baseDartTypeForName(arr.group(1)!.trim());
    return 'List<$inner>';
  }
  if (t == 'array') return 'List<dynamic>';
  return 'dynamic';
}

String _baseDartTypeForName(String t) {
  if (t == 'string' || t == 'uuid' || t.startsWith('record')) return 'String';
  if (t == 'int') return 'int';
  if (t == 'number' || t == 'float' || t == 'decimal') return 'num';
  if (t == 'bool') return 'bool';
  if (t == 'datetime') return 'DateTime';
  return 'dynamic';
}

String _fromJson(FieldDef f) {
  final base = _baseDartType(f);
  final access = "json['${f.name}']";
  if (f.isDateTime) {
    return f.optional
        ? '$access == null ? null : DateTime.parse($access as String)'
        : 'DateTime.parse($access as String)';
  }
  if (base.startsWith('List<')) {
    final inner = base.substring(5, base.length - 1);
    return f.optional
        ? '($access as List?)?.cast<$inner>()'
        : '($access as List).cast<$inner>()';
  }
  if (base == 'dynamic') return access;
  return f.optional ? '$access as $base?' : '$access as $base';
}

String _toJson(FieldDef f) {
  if (f.isDateTime) {
    return f.optional
        ? '${f.name}?.toUtc().toIso8601String()'
        : '${f.name}.toUtc().toIso8601String()';
  }
  return f.name;
}

/// PascalCase a (possibly snake_case) table name: `blog_post` -> `BlogPost`.
String _pascal(String name) => name
    .split(RegExp(r'[_\s]+'))
    .where((p) => p.isNotEmpty)
    .map((p) => p[0].toUpperCase() + p.substring(1))
    .join();

String _camel(String name) {
  final p = _pascal(name);
  return p.isEmpty ? p : p[0].toLowerCase() + p.substring(1);
}

/// The `TypedField` constructor for a filterable field, or null for types that
/// can't be type-safely filtered (object/array/dynamic).
String? _fieldTokenCtor(FieldDef f) {
  if (f.isRecord) return "RecordField('${f.name}')";
  if (f.isDateTime) return "DateTimeField('${f.name}')";
  final t = f.type.toLowerCase();
  if (t == 'string' || t == 'uuid') return "StringField('${f.name}')";
  if (t == 'int' || t == 'number' || t == 'float' || t == 'decimal') {
    return "NumField('${f.name}')";
  }
  if (t == 'bool') return "BoolField('${f.name}')";
  return null;
}

/// Emit per-table field tokens: `abstract final class Thread$ { ... }`.
String emitFieldTokens(List<TableDef> tables) {
  final buf = StringBuffer();
  for (final table in tables) {
    final fields = [
      FieldDef(
          name: 'id',
          type: 'string',
          optional: false,
          isRecord: false,
          isDateTime: false),
      ...table.fields.where((f) => f.name != 'id'),
    ];
    buf.writeln('abstract final class ${_pascal(table.name)}\$ {');
    for (final f in fields) {
      final ctor = _fieldTokenCtor(f);
      if (ctor != null) buf.writeln('  static const ${f.name} = $ctor;');
    }
    buf.writeln('}');
    buf.writeln();
  }
  return buf.toString().trimRight();
}

/// Emit an all-nullable `<Table>Patch` for partial updates.
String emitPatches(List<TableDef> tables) {
  final buf = StringBuffer();
  for (final table in tables) {
    final cls = '${_pascal(table.name)}Patch';
    final fields = table.fields.where((f) => f.name != 'id').toList();
    buf.writeln('class $cls {');
    if (fields.isEmpty) {
      buf.writeln('  $cls();');
    } else {
      buf.writeln('  $cls({');
      for (final f in fields) {
        buf.writeln('    this.${f.name},');
      }
      buf.writeln('  });');
    }
    buf.writeln();
    for (final f in fields) {
      buf.writeln('  final ${_baseDartType(f)}? ${f.name};');
    }
    buf.writeln();
    buf.writeln('  Map<String, dynamic> toJson() {');
    buf.writeln('    final m = <String, dynamic>{};');
    for (final f in fields) {
      final value =
          f.isDateTime ? '${f.name}!.toUtc().toIso8601String()' : f.name;
      buf.writeln("    if (${f.name} != null) m['${f.name}'] = $value;");
    }
    buf.writeln('    return m;');
    buf.writeln('  }');
    buf.writeln('}');
    buf.writeln();
  }
  return buf.toString().trimRight();
}

/// Emit a typed collection per table.
String emitCollections(List<TableDef> tables) {
  final buf = StringBuffer();
  for (final table in tables) {
    final cls = _pascal(table.name);
    buf.writeln('class ${cls}Collection {');
    buf.writeln('  ${cls}Collection(this._c);');
    buf.writeln('  final Sp00kyClient _c;');
    buf.writeln();
    buf.writeln('  TypedQuery<$cls> query() =>');
    buf.writeln("      TypedQuery(_c.query('${table.name}'), $cls.fromJson);");
    buf.writeln();
    buf.writeln('  Future<void> create($cls model) =>');
    buf.writeln("      _c.create(model.id, model.toJson()..remove('id'));");
    buf.writeln();
    buf.writeln('  Future<void> update(String id, ${cls}Patch patch) =>');
    buf.writeln("      _c.update('${table.name}', id, patch.toJson());");
    buf.writeln();
    buf.writeln('  Future<void> delete(String id) =>');
    buf.writeln("      _c.delete('${table.name}', id);");
    buf.writeln('}');
    buf.writeln();
  }
  return buf.toString().trimRight();
}

/// Emit typed auth methods per `DEFINE ACCESS` (params typed as String).
String emitAuthApi(List<AccessDef> accesses) {
  if (accesses.isEmpty) return '';
  final buf = StringBuffer('class AuthApi {\n');
  buf.writeln('  AuthApi(this._c);');
  buf.writeln('  final Sp00kyClient _c;');
  for (final a in accesses) {
    final suffix = _pascal(a.name);
    buf.writeln();
    buf.writeln(_authMethod('signIn$suffix', a.name, 'signIn', a.signinParams));
    buf.writeln();
    buf.writeln(_authMethod('signUp$suffix', a.name, 'signUp', a.signupParams));
  }
  buf.writeln('}');
  return buf.toString();
}

String _authMethod(
    String method, String access, String call, List<String> params) {
  final namedParams = params.map((p) => 'required String $p').join(', ');
  final mapEntries = params.map((p) => "'$p': $p").join(', ');
  return '  Future<void> $method({$namedParams}) =>\n'
      "      _c.auth.$call('$access', {$mapEntries});";
}

String _routeMethodName(String path) {
  final parts =
      path.split(RegExp(r'[/_\-\s]+')).where((p) => p.isNotEmpty).toList();
  if (parts.isEmpty) return 'call';
  return parts.first +
      parts.skip(1).map((p) => p[0].toUpperCase() + p.substring(1)).join();
}

/// Emit typed backend route methods.
String emitBackends(List<BackendDef> backends) {
  if (backends.isEmpty) return '';
  final buf = StringBuffer();
  // Aggregator.
  buf.writeln('class Backends {');
  buf.writeln('  Backends(this._c);');
  buf.writeln('  final Sp00kyClient _c;');
  for (final b in backends) {
    buf.writeln('  ${_pascal(b.name)}Backend get ${_camel(b.name)} =>'
        ' ${_pascal(b.name)}Backend(_c);');
  }
  buf.writeln('}');
  buf.writeln();
  // Per-backend classes.
  for (final b in backends) {
    buf.writeln('class ${_pascal(b.name)}Backend {');
    buf.writeln('  ${_pascal(b.name)}Backend(this._c);');
    buf.writeln('  final Sp00kyClient _c;');
    for (final r in b.routes) {
      final method = _routeMethodName(r.path);
      final named = r.args
          .map((a) => '${a.optional ? '' : 'required '}${a.type} ${a.name}')
          .join(', ');
      final entries = r.args.map((a) => "'${a.name}': ${a.name}").join(', ');
      buf.writeln();
      buf.writeln('  Future<void> $method({$named}) =>');
      buf.writeln("      _c.run('${b.name}', '${r.path}', {$entries});");
    }
    buf.writeln('}');
    buf.writeln();
  }
  return buf.toString().trimRight();
}

/// Emit the `AppDb` facade.
String emitAppDb(List<TableDef> tables, List<AccessDef> accesses,
    List<BackendDef> backends) {
  final buf = StringBuffer('class AppDb {\n');
  buf.writeln('  AppDb(this.client);');
  buf.writeln('  final Sp00kyClient client;');
  buf.writeln();
  buf.writeln('  factory AppDb.open(DatabaseConfig database) => AppDb(');
  buf.writeln('        Sp00kyClient(Sp00kyConfig(');
  buf.writeln('          database: database,');
  buf.writeln('          schema: spookySchema,');
  buf.writeln('          schemaSurql: surqlSchema,');
  buf.writeln('        )),');
  buf.writeln('      );');
  buf.writeln();
  buf.writeln('  Future<void> init() => client.init();');
  buf.writeln('  Future<void> close() => client.close();');
  for (final t in tables) {
    final cls = _pascal(t.name);
    buf.writeln('  ${cls}Collection get ${_camel(t.name)} =>'
        ' ${cls}Collection(client);');
  }
  if (backends.isNotEmpty) {
    buf.writeln('  Backends get run => Backends(client);');
  }
  if (accesses.isNotEmpty) {
    buf.writeln('  AuthApi get auth => AuthApi(client);');
  }
  buf.writeln('}');
  return buf.toString();
}

/// Emit a full, analyzable Dart source file: schema map + models + patches +
/// field tokens + collections + auth + backends + the `AppDb` facade.
String generateDartSource(String surql,
    {List<BackendDef> backends = const []}) {
  final parsed = parseProject(surql);
  final tables = parsed.tables;
  final buf = StringBuffer();
  buf.writeln('// GENERATED CODE - DO NOT EDIT BY HAND.');
  buf.writeln('// Generated from a SurrealQL schema by spooky_core codegen.');
  buf.writeln();
  buf.writeln("import 'package:spooky_core/spooky_core.dart';");
  buf.writeln("import 'package:spooky_core/typed.dart';");
  buf.writeln();
  buf.writeln(emitSchemaMap(tables, parsed.accesses));
  buf.writeln();
  // The embedded schema defines the CLIENT's local cache tables, so a
  // server-only field must not appear in it: the local table would carry a
  // column the client never receives a value for. `cleanRecord`'s Dart
  // equivalent filters on the generated schema map, which already omits the
  // field, so the column could only ever be null.
  buf.writeln('const surqlSchema = r\'\'\'\n${stripServerOnlyFields(surql)}\'\'\';');
  buf.writeln();
  buf.writeln(emitModels(tables));
  buf.writeln();
  buf.writeln(emitPatches(tables));
  buf.writeln();
  buf.writeln(emitFieldTokens(tables));
  buf.writeln();
  buf.writeln(emitCollections(tables));
  final auth = emitAuthApi(parsed.accesses);
  if (auth.isNotEmpty) {
    buf.writeln();
    buf.writeln(auth);
  }
  final be = emitBackends(backends);
  if (be.isNotEmpty) {
    buf.writeln();
    buf.writeln(be);
  }
  buf.writeln();
  buf.writeln(emitAppDb(tables, parsed.accesses, backends));
  buf.writeln();
  return buf.toString();
}
