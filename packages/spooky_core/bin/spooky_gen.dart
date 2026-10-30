import 'dart:io';

import 'package:spooky_core/codegen.dart';

/// CLI: generate a typed Dart client (models, field tokens, collections, auth,
/// backends, and an `AppDb` facade) from a SurrealQL schema.
///
/// Usage:
///   dart run spooky_core:spooky_gen <schema.surql> \
///       [--openapi <name>=<spec.yml> ...] [-o <out.dart>]
///
/// Each `--openapi name=path` adds a typed backend reachable via `run(name, …)`.
/// Writes to stdout when no `-o` is given.
void main(List<String> args) {
  if (args.isEmpty) {
    stderr.writeln('Usage: dart run spooky_core:spooky_gen <schema.surql> '
        '[--openapi <name>=<spec.yml> ...] [-o <out.dart>]');
    exitCode = 64; // EX_USAGE
    return;
  }

  String? schemaPath;
  String? outPath;
  final openapi = <String, String>{}; // backend name -> spec path

  for (var i = 0; i < args.length; i++) {
    final arg = args[i];
    if (arg == '-o' || arg == '--out') {
      outPath = args[++i];
    } else if (arg == '--openapi') {
      final spec = args[++i];
      final eq = spec.indexOf('=');
      if (eq < 0) {
        stderr.writeln('--openapi expects <name>=<path>, got: $spec');
        exitCode = 64;
        return;
      }
      openapi[spec.substring(0, eq)] = spec.substring(eq + 1);
    } else if (!arg.startsWith('-')) {
      schemaPath ??= arg;
    }
  }

  if (schemaPath == null) {
    stderr.writeln('Missing <schema.surql>');
    exitCode = 64;
    return;
  }
  final schemaFile = File(schemaPath);
  if (!schemaFile.existsSync()) {
    stderr.writeln('Schema file not found: $schemaPath');
    exitCode = 66; // EX_NOINPUT
    return;
  }

  final backends = <BackendDef>[];
  for (final entry in openapi.entries) {
    final specFile = File(entry.value);
    if (!specFile.existsSync()) {
      stderr.writeln('OpenAPI spec not found: ${entry.value}');
      exitCode = 66;
      return;
    }
    backends.add(parseOpenApi(entry.key, specFile.readAsStringSync()));
  }

  final String source;
  try {
    source =
        generateDartSource(schemaFile.readAsStringSync(), backends: backends);
  } on FormatException catch (e) {
    stderr.writeln('Invalid schema: ${e.message}');
    exitCode = 65; // EX_DATAERR
    return;
  }

  if (outPath != null) {
    File(outPath).writeAsStringSync(source);
    stdout.writeln('Wrote $outPath');
  } else {
    stdout.write(source);
  }
}
