import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'ssp_bindings.dart';
import 'stream_update.dart';

/// Dart handle to a native `ssp` circuit, bound via the `ssp-ffi` C ABI.
///
/// Mirrors the JS `Sp00kyProcessor` surface that `StreamProcessorService`
/// consumes. All data crosses the boundary as JSON strings; results arrive as
/// an `{"ok": ...}` / `{"err": ...}` envelope which [_decode] unwraps.
class StreamProcessor implements Finalizable {
  StreamProcessor._(this._b, this._ptr) {
    _finalizer.attach(this, _ptr.cast(), detach: this);
  }

  /// Open the native library and create a fresh processor.
  factory StreamProcessor.create([SspBindings? bindings]) {
    final b = bindings ?? SspBindings.open();
    final ptr = b.sspNew();
    if (ptr == nullptr) {
      throw SspException('ssp_new returned null (native init failed)');
    }
    return StreamProcessor._(b, ptr);
  }

  final SspBindings _b;
  Pointer<Void> _ptr;
  bool _disposed = false;

  late final NativeFinalizer _finalizer = NativeFinalizer(_b.library
      .lookup<NativeFunction<Void Function(Pointer<Void>)>>('ssp_free'));

  /// Ingest one record change, returning any affected view updates.
  List<StreamUpdate> ingest(
      String table, String op, String id, Map<String, dynamic> record) {
    final t = table.toNativeUtf8();
    final o = op.toNativeUtf8();
    final i = id.toNativeUtf8();
    final r = jsonEncode(record).toNativeUtf8();
    try {
      final decoded = _decode(_b.sspIngest(_ptr, t, o, i, r)) as List<dynamic>;
      return decoded
          .map((e) => StreamUpdate.fromWasm(e as Map<String, dynamic>, op: op))
          .toList();
    } finally {
      calloc.free(t);
      calloc.free(o);
      calloc.free(i);
      calloc.free(r);
    }
  }

  /// Ingest many record changes as ONE circuit step, returning the coalesced
  /// updates for the whole batch. Changes are applied in order, so repeated ids
  /// inside one batch settle last-write-wins.
  List<StreamUpdate> ingestMany(List<Map<String, dynamic>> items) {
    if (items.isEmpty) return const [];
    final payload = jsonEncode(items).toNativeUtf8();
    try {
      final decoded = _decode(_b.sspIngestMany(_ptr, payload)) as List<dynamic>;
      return [
        for (final u in decoded)
          StreamUpdate.fromWasm(u as Map<String, dynamic>)
      ];
    } finally {
      calloc.free(payload);
    }
  }

  /// Register a materialized view, returning its initial snapshot.
  Registration? registerView(Map<String, dynamic> config) {
    final c = jsonEncode(config).toNativeUtf8();
    try {
      final decoded = _decode(_b.sspRegisterView(_ptr, c));
      if (decoded == null) return null;
      final map = decoded as Map<String, dynamic>;
      final missing = map['missing_fields'];
      return Registration(
        update: StreamUpdate.fromWasm(map),
        missingFields: missing is Map
            ? {
                for (final e in missing.entries)
                  e.key.toString(): (e.value as List).cast<String>()
              }
            : const {},
      );
    } finally {
      calloc.free(c);
    }
  }

  /// Seed a table's `PERMISSIONS FOR select WHERE <expr>` text on the circuit.
  ///
  /// Required before [registerView] for a real table, since the circuit is
  /// default-deny. Seed from the schema during init (mirrors the SSP server).
  void setPermission(String table, String whereText) {
    final t = table.toNativeUtf8();
    final w = whereText.toNativeUtf8();
    try {
      _decode(_b.sspSetPermission(_ptr, t, w));
    } finally {
      calloc.free(t);
      calloc.free(w);
    }
  }

  /// Unregister a view by id.
  void unregisterView(String id) {
    final i = id.toNativeUtf8();
    try {
      _decode(_b.sspUnregisterView(_ptr, i));
    } finally {
      calloc.free(i);
    }
  }

  /// Serialize the current circuit state to a JSON string.
  String saveState() => _decode(_b.sspSaveState(_ptr)) as String;

  /// Restore circuit state from a JSON string.
  void loadState(String state) {
    final s = state.toNativeUtf8();
    try {
      _decode(_b.sspLoadState(_ptr, s));
    } finally {
      calloc.free(s);
    }
  }

  /// Compare one table against the caller's authoritative `(id, rv)` list.
  /// Rows the store holds but the list lacks are stepped out; ids the store
  /// lacks or holds stale come back in [Reconciled.fetch] to be ingested.
  Reconciled reconcile(String table, List<(String, int)> entries) {
    final t = table.toNativeUtf8();
    final e = jsonEncode([
      for (final (id, rv) in entries) [id, rv]
    ]).toNativeUtf8();
    try {
      final decoded =
          _decode(_b.sspReconcile(_ptr, t, e)) as Map<String, dynamic>;
      return Reconciled(
        fetch: (decoded['fetch'] as List).cast<String>(),
        deleted: (decoded['deleted'] as num).toInt(),
        updates: [
          for (final u in decoded['updates'] as List)
            StreamUpdate.fromWasm(u as Map<String, dynamic>)
        ],
      );
    } finally {
      calloc.free(t);
      calloc.free(e);
    }
  }

  /// Highest `_00_rv` folded into each table.
  Map<String, int> maxRowVersions() {
    final decoded = _decode(_b.sspMaxRowVersions(_ptr)) as Map<String, dynamic>;
    return {
      for (final e in decoded.entries) e.key: (e.value as num).toInt()
    };
  }

  /// Keep only the fields registered plans evaluate per stored row. Off by
  /// default; must be set before the first ingest to take effect on those rows.
  void setProjection(bool enabled) =>
      _decode(_b.sspSetProjection(_ptr, enabled));

  /// Snapshot the base collections as bytes. Views are deliberately left out:
  /// the client re-registers every query under a fresh session id on boot.
  Uint8List saveStoreState() {
    final outPtr = calloc<Pointer<Uint8>>();
    final outLen = calloc<Size>();
    try {
      _decode(_b.sspSaveStoreState(_ptr, outPtr, outLen));
      final buf = outPtr.value;
      final len = outLen.value;
      if (buf == nullptr || len == 0) return Uint8List(0);
      try {
        // Copy before freeing: the bytes are Rust-owned until `ssp_bytes_free`.
        return Uint8List.fromList(buf.asTypedList(len));
      } finally {
        _b.sspBytesFree(buf, len);
      }
    } finally {
      calloc.free(outPtr);
      calloc.free(outLen);
    }
  }

  /// Install a snapshot written by [saveStoreState] UNDER the views already
  /// registered. Every registered view is re-primed against the restored rows,
  /// so a query that registered against the empty pre-snapshot store catches up.
  List<StreamUpdate> loadStoreState(Uint8List bytes) {
    if (bytes.isEmpty) return const [];
    final buf = calloc<Uint8>(bytes.length);
    try {
      buf.asTypedList(bytes.length).setAll(0, bytes);
      final decoded =
          _decode(_b.sspLoadStoreState(_ptr, buf, bytes.length)) as List<dynamic>;
      return [
        for (final u in decoded)
          StreamUpdate.fromWasm(u as Map<String, dynamic>)
      ];
    } finally {
      calloc.free(buf);
    }
  }

  /// Free the native processor. Safe to call multiple times.
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _finalizer.detach(this);
    _b.sspFree(_ptr);
    _ptr = nullptr;
  }

  /// Copy out the Rust-owned result string, free it, and unwrap the envelope.
  dynamic _decode(Pointer<Utf8> ret) {
    if (ret == nullptr) {
      throw SspException('native call returned null pointer');
    }
    String jsonStr;
    try {
      jsonStr = ret.toDartString();
    } finally {
      _b.sspStringFree(ret);
    }
    final env = jsonDecode(jsonStr) as Map<String, dynamic>;
    if (env.containsKey('err')) {
      throw SspException(env['err'] as String);
    }
    return env['ok'];
  }
}
