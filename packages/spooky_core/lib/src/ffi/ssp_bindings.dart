import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'ssp_library.dart';

// Native C signatures (see packages/ssp-ffi/src/lib.rs).
typedef _NewNative = Pointer<Void> Function();
typedef _FreeNative = Void Function(Pointer<Void>);
typedef _FreeDart = void Function(Pointer<Void>);
typedef _StringFreeNative = Void Function(Pointer<Utf8>);
typedef _StringFreeDart = void Function(Pointer<Utf8>);

typedef _IngestNative = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>);
typedef _IngestDart = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>);

typedef _OnePtrNative = Pointer<Utf8> Function(Pointer<Void>, Pointer<Utf8>);
typedef _OnePtrDart = Pointer<Utf8> Function(Pointer<Void>, Pointer<Utf8>);

typedef _SaveNative = Pointer<Utf8> Function(Pointer<Void>);
typedef _SaveDart = Pointer<Utf8> Function(Pointer<Void>);

typedef _TwoPtrNative = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>);
typedef _TwoPtrDart = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>);

typedef _SetProjectionNative = Pointer<Utf8> Function(Pointer<Void>, Bool);
typedef _SetProjectionDart = Pointer<Utf8> Function(Pointer<Void>, bool);

// The store snapshot is megabytes on a warm client, so it does not travel
// through the JSON envelope: the envelope carries only the outcome and the
// buffer comes back through out-parameters, to be released with
// `ssp_bytes_free` once copied.
typedef _SaveStoreStateNative = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Pointer<Uint8>>, Pointer<Size>);
typedef _SaveStoreStateDart = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Pointer<Uint8>>, Pointer<Size>);

typedef _LoadStoreStateNative = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Uint8>, Size);
typedef _LoadStoreStateDart = Pointer<Utf8> Function(
    Pointer<Void>, Pointer<Uint8>, int);

typedef _BytesFreeNative = Void Function(Pointer<Uint8>, Size);
typedef _BytesFreeDart = void Function(Pointer<Uint8>, int);

/// Thin lookup wrapper around the `ssp-ffi` C ABI.
class SspBindings {
  SspBindings(this._lib)
      : sspNew = _lib.lookupFunction<_NewNative, _NewNative>('ssp_new'),
        sspFree = _lib.lookupFunction<_FreeNative, _FreeDart>('ssp_free'),
        sspStringFree = _lib.lookupFunction<_StringFreeNative, _StringFreeDart>(
            'ssp_string_free'),
        sspIngest =
            _lib.lookupFunction<_IngestNative, _IngestDart>('ssp_ingest'),
        sspRegisterView = _lib
            .lookupFunction<_OnePtrNative, _OnePtrDart>('ssp_register_view'),
        sspUnregisterView = _lib
            .lookupFunction<_OnePtrNative, _OnePtrDart>('ssp_unregister_view'),
        sspSaveState =
            _lib.lookupFunction<_SaveNative, _SaveDart>('ssp_save_state'),
        sspLoadState =
            _lib.lookupFunction<_OnePtrNative, _OnePtrDart>('ssp_load_state'),
        sspSetPermission = _lib
            .lookupFunction<_TwoPtrNative, _TwoPtrDart>('ssp_set_permission'),
        sspIngestMany =
            _lib.lookupFunction<_OnePtrNative, _OnePtrDart>('ssp_ingest_many'),
        sspReconcile =
            _lib.lookupFunction<_TwoPtrNative, _TwoPtrDart>('ssp_reconcile'),
        sspMaxRowVersions = _lib
            .lookupFunction<_SaveNative, _SaveDart>('ssp_max_row_versions'),
        sspSetProjection =
            _lib.lookupFunction<_SetProjectionNative, _SetProjectionDart>(
                'ssp_set_projection'),
        sspSaveStoreState =
            _lib.lookupFunction<_SaveStoreStateNative, _SaveStoreStateDart>(
                'ssp_save_store_state'),
        sspLoadStoreState =
            _lib.lookupFunction<_LoadStoreStateNative, _LoadStoreStateDart>(
                'ssp_load_store_state'),
        sspBytesFree = _lib
            .lookupFunction<_BytesFreeNative, _BytesFreeDart>('ssp_bytes_free');

  factory SspBindings.open() => SspBindings(openSspLibrary());

  final DynamicLibrary _lib;

  /// The underlying library handle, used to attach a [NativeFinalizer].
  DynamicLibrary get library => _lib;

  final Pointer<Void> Function() sspNew;
  final _FreeDart sspFree;
  final _StringFreeDart sspStringFree;
  final _IngestDart sspIngest;
  final _OnePtrDart sspRegisterView;
  final _OnePtrDart sspUnregisterView;
  final _SaveDart sspSaveState;
  final _OnePtrDart sspLoadState;
  final _TwoPtrDart sspSetPermission;
  final _OnePtrDart sspIngestMany;
  final _TwoPtrDart sspReconcile;
  final _SaveDart sspMaxRowVersions;
  final _SetProjectionDart sspSetProjection;
  final _SaveStoreStateDart sspSaveStoreState;
  final _LoadStoreStateDart sspLoadStoreState;
  final _BytesFreeDart sspBytesFree;
}
