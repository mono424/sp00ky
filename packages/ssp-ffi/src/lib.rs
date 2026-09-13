//! C ABI for the `ssp` stream processor, for use from Dart FFI.
//!
//! Mirrors the surface that `ssp-wasm` exposes to JavaScript, but instead of
//! `wasm-bindgen` it passes UTF-8 JSON across a C boundary. Each fallible call
//! returns a freshly-allocated, NUL-terminated C string holding a JSON
//! envelope: `{"ok": <data>}` on success or `{"err": "<message>"}` on failure.
//!
//! ## Memory ownership
//! - Every `*mut c_char` returned by an `ssp_*` function is owned by this
//!   library's allocator. The caller MUST return it via [`ssp_string_free`]
//!   after copying out the bytes.
//! - Every `*const c_char` argument is owned by the caller; this library only
//!   borrows it for the duration of the call.
//! - The `*mut Processor` handle is owned by the caller and MUST be released
//!   with [`ssp_free`]. It is never freed by any other function.

mod processor;

use processor::Processor;
use serde_json::Value;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Borrow a `*const c_char` as `&str`, erroring on null / invalid UTF-8.
///
/// # Safety
/// `p` must be null or a valid NUL-terminated C string that outlives the call.
unsafe fn cstr<'a>(p: *const c_char) -> anyhow::Result<&'a str> {
    if p.is_null() {
        anyhow::bail!("null pointer argument");
    }
    Ok(CStr::from_ptr(p).to_str()?)
}

/// Allocate a JSON envelope C string. Never panics on encoding (the envelope
/// is always valid JSON without interior NULs).
fn envelope(value: Value) -> *mut c_char {
    let s = value.to_string();
    // A serde_json string never contains an interior NUL byte.
    CString::new(s)
        .unwrap_or_else(|_| CString::new("{\"err\":\"interior nul in result\"}").unwrap())
        .into_raw()
}

/// Run `f`, catching panics, and serialize the outcome into a JSON envelope.
fn ffi_call(f: impl FnOnce() -> anyhow::Result<Value>) -> *mut c_char {
    let result = catch_unwind(AssertUnwindSafe(f));
    let value = match result {
        Ok(Ok(v)) => serde_json::json!({ "ok": v }),
        Ok(Err(e)) => serde_json::json!({ "err": e.to_string() }),
        Err(_) => serde_json::json!({ "err": "panic in ssp-ffi" }),
    };
    envelope(value)
}

/// Create a new processor. Caller must eventually call [`ssp_free`].
#[no_mangle]
pub extern "C" fn ssp_new() -> *mut Processor {
    match catch_unwind(|| Box::into_raw(Box::new(Processor::new()))) {
        Ok(ptr) => ptr,
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free a processor created by [`ssp_new`].
///
/// # Safety
/// `ptr` must be null or a pointer returned by [`ssp_new`] that has not already
/// been freed.
#[no_mangle]
pub unsafe extern "C" fn ssp_free(ptr: *mut Processor) {
    if ptr.is_null() {
        return;
    }
    drop(Box::from_raw(ptr));
}

/// Free a string returned by any `ssp_*` function.
///
/// # Safety
/// `s` must be null or a pointer returned by this library that has not already
/// been freed.
#[no_mangle]
pub unsafe extern "C" fn ssp_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    drop(CString::from_raw(s));
}

/// Ingest one record change. `record_json` is the JSON object for the record.
/// Returns `{"ok":[WasmViewUpdate,...]}` or `{"err":"..."}`.
///
/// # Safety
/// `ptr` must be a valid processor handle; the string args must be valid
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn ssp_ingest(
    ptr: *mut Processor,
    table: *const c_char,
    op: *const c_char,
    id: *const c_char,
    record_json: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        let table = cstr(table)?;
        let op = cstr(op)?;
        let id = cstr(id)?;
        let record: Value = serde_json::from_str(cstr(record_json)?)?;
        let updates = p.ingest(table, op, id, record)?;
        Ok(serde_json::to_value(updates)?)
    })
}

/// Register a materialized view from a JSON config.
/// Returns `{"ok":WasmViewUpdate}` or `{"err":"..."}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_register_view(
    ptr: *mut Processor,
    config_json: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        let config: Value = serde_json::from_str(cstr(config_json)?)?;
        Ok(serde_json::to_value(p.register_view(config)?)?)
    })
}

/// Unregister a view by id. Returns `{"ok":null}` or `{"err":"..."}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_unregister_view(
    ptr: *mut Processor,
    id: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        p.unregister_view(cstr(id)?);
        Ok(Value::Null)
    })
}

/// Register a table's `PERMISSIONS FOR select WHERE <expr>` text on the
/// circuit. Returns `{"ok":null}` or `{"err":"..."}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_set_permission(
    ptr: *mut Processor,
    table: *const c_char,
    where_text: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        p.set_permission(cstr(table)?, cstr(where_text)?);
        Ok(Value::Null)
    })
}

/// Save circuit state. Returns `{"ok":"<state json>"}` or `{"err":"..."}`.
///
/// # Safety
/// `ptr` must be a valid processor handle.
#[no_mangle]
pub unsafe extern "C" fn ssp_save_state(ptr: *const Processor) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &*ptr;
        Ok(Value::String(p.save_state()?))
    })
}

/// Load circuit state. Returns `{"ok":null}` or `{"err":"..."}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_load_state(
    ptr: *mut Processor,
    state: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        p.load_state(cstr(state)?)?;
        Ok(Value::Null)
    })
}

/// Ingest many record changes as ONE circuit step. `items_json` is a JSON array
/// of `{table, op, id, record}`. Returns `{"ok":[WasmViewUpdate,...]}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_ingest_many(
    ptr: *mut Processor,
    items_json: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        let items: Vec<processor::IngestItem> = serde_json::from_str(cstr(items_json)?)?;
        Ok(serde_json::to_value(p.ingest_many(items)?)?)
    })
}

/// Compare one table against the caller's authoritative `[[id, rv], ...]` list.
/// Returns `{"ok":{fetch, deleted, updates}}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_reconcile(
    ptr: *mut Processor,
    table: *const c_char,
    entries_json: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        let table = cstr(table)?;
        let entries: Vec<(String, i64)> = serde_json::from_str(cstr(entries_json)?)?;
        Ok(serde_json::to_value(p.reconcile(table, &entries))?)
    })
}

/// Highest `_00_rv` folded into each table. Returns `{"ok":{table: rv, ...}}`.
///
/// # Safety
/// `ptr` must be a valid processor handle.
#[no_mangle]
pub unsafe extern "C" fn ssp_max_row_versions(ptr: *const Processor) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &*ptr;
        Ok(serde_json::to_value(p.max_row_versions())?)
    })
}

/// Keep only the fields registered plans evaluate per stored row.
/// Returns `{"ok":null}` or `{"err":"..."}`.
///
/// # Safety
/// See [`ssp_ingest`].
#[no_mangle]
pub unsafe extern "C" fn ssp_set_projection(ptr: *mut Processor, enabled: bool) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        let p = &mut *ptr;
        p.set_projection(enabled);
        Ok(Value::Null)
    })
}

/// Snapshot the base collections as raw bytes.
///
/// The snapshot is megabytes on a warm client, so it does NOT travel through
/// the JSON envelope: on success `*out_ptr`/`*out_len` describe a buffer this
/// library owns, which the caller MUST release with [`ssp_bytes_free`] after
/// copying it out. The returned envelope carries only the outcome
/// (`{"ok":null}` or `{"err":"..."}`); on error the out-params are left as a
/// null pointer and a zero length.
///
/// # Safety
/// `ptr` must be a valid processor handle; `out_ptr` and `out_len` must be
/// valid, writable pointers.
#[no_mangle]
pub unsafe extern "C" fn ssp_save_store_state(
    ptr: *const Processor,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        if out_ptr.is_null() || out_len.is_null() {
            anyhow::bail!("null out parameter");
        }
        *out_ptr = std::ptr::null_mut();
        *out_len = 0;
        let p = &*ptr;
        let mut bytes = p.save_store_state()?.into_boxed_slice();
        *out_len = bytes.len();
        *out_ptr = bytes.as_mut_ptr();
        // The buffer is now owned by the caller until `ssp_bytes_free`.
        std::mem::forget(bytes);
        Ok(Value::Null)
    })
}

/// Release a buffer handed out by [`ssp_save_store_state`].
///
/// # Safety
/// `ptr`/`len` must be exactly what a single [`ssp_save_store_state`] call
/// wrote, and must not have been freed already.
#[no_mangle]
pub unsafe extern "C" fn ssp_bytes_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    drop(Box::from_raw(std::slice::from_raw_parts_mut(ptr, len)));
}

/// Install a snapshot written by [`ssp_save_store_state`] under the views that
/// are already registered. Returns `{"ok":[WasmViewUpdate,...]}` with each
/// view's new full result.
///
/// # Safety
/// `ptr` must be a valid processor handle; `bytes`/`len` must describe a
/// readable buffer owned by the caller for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn ssp_load_store_state(
    ptr: *mut Processor,
    bytes: *const u8,
    len: usize,
) -> *mut c_char {
    ffi_call(|| {
        if ptr.is_null() {
            anyhow::bail!("null processor handle");
        }
        if bytes.is_null() {
            anyhow::bail!("null snapshot pointer");
        }
        let p = &mut *ptr;
        let slice = std::slice::from_raw_parts(bytes, len);
        Ok(serde_json::to_value(p.load_store_state(slice)?)?)
    })
}
