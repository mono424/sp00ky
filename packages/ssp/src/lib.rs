// src/lib.rs

#[cfg(all(not(target_arch = "wasm32"), feature = "mimalloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod allowlist;
pub mod converter;
pub mod merge_key;
pub mod permission_inject;
pub mod sanitizer;
pub mod service;
pub mod size;

// DBSP-theoretic module structure
pub mod algebra;
pub mod types;
pub mod operator;
pub mod circuit;
pub mod eval;

#[cfg(all(feature = "parallel", not(target_arch = "wasm32")))]
pub use rayon::prelude::*;

/// The canonical in-circuit key for a registered query.
///
/// One registration reaches the SSP by two routes that spell its id
/// differently. A live client's id travels through `fn::query::register` as
/// `<string>$config.id`, and SurrealDB stringifies a record id WITH its table,
/// so the SSP receives `_00_query:<hash>`. Every DB-derived path — boot
/// re-registration from `_00_query`, the TTL sweep, an unregister — carries the
/// bare `<hash>` instead.
///
/// Keying the circuit on whichever string arrived therefore filed one query
/// under two keys, with two consequences that both read as data misbehaving:
///
/// 1. After a restart without a snapshot, `rebuild_from_db` registered the view
///    under `<hash>`; the client then reconnected under `_00_query:<hash>`,
///    missed it, and built a SECOND graph over the same rows. Both landed in
///    `dependency_map`, so every ingest stepped both and each wrote the same
///    edges to the same row.
/// 2. The TTL sweep passes the bare `<hash>`, so it never removed the in-memory
///    view of anything a live client had registered. The row and its edges went
///    away while the graph (the ~45 MB) stayed resident forever — and a later
///    re-registration found that orphan, took the "already exists" path, and so
///    never republished the membership the sweep had just deleted.
///
/// Normalising to the key alone is safe: a `_00_query` key is a hex hash and
/// contains no `:` of its own, so this is idempotent on either spelling.
pub fn canonical_query_id(id: &str) -> String {
    id.rsplit(':').next().unwrap_or(id).to_string()
}
