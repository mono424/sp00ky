//! SurrealQL schema -> Rust types, for build scripts.
//!
//! PORT: `packages/spooky_core/lib/src/codegen/{schema_parser,openapi_parser}.dart`,
//! with a Rust emitter in place of the Dart one. Use it from a `build.rs`:
//!
//! ```ignore
//! // build.rs
//! fn main() {
//!     let surql = std::fs::read_to_string("schema.surql").unwrap();
//!     let api = std::fs::read_to_string("api.openapi.yaml").unwrap();
//!     let backends = [spooky_core::codegen::parse_open_api("api", &api, Some("job")).unwrap()];
//!     let code = spooky_core::codegen::generate_rust(&surql, &backends).unwrap();
//!     let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("db.rs");
//!     std::fs::write(out, code).unwrap();
//!     println!("cargo:rerun-if-changed=schema.surql");
//! }
//!
//! // src/db.rs
//! include!(concat!(env!("OUT_DIR"), "/db.rs"));
//! ```
//!
//! The generated file needs only `spooky_core` (default features off is fine).

mod openapi;
mod rust_emitter;
mod schema_parser;

pub use openapi::{parse_open_api, ArgKind, OpenApiArg, OpenApiBackend, OpenApiRoute};
pub use rust_emitter::generate_rust;
pub use schema_parser::{
    derive_relationships, parse_accesses, parse_project, parse_schema, strip_server_only_fields,
    ParsedAccess, ParsedField, ParsedSchema, ParsedTable,
};
