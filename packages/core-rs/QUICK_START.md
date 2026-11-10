# spooky-core (Rust): Quick Start

The native Rust client for sp00ky local-first sync. Full guide: the docs
site's Rust guide (`/docs/guide/rust`); API surface for coding agents:
`AGENTS.md`.

## Install

Not on crates.io yet. Add it from git (pin a `rev` for reproducible builds),
for the app and for its build script:

```toml
[dependencies]
spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
# Only for the example's #[tokio::main]; the client needs no runtime of yours.
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

[build-dependencies]
spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }
```

Rust 1.85 or newer.

## Generate types from the schema

```rust
// build.rs
fn main() {
    let surql = std::fs::read_to_string("schema/src/schema.surql").unwrap();
    let code = spooky_core::codegen::generate_rust(&surql, &[]).unwrap();
    let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("db.rs");
    std::fs::write(out, code).unwrap();
    println!("cargo:rerun-if-changed=schema/src/schema.surql");
}
```

```rust
// src/db.rs
include!(concat!(env!("OUT_DIR"), "/db.rs"));
```

This gives you `db::config(..)`, a serde struct and a `Patch` per table, typed
field tokens and the `AppDb` facade.

## Open, sign in, query, write

The schema here has `DEFINE ACCESS account` (username + password) and a `note`
table (`owner: record<user>`, `title: string`, `body: option<string>`,
`rank: int`).

```rust
use serde_json::json;
use spooky_core::value::generate_id;
use spooky_core::{DatabaseConfig, Sp00kyClient, StoreType};

mod db;

#[tokio::main]
async fn main() -> spooky_core::Result<()> {
    let client = Sp00kyClient::open(db::config(
        DatabaseConfig::new("main", "app")
            .endpoint("ws://127.0.0.1:8666/rpc") // spky dev; leave it out for a local-only client
            .store(StoreType::File("spooky.db".into())),
    ))
    .await?;

    client
        .sign_in("account", json!({"username": "ada", "password": "correct horse"}))
        .await?;
    let me = client.user_id().expect("signed in");

    // A live query: paints from the local store, then stays current.
    let mut notes = client
        .live(client.query("note").where_eq("owner", me.as_str()).order_by("rank", "desc"))
        .await?;

    // An optimistic write: visible at once, synced in the background.
    let id = format!("note:{}", generate_id());
    client.create(&id, json!({"owner": me, "title": "Hello", "rank": 1})).await?;

    // Wait until it shows in the live query (changed() resolves on every update).
    while !notes.rows().iter().any(|r| r.get("id").map(|v| v.to_id_string()) == Some(id.clone())) {
        notes.changed().await?;
    }
    let typed: Vec<db::Note> = notes.rows_as()?;
    println!("{} notes, top: {:?}", typed.len(), typed.first().map(|n| &n.title));

    // Write the circuit snapshot and close the stores before exiting.
    client.close().await
}
```

`Sp00kyClient` runs its engine on its own thread, so the same code works from
GPUI, smol or `futures::executor::block_on`; `tokio` here is only the app's
choice of runtime.

## Next

- Debounced updates, the failed-write tray, sync indicators, buckets, feature
  flags, backend jobs, channels and push: the Rust guide on the docs site.
- `AGENTS.md`: the full public API and the gotchas.
- `tests/client_local.rs`: a local-only client in a test.
