//! The generated files `tests/codegen_generated.rs` compiles are exactly what
//! `generate_rust` emits today. Refresh them with
//! `SPOOKY_UPDATE_GOLDENS=1 cargo test -p spooky-core --test codegen_goldens`.

#![cfg(feature = "codegen")]

use std::path::PathBuf;

use spooky_core::codegen::{generate_rust, parse_open_api, OpenApiBackend};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn read(name: &str) -> String {
    std::fs::read_to_string(fixture(name)).unwrap()
}

fn check(golden: &str, surql: &str, backends: &[OpenApiBackend]) {
    let code = generate_rust(&read(surql), backends).unwrap();
    let path = fixture(golden);
    if std::env::var_os("SPOOKY_UPDATE_GOLDENS").is_some() {
        std::fs::write(&path, &code).unwrap();
        return;
    }
    let current = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        current == code,
        "{golden} is stale: run SPOOKY_UPDATE_GOLDENS=1 cargo test -p spooky-core --test codegen_goldens"
    );
    syn::parse_file(&code).unwrap();
}

#[test]
fn app_db_golden() {
    check("app_db.rs", "app_db.surql", &[]);
}

#[test]
fn rich_golden() {
    let backends = [
        parse_open_api("api", &read("rich_api.yaml"), Some("job_outbox")).unwrap(),
        parse_open_api("media-worker", &read("rich_media.yaml"), Some("media_job")).unwrap(),
    ];
    check("rich.rs", "rich.surql", &backends);
}
