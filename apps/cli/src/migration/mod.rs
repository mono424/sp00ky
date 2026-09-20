pub mod engine;
mod legacy;
pub(crate) mod sp00ky_engine;
pub(crate) mod surrealkit;

use anyhow::Result;
use std::path::PathBuf;

pub use engine::{MigrationEngine, MigrationEnvironment, MigrationInfo, MigrationState};
pub use sp00ky_engine::{InternalSchemaConfig, RemoteFunctionsConfig};

/// All parameters needed to construct a migration engine.
pub struct MigrationContext {
    pub environment: MigrationEnvironment,
    pub project_dir: PathBuf,
    pub migrations_dir: PathBuf,
    // Connection params
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: String,
    pub password: String,
    // surrealkit-specific (None = use legacy engine)
    pub surrealkit_binary: Option<String>,
    // Post-migration steps (optional, set by caller based on context)
    pub internal_schema: Option<InternalSchemaConfig>,
    pub remote_functions: Option<RemoteFunctionsConfig>,
    // Vault secrets for `{{KEY}}` substitution in user migrations at apply time.
    // None (or empty) = apply migrations verbatim (historical behavior). Only the
    // legacy engine consumes these; see migrate::apply_with_secrets.
    pub secrets: Option<Vec<(String, String)>>,
}

/// Factory: select the inner engine based on config, then wrap with Sp00kyEngine decorator.
///
/// The decorator handles internal schema + remote functions after `apply()`.
/// When both `internal_schema` and `remote_functions` are `None`, the decorator
/// delegates straight through with no overhead.
pub fn create_engine(ctx: MigrationContext) -> Result<Box<dyn MigrationEngine>> {
    let has_secrets = ctx.secrets.as_ref().is_some_and(|s| !s.is_empty());
    let inner: Box<dyn MigrationEngine> = if let Some(ref binary) = ctx.surrealkit_binary {
        if has_secrets {
            eprintln!(
                "WARNING: {{{{KEY}}}} placeholder substitution is not supported by the \
                 surrealkit engine; vault secrets will NOT be injected into migrations."
            );
        }
        Box::new(surrealkit::SurrealKitEngine::new(
            binary.clone(),
            ctx.environment,
            ctx.project_dir.clone(),
            ctx.url.clone(),
            ctx.namespace.clone(),
            ctx.database.clone(),
            ctx.username.clone(),
            ctx.password.clone(),
        )?)
    } else {
        Box::new(legacy::LegacyEngine::new(
            ctx.url.clone(),
            ctx.namespace.clone(),
            ctx.database.clone(),
            ctx.username.clone(),
            ctx.password.clone(),
            ctx.migrations_dir.clone(),
            ctx.secrets.clone(),
        )
        .with_sync_identity(crate::migrate::SyncIdentity::discover(&ctx.project_dir)))
    };

    Ok(Box::new(sp00ky_engine::Sp00kyEngine::new(
        inner,
        ctx.url,
        ctx.namespace,
        ctx.database,
        ctx.username,
        ctx.password,
        ctx.internal_schema,
        ctx.remote_functions,
    )))
}
