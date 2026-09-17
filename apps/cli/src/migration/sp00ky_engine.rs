use anyhow::{Context, Result};
use std::path::PathBuf;

use crate::backend::DeployMode;
use crate::surreal_client::SurrealClient;

use super::engine::{MigrationEngine, MigrationInfo};

/// Configuration for applying internal Sp00ky schema after user migrations.
pub struct InternalSchemaConfig {
    pub schema_path: PathBuf,
    pub config_path: Option<PathBuf>,
    pub deploy_mode: DeployMode,
    pub endpoint: Option<String>,
    pub secret: Option<String>,
}

/// Configuration for applying remote functions after user migrations.
pub struct RemoteFunctionsConfig {
    pub deploy_mode: DeployMode,
    pub endpoint: String,
    pub secret: String,
}

/// Decorator that wraps any `MigrationEngine` and applies sp00ky-specific
/// post-migration steps (remote functions + internal schema) after `apply()`.
///
/// When `internal_schema` and `remote_functions` are `None`, this delegates
/// straight through with no overhead.
pub(crate) struct Sp00kyEngine {
    inner: Box<dyn MigrationEngine>,
    url: String,
    namespace: String,
    database: String,
    username: String,
    password: String,
    internal_schema: Option<InternalSchemaConfig>,
    remote_functions: Option<RemoteFunctionsConfig>,
}

impl Sp00kyEngine {
    pub(crate) fn new(
        inner: Box<dyn MigrationEngine>,
        url: String,
        namespace: String,
        database: String,
        username: String,
        password: String,
        internal_schema: Option<InternalSchemaConfig>,
        remote_functions: Option<RemoteFunctionsConfig>,
    ) -> Self {
        Self {
            inner,
            url,
            namespace,
            database,
            username,
            password,
            internal_schema,
            remote_functions,
        }
    }

    fn make_client(&self) -> SurrealClient {
        if self.password.is_empty() {
            SurrealClient::new_unauthenticated(&self.url, &self.namespace, &self.database)
        } else {
            SurrealClient::new(
                &self.url,
                &self.namespace,
                &self.database,
                &self.username,
                &self.password,
            )
        }
    }
}

impl MigrationEngine for Sp00kyEngine {
    fn apply(&self) -> Result<()> {
        // 1. User migrations (delegated to inner engine)
        self.inner.apply()?;

        // 2. Remote functions (if configured) — skipped when unchanged.
        if let Some(ref rf) = self.remote_functions {
            let client = self.make_client();
            let impersonation = crate::backend::impersonation_settings_for(
                self.internal_schema.as_ref().and_then(|is| is.config_path.as_deref()),
            );
            let sql = crate::schema_builder::build_remote_functions_schema(
                &rf.deploy_mode,
                &rf.endpoint,
                &rf.secret,
                &impersonation,
            );
            crate::migrate::apply_remote_functions_if_changed(&client, &sql)?;
        }

        // 3. Internal sp00ky schema (if configured)
        if let Some(ref is) = self.internal_schema {
            let client = self.make_client();
            crate::migrate::apply_internal_schema(
                &client,
                &is.schema_path,
                is.config_path.as_deref(),
                &is.deploy_mode,
                is.endpoint.as_deref(),
                is.secret.as_deref(),
            )
            .context("Failed to apply internal Sp00ky schema")?;
        }

        Ok(())
    }

    fn status(&self) -> Result<Vec<MigrationInfo>> {
        self.inner.status()
    }

    fn fix(&self, fix_checksums: bool) -> Result<()> {
        self.inner.fix(fix_checksums)
    }
}
