use anyhow::{Context, Result};
use std::path::PathBuf;

use crate::migrate;
use crate::surreal_client::{MigrationDB, SurrealClient};

use super::engine::{MigrationEngine, MigrationInfo, MigrationState};

/// Legacy migration engine wrapping the existing `migrate.rs` functions.
///
/// This adapter delegates to the original migration code with zero behavioral
/// changes, translating results into the generic `MigrationEngine` types.
pub struct LegacyEngine {
    url: String,
    namespace: String,
    database: String,
    username: String,
    password: String,
    migrations_dir: PathBuf,
    /// Vault secrets for `{{KEY}}` substitution. `None` = verbatim apply (the
    /// ephemeral schema-diff replay, where `{{...}}` is left literal so it doesn't
    /// read as drift). `Some(_)` = injection was requested (prod / cloud apply):
    /// substitute and FAIL on any unresolved placeholder, even when the vault load
    /// came back empty — so a missing secret never silently writes a literal key.
    secrets: Option<Vec<(String, String)>>,
    /// Completes each migration's tables with their sync identity as they apply.
    /// `None` = verbatim (no manifest found). See [`migrate::SyncIdentity`].
    identity: Option<migrate::SyncIdentity>,
}

impl LegacyEngine {
    pub fn new(
        url: String,
        namespace: String,
        database: String,
        username: String,
        password: String,
        migrations_dir: PathBuf,
        secrets: Option<Vec<(String, String)>>,
    ) -> Self {
        Self {
            url,
            namespace,
            database,
            username,
            password,
            migrations_dir,
            secrets,
            identity: None,
        }
    }

    pub fn with_sync_identity(mut self, identity: Option<migrate::SyncIdentity>) -> Self {
        self.identity = identity;
        self
    }

    fn make_client(&self) -> SurrealClient {
        SurrealClient::new(
            &self.url,
            &self.namespace,
            &self.database,
            &self.username,
            &self.password,
        )
    }
}

impl MigrationEngine for LegacyEngine {
    fn apply(&self) -> Result<()> {
        let client = self.make_client();
        // `secrets: Some(_)` = injection requested (prod / cloud apply): the checked
        // path, so an unresolved `{{KEY}}` errors loudly - even if the vault load
        // came back empty - instead of writing a literal `{{...}}` (e.g. a broken
        // JWT signing key). `None` leaves `{{...}}` literal.
        migrate::apply_completing(
            &client,
            &self.migrations_dir,
            self.secrets.as_deref(),
            self.identity.as_ref(),
        )
    }

    fn status(&self) -> Result<Vec<MigrationInfo>> {
        let client = self.make_client();
        client.ping().context("Cannot connect to SurrealDB")?;
        client.ensure_migration_table()?;

        let applied = client.get_applied_migrations()?;
        let filesystem = migrate::scan_migrations(&self.migrations_dir)?;

        let mut infos = Vec::new();

        for fm in &filesystem {
            if let Some(am) = applied.iter().find(|a| a.version == fm.version) {
                if fm.path.exists() {
                    let disk_checksum = migrate::checksum(&fm.path)?;
                    if disk_checksum != am.checksum {
                        infos.push(MigrationInfo {
                            id: fm.version.clone(),
                            name: fm.name.clone(),
                            state: MigrationState::Drift,
                            applied_at: Some(am.applied_at.clone()),
                            detail: Some("checksum mismatch".to_string()),
                        });
                        continue;
                    }
                }
                infos.push(MigrationInfo {
                    id: fm.version.clone(),
                    name: fm.name.clone(),
                    state: MigrationState::Applied,
                    applied_at: Some(am.applied_at.clone()),
                    detail: None,
                });
            } else {
                infos.push(MigrationInfo {
                    id: fm.version.clone(),
                    name: fm.name.clone(),
                    state: MigrationState::Pending,
                    applied_at: None,
                    detail: None,
                });
            }
        }

        // Warn about applied migrations not on disk
        for am in &applied {
            if !filesystem.iter().any(|f| f.version == am.version) {
                infos.push(MigrationInfo {
                    id: am.version.clone(),
                    name: am.name.clone(),
                    state: MigrationState::Drift,
                    applied_at: Some(am.applied_at.clone()),
                    detail: Some("not present on disk".to_string()),
                });
            }
        }

        Ok(infos)
    }

    fn fix(&self, fix_checksums: bool) -> Result<()> {
        let client = self.make_client();
        migrate::fix(&client, &self.migrations_dir, fix_checksums)
    }
}
