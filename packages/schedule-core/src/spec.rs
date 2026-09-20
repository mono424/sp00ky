//! Schedule + workflow definitions as the engine sees them.
//!
//! The CLI normalizes YAML sugar away at deploy time (`every: 5m` → `every_ms`,
//! `backend: api` → the app's outbox `target_table`, step maps → ordered step
//! lists), so the engine only ever parses this already-flat shape out of a
//! `_00_schedule` row. Unknown fields are ignored on purpose: a newer CLI may
//! write spec fields an older engine doesn't understand yet, and dropping them
//! is preferable to refusing to run the schedule at all.

use serde::{Deserialize, Serialize};

use crate::cron::{FireSpec, FireSpecError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleKind {
    /// Each fire spawns one atomic job per fan-out row.
    Job,
    /// Each fire spawns one workflow run (DAG of atomic jobs) per fan-out row.
    Workflow,
}

/// What to do when a fire lands while the previous run for the same fan-out key
/// is still in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Concurrency {
    /// Record a `skipped` run and move on (the default: a slow hourly sync
    /// should not pile up).
    #[default]
    Skip,
    /// Spawn regardless — overlapping runs are fine.
    Allow,
    /// Kill the in-flight run, mark it `replaced`, spawn the new one.
    Replace,
}

/// One row of `_00_schedule`, spec fields only. Engine-owned and operator-owned
/// fields (`next_fire_at`, `paused`, …) are read separately by the engine's SQL
/// so this type can never be the vehicle that overwrites them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleSpec {
    pub name: String,
    pub kind: ScheduleKind,

    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub every_ms: Option<i64>,
    #[serde(default)]
    pub timezone: Option<String>,

    /// Outbox table the spawned job rows are created in. Required for
    /// `kind: job`; a workflow step may override it per step.
    #[serde(default)]
    pub target_table: Option<String>,
    /// Backend route the spawned job POSTs to (`kind: job` only).
    #[serde(default)]
    pub path: Option<String>,
    /// Static payload merged into every spawned job's payload.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,

    /// DAG definition (`kind: workflow` only). Frozen onto each workflow run at
    /// spawn so a redeploy cannot change in-flight semantics.
    #[serde(default)]
    pub workflow: Option<WorkflowDef>,

    /// SurrealQL SELECT run each fire; one run is spawned per returned row.
    #[serde(default)]
    pub for_each: Option<String>,
    /// Row field whose value is the per-key concurrency key. Defaults to `id`.
    #[serde(default)]
    pub for_each_key: Option<String>,

    #[serde(default)]
    pub concurrency: Concurrency,

    #[serde(default)]
    pub max_retries: Option<i64>,
    #[serde(default)]
    pub retry_strategy: Option<String>,
    /// Per-job HTTP timeout in seconds (honoured only when the backend allows
    /// overrides — see `BackendInfo::effective_timeout` in ssp-node).
    #[serde(default)]
    pub timeout: Option<i64>,

    /// `all` (or unset) keeps every run until its retention window elapses;
    /// `failures-only` never persists a successful one at all.
    #[serde(default)]
    pub history_mode: Option<String>,

    /// Wall-clock ceiling on one run of this schedule, in seconds. Unset means
    /// the project default (`_00_retention.run_deadline_secs`) applies.
    ///
    /// Distinct from `timeout`, which bounds one HTTP request: this bounds the
    /// run, including every step and every job-level retry inside it.
    #[serde(default)]
    pub deadline_secs: Option<i64>,

    /// Consecutive failures of ONE forEach key before the gate stops firing it.
    /// Unset (the default) means never.
    ///
    /// The budget is read at the gate rather than frozen onto a key, unlike
    /// `history_mode` and `deadline_secs`: those bound a run already in flight,
    /// where a config change must not move the goalposts, while this decides
    /// whether to start one at all. Raising or lowering it therefore takes
    /// effect on the next fire, which is what an operator releasing a stuck key
    /// expects.
    #[serde(default)]
    pub quarantine_after: Option<i64>,
}

/// `history_mode` value that suppresses successful run rows entirely.
pub const HISTORY_FAILURES_ONLY: &str = "failures-only";

impl ScheduleSpec {
    /// Deserialize from a `_00_schedule` row (flattened-JSON convention).
    pub fn from_row(row: &serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(row.clone())
    }

    pub fn fire_spec(&self) -> Result<FireSpec, FireSpecError> {
        FireSpec::parse(self.cron.as_deref(), self.every_ms, self.timezone.as_deref())
    }

    /// Row field to read the concurrency key from.
    pub fn key_field(&self) -> &str {
        self.for_each_key.as_deref().unwrap_or("id")
    }
}

/// Workflow DAG definition. Step order in the vec is irrelevant; execution
/// order comes from `depends_on`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDef {
    pub steps: Vec<StepDef>,
    /// What happens to not-yet-dispatched steps when a step fails.
    #[serde(default)]
    pub on_failure: OnFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OnFailure {
    /// Fail the run; skip every step that hasn't been dispatched yet.
    #[default]
    Halt,
    /// Keep running branches that don't depend on the failed step; the run
    /// still ends `failed`.
    ContinueIndependent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub name: String,
    pub path: String,
    /// Outbox table override; falls back to the schedule's `target_table`.
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub max_retries: Option<i64>,
    #[serde(default)]
    pub retry_strategy: Option<String>,
    #[serde(default)]
    pub timeout: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_a_minimal_job_schedule_row() {
        let row = json!({
            "id": "_00_schedule:nightly",
            "name": "nightly",
            "kind": "job",
            "cron": "0 3 * * *",
            "target_table": "job",
            "path": "/cleanup",
            // engine/operator fields the spec type must tolerate and ignore
            "paused": false,
            "next_fire_at": "2026-07-26T01:00:00Z",
            "spec_hash": "abc",
        });
        let spec = ScheduleSpec::from_row(&row).unwrap();
        assert_eq!(spec.name, "nightly");
        assert_eq!(spec.kind, ScheduleKind::Job);
        assert_eq!(spec.concurrency, Concurrency::Skip);
        assert_eq!(spec.key_field(), "id");
        assert!(spec.fire_spec().is_ok());
    }

    #[test]
    fn parses_fan_out_and_concurrency() {
        let row = json!({
            "name": "game-sync",
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/syncGames",
            "for_each": "SELECT id FROM connection WHERE active = true",
            "for_each_key": "id",
            "concurrency": "replace",
        });
        let spec = ScheduleSpec::from_row(&row).unwrap();
        assert_eq!(spec.concurrency, Concurrency::Replace);
        assert_eq!(spec.for_each.as_deref(), Some("SELECT id FROM connection WHERE active = true"));
    }

    #[test]
    fn parses_a_workflow_dag() {
        let row = json!({
            "name": "monthly-report",
            "kind": "workflow",
            "cron": "0 6 1 * *",
            "target_table": "job",
            "workflow": {
                "steps": [
                    {"name": "extract", "path": "/extract"},
                    {"name": "load", "path": "/load", "depends_on": ["extract"]},
                ],
                "on_failure": "continue-independent",
            },
        });
        let spec = ScheduleSpec::from_row(&row).unwrap();
        let wf = spec.workflow.as_ref().unwrap();
        assert_eq!(wf.steps.len(), 2);
        assert_eq!(wf.on_failure, OnFailure::ContinueIndependent);
        assert_eq!(wf.steps[1].depends_on, vec!["extract"]);
    }
}
