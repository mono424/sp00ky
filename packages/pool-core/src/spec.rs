//! Typed views of the `_00_pool` and `_00_machine` rows.
//!
//! Rows arrive as flattened JSON (record ids and datetimes as strings), the same
//! convention `schedule-core` reads. Anything time-dependent is NOT parsed here:
//! the statements compute it against the database clock and hand back booleans,
//! so the engine never compares its own clock with the database's.

use serde_json::Value;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SpecError {
    #[error("pool row is missing `{0}`")]
    Missing(&'static str),
    #[error("pool `{0}` has an unusable outbox table name `{1}`")]
    BadTable(String, String),
}

/// One `_00_pool` row: the deployed spec plus the operator and engine fields.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolSpec {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub machine_type: Option<String>,
    pub locations: Vec<String>,
    pub slots: u32,
    pub min: u32,
    pub autoscale: bool,
    pub max: Option<u32>,
    pub buffer: u32,
    pub idle_timeout_secs: i64,
    pub recycle_per_job: bool,
    pub max_job_duration_secs: i64,
    pub max_lifetime_secs: i64,
    pub lease_secs: i64,
    pub boot_timeout_secs: i64,
    pub backend: String,
    pub target_table: String,
    pub container: Value,
    pub spec_hash: String,
    pub paused: bool,
    pub boot_failures: i64,
    /// `breaker_until` is still in the future (computed by the SELECT).
    pub breaker_open: bool,
}

fn str_field(row: &Value, key: &'static str) -> Result<String, SpecError> {
    row.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or(SpecError::Missing(key))
}

fn int_field(row: &Value, key: &str, default: i64) -> i64 {
    row.get(key).and_then(Value::as_i64).unwrap_or(default)
}

fn uint_field(row: &Value, key: &str, default: u32) -> u32 {
    int_field(row, key, default as i64).clamp(0, u32::MAX as i64) as u32
}

impl PoolSpec {
    pub fn from_row(row: &Value) -> Result<Self, SpecError> {
        let name = str_field(row, "name")?;
        let target_table = str_field(row, "target_table")?;
        // Interpolated into statements (a table name cannot be bound), so it must
        // be a plain identifier. Deploy validates this too; never trust one check.
        if !schedule_core::sql::is_plain_identifier(&target_table) {
            return Err(SpecError::BadTable(name, target_table));
        }
        Ok(Self {
            id: str_field(row, "id")?,
            provider: str_field(row, "provider")?,
            machine_type: row
                .get("machine_type")
                .and_then(Value::as_str)
                .map(str::to_string),
            locations: row
                .get("locations")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            slots: uint_field(row, "slots", 1).max(1),
            min: uint_field(row, "min", 0),
            autoscale: row
                .get("autoscale")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            max: row
                .get("max")
                .and_then(Value::as_i64)
                .map(|m| m.clamp(0, u32::MAX as i64) as u32),
            buffer: uint_field(row, "buffer", 0),
            idle_timeout_secs: int_field(row, "idle_timeout_secs", 600).max(0),
            recycle_per_job: row.get("recycle").and_then(Value::as_str).unwrap_or("job") == "job",
            max_job_duration_secs: int_field(row, "max_job_duration_secs", 28_800).max(1),
            max_lifetime_secs: int_field(row, "max_lifetime_secs", 86_400).max(1),
            lease_secs: int_field(row, "lease_secs", 90).max(1),
            boot_timeout_secs: int_field(row, "boot_timeout_secs", 300).max(1),
            backend: str_field(row, "backend")?,
            container: row.get("container").cloned().unwrap_or(Value::Null),
            spec_hash: str_field(row, "spec_hash")?,
            paused: row.get("paused").and_then(Value::as_bool).unwrap_or(false),
            boot_failures: int_field(row, "boot_failures", 0),
            breaker_open: row
                .get("breaker_open")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            name,
            target_table,
        })
    }

    /// The most machines this pool may have in supply (requested, booting,
    /// ready). A fixed pool's ceiling is its size. An autoscaled pool with no
    /// `max` (which deploy rejects, but a hand-written row could carry) is
    /// treated as fixed rather than unbounded: fail closed on cost.
    pub fn ceiling(&self) -> u32 {
        if self.autoscale {
            self.max.unwrap_or(self.min).max(self.min)
        } else {
            self.min
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineState {
    Requested,
    Booting,
    Ready,
    Draining,
    Terminating,
    Gone,
    Failed,
}

impl MachineState {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "requested" => Self::Requested,
            "booting" => Self::Booting,
            "ready" => Self::Ready,
            "draining" => Self::Draining,
            "terminating" => Self::Terminating,
            "gone" => Self::Gone,
            "failed" => Self::Failed,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Booting => "booting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Terminating => "terminating",
            Self::Gone => "gone",
            Self::Failed => "failed",
        }
    }

    /// Counts toward the pool's capacity: it serves jobs now or is about to.
    pub fn is_supply(self) -> bool {
        matches!(self, Self::Requested | Self::Booting | Self::Ready)
    }

    /// Still exists at the provider (or may), so it still costs money.
    pub fn is_live(self) -> bool {
        !matches!(self, Self::Gone | Self::Failed)
    }
}

/// One live `_00_machine` row, with its clocks already evaluated by the database.
#[derive(Debug, Clone, PartialEq)]
pub struct MachineRow {
    pub id: String,
    pub state: MachineState,
    pub provider_id: Option<String>,
    pub slots: u32,
    pub busy_slots: u32,
    pub spec_hash: String,
    pub has_idle_since: bool,
    /// Created longer ago than the pool's boot timeout.
    pub boot_overdue: bool,
    /// No poll for a whole lease: its jobs are reclaimable, it is lost.
    pub heartbeat_lost: bool,
    /// Polled recently enough to be trusted with a new job.
    pub fresh: bool,
    /// Idle for longer than the pool's idle timeout.
    pub idle_expired: bool,
    pub lifetime_over: bool,
    /// Old enough that the provider must know about it by now.
    pub settled: bool,
}

impl MachineRow {
    pub fn from_row(row: &Value) -> Option<Self> {
        let flag = |k: &str| row.get(k).and_then(Value::as_bool).unwrap_or(false);
        Some(Self {
            id: row.get("id")?.as_str()?.to_string(),
            state: MachineState::parse(row.get("state")?.as_str()?)?,
            provider_id: row
                .get("provider_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            slots: uint_field(row, "slots", 1).max(1),
            busy_slots: uint_field(row, "busy_slots", 0),
            spec_hash: row
                .get("spec_hash")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            has_idle_since: row.get("idle_since").map(|v| !v.is_null()).unwrap_or(false),
            boot_overdue: flag("boot_overdue"),
            heartbeat_lost: flag("heartbeat_lost"),
            fresh: flag("fresh"),
            idle_expired: flag("idle_expired"),
            lifetime_over: flag("lifetime_over"),
            settled: flag("settled"),
        })
    }

    /// The key part of `_00_machine:<key>`: what providers name the machine by.
    pub fn key(&self) -> &str {
        machine_key(&self.id)
    }
}

/// `_00_machine:abc` -> `abc` (tolerates the `⟨abc⟩` escaping SurrealDB applies to
/// keys that are not plain identifiers).
pub fn machine_key(id: &str) -> &str {
    let key = id.split_once(':').map(|(_, k)| k).unwrap_or(id);
    key.trim_start_matches('⟨')
        .trim_end_matches('⟩')
        .trim_matches('`')
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row() -> Value {
        json!({
            "id": "_00_pool:render", "name": "render", "provider": "docker",
            "slots": 1, "min": 1, "autoscale": true, "max": 4, "buffer": 2,
            "backend": "renderer", "target_table": "render_job",
            "container": { "image": "r:1", "port": 8080 }, "spec_hash": "h1",
        })
    }

    #[test]
    fn reads_a_pool_row_with_defaults() {
        let spec = PoolSpec::from_row(&row()).unwrap();
        assert_eq!(spec.ceiling(), 4);
        assert_eq!(spec.lease_secs, 90);
        assert!(spec.recycle_per_job);
        assert!(!spec.paused && !spec.breaker_open);
    }

    #[test]
    fn a_fixed_pool_ceiling_is_its_size_and_a_maxless_autoscaler_fails_closed() {
        let mut r = row();
        r["autoscale"] = json!(false);
        assert_eq!(PoolSpec::from_row(&r).unwrap().ceiling(), 1);

        let mut r = row();
        r.as_object_mut().unwrap().remove("max");
        assert_eq!(
            PoolSpec::from_row(&r).unwrap().ceiling(),
            1,
            "no max must not mean unbounded"
        );
    }

    #[test]
    fn rejects_a_table_name_that_cannot_be_interpolated() {
        let mut r = row();
        r["target_table"] = json!("job; REMOVE TABLE user");
        assert!(matches!(
            PoolSpec::from_row(&r),
            Err(SpecError::BadTable(..))
        ));
    }

    #[test]
    fn machine_keys_survive_surreal_escaping() {
        assert_eq!(machine_key("_00_machine:abc123"), "abc123");
        assert_eq!(machine_key("_00_machine:⟨a-b⟩"), "a-b");
        assert_eq!(machine_key("abc"), "abc");
    }

    #[test]
    fn supply_and_live_partition_the_states() {
        use MachineState::*;
        for s in [Requested, Booting, Ready] {
            assert!(s.is_supply() && s.is_live());
        }
        for s in [Draining, Terminating] {
            assert!(!s.is_supply() && s.is_live());
        }
        for s in [Gone, Failed] {
            assert!(!s.is_supply() && !s.is_live());
        }
        for s in [
            Requested,
            Booting,
            Ready,
            Draining,
            Terminating,
            Gone,
            Failed,
        ] {
            assert_eq!(MachineState::parse(s.as_str()), Some(s));
        }
    }
}
