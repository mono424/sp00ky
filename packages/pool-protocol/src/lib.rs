//! Wire protocol between `spky-agent` (on a pool machine) and the scheduler.
//!
//! Plain HTTP + JSON, and **the machine always dials out**: nothing ever connects
//! to a machine, so pool machines need no inbound ports and no service discovery.
//!
//! ```text
//! POST /pool/v1/hello    once per agent start   -> how to run and watch the backend
//! POST /pool/v1/ready    backend is healthy     -> machine starts accepting jobs
//! POST /pool/v1/poll     every few seconds      -> heartbeat in, commands out
//! POST /pool/v1/result   a job attempt ended    -> terminal write, fenced
//! ```
//!
//! The scheduler keeps **no per-agent command queue**. Every `poll` reply is derived
//! from database state at that moment: a job bound to this machine that the agent
//! does not report running becomes an `Assign`; a job the agent reports that is no
//! longer bound to it becomes a `Cancel`. A scheduler restart therefore loses
//! nothing, and a lost reply is simply re-derived on the next poll.
//!
//! Every request carries `Authorization: Bearer <machine token>`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Route prefix on the scheduler's pool listener.
pub const ROUTE_PREFIX: &str = "/pool/v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HelloRequest {
    pub machine: String,
    #[serde(default)]
    pub agent_version: String,
}

/// Everything the agent needs to supervise the backend on its machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HelloReply {
    /// Port the backend listens on, on the machine's loopback.
    pub port: u16,
    /// Path probed until it answers 2xx before the machine reports ready.
    /// `None` = a TCP connect to `port` is enough.
    #[serde(default)]
    pub healthcheck: Option<String>,
    /// Extra environment for the backend process (secrets travel here, over the
    /// authenticated channel, never in provider user-data).
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// Seconds between polls. Also the heartbeat period.
    pub poll_secs: u64,
    /// Job lease. The agent stops a job on its own once it has gone
    /// `lease_secs - stop_margin_secs` without a successful poll, which is always
    /// before the scheduler may hand the job to another machine.
    pub lease_secs: u64,
    pub stop_margin_secs: u64,
    /// Restart the backend process after every job.
    pub recycle_per_job: bool,
    /// With no scheduler contact for this long the agent shuts its machine down
    /// (the dead-man switch behind the provider-side reaper).
    pub orphan_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadyRequest {
    pub machine: String,
}

/// One job attempt as the agent knows it. `(job, epoch)` is the identity: the
/// same job under a later epoch is a different attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobRef {
    pub job: String,
    pub epoch: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PollRequest {
    pub machine: String,
    /// Attempts currently running on this machine.
    #[serde(default)]
    pub running: Vec<JobRef>,
    /// Free-form self report (cpu, memory, net counters, version). Stored as is.
    #[serde(default)]
    pub stats: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    /// Run this attempt: `POST http://127.0.0.1:{port}{path}` with `payload`,
    /// held open until the backend answers or `deadline_secs` passes.
    Assign {
        job: String,
        epoch: i64,
        path: String,
        payload: Value,
        deadline_secs: u64,
    },
    /// Stop this attempt. Its result, if any, no longer counts.
    Cancel { job: String, epoch: i64 },
    /// Stop everything and exit; the machine is being taken away.
    Shutdown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PollReply {
    #[serde(default)]
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    /// Backend answered 2xx. `body` is its response, verbatim.
    Success { body: String },
    /// Backend answered non-2xx, or the request failed. Retried per the row's
    /// retry budget.
    Failed { code: Value, reason: String },
    /// Stopped on a `Cancel`. Terminal, never retried.
    Cancelled,
    /// Stopped for exceeding `deadline_secs`. Terminal, never retried.
    DeadlineExceeded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResultRequest {
    pub machine: String,
    pub job: String,
    pub epoch: i64,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ResultReply {
    /// False when the attempt was already fenced out (reclaimed, killed). The
    /// agent just drops it either way.
    pub accepted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn commands_are_tagged_for_forward_compatibility() {
        let assign = Command::Assign {
            job: "render_job:a".into(),
            epoch: 2,
            path: "/render".into(),
            payload: json!({ "streamId": "s" }),
            deadline_secs: 60,
        };
        let v = serde_json::to_value(&assign).unwrap();
        assert_eq!(v["type"], "assign");
        assert_eq!(serde_json::from_value::<Command>(v).unwrap(), assign);
        assert_eq!(
            serde_json::to_value(Command::Shutdown).unwrap(),
            json!({ "type": "shutdown" })
        );
    }

    #[test]
    fn a_minimal_poll_parses() {
        let req: PollRequest =
            serde_json::from_value(json!({ "machine": "_00_machine:x" })).unwrap();
        assert!(req.running.is_empty());
        assert!(req.stats.is_none());
    }

    #[test]
    fn outcomes_round_trip() {
        for o in [
            Outcome::Success { body: "{}".into() },
            Outcome::Failed {
                code: json!(500),
                reason: "boom".into(),
            },
            Outcome::Cancelled,
            Outcome::DeadlineExceeded,
        ] {
            let v = serde_json::to_value(&o).unwrap();
            assert_eq!(serde_json::from_value::<Outcome>(v).unwrap(), o);
        }
    }
}
