//! Operations: the one place the dashboard watches long-running actions.
//!
//! Every action an operator can take from the dashboard is asynchronous from
//! their point of view. An SSP told to restart exits on its *next* heartbeat;
//! a reclone fetches the whole upstream; a restore has five stages. Rather
//! than each handler inventing its own status field, all of them record an
//! [`Operation`] here and update it from a background task. The overview
//! embeds the running ones, so the 3s poll the dashboard already runs shows
//! activity without a second request, and `/operations/stream` pushes changes
//! for the page that wants them live.
//!
//! In memory and bounded. Operations are a UI convenience, not an audit log;
//! the audit trail is the scheduler's own tracing output, which records the
//! session subject on every action.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;

/// How many finished operations to keep for the "recent" list.
const RECENT: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OpKind {
    SspRestart,
    SspClean,
    SspReload,
    RollingRestart,
    SchedulerRestart,
    Reclone,
    Rehash,
    CloudRestart,
    BackupCreate,
    BackupRestore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OpStatus {
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Operation {
    pub id: String,
    pub kind: OpKind,
    pub target: Option<String>,
    pub requested_by: String,
    /// Epoch milliseconds; the dashboard renders elapsed time from these.
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub status: OpStatus,
    /// The server's own words about how it ended, when it has any.
    pub message: Option<String>,
    /// Free-form progress, shaped per kind (a rolling restart carries
    /// `{done, total, current}`, a backup carries the registry state).
    pub detail: Value,
}

pub struct Operations {
    inner: Mutex<VecDeque<Operation>>,
    tx: broadcast::Sender<Value>,
}

impl Operations {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(64);
        Arc::new(Self {
            inner: Mutex::new(VecDeque::new()),
            tx,
        })
    }

    /// Record a new running operation and return its id.
    pub fn start(
        &self,
        kind: OpKind,
        target: Option<String>,
        requested_by: String,
        detail: Value,
    ) -> Operation {
        let op = Operation {
            id: uuid::Uuid::new_v4().simple().to_string(),
            kind,
            target,
            requested_by,
            started_at: now_ms(),
            finished_at: None,
            status: OpStatus::Running,
            message: None,
            detail,
        };
        {
            let mut ops = self.inner.lock().expect("operations poisoned");
            ops.push_front(op.clone());
            // Never evict a running operation, whatever its age: a stuck one
            // is exactly the thing the operator needs to still be able to see.
            while ops.len() > RECENT {
                match ops.iter().rposition(|o| o.status != OpStatus::Running) {
                    Some(i) => {
                        ops.remove(i);
                    }
                    None => break,
                }
            }
        }
        super::incidents::emit(op.target.as_deref().unwrap_or("scheduler"), "operator_action", "open", &format!("Operator action {:?} requested", op.kind), Some(&op.id));
        self.publish();
        op
    }

    /// Merge fields into a running operation's `detail`.
    pub fn progress(&self, id: &str, patch: Value) {
        self.update(id, |op| {
            if let (Value::Object(base), Value::Object(p)) = (&mut op.detail, patch) {
                for (k, v) in p {
                    base.insert(k, v);
                }
            }
        });
    }

    pub fn finish(&self, id: &str, message: impl Into<Option<String>>) {
        let message = message.into();
        self.update(id, |op| {
            op.status = OpStatus::Done;
            op.finished_at = Some(now_ms());
            op.message = message;
        });
    }

    pub fn fail(&self, id: &str, message: impl Into<String>) {
        let message = message.into();
        self.update(id, |op| {
            op.status = OpStatus::Failed;
            op.finished_at = Some(now_ms());
            op.message = Some(message);
        });
    }

    fn update(&self, id: &str, f: impl FnOnce(&mut Operation)) {
        let changed = {
            let mut ops = self.inner.lock().expect("operations poisoned");
            match ops.iter_mut().find(|o| o.id == id) {
                Some(op) => {
                    f(op);
                    true
                }
                None => false,
            }
        };
        if changed {
            if let Some(op) = self.get(id) {
                if op.status != OpStatus::Running {
                    super::incidents::emit(op.target.as_deref().unwrap_or("scheduler"), "operator_action", if op.status == OpStatus::Done { "recovered" } else { "failed" }, &format!("Operator action {:?} {:?}", op.kind, op.status), Some(&op.id));
                }
            }
            self.publish();
        }
    }

    pub fn get(&self, id: &str) -> Option<Operation> {
        self.inner
            .lock()
            .expect("operations poisoned")
            .iter()
            .find(|o| o.id == id)
            .cloned()
    }

    /// Newest first.
    pub fn recent(&self) -> Vec<Operation> {
        self.inner
            .lock()
            .expect("operations poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub fn running(&self) -> Vec<Operation> {
        self.inner
            .lock()
            .expect("operations poisoned")
            .iter()
            .filter(|o| o.status == OpStatus::Running)
            .cloned()
            .collect()
    }

    /// Whether an operation of this kind is running, optionally on this target.
    pub fn is_running(&self, kind: OpKind, target: Option<&str>) -> bool {
        self.inner
            .lock()
            .expect("operations poisoned")
            .iter()
            .any(|o| {
                o.status == OpStatus::Running
                    && o.kind == kind
                    && target.map_or(true, |t| o.target.as_deref() == Some(t))
            })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.tx.subscribe()
    }

    pub fn snapshot(&self) -> Value {
        json!({ "operations": self.recent() })
    }

    fn publish(&self) {
        // No subscribers is the common case and not an error.
        let _ = self.tx.send(self.snapshot());
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_finish_round_trip() {
        let ops = Operations::new();
        let op = ops.start(
            OpKind::SspRestart,
            Some("ssp-1".into()),
            "me".into(),
            json!({}),
        );
        assert!(ops.is_running(OpKind::SspRestart, Some("ssp-1")));
        assert!(!ops.is_running(OpKind::SspRestart, Some("ssp-2")));
        ops.progress(&op.id, json!({ "phase": "waiting" }));
        assert_eq!(ops.get(&op.id).unwrap().detail["phase"], "waiting");
        ops.finish(&op.id, Some("back".to_string()));
        let done = ops.get(&op.id).unwrap();
        assert_eq!(done.status, OpStatus::Done);
        assert!(done.finished_at.is_some());
        assert!(ops.running().is_empty());
    }

    #[test]
    fn finished_operations_are_bounded_but_running_ones_survive() {
        let ops = Operations::new();
        let keep = ops.start(OpKind::Reclone, None, "me".into(), json!({}));
        for i in 0..(RECENT + 20) {
            let op = ops.start(
                OpKind::SspRestart,
                Some(format!("ssp-{i}")),
                "me".into(),
                json!({}),
            );
            ops.finish(&op.id, None);
        }
        assert!(ops.recent().len() <= RECENT + 1);
        assert!(
            ops.get(&keep.id).is_some(),
            "a running op must never be evicted"
        );
    }

    #[test]
    fn changes_are_broadcast() {
        let ops = Operations::new();
        let mut rx = ops.subscribe();
        let op = ops.start(OpKind::Rehash, None, "me".into(), json!({}));
        let frame = rx.try_recv().expect("start publishes");
        assert_eq!(frame["operations"][0]["id"], op.id);
        ops.fail(&op.id, "boom");
        let frame = rx.try_recv().expect("fail publishes");
        assert_eq!(frame["operations"][0]["status"], "failed");
        assert_eq!(frame["operations"][0]["message"], "boom");
    }
}
