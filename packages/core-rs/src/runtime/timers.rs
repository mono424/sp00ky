//! Wall-clock timers on the runtime's local executor: the only place the engine
//! reaches for one. Under `tokio::time::pause()` they advance with the test
//! clock.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use crate::kernel::ports::TimerPort;

#[derive(Default)]
struct Pending {
    tasks: HashMap<String, (u64, tokio::task::JoinHandle<()>)>,
}

/// Keyed one-shot timers backed by `tokio::task::spawn_local`. Must be used
/// inside a `LocalSet`.
#[derive(Clone, Default)]
pub struct TokioTimers {
    pending: Rc<RefCell<Pending>>,
    generation: Rc<Cell<u64>>,
}

impl TokioTimers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keys with a pending timer (diagnostics and tests).
    pub fn pending_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.pending.borrow().tasks.keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl TimerPort for TokioTimers {
    fn set(&self, key: &str, ms: u64, fire: Box<dyn FnOnce()>) {
        let gen = self.generation.get() + 1;
        self.generation.set(gen);
        let pending = Rc::downgrade(&self.pending);
        let owned_key = key.to_string();
        let handle = tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            if let Some(p) = pending.upgrade() {
                let mut p = p.borrow_mut();
                if p.tasks.get(&owned_key).is_some_and(|(g, _)| *g == gen) {
                    p.tasks.remove(&owned_key);
                }
            }
            fire();
        });
        if let Some((_, old)) = self.pending.borrow_mut().tasks.insert(key.to_string(), (gen, handle)) {
            old.abort();
        }
    }

    fn clear(&self, key: &str) {
        if let Some((_, h)) = self.pending.borrow_mut().tasks.remove(key) {
            h.abort();
        }
    }

    fn clear_all(&self) {
        for (_, (_, h)) in self.pending.borrow_mut().tasks.drain() {
            h.abort();
        }
    }
}
