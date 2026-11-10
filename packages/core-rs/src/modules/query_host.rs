//! The narrow seam a module needs from the client: register a shared query and
//! subscribe to its rows.
//!
//! PORT: `packages/spooky_core/lib/src/modules/query_host.dart`. Keeps the
//! query builder, [`crate::modules::feature_flag`] and
//! [`crate::modules::app_release`] off the engine's internals: they only ever
//! want to register a query and watch it. The client implements it over
//! `Runtime::subscribe` and the register saga.

use async_trait::async_trait;

use crate::error::Result;
use crate::modules::query_builder::RelationPlan;
use crate::runtime::{RecordsCallback, Subscription};
use crate::types::QueryHash;
use crate::value::Object;

#[async_trait(?Send)]
pub trait QueryHost {
    /// Register `surql` (reading `table`) with its bound `params`, and return
    /// the query's hash. `relations` carries the `.related()` plan so the
    /// client can resolve joined rows from the local cache.
    async fn register_query(
        &self,
        table: &str,
        surql: &str,
        params: Object,
        ttl: &str,
        relations: Vec<RelationPlan>,
    ) -> Result<QueryHash>;

    /// Watch a registered query's rows. `immediate` replays the current rows.
    /// Counts as a subscriber (keeps the query alive) until the guard drops.
    fn subscribe(&self, hash: &str, callback: RecordsCallback, immediate: bool) -> Subscription;
}

/// Fakes for module tests: a host that records registrations and lets a test
/// push the rows the shared query would deliver, and an auth source a test
/// drives by hand.
#[cfg(test)]
pub(crate) mod fakes {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::QueryHost;
    use crate::error::Result;
    use crate::modules::auth::{AuthListener, AuthSource};
    use crate::modules::query_builder::RelationPlan;
    use crate::runtime::{RecordsCallback, Subscription};
    use crate::types::QueryHash;
    use crate::value::{Object, Row};

    /// One registration a module asked for: `(table, surql, ttl)`.
    pub type Registration = (String, String, String);

    #[derive(Default)]
    pub struct FakeQueryHost {
        pub registrations: RefCell<Vec<Registration>>,
        subscribers: Rc<RefCell<BTreeMap<String, RecordsCallback>>>,
        next: Cell<u32>,
    }

    impl FakeQueryHost {
        /// How many subscriptions are currently live.
        pub fn live_subscriptions(&self) -> usize {
            self.subscribers.borrow().len()
        }

        /// Deliver `rows` to every live subscriber, as a materialization would.
        pub fn emit(&self, rows: Vec<Row>) {
            let rows = Arc::new(rows);
            let cbs: Vec<RecordsCallback> = self.subscribers.borrow().values().cloned().collect();
            for cb in cbs {
                cb(&rows);
            }
        }
    }

    #[async_trait(?Send)]
    impl QueryHost for FakeQueryHost {
        async fn register_query(
            &self,
            table: &str,
            surql: &str,
            _params: Object,
            ttl: &str,
            _relations: Vec<RelationPlan>,
        ) -> Result<QueryHash> {
            self.registrations.borrow_mut().push((
                table.to_string(),
                surql.to_string(),
                ttl.to_string(),
            ));
            let n = self.next.get();
            self.next.set(n + 1);
            Ok(format!("hash-{n}"))
        }

        fn subscribe(
            &self,
            hash: &str,
            callback: RecordsCallback,
            immediate: bool,
        ) -> Subscription {
            self.subscribers
                .borrow_mut()
                .insert(hash.to_string(), callback.clone());
            if immediate {
                callback(&Arc::new(Vec::new()));
            }
            let subscribers = self.subscribers.clone();
            let hash = hash.to_string();
            Subscription::new(move || {
                subscribers.borrow_mut().remove(&hash);
            })
        }
    }

    /// An [`AuthSource`] whose user a test sets with [`FakeAuth::emit`].
    #[derive(Default)]
    pub struct FakeAuth {
        user: RefCell<Option<String>>,
        listeners: Rc<RefCell<Vec<(u64, AuthListener)>>>,
        next: Cell<u64>,
    }

    impl FakeAuth {
        pub fn emit(&self, next: Option<&str>) {
            *self.user.borrow_mut() = next.map(str::to_string);
            let listeners: Vec<AuthListener> = self
                .listeners
                .borrow()
                .iter()
                .map(|(_, l)| l.clone())
                .collect();
            for l in listeners {
                l(next);
            }
        }
    }

    impl AuthSource for FakeAuth {
        fn subscribe_user(&self, listener: AuthListener) -> Subscription {
            let user = self.user.borrow().clone();
            listener(user.as_deref());
            let id = self.next.get() + 1;
            self.next.set(id);
            self.listeners.borrow_mut().push((id, listener));
            let listeners = self.listeners.clone();
            Subscription::new(move || listeners.borrow_mut().retain(|(i, _)| *i != id))
        }
    }

    /// Let spawned registrations land.
    pub async fn tick() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
}
