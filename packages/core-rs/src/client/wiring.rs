//! The seams between the modules and the engine's services: the auth module
//! over the socket and the persistence, the auth session the boot sagas read,
//! and the query host the builder and the modules register through.

use std::rc::{Rc, Weak};
use std::sync::Arc;

use async_trait::async_trait;

use crate::client::engine::{Engine, QueryOptions};
use crate::error::{Error, Result};
use crate::modules::auth::{AuthPersistence, AuthService};
use crate::modules::query_builder::RelationPlan;
use crate::modules::query_host::QueryHost;
use crate::runtime::services::AuthSession;
use crate::runtime::{RecordsCallback, Subscription};
use crate::services::persistence::PersistenceClient;
use crate::types::QueryHash;
use crate::value::{Object, Value};

#[cfg(feature = "remote")]
mod remote_auth {
    use super::*;
    use crate::modules::auth::AuthRemote;
    use crate::services::remote::RemoteDatabaseService;

    #[async_trait(?Send)]
    impl AuthRemote for RemoteDatabaseService {
        async fn signin(&self, params: Object) -> Result<Value> {
            RemoteDatabaseService::signin(self, params).await
        }
        async fn signup(&self, params: Object) -> Result<Value> {
            RemoteDatabaseService::signup(self, params).await
        }
        async fn authenticate(&self, token: &str) -> Result<Value> {
            RemoteDatabaseService::authenticate(self, token).await
        }
        async fn invalidate(&self) -> Result<()> {
            RemoteDatabaseService::invalidate(self).await
        }
        async fn query_auth_user(&self) -> Result<Vec<Value>> {
            RemoteDatabaseService::query_auth_user(self).await
        }
        fn begin_session_transition(&self) -> u64 {
            RemoteDatabaseService::begin_session_transition(self)
        }
        fn end_session_transition(&self, transition: u64) {
            RemoteDatabaseService::end_session_transition(self, transition)
        }
        async fn force_close(&self) {
            RemoteDatabaseService::force_close(self).await
        }
        fn set_auth_token(&self, token: Option<&str>) {
            RemoteDatabaseService::set_auth_token(self, token.map(str::to_string))
        }
        fn take_handshake_rejection(&self, token: &str) -> Option<crate::error::Error> {
            RemoteDatabaseService::take_handshake_rejection(self, token)
        }
    }
}

/// The client's persistence, as the auth module stores its token.
#[cfg_attr(not(feature = "remote"), allow(dead_code))]
pub(crate) struct PersistenceBridge(pub Rc<dyn PersistenceClient>);

#[async_trait(?Send)]
impl AuthPersistence for PersistenceBridge {
    async fn set(&self, key: &str, value: Value) -> Result<()> {
        self.0.set(key, value).await
    }
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.0.get(key).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.0.remove(key).await
    }
}

#[async_trait(?Send)]
impl AuthSession for AuthService {
    async fn restore_session(&self) -> Result<Option<String>> {
        self.restore_session_from_token(false).await
    }
    async fn init(&self) -> Result<()> {
        AuthService::init(self).await
    }
    fn current_user(&self) -> Option<Object> {
        AuthService::current_user(self).map(|u| (*u).clone())
    }
    fn access(&self) -> Option<String> {
        AuthService::access(self)
    }
    fn token(&self) -> Option<String> {
        AuthService::token(self)
    }
}

/// The engine as the builder and the modules see it.
pub(crate) struct EngineHost(pub Weak<Engine>);

impl EngineHost {
    fn engine(&self) -> Result<Rc<Engine>> {
        self.0.upgrade().ok_or(Error::Closed)
    }
}

#[async_trait(?Send)]
impl QueryHost for EngineHost {
    async fn register_query(
        &self,
        _table: &str,
        surql: &str,
        params: Object,
        ttl: &str,
        relations: Vec<RelationPlan>,
    ) -> Result<QueryHash> {
        self.engine()?
            .query_raw(
                surql,
                params,
                QueryOptions {
                    ttl: Some(ttl.to_string()),
                    relations,
                },
            )
            .await
    }

    fn subscribe(&self, hash: &str, callback: RecordsCallback, immediate: bool) -> Subscription {
        match self.0.upgrade() {
            Some(engine) => engine.subscribe(hash, move |rows: &Arc<Vec<_>>| callback(rows), immediate),
            None => Subscription::noop(),
        }
    }
}
