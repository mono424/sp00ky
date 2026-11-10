//! Native push registration on the client: [`Engine::push`] builds the
//! engine-thread [`PushModule`] (Dart `Sp00kyClient.push`), and [`Push`] is
//! its thread-safe facade on [`Sp00kyClient`].

use std::future::Future;
use std::rc::Rc;

use futures::future::FutureExt;

use crate::client::engine::Engine;
use crate::client::handle::Sp00kyClient;
use crate::error::{Error, Result};
use crate::modules::bucket::RemoteQuery;
use crate::modules::push::{
    PushDevice, PushDeviceUpdate, PushError, PushInfo, PushMessage, PushMessageInput, PushModule,
    PushRegisterOptions, PushSyncOptions, PushSyncStatus, PushToken,
};
use crate::value::Object;

impl Engine {
    /// Native push registration for this device (APNs / FCM). The token comes
    /// from the platform. Built on first use, which also installs the
    /// sign-out hook that unregisters the device while the session is still
    /// valid, so touch it early. Requires a remote endpoint.
    pub fn push(self: &Rc<Self>) -> Result<PushModule> {
        if let Some(p) = self.push.borrow().as_ref() {
            return Ok(p.clone());
        }
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let auth = self
            .auth()
            .cloned()
            .ok_or_else(|| Error::NoRemote("push requires a remote endpoint".into()))?;
        // Weak: the module lives on the engine and must not keep it alive.
        let weak = Rc::downgrade(self);
        let remote: RemoteQuery = Rc::new(move |sql: String, vars: Option<Object>| {
            let weak = weak.clone();
            async move {
                let engine = weak.upgrade().ok_or(Error::Closed)?;
                engine.query_remote(&sql, vars).await
            }
            .boxed_local()
        });
        let module = PushModule::new(remote, Rc::new(auth), self.persistence().clone());
        module.attach();
        *self.push.borrow_mut() = Some(module.clone());
        Ok(module)
    }
}

impl Sp00kyClient {
    /// Native push registration for this device (APNs / FCM), see
    /// [`PushModule`]. The first call builds the module on the engine thread
    /// and installs its sign-out hook, so touch it early (at startup).
    pub fn push(&self) -> Push {
        self.fire(|w| {
            if let Err(e) = w.engine.push() {
                tracing::debug!(target: "spooky", "push is unavailable: {e}");
            }
        });
        Push {
            client: self.clone(),
        }
    }
}

/// The push module of a [`Sp00kyClient`], callable from any thread. Each call
/// runs on the engine thread. A client without a remote endpoint (or one that
/// is closed) answers every call with a [`PushErrorCode::Server`] error.
///
/// [`PushErrorCode::Server`]: crate::modules::push::PushErrorCode::Server
#[derive(Clone)]
pub struct Push {
    client: Sp00kyClient,
}

impl std::fmt::Debug for Push {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Push").finish_non_exhaustive()
    }
}

impl Push {
    async fn run<T, F, Fut>(&self, f: F) -> std::result::Result<T, PushError>
    where
        T: Send + 'static,
        F: FnOnce(PushModule) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, PushError>> + 'static,
    {
        self.client
            .call(move |w| async move {
                Ok(match w.engine.push() {
                    Ok(module) => f(module).await,
                    Err(e) => Err(PushError::from(e)),
                })
            })
            .await
            .unwrap_or_else(|e| Err(PushError::from(e)))
    }

    /// `fn::push::info()`, cached; `refresh` reads it again.
    pub async fn info(&self, refresh: bool) -> std::result::Result<PushInfo, PushError> {
        self.run(move |p| async move { p.info(refresh).await })
            .await
    }

    /// This device has a stored, enabled registration for the signed-in user.
    pub async fn is_registered(&self) -> std::result::Result<bool, PushError> {
        self.run(|p| async move { p.is_registered().await }).await
    }

    /// Register this device for the signed-in user. Remembers the choice.
    pub async fn register(
        &self,
        token: PushToken,
        options: PushRegisterOptions,
    ) -> std::result::Result<PushDevice, PushError> {
        self.run(move |p| async move { p.register(token, options).await })
            .await
    }

    /// Turn push off on this device (`all`: on every device of the user).
    pub async fn unsubscribe(&self, all: bool) -> std::result::Result<i64, PushError> {
        self.run(move |p| async move { p.unsubscribe(all).await })
            .await
    }

    /// Bring the server in line with this device. Never fails: a client that
    /// cannot run it answers [`PushSyncStatus::Error`].
    pub async fn sync(&self, token: Option<PushToken>, options: PushSyncOptions) -> PushSyncStatus {
        self.run(move |p| async move { Ok(p.sync(token, options).await) })
            .await
            .unwrap_or(PushSyncStatus::Error)
    }

    /// Change this device's label / rules / meta.
    pub async fn update(
        &self,
        changes: PushDeviceUpdate,
    ) -> std::result::Result<Option<PushDevice>, PushError> {
        self.run(move |p| async move { p.update(changes).await })
            .await
    }

    /// Every device of the signed-in user (web and native).
    pub async fn devices(&self) -> std::result::Result<Vec<PushDevice>, PushError> {
        self.run(|p| async move { p.devices().await }).await
    }

    /// A push to yourself, now or at [`PushMessageInput::send_at`].
    pub async fn notify(
        &self,
        message: PushMessageInput,
    ) -> std::result::Result<PushMessage, PushError> {
        self.run(move |p| async move { p.notify(&message).await })
            .await
    }

    /// Cancel one of your scheduled pushes.
    pub async fn cancel(&self, id: &str) -> std::result::Result<bool, PushError> {
        let id = id.to_string();
        self.run(move |p| async move { p.cancel(&id).await }).await
    }

    /// A visible test push to every device of the user.
    pub async fn test(
        &self,
        title: Option<&str>,
        body: Option<&str>,
    ) -> std::result::Result<PushMessage, PushError> {
        let (title, body) = (title.map(str::to_string), body.map(str::to_string));
        self.run(move |p| async move { p.test(title.as_deref(), body.as_deref()).await })
            .await
    }

    /// Whether sign-out removes this device's row (on by default).
    pub async fn unsubscribe_on_sign_out(&self) -> std::result::Result<bool, PushError> {
        self.run(|p| async move { Ok(p.unsubscribe_on_sign_out()) })
            .await
    }

    pub async fn set_unsubscribe_on_sign_out(
        &self,
        on: bool,
    ) -> std::result::Result<(), PushError> {
        self.run(move |p| async move {
            p.set_unsubscribe_on_sign_out(on);
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::modules::push::{PushErrorCode, PushSyncOptions, PushSyncStatus};
    use crate::{DatabaseConfig, Error, Schema, Sp00kyClient, Sp00kyConfig, StoreType};

    fn local_only() -> Sp00kyConfig {
        Sp00kyConfig::new(
            DatabaseConfig::new("test", "test").store(StoreType::Memory),
            Schema::default(),
            "",
        )
    }

    #[tokio::test]
    async fn a_client_without_a_remote_refuses_push_without_panicking() {
        let client = Sp00kyClient::open(local_only()).await.unwrap();
        let push = client.push();
        let err = push.devices().await.unwrap_err();
        assert_eq!(err.code, PushErrorCode::Server);
        assert!(matches!(err.cause, Some(Error::NoRemote(_))), "{err:?}");
        assert_eq!(
            push.sync(None, PushSyncOptions::default()).await,
            PushSyncStatus::Error
        );
        client.close().await.unwrap();
        let err = push.is_registered().await.unwrap_err();
        assert_eq!(err.code, PushErrorCode::Server);
    }
}
