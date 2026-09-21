//! `hetzner` machine provider: ask Sp00ky Cloud for the machine.
//!
//! A tenant never holds cloud credentials. The scheduler states what it needs
//! over the same internal, per-project route family the dashboard's cloud
//! actions already use ([`CloudLink`]), and the control plane creates the VM,
//! enforces the caps again on its side, and runs its own reaper, so a buggy or
//! dead scheduler can neither overspend nor leak machines.
//!
//! ```text
//! POST   /v1/internal/projects/{project}/pools/{pool}/machines
//! GET    /v1/internal/projects/{project}/pools/{pool}/machines
//! DELETE /v1/internal/projects/{project}/pool-machines/{machine key}
//! ```
//!
//! The create call carries the machine's bearer token and the pool listener's
//! public URL: the control plane puts both into the VM's bootstrap so its agent
//! can dial in. The token opens exactly one machine row and dies with it. What
//! is deliberately NOT sent is the backend's environment: secrets reach a
//! machine through the agent's authenticated `hello`, never through provider
//! metadata.

use axum::http::StatusCode;
use pool_core::spec::machine_key;
use pool_core::{CreateMachine, MachineProvider, ProviderError, ProviderMachine};
use reqwest::Method;
use serde_json::{json, Value};

use crate::admin::cloud::CloudLink;

pub struct CloudProvider {
    link: CloudLink,
    /// URL of this scheduler's pool listener as a machine on the internet sees it.
    pool_url: String,
    /// Image carrying the `spky-agent` binary the VM installs. Named by the
    /// scheduler, not the control plane, so agent and scheduler (which speak the
    /// pool protocol to each other) are always the same release.
    agent_image: String,
    token_for: Box<dyn Fn(&str) -> String + Send + Sync>,
}

impl CloudProvider {
    pub fn new(
        link: CloudLink,
        pool_url: String,
        agent_image: String,
        token_for: Box<dyn Fn(&str) -> String + Send + Sync>,
    ) -> Self {
        Self {
            link,
            pool_url,
            agent_image,
            token_for,
        }
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ProviderError> {
        match self.link.call(method, path, body).await {
            Ok((_, value)) => Ok(value),
            Err((status, body)) => {
                let message = body
                    .0
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("control plane refused")
                    .to_string();
                Err(classify(status, message))
            }
        }
    }
}

/// Which failures are worth asking again soon, and which are a "no".
///
/// `CloudLink` reports an unreachable control plane (and its own rewritten
/// upstream 401/403) as a 5xx, so both land in `Transient`: the breaker's
/// backoff is the right response to a control plane that is down or restarting.
fn classify(status: StatusCode, message: String) -> ProviderError {
    match status.as_u16() {
        // Over a cap, plan does not include pools, provider out of capacity.
        402 | 403 | 409 | 422 => ProviderError::Refused(message),
        408 | 425 | 429 | 500..=599 => ProviderError::Transient(message),
        _ => ProviderError::Other(message),
    }
}

/// The spec the control plane needs, minus everything secret.
fn container_without_env(container: &Value) -> Value {
    let mut out = container.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.remove("env");
    }
    out
}

fn machine_from(value: &Value) -> Option<ProviderMachine> {
    Some(ProviderMachine {
        provider_id: value.get("provider_id")?.as_str()?.to_string(),
        machine_id: value.get("machine_id")?.as_str()?.to_string(),
    })
}

#[async_trait::async_trait]
impl MachineProvider for CloudProvider {
    async fn create(&self, req: &CreateMachine) -> Result<ProviderMachine, ProviderError> {
        let body = json!({
            "machine_id": req.machine_id,
            "machine_type": req.machine_type,
            "locations": req.locations,
            "slots": req.slots,
            "container": container_without_env(&req.container),
            "agent": {
                "pool_url": self.pool_url,
                "token": (self.token_for)(&req.machine_id),
                "image": self.agent_image,
            },
        });
        let path = format!("/pools/{}/machines", req.pool);
        let made = self.call(Method::POST, &path, Some(body)).await?;
        machine_from(&made)
            .ok_or_else(|| ProviderError::Other("control plane answered without a machine".into()))
    }

    async fn destroy(
        &self,
        machine_id: &str,
        _provider_id: Option<&str>,
    ) -> Result<(), ProviderError> {
        // By the machine's own key: the control plane keeps the provider id, and
        // a row that never recorded one must still be destroyable.
        let path = format!("/pool-machines/{}", machine_key(machine_id));
        match self.call(Method::DELETE, &path, None).await {
            Ok(_) => Ok(()),
            // Already gone is the outcome we wanted.
            Err(ProviderError::Other(m)) if m.contains("not found") => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn list(&self, pool: &str) -> Result<Vec<ProviderMachine>, ProviderError> {
        let listed = self
            .call(Method::GET, &format!("/pools/{pool}/machines"), None)
            .await?;
        Ok(listed
            .get("machines")
            .and_then(Value::as_array)
            .map(|all| all.iter().filter_map(machine_from).collect())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path, State};
    use axum::http::HeaderMap;
    use axum::routing::{delete, post};
    use axum::{Json, Router};

    use super::*;

    #[derive(Default)]
    struct Plane {
        created: Mutex<Vec<Value>>,
        deleted: Mutex<Vec<String>>,
        refuse_with: Mutex<Option<u16>>,
    }

    fn authed(headers: &HeaderMap) -> bool {
        headers.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer cluster-secret")
    }

    async fn create(
        State(p): State<Arc<Plane>>,
        Path((project, pool)): Path<(String, String)>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        if !authed(&headers) {
            return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "no" })));
        }
        if let Some(code) = *p.refuse_with.lock().unwrap() {
            let status = StatusCode::from_u16(code).unwrap();
            return (status, Json(json!({ "error": "pool machine cap reached" })));
        }
        assert_eq!((project.as_str(), pool.as_str()), ("whitepawn", "render"));
        let id = body["machine_id"].as_str().unwrap().to_string();
        p.created.lock().unwrap().push(body);
        (
            StatusCode::CREATED,
            Json(json!({ "provider_id": "srv-1", "machine_id": id })),
        )
    }

    async fn list(State(p): State<Arc<Plane>>) -> Json<Value> {
        let machines: Vec<Value> = p
            .created
            .lock()
            .unwrap()
            .iter()
            .map(|c| json!({ "provider_id": "srv-1", "machine_id": c["machine_id"] }))
            .collect();
        Json(json!({ "machines": machines }))
    }

    async fn destroy(
        State(p): State<Arc<Plane>>,
        Path((_, key)): Path<(String, String)>,
    ) -> (StatusCode, Json<Value>) {
        if key == "missing" {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "machine not found" })),
            );
        }
        p.deleted.lock().unwrap().push(key);
        (StatusCode::OK, Json(json!({ "status": "destroyed" })))
    }

    async fn provider() -> (CloudProvider, Arc<Plane>) {
        let plane = Arc::new(Plane::default());
        let router = Router::new()
            .route(
                "/v1/internal/projects/:project/pools/:pool/machines",
                post(create).get(list),
            )
            .route(
                "/v1/internal/projects/:project/pool-machines/:key",
                delete(destroy),
            )
            .with_state(plane.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let link = CloudLink::new(url, "whitepawn".into(), "cluster-secret".into()).unwrap();
        let provider = CloudProvider::new(
            link,
            "https://whitepawn-pool.example".into(),
            "mono424/spooky-agent:test".into(),
            Box::new(|id| format!("token-for-{id}")),
        );
        (provider, plane)
    }

    fn request() -> CreateMachine {
        CreateMachine {
            machine_id: "_00_machine:abc".into(),
            pool: "render".into(),
            machine_type: Some("cpx32".into()),
            locations: vec!["fsn1".into()],
            slots: 1,
            container: json!({ "image": "whitepawn/renderer", "port": 8080,
                               "env": { "RTMP_KEY": "super-secret" } }),
        }
    }

    #[tokio::test]
    async fn create_sends_the_agent_bootstrap_and_never_the_backend_environment() {
        let (provider, plane) = provider().await;
        let made = provider.create(&request()).await.unwrap();
        assert_eq!(
            made,
            ProviderMachine {
                provider_id: "srv-1".into(),
                machine_id: "_00_machine:abc".into()
            }
        );

        let sent = plane.created.lock().unwrap()[0].clone();
        assert_eq!(sent["agent"]["pool_url"], "https://whitepawn-pool.example");
        assert_eq!(sent["agent"]["token"], "token-for-_00_machine:abc");
        assert_eq!(sent["agent"]["image"], "mono424/spooky-agent:test");
        assert_eq!(sent["machine_type"], "cpx32");
        assert!(
            sent["container"].get("env").is_none(),
            "secrets travel through hello, not provider metadata"
        );
        assert!(!sent.to_string().contains("super-secret"));
    }

    #[tokio::test]
    async fn a_cap_is_a_refusal_and_an_outage_is_transient() {
        let (provider, plane) = provider().await;
        *plane.refuse_with.lock().unwrap() = Some(409);
        assert!(
            matches!(provider.create(&request()).await, Err(ProviderError::Refused(m)) if m.contains("cap"))
        );
        *plane.refuse_with.lock().unwrap() = Some(503);
        assert!(matches!(
            provider.create(&request()).await,
            Err(ProviderError::Transient(_))
        ));
    }

    #[tokio::test]
    async fn destroy_goes_by_machine_key_and_already_gone_is_success() {
        let (provider, plane) = provider().await;
        provider.destroy("_00_machine:abc", None).await.unwrap();
        assert_eq!(*plane.deleted.lock().unwrap(), vec!["abc".to_string()]);
        provider
            .destroy("_00_machine:missing", Some("srv-9"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_reports_what_the_control_plane_has_for_the_pool() {
        let (provider, _plane) = provider().await;
        assert!(provider.list("render").await.unwrap().is_empty());
        provider.create(&request()).await.unwrap();
        let listed = provider.list("render").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].machine_id, "_00_machine:abc");
    }
}
