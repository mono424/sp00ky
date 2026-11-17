//! Talking to the server under test: raw `/sql` for loading and DDL, and the
//! Rust SDK (HTTP for root, WebSocket for record users and LIVE queries) for
//! the workloads, the way the SSP and the scheduler send their queries.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

pub const NS: &str = "main";
pub const DB: &str = "main";

/// One statement's outcome from `/sql`.
#[derive(Clone, Debug)]
pub struct Stmt {
    pub ok: bool,
    pub result: Value,
}

#[derive(Clone)]
pub struct Http {
    client: reqwest::Client,
    url: String,
}

impl Http {
    pub fn new(url: &str) -> Http {
        Http {
            client: reqwest::Client::builder()
                .pool_max_idle_per_host(32)
                .build()
                .expect("reqwest"),
            url: url.trim_end_matches('/').to_string(),
        }
    }

    /// Every statement's outcome. Errors only on transport failure.
    pub async fn sql_raw(&self, sql: &str, timeout: Duration) -> Result<Vec<Stmt>> {
        let response = self
            .client
            .post(format!("{}/sql", self.url))
            .basic_auth("root", Some("root"))
            .header("surreal-ns", NS)
            .header("surreal-db", DB)
            .header("Accept", "application/json")
            .timeout(timeout)
            .body(sql.to_string())
            .send()
            .await
            .context("POST /sql")?;
        let status = response.status();
        let text = response.text().await.context("reading /sql")?;
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|e| anyhow!("/sql {status}: {e}: {}", truncate(&text, 400)))?;
        let Value::Array(items) = parsed else {
            bail!("/sql {status}: {}", truncate(&text, 400));
        };
        Ok(items
            .into_iter()
            .map(|v| Stmt {
                ok: v.get("status").and_then(Value::as_str) == Some("OK"),
                result: v.get("result").cloned().unwrap_or(Value::Null),
            })
            .collect())
    }

    /// Every statement's result, or the first error.
    pub async fn sql(&self, sql: &str) -> Result<Vec<Value>> {
        self.sql_t(sql, Duration::from_secs(3600)).await
    }

    pub async fn sql_t(&self, sql: &str, timeout: Duration) -> Result<Vec<Value>> {
        let stmts = self.sql_raw(sql, timeout).await?;
        let mut out = Vec::with_capacity(stmts.len());
        for s in stmts {
            if !s.ok {
                bail!("{}\n-> {}", truncate(sql, 300), s.result);
            }
            out.push(s.result);
        }
        Ok(out)
    }

    pub async fn last(&self, sql: &str) -> Result<Value> {
        Ok(self.sql(sql).await?.pop().unwrap_or(Value::Null))
    }

    pub async fn version(&self) -> Result<String> {
        let text = self
            .client
            .get(format!("{}/version", self.url))
            .send()
            .await?
            .text()
            .await?;
        Ok(text.trim().to_string())
    }
}

pub fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... ({} bytes)", &s[..end], s.len())
}

/// A root SDK connection over HTTP, as the SSP and the scheduler hold one.
pub async fn root_http(url: &str) -> Result<Surreal<Any>> {
    let db = surrealdb::engine::any::connect(url).await?;
    db.signin(Root {
        username: "root".into(),
        password: "root".into(),
    })
    .await?;
    db.use_ns(NS).use_db(DB).await?;
    Ok(db)
}

/// A root SDK connection over WebSocket.
pub async fn root_ws(url: &str) -> Result<Surreal<Any>> {
    let ws = url.replacen("http://", "ws://", 1);
    let db = surrealdb::engine::any::connect(ws).await?;
    db.signin(Root {
        username: "root".into(),
        password: "root".into(),
    })
    .await?;
    db.use_ns(NS).use_db(DB).await?;
    Ok(db)
}

/// Run `sql` with `vars` through the SDK and return every statement's value
/// as JSON, or the first statement error.
pub async fn query(db: &Surreal<Any>, sql: &str, vars: &Value) -> Result<Vec<Value>> {
    let mut q = db.query(sql.to_string());
    if let Value::Object(map) = vars {
        for (k, v) in map {
            q = q.bind((k.clone(), to_surreal(v)));
        }
    }
    let mut response = q.await.map_err(|e| anyhow!("{e}"))?;
    let n = response.num_statements();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        match response.take::<surrealdb::types::Value>(i) {
            Ok(v) => out.push(v.into_json_value()),
            Err(e) => bail!("{}\n-> statement {i}: {e}", truncate(sql, 300)),
        }
    }
    Ok(out)
}

/// JSON to a SurrealDB value. Strings of the form `r"table:id"` become record
/// ids, `d"..."` datetimes: that is how the bench marks typed parameters,
/// which the SSP and scheduler bind as `RecordId` / `Datetime`.
pub fn to_surreal(v: &Value) -> surrealdb::types::Value {
    use surrealdb::types::{SurrealValue, Value as SV};
    match v {
        Value::String(s) if s.starts_with("r\"") && s.ends_with('"') => {
            let inner = &s[2..s.len() - 1];
            match surrealdb::types::RecordId::parse_simple(inner) {
                Ok(r) => SV::RecordId(r),
                Err(_) => SV::String(s.clone()),
            }
        }
        Value::String(s) if s.starts_with("d\"") && s.ends_with('"') => {
            let inner = &s[2..s.len() - 1];
            match inner.parse::<surrealdb::types::Datetime>() {
                Ok(d) => SV::Datetime(d),
                Err(_) => SV::String(s.clone()),
            }
        }
        Value::Array(items) => SV::Array(
            items
                .iter()
                .map(to_surreal)
                .collect::<surrealdb::types::Array>(),
        ),
        Value::Object(map) => {
            let mut obj = surrealdb::types::Object::new();
            for (k, v) in map {
                obj.insert(k.clone(), to_surreal(v));
            }
            SV::Object(obj)
        }
        other => other.clone().into_value(),
    }
}

/// Time one async call.
pub async fn timed<T, F: std::future::Future<Output = Result<T>>>(f: F) -> (Duration, Result<T>) {
    let started = Instant::now();
    let r = f.await;
    (started.elapsed(), r)
}

/// The SSP's database port over the SDK, binding parameters the way
/// `apps/ssp/src/adapters/db.rs` (`SurrealSdkDb`) does: plain JSON, so ids
/// travel as strings and the SQL types them (`type::record($id)`). Lets the
/// bench call the SSP's own functions (`ssp_node::tables`, `edges`, `jobs`).
#[derive(Clone)]
pub struct BenchDb {
    pub db: Surreal<Any>,
}

#[async_trait::async_trait]
impl ssp_node::ports::Db for BenchDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, serde_json::Value)],
    ) -> std::result::Result<Vec<serde_json::Value>, ssp_node::ports::DbError> {
        use ssp_node::ports::DbError;
        let started = Instant::now();
        let mut q = self.db.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| DbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value = response
                .take(i)
                .map_err(|e| DbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        if started.elapsed() > Duration::from_secs(2) {
            eprintln!(
                "    slow ({:.1} s): {}",
                started.elapsed().as_secs_f64(),
                truncate(surql, 240)
            );
        }
        Ok(out)
    }

    async fn version(&self) -> std::result::Result<String, ssp_node::ports::DbError> {
        self.db
            .version()
            .await
            .map(|v| v.to_string())
            .map_err(|e| ssp_node::ports::DbError::Transport(e.to_string()))
    }
}

/// The schedule engine's database port, over the same SDK handle.
#[async_trait::async_trait]
impl schedule_core::db::ScheduleDb for BenchDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, serde_json::Value)],
    ) -> std::result::Result<Vec<serde_json::Value>, schedule_core::db::ScheduleDbError> {
        use schedule_core::db::ScheduleDbError;
        <Self as ssp_node::ports::Db>::query(self, surql, binds)
            .await
            .map_err(|e| match e {
                ssp_node::ports::DbError::Auth(m) => ScheduleDbError::Auth(m),
                ssp_node::ports::DbError::Transport(m) => ScheduleDbError::Transport(m),
                ssp_node::ports::DbError::Query(m) => ScheduleDbError::Query(m),
            })
    }
}
