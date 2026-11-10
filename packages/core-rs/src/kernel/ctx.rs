//! [`Ctx`]: the one seam between a saga and the world.
//!
//! A saga is an `async fn(ctx: &Ctx, ...) -> Result<_>`. It holds no adapter
//! and no state; every read, write, remote call, timer and dispatch goes
//! through its context, so the same saga runs against the real adapters or
//! the fakes in `crate::testing`.
//!
//! Every call is fenced: a saga started for one account (one store epoch)
//! fails with [`Error::AccountChanged`] at its next call once the bucket has
//! switched, so a late remote answer can never update the next account's
//! state or circuit. Async calls are checked before and after they run.

use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::kernel::events::{LogLevel, OutEvent, RuntimeEvent};
use crate::kernel::ports::{
    IdScope, IngestRecord, LocalOp, RegisterPlan, RegisterResult, Services, StatementResult, WriteMode,
};
use crate::runtime::Runtime;
use crate::state::ClientState;
use crate::value::{Object, Row, Value};

/// A saga's handle on the runtime. Cheap to clone.
#[derive(Clone)]
pub struct Ctx {
    rt: Runtime,
    /// The store epoch this saga runs for; `None` for the sagas that cause an
    /// account change.
    fence: Option<u64>,
}

impl Ctx {
    pub(crate) fn new(rt: Runtime, fence: Option<u64>) -> Self {
        Ctx { rt, fence }
    }

    /// The runtime this context drives.
    pub fn runtime(&self) -> &Runtime {
        &self.rt
    }

    /// Fail when the runtime is gone or the account changed under this saga.
    pub fn check(&self) -> Result<()> {
        if self.rt.is_disposed() {
            return Err(Error::Closed);
        }
        match self.fence {
            Some(epoch) if epoch != self.rt.adapters().local.epoch() => Err(Error::AccountChanged),
            _ => Ok(()),
        }
    }

    async fn fenced<R>(&self, fut: impl Future<Output = Result<R>>) -> Result<R> {
        self.check()?;
        let out = fut.await;
        self.check()?;
        out
    }

    // ---- state ---------------------------------------------------------------

    /// Read through a selector.
    pub fn read<R>(&self, select: impl FnOnce(&ClientState) -> R) -> Result<R> {
        self.check()?;
        Ok(self.rt.read(select))
    }

    /// Apply reducers. Subscribers, waiters and materialization are told after
    /// `f` returns.
    pub fn update<R>(&self, f: impl FnOnce(&mut ClientState) -> R) -> Result<R> {
        self.check()?;
        Ok(self.rt.update(f))
    }

    /// Suspend until `until` holds (checked after every update).
    pub async fn wait(&self, until: impl Fn(&ClientState) -> bool + 'static) -> Result<()> {
        self.fenced(self.rt.wait_for(Box::new(until))).await
    }

    // ---- pure ports ------------------------------------------------------------

    /// Milliseconds since the epoch.
    pub fn now(&self) -> Result<i64> {
        self.check()?;
        Ok((self.rt.adapters().now)())
    }

    pub fn id(&self, scope: IdScope) -> Result<String> {
        self.check()?;
        Ok((self.rt.adapters().ids)(scope))
    }

    /// Lowercase hex sha256 of `input`.
    pub fn hash(&self, input: &str) -> Result<String> {
        self.check()?;
        Ok((self.rt.adapters().hash)(input))
    }

    pub fn emit(&self, event: OutEvent) -> Result<()> {
        self.check()?;
        self.rt.emit(event);
        Ok(())
    }

    pub fn log(&self, level: LogLevel, message: impl Into<String>, data: Option<Value>) -> Result<()> {
        self.emit(OutEvent::Log {
            level,
            message: message.into(),
            data,
        })
    }

    pub fn dispatch(&self, event: RuntimeEvent) -> Result<()> {
        self.check()?;
        self.rt.dispatch(event);
        Ok(())
    }

    // ---- timers -----------------------------------------------------------------

    /// Dispatch `event` after `ms`; replaces a pending timer of the same key.
    pub fn timer_set(&self, key: impl Into<String>, ms: u64, event: RuntimeEvent) -> Result<()> {
        self.check()?;
        let weak = self.rt.downgrade();
        self.rt.adapters().timers.set(
            &key.into(),
            ms,
            Box::new(move || {
                if let Some(rt) = weak.upgrade() {
                    rt.dispatch(event);
                }
            }),
        );
        Ok(())
    }

    pub fn timer_clear(&self, key: &str) -> Result<()> {
        self.check()?;
        self.rt.adapters().timers.clear(key);
        Ok(())
    }

    // ---- local store ----------------------------------------------------------------

    pub fn local_get(&self, table: &str, id: &str) -> Result<Option<Row>> {
        self.check()?;
        self.rt.adapters().local.get(table, id)
    }

    pub fn local_get_many(&self, table: &str, ids: &[String]) -> Result<Vec<Row>> {
        self.check()?;
        self.rt.adapters().local.get_many(table, ids)
    }

    pub fn local_get_all(&self, table: &str) -> Result<Vec<Row>> {
        self.check()?;
        self.rt.adapters().local.get_all(table)
    }

    /// A write planned against an older store (`epoch`) is dropped: the bucket
    /// switched under it.
    fn epoch_fenced(&self, epoch: Option<u64>) -> bool {
        epoch.is_some_and(|e| e != self.rt.adapters().local.epoch())
    }

    pub fn local_put(&self, table: &str, id: &str, data: Row, mode: WriteMode, epoch: Option<u64>) -> Result<()> {
        self.check()?;
        if self.epoch_fenced(epoch) {
            return Ok(());
        }
        self.rt.adapters().local.put(table, id, data, mode)
    }

    pub fn local_delete(&self, table: &str, id: &str, epoch: Option<u64>) -> Result<()> {
        self.check()?;
        if self.epoch_fenced(epoch) {
            return Ok(());
        }
        self.rt.adapters().local.delete(table, id)
    }

    pub fn local_tx(&self, ops: Vec<LocalOp>, epoch: Option<u64>) -> Result<()> {
        self.check()?;
        if self.epoch_fenced(epoch) {
            return Ok(());
        }
        self.rt.adapters().local.tx(ops)
    }

    /// The store's current epoch (never mirror it in state: it moves on a
    /// bucket switch the state does not see).
    pub fn local_epoch(&self) -> Result<u64> {
        self.check()?;
        Ok(self.rt.adapters().local.epoch())
    }

    // ---- remote ---------------------------------------------------------------------

    async fn with_timeout<R>(timeout_ms: Option<u64>, fut: impl Future<Output = Result<R>>) -> Result<R> {
        match timeout_ms {
            None => fut.await,
            Some(ms) => tokio::time::timeout(Duration::from_millis(ms), fut)
                .await
                .unwrap_or(Err(Error::Timeout(ms))),
        }
    }

    /// A read: per-statement outcomes, in order.
    pub async fn remote_query(&self, sql: &str, vars: Option<Object>, timeout_ms: Option<u64>) -> Result<Vec<StatementResult>> {
        let remote = self.rt.adapters().remote.clone();
        self.fenced(Self::with_timeout(timeout_ms, remote.query(sql, vars))).await
    }

    /// A write: runs in the transport's write lane, one at a time.
    pub async fn remote_write(&self, sql: &str, vars: Option<Object>, timeout_ms: Option<u64>) -> Result<Vec<StatementResult>> {
        let remote = self.rt.adapters().remote.clone();
        self.fenced(Self::with_timeout(timeout_ms, remote.write(sql, vars))).await
    }

    /// Subscribe to `table`'s LIVE edges; every change is dispatched as
    /// [`RuntimeEvent::LiveChange`]. Resolves to the live uuid.
    pub async fn remote_live(&self, table: &str) -> Result<String> {
        let weak = self.rt.downgrade();
        let remote = self.rt.adapters().remote.clone();
        let cb: crate::kernel::ports::LiveCallback = Rc::new(move |hashes, rows| {
            if let Some(rt) = weak.upgrade() {
                rt.dispatch(RuntimeEvent::LiveChange { hashes, rows });
            }
        });
        self.fenced(remote.live(table, cb)).await
    }

    pub async fn remote_kill(&self, uuid: &str) -> Result<()> {
        let remote = self.rt.adapters().remote.clone();
        self.fenced(remote.kill(uuid)).await
    }

    // ---- circuit ----------------------------------------------------------------------

    pub fn ssp_register(&self, plan: RegisterPlan) -> Result<RegisterResult> {
        self.check()?;
        self.rt.adapters().ssp.register(plan)
    }

    pub fn ssp_unregister(&self, hash: &str) -> Result<()> {
        self.check()?;
        self.rt.adapters().ssp.unregister(hash)
    }

    pub fn ssp_ingest(&self, records: Vec<IngestRecord>) -> Result<()> {
        self.check()?;
        if records.is_empty() {
            return Ok(());
        }
        self.rt.adapters().ssp.ingest_many(records)
    }

    // ---- services ---------------------------------------------------------------------

    /// An async service call, fenced before and after:
    /// `ctx.service(|s| async move { s.local_connect(&b).await }).await?`.
    pub async fn service<R, F, Fut>(&self, call: F) -> Result<R>
    where
        F: FnOnce(Rc<dyn Services>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let services = self.rt.adapters().services.clone();
        self.fenced(call(services)).await
    }

    /// A synchronous service call (`ctx.service_sync(|s| s.auth_access())`).
    pub fn service_sync<R>(&self, call: impl FnOnce(&dyn Services) -> R) -> Result<R> {
        self.check()?;
        Ok(call(self.rt.adapters().services.as_ref()))
    }
}
