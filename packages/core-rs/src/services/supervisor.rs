//! Keeps the remote WebSocket alive for the whole life of the process.
//!
//! The transport has no reconnect loop of its own (`connect` is called once
//! and nothing ever re-dials), so unlike the browser client's supervisor, which
//! coordinates with the SurrealDB SDK's own retry, this one owns every recovery
//! path:
//!
//! 1. **The socket closes.** `Disconnected` fires; the revive loop re-opens it
//!    on exponential backoff and never gives up.
//! 2. **The socket never closes at all.** A half-open connection: the peer is
//!    gone (NAT timeout, wifi switch, the device slept) but no FIN arrives, so
//!    nothing fires. The heartbeat detects this and forces the teardown that
//!    case 1 then repairs.
//! 3. **A wake signal.** Coming back online or returning to the foreground is
//!    the strongest hint a reconnect will now succeed, so
//!    [`ConnectionSupervisor::wake`] probes immediately rather than waiting out
//!    a backoff scheduled under worse conditions. The core is
//!    framework-agnostic, so the host calls it (in Flutter, from
//!    `AppLifecycleState.resumed`).
//!
//! Single-threaded: timers run on the current tokio `LocalSet`.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::time::Duration;

use futures::future::{FutureExt, LocalBoxFuture};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::services::remote::RemoteDatabaseService;
use crate::surreal::rpc::TransportEvent;
use crate::types::{ConnectionState, ReconnectConfig};

/// An async hook the supervisor drives.
pub type AsyncHook = Rc<dyn Fn() -> LocalBoxFuture<'static, Result<()>>>;

/// What the supervisor needs from the transport.
#[derive(Clone)]
pub struct SupervisorHooks {
    /// Re-open the socket (with its handshake).
    pub reconnect: AsyncHook,
    /// One end-to-end round trip.
    pub probe: AsyncHook,
    /// Drop the socket; its `Disconnected` event drives the revive loop.
    pub force_close: Rc<dyn Fn() -> LocalBoxFuture<'static, ()>>,
    pub is_connected: Rc<dyn Fn() -> bool>,
}

impl SupervisorHooks {
    /// Hooks over a [`RemoteDatabaseService`]. The client wiring that also
    /// retries auth verification after a probe builds its own `probe`.
    pub fn for_remote(remote: &RemoteDatabaseService) -> Self {
        let (r1, r2, r3, r4) = (
            remote.clone(),
            remote.clone(),
            remote.clone(),
            remote.clone(),
        );
        SupervisorHooks {
            reconnect: Rc::new(move || {
                let r = r1.clone();
                async move { r.connect().await }.boxed_local()
            }),
            probe: Rc::new(move || r2.probe().boxed_local()),
            force_close: Rc::new(move || {
                let r = r3.clone();
                async move { r.force_close().await }.boxed_local()
            }),
            is_connected: Rc::new(move || r4.is_connected()),
        }
    }
}

type Subscriber = Rc<dyn Fn(ConnectionState)>;

struct Inner {
    hooks: SupervisorHooks,
    config: ReconnectConfig,
    /// Milliseconds. Injectable so a test can drive the wake rate limit by
    /// hand.
    now: RefCell<Rc<dyn Fn() -> i64>>,
    events: RefCell<Option<broadcast::Receiver<TransportEvent>>>,

    state: Cell<ConnectionState>,
    subscribers: RefCell<Vec<(u64, Subscriber)>>,
    next_subscriber: Cell<u64>,
    listener: RefCell<Option<JoinHandle<()>>>,

    started: Cell<bool>,
    disposed: Cell<bool>,

    heartbeat_timer: RefCell<Option<JoinHandle<()>>>,
    heartbeat_in_flight: Cell<bool>,
    heartbeat_failures: Cell<u32>,

    revive_timer: RefCell<Option<JoinHandle<()>>>,
    revive_attempts: Cell<u32>,
    /// `None` until the first wake probe: the very first one is never
    /// rate-limited.
    last_wake_probe_at: Cell<Option<i64>>,
    reviving: Cell<bool>,

    /// Set while the host reports itself offline. Retrying a socket against a
    /// down interface only burns backoff, so the loop parks until `wake`.
    suspended: Cell<bool>,
}

/// Supervises the remote socket. Cheap to clone; all clones share one loop.
#[derive(Clone)]
pub struct ConnectionSupervisor(Rc<Inner>);

/// Milliseconds on tokio's clock, which `tokio::time::pause` freezes too.
fn monotonic_ms() -> i64 {
    thread_local! {
        static ORIGIN: tokio::time::Instant = tokio::time::Instant::now();
    }
    ORIGIN.with(|origin| origin.elapsed().as_millis() as i64)
}

/// Run `hook` to completion on its own task and wait for it at most `ms`. A
/// timed-out hook keeps running, as an un-awaited future would: cancelling a
/// half-done connect could leave the socket open without its handshake.
async fn bounded(hook: LocalBoxFuture<'static, Result<()>>, ms: u64, what: &str) -> Result<()> {
    let task = tokio::task::spawn_local(hook);
    match tokio::time::timeout(Duration::from_millis(ms), task).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => Err(Error::other(format!("{what} task failed: {e}"))),
        Err(_) => Err(Error::other(format!("{what} timed out after {ms}ms"))),
    }
}

impl ConnectionSupervisor {
    /// How many consecutive heartbeat failures it takes to tear the socket
    /// down.
    ///
    /// The probe waits for a slot in the same lanes as every other RPC
    /// (deliberately, see `beat`), so it cannot tell a WEDGED lane from a
    /// merely BUSY one. A single slow window (a large sync burst, one heavy app
    /// query) must not force-close a healthy socket, because the resulting
    /// reconnect re-registers every active query a second later: that
    /// self-inflicted teardown manufactures the very reconnect storms this
    /// type exists to survive. A genuinely dead socket still fails every probe
    /// and is torn down one interval later than it would have been.
    pub const FAILURES_BEFORE_TEARDOWN: u32 = 2;
    /// Retry delay after an inconclusive (first) heartbeat failure.
    pub const HEARTBEAT_RETRY_MS: u64 = 5000;
    /// Floor between probes triggered by wake events.
    pub const WAKE_PROBE_MIN_INTERVAL_MS: i64 = 10_000;
    const REVIVE_BASE_MS: u64 = 1000;

    /// `events` are the transport's lifecycle events; only those after
    /// [`ConnectionSupervisor::start`] count.
    pub fn new(
        hooks: SupervisorHooks,
        events: broadcast::Receiver<TransportEvent>,
        config: ReconnectConfig,
    ) -> Self {
        ConnectionSupervisor(Rc::new(Inner {
            hooks,
            config,
            now: RefCell::new(Rc::new(monotonic_ms)),
            events: RefCell::new(Some(events)),
            state: Cell::new(ConnectionState::Disconnected),
            subscribers: RefCell::default(),
            next_subscriber: Cell::new(0),
            listener: RefCell::new(None),
            started: Cell::new(false),
            disposed: Cell::new(false),
            heartbeat_timer: RefCell::new(None),
            heartbeat_in_flight: Cell::new(false),
            heartbeat_failures: Cell::new(0),
            revive_timer: RefCell::new(None),
            revive_attempts: Cell::new(0),
            last_wake_probe_at: Cell::new(None),
            reviving: Cell::new(false),
            suspended: Cell::new(false),
        }))
    }

    /// Supervise a [`RemoteDatabaseService`] with [`SupervisorHooks::for_remote`].
    pub fn for_remote(remote: &RemoteDatabaseService, config: ReconnectConfig) -> Self {
        Self::new(SupervisorHooks::for_remote(remote), remote.events(), config)
    }

    /// Replace the clock (milliseconds) the wake rate limit reads.
    pub fn with_clock(self, now: impl Fn() -> i64 + 'static) -> Self {
        *self.0.now.borrow_mut() = Rc::new(now);
        self
    }

    pub fn connection(&self) -> ConnectionState {
        self.0.state.get()
    }

    /// Observe transport state. Fires immediately with the current value and
    /// again on every change. Returns an unsubscribe.
    pub fn subscribe(&self, cb: impl Fn(ConnectionState) + 'static) -> Box<dyn FnOnce()> {
        let cb: Subscriber = Rc::new(cb);
        cb(self.0.state.get());
        let id = self.0.next_subscriber.get() + 1;
        self.0.next_subscriber.set(id);
        self.0.subscribers.borrow_mut().push((id, cb));
        let weak = Rc::downgrade(&self.0);
        Box::new(move || {
            if let Some(inner) = weak.upgrade() {
                inner.subscribers.borrow_mut().retain(|(i, _)| *i != id);
            }
        })
    }

    /// Begin supervising. Call once, after the initial connect. Idempotent.
    pub fn start(&self) {
        let inner = &self.0;
        if inner.started.get() || inner.disposed.get() {
            return;
        }
        inner.started.set(true);
        inner.set_state(if (inner.hooks.is_connected)() {
            ConnectionState::Connected
        } else {
            ConnectionState::Disconnected
        });

        let events = inner.events.borrow_mut().take();
        if let Some(events) = events {
            // Only events from now on: the current state was just read.
            let mut events = events.resubscribe();
            let weak = Rc::downgrade(inner);
            let listener = tokio::task::spawn_local(async move {
                loop {
                    let event = match events.recv().await {
                        Ok(event) => event,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Events were missed: act on the transport as it is now.
                            let Some(inner) = weak.upgrade() else { return };
                            if (inner.hooks.is_connected)() {
                                TransportEvent::Connected
                            } else {
                                TransportEvent::Disconnected
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    };
                    let Some(inner) = weak.upgrade() else { return };
                    match event {
                        TransportEvent::Connected => inner.on_connected(),
                        TransportEvent::Disconnected => inner.on_disconnected(),
                    }
                }
            });
            *inner.listener.borrow_mut() = Some(listener);
        }

        if inner.state.get() == ConnectionState::Connected {
            inner.start_heartbeat();
        } else {
            inner.schedule_revive();
        }
    }

    /// The host lost its network. Park reconnects rather than burning backoff.
    pub fn suspend(&self) {
        let inner = &self.0;
        inner.suspended.set(true);
        inner.stop_heartbeat();
        inner.clear_revive_timer();
        inner.set_state(ConnectionState::Disconnected);
    }

    /// Reset the backoff and act on whichever problem is present: reconnect
    /// when the socket is gone, otherwise probe it (a half-open socket is
    /// exactly what a sleep/wake cycle produces).
    ///
    /// Probes from wake triggers are rate-limited: a healthy socket does not
    /// become unhealthy because the app was backgrounded for four seconds, and
    /// an unthrottled probe per resume would trip the teardown above.
    pub fn wake(&self, reason: &str) {
        let inner = &self.0;
        inner.suspended.set(false);
        if inner.disposed.get() {
            return;
        }
        let now = (inner.now.borrow())();
        let connected = (inner.hooks.is_connected)();
        if connected
            && inner
                .last_wake_probe_at
                .get()
                .is_some_and(|last| now - last < Self::WAKE_PROBE_MIN_INTERVAL_MS)
        {
            return;
        }
        inner.last_wake_probe_at.set(Some(now));
        tracing::debug!(target: "spooky", "Wake trigger ({reason}); probing the connection");
        inner.revive_attempts.set(0);
        if connected {
            inner.stop_heartbeat();
            tokio::task::spawn_local(Inner::beat(inner.clone()));
            return;
        }
        inner.clear_revive_timer();
        tokio::task::spawn_local(Inner::revive(inner.clone()));
    }

    /// Stop supervising: timers cancelled, subscribers dropped.
    pub fn dispose(&self) {
        let inner = &self.0;
        inner.disposed.set(true);
        inner.started.set(false);
        inner.stop_heartbeat();
        inner.clear_revive_timer();
        if let Some(listener) = inner.listener.borrow_mut().take() {
            listener.abort();
        }
        inner.subscribers.borrow_mut().clear();
    }
}

impl Inner {
    fn is_connected(&self) -> bool {
        (self.hooks.is_connected)()
    }

    fn parked(&self) -> bool {
        self.disposed.get() || self.suspended.get()
    }

    fn set_state(&self, next: ConnectionState) {
        if self.state.get() == next {
            return;
        }
        self.state.set(next);
        tracing::info!(target: "spooky", "Connection state changed: {next:?}");
        let subscribers: Vec<Subscriber> = self
            .subscribers
            .borrow()
            .iter()
            .map(|(_, cb)| cb.clone())
            .collect();
        for cb in subscribers {
            cb(next);
        }
    }

    fn on_connected(self: &Rc<Self>) {
        self.revive_attempts.set(0);
        self.clear_revive_timer();
        self.set_state(ConnectionState::Connected);
        self.start_heartbeat();
    }

    fn on_disconnected(self: &Rc<Self>) {
        self.set_state(ConnectionState::Disconnected);
        self.stop_heartbeat();
        self.schedule_revive();
    }

    /// A one-shot timer. The task drops its own handle before acting, so
    /// nothing can abort an action already under way (a Dart `Timer` cancelled
    /// after firing does not cancel what it started either).
    fn timer(
        self: &Rc<Self>,
        ms: u64,
        slot: fn(&Inner) -> &RefCell<Option<JoinHandle<()>>>,
        act: fn(Rc<Inner>) -> LocalBoxFuture<'static, ()>,
    ) {
        let weak: Weak<Inner> = Rc::downgrade(self);
        let task = tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            let Some(inner) = weak.upgrade() else { return };
            slot(&inner).borrow_mut().take();
            act(inner).await;
        });
        if let Some(old) = slot(self).borrow_mut().replace(task) {
            old.abort();
        }
    }

    // ---- revive loop ---------------------------------------------------------------

    fn clear_revive_timer(&self) {
        if let Some(task) = self.revive_timer.borrow_mut().take() {
            task.abort();
        }
    }

    /// Queue the next reconnect on exponential backoff, capped at
    /// [`ReconnectConfig::retry_delay_max_ms`]. Never gives up: the process is
    /// expected to outlive any outage.
    fn schedule_revive(self: &Rc<Self>) {
        if self.parked() {
            return;
        }
        if self.revive_timer.borrow().is_some() || self.reviving.get() {
            return;
        }
        let shift = self.revive_attempts.get().min(30);
        let delay = self
            .config
            .retry_delay_max_ms
            .min(ConnectionSupervisor::REVIVE_BASE_MS << shift);
        self.timer(
            delay,
            |i| &i.revive_timer,
            |i| Inner::revive(i).boxed_local(),
        );
    }

    async fn revive(self: Rc<Self>) {
        if self.parked() || self.reviving.get() {
            return;
        }
        if self.is_connected() {
            self.revive_attempts.set(0);
            return;
        }
        self.reviving.set(true);
        let attempt = self.revive_attempts.get() + 1;
        self.revive_attempts.set(attempt);
        self.set_state(ConnectionState::Reconnecting);
        tracing::info!(target: "spooky", "Re-opening the remote connection (attempt {attempt})");
        // Bounded: a black-holed endpoint never answers, and an unbounded
        // attempt would park the loop inside it forever, never retrying, never
        // reporting the connection as gone.
        //
        // `revive_attempts` and the heartbeat are reset by the `Connected`
        // listener: that is the only signal the handshake actually completed.
        if let Err(e) = bounded(
            (self.hooks.reconnect)(),
            self.config.connect_timeout_ms,
            "Connect",
        )
        .await
        {
            tracing::warn!(target: "spooky", "Reconnect attempt {attempt} failed; will retry: {e}");
        }
        self.reviving.set(false);
        if !self.is_connected() {
            self.schedule_revive();
        }
    }

    // ---- heartbeat watchdog -----------------------------------------------------------

    fn stop_heartbeat(&self) {
        if let Some(task) = self.heartbeat_timer.borrow_mut().take() {
            task.abort();
        }
    }

    fn start_heartbeat(self: &Rc<Self>) {
        self.stop_heartbeat();
        if self.parked() || self.config.heartbeat_interval_ms == 0 {
            return;
        }
        self.heartbeat_after(self.config.heartbeat_interval_ms);
    }

    fn heartbeat_after(self: &Rc<Self>, ms: u64) {
        self.timer(ms, |i| &i.heartbeat_timer, |i| Inner::beat(i).boxed_local());
    }

    /// Probe the server end to end. Deliberately goes through the same lanes
    /// every other remote call uses (`RemoteDatabaseService::probe`), so a lane
    /// wedged behind a stuck RPC fails the heartbeat instead of being invisible
    /// to it.
    async fn beat(self: Rc<Self>) {
        if self.parked() || !self.is_connected() {
            return;
        }
        if self.heartbeat_in_flight.get() {
            self.start_heartbeat();
            return;
        }
        self.heartbeat_in_flight.set(true);
        let probe = bounded(
            (self.hooks.probe)(),
            self.config.heartbeat_timeout_ms,
            "Heartbeat",
        )
        .await;
        match probe {
            Ok(()) => {
                self.heartbeat_failures.set(0);
                // A round trip is the strongest evidence there is that the
                // socket works, so it also RECONCILES the reported state: a
                // `suspend()` that forced `Disconnected` without the transport's
                // agreement would otherwise stick for the life of the process,
                // traffic flowing while every indicator says offline.
                self.set_state(ConnectionState::Connected);
                self.start_heartbeat();
            }
            Err(e) => {
                let failures = self.heartbeat_failures.get() + 1;
                self.heartbeat_failures.set(failures);
                if failures < ConnectionSupervisor::FAILURES_BEFORE_TEARDOWN {
                    // Inconclusive: the probe shares lanes with ordinary
                    // traffic, so this may be a busy window rather than a dead
                    // socket. Re-probe soon instead of tearing down a
                    // connection that is probably fine.
                    tracing::debug!(
                        target: "spooky",
                        "Heartbeat failed ({failures}); re-probing before tearing the socket down: {e}"
                    );
                    self.stop_heartbeat();
                    if !self.parked() {
                        let retry = ConnectionSupervisor::HEARTBEAT_RETRY_MS
                            .min(self.config.heartbeat_interval_ms);
                        self.heartbeat_after(retry);
                    }
                } else {
                    tracing::warn!(
                        target: "spooky",
                        "Heartbeat failed {failures} times; tearing the socket down to force a reconnect: {e}"
                    );
                    self.heartbeat_failures.set(0);
                    // Force the close the transport never delivered. The
                    // resulting `Disconnected` event drives the revive loop.
                    (self.hooks.force_close)().await;
                    if !self.is_connected() {
                        self.schedule_revive();
                    }
                }
            }
        }
        self.heartbeat_in_flight.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::run_local;

    async fn pump() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    /// Advance the paused clock by `ms`, firing every timer due on the way at
    /// its own instant, then let what they started settle.
    async fn elapse(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        pump().await;
    }

    struct Transport {
        now_ms: Cell<i64>,
        events: broadcast::Sender<TransportEvent>,
        open: Cell<bool>,
        reconnects: Cell<u32>,
        probes: Cell<u32>,
        force_closes: Cell<u32>,
        probe_fails: Cell<bool>,
        reconnect_fails: Cell<bool>,
        /// Never answers, so the probe's timeout fires instead.
        probe_hangs: Cell<bool>,
    }

    impl Transport {
        fn new() -> Rc<Transport> {
            Rc::new(Transport {
                now_ms: Cell::new(0),
                events: broadcast::channel(16).0,
                open: Cell::new(true),
                reconnects: Cell::new(0),
                probes: Cell::new(0),
                force_closes: Cell::new(0),
                probe_fails: Cell::new(false),
                reconnect_fails: Cell::new(false),
                probe_hangs: Cell::new(false),
            })
        }

        fn emit(&self, event: TransportEvent) {
            let _ = self.events.send(event);
        }

        fn supervisor(self: &Rc<Self>, config: ReconnectConfig) -> ConnectionSupervisor {
            let (t1, t2, t3, t4, t5) = (
                self.clone(),
                self.clone(),
                self.clone(),
                self.clone(),
                self.clone(),
            );
            let hooks = SupervisorHooks {
                reconnect: Rc::new(move || {
                    let t = t1.clone();
                    async move {
                        t.reconnects.set(t.reconnects.get() + 1);
                        if t.reconnect_fails.get() {
                            return Err(Error::Network("nope".into()));
                        }
                        t.open.set(true);
                        t.emit(TransportEvent::Connected);
                        Ok(())
                    }
                    .boxed_local()
                }),
                probe: Rc::new(move || {
                    let t = t2.clone();
                    t.probes.set(t.probes.get() + 1);
                    async move {
                        if t.probe_hangs.get() {
                            futures::future::pending::<()>().await;
                        }
                        if t.probe_fails.get() {
                            return Err(Error::Network("dead".into()));
                        }
                        Ok(())
                    }
                    .boxed_local()
                }),
                force_close: Rc::new(move || {
                    let t = t3.clone();
                    async move {
                        t.force_closes.set(t.force_closes.get() + 1);
                        t.open.set(false);
                        t.emit(TransportEvent::Disconnected);
                    }
                    .boxed_local()
                }),
                is_connected: Rc::new(move || t4.open.get()),
            };
            ConnectionSupervisor::new(hooks, self.events.subscribe(), config)
                .with_clock(move || t5.now_ms.get())
        }
    }

    fn heartbeat(interval: u64, timeout: u64) -> ReconnectConfig {
        ReconnectConfig {
            heartbeat_interval_ms: interval,
            heartbeat_timeout_ms: timeout,
            ..ReconnectConfig::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_socket_is_reopened_on_backoff_and_never_given_up_on() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            sup.start();
            assert_eq!(sup.connection(), ConnectionState::Connected);

            t.reconnect_fails.set(true);
            t.open.set(false);
            t.emit(TransportEvent::Disconnected);
            pump().await;
            assert_eq!(sup.connection(), ConnectionState::Disconnected);

            elapse(1000).await;
            assert_eq!(t.reconnects.get(), 1);
            assert_eq!(sup.connection(), ConnectionState::Reconnecting);

            // Backoff doubles: the second attempt is 2s after the first failed.
            elapse(1999).await;
            assert_eq!(t.reconnects.get(), 1);
            elapse(1).await;
            assert_eq!(t.reconnects.get(), 2);

            t.reconnect_fails.set(false);
            elapse(10_000).await;
            assert_eq!(sup.connection(), ConnectionState::Connected);
            assert!(t.open.get());
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_capped() {
        run_local(async {
            let t = Transport::new();
            t.open.set(false);
            t.reconnect_fails.set(true);
            let sup = t.supervisor(ReconnectConfig {
                retry_delay_max_ms: 3000,
                ..ReconnectConfig::default()
            });
            sup.start();
            // 1s, 2s, then 3s (capped) apart: attempts at 1s, 3s, 6s, 9s.
            elapse(9000).await;
            assert_eq!(t.reconnects.get(), 4);
            elapse(2999).await;
            assert_eq!(t.reconnects.get(), 4);
            elapse(1).await;
            assert_eq!(t.reconnects.get(), 5);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_reconnect_that_never_answers_is_bounded_by_the_connect_timeout() {
        run_local(async {
            let t = Transport::new();
            t.open.set(false);
            let attempts = Rc::new(Cell::new(0));
            let a = attempts.clone();
            let open = t.clone();
            let hooks = SupervisorHooks {
                reconnect: Rc::new(move || {
                    a.set(a.get() + 1);
                    futures::future::pending().boxed_local()
                }),
                probe: Rc::new(|| async { Ok(()) }.boxed_local()),
                force_close: Rc::new(|| async {}.boxed_local()),
                is_connected: Rc::new(move || open.open.get()),
            };
            let sup = ConnectionSupervisor::new(
                hooks,
                t.events.subscribe(),
                ReconnectConfig {
                    connect_timeout_ms: 500,
                    ..ReconnectConfig::default()
                },
            );
            sup.start();
            elapse(1000).await;
            assert_eq!(attempts.get(), 1);
            assert_eq!(sup.connection(), ConnectionState::Reconnecting);
            // Times out at 1.5s, retries 2s later.
            elapse(2499).await;
            assert_eq!(attempts.get(), 1);
            elapse(1).await;
            assert_eq!(attempts.get(), 2);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn one_failed_probe_is_inconclusive_two_tear_the_socket_down() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(heartbeat(20_000, 1000));
            sup.start();

            t.probe_fails.set(true);
            elapse(20_000).await;
            assert_eq!(t.probes.get(), 1);
            assert_eq!(
                t.force_closes.get(),
                0,
                "a busy window is not a dead socket"
            );

            // The re-probe runs at min(HEARTBEAT_RETRY_MS, interval) = 5s.
            elapse(5000).await;
            assert_eq!(t.probes.get(), 2);
            assert_eq!(t.force_closes.get(), 1);
            assert_eq!(sup.connection(), ConnectionState::Disconnected);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_never_answers_counts_as_a_failure() {
        run_local(async {
            let t = Transport::new();
            t.probe_hangs.set(true);
            let sup = t.supervisor(heartbeat(1000, 500));
            sup.start();
            elapse(1500).await;
            assert_eq!(t.probes.get(), 1);
            // min(5000, 1000) = 1000 before the second probe, which also times out.
            elapse(1500).await;
            assert_eq!(t.force_closes.get(), 1);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_healthy_probe_resets_the_failure_count_and_keeps_beating() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(heartbeat(1000, 500));
            sup.start();
            t.probe_fails.set(true);
            elapse(1000).await;
            assert_eq!(t.probes.get(), 1);
            t.probe_fails.set(false);
            elapse(1000).await;
            assert_eq!(t.probes.get(), 2);
            t.probe_fails.set(true);
            elapse(1000).await;
            assert_eq!(t.probes.get(), 3);
            assert_eq!(t.force_closes.get(), 0, "failures are consecutive only");
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_healthy_probe_reconciles_a_state_forced_by_suspend() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            sup.start();
            sup.suspend();
            assert_eq!(sup.connection(), ConnectionState::Disconnected);
            // The socket never actually closed, so nothing else would correct this.
            sup.wake("resumed");
            pump().await;
            assert_eq!(t.probes.get(), 1);
            assert_eq!(sup.connection(), ConnectionState::Connected);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn suspend_parks_the_revive_loop() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            sup.start();
            t.open.set(false);
            t.emit(TransportEvent::Disconnected);
            pump().await;
            sup.suspend();
            elapse(60_000).await;
            assert_eq!(t.reconnects.get(), 0);
            sup.wake("online");
            pump().await;
            assert_eq!(t.reconnects.get(), 1);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn wake_probes_are_rate_limited_while_connected() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            sup.start();
            sup.wake("wake");
            pump().await;
            assert_eq!(t.probes.get(), 1);
            sup.wake("wake");
            sup.wake("wake");
            pump().await;
            assert_eq!(t.probes.get(), 1, "a resume storm must not probe per event");
            t.now_ms
                .set(t.now_ms.get() + ConnectionSupervisor::WAKE_PROBE_MIN_INTERVAL_MS);
            sup.wake("wake");
            pump().await;
            assert_eq!(t.probes.get(), 2);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn wake_reconnects_immediately_when_the_socket_is_gone() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            sup.start();
            t.open.set(false);
            t.emit(TransportEvent::Disconnected);
            pump().await;
            sup.wake("resumed");
            pump().await;
            assert_eq!(
                t.reconnects.get(),
                1,
                "not waiting out the scheduled backoff"
            );
            assert_eq!(sup.connection(), ConnectionState::Connected);
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn subscribers_see_the_current_state_and_every_change() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            let seen = Rc::new(RefCell::new(Vec::new()));
            let s = seen.clone();
            let off = sup.subscribe(move |state| s.borrow_mut().push(state));
            sup.start();
            t.open.set(false);
            t.emit(TransportEvent::Disconnected);
            pump().await;
            off();
            t.open.set(true);
            t.emit(TransportEvent::Connected);
            pump().await;
            assert_eq!(
                *seen.borrow(),
                [
                    ConnectionState::Disconnected,
                    ConnectionState::Connected,
                    ConnectionState::Disconnected,
                ]
            );
            sup.dispose();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn events_before_start_are_ignored() {
        run_local(async {
            let t = Transport::new();
            let sup = t.supervisor(ReconnectConfig::default());
            t.emit(TransportEvent::Disconnected);
            sup.start();
            pump().await;
            assert_eq!(sup.connection(), ConnectionState::Connected);
            sup.dispose();
        })
        .await;
    }
}
