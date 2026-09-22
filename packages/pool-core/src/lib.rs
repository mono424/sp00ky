//! Machine pool engine core.
//!
//! A pool is a set of machines that exist to run one backend's outbox jobs, one
//! job (or `slots` jobs) per machine, sized on demand. The cluster scheduler
//! hosts this crate the way it hosts `schedule-core`: it supplies the database
//! port and the machine providers, and drives [`PoolEngine::tick_pass`] on an
//! interval. Agents on the machines reach the engine through the scheduler's
//! pool listener ([`PoolEngine::on_hello`] / `on_ready` / `on_poll` / `on_result`).
//!
//! The design rule everything else follows from: **the outbox row is the only
//! record of which machine runs which job.** `status = 'processing'` and
//! `assignee = <machine id>`, held under the row's existing lease and fencing
//! token. Occupancy, free slots, what to tell an agent on its next poll - all of
//! it is derived from those rows on demand. So there is no second copy of the
//! truth to repair after a crash, no in-memory command queue to lose on a
//! restart, and every pass is level-triggered: it looks at what is, and moves it
//! toward what should be.

pub mod engine;
pub mod provider;
pub mod sizing;
pub mod spec;
pub mod sql;

#[cfg(test)]
mod db_tests;

pub use engine::{PoolEngine, PoolEngineConfig, PoolObservation, PoolTransition, TickReport};
pub use provider::{CreateMachine, MachineProvider, ProviderError, ProviderMachine};
pub use sizing::{desired_machines, Demand};
pub use spec::{MachineRow, MachineState, PoolSpec, SpecError};

pub use pool_protocol as protocol;
