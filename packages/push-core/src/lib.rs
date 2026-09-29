//! Portable Web Push engine.
//!
//! Two hosts embed this crate, exactly like `schedule-core`:
//! - the cluster scheduler (`apps/scheduler`), fed from `ingest_event`
//! - the standalone SSP (`packages/ssp-node`), fed from `ingest_handler`
//!
//! The host supplies a database port ([`schedule_core::ScheduleDb`]) and an
//! outbound HTTP port ([`PushHttp`]); everything else lives here: the `push:`
//! rules of sp00ky.yml ([`config`]), VAPID keys derived from the auth secret,
//! RFC 8291 payload encryption, delivery with throttling and rate limits, and
//! the `_00_push_subscription` / `_00_push_message` bookkeeping, and native
//! delivery to iOS (APNs) and Android (FCM) from the same rules ([`native`]).

pub mod config;
pub mod ece;
pub mod engine;
#[cfg(feature = "reqwest")]
pub mod http_reqwest;
pub mod native;
pub mod rules;
pub mod template;
pub mod util;
pub mod vapid;

pub use config::*;
pub use engine::{
    endpoint_allowed, Clock, EngineOptions, ObservedChange, Origin, PushCounters, PushEngine,
    PushHttp, PushQueues, PushStatus,
};
pub use schedule_core::{MaybeSendSync, ScheduleDb, ScheduleDbError};
pub use vapid::{VapidError, VapidKeys};
#[cfg(feature = "reqwest")]
pub use http_reqwest::ReqwestPushHttp;

#[cfg(test)]
mod db_tests;
