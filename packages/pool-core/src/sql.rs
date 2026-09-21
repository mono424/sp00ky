//! Every statement the pool engine runs.
//!
//! Two rules carried over from the job runner, because both were learned the
//! hard way there:
//!
//! - **All clocks are the database's.** Leases, boot timeouts, idle timers and
//!   heartbeats are compared with `time::now()` inside the statement, and handed
//!   back as booleans. The engine never compares its own clock with a stored one.
//! - **A fence is part of the statement text, never a nullable bind.** A bound
//!   JSON `null` is SurrealDB's NULL, `NULL = NONE` is false, and a guard written
//!   that way silently matches nothing.
//!
//! Table names cannot be bound, so the per-table statements are built with
//! `format!` - only ever from a name [`crate::PoolSpec::from_row`] has already
//! checked with `is_plain_identifier`.

use schedule_core::sql::LEASE_EXPIRED;

/// `<duration>` from a bound number of seconds (a duration cannot be multiplied
/// by a parameter in SurrealQL).
macro_rules! secs {
    ($bind:literal) => {
        concat!("<duration>(string::concat(<string>", $bind, ", 's'))")
    };
}

/// DUE now? Byte-identical to `ssp_node::jobs::runner::PENDING_DUE_CLAUSE` (and to
/// the copy in the cluster recovery sweep): a job is due at `created_at + delay`.
/// Duplicated because this crate cannot see ssp-node, exactly as the sweep does.
pub const PENDING_DUE_CLAUSE: &str =
    "(created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()";

// -- pools -------------------------------------------------------------------

pub const SELECT_POOLS: &str = "SELECT *, \
     (breaker_until != NONE AND breaker_until > time::now()) AS breaker_open \
     FROM _00_pool";

pub const SELECT_POOL_BY_NAME: &str = "SELECT *, \
     (breaker_until != NONE AND breaker_until > time::now()) AS breaker_open \
     FROM _00_pool WHERE name = $name LIMIT 1";

// Pools are addressed by `name`, never by record id: pool names routinely carry
// hyphens, which puts the key in `⟨⟩` quotes, and a quoted id does not survive a
// round trip through `type::record()`. The name is unique by construction (it IS
// the key deploy writes the row under).

/// Engine-owned fields only. `$until_secs = 0` closes the breaker.
pub const RECORD_BOOT_FAILURE: &str = concat!(
    "UPDATE _00_pool SET boot_failures = $failures, last_error = $error, \
     breaker_until = IF $until_secs > 0 { time::now() + ",
    secs!("$until_secs"),
    " } ELSE { NONE } WHERE name = $name"
);

pub const RESET_BOOT_FAILURES: &str = "UPDATE _00_pool SET boot_failures = 0, \
     breaker_until = NONE, last_error = NONE \
     WHERE name = $name AND (boot_failures > 0 OR breaker_until != NONE)";

pub const RECORD_POOL_ERROR: &str = "UPDATE _00_pool SET last_error = $error WHERE name = $name";

// -- machines ----------------------------------------------------------------

/// Live machines of one pool, oldest first, with every clock already evaluated.
pub const SELECT_LIVE_MACHINES: &str = concat!(
    "SELECT id, state, provider_id, slots, busy_slots, spec_hash, idle_since, created_at, \
     (created_at + ",
    secs!("$boot"),
    ") < time::now() AS boot_overdue, \
     ((last_seen ?? created_at) + ",
    secs!("$lease"),
    ") < time::now() AS heartbeat_lost, \
     (last_seen != NONE AND (last_seen + ",
    secs!("$fresh"),
    ") > time::now()) AS fresh, \
     (idle_since != NONE AND (idle_since + ",
    secs!("$idle"),
    ") < time::now()) AS idle_expired, \
     (created_at + ",
    secs!("$life"),
    ") < time::now() AS lifetime_over, \
     (created_at + 15s) < time::now() AS settled \
     FROM _00_machine WHERE pool = $pool AND state NOT IN ['gone', 'failed'] \
     ORDER BY created_at ASC"
);

/// The row is written BEFORE the provider is asked, so its id can be the
/// provider's idempotency key and a crash in between re-creates the same machine.
pub const CREATE_MACHINE: &str = "CREATE _00_machine CONTENT { pool: $pool, state: 'requested', \
     provider: $provider, slots: $slots, spec_hash: $hash } RETURN id";

pub const SET_PROVIDER_ID: &str =
    "UPDATE type::record($id) SET provider_id = $provider_id WHERE state NOT IN ['gone', 'failed']";

/// Compare-and-swap on the state. `$from` is the set of states it may leave.
pub const MOVE_MACHINE: &str = "UPDATE type::record($id) SET state = $to \
     WHERE state INSIDE $from RETURN AFTER";

/// Take a machine away. `failure` decides whether it is filed as `failed` or
/// `gone` once the provider has confirmed the destroy.
pub const BEGIN_TERMINATE: &str = "UPDATE type::record($id) SET state = 'terminating', \
     reason = $reason, failure = $failure \
     WHERE state INSIDE $from RETURN AFTER";

pub const FINISH_TERMINATE: &str = "UPDATE type::record($id) SET \
     state = IF failure { 'failed' } ELSE { 'gone' }, ended_at = time::now(), busy_slots = 0 \
     WHERE state = 'terminating' RETURN AFTER";

pub const MARK_READY: &str = "UPDATE type::record($id) SET state = 'ready', \
     ready_at = ready_at ?? time::now(), last_seen = time::now(), idle_since = time::now() \
     WHERE state INSIDE ['requested', 'booting'] RETURN AFTER";

/// The heartbeat. Returns the state so the caller can tell a machine that is
/// being taken away (or whose row is gone) to shut down.
///
/// Two statements, not one with an optional bind: a bound JSON `null` arrives as
/// SurrealDB's NULL, which an `option<object>` field rejects (it wants NONE), and
/// that would turn every stats-less heartbeat into a failed poll.
pub const TOUCH_MACHINE: &str = "UPDATE type::record($id) SET last_seen = time::now() \
     WHERE state NOT IN ['gone', 'failed'] RETURN id, state, pool";

pub const TOUCH_MACHINE_WITH_STATS: &str = "UPDATE type::record($id) SET last_seen = time::now(), \
     agent = $stats WHERE state NOT IN ['gone', 'failed'] RETURN id, state, pool";

/// Not `FROM ONLY`: a missing row must read as "no rows", never as an error,
/// because "no row" is exactly what makes a provider machine an orphan.
pub const SELECT_MACHINE: &str = "SELECT id, state, pool FROM type::record($id)";

pub const SET_OCCUPANCY_IDLE: &str = "UPDATE type::record($id) SET busy_slots = 0, \
     idle_since = idle_since ?? time::now()";

pub const SET_OCCUPANCY_BUSY: &str =
    "UPDATE type::record($id) SET busy_slots = $busy, idle_since = NONE";

pub const PRUNE_MACHINES: &str = concat!(
    "DELETE _00_machine WHERE state INSIDE ['gone', 'failed'] AND ended_at != NONE AND (ended_at + ",
    secs!("$keep"),
    ") < time::now()"
);

// -- jobs (per outbox table) --------------------------------------------------

pub fn count_queued(table: &str) -> String {
    format!(
        "SELECT count() FROM {table} WHERE status = 'pending' AND {PENDING_DUE_CLAUSE} GROUP ALL"
    )
}

/// Oldest first, and `created_at` rather than `updated_at` so a retry does not
/// jump the line (same ordering the dispatcher drains by).
pub fn select_queued(table: &str) -> String {
    format!(
        "SELECT id, created_at FROM {table} WHERE status = 'pending' AND {PENDING_DUE_CLAUSE} \
         ORDER BY created_at ASC LIMIT $n"
    )
}

/// Jobs bound to each of `$machines` right now. This IS the occupancy: there is
/// no separate bookkeeping to fall out of step with it.
pub fn select_occupancy(table: &str) -> String {
    format!(
        "SELECT assignee, count() AS n FROM {table} \
         WHERE status = 'processing' AND assignee INSIDE $machines GROUP BY assignee"
    )
}

pub fn select_bound(table: &str) -> String {
    format!(
        "SELECT id, path, payload, timeout, lease_epoch FROM {table} \
         WHERE status = 'processing' AND assignee = $machine"
    )
}

/// Bind a pending job to a machine. Same CAS, lease and fencing token as the job
/// runner's claim; `assignee` names the machine instead of an SSP.
pub const CLAIM_JOB: &str = concat!(
    "UPDATE type::record($id) SET status = 'processing', assignee = $machine, \
     lease_epoch = (lease_epoch ?? 0) + 1, lease_until = time::now() + ",
    secs!("$lease"),
    ", updated_at = time::now() WHERE status = 'pending' RETURN AFTER"
);

/// The lease renewal ordinary jobs do not have: every agent poll pushes the
/// lease of each attempt it is really running out by one lease length. It does
/// NOT touch `updated_at`, which therefore keeps marking when the attempt began.
pub const RENEW_LEASE: &str = concat!(
    "UPDATE type::record($id) SET lease_until = time::now() + ",
    secs!("$lease"),
    " WHERE status = 'processing' AND assignee = $machine AND (lease_epoch ?? 0) = $epoch RETURN id"
);

/// Attempts whose lease ran out, and that have no retry budget left: terminal.
/// Runs BEFORE the requeue so the two statements partition the expired rows.
///
/// `retries` is assigned last on purpose: the branch reads the pre-update value
/// whichever way the engine evaluates a multi-field SET.
pub fn fail_expired_exhausted(table: &str) -> String {
    format!(
        "UPDATE {table} SET status = 'failed', lease_until = NONE, \
         lease_epoch = (lease_epoch ?? 0) + 1, errors = array::append(errors ?? [], $error), \
         updated_at = time::now(), retries = (retries ?? 0) + 1 \
         WHERE status = 'processing' AND {LEASE_EXPIRED} \
         AND (retries ?? 0) + 1 >= (max_retries ?? 3) RETURN id"
    )
}

/// Everything else whose lease ran out goes back to the queue. Bumping the epoch
/// fences the attempt that lost the lease; clearing `assignee` frees the slot.
pub fn requeue_expired(table: &str) -> String {
    format!(
        "UPDATE {table} SET status = 'pending', assignee = NONE, lease_until = NONE, \
         lease_epoch = (lease_epoch ?? 0) + 1, errors = array::append(errors ?? [], $error), \
         updated_at = time::now(), retries = (retries ?? 0) + 1 \
         WHERE status = 'processing' AND {LEASE_EXPIRED} RETURN id"
    )
}

/// Backstop for the agent's own deadline: an attempt that has been running past
/// the pool's hard limit. Terminal - it would only overrun again.
pub fn fail_overdue(table: &str) -> String {
    format!(
        concat!(
            "UPDATE {table} SET status = 'failed', lease_until = NONE, \
             lease_epoch = (lease_epoch ?? 0) + 1, errors = array::append(errors ?? [], $error), \
             updated_at = time::now() \
             WHERE status = 'processing' AND (updated_at + ",
            secs!("$limit"),
            ") < time::now() RETURN id"
        ),
        table = table
    )
}

const ATTEMPT_FENCE: &str =
    "WHERE status = 'processing' AND assignee = $machine AND (lease_epoch ?? 0) = $epoch";

pub fn complete_success() -> String {
    format!(
        "UPDATE type::record($id) SET status = 'success', result = $result, lease_until = NONE, \
         updated_at = time::now() {ATTEMPT_FENCE} RETURN id"
    )
}

/// For a table that predates the `result` field: finishing the job matters more
/// than capturing its output (same fallback as the job runner).
pub fn complete_success_without_result() -> String {
    format!(
        "UPDATE type::record($id) SET status = 'success', lease_until = NONE, \
         updated_at = time::now() {ATTEMPT_FENCE} RETURN id"
    )
}

/// A failed attempt: back to `pending` while the row has retry budget, else
/// `failed`. One statement, so the decision and the write cannot disagree.
/// `retries` last, for the reason given on [`fail_expired_exhausted`].
pub fn complete_failure() -> String {
    format!(
        "UPDATE type::record($id) SET \
         status = IF (retries ?? 0) + 1 >= (max_retries ?? 3) {{ 'failed' }} ELSE {{ 'pending' }}, \
         assignee = IF (retries ?? 0) + 1 >= (max_retries ?? 3) {{ assignee }} ELSE {{ NONE }}, \
         lease_until = NONE, errors = array::append(errors ?? [], $error), \
         updated_at = time::now(), retries = (retries ?? 0) + 1 {ATTEMPT_FENCE} RETURN id, status"
    )
}

/// Cancelled or past its deadline: terminal, never retried.
pub fn complete_terminal_failure() -> String {
    format!(
        "UPDATE type::record($id) SET status = 'failed', lease_until = NONE, \
         errors = array::append(errors ?? [], $error), updated_at = time::now() \
         {ATTEMPT_FENCE} RETURN id"
    )
}

/// Operator kill. Bumping the epoch fences the running attempt; the agent finds
/// the job no longer bound to it on its next poll and is told to cancel.
/// Operator retry of a finished pool job. The same reset the SSPs apply to their
/// own jobs (`reset_for_retry_helper`): back to `pending` with a fresh retry
/// budget and an empty error history. It also lets go of the machine the last
/// attempt ran on; the next sweep assigns the job like any other pending one.
/// Only a terminal job: retrying one that is still queued or running would race
/// the attempt that owns it.
pub const RETRY_JOB: &str = "UPDATE type::record($id) SET status = 'pending', retries = 0, \
     errors = [], assignee = NONE, lease_until = NONE, updated_at = time::now() \
     WHERE status INSIDE ['failed', 'success'] RETURN id";

pub const KILL_JOB: &str = "UPDATE type::record($id) SET status = 'failed', lease_until = NONE, \
     lease_epoch = (lease_epoch ?? 0) + 1, \
     errors = array::append(errors ?? [], { code: 'killed', reason: 'killed by operator' }), \
     updated_at = time::now() WHERE status INSIDE ['pending', 'processing'] RETURN id";

#[cfg(test)]
mod tests {
    use super::*;

    /// The due clause is copied from ssp-node by hand; pin it so an edit here is
    /// a deliberate one.
    #[test]
    fn due_clause_matches_the_runner() {
        assert_eq!(
            PENDING_DUE_CLAUSE,
            "(created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()"
        );
    }

    #[test]
    fn the_duration_macro_expands_inline() {
        assert!(CLAIM_JOB.contains("<duration>(string::concat(<string>$lease, 's'))"));
        assert!(fail_overdue("job").contains("FROM") == false);
        assert!(fail_overdue("job").starts_with("UPDATE job SET status = 'failed'"));
    }
}
