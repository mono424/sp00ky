//! How many machines a pool should have. Pure arithmetic, so the three modes
//! the feature promises are pinned down by tests rather than by reading the
//! engine.

use crate::spec::PoolSpec;

/// What the pool is being asked to do right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Demand {
    /// Slots currently running a job, on machines that count as supply.
    pub busy_slots: u32,
    /// Due `pending` jobs that have no machine yet.
    pub queued_jobs: u32,
}

/// Machines the pool should have in supply (requested + booting + ready).
///
/// ```text
/// capacity = busy_slots + queued_jobs + buffer * slots
/// needed   = ceil(capacity / slots)
/// desired  = autoscale ? clamp(max(min, needed), min, ceiling) : min
/// ```
///
/// - **Fixed** (`autoscale: false`): always `min`. Extra jobs wait for a free slot.
/// - **Baseline + scale on demand** (`buffer: 0`): a queued job raises `needed`
///   by exactly the machine it lacks, so it waits one boot and no longer.
/// - **Warm buffer**: `buffer` whole machines stay ready ABOVE what is in use, so
///   a job starts at once and the buffer refills behind it.
pub fn desired_machines(spec: &PoolSpec, demand: Demand) -> u32 {
    if !spec.autoscale {
        return spec.min;
    }
    let slots = spec.slots.max(1) as u64;
    let capacity =
        demand.busy_slots as u64 + demand.queued_jobs as u64 + spec.buffer as u64 * slots;
    let needed = capacity.div_ceil(slots).min(u32::MAX as u64) as u32;
    needed.max(spec.min).min(spec.ceiling())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(autoscale: bool, min: u32, max: u32, buffer: u32, slots: u32) -> PoolSpec {
        PoolSpec::from_row(&json!({
            "id": "_00_pool:p", "name": "p", "provider": "docker", "slots": slots,
            "min": min, "autoscale": autoscale, "max": max, "buffer": buffer,
            "backend": "b", "target_table": "job", "spec_hash": "h",
        }))
        .unwrap()
    }

    fn d(busy: u32, queued: u32) -> Demand {
        Demand {
            busy_slots: busy,
            queued_jobs: queued,
        }
    }

    #[test]
    fn fixed_size_ignores_demand_entirely() {
        let s = spec(false, 3, 10, 5, 1);
        assert_eq!(desired_machines(&s, d(0, 0)), 3);
        assert_eq!(
            desired_machines(&s, d(3, 40)),
            3,
            "extra jobs wait; the pool does not grow"
        );
    }

    #[test]
    fn baseline_with_no_buffer_grows_by_exactly_what_is_queued() {
        let s = spec(true, 1, 8, 0, 1);
        assert_eq!(desired_machines(&s, d(0, 0)), 1, "idle: just the baseline");
        assert_eq!(
            desired_machines(&s, d(1, 0)),
            1,
            "baseline busy, nothing waiting"
        );
        assert_eq!(
            desired_machines(&s, d(1, 1)),
            2,
            "one job waiting -> one more machine"
        );
        assert_eq!(desired_machines(&s, d(1, 3)), 4);
    }

    #[test]
    fn buffer_keeps_that_many_machines_above_usage() {
        let s = spec(true, 0, 8, 2, 1);
        assert_eq!(
            desired_machines(&s, d(0, 0)),
            2,
            "nothing running: two warm"
        );
        assert_eq!(
            desired_machines(&s, d(1, 0)),
            3,
            "one in use: still two warm"
        );
        assert_eq!(desired_machines(&s, d(3, 0)), 5);
    }

    #[test]
    fn max_is_a_hard_ceiling_and_min_a_hard_floor() {
        let s = spec(true, 2, 4, 2, 1);
        assert_eq!(desired_machines(&s, d(0, 0)), 2);
        assert_eq!(desired_machines(&s, d(10, 50)), 4, "never above max");
        let scale_to_zero = spec(true, 0, 4, 0, 1);
        assert_eq!(desired_machines(&scale_to_zero, d(0, 0)), 0);
    }

    #[test]
    fn slots_pack_jobs_and_the_buffer_is_counted_in_whole_machines() {
        let s = spec(true, 0, 10, 1, 4);
        assert_eq!(
            desired_machines(&s, d(0, 0)),
            1,
            "buffer of one whole machine"
        );
        assert_eq!(
            desired_machines(&s, d(4, 0)),
            2,
            "one full machine + the buffer"
        );
        assert_eq!(
            desired_machines(&s, d(5, 0)),
            3,
            "5 busy slots need 2 machines, +1 buffer"
        );
        assert_eq!(
            desired_machines(&s, d(0, 9)),
            4,
            "9 queued need 3 machines, +1 buffer"
        );
    }

    #[test]
    fn huge_demand_cannot_overflow() {
        let s = spec(true, 0, 5, u32::MAX, 1);
        assert_eq!(desired_machines(&s, d(u32::MAX, u32::MAX)), 5);
    }
}
