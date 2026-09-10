//! The edge-update service carries what a flush could not write into the next
//! window, ahead of newer deltas, and once the carry budget is out it PARKS
//! the delta and retries it on a slower cadence rather than dropping it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ssp::circuit::ViewDelta;
use ssp_node::edges::{run_edge_update_service, EdgeSink, MAX_EDGE_CARRY};
use ssp_node::ports::Scheduler;
use ssp_node::TimerKind;
use tokio::sync::mpsc;

fn delta(query_id: &str) -> ViewDelta {
    ViewDelta {
        query_id: query_id.to_string(),
        additions: vec!["user:x".to_string()],
        removals: vec![],
        updates: vec![],
        records: vec![],
        result_hash: String::new(),
        subquery_items: vec![],
        auth_id: "user:a".to_string(),
        initial: false,
    }
}

/// Records every batch it is handed; fails (returns as leftovers) the deltas
/// whose query_id starts with "bad" for the first `fail_rounds` flushes that
/// contain one.
struct RecordingSink {
    batches: Arc<Mutex<Vec<Vec<String>>>>,
    fail_rounds: usize,
    failed: Arc<Mutex<usize>>,
}

impl EdgeSink for RecordingSink {
    async fn flush(&self, deltas: Vec<ViewDelta>) -> Vec<ViewDelta> {
        self.batches.lock().unwrap().push(deltas.iter().map(|d| d.query_id.clone()).collect());
        let mut failed = self.failed.lock().unwrap();
        if *failed < self.fail_rounds && deltas.iter().any(|d| d.query_id.starts_with("bad")) {
            *failed += 1;
            return deltas.into_iter().filter(|d| d.query_id.starts_with("bad")).collect();
        }
        Vec::new()
    }
}

/// A scheduler whose window is a yield, so the loop ticks as fast as the test
/// can feed it.
struct YieldScheduler;
#[async_trait::async_trait]
impl Scheduler for YieldScheduler {
    async fn sleep(&self, _d: Duration) {
        tokio::task::yield_now().await;
    }
    async fn schedule(&self, _kind: TimerKind, _at_epoch_ms: u64) {}
    async fn cancel(&self, _kind: &TimerKind) {}
}

async fn run(fail_rounds: usize, feed: Vec<Vec<ViewDelta>>) -> Vec<Vec<String>> {
    let (tx, rx) = mpsc::unbounded_channel::<Vec<ViewDelta>>();
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = RecordingSink { batches: Arc::clone(&batches), fail_rounds, failed: Arc::new(Mutex::new(0)) };
    let service = tokio::spawn(run_edge_update_service(rx, sink, Arc::new(YieldScheduler), Duration::from_millis(1), 4096));
    for deltas in feed {
        tx.send(deltas).unwrap();
        // Let the window tick between sends so each lands in its own flush.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(tx);
    service.await.unwrap();
    let out = batches.lock().unwrap().clone();
    out
}

#[tokio::test]
async fn leftovers_lead_the_next_batch() {
    let batches = run(1, vec![vec![delta("bad:1"), delta("good:1")], vec![delta("good:2")]]).await;
    // First flush: both; it fails bad:1. The carried bad:1 is flushed again
    // exactly once, and in whatever batch it rides it comes FIRST - ahead of
    // anything that arrived after it (the window may tick before or after
    // good:2 lands, so the batch it shares is not pinned).
    assert_eq!(batches[0], vec!["bad:1", "good:1"]);
    let carried: Vec<&Vec<String>> = batches[1..].iter().filter(|b| b.iter().any(|q| q == "bad:1")).collect();
    assert_eq!(carried.len(), 1, "carried exactly once: {batches:?}");
    assert_eq!(carried[0][0], "bad:1", "carried delta leads its batch: {batches:?}");
    assert!(batches.iter().flatten().any(|q| q == "good:2"), "the later delta still flushed: {batches:?}");
}

#[tokio::test]
async fn a_permanently_failing_delta_stops_riding_every_window_but_is_never_dropped() {
    let feed: Vec<Vec<ViewDelta>> = (0..(MAX_EDGE_CARRY as usize + 3)).map(|i| vec![delta(&format!("good:{i}"))]).collect();
    let mut feed_with_bad = vec![vec![delta("bad:x")]];
    feed_with_bad.extend(feed);
    let batches = run(usize::MAX, feed_with_bad).await;
    let carried = batches.iter().filter(|b| b.iter().any(|q| q == "bad:x")).count();
    // The first flush plus MAX_EDGE_CARRY carries, and after that it rides
    // only the parked cadence — so it appears again, but not in every window.
    assert!(
        carried > MAX_EDGE_CARRY as usize,
        "a delta that cannot be written is parked and retried, not dropped: {batches:?}"
    );
    assert!(
        carried < batches.len(),
        "a parked delta does not ride every window: {batches:?}"
    );
    let healthy = batches.iter().filter(|b| b.iter().any(|q| q.starts_with("good"))).count();
    assert_eq!(
        healthy,
        MAX_EDGE_CARRY as usize + 3,
        "every healthy delta still flushed: {batches:?}"
    );
}
