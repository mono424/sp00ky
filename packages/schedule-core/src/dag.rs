//! DAG validation and readiness computation.
//!
//! Validation runs twice: in the CLI at lint/deploy time (so a broken DAG never
//! reaches the database) and defensively in the engine at spawn time (so a
//! hand-edited row fails the run instead of wedging the sweep).
//!
//! Readiness is computed from step STATUS rows rather than in-memory state, so
//! advancement is idempotent and survives restarts: a step is ready when every
//! dependency has succeeded, and it is only ever dispatched by whichever
//! advancement pass wins the `blocked → ready` compare-and-swap.

use std::collections::{BTreeMap, VecDeque};

use crate::spec::{StepDef, WorkflowDef};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DagError {
    #[error("workflow has no steps")]
    Empty,
    #[error("duplicate step name `{0}`")]
    DuplicateStep(String),
    #[error("step `{step}` depends on unknown step `{dep}`")]
    UnknownDependency { step: String, dep: String },
    #[error("step `{0}` depends on itself")]
    SelfDependency(String),
    #[error("dependency cycle among steps: {}", .0.join(" -> "))]
    Cycle(Vec<String>),
}

/// Terminal + non-terminal step states, as stored in `_00_step_run.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepStatus {
    Blocked,
    Ready,
    Dispatched,
    Success,
    Failed,
    Skipped,
}

impl StepStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            StepStatus::Blocked => "blocked",
            StepStatus::Ready => "ready",
            StepStatus::Dispatched => "dispatched",
            StepStatus::Success => "success",
            StepStatus::Failed => "failed",
            StepStatus::Skipped => "skipped",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "blocked" => StepStatus::Blocked,
            "ready" => StepStatus::Ready,
            "dispatched" => StepStatus::Dispatched,
            "success" => StepStatus::Success,
            "failed" => StepStatus::Failed,
            "skipped" => StepStatus::Skipped,
            _ => return None,
        })
    }

    /// A step that will never change state again.
    pub fn is_terminal(self) -> bool {
        matches!(self, StepStatus::Success | StepStatus::Failed | StepStatus::Skipped)
    }
}

/// A validated DAG. Construction is the validation.
#[derive(Debug, Clone)]
pub struct WorkflowDag {
    steps: BTreeMap<String, StepDef>,
    /// Topological order, used for stable layering in the CLI's visualization.
    order: Vec<String>,
}

impl WorkflowDag {
    pub fn validate(def: &WorkflowDef) -> Result<Self, DagError> {
        if def.steps.is_empty() {
            return Err(DagError::Empty);
        }

        let mut steps: BTreeMap<String, StepDef> = BTreeMap::new();
        for step in &def.steps {
            if steps.insert(step.name.clone(), step.clone()).is_some() {
                return Err(DagError::DuplicateStep(step.name.clone()));
            }
        }

        for step in steps.values() {
            for dep in &step.depends_on {
                if dep == &step.name {
                    return Err(DagError::SelfDependency(step.name.clone()));
                }
                if !steps.contains_key(dep) {
                    return Err(DagError::UnknownDependency {
                        step: step.name.clone(),
                        dep: dep.clone(),
                    });
                }
            }
        }

        let order = topo_order(&steps)?;
        Ok(Self { steps, order })
    }

    pub fn steps(&self) -> impl Iterator<Item = &StepDef> {
        self.order.iter().filter_map(|name| self.steps.get(name))
    }

    pub fn step(&self, name: &str) -> Option<&StepDef> {
        self.steps.get(name)
    }

    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Steps with no dependencies — dispatched immediately at spawn.
    pub fn roots(&self) -> impl Iterator<Item = &StepDef> {
        self.steps().filter(|s| s.depends_on.is_empty())
    }

    /// Topological order (stable: ties broken by name).
    pub fn topo_order(&self) -> &[String] {
        &self.order
    }

    /// Longest-path layer per step — the column index used by the CLI's DAG
    /// renderer. `layer(n) = 0` for roots, else `1 + max(layer(deps))`.
    pub fn layers(&self) -> BTreeMap<String, usize> {
        let mut layers: BTreeMap<String, usize> = BTreeMap::new();
        for name in &self.order {
            let step = &self.steps[name];
            let layer = step
                .depends_on
                .iter()
                .filter_map(|dep| layers.get(dep).copied())
                .max()
                .map_or(0, |max| max + 1);
            layers.insert(name.clone(), layer);
        }
        layers
    }

    /// Steps currently `blocked` whose every dependency has succeeded.
    ///
    /// Deliberately status-driven rather than derived from a traversal: the
    /// caller passes the step rows it just read, so two concurrent advancement
    /// passes compute the same answer and the CAS decides who dispatches.
    pub fn ready_steps<'a>(
        &'a self,
        statuses: &'a BTreeMap<String, StepStatus>,
    ) -> impl Iterator<Item = &'a StepDef> + 'a {
        self.steps().filter(move |step| {
            statuses.get(&step.name).copied() == Some(StepStatus::Blocked)
                && step
                    .depends_on
                    .iter()
                    .all(|dep| statuses.get(dep).copied() == Some(StepStatus::Success))
        })
    }

    /// Steps that can never run because an ancestor failed or was skipped, each
    /// paired with the dependency that dooms it.
    ///
    /// Used by `on_failure: continue-independent` to skip exactly the affected
    /// branch instead of the whole remainder of the DAG. The blocking dependency
    /// comes back because the pass below is the only place that knows it: by the
    /// time a skip is written, every doomed step looks alike, and the step that
    /// actually broke may be several hops up the graph.
    pub fn doomed_steps<'a>(
        &'a self,
        statuses: &'a BTreeMap<String, StepStatus>,
    ) -> Vec<(&'a StepDef, &'a str)> {
        let mut doomed: BTreeMap<&str, &str> = BTreeMap::new();
        // `order` is topological, so a single forward pass propagates doom.
        for name in &self.order {
            let step = &self.steps[name];
            let status = statuses.get(name).copied();
            if !matches!(status, Some(StepStatus::Blocked) | Some(StepStatus::Ready)) {
                continue;
            }
            let blocker = step.depends_on.iter().find(|dep| {
                doomed.contains_key(dep.as_str())
                    || matches!(
                        statuses.get(*dep).copied(),
                        Some(StepStatus::Failed) | Some(StepStatus::Skipped)
                    )
            });
            if let Some(blocker) = blocker {
                doomed.insert(step.name.as_str(), blocker.as_str());
            }
        }
        doomed
            .into_iter()
            .filter_map(|(name, blocker)| self.steps.get(name).map(|step| (step, blocker)))
            .collect()
    }
}

/// Kahn's algorithm, mirroring the docker `dependsOn` cycle check in the CLI.
fn topo_order(steps: &BTreeMap<String, StepDef>) -> Result<Vec<String>, DagError> {
    let mut indegree: BTreeMap<&str, usize> =
        steps.keys().map(|name| (name.as_str(), 0usize)).collect();
    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for step in steps.values() {
        for dep in &step.depends_on {
            *indegree.get_mut(step.name.as_str()).expect("step present") += 1;
            dependents.entry(dep.as_str()).or_default().push(step.name.as_str());
        }
    }

    // BTreeMap iteration is name-sorted, so the queue seeds deterministically.
    let mut queue: VecDeque<&str> = indegree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(name, _)| *name)
        .collect();

    let mut order = Vec::with_capacity(steps.len());
    while let Some(name) = queue.pop_front() {
        order.push(name.to_string());
        for dependent in dependents.get(name).into_iter().flatten() {
            let deg = indegree.get_mut(*dependent).expect("dependent present");
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(dependent);
            }
        }
    }

    if order.len() != steps.len() {
        let mut cycle: Vec<String> = indegree
            .iter()
            .filter(|(_, deg)| **deg > 0)
            .map(|(name, _)| (*name).to_string())
            .collect();
        cycle.sort();
        return Err(DagError::Cycle(cycle));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::OnFailure;

    fn step(name: &str, deps: &[&str]) -> StepDef {
        StepDef {
            name: name.to_string(),
            path: format!("/{name}"),
            table: None,
            payload: None,
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
            max_retries: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    fn def(steps: Vec<StepDef>) -> WorkflowDef {
        WorkflowDef { steps, on_failure: OnFailure::Halt }
    }

    /// extract-orders ┐
    ///                ├→ transform → {notify, archive}
    /// extract-users  ┘
    fn diamond() -> WorkflowDag {
        WorkflowDag::validate(&def(vec![
            step("extract-orders", &[]),
            step("extract-users", &[]),
            step("transform", &["extract-orders", "extract-users"]),
            step("notify", &["transform"]),
            step("archive", &["transform"]),
        ]))
        .unwrap()
    }

    #[test]
    fn validates_and_orders_a_diamond() {
        let dag = diamond();
        assert_eq!(dag.len(), 5);
        let roots: Vec<_> = dag.roots().map(|s| s.name.clone()).collect();
        assert_eq!(roots, vec!["extract-orders", "extract-users"]);

        let order = dag.topo_order();
        let pos = |name: &str| order.iter().position(|n| n == name).unwrap();
        assert!(pos("extract-orders") < pos("transform"));
        assert!(pos("extract-users") < pos("transform"));
        assert!(pos("transform") < pos("notify"));
        assert!(pos("transform") < pos("archive"));
    }

    #[test]
    fn layers_are_longest_path() {
        let layers = diamond().layers();
        assert_eq!(layers["extract-orders"], 0);
        assert_eq!(layers["extract-users"], 0);
        assert_eq!(layers["transform"], 1);
        assert_eq!(layers["notify"], 2);
        assert_eq!(layers["archive"], 2);
    }

    #[test]
    fn join_waits_for_every_dependency() {
        let dag = diamond();
        let mut statuses: BTreeMap<String, StepStatus> = dag
            .steps()
            .map(|s| (s.name.clone(), StepStatus::Blocked))
            .collect();

        // Both roots ready at spawn.
        let ready: Vec<_> = dag.ready_steps(&statuses).map(|s| s.name.clone()).collect();
        assert_eq!(ready, vec!["extract-orders", "extract-users"]);

        // One branch done: the join is still blocked.
        statuses.insert("extract-orders".into(), StepStatus::Success);
        statuses.insert("extract-users".into(), StepStatus::Dispatched);
        assert_eq!(dag.ready_steps(&statuses).count(), 0);

        // Both done: the join unblocks, and only the join.
        statuses.insert("extract-users".into(), StepStatus::Success);
        let ready: Vec<_> = dag.ready_steps(&statuses).map(|s| s.name.clone()).collect();
        assert_eq!(ready, vec!["transform"]);

        // Join done: both dependents become ready in parallel.
        statuses.insert("transform".into(), StepStatus::Success);
        let ready: Vec<_> = dag.ready_steps(&statuses).map(|s| s.name.clone()).collect();
        assert_eq!(ready, vec!["archive", "notify"]);
    }

    #[test]
    fn doomed_steps_follow_a_failure_downstream() {
        let dag = diamond();
        let mut statuses: BTreeMap<String, StepStatus> = dag
            .steps()
            .map(|s| (s.name.clone(), StepStatus::Blocked))
            .collect();
        statuses.insert("extract-orders".into(), StepStatus::Failed);
        statuses.insert("extract-users".into(), StepStatus::Dispatched);

        let doomed: Vec<_> = dag
            .doomed_steps(&statuses)
            .iter()
            .map(|(s, blocker)| (s.name.clone(), blocker.to_string()))
            .collect();
        // transform depends on the failed step; notify/archive depend on transform.
        // Each step names the dependency that dooms IT, not the original failure,
        // so a reader can walk the chain one hop at a time.
        assert_eq!(
            doomed,
            vec![
                ("archive".to_string(), "transform".to_string()),
                ("notify".to_string(), "transform".to_string()),
                ("transform".to_string(), "extract-orders".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_cycles_and_bad_dependencies() {
        assert_eq!(WorkflowDag::validate(&def(vec![])).unwrap_err(), DagError::Empty);

        assert_eq!(
            WorkflowDag::validate(&def(vec![step("a", &["a"])])).unwrap_err(),
            DagError::SelfDependency("a".into())
        );

        assert_eq!(
            WorkflowDag::validate(&def(vec![step("a", &["ghost"])])).unwrap_err(),
            DagError::UnknownDependency { step: "a".into(), dep: "ghost".into() }
        );

        assert_eq!(
            WorkflowDag::validate(&def(vec![step("a", &[]), step("a", &[])])).unwrap_err(),
            DagError::DuplicateStep("a".into())
        );

        let cycle = WorkflowDag::validate(&def(vec![
            step("a", &["c"]),
            step("b", &["a"]),
            step("c", &["b"]),
        ]))
        .unwrap_err();
        assert_eq!(cycle, DagError::Cycle(vec!["a".into(), "b".into(), "c".into()]));
    }
}
