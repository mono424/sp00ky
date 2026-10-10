use crate::operator::predicate::Predicate;
use crate::operator::{self, Operator, OrderSpec};
use std::collections::HashMap;

/// Chooses index-backed source operators while a graph is built; see
/// `Circuit::build_graph`. Each method answers for one plan shape with the
/// operator to stand in for it, or `None` to build the plan as written.
pub trait IndexPlanner {
    /// `Limit` over `Filter`s over `Scan(table)` (any `Project`s in between
    /// are kept above the window).
    fn window(
        &self,
        table: &str,
        predicates: &[&Predicate],
        order_by: Option<&[OrderSpec]>,
        limit: usize,
        start: usize,
    ) -> Option<Box<dyn Operator>>;

    /// The `Scan(table)` under a chain of `Filter`s.
    fn scan(&self, table: &str, predicates: &[&Predicate]) -> Option<Box<dyn Operator>>;
}

/// Builds every plan as written.
pub struct NoIndexes;

impl IndexPlanner for NoIndexes {
    fn window(&self, _: &str, _: &[&Predicate], _: Option<&[OrderSpec]>, _: usize, _: usize) -> Option<Box<dyn Operator>> {
        None
    }

    fn scan(&self, _: &str, _: &[&Predicate]) -> Option<Box<dyn Operator>> {
        None
    }
}

/// `Project`s (outermost first), then `Filter` predicates (outermost first),
/// then the table, for a plan that is exactly that chain over a `Scan`.
fn window_shape(plan: &operator::OperatorPlan) -> Option<(Vec<&Vec<operator::Projection>>, Vec<&Predicate>, &str)> {
    let mut projections = Vec::new();
    let mut node = plan;
    while let operator::OperatorPlan::Project { input, projections: p } = node {
        projections.push(p);
        node = input;
    }
    let (predicates, table) = filter_chain(node)?;
    Some((projections, predicates, table))
}

/// `Filter` predicates (outermost first) down to a `Scan`, or `None` when
/// the chain ends in anything else.
fn filter_chain(plan: &operator::OperatorPlan) -> Option<(Vec<&Predicate>, &str)> {
    let mut predicates = Vec::new();
    let mut node = plan;
    while let operator::OperatorPlan::Filter { input, predicate } = node {
        predicates.push(predicate);
        node = input;
    }
    match node {
        operator::OperatorPlan::Scan { table } => Some((predicates, table)),
        _ => None,
    }
}

/// Unique identifier for a node in the circuit graph.
pub type NodeId = usize;

/// A node in the circuit DAG.
pub struct Node {
    pub id: NodeId,
    pub operator: Box<dyn Operator>,
    /// Indices of input nodes (upstream dependencies).
    pub inputs: Vec<NodeId>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.id)
            .field("operator", &self.operator)
            .field("inputs", &self.inputs)
            .finish()
    }
}

/// The circuit as a directed acyclic graph of operator nodes.
///
/// Contains topology only. Execution state lives in the operators
/// (via their Z⁻¹ state) and in ViewState.
pub struct Graph {
    pub nodes: Vec<Node>,
    /// Topological execution order (computed once on construction).
    topo_order: Vec<NodeId>,
    /// Scan nodes: table_name → [NodeId] for routing deltas.
    scan_index: HashMap<String, Vec<NodeId>>,
    /// The output (terminal) node of this graph.
    pub output_node: NodeId,
}

impl Graph {
    /// Approximate heap bytes held in this graph's operators' Z⁻¹ state.
    ///
    /// Topology (`nodes`, `topo_order`, `scan_index`) is O(plan size) and
    /// negligible; the operator state is the term that scales with the data.
    pub fn state_bytes(&self) -> usize {
        self.nodes.iter().map(|n| n.operator.state_bytes()).sum()
    }

    /// Build a Graph from an OperatorPlan tree.
    ///
    /// Recursively walks the plan, creating operator nodes and wiring edges.
    pub fn from_plan(plan: &operator::OperatorPlan) -> Self {
        Self::from_plan_with(plan, &NoIndexes)
    }

    /// Build a Graph, letting `planner` put index-backed sources in place of
    /// the shapes it recognises.
    pub fn from_plan_with(plan: &operator::OperatorPlan, planner: &dyn IndexPlanner) -> Self {
        let mut nodes = Vec::new();
        let mut scan_index: HashMap<String, Vec<NodeId>> = HashMap::new();
        let output_node = Self::build_node(plan, &mut nodes, &mut scan_index, planner);

        let topo_order = Self::compute_topo_order(&nodes);

        Graph {
            nodes,
            topo_order,
            scan_index,
            output_node,
        }
    }

    /// Add a source node reading `table`, routed its deltas like a Scan.
    fn push_source(
        operator: Box<dyn Operator>,
        table: &str,
        nodes: &mut Vec<Node>,
        scan_index: &mut HashMap<String, Vec<NodeId>>,
    ) -> NodeId {
        let id = nodes.len();
        nodes.push(Node { id, operator, inputs: vec![] });
        scan_index.entry(table.to_string()).or_default().push(id);
        id
    }

    fn build_node(
        plan: &operator::OperatorPlan,
        nodes: &mut Vec<Node>,
        scan_index: &mut HashMap<String, Vec<NodeId>>,
        planner: &dyn IndexPlanner,
    ) -> NodeId {
        match plan {
            operator::OperatorPlan::Scan { table } => {
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::Scan::new(table)),
                    inputs: vec![],
                });
                scan_index.entry(table.clone()).or_default().push(id);
                id
            }
            operator::OperatorPlan::Filter { input, predicate } => {
                // A filter chain straight over a Scan may start from an index
                // range; every Filter is still built above it.
                if let Some((predicates, table)) = filter_chain(plan) {
                    if let Some(source) = planner.scan(table, &predicates) {
                        let mut id = Self::push_source(source, table, nodes, scan_index);
                        for predicate in predicates.iter().rev() {
                            let next = nodes.len();
                            nodes.push(Node {
                                id: next,
                                operator: Box::new(operator::Filter::new((*predicate).clone())),
                                inputs: vec![id],
                            });
                            id = next;
                        }
                        return id;
                    }
                }
                let input_id = Self::build_node(input, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::Filter::new(predicate.clone())),
                    inputs: vec![input_id],
                });
                id
            }
            operator::OperatorPlan::Join { left, right, on } => {
                let left_id = Self::build_node(left, nodes, scan_index, planner);
                let right_id = Self::build_node(right, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::Join::new(on.clone())),
                    inputs: vec![left_id, right_id],
                });
                id
            }
            operator::OperatorPlan::SemiJoin { left, right, on } => {
                let left_id = Self::build_node(left, nodes, scan_index, planner);
                let right_id = Self::build_node(right, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::SemiJoin::new(on.clone())),
                    inputs: vec![left_id, right_id],
                });
                id
            }
            operator::OperatorPlan::AntiJoin { left, right, on } => {
                let left_id = Self::build_node(left, nodes, scan_index, planner);
                let right_id = Self::build_node(right, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::AntiJoin::new(on.clone())),
                    inputs: vec![left_id, right_id],
                });
                id
            }
            operator::OperatorPlan::Union { left, right } => {
                let left_id = Self::build_node(left, nodes, scan_index, planner);
                let right_id = Self::build_node(right, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::Union::new()),
                    inputs: vec![left_id, right_id],
                });
                id
            }
            operator::OperatorPlan::Distinct { input } => {
                let input_id = Self::build_node(input, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::Distinct::new()),
                    inputs: vec![input_id],
                });
                id
            }
            operator::OperatorPlan::Project { input, projections } => {
                let input_id = Self::build_node(input, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::Map::new(projections.clone())),
                    inputs: vec![input_id],
                });
                id
            }
            operator::OperatorPlan::Limit {
                input,
                limit,
                start,
                order_by,
            } => {
                // A window over an index range: Projects stay above it.
                if let Some((projections, predicates, table)) = window_shape(input) {
                    if let Some(source) = planner.window(table, &predicates, order_by.as_deref(), *limit, *start) {
                        let mut id = Self::push_source(source, table, nodes, scan_index);
                        for projection in projections.iter().rev() {
                            let next = nodes.len();
                            nodes.push(Node {
                                id: next,
                                operator: Box::new(operator::Map::new((*projection).clone())),
                                inputs: vec![id],
                            });
                            id = next;
                        }
                        return id;
                    }
                }
                let input_id = Self::build_node(input, nodes, scan_index, planner);
                let id = nodes.len();
                nodes.push(Node {
                    id,
                    operator: Box::new(operator::TopK::new(*limit, *start, order_by.clone())),
                    inputs: vec![input_id],
                });
                id
            }
        }
    }

    /// Compute topological order using Kahn's algorithm.
    fn compute_topo_order(nodes: &[Node]) -> Vec<NodeId> {
        let n = nodes.len();
        let mut in_degree = vec![0usize; n];
        let mut adj: Vec<Vec<NodeId>> = vec![vec![]; n];

        for node in nodes {
            for &input_id in &node.inputs {
                adj[input_id].push(node.id);
                in_degree[node.id] += 1;
            }
        }

        let mut queue: std::collections::VecDeque<NodeId> = std::collections::VecDeque::new();
        for i in 0..n {
            if in_degree[i] == 0 {
                queue.push_back(i);
            }
        }

        let mut order = Vec::with_capacity(n);
        while let Some(id) = queue.pop_front() {
            order.push(id);
            for &next in &adj[id] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    queue.push_back(next);
                }
            }
        }

        order
    }

    pub fn topo_order(&self) -> &[NodeId] {
        &self.topo_order
    }

    pub fn scan_nodes_for_table(&self, table: &str) -> &[NodeId] {
        self.scan_index
            .get(table)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Some operator's state can no longer be stepped; see
    /// [`Operator::needs_rebuild`](crate::operator::Operator::needs_rebuild).
    pub fn needs_rebuild(&self) -> bool {
        self.nodes.iter().any(|node| node.operator.needs_rebuild())
    }

    /// Every `(table, index name)` this graph's sources read.
    pub fn index_uses(&self) -> Vec<(String, String)> {
        self.nodes
            .iter()
            .flat_map(|node| node.operator.index_uses())
            .map(|(table, index)| (table.to_string(), index.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::plan::*;
    use crate::operator::predicate::Predicate;
    use crate::types::Path;
    use serde_json::json;

    // ── Helper: build a plan and return its Graph ─────────────────────

    fn scan(table: &str) -> OperatorPlan {
        OperatorPlan::Scan {
            table: table.to_string(),
        }
    }

    fn filter(input: OperatorPlan, pred: Predicate) -> OperatorPlan {
        OperatorPlan::Filter {
            input: Box::new(input),
            predicate: pred,
        }
    }

    fn join(left: OperatorPlan, right: OperatorPlan, lf: &str, rf: &str) -> OperatorPlan {
        OperatorPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on: JoinCondition {
                left_field: Path::new(lf),
                right_field: Path::new(rf),
            },
        }
    }

    fn project(input: OperatorPlan, fields: &[&str]) -> OperatorPlan {
        let projections = fields
            .iter()
            .map(|f| Projection::Field {
                field: Path::new(f),
                alias: None,
            })
            .collect();
        OperatorPlan::Project {
            input: Box::new(input),
            projections,
        }
    }

    fn limit(input: OperatorPlan, n: usize, order: Option<Vec<OrderSpec>>) -> OperatorPlan {
        OperatorPlan::Limit {
            input: Box::new(input),
            limit: n,
            start: 0,
            order_by: order,
        }
    }

    fn order_desc(field: &str) -> OrderSpec {
        OrderSpec {
            field: Path::new(field),
            direction: "DESC".to_string(),
        }
    }

    fn eq_pred(field: &str, val: serde_json::Value) -> Predicate {
        Predicate::Eq {
            field: Path::new(field),
            value: val,
        }
    }

    fn gte_pred(field: &str, val: serde_json::Value) -> Predicate {
        Predicate::Gte {
            field: Path::new(field),
            value: val,
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 1. Single-operator graphs
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn scan_creates_single_node_graph() {
        let g = Graph::from_plan(&scan("users"));

        assert_eq!(g.node_count(), 1);
        assert_eq!(g.output_node, 0);
        assert_eq!(g.topo_order(), &[0]);
        assert_eq!(g.nodes[0].inputs, Vec::<NodeId>::new());
        assert_eq!(g.nodes[0].operator.arity(), 0);
        assert_eq!(g.nodes[0].operator.collections(), vec!["users"]);
    }

    #[test]
    fn scan_index_maps_table_to_node() {
        let g = Graph::from_plan(&scan("posts"));

        assert_eq!(g.scan_nodes_for_table("posts"), &[0]);
        assert_eq!(g.scan_nodes_for_table("nonexistent"), &[] as &[NodeId]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. Linear chains: Scan → unary operator
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn scan_then_filter_creates_two_nodes() {
        let plan = filter(scan("users"), eq_pred("active", json!(true)));
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.output_node, 1);
        // Scan is node 0, Filter is node 1
        assert_eq!(g.nodes[0].operator.arity(), 0); // Scan
        assert_eq!(g.nodes[1].operator.arity(), 1); // Filter
        assert_eq!(g.nodes[1].inputs, vec![0]);
    }

    #[test]
    fn scan_then_project_creates_two_nodes() {
        let plan = project(scan("users"), &["name", "email"]);
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.output_node, 1);
        assert_eq!(g.nodes[1].inputs, vec![0]);
        assert_eq!(g.nodes[1].operator.arity(), 1);
    }

    #[test]
    fn scan_then_limit_creates_two_nodes() {
        let plan = limit(scan("posts"), 10, Some(vec![order_desc("created_at")]));
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.output_node, 1);
        assert_eq!(g.nodes[1].inputs, vec![0]);
    }

    #[test]
    fn limit_without_order_by() {
        let plan = limit(scan("posts"), 5, None);
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.output_node, 1);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. Topological order for linear chains
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn topo_order_for_linear_chain() {
        // Scan(0) → Filter(1) → Limit(2)
        let plan = limit(
            filter(scan("users"), gte_pred("age", json!(18))),
            100,
            None,
        );
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 3);
        let order = g.topo_order();
        // Scan must come before Filter, Filter before Limit
        let scan_pos = order.iter().position(|&n| n == 0).unwrap();
        let filter_pos = order.iter().position(|&n| n == 1).unwrap();
        let limit_pos = order.iter().position(|&n| n == 2).unwrap();
        assert!(scan_pos < filter_pos);
        assert!(filter_pos < limit_pos);
    }

    #[test]
    fn topo_order_covers_all_nodes() {
        let plan = project(
            filter(scan("items"), eq_pred("visible", json!(true))),
            &["name"],
        );
        let g = Graph::from_plan(&plan);

        assert_eq!(g.topo_order().len(), g.node_count());
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. Join (binary operator, two scan roots)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn join_creates_three_nodes() {
        let plan = join(scan("users"), scan("posts"), "id", "author");
        let g = Graph::from_plan(&plan);

        // Scan(users)=0, Scan(posts)=1, Join=2
        assert_eq!(g.node_count(), 3);
        assert_eq!(g.output_node, 2);
        assert_eq!(g.nodes[2].inputs, vec![0, 1]);
        assert_eq!(g.nodes[2].operator.arity(), 2);
    }

    #[test]
    fn join_scan_index_has_both_tables() {
        let plan = join(scan("users"), scan("posts"), "id", "author");
        let g = Graph::from_plan(&plan);

        assert_eq!(g.scan_nodes_for_table("users"), &[0]);
        assert_eq!(g.scan_nodes_for_table("posts"), &[1]);
    }

    #[test]
    fn join_topo_order_scans_before_join() {
        let plan = join(scan("users"), scan("posts"), "id", "author");
        let g = Graph::from_plan(&plan);

        let order = g.topo_order();
        let join_pos = order.iter().position(|&n| n == 2).unwrap();
        let left_pos = order.iter().position(|&n| n == 0).unwrap();
        let right_pos = order.iter().position(|&n| n == 1).unwrap();
        assert!(left_pos < join_pos);
        assert!(right_pos < join_pos);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. Complex DAGs (multi-level nesting)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn join_with_filtered_inputs() {
        // Filter(Scan(users)) ⋈ Filter(Scan(posts))
        let plan = join(
            filter(scan("users"), eq_pred("active", json!(true))),
            filter(scan("posts"), eq_pred("published", json!(true))),
            "id",
            "author",
        );
        let g = Graph::from_plan(&plan);

        // Scan(users)=0, Filter(users)=1, Scan(posts)=2, Filter(posts)=3, Join=4
        assert_eq!(g.node_count(), 5);
        assert_eq!(g.output_node, 4);
        assert_eq!(g.nodes[4].inputs, vec![1, 3]); // Join reads from both filters
    }

    #[test]
    fn join_then_limit_creates_four_nodes() {
        // Limit(Join(Scan(users), Scan(posts)))
        let plan = limit(
            join(scan("users"), scan("posts"), "id", "author"),
            10,
            Some(vec![order_desc("created_at")]),
        );
        let g = Graph::from_plan(&plan);

        // Scan(users)=0, Scan(posts)=1, Join=2, Limit=3
        assert_eq!(g.node_count(), 4);
        assert_eq!(g.output_node, 3);
        assert_eq!(g.nodes[3].inputs, vec![2]);
    }

    #[test]
    fn deep_chain_scan_filter_project_limit() {
        // Limit(Project(Filter(Scan(users))))
        let plan = limit(
            project(
                filter(scan("users"), gte_pred("age", json!(21))),
                &["name", "email"],
            ),
            50,
            None,
        );
        let g = Graph::from_plan(&plan);

        // Scan=0, Filter=1, Project=2, Limit=3
        assert_eq!(g.node_count(), 4);
        assert_eq!(g.output_node, 3);

        // Verify chain wiring
        assert_eq!(g.nodes[0].inputs, Vec::<NodeId>::new()); // Scan
        assert_eq!(g.nodes[1].inputs, vec![0]);      // Filter ← Scan
        assert_eq!(g.nodes[2].inputs, vec![1]);      // Project ← Filter
        assert_eq!(g.nodes[3].inputs, vec![2]);      // Limit ← Project

        // Verify topo order is strictly monotonic for a chain
        assert_eq!(g.topo_order(), &[0, 1, 2, 3]);
    }

    #[test]
    fn topo_order_valid_for_join_with_filters_then_limit() {
        // Limit(Join(Filter(Scan(A)), Filter(Scan(B))))
        let plan = limit(
            join(
                filter(scan("a"), eq_pred("x", json!(1))),
                filter(scan("b"), eq_pred("y", json!(2))),
                "id",
                "ref",
            ),
            5,
            None,
        );
        let g = Graph::from_plan(&plan);

        // 6 nodes: ScanA=0, FilterA=1, ScanB=2, FilterB=3, Join=4, Limit=5
        assert_eq!(g.node_count(), 6);
        assert_eq!(g.output_node, 5);

        // Verify every node appears before its dependents in topo order
        let order = g.topo_order();
        for node in &g.nodes {
            let node_pos = order.iter().position(|&n| n == node.id).unwrap();
            for &input_id in &node.inputs {
                let input_pos = order.iter().position(|&n| n == input_id).unwrap();
                assert!(
                    input_pos < node_pos,
                    "input node {} (pos {}) should come before dependent node {} (pos {})",
                    input_id,
                    input_pos,
                    node.id,
                    node_pos,
                );
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 6. Scan index edge cases
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn duplicate_table_scans_both_indexed() {
        // Self-join: Scan(users) ⋈ Scan(users)
        let plan = join(scan("users"), scan("users"), "manager_id", "id");
        let g = Graph::from_plan(&plan);

        // Both scan nodes should be in the scan index for "users"
        let scan_nodes = g.scan_nodes_for_table("users");
        assert_eq!(scan_nodes.len(), 2);
        assert_eq!(scan_nodes, &[0, 1]);
    }

    #[test]
    fn scan_index_three_distinct_tables() {
        // Join(Join(Scan(a), Scan(b)), Scan(c))
        let inner = join(scan("a"), scan("b"), "id", "a_id");
        let plan = join(inner, scan("c"), "id", "b_id");
        let g = Graph::from_plan(&plan);

        assert_eq!(g.scan_nodes_for_table("a"), &[0]);
        assert_eq!(g.scan_nodes_for_table("b"), &[1]);
        assert_eq!(g.scan_nodes_for_table("c"), &[3]); // after inner join node
    }

    // ═══════════════════════════════════════════════════════════════════
    // 7. Operator type correctness
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn arity_correct_for_all_operator_types() {
        let plan = limit(
            project(
                join(
                    filter(scan("users"), eq_pred("active", json!(true))),
                    scan("posts"),
                    "id",
                    "author",
                ),
                &["name"],
            ),
            10,
            None,
        );
        let g = Graph::from_plan(&plan);

        // Scan(users)=0 → Filter=1 → ⋈ with Scan(posts)=2 → Join=3 → Project=4 → Limit=5
        assert_eq!(g.nodes[0].operator.arity(), 0); // Scan
        assert_eq!(g.nodes[1].operator.arity(), 1); // Filter
        assert_eq!(g.nodes[2].operator.arity(), 0); // Scan
        assert_eq!(g.nodes[3].operator.arity(), 2); // Join
        assert_eq!(g.nodes[4].operator.arity(), 1); // Map/Project
        assert_eq!(g.nodes[5].operator.arity(), 1); // TopK/Limit
    }

    #[test]
    fn collections_only_set_for_scan_nodes() {
        let plan = filter(scan("users"), eq_pred("x", json!(1)));
        let g = Graph::from_plan(&plan);

        assert_eq!(g.nodes[0].operator.collections(), vec!["users"]);
        assert_eq!(g.nodes[1].operator.collections(), Vec::<String>::new());
    }

    // ═══════════════════════════════════════════════════════════════════
    // 8. Node ID consistency
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn node_ids_are_sequential_from_zero() {
        let plan = limit(
            join(
                filter(scan("a"), eq_pred("x", json!(1))),
                scan("b"),
                "id",
                "ref",
            ),
            5,
            None,
        );
        let g = Graph::from_plan(&plan);

        for (i, node) in g.nodes.iter().enumerate() {
            assert_eq!(node.id, i, "node at index {} has id {}", i, node.id);
        }
    }

    #[test]
    fn output_node_is_last_node() {
        let plan = project(
            filter(scan("t"), eq_pred("x", json!(1))),
            &["y"],
        );
        let g = Graph::from_plan(&plan);
        assert_eq!(g.output_node, g.node_count() - 1);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 9. Compound predicates in Filter
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn filter_with_and_predicate() {
        let pred = Predicate::And {
            predicates: vec![
                eq_pred("active", json!(true)),
                gte_pred("age", json!(18)),
            ],
        };
        let plan = filter(scan("users"), pred);
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.nodes[1].operator.arity(), 1);
    }

    #[test]
    fn filter_with_or_predicate() {
        let pred = Predicate::Or {
            predicates: vec![
                eq_pred("role", json!("admin")),
                eq_pred("role", json!("mod")),
            ],
        };
        let plan = filter(scan("users"), pred);
        let g = Graph::from_plan(&plan);

        assert_eq!(g.node_count(), 2);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 10. Project with subquery
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn project_with_subquery_adds_extra_scan_node() {
        // Project(Scan(users), projections=[Field("name"), Subquery(Scan(posts))])
        let plan = OperatorPlan::Project {
            input: Box::new(scan("users")),
            projections: vec![
                Projection::Field {
                    field: Path::new("name"),
                    alias: None,
                },
                Projection::Subquery {
                    alias: "posts".to_string(),
                    plan: Box::new(scan("posts")),
                    parent_key: None,
                },
            ],
        };
        let g = Graph::from_plan(&plan);

        // Only 2 nodes: Scan(users)=0, Project=1
        // Subquery plans in projections are NOT built into the graph —
        // they are metadata held inside the Map operator. The graph only
        // contains the operator nodes for the main pipeline.
        assert_eq!(g.node_count(), 2);
        assert_eq!(g.output_node, 1);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 11. referenced_tables (OperatorPlan method)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn referenced_tables_simple_scan() {
        let plan = scan("users");
        assert_eq!(plan.referenced_tables(), vec!["users"]);
    }

    #[test]
    fn referenced_tables_join() {
        let plan = join(scan("users"), scan("posts"), "id", "author");
        let tables = plan.referenced_tables();
        assert_eq!(tables, vec!["users", "posts"]);
    }

    #[test]
    fn referenced_tables_deduplicates() {
        // Self-join on same table
        let plan = join(scan("users"), scan("users"), "manager", "id");
        let tables = plan.referenced_tables();
        assert_eq!(tables, vec!["users"]); // deduplicated
    }

    #[test]
    fn referenced_tables_deep_tree() {
        let plan = limit(
            join(
                filter(scan("a"), eq_pred("x", json!(1))),
                filter(scan("b"), eq_pred("y", json!(2))),
                "id",
                "ref",
            ),
            10,
            None,
        );
        let tables = plan.referenced_tables();
        assert_eq!(tables, vec!["a", "b"]);
    }

    #[test]
    fn referenced_tables_with_subquery_projection() {
        let plan = OperatorPlan::Project {
            input: Box::new(scan("users")),
            projections: vec![Projection::Subquery {
                alias: "posts".to_string(),
                plan: Box::new(scan("posts")),
                parent_key: None,
            }],
        };
        let tables = plan.referenced_tables();
        assert_eq!(tables, vec!["users", "posts"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 11b. subquery_tables (OperatorPlan method)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn subquery_tables_empty_for_simple_scan() {
        let plan = scan("users");
        assert!(plan.subquery_tables().is_empty());
    }

    #[test]
    fn subquery_tables_empty_for_join() {
        let plan = join(scan("users"), scan("posts"), "id", "author");
        assert!(plan.subquery_tables().is_empty());
    }

    #[test]
    fn subquery_tables_returns_subquery_referenced_tables() {
        let plan = OperatorPlan::Project {
            input: Box::new(scan("thread")),
            projections: vec![
                Projection::All,
                Projection::Subquery {
                    alias: "comments".to_string(),
                    plan: Box::new(scan("comment")),
                    parent_key: None,
                },
            ],
        };
        assert_eq!(plan.subquery_tables(), vec!["comment"]);
    }

    #[test]
    fn subquery_tables_overlapping_with_primary() {
        // Self-referencing: SELECT *, (SELECT * FROM thread WHERE parent=$parent.id) FROM thread
        let plan = OperatorPlan::Project {
            input: Box::new(scan("thread")),
            projections: vec![
                Projection::All,
                Projection::Subquery {
                    alias: "children".to_string(),
                    plan: Box::new(scan("thread")),
                    parent_key: None,
                },
            ],
        };
        // "thread" appears in both primary and subquery — subquery_tables includes it
        assert_eq!(plan.subquery_tables(), vec!["thread"]);
        // referenced_tables also includes it (deduplicated)
        assert_eq!(plan.referenced_tables(), vec!["thread"]);
    }

    #[test]
    fn subquery_tables_nested_subquery() {
        // SELECT *, (SELECT *, (SELECT * FROM author) AS a FROM comment) AS comments FROM thread
        let inner_subquery = OperatorPlan::Project {
            input: Box::new(scan("comment")),
            projections: vec![
                Projection::All,
                Projection::Subquery {
                    alias: "author".to_string(),
                    plan: Box::new(scan("author")),
                    parent_key: None,
                },
            ],
        };
        let plan = OperatorPlan::Project {
            input: Box::new(scan("thread")),
            projections: vec![
                Projection::All,
                Projection::Subquery {
                    alias: "comments".to_string(),
                    plan: Box::new(inner_subquery),
                    parent_key: None,
                },
            ],
        };
        let mut sq = plan.subquery_tables();
        sq.sort();
        assert_eq!(sq, vec!["author", "comment"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 12. Topo order invariant: universal validation
    // ═══════════════════════════════════════════════════════════════════

    /// Generic validator: for ANY graph, every node's inputs must appear
    /// earlier in the topological order.
    fn assert_topo_valid(g: &Graph) {
        let order = g.topo_order();
        assert_eq!(
            order.len(),
            g.node_count(),
            "topo order must include all nodes"
        );

        let mut position = vec![0usize; g.node_count()];
        for (pos, &node_id) in order.iter().enumerate() {
            position[node_id] = pos;
        }

        for node in &g.nodes {
            for &input_id in &node.inputs {
                assert!(
                    position[input_id] < position[node.id],
                    "topo violation: input {} (pos {}) must come before node {} (pos {})",
                    input_id,
                    position[input_id],
                    node.id,
                    position[node.id],
                );
            }
        }
    }

    #[test]
    fn topo_valid_single_scan() {
        assert_topo_valid(&Graph::from_plan(&scan("t")));
    }

    #[test]
    fn topo_valid_filter_chain() {
        let plan = filter(
            filter(scan("t"), eq_pred("a", json!(1))),
            eq_pred("b", json!(2)),
        );
        assert_topo_valid(&Graph::from_plan(&plan));
    }

    #[test]
    fn topo_valid_join() {
        let plan = join(scan("a"), scan("b"), "id", "ref");
        assert_topo_valid(&Graph::from_plan(&plan));
    }

    #[test]
    fn topo_valid_complex_dag() {
        // Limit(Project(Join(Filter(Scan(a)), Filter(Scan(b)))))
        let plan = limit(
            project(
                join(
                    filter(scan("a"), eq_pred("x", json!(1))),
                    filter(scan("b"), gte_pred("y", json!(0))),
                    "id",
                    "ref",
                ),
                &["x", "y"],
            ),
            25,
            Some(vec![order_desc("x")]),
        );
        let g = Graph::from_plan(&plan);
        assert_topo_valid(&g);
        assert_eq!(g.node_count(), 7); // 2 scans + 2 filters + join + project + limit
    }

    #[test]
    fn topo_valid_nested_joins() {
        // Join(Join(Scan(a), Scan(b)), Join(Scan(c), Scan(d)))
        let plan = join(
            join(scan("a"), scan("b"), "id", "a_ref"),
            join(scan("c"), scan("d"), "id", "c_ref"),
            "id",
            "id",
        );
        let g = Graph::from_plan(&plan);
        assert_topo_valid(&g);
        assert_eq!(g.node_count(), 7); // 4 scans + 2 inner joins + 1 outer join
    }

    // ═══════════════════════════════════════════════════════════════════
    // New permission-lowering operators
    // ═══════════════════════════════════════════════════════════════════

    fn semi_join(left: OperatorPlan, right: OperatorPlan, lf: &str, rf: &str) -> OperatorPlan {
        OperatorPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            on: JoinCondition {
                left_field: Path::new(lf),
                right_field: Path::new(rf),
            },
        }
    }

    fn anti_join(left: OperatorPlan, right: OperatorPlan, lf: &str, rf: &str) -> OperatorPlan {
        OperatorPlan::AntiJoin {
            left: Box::new(left),
            right: Box::new(right),
            on: JoinCondition {
                left_field: Path::new(lf),
                right_field: Path::new(rf),
            },
        }
    }

    fn union(left: OperatorPlan, right: OperatorPlan) -> OperatorPlan {
        OperatorPlan::Union {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn distinct(input: OperatorPlan) -> OperatorPlan {
        OperatorPlan::Distinct {
            input: Box::new(input),
        }
    }

    #[test]
    fn semi_join_builds_two_input_node() {
        let plan = semi_join(scan("threads"), scan("collab"), "id", "out");
        let g = Graph::from_plan(&plan);
        assert_eq!(g.node_count(), 3);
        assert_eq!(g.nodes[2].operator.arity(), 2);
        assert_eq!(g.nodes[2].inputs, vec![0, 1]);
    }

    #[test]
    fn anti_join_builds_two_input_node() {
        let plan = anti_join(scan("threads"), scan("collab"), "id", "out");
        let g = Graph::from_plan(&plan);
        assert_eq!(g.node_count(), 3);
        assert_eq!(g.nodes[2].operator.arity(), 2);
    }

    #[test]
    fn union_builds_two_input_node() {
        let plan = union(scan("a"), scan("b"));
        let g = Graph::from_plan(&plan);
        assert_eq!(g.node_count(), 3);
        assert_eq!(g.nodes[2].operator.arity(), 2);
    }

    #[test]
    fn distinct_builds_one_input_node() {
        let plan = distinct(scan("a"));
        let g = Graph::from_plan(&plan);
        assert_eq!(g.node_count(), 2);
        assert_eq!(g.nodes[1].operator.arity(), 1);
    }

    #[test]
    fn permission_or_lowering_shape_is_buildable() {
        // The lowered shape we'll produce in Step 5 for an OR of three branches:
        // Distinct(Union(Union(Filter, Filter), SemiJoin)) over Scan(threads).
        // This test pins that the graph builder accepts it end-to-end and the
        // dependency-tracking helpers see all three primary tables.
        let scan_threads = scan("threads");
        let scan_collab = scan("collab");
        let pred_pub = eq_pred("published", json!(true));
        let pred_author = eq_pred("author.id", json!("user:owner"));
        let lowered = distinct(union(
            union(
                filter(scan_threads.clone(), pred_pub),
                filter(scan_threads.clone(), pred_author),
            ),
            semi_join(scan_threads, scan_collab, "id", "out"),
        ));
        let g = Graph::from_plan(&lowered);
        assert_topo_valid(&g);
        // Three Scan(threads) nodes (one per OR branch) + one Scan(collab) +
        // 2 filters + semi-join + 2 unions + distinct = 10 nodes.
        assert_eq!(g.node_count(), 10);
        // The dependency map (via referenced_tables on the plan) sees both tables.
        let referenced = lowered.referenced_tables();
        assert!(referenced.contains(&"threads".to_string()));
        assert!(referenced.contains(&"collab".to_string()));
    }
}
