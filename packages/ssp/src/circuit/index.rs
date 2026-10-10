//! Secondary indexes, mirrored from upstream's `DEFINE INDEX`.
//!
//! The SSP holds the same indexes SurrealDB does for every synced table:
//! `INFO FOR TABLE` lists them, [`parse_define_index`] reads each definition,
//! and the circuit plans windowed and filtered views over them the way
//! SurrealDB plans the same query (an equality prefix, then the `ORDER BY`
//! fields). An index someone defines upstream reaches the SSP with the next
//! schema read.
//!
//! An [`OrderedIndex`] is the index itself: every row of the table, keyed by
//! its indexed field values and then its row key, in a B-tree with
//! positional lookup. A collection builds one only when a view first plans
//! over it, keeps it in step with every write after that, and drops it when
//! the last such view goes (see [`super::store::Collection::with_index`]).

use std::collections::HashMap;

use indexset::BTreeSet;
use serde_json::Value;
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::algebra::RowKey;
use crate::circuit::graph::IndexPlanner;
use crate::eval::value_ops::{compare_values, resolve_field};
use crate::eval::value_ref::ValueRef;
use crate::operator::filter::resolve_predicate_value;
use crate::operator::predicate::Predicate;
use crate::operator::top_k::{Scalar, SortableValue};
use crate::operator::{IndexBinding, IndexWindow, IndexedScan, KeyScan, Operator, OrderSpec};
use crate::types::{Path, Sp00kyValue};

/// One `DEFINE INDEX … FIELDS a, b` as the circuit uses it: the name, to tell
/// definitions apart, and the field paths in index order.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
}

impl IndexDef {
    pub fn paths(&self) -> Vec<Path> {
        self.fields.iter().map(|f| Path::new(f)).collect()
    }
}

/// Read a `DEFINE INDEX` statement as `INFO FOR TABLE` prints it.
///
/// Only plain `FIELDS` / `COLUMNS` indexes are mirrored. Full-text, vector
/// (`HNSW`, `MTREE`) and `COUNT` indexes answer questions the circuit does not
/// ask, so they come back `None`, as does anything unreadable.
pub fn parse_define_index(define: &str) -> Option<IndexDef> {
    let text = define.trim().trim_end_matches(';');
    let upper = text.to_ascii_uppercase();
    let words: Vec<&str> = text.split_whitespace().collect();
    let upper_words: Vec<String> = words.iter().map(|w| w.to_ascii_uppercase()).collect();
    if upper_words.first().map(String::as_str) != Some("DEFINE") || upper_words.get(1).map(String::as_str) != Some("INDEX") {
        return None;
    }
    if ["SEARCH", "FULLTEXT", "HNSW", "MTREE", "COUNT"]
        .iter()
        .any(|kind| upper_words.iter().any(|w| w == kind))
    {
        return None;
    }
    // `DEFINE INDEX [OVERWRITE | IF NOT EXISTS] <name> ON [TABLE] <table> …`
    let mut at = 2;
    while matches!(upper_words.get(at).map(String::as_str), Some("OVERWRITE" | "IF" | "NOT" | "EXISTS")) {
        at += 1;
    }
    let name = unquote(words.get(at)?);

    let fields_at = ["FIELDS", "COLUMNS"]
        .iter()
        .filter_map(|kw| find_keyword(&upper, kw))
        .min()?;
    let after = &text[fields_at..];
    let after = after.split_once(char::is_whitespace).map(|(_, rest)| rest).unwrap_or("");
    let after_upper = after.to_ascii_uppercase();
    let end = ["UNIQUE", "COMMENT", "CONCURRENTLY", "DEFER"]
        .iter()
        .filter_map(|kw| find_keyword(&after_upper, kw))
        .min()
        .unwrap_or(after.len());
    let fields: Vec<String> = after[..end]
        .split(',')
        .map(|f| unquote(f.trim()))
        .filter(|f| !f.is_empty())
        .collect();
    if fields.is_empty() || fields.iter().any(|f| !f.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')) {
        return None;
    }
    Some(IndexDef { name, fields })
}

/// Byte offset of `keyword` as a whole word in `upper`.
fn find_keyword(upper: &str, keyword: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    upper.match_indices(keyword).map(|(i, _)| i).find(|&i| {
        let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
        let end = i + keyword.len();
        let after_ok = end == bytes.len() || bytes[end].is_ascii_whitespace();
        before_ok && after_ok
    })
}

fn unquote(word: &str) -> String {
    word.trim_matches(|c| c == '`' || c == '⟨' || c == '⟩' || c == '"' || c == '\'').to_string()
}

/// Longest string `SmolStr` stores without a heap allocation.
const SMOL_INLINE: usize = 22;

/// Distinct long strings one index build shares; see
/// [`OrderedIndex::key_of_shared`].
const SHARE_DISTINCT: usize = 4096;

/// The indexed values of one row, in index field order.
/// Bare scalars: an index is ascending throughout, so it needs none of a
/// sort key's per-field direction (24 bytes a value, not 32).
pub(crate) type IndexKey = SmallVec<[Scalar; 2]>;

/// A built index: `(indexed values, row key)` for every row, ordered, with
/// positional lookup so a window is a rank range rather than a walk.
#[derive(Debug)]
pub struct OrderedIndex {
    pub(crate) def: IndexDef,
    paths: Vec<Path>,
    entries: BTreeSet<(IndexKey, RowKey)>,
    /// Heap the entries point at; see [`Self::bytes`].
    heap: usize,
}

fn entry_heap(key: &IndexKey, row: &RowKey) -> usize {
    let strings: usize = key
        .iter()
        .map(|v| match v {
            Scalar::Str(s) if s.len() > SMOL_INLINE => s.len() + 16,
            _ => 0,
        })
        .sum();
    strings + row.len() + 16
}

impl OrderedIndex {
    pub(crate) fn new(def: IndexDef) -> Self {
        let paths = def.paths();
        Self { def, paths, entries: BTreeSet::new(), heap: 0 }
    }

    /// The key `row` files under. `id` is the record key, not a body field,
    /// so it is never part of a mirrored index's values (see the planner).
    pub(crate) fn key_of(&self, row: ValueRef<'_>) -> IndexKey {
        self.paths
            .iter()
            .map(|path| SortableValue::from_value(resolve_field(row, path), false).scalar)
            .collect()
    }

    /// [`Self::key_of`] for a bulk build. A string too long to store inline
    /// is a heap allocation per row, and a leading field is usually a parent
    /// link hundreds of thousands of rows hold (`database`, `owner`): equal
    /// ones share one allocation through `seen`. `seen` stops growing at
    /// [`SHARE_DISTINCT`] values, so a field with a value per row (a
    /// timestamp) costs the build no more than it did.
    pub(crate) fn key_of_shared<'r>(&self, row: ValueRef<'r>, seen: &mut HashMap<&'r str, SmolStr>) -> IndexKey {
        self.paths
            .iter()
            .map(|path| match resolve_field(row, path) {
                ValueRef::Str(s) if s.len() > SMOL_INLINE => {
                    let shared = match seen.get(s) {
                        Some(shared) => shared.clone(),
                        None => {
                            let fresh = SmolStr::new(s);
                            if seen.len() < SHARE_DISTINCT {
                                seen.insert(s, fresh.clone());
                            }
                            fresh
                        }
                    };
                    Scalar::Str(shared)
                }
                value => SortableValue::from_value(value, false).scalar,
            })
            .collect()
    }

    /// Fill an empty index from unordered entries: sorted first, so the
    /// B-tree is filled front to back.
    pub(crate) fn fill(&mut self, mut entries: Vec<(IndexKey, RowKey)>) {
        entries.sort_unstable();
        for (key, row) in entries {
            self.insert(key, row);
        }
    }

    pub(crate) fn insert(&mut self, key: IndexKey, row: RowKey) {
        let heap = entry_heap(&key, &row);
        if self.entries.insert((key, row)) {
            self.heap += heap;
        }
    }

    pub(crate) fn remove(&mut self, key: IndexKey, row: RowKey) {
        let heap = entry_heap(&key, &row);
        if self.entries.remove(&(key, row)) {
            self.heap = self.heap.saturating_sub(heap);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `[lo, hi)`: the positions of every entry whose leading values equal
    /// `prefix`.
    pub(crate) fn prefix_range(&self, prefix: &[Scalar]) -> (usize, usize) {
        let lo_key: IndexKey = prefix.iter().cloned().collect();
        let mut hi_key = lo_key.clone();
        hi_key.push(Scalar::Top);
        let lo = self.entries.rank(&(lo_key, RowKey::from("")));
        let hi = self.entries.rank(&(hi_key, RowKey::from("")));
        (lo, hi.max(lo))
    }

    /// The row at `position`.
    pub(crate) fn row_at(&self, position: usize) -> Option<&RowKey> {
        self.entries.get_index(position).map(|(_, row)| row)
    }

    /// `[lo, hi)`: the positions of every entry sharing its first `fields`
    /// values with the entry at `position`.
    pub(crate) fn group_of(&self, position: usize, fields: usize) -> Option<(usize, usize)> {
        let (values, _) = self.entries.get_index(position)?;
        let leading = &values[..fields.min(values.len())];
        Some(self.prefix_range(leading))
    }

    /// Approximate heap bytes held, for the admin memory views: the entries
    /// plus the row keys and long strings they point at (counted as if
    /// unshared, so an upper bound).
    pub fn bytes(&self) -> usize {
        self.entries.len() * (std::mem::size_of::<(IndexKey, RowKey)>() + 16) + self.heap
    }
}

/// Plans a view over the indexes mirrored for its tables, the way SurrealDB
/// would pick one for the same query:
///
/// - a window (`LIMIT`, optional `START` and `ORDER BY`) whose filter is
///   equalities on an index's leading fields, in any order, and whose
///   `ORDER BY` is the rest of that index (each field in either direction),
///   reads its rows off the index ([`IndexWindow`]);
/// - any other filter holding `id` equal to one record starts from that row
///   ([`KeyScan`]), the record key being the index every table has;
/// - any other filter with equalities on an index's leading fields starts
///   from that range ([`IndexedScan`]).
///
/// The last two keep every `Filter` above them.
///
/// Anything else builds as written. Either way the view's result is the one
/// the plan as written would produce; this only changes how much of the
/// table a registration and a write have to touch.
pub(crate) struct IndexChoice<'a> {
    pub defs: &'a HashMap<String, Vec<IndexDef>>,
    pub params: Option<&'a Sp00kyValue>,
}

/// A filter chain split the way the planner reads it.
#[derive(Default)]
struct Conjuncts<'p> {
    /// `field = operand` on a body field, operand an orderable scalar for
    /// this view.
    eqs: Vec<(String, &'p Value)>,
    /// Predicates that read no row.
    gates: Vec<&'p Predicate>,
    /// `id = operand`: the record key.
    id: Option<&'p Value>,
    /// Anything an index cannot answer (ranges, `OR` over fields, `id`, …).
    residual: bool,
}

impl IndexChoice<'_> {
    fn conjuncts<'p>(&self, predicates: &[&'p Predicate]) -> Option<Conjuncts<'p>> {
        let mut out = Conjuncts::default();
        for predicate in predicates {
            self.split(predicate, &mut out);
        }
        // The same field held equal to two values matches nothing; leave that
        // to the Filter rather than reason about it here.
        for (i, (field, value)) in out.eqs.iter().enumerate() {
            for (other_field, other_value) in &out.eqs[i + 1..] {
                if field == other_field && !self.same_operand(value, other_value) {
                    return None;
                }
            }
        }
        let mut seen = std::collections::HashSet::new();
        out.eqs.retain(|(field, _)| seen.insert(field.clone()));
        Some(out)
    }

    fn split<'p>(&self, predicate: &'p Predicate, out: &mut Conjuncts<'p>) {
        match predicate {
            Predicate::True => {}
            Predicate::And { predicates } => {
                for p in predicates {
                    self.split(p, out);
                }
            }
            Predicate::Eq { field, value } if field.segments() == ["id"] => {
                out.id.get_or_insert(value);
                out.residual = true;
            }
            Predicate::Eq { field, value } if field.segments().first().is_some_and(|f| f != "id") && self.orderable(value) => {
                out.eqs.push((field.as_str(), value));
            }
            other if other.field_roots().is_empty() => out.gates.push(other),
            _ => out.residual = true,
        }
    }

    fn resolve(&self, operand: &Value) -> Option<Sp00kyValue> {
        resolve_predicate_value(operand, self.params)
    }

    fn orderable(&self, operand: &Value) -> bool {
        self.resolve(operand)
            .is_some_and(|v| SortableValue::is_orderable(ValueRef::from_value(&v)))
    }

    fn same_operand(&self, a: &Value, b: &Value) -> bool {
        match (self.resolve(a), self.resolve(b)) {
            (Some(a), Some(b)) => {
                compare_values(ValueRef::from_value(&a), ValueRef::from_value(&b)) == std::cmp::Ordering::Equal
            }
            _ => false,
        }
    }

    /// The table's indexes in a fixed order, so the same schema always picks
    /// the same index.
    fn candidates(&self, table: &str) -> Vec<&IndexDef> {
        let mut defs: Vec<&IndexDef> = self
            .defs
            .get(table)
            .map(|defs| defs.iter().filter(|d| !d.fields.iter().any(|f| f == "id")).collect())
            .unwrap_or_default();
        defs.sort();
        defs
    }

    fn binding(table: &str, def: &IndexDef, eqs: &[(String, &Value)], gates: Vec<Predicate>, prefix_len: usize) -> IndexBinding {
        let prefix = def.fields[..prefix_len]
            .iter()
            .map(|field| {
                let (_, operand) = eqs.iter().find(|(f, _)| f == field).expect("prefix field is held equal");
                (*operand).clone()
            })
            .collect();
        IndexBinding { table: table.to_string(), def: def.clone(), prefix, gates }
    }
}

impl IndexPlanner for IndexChoice<'_> {
    fn window(
        &self,
        table: &str,
        predicates: &[&Predicate],
        order_by: Option<&[OrderSpec]>,
        limit: usize,
        start: usize,
    ) -> Option<Box<dyn Operator>> {
        let conj = self.conjuncts(predicates)?;
        if conj.residual {
            return None;
        }
        let mut order: Vec<(String, bool)> = order_by
            .unwrap_or_default()
            .iter()
            .map(|spec| (spec.field.as_str(), spec.direction.eq_ignore_ascii_case("DESC")))
            .collect();
        // Ties already order by record key ascending, so a trailing
        // `id ASC` adds nothing. (`id DESC` is left to TopK.)
        if order.last().is_some_and(|(field, desc)| field == "id" && !desc) {
            order.pop();
        }
        if order.iter().any(|(field, _)| field == "id" || conj.eqs.iter().any(|(f, _)| f == field)) {
            return None;
        }
        let descending: Vec<bool> = order.iter().map(|(_, desc)| *desc).collect();
        let k = conj.eqs.len();
        let def = self.candidates(table).into_iter().find(|def| {
            def.fields.len() == k + order.len()
                && def.fields[..k].iter().all(|f| conj.eqs.iter().any(|(e, _)| e == f))
                && def.fields[k..].iter().zip(&order).all(|(f, (o, _))| f == o)
        })?;
        let gates = conj.gates.into_iter().cloned().collect();
        let binding = Self::binding(table, def, &conj.eqs, gates, k);
        Some(Box::new(IndexWindow::new(binding, limit, start, descending)))
    }

    fn scan(&self, table: &str, predicates: &[&Predicate]) -> Option<Box<dyn Operator>> {
        let conj = self.conjuncts(predicates)?;
        if let Some(key) = conj.id {
            return Some(Box::new(KeyScan { table: table.to_string(), key: key.clone() }));
        }
        let (def, k) = self
            .candidates(table)
            .into_iter()
            .map(|def| {
                let k = def.fields.iter().take_while(|f| conj.eqs.iter().any(|(e, _)| e == *f)).count();
                (def, k)
            })
            .filter(|(_, k)| *k > 0)
            .max_by(|(a, ka), (b, kb)| ka.cmp(kb).then_with(|| b.cmp(a)))?;
        let binding = Self::binding(table, def, &conj.eqs, Vec::new(), k);
        Some(Box::new(IndexedScan::new(binding)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_definitions_info_for_table_prints() {
        assert_eq!(
            parse_define_index("DEFINE INDEX game_database_sort ON game FIELDS database, sort_index"),
            Some(IndexDef { name: "game_database_sort".into(), fields: vec!["database".into(), "sort_index".into()] })
        );
        assert_eq!(
            parse_define_index("DEFINE INDEX OVERWRITE player_name_unique ON TABLE player_name FIELDS author, type, name UNIQUE"),
            Some(IndexDef { name: "player_name_unique".into(), fields: vec!["author".into(), "type".into(), "name".into()] })
        );
        assert_eq!(
            parse_define_index("DEFINE INDEX idx ON t COLUMNS a.b COMMENT 'x'"),
            Some(IndexDef { name: "idx".into(), fields: vec!["a.b".into()] })
        );
        assert_eq!(
            parse_define_index("DEFINE INDEX `quoted` ON t FIELDS `owner` CONCURRENTLY"),
            Some(IndexDef { name: "quoted".into(), fields: vec!["owner".into()] })
        );
    }

    #[test]
    fn ignores_indexes_the_circuit_cannot_use() {
        assert_eq!(parse_define_index("DEFINE INDEX ft ON t FIELDS body SEARCH ANALYZER simple BM25"), None);
        assert_eq!(parse_define_index("DEFINE INDEX ft ON t FIELDS body FULLTEXT ANALYZER simple BM25"), None);
        assert_eq!(parse_define_index("DEFINE INDEX v ON t FIELDS emb HNSW DIMENSION 4"), None);
        assert_eq!(parse_define_index("DEFINE INDEX c ON t COUNT"), None);
        assert_eq!(parse_define_index("DEFINE TABLE t"), None);
    }
}
