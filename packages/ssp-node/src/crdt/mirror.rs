//! The plain text a `@crdt` field shows as its column value.
//!
//! Lists, search and `WHERE` read the parent's field, never the Loro
//! document, so the fold renders the document to text on every fold. The
//! client renders the same text for its optimistic local copy
//! (`packages/core/src/modules/crdt/mirror.ts`); the two must agree, or a list
//! would flicker when the server's version lands.

use loro::{Container, LoroDoc, LoroList, LoroMap, LoroValue, ValueOrContainer};

/// The document shape a `-- @crdt <kind>` field uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// One `LoroText` named after the field.
    Text,
    /// A loro-prosemirror tree: root map `"doc"` of
    /// `{ nodeName, attributes, children: [map | text] }`.
    Prosemirror,
}

impl Kind {
    pub fn parse(kind: &str) -> Option<Self> {
        match kind {
            "text" => Some(Kind::Text),
            "prosemirror" => Some(Kind::Prosemirror),
            _ => None,
        }
    }
}

/// loro-prosemirror's root map key.
const PM_ROOT: &str = "doc";
const PM_NODE_NAME: &str = "nodeName";
const PM_CHILDREN: &str = "children";
/// Inline nodes that read as a line break.
const PM_LINE_BREAKS: &[&str] = &["hard_break", "hardBreak"];

pub fn render(doc: &LoroDoc, field: &str, kind: Kind) -> String {
    match kind {
        Kind::Text => doc.get_text(field).to_string(),
        Kind::Prosemirror => node_text(&doc.get_map(PM_ROOT)),
    }
}

/// A node's text. A node holding text runs is inline content (a paragraph,
/// a heading): its children concatenate. A node holding only nodes is a
/// block container (the doc, a list, a quote): its children are lines.
fn node_text(node: &LoroMap) -> String {
    if let Some(ValueOrContainer::Value(LoroValue::String(name))) = node.get(PM_NODE_NAME) {
        if PM_LINE_BREAKS.contains(&name.as_str()) {
            return "\n".to_string();
        }
    }
    let Some(ValueOrContainer::Container(Container::List(children))) = node.get(PM_CHILDREN) else {
        return String::new();
    };
    let parts = child_texts(&children);
    let inline = parts.iter().any(|(is_text, _)| *is_text);
    let texts = parts.into_iter().map(|(_, text)| text);
    if inline {
        texts.collect()
    } else {
        texts.collect::<Vec<_>>().join("\n")
    }
}

fn child_texts(children: &LoroList) -> Vec<(bool, String)> {
    (0..children.len())
        .filter_map(|i| match children.get(i)? {
            ValueOrContainer::Container(Container::Text(text)) => Some((true, text.to_string())),
            ValueOrContainer::Container(Container::Map(map)) => Some((false, node_text(&map))),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use loro::{LoroList, LoroMap, LoroText};

    fn node(parent: &LoroList, name: &str) -> LoroMap {
        let map = parent.insert_container(parent.len(), LoroMap::new()).unwrap();
        map.insert(PM_NODE_NAME, name).unwrap();
        map
    }

    fn children(map: &LoroMap) -> LoroList {
        map.get_or_create_container(PM_CHILDREN, LoroList::new()).unwrap()
    }

    fn text(parent: &LoroList, s: &str) {
        let t = parent.insert_container(parent.len(), LoroText::new()).unwrap();
        t.insert(0, s).unwrap();
    }

    #[test]
    fn plain_text_field() {
        let doc = LoroDoc::new();
        doc.get_text("summary").insert(0, "héllo 🌍").unwrap();
        assert_eq!(render(&doc, "summary", Kind::Text), "héllo 🌍");
        assert_eq!(render(&LoroDoc::new(), "summary", Kind::Text), "");
    }

    #[test]
    fn prosemirror_blocks_are_lines_and_inline_runs_concatenate() {
        let doc = LoroDoc::new();
        let root = doc.get_map(PM_ROOT);
        root.insert(PM_NODE_NAME, "doc").unwrap();
        let top = children(&root);

        let h = node(&top, "heading");
        text(&children(&h), "Title");

        let p = node(&top, "paragraph");
        let pc = children(&p);
        text(&pc, "one ");
        text(&pc, "bold");
        node(&pc, "hard_break");
        text(&pc, "two");

        node(&top, "paragraph"); // empty paragraph

        let list = node(&top, "bullet_list");
        let items = children(&list);
        for s in ["a", "b"] {
            let item = node(&items, "list_item");
            let para = node(&children(&item), "paragraph");
            text(&children(&para), s);
        }
        doc.commit();

        assert_eq!(render(&doc, "content", Kind::Prosemirror), "Title\none bold\ntwo\n\na\nb");
    }

    #[test]
    fn empty_prosemirror_doc_is_empty_text() {
        assert_eq!(render(&LoroDoc::new(), "content", Kind::Prosemirror), "");
    }
}
