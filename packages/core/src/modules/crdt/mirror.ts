import type { Container, LoroDoc, LoroList, LoroMap } from 'loro-crdt';

/**
 * The plain text a `@crdt` field shows as its column value, rendered the same
 * way the SSP fold renders it (`packages/ssp-node/src/crdt/mirror.rs`).
 */

export type CrdtKind = 'text' | 'prosemirror';

const PM_ROOT = 'doc';
const PM_NODE_NAME = 'nodeName';
const PM_CHILDREN = 'children';
const PM_LINE_BREAKS = new Set(['hard_break', 'hardBreak']);

export function renderText(doc: LoroDoc, field: string, kind: CrdtKind): string {
  return kind === 'text' ? doc.getText(field).toString() : nodeText(doc.getMap(PM_ROOT));
}

/** A node holding text runs is inline content (its children concatenate); a
 *  node holding only nodes is a block container (its children are lines). */
function nodeText(node: LoroMap): string {
  const name = node.get(PM_NODE_NAME);
  if (typeof name === 'string' && PM_LINE_BREAKS.has(name)) return '\n';
  const children = node.get(PM_CHILDREN) as Container | undefined;
  if (!children || children.kind() !== 'List') return '';
  const list = children as LoroList;
  const parts: Array<{ text: boolean; value: string }> = [];
  for (let i = 0; i < list.length; i++) {
    const child = list.get(i) as Container | unknown;
    if (!child || typeof child !== 'object' || !('kind' in child)) continue;
    const c = child as Container;
    if (c.kind() === 'Text') parts.push({ text: true, value: c.toString() });
    else if (c.kind() === 'Map') parts.push({ text: false, value: nodeText(c as LoroMap) });
  }
  const inline = parts.some((p) => p.text);
  return parts.map((p) => p.value).join(inline ? '' : '\n');
}
