import type { Container, LoroDoc, LoroTreeNode } from 'loro-crdt';
import { encodeBase64 } from './bytes';

type LoroModule = typeof import('loro-crdt');

/**
 * Canonical state hash of a document: the same function as the server fold's
 * (`packages/ssp-node/src/crdt/hash.rs`), checked against the shared vectors in
 * `packages/ssp-node/tests/fixtures/crdt_state_hash.json`.
 *
 *   stateHash(doc) = hex(SHA-256(utf8(JCS(roots(doc)))))
 *
 * JCS is RFC 8785 (keys in UTF-16 order, `JSON.stringify` strings,
 * `Number#toString` numbers). Loro values map as: integers to numbers,
 * NaN/Infinity to `{"$f64": ...}`, bytes to `{"$bytes": "<base64>"}`, a text
 * to its string (marks are not hashed). Root keys holding `""`, `{}`, `[]` or
 * `0` are left out: touching a root creates it, but a document built from
 * update blobs alone does not have it.
 */
export async function stateHash(loro: LoroModule, doc: LoroDoc): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalState(loro, doc));
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return Array.from(digest, (b) => b.toString(16).padStart(2, '0')).join('');
}

/** The canonical JSON {@link stateHash} is taken over. */
export function canonicalState(loro: LoroModule, doc: LoroDoc): string {
  // Built by walking the containers into null-prototype objects: `toJSON()`
  // assigns keys, which silently drops a map key named `__proto__`.
  const roots: Record<string, unknown> = Object.create(null);
  for (const [name, id] of Object.entries(doc.getShallowValue())) {
    const value = walk(loro, doc.getContainerById(id));
    if (!isEmptyRoot(value)) roots[name] = value;
  }
  return canonicalize(roots);
}

function walk(loro: LoroModule, v: unknown): unknown {
  if (!loro.isContainer(v)) return v;
  const c = v as Container;
  switch (c.kind()) {
    case 'Map': {
      const map = c as import('loro-crdt').LoroMap;
      const out: Record<string, unknown> = Object.create(null);
      for (const k of map.keys()) out[k] = walk(loro, map.get(k));
      return out;
    }
    case 'List':
    case 'MovableList':
      return (c as import('loro-crdt').LoroList).toArray().map((item) => walk(loro, item));
    case 'Text':
      return (c as import('loro-crdt').LoroText).toString();
    case 'Counter':
      return (c as unknown as { value: number }).value;
    case 'Tree':
      return (c as import('loro-crdt').LoroTree).roots().map((n) => walkTreeNode(loro, n));
    default:
      throw new Error(`unsupported Loro container ${c.kind()}`);
  }
}

function walkTreeNode(loro: LoroModule, n: LoroTreeNode): unknown {
  const out: Record<string, unknown> = Object.create(null);
  out.parent = n.parent()?.id ?? null;
  out.index = n.index();
  out.meta = walk(loro, n.data);
  out.id = n.id;
  out.fractional_index = n.fractionalIndex();
  out.children = (n.children() ?? []).map((child) => walkTreeNode(loro, child));
  return out;
}

function isEmptyRoot(v: unknown): boolean {
  if (v === '' || v === 0) return true;
  if (Array.isArray(v)) return v.length === 0;
  if (v && typeof v === 'object' && !(v instanceof Uint8Array)) return Object.keys(v).length === 0;
  return false;
}

function canonicalize(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  switch (typeof v) {
    case 'boolean':
      return v ? 'true' : 'false';
    case 'number':
      if (!Number.isFinite(v)) return canonicalize({ $f64: String(v) });
      return Object.is(v, -0) ? '0' : String(v);
    case 'string':
      return JSON.stringify(v);
    case 'bigint':
      return canonicalize(Number(v));
    case 'object': {
      if (v instanceof Uint8Array) return canonicalize({ $bytes: encodeBase64(v) });
      if (Array.isArray(v)) return '[' + v.map(canonicalize).join(',') + ']';
      const obj = v as Record<string, unknown>;
      // Default sort compares UTF-16 code units, as RFC 8785 asks.
      const keys = Object.keys(obj).sort();
      return '{' + keys.map((k) => JSON.stringify(k) + ':' + canonicalize(obj[k])).join(',') + '}';
    }
    default:
      throw new Error(`unsupported value type ${typeof v}`);
  }
}
