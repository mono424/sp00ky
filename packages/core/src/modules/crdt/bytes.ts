/**
 * Byte columns reach the client in several shapes depending on the engine and
 * the path: `Uint8Array` (SQLite, a fresh local write), `ArrayBuffer` (the
 * surrealdb SDK's CBOR decode), a typed-array view, or a `number[]` (bytes
 * nested in a FLEXIBLE object on the SurrealDB-WASM engine). Normalize all of
 * them; anything else is not bytes.
 */
export function asBytes(value: unknown): Uint8Array | undefined {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (Array.isArray(value) && value.length > 0 && value.every((n) => typeof n === 'number')) {
    return Uint8Array.from(value as number[]);
  }
  return undefined;
}

export function decodeBase64(b64: string): Uint8Array {
  if (typeof atob === 'function') {
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
  }
  return new Uint8Array(Buffer.from(b64, 'base64'));
}

export function encodeBase64(bytes: Uint8Array): string {
  if (typeof btoa === 'function') {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
    return btoa(binary);
  }
  return Buffer.from(bytes).toString('base64');
}

/**
 * A random Loro peer id below 2^53 (so it is exact as a JS number), as the
 * decimal string Loro uses for `PeerID`. Each tab picks a new one per session
 * and claims it in `_00_crdt_peer` before any of its edits are sent.
 */
export function randomPeer(): `${number}` {
  const words = new Uint32Array(2);
  crypto.getRandomValues(words);
  const high = words[0] & 0x1fffff; // 21 bits
  const id = high * 0x100000000 + words[1];
  return String(id === 0 ? 1 : id) as `${number}`;
}
