/**
 * The workspace vault, unlocked in the browser.
 *
 * Env values are encrypted with the workspace's vault key, which only a
 * member's passphrase unwraps. The passphrase never leaves this page: it is
 * stretched here with exactly the CLI's argon2id (m = 64 MiB, t = 1, p = 4,
 * 32 bytes, the member's salt from `/vault/key-params`) and only the derived
 * key is sent, to `/envs/load` (reveal) or `/tenants/{id}/invitations` (share
 * the vault with an invitee). The derived key is held in memory, per
 * workspace, and forgotten after five idle minutes, when the tab has been
 * hidden for one, on sign-out, and on reload.
 */

import { createSignal } from 'solid-js';
import { cloud } from './cloud';

const IDLE_MS = 5 * 60_000;
const HIDDEN_MS = 60_000;

interface Unlocked {
  tenant: string;
  key: string;
  at: number;
}

const [unlocked, setUnlocked] = createSignal<Unlocked | null>(null);
let idleTimer: ReturnType<typeof setTimeout> | undefined;
let hiddenTimer: ReturnType<typeof setTimeout> | undefined;

export function lock() {
  clearTimeout(idleTimer);
  setUnlocked(null);
}

function touch() {
  clearTimeout(idleTimer);
  idleTimer = setTimeout(lock, IDLE_MS);
}

if (typeof document !== 'undefined') {
  document.addEventListener('visibilitychange', () => {
    clearTimeout(hiddenTimer);
    if (document.hidden) hiddenTimer = setTimeout(lock, HIDDEN_MS);
  });
}

/** Whether `tenant`'s vault is unlocked in this tab. */
export function isUnlocked(tenant: string | undefined): boolean {
  const u = unlocked();
  return !!u && !!tenant && u.tenant === tenant;
}

/** The derived key for `tenant`, refreshing the idle clock; null when locked. */
export function derivedKey(tenant: string): string | null {
  const u = unlocked();
  if (!u || u.tenant !== tenant) return null;
  touch();
  return u.key;
}

export interface VaultStatus {
  initialized: boolean;
}

export const vaultStatus = (tenant: string) => cloud<VaultStatus>('/vault/status', { tenant });

/**
 * Stretch `passphrase` into the member's derived key and check it against the
 * server by loading one environment. Throws with the server's words when the
 * passphrase is wrong.
 */
export async function unlock(tenant: string, projectId: string | null, passphrase: string): Promise<void> {
  const params = await cloud<{ salt: string }>('/vault/key-params', { tenant });
  const { argon2id } = await import('hash-wasm');
  const salt = new Uint8Array((params.salt.match(/.{2}/g) ?? []).map((b) => parseInt(b, 16)));
  const key = await argon2id({
    password: passphrase,
    salt,
    parallelism: 4,
    iterations: 1,
    memorySize: 65536,
    hashLength: 32,
    outputType: 'hex',
  });
  // A wrong passphrase still derives a key; only the server can tell. Ask it
  // with a cheap read when there is a project to read from.
  if (projectId) {
    await cloud(`/projects/${encodeURIComponent(projectId)}/envs/load`, {
      method: 'POST',
      tenant,
      body: { derived_key: key, environment: 'production' },
    });
  }
  setUnlocked({ tenant, key, at: Date.now() });
  touch();
}

/** One environment's values, decrypted by the server with the derived key. */
export async function loadValues(tenant: string, projectId: string, environment: 'development' | 'production') {
  const key = derivedKey(tenant);
  if (!key) throw new Error('The vault is locked');
  const res = await cloud<{ variables: Record<string, string> }>(`/projects/${encodeURIComponent(projectId)}/envs/load`, {
    method: 'POST',
    tenant,
    body: { derived_key: key, environment },
  });
  return res.variables ?? {};
}
