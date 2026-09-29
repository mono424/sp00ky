/**
 * Web Push wire types shared by the page (`db.webPush`), the service worker
 * bridge (`@spooky-sync/core/sw`) and the live feed. Pure: types plus a few
 * dependency-free helpers, safe in any context.
 *
 * `PushPayload` mirrors `packages/push-core/src/config.rs` (`PushPayload`,
 * `PayloadKind`, `Op`, `NotificationTemplate`). Keep the two in step.
 */

/** Payload format version this build understands. */
export const PUSH_PAYLOAD_VERSION = 1;

export type PushPayloadKind = 'rule' | 'message';
export type PushOp = 'create' | 'update' | 'delete';
export type PushUrgency = 'very-low' | 'low' | 'normal' | 'high';

/** One button on a notification (`NotificationAction` plus a per-action `url`). */
export interface PushNotificationAction {
  action: string;
  title: string;
  icon?: string;
  /** Opened instead of the notification's `url` when this action is clicked. */
  url?: string;
  [key: string]: unknown;
}

/**
 * What a content push shows. Every key other than `title` and `url` is passed
 * to `showNotification` as is, unknown ones included (`vibrate`, `timestamp`,
 * whatever a browser adds later).
 */
export interface PushNotification {
  title?: string;
  body?: string;
  icon?: string;
  badge?: string;
  image?: string;
  lang?: string;
  dir?: string;
  /** Defaults to the push's `topic`. */
  tag?: string;
  /** Opened (or focused) on click. */
  url?: string;
  requireInteraction?: boolean;
  renotify?: boolean;
  silent?: boolean;
  actions?: PushNotificationAction[];
  /** Merged into the notification's `data` next to the bridge's own keys. */
  data?: Record<string, unknown>;
  [key: string]: unknown;
}

/**
 * The JSON every push carries (then RFC 8291 encrypted). `notification`
 * present: a content push the service worker shows as is. Absent: a
 * content-free nudge, the app renders from its own data.
 */
export interface PushPayload {
  v: number;
  kind: PushPayloadKind;
  /** Rule name (`kind: 'rule'`). */
  rule?: string;
  /** `_00_push_message` id (`kind: 'message'`). */
  message?: string;
  /** The row that fired the rule. */
  table?: string;
  id?: string;
  op?: PushOp;
  topic?: string;
  notification?: PushNotification;
  data?: Record<string, unknown>;
  /** Epoch ms when the engine built it. */
  ts: number;
}

/** Page -> service worker: the session to use for live rendering. */
export interface BridgeTokenMessage {
  type: 'sp00ky:token';
  token: string;
  userId: string;
  /** The SurrealDB endpoint (`wss://host/rpc`), not the push endpoint. */
  endpoint: string;
  namespace: string;
  database: string;
  /** VAPID public key (base64url) the page subscribed with, for `pushsubscriptionchange`. */
  publicKey?: string;
  /** This browser's push endpoint, when subscribed. */
  pushEndpoint?: string;
}

/** Page -> service worker: forget the session and close every notification. */
export interface BridgeSignOutMessage {
  type: 'sp00ky:signout';
}

/** Service worker -> page messages. */
export type WebPushClientMessage =
  /** A push arrived while this page was visible, so nothing was shown. */
  | { type: 'sp00ky:push'; payload: PushPayload }
  /** A notification was clicked and the bridge was told to route in-app. */
  | { type: 'sp00ky:navigate'; url: string; payload?: PushPayload; action?: string }
  /** The browser rotated the push subscription; the page should `sync()`. */
  | { type: 'sp00ky:subscriptionchange'; endpoint?: string };

/** `urlBase64ToUint8Array`: the VAPID key as `applicationServerKey` wants it. */
export function base64UrlToBytes(value: string): Uint8Array {
  const padded =
    value.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (value.length % 4)) % 4);
  const raw = typeof atob === 'function' ? atob(padded) : decodeBase64Fallback(padded);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
  return out;
}

export function bytesToBase64Url(bytes: ArrayBuffer | Uint8Array): string {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let raw = '';
  for (let i = 0; i < view.length; i++) raw += String.fromCharCode(view[i]);
  const b64 = typeof btoa === 'function' ? btoa(raw) : encodeBase64Fallback(raw);
  return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export function sameBytes(
  a: ArrayBuffer | Uint8Array | null | undefined,
  b: ArrayBuffer | Uint8Array | null | undefined
): boolean {
  if (!a || !b) return false;
  const x = a instanceof Uint8Array ? a : new Uint8Array(a);
  const y = b instanceof Uint8Array ? b : new Uint8Array(b);
  if (x.length !== y.length) return false;
  for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return false;
  return true;
}

/** A payload this build can act on (`v` known, `kind` set). */
export function isPushPayload(value: unknown): value is PushPayload {
  if (!value || typeof value !== 'object') return false;
  const v = value as { v?: unknown; kind?: unknown };
  return v.v === PUSH_PAYLOAD_VERSION && (v.kind === 'rule' || v.kind === 'message');
}

const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

function decodeBase64Fallback(input: string): string {
  const clean = input.replace(/=+$/, '');
  let out = '';
  let buffer = 0;
  let bits = 0;
  for (const ch of clean) {
    const idx = B64.indexOf(ch);
    if (idx < 0) continue;
    buffer = (buffer << 6) | idx;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out += String.fromCharCode((buffer >> bits) & 0xff);
    }
  }
  return out;
}

function encodeBase64Fallback(raw: string): string {
  let out = '';
  for (let i = 0; i < raw.length; i += 3) {
    const a = raw.charCodeAt(i);
    const b = i + 1 < raw.length ? raw.charCodeAt(i + 1) : 0;
    const c = i + 2 < raw.length ? raw.charCodeAt(i + 2) : 0;
    const n = (a << 16) | (b << 8) | c;
    out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63];
    out += i + 1 < raw.length ? B64[(n >> 6) & 63] : '=';
    out += i + 2 < raw.length ? B64[n & 63] : '=';
  }
  return out;
}
