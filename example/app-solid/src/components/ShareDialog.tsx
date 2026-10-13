import { createResource, createSignal, For, Show } from 'solid-js';
import { RecordId, Uuid, useDb, useQuery } from '@spooky-sync/client-solid';
import * as jose from 'jose';
import QRCode from 'qrcode';
import type { schema } from '../schema.gen';
import { useAuth } from '../lib/auth';
import { createHotkey } from '../lib/keyboard';
import { Tooltip } from './Tooltip';

interface ShareDialogProps {
  threadId: string;
  /** Collaborators can only open a published thread (see the thread table's
   *  SELECT rule in the schema). */
  published: boolean;
  isOpen: boolean;
  onClose: () => void;
}

const SHARE_TTL_DAYS = 7;

const parseRecordId = (id: string | RecordId): RecordId => {
  // `auth.userId()` is typed `string` but actually emits the SurrealDB SDK's
  // `RecordId` object at runtime. Pass that through; only parse the colon
  // form when we genuinely got a string.
  if (id instanceof RecordId) return id;
  const s = String(id);
  const idx = s.indexOf(':');
  if (idx <= 0) throw new Error(`Invalid record id: ${s}`);
  return new RecordId(s.slice(0, idx), s.slice(idx + 1));
};

const inviteUrl = (jwt: string) => `${window.location.origin}/invite/${jwt}`;

function formatExpiry(d: Date | string | number | null | undefined): string {
  if (d == null) return '';
  const t = +new Date(d as any);
  if (!Number.isFinite(t)) return '';
  const diff = t - Date.now();
  if (diff <= 0) return 'Expired';
  const days = Math.floor(diff / 86_400_000);
  const hours = Math.floor((diff % 86_400_000) / 3_600_000);
  const mins = Math.floor((diff % 3_600_000) / 60_000);
  if (days > 0) return `Expires in ${days}d`;
  if (hours > 0) return `Expires in ${hours}h`;
  if (mins > 0) return `Expires in ${mins}m`;
  return 'Expires soon';
}

function linkIdStr(link: { id: any }): string {
  return typeof link.id === 'string' ? link.id : `${link.id.tb}:${String(link.id.id)}`;
}

function genJti(): string {
  // 16 random bytes → base64url, plenty for a uniqueness nonce.
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export function ShareDialog(props: ShareDialogProps) {
  const db = useDb<typeof schema>();
  const auth = useAuth();
  const [isCreating, setIsCreating] = createSignal(false);
  const [copiedId, setCopiedId] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  // Single-expansion: at most one row shows its QR. New links open it,
  // toggling another collapses the previous.
  const [expandedId, setExpandedId] = createSignal<string | null>(null);

  // Local-first query — pulls from the synced cache, so the dialog renders
  // even when offline. The schema's `share_link` SELECT rule already scopes
  // to the issuer, no extra filter needed beyond the thread.
  const linksQuery = useQuery(
    () => {
      if (!props.isOpen) return null;
      return db
        .query('share_link')
        .where({ thread: props.threadId })
        .build();
    },
    { enabled: () => props.isOpen },
  );

  createHotkey('Escape', () => props.onClose(), () => ({ enabled: props.isOpen, ignoreInputs: false }));

  const create = async () => {
    if (isCreating()) return;
    setIsCreating(true);
    setError(null);
    try {
      const userId = auth.userId();
      if (!userId) throw new Error('Not authenticated.');

      // Read the private key from the synced local user row — the
      // field-level SELECT rule (`id = $auth.id`) means the sync stream
      // already includes it, and the dialog can sign offline.
      const privPem = auth.user()?.share_privkey;
      if (!privPem) {
        throw new Error("Couldn't find your sharing key. Try reloading after a brief reconnect.");
      }

      // Sign the JWT locally — no network needed.
      const privKey = await jose.importPKCS8(privPem, 'EdDSA');
      const expSeconds = Math.floor(Date.now() / 1000) + SHARE_TTL_DAYS * 24 * 3600;
      const jti = genJti();
      const jwt = await new jose.SignJWT({ jti })
        .setProtectedHeader({ alg: 'EdDSA' })
        .setIssuer(String(userId))
        .setSubject(String(props.threadId))
        .setIssuedAt()
        .setExpirationTime(expSeconds)
        .sign(privKey);

      // Write through the local mutation queue so `linksQuery` picks up
      // the new row immediately; sync propagates the row to remote where
      // the table-level CREATE rule (thread author only) still gates it.
      // A plain `table:key` string; see `idString`.
      const linkId = `share_link:${Uuid.v4().toString().replace(/-/g, '')}`;
      await db.create(linkId, {
        thread: parseRecordId(props.threadId),
        issuer: parseRecordId(userId),
        jwt,
        jti,
        exp: new Date(expSeconds * 1000),
      });
      // Open the freshly created row, collapsing any other.
      setExpandedId(linkId);
    } catch (e: any) {
      setError(e?.message || 'Failed to create share link.');
    } finally {
      setIsCreating(false);
    }
  };

  const copy = async (link: { id: string; jwt: string }) => {
    try {
      await navigator.clipboard.writeText(inviteUrl(link.jwt));
      setCopiedId(link.id);
      setTimeout(() => setCopiedId((cur) => (cur === link.id ? null : cur)), 1500);
    } catch (e: any) {
      setError(e?.message || 'Failed to copy.');
    }
  };

  const sortedLinks = () =>
    (linksQuery.data() ?? [])
      .slice()
      .sort((a: any, b: any) => +new Date(b.exp) - +new Date(a.exp));

  return (
    <Show when={props.isOpen}>
      <div
        class="fixed inset-0 bg-black/50 backdrop-blur-md z-[100] flex items-center justify-center p-4"
        onMouseDown={props.onClose}
      >
        <div
          class="animate-slide-up w-full max-w-md rounded-2xl overflow-hidden flex flex-col max-h-[85vh]"
          style="background: rgba(255, 255, 255, 0.05); backdrop-filter: blur(40px) saturate(1.5); -webkit-backdrop-filter: blur(40px) saturate(1.5); border: 1px solid rgba(255, 255, 255, 0.1); box-shadow: 0 8px 48px rgba(0, 0, 0, 0.4), inset 0 0.5px 0 rgba(255, 255, 255, 0.12);"
          onMouseDown={(e) => e.stopPropagation()}
        >
          <div class="px-6 pt-6 pb-5 shrink-0">
            <div class="flex items-start justify-between gap-3">
              <div class="space-y-1.5 min-w-0">
                <h2 class="text-base font-semibold text-zinc-100 tracking-tight">Share thread</h2>
                <p class="text-[12.5px] text-zinc-500 leading-relaxed">
                  Anyone with a link joins as an editor. Links can't be revoked and expire after {SHARE_TTL_DAYS} days.
                </p>
                <Show when={!props.published}>
                  <p class="text-[12.5px] text-amber-300/80 leading-relaxed">
                    This thread is a draft: people you invite can open it once you publish it.
                  </p>
                </Show>
              </div>
              <Tooltip text="Close" kbd="Esc" position="bottom">
                <button
                  onMouseDown={props.onClose}
                  class="-mr-1 -mt-1 text-zinc-500 hover:text-white transition-colors duration-150 p-1.5 rounded-lg hover:bg-white/[0.06] shrink-0"
                  aria-label="Close"
                >
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </Tooltip>
            </div>

            <button
              onMouseDown={create}
              disabled={isCreating()}
              class="mt-5 w-full h-9 inline-flex items-center justify-center gap-2 bg-white text-zinc-900 hover:bg-zinc-200 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg text-[13px] font-medium transition-colors duration-150 leading-none"
            >
              <Show when={!isCreating()} fallback={<span class="opacity-70">Creating link…</span>}>
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M12 5v14M5 12h14" />
                </svg>
                <span>New link</span>
              </Show>
            </button>

            <Show when={error()}>
              <p class="mt-2.5 text-[12px] text-red-400/90">{error()}</p>
            </Show>
          </div>

          <div class="shrink-0" style="border-top: 1px solid rgba(255, 255, 255, 0.06);" />

          <div class="overflow-y-auto px-2 py-2 max-h-[260px]">
            <Show
              when={sortedLinks().length > 0}
              fallback={
                <div class="text-center text-[12px] text-zinc-600 py-8">
                  No active links.
                </div>
              }
            >
              <ul class="space-y-0.5">
                <For each={sortedLinks()}>
                  {(link) => {
                    const id = linkIdStr(link);
                    return (
                      <ShareLinkRow
                        link={link}
                        copiedId={copiedId()}
                        onCopy={copy}
                        isExpanded={expandedId() === id}
                        onToggle={() => setExpandedId((cur) => (cur === id ? null : id))}
                      />
                    );
                  }}
                </For>
              </ul>
            </Show>
          </div>
        </div>
      </div>
    </Show>
  );
}

interface ShareLinkRow {
  id: any;
  jwt: string;
  exp?: any;
}

function ShareLinkRow(props: {
  link: ShareLinkRow;
  copiedId: string | null;
  onCopy: (link: { id: string; jwt: string }) => void;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const idStr = () => linkIdStr(props.link);

  const url = () => inviteUrl(props.link.jwt);
  const copied = () => props.copiedId === idStr();

  const [qrSvg] = createResource(
    () => (props.isExpanded ? props.link.jwt : null),
    async (jwt) => {
      if (!jwt) return '';
      try {
        return await QRCode.toString(inviteUrl(jwt), {
          type: 'svg',
          margin: 0,
          width: 144,
          color: { dark: '#e4e4e7', light: '#0000' },
        });
      } catch {
        return '';
      }
    },
  );

  return (
    <li class="rounded-lg transition-colors duration-150 hover:bg-white/[0.04]">
      <div class="flex items-center gap-1 px-2 py-1.5">
        <div class="flex-1 min-w-0 px-1">
          <div class="text-[12px] text-zinc-200 font-mono truncate" title={url()}>
            {url()}
          </div>
          <div class="text-[11px] leading-none text-zinc-600 mt-1">{formatExpiry(props.link.exp)}</div>
        </div>
        <Tooltip text={props.isExpanded ? 'Hide QR' : 'Show QR'} position="top">
          <button
            onMouseDown={props.onToggle}
            class={`h-7 w-7 inline-flex items-center justify-center rounded-md transition-colors duration-150 ${
              props.isExpanded
                ? 'text-zinc-100 bg-white/[0.08]'
                : 'text-zinc-500 hover:text-zinc-100 hover:bg-white/[0.06]'
            }`}
            aria-label={props.isExpanded ? 'Hide QR code' : 'Show QR code'}
          >
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
              <rect x="3" y="3" width="7" height="7" rx="1" />
              <rect x="14" y="3" width="7" height="7" rx="1" />
              <rect x="3" y="14" width="7" height="7" rx="1" />
              <path stroke-linecap="round" d="M14 14h3v3M21 14v3M14 18v3M17 21h4M21 18v3" />
            </svg>
          </button>
        </Tooltip>
        <button
          onMouseDown={() => props.onCopy({ id: idStr(), jwt: props.link.jwt })}
          class={`h-7 inline-flex items-center justify-center px-2.5 rounded-md text-[11px] font-medium transition-colors duration-150 ${
            copied()
              ? 'text-emerald-300/90 bg-emerald-400/[0.06]'
              : 'text-zinc-400 hover:text-white hover:bg-white/[0.06]'
          }`}
        >
          {copied() ? 'Copied' : 'Copy'}
        </button>
      </div>
      <Show when={props.isExpanded}>
        <div class="px-3 pb-3 pt-1 flex justify-center animate-fade-in">
          <Show
            when={qrSvg()}
            fallback={<div class="w-[144px] h-[144px] rounded-md" style="background: rgba(255, 255, 255, 0.02);" />}
          >
            <div
              class="rounded-md p-2.5"
              style="background: rgba(255, 255, 255, 0.03); border: 1px solid rgba(255, 255, 255, 0.06);"
              innerHTML={qrSvg()}
            />
          </Show>
        </div>
      </Show>
    </li>
  );
}
