import { createEffect, createMemo, createSignal, For, Show } from 'solid-js';
import { useDevTools } from '../../context/DevToolsContext';
import { JsonView } from '../ui/JsonView';
import { formatTime, formatRelativeTime } from '../../utils/formatters';
import type { FlagRow } from '../../types/devtools';
import { ImpersonationSection } from './ImpersonationSection';

/**
 * Everything about *who this browser is* and *what it is allowed to see*: the
 * session, plus the flags that gate features on top of it. They were two tabs
 * (Auth, Flags) and read as one story — the flag sections are meaningless
 * without knowing which user (and whether an admin) is signed in.
 *
 * Two sections: **Session** (read-only page state) and **Flags** (one card per
 * flag). Inside a card the two blast radii are labelled rather than separated:
 *
 *  - **Override** changes what THIS browser resolves. No auth, no network,
 *    works signed out. Always shown, for every key.
 *  - Everything else changes the flag for EVERY user. Admin only (`spky admin
 *    add`), enforced by SurrealDB rather than by hiding the UI — a non-admin
 *    simply has no definition to render those controls from.
 */
export function AccessTab() {
  const {
    state,
    flagsSnapshot,
    flagsError,
    isMutatingFlag,
    fetchFlags,
    isSp00kyAvailable,
    setFlagEnabled,
    setFlagUserVariant,
    setFlagOverride,
    clearFlagOverrides,
  } = useDevTools();

  // The tab body is unmounted while inactive (App.tsx wraps it in <Show>), so
  // this runs on every open — the snapshot is a point-in-time remote read.
  //
  // Gated on availability because the panel can mount before the page's client
  // is up, and an early call can only time out (30s) rather than fail fast.
  //
  // `attempted` rather than inferring intent from `flagsSnapshot()`: fetchFlags
  // CATCHES its errors, so on failure the snapshot stays null while
  // isFetchingFlags flips true -> false — which re-runs this effect, which
  // refetches, forever, one 30s remote read after another. A plain `let` dies
  // with the component, so re-opening the tab retries exactly once.
  let attempted = false;
  createEffect(() => {
    if (!isSp00kyAvailable()) return;
    if (attempted || flagsSnapshot()) return;
    attempted = true;
    void fetchFlags();
  });

  const snap = () => flagsSnapshot();
  const overrides = () => snap()?.overrides ?? {};

  /**
   * One card per flag this browser knows about, whether the key came from an
   * assignment, a local override, or (for admins) a definition. A key with only
   * an override still needs a card, otherwise there'd be no way to clear it —
   * which is also why `definition` is optional and every admin-only control is
   * rendered off it rather than off `isAdmin`.
   */
  const flagRows = createMemo(() => {
    const s = snap();
    if (!s) return [];
    const keys = new Set<string>();
    for (const a of s.assignments) keys.add(a.key);
    for (const k of Object.keys(s.overrides)) keys.add(k);
    for (const f of s.flags) keys.add(f.key);

    return [...keys].sort().map((key) => {
      const assigned = s.assignments.find((a) => a.key === key)?.variant;
      const definition = s.flags.find((f) => f.key === key);
      // Variants come from the definition when we can see it; otherwise infer
      // what we can, so a non-admin still gets a usable picker.
      const variants = definition?.variants?.length
        ? definition.variants
        : [...new Set(['off', 'on', assigned, s.overrides[key]?.variant].filter(Boolean))];
      return {
        key,
        assigned,
        definition,
        override: s.overrides[key]?.variant,
        variants: variants as string[],
      };
    });
  });

  const statusPill = () => {
    const s = snap();
    if (!s) return { label: 'Loading', cls: 'status-initializing' };
    if (!s.userId) return { label: 'Signed out', cls: 'status-destroyed' };
    if (s.isAdmin) return { label: 'Admin', cls: 'status-active' };
    return { label: 'Read-only', cls: 'status-updating' };
  };

  return (
    <div class="mcp-container">
      <div class="mcp-header">
        <h2>Access</h2>
        <div class="mcp-header-controls">
          {/* Refresh lives in the top toolbar now — it re-reads flags when this
              tab is active. */}
          <div class={`status-pill ${statusPill().cls}`}>
            <span class="status-dot" />
            {statusPill().label}
          </div>
        </div>
      </div>

      <Show when={flagsError()}>
        <div class="storage-health-banner error">{flagsError()}</div>
      </Show>

      {/* ---- Session ---------------------------------------------------- */}
      <div class="mcp-section">
        <h3>Session</h3>
        <div
          class="auth-status"
          classList={{
            authenticated: state.auth.isAuthenticated,
            'not-authenticated': !state.auth.isAuthenticated,
          }}
        >
          <div>
            <strong>Status:</strong>{' '}
            {state.auth.isAuthenticated ? 'Authenticated' : 'Not authenticated'}
          </div>

          <Show when={state.auth.user}>
            <div style="margin-top: 12px;">
              <strong>User:</strong>
              <div style="margin-left: 12px; margin-top: 4px;">
                <Show when={state.auth.user?.email}>
                  <div>
                    {/* oxlint-disable-next-line no-non-null-assertion */}
                    <strong>Email:</strong> {state.auth.user!.email}
                  </div>
                </Show>
                <Show when={state.auth.user?.roles && state.auth.user.roles.length > 0}>
                  <div style="margin-top: 4px;">
                    {/* oxlint-disable-next-line no-non-null-assertion */}
                    <strong>Roles:</strong> {state.auth.user!.roles!.join(', ')}
                  </div>
                </Show>
              </div>
            </div>
          </Show>

          {/* The flag snapshot is the only place the record id surfaces, and it
              is what `spky admin add` and the "Set for user" box below take. */}
          <Show when={snap()?.userId}>
            <div style="margin-top: 12px;">
              {/* oxlint-disable-next-line no-non-null-assertion */}
              <strong>Record:</strong> <span class="mono">{snap()!.userId}</span>
            </div>
          </Show>

          <div style="margin-top: 12px;">
            <strong>Last Check:</strong> {formatTime(state.auth.lastAuthCheck)} (
            {formatRelativeTime(state.auth.lastAuthCheck)})
          </div>
        </div>
      </div>

      <ImpersonationSection />

      {/* ---- Flags ------------------------------------------------------ */}
      <div class="mcp-section">
        <div class="flags-section-head">
          <h3>Flags</h3>
          <Show when={Object.keys(overrides()).length > 0}>
            <button class="btn" onClick={() => void clearFlagOverrides()}>
              Clear all overrides
            </button>
          </Show>
        </div>
        <p class="muted">
          <strong>Override</strong> forces a variant in this browser only — nothing is sent to the
          server, it survives reloads, and clearing it restores whatever the server says. Every
          other control on a card applies to <strong>every user</strong> and takes effect live.
          Creating, deleting and percentage rollouts stay with <code>spky flag</code>.
        </p>

        <Show
          when={flagRows().length > 0}
          fallback={
            <div class="empty-state">
              No flags seen yet. Assignments arrive once you sign in and the app calls{' '}
              <code>client.feature(...)</code>. Define one with{' '}
              <code>spky flag create &lt;key&gt;</code>.
            </div>
          }
        >
          <For each={flagRows()}>
            {(row) => (
              <FlagCard
                row={row}
                busy={isMutatingFlag() === row.key}
                anyBusy={isMutatingFlag() !== null}
                onToggle={(enabled) => void setFlagEnabled(row.key, enabled)}
                onVariant={(variant, remove, userId) =>
                  void setFlagUserVariant(row.key, variant, remove, userId)
                }
                onOverride={(variant) => void setFlagOverride(row.key, variant)}
              />
            )}
          </For>
        </Show>

        {/* Only the admin-only halves of the cards are missing, so this is a
            footnote under the list rather than a replacement for it. */}
        <Show when={!snap()?.isAdmin}>
          <AdminFallback />
        </Show>
      </div>
    </div>
  );
}

/**
 * Why the cards carry no server-side controls. These are four genuinely
 * different problems with four different fixes, so they must not collapse into
 * one message.
 */
function AdminFallback() {
  const { flagsSnapshot, flagsError } = useDevTools();
  const s = () => flagsSnapshot();

  return (
    <Show when={s()} fallback={<div class="empty-state">Loading…</div>}>
      <Show
        when={s()!.userId}
        fallback={
          <div class="empty-state">
            Sign in to manage flags for everyone. Overrides work while signed out.
          </div>
        }
      >
        <Show
          when={!flagsError()}
          fallback={
            <div class="empty-state">
              Couldn't reach the flag tables. If this deployment predates the Access tab, run{' '}
              <code>spky migrate</code> (or redeploy) to apply the internal schema.
            </div>
          }
        >
          <div class="empty-state">
            You're not an admin, so flag definitions are hidden from this client and the cards show
            overrides only. Grant access with{' '}
            <code class="mono">spky admin add {shortUser(s()!.userId!)}</code>.
          </div>
        </Show>
      </Show>
    </Show>
  );
}

function shortUser(id: string): string {
  return id.startsWith('user:') ? id : `user:${id}`;
}

/**
 * SurrealDB datetimes arrive as `Date` objects (surrealdb 2.x decodes the CBOR
 * tag), even though the row type calls them strings. Solid renders a plain
 * object as NOTHING, which is why the raw value showed an empty "Updated" row.
 */
function formatWhen(value: unknown): string | null {
  if (value == null) return null;
  const d = value instanceof Date ? value : new Date(String(value));
  return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleString();
}

/** Every user id currently allowlisted, flattened to `[variant, userId]` pairs. */
function allowlistEntries(flag: FlagRow): Array<{ variant: string; user: string }> {
  const out: Array<{ variant: string; user: string }> = [];
  for (const rule of flag.rules) {
    const r = rule as { kind?: string; variant?: string; users?: unknown[] };
    if (r.kind !== 'allowlist' || !r.variant) continue;
    for (const u of r.users ?? []) out.push({ variant: r.variant, user: String(u) });
  }
  return out;
}

interface FlagCardRow {
  key: string;
  assigned?: string;
  /** Absent for a non-admin (or for a key known only from a local override). */
  definition?: FlagRow;
  override?: string;
  variants: string[];
}

/**
 * One flag, both scopes. The local override sits in the same key/value block as
 * the server-resolved value it shadows — that adjacency IS the point, since the
 * tab's most confusing failure mode is "why is this behaving differently for
 * me?". Everything below the block is a server mutation and needs the
 * definition, which only an admin can read.
 */
function FlagCard(props: {
  row: FlagCardRow;
  busy: boolean;
  anyBusy: boolean;
  onToggle: (enabled: boolean) => void;
  onVariant: (variant: string, remove: boolean, userId?: string) => void;
  onOverride: (variant: string | null) => void;
}) {
  const flag = () => props.row.definition;
  const allowlisted = () => flag()?.selfAllowlistedVariant;
  const [target, setTarget] = createSignal('');
  // `fn::feature::allow` takes a `record`, so only a record id works here — a
  // bare username would fail the type check server-side with a worse message.
  const targetId = () => {
    const raw = target().trim();
    if (!raw) return null;
    return raw.includes(':') ? raw : `user:${raw}`;
  };
  const entries = createMemo(() => {
    const f = flag();
    return f ? allowlistEntries(f) : [];
  });

  return (
    <div
      class="flags-card"
      classList={{ 'flags-busy': props.busy, 'flags-overridden': !!props.row.override }}
    >
      <div class="flags-card-head">
        <span class="mono flags-key">{props.row.key}</span>
        <Show when={flag()}>
          {(f) => (
            <label class="mcp-toggle" title="Enable/disable for all users">
              <input
                type="checkbox"
                checked={f().enabled}
                // Disabled while ANY flag is mutating: materialize writes
                // `_00_user_feature` rows under a UNIQUE (user, key) index, and
                // overlapping runs can collide.
                disabled={props.anyBusy}
                onChange={(e) => props.onToggle(e.currentTarget.checked)}
              />
              <span class="mcp-toggle-slider" />
            </label>
          )}
        </Show>
      </div>

      <Show when={flag()?.description}>
        {(description) => <p class="muted">{description()}</p>}
      </Show>

      <div class="kv">
        <div class="kv-row">
          <span class="kv-k">Assigned</span>
          <span class="kv-v mono muted">{props.row.assigned ?? '—'}</span>
        </div>
        <div class="kv-row">
          <span class="kv-k">Override</span>
          <span class="kv-v flags-override-control">
            <select
              class="flags-variant-select"
              title="Forces this variant in this browser only"
              value={props.row.override ?? ''}
              onChange={(e) => props.onOverride(e.currentTarget.value || null)}
            >
              <option value="">(none)</option>
              <For each={props.row.variants}>
                {(variant) => <option value={variant}>{variant}</option>}
              </For>
            </select>
            <Show when={props.row.override}>
              <button
                class="icon-btn"
                title="Clear override"
                onClick={() => props.onOverride(null)}
              >
                ✕
              </button>
            </Show>
          </span>
        </div>
        <Show when={flag()}>
          {(f) => (
            <>
              <div class="kv-row">
                <span class="kv-k">Default</span>
                <span class="kv-v mono">{f().default_variant}</span>
              </div>
              <div class="kv-row">
                <span class="kv-k">You</span>
                <span class="kv-v mono">
                  {allowlisted() ? `allowlisted → ${allowlisted()}` : 'not allowlisted'}
                </span>
              </div>
              <Show when={formatWhen(f().updated_at)}>
                {(when) => (
                  <div class="kv-row">
                    <span class="kv-k">Updated</span>
                    <span class="kv-v mono muted">{when()}</span>
                  </div>
                )}
              </Show>
            </>
          )}
        </Show>
      </div>

      <Show when={flag()}>
        {(f) => (
          <>
            <div class="flags-variant-group">
              <span class="kv-k">Set for me</span>
              <For each={f().variants}>
                {(variant) => (
                  <button
                    class="btn"
                    classList={{ 'flags-variant-active': allowlisted() === variant }}
                    disabled={props.anyBusy}
                    onClick={() => props.onVariant(variant, false)}
                  >
                    {variant}
                  </button>
                )}
              </For>
              <Show when={allowlisted()}>
                {/* NOT `.delete-btn` — that is a 20x20 icon box, so a text label
                overflows it and paints over the variant buttons. */}
                <button
                  class="btn flags-btn-danger"
                  disabled={props.anyBusy}
                  onClick={() => props.onVariant(allowlisted()!, true)}
                >
                  Remove me
                </button>
              </Show>
            </div>

            {/* Same two mutations, aimed at somebody else's record id. The allowlist
            is a property of the flag, so this is exactly what `spky flag set
            --for-user` writes; nothing here is local to this browser. */}
            <div class="flags-variant-group flags-target-row">
              <span class="kv-k">Set for user</span>
              <input
                class="flags-target-input mono"
                placeholder="user:xxxxxxxx"
                value={target()}
                disabled={props.anyBusy}
                onInput={(e) => setTarget(e.currentTarget.value)}
              />
              <For each={f().variants}>
                {(variant) => (
                  <button
                    class="btn"
                    disabled={props.anyBusy || !targetId()}
                    title={
                      targetId() ? `Allowlist ${targetId()} → ${variant}` : 'Enter a user record id'
                    }
                    onClick={() => props.onVariant(variant, false, targetId()!)}
                  >
                    {variant}
                  </button>
                )}
              </For>
              <button
                class="btn flags-btn-danger"
                disabled={props.anyBusy || !targetId()}
                title="Remove this user from every allowlist on this flag"
                onClick={() => props.onVariant(f().default_variant, true, targetId()!)}
              >
                Remove
              </button>
            </div>

            <Show when={entries().length > 0}>
              <details class="detail-section">
                <summary>Allowlisted users ({entries().length})</summary>
                <table class="data-table">
                  <tbody>
                    <For each={entries()}>
                      {(entry) => (
                        <tr>
                          <td class="mono">{entry.user}</td>
                          <td class="mono muted">→ {entry.variant}</td>
                          <td>
                            <button
                              class="icon-btn"
                              title={`Remove ${entry.user} from this flag`}
                              disabled={props.anyBusy}
                              onClick={() => props.onVariant(entry.variant, true, entry.user)}
                            >
                              ✕
                            </button>
                          </td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </details>
            </Show>

            <Show when={f().rules.length > 0 || f().payloads}>
              <details class="detail-section">
                <summary>Rules &amp; payloads</summary>
                <JsonView
                  value={{ rules: f().rules, payloads: f().payloads }}
                  class="row-pane-json"
                />
              </details>
            </Show>
          </>
        )}
      </Show>
    </div>
  );
}
