import { createEffect, createMemo, createSignal, For, onCleanup, Show } from 'solid-js';
import { useDevTools } from '../../context/DevToolsContext';
import type { ImpersonationUser } from '../../types/devtools';

/**
 * Act as another user, from the Access tab.
 *
 * Nothing here is a permission. The page's core calls
 * `fn::_00_impersonate::*`, which SurrealDB only defines when the project set
 * `impersonation.enabled` and only lets `_00_admin` members run. This section
 * reads that state back so it can explain itself instead of failing on click.
 */
export function ImpersonationSection() {
  const {
    impersonation,
    impersonationError,
    isImpersonationBusy,
    fetchImpersonation,
    isSp00kyAvailable,
    stopImpersonation,
  } = useDevTools();

  // Same one-shot-per-mount guard as the flag snapshot in AccessTab.
  let attempted = false;
  createEffect(() => {
    if (!isSp00kyAvailable() || attempted || impersonation()) return;
    attempted = true;
    void fetchImpersonation();
  });

  const status = () => impersonation();
  const current = () => status()?.current ?? null;

  return (
    <div class="mcp-section">
      <div class="flags-section-head">
        <h3>Impersonate</h3>
        <Show when={status()}>
          {(s) => (
            <div class={`status-pill ${pillClass(s())}`}>
              <span class="status-dot" />
              {pillLabel(s())}
            </div>
          )}
        </Show>
      </div>

      <Show when={impersonationError()}>
        <div class="storage-health-banner error">{impersonationError()}</div>
      </Show>

      <Show when={status()} fallback={<div class="empty-state">Loading…</div>}>
        <Show when={current()} fallback={<StartCard />}>
          {(session) => (
            <div class="imp-card active">
              <div class="imp-stripe" />
              <div class="imp-body">
                <div class="imp-head">
                  <span class="imp-title">
                    Acting as <span class="mono">{session().target}</span>
                  </span>
                  <span class="imp-actions">
                    <button
                      class="storage-persist-btn imp-stop"
                      disabled={isImpersonationBusy()}
                      onClick={() => void stopImpersonation()}
                    >
                      {isImpersonationBusy() ? 'Stopping…' : 'Stop'}
                    </button>
                  </span>
                </div>
                <div class="kv imp-kv">
                  <div class="kv-row">
                    <span class="kv-k">Admin</span>
                    <span class="kv-v mono">{session().admin}</span>
                  </div>
                  <div class="kv-row">
                    <span class="kv-k">Token renews</span>
                    <span class="kv-v muted">{formatWhen(session().tokenExpiresAt) ?? 'automatically'}</span>
                  </div>
                  <div class="kv-row">
                    <span class="kv-k">Session</span>
                    <span class="kv-v mono muted">{session().session}</span>
                  </div>
                </div>
                <p class="imp-note" style="margin: 8px 0 0;">
                  The page shows a warning while this lasts, and every write is recorded in{' '}
                  <code>_00_impersonation_write</code>.
                </p>
              </div>
            </div>
          )}
        </Show>
      </Show>
    </div>
  );

  function StartCard() {
    const { searchImpersonationUsers, startImpersonation } = useDevTools();
    const [search, setSearch] = createSignal('');
    const [results, setResults] = createSignal<ImpersonationUser[]>([]);
    const [searchError, setSearchError] = createSignal<string | null>(null);
    const [target, setTarget] = createSignal<ImpersonationUser | null>(null);
    const [reason, setReason] = createSignal('');
    const [confirming, setConfirming] = createSignal(false);

    // Debounced: every keystroke is a remote call.
    let timer: ReturnType<typeof setTimeout> | undefined;
    let seq = 0;
    createEffect(() => {
      if (!status()?.enabled || !status()?.isAdmin) return;
      const q = search();
      clearTimeout(timer);
      timer = setTimeout(async () => {
        const mine = ++seq;
        try {
          const users = await searchImpersonationUsers(q);
          if (mine === seq) {
            setResults(users);
            setSearchError(null);
          }
        } catch (e) {
          if (mine === seq) setSearchError(e instanceof Error ? e.message : String(e));
        }
      }, 250);
    });
    onCleanup(() => clearTimeout(timer));

    const reasonOk = () => reason().trim().length >= 3;
    const canStart = () => !!target() && reasonOk() && !isImpersonationBusy();
    const sessions = createMemo(() => status()?.active ?? []);

    const start = async () => {
      const t = target();
      if (!t) return;
      const ok = await startImpersonation(t.id, reason().trim());
      setConfirming(false);
      if (ok) {
        setTarget(null);
        setReason('');
      }
    };

    return (
      <Show
        when={status()!.enabled}
        fallback={
          <div class="empty-state">
            Impersonation is off for this project. Turn it on with{' '}
            <code>impersonation: {'{'} enabled: true {'}'}</code> in <code>sp00ky.yml</code> and
            redeploy; until then the server cannot issue an impersonation token.
          </div>
        }
      >
        <Show
          when={status()!.isAdmin}
          fallback={
            <div class="empty-state">
              Admins only. Grant access with <code class="mono">spky admin add &lt;user&gt;</code>.
            </div>
          }
        >
          <div class="imp-card">
            <div class="imp-body">
              <p class="imp-note">
                The page switches to this user's session and sees exactly what they can. Admins
                cannot be impersonated; sessions are time-limited and audited.
              </p>
              <input
                class="imp-field"
                placeholder="Search users by id, name or email"
                value={search()}
                onInput={(e) => setSearch(e.currentTarget.value)}
              />
              <Show when={searchError()}>
                <div class="storage-health-banner error">{searchError()}</div>
              </Show>
              <div class="imp-list">
                <For
                  each={results()}
                  fallback={
                    <div class="empty-state" style="padding: 12px;">
                      {search().trim() ? 'No matching users.' : 'No users yet.'}
                    </div>
                  }
                >
                  {(user) => (
                    <button
                      class="imp-row"
                      classList={{ selected: target()?.id === user.id }}
                      disabled={user.is_admin}
                      title={user.is_admin ? 'Admins cannot be impersonated' : `Select ${user.id}`}
                      onClick={() => {
                        setTarget(user);
                        setConfirming(false);
                      }}
                    >
                      <span class="imp-id mono">{user.id}</span>
                      <span class="imp-sub muted">{describe(user)}</span>
                      <Show when={user.is_admin}>
                        <span class="imp-tag">admin</span>
                      </Show>
                    </button>
                  )}
                </For>
              </div>

              <Show when={target()}>
                {(t) => (
                  <div class="imp-confirm">
                    <div class="kv">
                      <div class="kv-row">
                        <span class="kv-k">Target</span>
                        <span class="kv-v mono">{t().id}</span>
                      </div>
                    </div>
                    <input
                      class="imp-field"
                      placeholder="Reason (required, kept in the audit log)"
                      value={reason()}
                      onInput={(e) => setReason(e.currentTarget.value)}
                    />
                    <Show
                      when={confirming()}
                      fallback={
                        <div class="imp-buttons">
                          <button
                            class="storage-persist-btn"
                            disabled={!canStart()}
                            onClick={() => setConfirming(true)}
                          >
                            Impersonate
                          </button>
                          <Show when={!reasonOk()}>
                            <span class="muted">A reason of at least 3 characters is required.</span>
                          </Show>
                        </div>
                      }
                    >
                      <div class="imp-buttons">
                        <button
                          class="storage-persist-btn"
                          disabled={!canStart()}
                          onClick={() => void start()}
                        >
                          {isImpersonationBusy() ? 'Starting…' : 'Start as this user'}
                        </button>
                        <button class="btn" onClick={() => setConfirming(false)}>
                          Cancel
                        </button>
                        <span class="muted imp-warn">This page will act as {t().id}.</span>
                      </div>
                    </Show>
                  </div>
                )}
              </Show>

              <Show when={sessions().length > 0}>
                <div class="imp-sessions">
                  <div class="imp-sessions-title">Open sessions</div>
                  <div class="kv">
                    <For each={sessions()}>
                      {(s) => (
                        <div class="kv-row">
                          <span class="kv-k mono">{s.admin}</span>
                          <span class="kv-v mono muted">
                            {s.target} · {s.reason} · until {formatWhen(s.expires_at) ?? '?'}
                          </span>
                        </div>
                      )}
                    </For>
                  </div>
                </div>
              </Show>
            </div>
          </div>
        </Show>
      </Show>
    );
  }
}

function pillLabel(status: { enabled?: boolean; isAdmin?: boolean; current?: unknown }): string {
  if (status.current) return 'Active';
  if (!status.enabled) return 'Disabled';
  return status.isAdmin ? 'Available' : 'Admins only';
}

function pillClass(status: { enabled?: boolean; isAdmin?: boolean; current?: unknown }): string {
  if (status.current) return 'status-updating';
  if (!status.enabled) return 'status-destroyed';
  return status.isAdmin ? 'status-active' : 'status-initializing';
}

/** The project's search fields, flattened to one line. */
function describe(user: ImpersonationUser): string {
  return Object.entries(user)
    .filter(([k, v]) => k !== 'id' && k !== 'is_admin' && v !== null && v !== undefined && v !== '')
    .map(([, v]) => String(v))
    .join(' · ');
}

function formatWhen(value: unknown): string | null {
  if (value == null) return null;
  const d = value instanceof Date ? value : new Date(String(value));
  return Number.isNaN(d.getTime()) ? null : d.toLocaleString();
}
