import { createEffect, createSignal, For, onCleanup, Show } from 'solid-js';
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
    searchImpersonationUsers,
    startImpersonation,
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

  return (
    <div class="mcp-section">
      <h3>Impersonate</h3>
      <Show when={impersonationError()}>
        <div class="storage-health-banner error">{impersonationError()}</div>
      </Show>
      <Show when={status()} fallback={<div class="empty-state">Loading…</div>}>
        <Show when={status()!.current} fallback={<StartPanel />}>
          {(current) => (
            <div class="impersonation-active">
              <div>
                Acting as <span class="mono">{current().target}</span> (you are{' '}
                <span class="mono">{current().admin}</span>). The page shows a warning bar while
                this lasts, and every write is recorded in <code>_00_impersonation_write</code>.
              </div>
              <Show when={formatWhen(current().tokenExpiresAt)}>
                {(when) => <div class="muted">Token renews automatically; current one expires {when()}.</div>}
              </Show>
              <button
                class="btn impersonation-stop"
                disabled={isImpersonationBusy()}
                onClick={() => void stopImpersonation()}
              >
                {isImpersonationBusy() ? 'Stopping…' : 'Stop impersonating'}
              </button>
            </div>
          )}
        </Show>
      </Show>
    </div>
  );

  function StartPanel() {
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

    const canStart = () => !!target() && reason().trim().length >= 3 && !isImpersonationBusy();

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
            <code>impersonation: {'{'} enabled: true {'}'}</code> in <code>sp00ky.yml</code>, then
            redeploy. While it is off, the server has no way to issue an impersonation token.
          </div>
        }
      >
        <Show
          when={status()!.isAdmin}
          fallback={
            <div class="empty-state">
              Only admins can impersonate. Grant access with <code>spky admin add &lt;user&gt;</code>.
            </div>
          }
        >
          <p class="muted">
            The page switches to the chosen user's session: it sees and changes exactly what they
            can. Admins cannot be impersonated. Sessions are time-limited and audited.
          </p>
          <input
            class="flags-target-input impersonation-search"
            placeholder="Search users by id or name"
            value={search()}
            onInput={(e) => setSearch(e.currentTarget.value)}
          />
          <Show when={searchError()}>
            <div class="storage-health-banner error">{searchError()}</div>
          </Show>
          <div class="impersonation-results">
            <For each={results()} fallback={<div class="empty-state">No matching users.</div>}>
              {(user) => (
                <button
                  class="impersonation-user"
                  classList={{ selected: target()?.id === user.id }}
                  disabled={user.is_admin}
                  title={user.is_admin ? 'Admins cannot be impersonated' : `Select ${user.id}`}
                  onClick={() => {
                    setTarget(user);
                    setConfirming(false);
                  }}
                >
                  <span class="mono">{user.id}</span>
                  <span class="muted">{describe(user)}</span>
                  <Show when={user.is_admin}>
                    <span class="impersonation-tag">admin</span>
                  </Show>
                </button>
              )}
            </For>
          </div>
          <Show when={target()}>
            {(t) => (
              <div class="impersonation-confirm">
                <input
                  class="flags-target-input"
                  placeholder="Reason (required, kept in the audit log)"
                  value={reason()}
                  onInput={(e) => setReason(e.currentTarget.value)}
                />
                <Show
                  when={confirming()}
                  fallback={
                    <button class="btn" disabled={!canStart()} onClick={() => setConfirming(true)}>
                      Impersonate {t().id}
                    </button>
                  }
                >
                  <div class="impersonation-warning">
                    This page will act as <span class="mono">{t().id}</span> until you stop.
                  </div>
                  <div class="flags-variant-group">
                    <button class="btn impersonation-stop" disabled={!canStart()} onClick={() => void start()}>
                      {isImpersonationBusy() ? 'Starting…' : 'Start impersonating'}
                    </button>
                    <button class="btn" onClick={() => setConfirming(false)}>
                      Cancel
                    </button>
                  </div>
                </Show>
              </div>
            )}
          </Show>
          <Show when={(status()!.active ?? []).length > 0}>
            <h4>Open sessions</h4>
            <div class="kv">
              <For each={status()!.active}>
                {(s) => (
                  <div class="kv-row">
                    <span class="kv-k mono">{s.admin}</span>
                    <span class="kv-v mono muted">
                      → {s.target} · {s.reason} · until {formatWhen(s.expires_at) ?? '?'}
                    </span>
                  </div>
                )}
              </For>
            </div>
          </Show>
        </Show>
      </Show>
    );
  }
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
