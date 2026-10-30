import { For, Show, createMemo, createResource, createSignal } from 'solid-js';
import { api, currentMode } from '../api/client';
import { PageHead } from '../components/Chrome';
import { FilterMenu, FxCell, Sq, type FilterValue } from '../components/Frame';
import { toast } from '../components/Actions';
import { formatStamp } from '../lib/format';
import type { McpTool, MeResponse, SessionScope, TokenResponse } from '../api/types';

/**
 * MCP access: mint a token an AI agent can use against this scheduler's own
 * MCP endpoint, and show what that agent will be able to do.
 *
 * The token is shown exactly once. It is a signed session with a long life,
 * not a row anywhere, so the server cannot show it again; the page says so
 * rather than offering a "reveal" that would have nothing to reveal.
 */

const LIFETIMES = [7, 30, 90, 365] as const;

interface JsonRpcResult<T> {
  result?: T;
  error?: { code: number; message: string };
}

async function listTools(): Promise<McpTool[]> {
  const res = await api.post<JsonRpcResult<{ tools: McpTool[] }>>('/mcp', {
    jsonrpc: '2.0',
    id: 1,
    method: 'tools/list',
  });
  if (res.error) throw new Error(res.error.message);
  return res.result?.tools ?? [];
}

/** Where an MCP client must point: the page origin, or the standalone endpoint. */
function endpointBase(): string {
  const mode = currentMode();
  return mode.embedded ? window.location.origin : mode.baseUrl;
}

function serverName(): string {
  const slug = currentMode().config?.project_slug;
  return slug ? `spky-admin-${slug}` : 'spky-admin';
}

/** The area a tool belongs to, by name, in the sidebar's own groups. */
function groupOf(name: string): string {
  // Before the Run rule, which `schedule` would otherwise catch.
  if (/^scheduler_/.test(name)) return 'Cluster';
  if (/^(ssp|backend|view|presence)/.test(name)) return 'Sync';
  if (/^(workflow_|schedule|job|durable_)/.test(name)) return 'Run';
  if (/^(backup|cloud_|pool|machine_)/.test(name)) return 'Data';
  return 'Cluster';
}

const GROUP_ORDER = ['Cluster', 'Sync', 'Run', 'Data'];

type Kind = 'read' | 'write' | 'destructive';
const kindOf = (t: McpTool): Kind =>
  t.annotations?.destructiveHint ? 'destructive' : t.annotations?.readOnlyHint ? 'read' : 'write';
const KIND_TONE: Record<Kind, string> = { read: 'idle', write: 'warn', destructive: 'bad' };

function CopyButton(props: { value: string; label?: string }) {
  const [done, setDone] = createSignal(false);
  return (
    <button
      class="btn btn-sm"
      onClick={() => {
        void navigator.clipboard?.writeText(props.value);
        setDone(true);
        setTimeout(() => setDone(false), 1400);
      }}
    >
      {done() ? 'Copied' : (props.label ?? 'Copy')}
    </button>
  );
}

/** The ways to plug a token into an agent, one at a time behind tabs. */
function Clients(props: { snippets: { title: string; file?: string; code: string }[] }) {
  const [tab, setTab] = createSignal(0);
  const cur = () => props.snippets[tab()];
  return (
    <div class="ax-clients">
      <div class="ax-tabs">
        <div class="fx-range" role="tablist">
          <For each={props.snippets}>
            {(sn, i) => (
              <button role="tab" aria-selected={tab() === i()} classList={{ on: tab() === i() }} onClick={() => setTab(i())}>
                {sn.title}
              </button>
            )}
          </For>
        </div>
        <Show when={cur().file}><span class="cap">{cur().file}</span></Show>
        <span style={{ 'margin-left': 'auto' }}><CopyButton value={cur().code} /></span>
      </div>
      <pre class="vw-pre ax-pre">{cur().code}</pre>
    </div>
  );
}

export function Access() {
  const [me] = createResource(() => api.getResult<MeResponse>('/me'));
  const [tools, { refetch: refetchTools }] = createResource(async () => {
    try {
      return { ok: true as const, tools: await listTools() };
    } catch (err) {
      return {
        ok: false as const,
        message: err instanceof Error ? err.message : 'tools/list failed',
      };
    }
  });

  const [label, setLabel] = createSignal('');
  const [scope, setScope] = createSignal<SessionScope>('read');
  const [ttl, setTtl] = createSignal<number>(90);
  const [busy, setBusy] = createSignal(false);
  const [minted, setMinted] = createSignal<TokenResponse | null>(null);
  const [revokeValue, setRevokeValue] = createSignal('');
  const [filters, setFilters] = createSignal<FilterValue>({});
  const [search, setSearch] = createSignal('');

  const config = () => currentMode().config;
  const persistent = () => config()?.sessions_persistent !== false;
  const canMint = () => {
    const m = me();
    if (!m || !m.ok) return true;
    return m.value.mode !== 'mcp' && (m.value.scope ?? 'full') === 'full';
  };

  const mcpUrl = (t: TokenResponse) => `${endpointBase()}${t.endpoint}`;

  const claudeCmd = (t: TokenResponse) =>
    `claude mcp add --transport http ${serverName()} ${mcpUrl(t)} \\\n  --header "Authorization: Bearer ${t.token}" --scope user`;

  const cursorJson = (t: TokenResponse) =>
    JSON.stringify(
      {
        mcpServers: {
          [serverName()]: {
            url: mcpUrl(t),
            headers: { Authorization: `Bearer ${t.token}` },
          },
        },
      },
      null,
      2,
    );

  const vscodeJson = (t: TokenResponse) =>
    JSON.stringify(
      {
        servers: {
          [serverName()]: {
            type: 'http',
            url: mcpUrl(t),
            headers: { Authorization: `Bearer ${t.token}` },
          },
        },
      },
      null,
      2,
    );

  const mint = async (e: Event) => {
    e.preventDefault();
    if (busy()) return;
    setBusy(true);
    try {
      const t = await api.post<TokenResponse>('/tokens', {
        label: label().trim() || 'mcp',
        scope: scope(),
        ttl_days: ttl(),
      });
      setMinted(t);
      toast('ok', 'Token created', 'Copy it now; it is not shown again.');
    } catch (err) {
      toast('bad', 'Could not create token', err instanceof Error ? err.message : undefined);
    } finally {
      setBusy(false);
    }
  };

  const revoke = async (e: Event) => {
    e.preventDefault();
    const value = revokeValue().trim();
    if (!value || busy()) return;
    setBusy(true);
    try {
      await api.delete('/tokens', { token: value });
      setRevokeValue('');
      toast('ok', 'Token revoked');
    } catch (err) {
      toast('bad', 'Could not revoke', err instanceof Error ? err.message : undefined);
    } finally {
      setBusy(false);
    }
  };

  const allTools = () => {
    const t = tools();
    return t && t.ok ? t.tools : [];
  };
  const kindCount = (k: Kind) => allTools().filter((t) => kindOf(t) === k).length;
  const grouped = createMemo(() => {
    const kinds = filters().kind ?? [];
    const areas = filters().area ?? [];
    const q = search().trim().toLowerCase();
    const map = new Map<string, McpTool[]>();
    for (const tool of allTools()) {
      const g = groupOf(tool.name);
      if (kinds.length && !kinds.includes(kindOf(tool))) continue;
      if (areas.length && !areas.includes(g)) continue;
      if (q && !`${tool.name} ${tool.description}`.toLowerCase().includes(q)) continue;
      map.set(g, [...(map.get(g) ?? []), tool]);
    }
    return GROUP_ORDER.filter((g) => map.has(g)).map((g) => ({ group: g, tools: map.get(g)! }));
  });
  const shown = () => grouped().reduce((n, g) => n + g.tools.length, 0);

  const meValue = () => {
    const m = me();
    return m && m.ok ? m.value : undefined;
  };

  const endpoint = () => `${endpointBase()}/admin/api/mcp`;

  return (
    <>
      <PageHead
        title="Access"
        actions={
          <Show when={meValue()}>
            {(m) => (
              <span class="cap fx-state">
                <Sq tone={m().mode === 'breakglass' ? 'warn' : ''} />
                {m().label} · {m().scope ?? 'full'}
                <Show when={m().mode !== 'roster'}> · {m().mode}</Show>
              </span>
            )}
          </Show>
        }
      />

      <div class="fx-page">
        <Show when={!persistent()}>
          <section class="fx-row">
            <FxCell>
              <div class="fx-log"><span class="fx-what">SPKY_AUTH_SECRET is not set · tokens die with this scheduler process</span></div>
            </FxCell>
          </section>
        </Show>

        {/* where agents connect, and what they can do =========================== */}
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) repeat(3, minmax(0, 1fr))' }}>
          <FxCell label="MCP endpoint" meta={<span>{serverName()}</span>}>
            <div class="ax-endpoint">
              <span class="ax-url">{endpoint()}</span>
              <CopyButton value={endpoint()} />
            </div>
            <div class="fx-sub cap"><span>any admin session token works as the bearer</span></div>
          </FxCell>
          <FxCell label="Read">
            <div class="fx-big">{allTools().length ? kindCount('read') : '—'}<small>tools</small></div>
            <div class="fx-sub cap"><span>open to read-only tokens</span></div>
          </FxCell>
          <FxCell label="Write">
            <div class="fx-big"><span classList={{ warn: kindCount('write') > 0 }}>{allTools().length ? kindCount('write') : '—'}</span><small>tools</small></div>
            <div class="fx-sub cap"><span>retries, triggers, pauses</span></div>
          </FxCell>
          <FxCell label="Destructive">
            <div class="fx-big"><span classList={{ bad: kindCount('destructive') > 0 }}>{allTools().length ? kindCount('destructive') : '—'}</span><small>tools</small></div>
            <div class="fx-sub cap"><span>restarts, restores, deletes</span></div>
          </FxCell>
        </section>

        {/* make a token, or take one away ======================================= */}
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
          <FxCell label="New token" meta={<span>for an agent to call this endpoint</span>}>
            <Show when={canMint()} fallback={<div class="fx-empty cap">Only a full, interactive sign-in can create tokens</div>}>
              <form class="ax-form" onSubmit={mint}>
                <label class="li-field ax-label">
                  <span>Label</span>
                  <input placeholder="claude-code on my laptop" value={label()} onInput={(e) => setLabel(e.currentTarget.value)} />
                </label>
                <div class="ax-scopes" role="radiogroup" aria-label="Scope">
                  <button type="button" role="radio" aria-checked={scope() === 'read'} class="ax-scope" classList={{ on: scope() === 'read' }} onClick={() => setScope('read')}>
                    <span class="fx-who"><span class="fm-check radio" classList={{ on: scope() === 'read' }} /><b>Read-only</b></span>
                    <span class="cap">{allTools().length ? `${kindCount('read')} tools` : ''} · every write is refused</span>
                  </button>
                  <button type="button" role="radio" aria-checked={scope() === 'full'} class="ax-scope" classList={{ on: scope() === 'full' }} onClick={() => setScope('full')}>
                    <span class="fx-who"><span class="fm-check radio" classList={{ on: scope() === 'full' }} /><b>Full</b></span>
                    <span class="cap">all {allTools().length || ''} tools · restarts, retries, restores</span>
                  </button>
                </div>
                <div class="ax-foot">
                  <span class="cap">Expires in</span>
                  <div class="fx-range" role="group" aria-label="Expires in">
                    <For each={LIFETIMES}>
                      {(d) => (
                        <button type="button" classList={{ on: ttl() === d }} aria-pressed={ttl() === d} onClick={() => setTtl(d)}>{d}d</button>
                      )}
                    </For>
                  </div>
                  <button class="btn btn-primary" type="submit" disabled={busy()} style={{ 'margin-left': 'auto' }}>
                    Create token
                  </button>
                </div>
              </form>
            </Show>
          </FxCell>
          <FxCell label="Revoke" meta={<span>{persistent() ? 'a restart forgets it until expiry' : 'immediate'}</span>}>
            <form class="ax-revoke" onSubmit={revoke}>
              <input aria-label="Token to revoke" placeholder="Paste the token" value={revokeValue()} onInput={(e) => setRevokeValue(e.currentTarget.value)} />
              <button class="btn btn-sm" type="submit" disabled={busy() || !revokeValue().trim()}>Revoke</button>
            </form>
            <div class="fx-sub cap" style={{ 'margin-top': '12px' }}>
              <span>tokens are signed, not stored · the token itself is the only handle on it</span>
            </div>
          </FxCell>
        </section>

        {/* the token just made, once ============================================ */}
        <Show when={minted()}>
          {(t) => (
            <section class="fx-row ax-minted">
              <FxCell label="Your token" meta={<span class="warn">shown once · copy it now</span>}>
                <div class="ax-token">
                  <span class="ax-token-v">{t().token}</span>
                  <CopyButton value={t().token} />
                </div>
                <div class="fx-sub cap">
                  <span><b>{t().label}</b></span>
                  <span><b>{t().scope}</b> scope</span>
                  <span>expires <b>{formatStamp(t().expires_at)}</b></span>
                </div>
                <Clients
                  snippets={[
                    { title: 'Claude Code', code: claudeCmd(t()) },
                    { title: 'Cursor', file: '~/.cursor/mcp.json', code: cursorJson(t()) },
                    { title: 'VS Code', file: '.vscode/mcp.json', code: vscodeJson(t()) },
                  ]}
                />
              </FxCell>
            </section>
          )}
        </Show>

        {/* every tool =========================================================== */}
        <section class="fx-row">
          <FxCell label="Tools" meta={<span><b>{shown()}</b>{shown() !== allTools().length ? ` of ${allTools().length}` : ''}</span>}>
            <div class="inc-filters">
              <FilterMenu
                value={filters()}
                onChange={setFilters}
                fields={[
                  {
                    key: 'kind',
                    label: 'Kind',
                    options: (['read', 'write', 'destructive'] as Kind[]).map((k) => ({ value: k, label: k, count: kindCount(k), tone: KIND_TONE[k] === 'idle' ? '' : KIND_TONE[k] })),
                  },
                  {
                    key: 'area',
                    label: 'Area',
                    options: GROUP_ORDER.map((g) => ({ value: g, label: g, count: allTools().filter((t) => groupOf(t.name) === g).length })).filter((o) => o.count),
                  },
                ]}
              />
              <div class="inc-find">
                <input type="search" placeholder="Search tools" value={search()} onInput={(e) => setSearch(e.currentTarget.value)} aria-label="Search tools" />
                <button class="btn btn-sm" onClick={() => void refetchTools()}>Refresh</button>
              </div>
            </div>
            <Show when={tools()} fallback={<div class="fx-empty cap">Loading…</div>}>
              {(t) => (
                <Show when={t().ok} fallback={<div class="fx-log bad"><span class="fx-what">{(t() as { ok: false; message: string }).message}</span></div>}>
                  <Show when={shown()} fallback={<div class="fx-empty cap">No tool matches</div>}>
                    <table class="fx-list ax-tools">
                      <colgroup><col style={{ width: '28%' }} /><col /><col style={{ width: '14%' }} /></colgroup>
                      <For each={grouped()}>
                        {(g) => (
                          <tbody>
                            <tr class="fx-gh"><th class="cap" colSpan={3}><span class="fx-gt">{g.group}</span><span>{g.tools.length}</span></th></tr>
                            <For each={g.tools}>
                              {(tool) => (
                                <tr>
                                  <td style={{ color: 'var(--ink)' }}>{tool.name}</td>
                                  <td class="inc-sum">{tool.description}</td>
                                  <td class="r">
                                    <span class="fx-state cap" classList={{ warn: kindOf(tool) === 'write', bad: kindOf(tool) === 'destructive' }}>
                                      <Sq tone={KIND_TONE[kindOf(tool)]} />
                                      {kindOf(tool)}
                                    </span>
                                  </td>
                                </tr>
                              )}
                            </For>
                          </tbody>
                        )}
                      </For>
                    </table>
                  </Show>
                </Show>
              )}
            </Show>
          </FxCell>
        </section>
      </div>
    </>
  );
}
