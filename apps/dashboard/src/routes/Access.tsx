import { For, Show, createResource, createSignal } from 'solid-js';
import { api, currentMode } from '../api/client';
import { Empty, PageHead, Panel, SkeletonList } from '../components/Chrome';
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

/** Group a tool by its name prefix, so the list reads by area. */
function groupOf(name: string): string {
  if (/^(workflow_|schedule_|schedules_|job_)/.test(name)) return 'Workflows';
  // Before the `ssp` rule: `ssp` alone would otherwise swallow nothing here,
  // but the presence tools have no shared prefix of their own, so they are
  // matched by name.
  if (/^(presence|views_|view_)/.test(name)) return 'Views';
  if (/^(ssp|scheduler_|cloud_)/.test(name)) return 'Restart';
  if (/^backup/.test(name)) return 'Backups';
  return 'Cluster';
}

const GROUP_ORDER = ['Cluster', 'Views', 'Workflows', 'Restart', 'Backups'];

function toolTone(t: McpTool): { label: string; tone: string } {
  if (t.annotations?.destructiveHint) return { label: 'destructive', tone: 'bad' };
  if (t.annotations?.readOnlyHint) return { label: 'read', tone: 'idle' };
  return { label: 'write', tone: 'warn' };
}

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

function Snippet(props: { title: string; file?: string; code: string }) {
  return (
    <div class="snippet">
      <div class="snippet-head">
        <span class="ac-snippet-title">
          {props.title}
          <Show when={props.file}>
            {' '}
            <span class="mono ac-snippet-file">{props.file}</span>
          </Show>
        </span>
        <CopyButton value={props.code} />
      </div>
      <pre>{props.code}</pre>
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

  const grouped = () => {
    const t = tools();
    if (!t || !t.ok) return [];
    const map = new Map<string, McpTool[]>();
    for (const tool of t.tools) {
      const g = groupOf(tool.name);
      map.set(g, [...(map.get(g) ?? []), tool]);
    }
    return GROUP_ORDER.filter((g) => map.has(g)).map((g) => ({
      group: g,
      tools: map.get(g)!,
    }));
  };

  const meValue = () => {
    const m = me();
    return m && m.ok ? m.value : undefined;
  };

  return (
    <>
      <PageHead
        title="Access"
        subtitle={
          <Show when={meValue()}>
            {(m) => (
              <>
                Signed in as {m().label}, {m().scope ?? 'full'} access
                <Show when={m().mode !== 'roster'}> ({m().mode})</Show>
              </>
            )}
          </Show>
        }
      />

      <div class="page-body page-stack">
        <Show when={!persistent()}>
          <div class="banner">
            <span class="dot warn" />
            <span>
              <span class="mono">SPKY_AUTH_SECRET</span> is not set, so tokens die with this
              scheduler process.
            </span>
          </div>
        </Show>

        <div class="ac-grid">
          <Panel title="New MCP token" sub="For an AI agent to call this scheduler's MCP endpoint.">
            <Show
              when={canMint()}
              fallback={<Empty>Only a full, interactive sign-in can create tokens.</Empty>}
            >
              <form class="ac-form" onSubmit={mint}>
                <div class="ac-row">
                  <div class="ac-grow">
                    <label for="ac-label">Label</label>
                    <input
                      id="ac-label"
                      placeholder="claude-code on my laptop"
                      value={label()}
                      onInput={(e) => setLabel(e.currentTarget.value)}
                    />
                  </div>
                  <div class="ac-ttl">
                    <label for="ac-ttl">Expires in</label>
                    <select
                      id="ac-ttl"
                      value={String(ttl())}
                      onChange={(e) => setTtl(Number(e.currentTarget.value))}
                    >
                      <For each={LIFETIMES}>
                        {(d) => <option value={String(d)}>{d} days</option>}
                      </For>
                    </select>
                  </div>
                </div>

                <div role="radiogroup" aria-label="Scope" class="ac-scopes">
                  <label class="ac-scope" classList={{ on: scope() === 'read' }}>
                    <input
                      type="radio"
                      name="scope"
                      checked={scope() === 'read'}
                      onChange={() => setScope('read')}
                    />
                    <span class="ac-scope-title">Read-only</span>
                    <span class="ac-scope-sub">Every write is refused.</span>
                  </label>
                  <label class="ac-scope" classList={{ on: scope() === 'full' }}>
                    <input
                      type="radio"
                      name="scope"
                      checked={scope() === 'full'}
                      onChange={() => setScope('full')}
                    />
                    <span class="ac-scope-title">Full</span>
                    <span class="ac-scope-sub">Restarts, retries, backups and restores.</span>
                  </label>
                </div>

                <div>
                  <button class="btn btn-primary" type="submit" disabled={busy()}>
                    Create token
                  </button>
                </div>
              </form>
            </Show>

            <Show when={minted()}>
              {(t) => (
                <div class="ac-minted">
                  <div class="ac-minted-head">
                    <span class="dot warn" />
                    Shown once, copy it now.
                    <span class="ac-minted-meta">
                      {t().label}, {t().scope}, expires {formatStamp(t().expires_at)}
                    </span>
                  </div>
                  <div class="token-box">
                    <span class="token-value">{t().token}</span>
                    <CopyButton value={t().token} />
                  </div>
                  <Snippet title="Claude Code" code={claudeCmd(t())} />
                  <Snippet title="Cursor" file="~/.cursor/mcp.json" code={cursorJson(t())} />
                  <Snippet title="VS Code" file=".vscode/mcp.json" code={vscodeJson(t())} />
                </div>
              )}
            </Show>
          </Panel>

          <div class="page-stack">
            <Panel title="Endpoint" sub="Any admin session token works as the bearer.">
              <div class="ac-endpoint">
                <span class="token-value mono">{endpointBase()}/admin/api/mcp</span>
                <CopyButton value={`${endpointBase()}/admin/api/mcp`} />
              </div>
            </Panel>

            <Panel
              title="Revoke a token"
              sub={persistent() ? 'Immediate; a restart forgets it until the token expires.' : 'Immediate.'}
            >
              <form class="ac-revoke" onSubmit={revoke}>
                <input
                  aria-label="Token to revoke"
                  placeholder="Paste the token"
                  value={revokeValue()}
                  onInput={(e) => setRevokeValue(e.currentTarget.value)}
                />
                <button class="btn" type="submit" disabled={busy() || !revokeValue().trim()}>
                  Revoke
                </button>
              </form>
            </Panel>
          </div>
        </div>

        <Panel
          flush
          title="Tools"
          sub="What an agent can call here."
          actions={
            <button class="btn btn-sm" onClick={() => void refetchTools()}>
              Refresh
            </button>
          }
        >
          <Show when={tools()} fallback={<SkeletonList rows={5} />}>
            {(t) => (
              <Show
                when={t().ok}
                fallback={<Empty>{(t() as { ok: false; message: string }).message}</Empty>}
              >
                <For each={grouped()}>
                  {(g) => (
                    <div class="ac-group">
                      <div class="ac-group-name">{g.group}</div>
                      <For each={g.tools}>
                        {(tool) => (
                          <div class="ac-tool">
                            <span class="ac-tool-name mono">{tool.name}</span>
                            <span class="ac-tool-desc">{tool.description}</span>
                            <span class={`ac-kind ${toolTone(tool).tone}`}>{toolTone(tool).label}</span>
                          </div>
                        )}
                      </For>
                    </div>
                  )}
                </For>
              </Show>
            )}
          </Show>
        </Panel>
      </div>
    </>
  );
}
