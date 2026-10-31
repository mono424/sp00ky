/**
 * A workspace's own pages in the console: its projects, members, billing,
 * vault, API keys, activity, and the signed-in person's sessions. All of it
 * is Sp00ky Cloud's; no project's scheduler is involved.
 */

import { For, Show, createResource, createSignal, type JSX } from 'solid-js';
import { PageHead } from '../components/Chrome';
import { FxCell, Sq } from '../components/Frame';
import { confirm, toast } from '../components/Actions';
import { formatStamp } from '../lib/format';
import { cloud, consoleApi, type Workspace } from './cloud';
import { projectLine, projectTone, useConsoleChrome } from './chrome';
import { derivedKey, isUnlocked, lock, unlock, vaultStatus } from './vault';

type Props = { workspace: Workspace };

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

async function act(label: string, run: () => Promise<unknown>, done?: string): Promise<boolean> {
  try {
    await run();
    toast('ok', done ?? `${label} done`);
    return true;
  } catch (err) {
    toast('bad', `${label} failed`, message(err));
    return false;
  }
}

/** The workspace's page head: the workspace name where a project would be. */
function Head(props: { title: string; actions?: JSX.Element }) {
  return <PageHead title={props.title} actions={props.actions} />;
}

/* ---------- projects ---------- */

export function Projects(props: Props) {
  const cx = useConsoleChrome();
  return (
    <>
      <Head title="Projects" />
      <div class="fx-page">
        <section class="fx-row cx-cards">
          <For each={props.workspace.projects} fallback={<FxCell><p class="cx-note">No projects in {props.workspace.name} yet. Create one with `spky deploy`.</p></FxCell>}>
            {(p) => (
              <button class="fx-cell cx-card" onClick={() => cx?.go(`/${props.workspace.slug}/${p.slug}`)}>
                <div class="cx-card-top">
                  <span class={`fx-sq ${projectTone(p)}`} />
                  <b>{p.slug}</b>
                  <span class="cx-tag" classList={{ cf: p.platform === 'cloudflare' }}>{p.platform === 'cloudflare' ? 'CF' : 'VM'}</span>
                </div>
                <span class="cap">{projectLine(p)} · {p.plan}</span>
              </button>
            )}
          </For>
        </section>
      </div>
    </>
  );
}

/* ---------- members ---------- */

interface Member {
  id: string;
  account_id: string;
  role: string;
  email?: string;
  github_login?: string;
  created_at: string;
}

interface Invitation {
  id: string;
  email: string;
  role: string;
  expires_at: string;
}

export function Members(props: Props) {
  const tid = () => encodeURIComponent(props.workspace.id);
  const isAdmin = () => props.workspace.role === 'admin';
  const [members, { refetch }] = createResource(() => cloud<Member[]>(`/tenants/${tid()}/members`));
  const [invites, { refetch: refetchInvites }] = createResource(
    () => isAdmin(),
    () => cloud<Invitation[]>(`/tenants/${tid()}/invitations`),
  );
  const [email, setEmail] = createSignal('');
  const [role, setRole] = createSignal<'member' | 'admin'>('member');

  const invite = async (e: Event) => {
    e.preventDefault();
    // With the vault unlocked here, the invitee gets the vault too.
    const key = derivedKey(props.workspace.id);
    const body: Record<string, string> = { email: email().trim(), role: role() };
    if (key) body.derived_key = key;
    if (await act('Invitation', () => cloud(`/tenants/${tid()}/invitations`, { method: 'POST', body }), `Invited ${body.email}`)) {
      setEmail('');
      void refetchInvites();
    }
  };
  const remove = async (m: Member) => {
    const who = m.github_login ?? m.email ?? 'this member';
    const ok = await confirm({ title: `Remove ${who}`, consequences: [`${who} loses access to every project in ${props.workspace.name}.`, 'Console sessions they hold on a project end within 15 minutes.'], verb: 'Remove' });
    if (ok && (await act('Removing ' + who, () => cloud(`/tenants/${tid()}/members/${encodeURIComponent(m.id)}`, { method: 'DELETE' })))) void refetch();
  };
  const revoke = async (i: Invitation) => {
    if (await act('Revoking', () => cloud(`/tenants/${tid()}/invitations/${encodeURIComponent(i.id)}`, { method: 'DELETE' }), 'Invitation revoked')) void refetchInvites();
  };

  return (
    <>
      <Head title="Members" />
      <div class="fx-page">
        <section class="fx-row">
          <FxCell label="Members" meta={<span>{members()?.length ?? 0}</span>}>
            <table class="fx-list">
              <thead><tr><th class="cap">Who</th><th class="cap">Role</th><th class="cap">Since</th><th class="cap" /></tr></thead>
              <tbody>
                <For each={members() ?? []} fallback={<tr><td colspan="4" class="dim">{members.loading ? 'Loading' : members.error ? message(members.error) : 'Nobody yet'}</td></tr>}>
                  {(m) => (
                    <tr>
                      <td><b>{m.github_login ?? m.email ?? m.account_id}</b> <span class="dim">{m.github_login ? m.email ?? '' : ''}</span></td>
                      <td><span class={`cx-chip ${m.role === 'admin' ? 'live' : 'dim'}`}>{m.role}</span></td>
                      <td>{formatStamp(m.created_at)}</td>
                      <td class="r"><Show when={isAdmin()}><button class="btn btn-sm" onClick={() => void remove(m)}>Remove</button></Show></td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
        </section>
        <Show when={isAdmin()}>
          <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 1fr)' }}>
            <FxCell label="Invite" meta={<span>{isUnlocked(props.workspace.id) ? 'shares the vault' : 'unlock the vault to share it'}</span>}>
              <form class="cx-form" onSubmit={invite}>
                <label class="li-field"><span>Email</span><input type="email" value={email()} onInput={(e) => setEmail(e.currentTarget.value)} placeholder="someone@example.com" /></label>
                <div class="fx-range" role="radiogroup">
                  <button type="button" classList={{ on: role() === 'member' }} onClick={() => setRole('member')}>Member</button>
                  <button type="button" classList={{ on: role() === 'admin' }} onClick={() => setRole('admin')}>Admin</button>
                </div>
                <button class="btn btn-primary" type="submit" disabled={!email().includes('@')}>Invite</button>
              </form>
            </FxCell>
            <FxCell label="Pending invitations" meta={<span>{invites()?.length ?? 0}</span>}>
              <For each={invites() ?? []} fallback={<div class="fx-empty cap">None pending</div>}>
                {(i) => (
                  <div class="cx-line">
                    <b>{i.email}</b><span class="cap dim">{i.role} · expires {formatStamp(i.expires_at)}</span>
                    <button class="btn btn-sm" onClick={() => void revoke(i)}>Revoke</button>
                  </div>
                )}
              </For>
            </FxCell>
          </section>
        </Show>
      </div>
    </>
  );
}

/* ---------- billing ---------- */

interface UsageRow {
  project: string;
  metric: string;
  total: number;
}

export function Billing(props: Props) {
  const tenant = () => props.workspace.id;
  const [usage] = createResource(() => cloud<UsageRow[]>('/billing/usage', { tenant: tenant() }));
  const portal = async () => {
    try {
      const r = await cloud<{ url: string }>('/billing/portal', { method: 'POST', tenant: tenant() });
      window.open(r.url, '_blank', 'noopener');
    } catch (err) {
      toast('bad', 'Billing portal', message(err));
    }
  };
  const byProject = () => {
    const out = new Map<string, UsageRow[]>();
    for (const r of usage() ?? []) out.set(r.project, [...(out.get(r.project) ?? []), r]);
    return [...out.entries()];
  };
  return (
    <>
      <Head title="Billing & usage" actions={<button class="btn btn-sm btn-primary" onClick={() => void portal()}>Invoices and payment</button>} />
      <div class="fx-page">
        <section class="fx-row">
          <FxCell label="Usage this period" meta={<span>per project</span>}>
            <table class="fx-list">
              <thead><tr><th class="cap">Project</th><th class="cap">Metric</th><th class="cap r">Total</th></tr></thead>
              <tbody>
                <For each={byProject()} fallback={<tr><td colspan="3" class="dim">{usage.loading ? 'Loading' : usage.error ? message(usage.error) : 'No usage recorded yet'}</td></tr>}>
                  {([project, rows]) => (
                    <For each={rows}>
                      {(r, i) => (
                        <tr>
                          <td>{i() === 0 ? <b>{project}</b> : ''}</td>
                          <td>{r.metric.replace(/_/g, ' ')}</td>
                          <td class="r">{Number.isInteger(r.total) ? r.total : r.total.toFixed(2)}</td>
                        </tr>
                      )}
                    </For>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
        </section>
        <section class="fx-row">
          <FxCell><p class="cx-note">Plan changes and spend caps: `spky cloud billing` for now; invoices and payment methods open in Stripe above.</p></FxCell>
        </section>
      </div>
    </>
  );
}

/* ---------- vault ---------- */

export function Vault(props: Props) {
  const [status] = createResource(() => vaultStatus(props.workspace.id));
  const [pass, setPass] = createSignal('');
  const [busy, setBusy] = createSignal(false);
  const firstProject = () => props.workspace.projects[0]?.id ?? null;
  const doUnlock = async (e: Event) => {
    e.preventDefault();
    setBusy(true);
    try {
      await unlock(props.workspace.id, firstProject(), pass());
      setPass('');
      toast('ok', 'Vault unlocked', 'It locks itself after five idle minutes.');
    } catch (err) {
      toast('bad', 'Could not unlock', message(err));
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      <Head title="Vault" />
      <div class="fx-page">
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 2fr)' }}>
          <FxCell label="State">
            <div class="cx-row">
              <Sq tone={status()?.initialized ? (isUnlocked(props.workspace.id) ? 'ok' : 'idle') : 'warn'} />
              <span class="fx-big cx-mid">{!status() ? '…' : !status()!.initialized ? 'not set up' : isUnlocked(props.workspace.id) ? 'unlocked here' : 'locked'}</span>
            </div>
          </FxCell>
          <FxCell label="Unlock in this tab">
            <Show when={status()?.initialized} fallback={<p class="cx-note">Set the vault up once with `spky cloud vault init`; every env value is encrypted with it.</p>}>
              <Show when={!isUnlocked(props.workspace.id)} fallback={<div class="cx-row"><span class="cx-note">Unlocked. Env values can be revealed and invitations share the vault.</span><button class="btn btn-sm" onClick={lock}>Lock now</button></div>}>
                <form class="cx-form" onSubmit={doUnlock}>
                  <label class="li-field"><span>Passphrase</span><input type="password" value={pass()} onInput={(e) => setPass(e.currentTarget.value)} autocomplete="current-password" /></label>
                  <button class="btn btn-primary" type="submit" disabled={busy() || !pass()}>{busy() ? 'Unlocking' : 'Unlock'}</button>
                </form>
                <p class="cx-note">The passphrase never leaves this page: it is stretched here (argon2id) and only the result is sent.</p>
              </Show>
            </Show>
          </FxCell>
        </section>
      </div>
    </>
  );
}

/* ---------- API keys and MCP tokens ---------- */

interface ApiKey {
  id: string;
  prefix: string;
  scopes: string[] | null;
  created_at: string;
  expires_at?: string;
}

interface McpToken {
  id: string;
  name: string;
  prefix: string;
  scopes: string[] | null;
  created_at: string;
  last_used_at?: string;
}

export function Keys() {
  const [keys, { refetch }] = createResource(() => cloud<ApiKey[] | null>('/auth/keys').then((r) => r ?? []));
  const [tokens, { refetch: refetchTokens }] = createResource(() => cloud<McpToken[]>('/mcp/tokens'));
  const [minted, setMinted] = createSignal<string | null>(null);
  const [name, setName] = createSignal('');

  const createKey = async () => {
    try {
      const r = await cloud<{ key: string }>('/auth/keys', { method: 'POST', body: {} });
      setMinted(r.key);
      void refetch();
    } catch (err) {
      toast('bad', 'API key', message(err));
    }
  };
  const createToken = async (e: Event) => {
    e.preventDefault();
    try {
      const r = await cloud<{ key: string }>('/mcp/tokens', { method: 'POST', body: { name: name().trim() } });
      setMinted(r.key);
      setName('');
      void refetchTokens();
    } catch (err) {
      toast('bad', 'MCP token', message(err));
    }
  };
  const revoke = async (kind: 'key' | 'token', id: string, label: string) => {
    const ok = await confirm({ title: `Revoke ${label}`, consequences: ['Anything using it stops working at once.'], verb: 'Revoke' });
    if (!ok) return;
    const path = kind === 'key' ? `/auth/keys/${encodeURIComponent(id)}` : `/mcp/tokens/${encodeURIComponent(id)}`;
    if (await act('Revoking', () => cloud(path, { method: 'DELETE' }), `${label} revoked`)) {
      if (kind === 'key') void refetch();
      else void refetchTokens();
    }
  };

  return (
    <>
      <Head title="API keys & MCP tokens" />
      <div class="fx-page">
        <Show when={minted()}>
          <section class="fx-row">
            <FxCell label="Copy it now" meta={<button class="btn btn-sm" onClick={() => setMinted(null)}>Done</button>}>
              <div class="cx-secret"><code>{minted()}</code><button class="btn btn-sm" onClick={() => void navigator.clipboard.writeText(minted()!)}>Copy</button></div>
              <p class="cx-note">Shown once; Sp00ky Cloud keeps only a hash.</p>
            </FxCell>
          </section>
        </Show>
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 1fr)' }}>
          <FxCell label="API keys" meta={<button class="btn btn-sm btn-primary" onClick={() => void createKey()}>New key</button>}>
            <For each={keys() ?? []} fallback={<div class="fx-empty cap">{keys.loading ? 'Loading' : 'No API keys'}</div>}>
              {(k) => (
                <div class="cx-line">
                  <b class="cx-sha">{k.prefix}…</b><span class="cap dim">created {formatStamp(k.created_at)}</span>
                  <button class="btn btn-sm" onClick={() => void revoke('key', k.id, `${k.prefix}…`)}>Revoke</button>
                </div>
              )}
            </For>
            <p class="cx-note">For CI and scripts (`SPOOKY_API_KEY`). A key acts as you in every workspace you belong to.</p>
          </FxCell>
          <FxCell label="MCP tokens" meta={<span>{tokens()?.length ?? 0}</span>}>
            <form class="cx-form" onSubmit={createToken}>
              <label class="li-field"><span>Name</span><input value={name()} onInput={(e) => setName(e.currentTarget.value)} placeholder="claude on my laptop" /></label>
              <button class="btn btn-primary" type="submit" disabled={!name().trim()}>New token</button>
            </form>
            <For each={tokens() ?? []}>
              {(t) => (
                <div class="cx-line">
                  <b>{t.name}</b><span class="cap dim">{t.prefix}… · {t.last_used_at ? `used ${formatStamp(t.last_used_at)}` : 'never used'}</span>
                  <button class="btn btn-sm" onClick={() => void revoke('token', t.id, t.name)}>Revoke</button>
                </div>
              )}
            </For>
          </FxCell>
        </section>
      </div>
    </>
  );
}

/* ---------- activity ---------- */

const ACTIONS: Record<string, string> = {
  'console.grant': 'opened the project dashboard',
  'console.sign_in': 'signed in',
  'console.sign_out_everywhere': 'signed out everywhere',
};

export function Activity(props: Props) {
  const [events] = createResource(() => consoleApi.activity(props.workspace.id));
  return (
    <>
      <Head title="Activity" />
      <div class="fx-page">
        <section class="fx-row">
          <FxCell label="Recent" meta={<span>{events()?.length ?? 0}</span>}>
            <table class="fx-list">
              <thead><tr><th class="cap">When</th><th class="cap">Who</th><th class="cap">What</th><th class="cap">Project</th><th class="cap">From</th></tr></thead>
              <tbody>
                <For each={events() ?? []} fallback={<tr><td colspan="5" class="dim">{events.loading ? 'Loading' : events.error ? message(events.error) : 'Nothing yet'}</td></tr>}>
                  {(e) => (
                    <tr>
                      <td>{formatStamp(e.at)}</td>
                      <td><b>{e.actor}</b></td>
                      <td>{ACTIONS[e.action] ?? e.action}</td>
                      <td>{e.project ?? '—'}</td>
                      <td class="dim">{e.ip ?? ''}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
        </section>
      </div>
    </>
  );
}

/* ---------- sessions ---------- */

export function Sessions() {
  const [sessions, { refetch }] = createResource(() => consoleApi.sessions());
  const revoke = async (id: string) => {
    if (await act('Signing out', () => consoleApi.revokeSession(id), 'Session ended')) void refetch();
  };
  const others = async () => {
    const ok = await confirm({ title: 'Sign out everywhere else', consequences: ['Every other console session ends now.'], verb: 'Sign out others' });
    if (ok && (await act('Signing out', () => consoleApi.revokeOthers(), 'Other sessions ended'))) void refetch();
  };
  return (
    <>
      <Head title="Sessions" actions={<button class="btn btn-sm" onClick={() => void others()}>Sign out everywhere else</button>} />
      <div class="fx-page">
        <section class="fx-row">
          <FxCell label="Console sessions">
            <table class="fx-list">
              <thead><tr><th class="cap">Browser</th><th class="cap">From</th><th class="cap">Signed in</th><th class="cap">Last seen</th><th class="cap" /></tr></thead>
              <tbody>
                <For each={sessions() ?? []}>
                  {(s) => (
                    <tr>
                      <td><b>{(s.user_agent ?? 'unknown').replace(/\).*$/, ')').slice(0, 60)}</b> <Show when={s.current}><span class="cx-chip live">This one</span></Show></td>
                      <td class="dim">{s.ip ?? ''}</td>
                      <td>{formatStamp(s.created_at)}</td>
                      <td>{formatStamp(s.last_seen_at)}</td>
                      <td class="r"><Show when={!s.current}><button class="btn btn-sm" onClick={() => void revoke(s.id)}>Sign out</button></Show></td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
        </section>
      </div>
    </>
  );
}
