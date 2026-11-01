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
import { Stamp, projectLine, projectTone, useConsoleChrome } from './chrome';
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
            <table class="fx-list cx-members">
              <thead><tr><th class="cap">Who</th><th class="cap">Role</th><th class="cap">Since</th><th class="cap" /></tr></thead>
              <tbody>
                <For each={members() ?? []} fallback={<tr><td colspan="4" class="dim">{members.loading ? 'Loading' : members.error ? message(members.error) : 'Nobody yet'}</td></tr>}>
                  {(m) => (
                    <tr>
                      <td class="cx-who"><b>{m.github_login ?? m.email ?? m.account_id}</b> <span class="dim">{m.github_login ? m.email ?? '' : ''}</span></td>
                      <td><span class={`cx-chip ${m.role === 'admin' ? 'live' : 'dim'}`}>{m.role}</span></td>
                      <td><Stamp at={m.created_at} /></td>
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
                    <span class="cx-line-main"><b>{i.email}</b><span class="cap dim">{i.role} · expires {formatStamp(i.expires_at)}</span></span>
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
                          <td class="r">{r.total.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
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

interface EnvCount {
  name: string;
  has_dev: boolean;
  has_prod: boolean;
}

export function Vault(props: Props) {
  const cx = useConsoleChrome();
  const tenant = () => props.workspace.id;
  const [status] = createResource(() => vaultStatus(tenant()));
  const [ci, { refetch: refetchCi }] = createResource(() =>
    cloud<{ enabled: boolean }>('/vault/ci-access', { tenant: tenant() }).catch(() => null),
  );
  // What the vault holds: every project's variables, names only.
  const [secrets] = createResource(() =>
    Promise.all(
      props.workspace.projects.map(async (p) => ({
        project: p,
        rows: await cloud<EnvCount[]>(`/projects/${encodeURIComponent(p.id)}/envs`, { tenant: tenant() }).catch(() => null),
      })),
    ),
  );
  const [pass, setPass] = createSignal('');
  const [busy, setBusy] = createSignal(false);
  const unlocked = () => isUnlocked(tenant());
  const firstProject = () => props.workspace.projects[0]?.id ?? null;
  const total = () => (secrets() ?? []).reduce((n, s) => n + (s.rows?.length ?? 0), 0);

  const doUnlock = async (e: Event) => {
    e.preventDefault();
    setBusy(true);
    try {
      await unlock(tenant(), firstProject(), pass());
      setPass('');
      toast('ok', 'Vault unlocked', 'It locks itself after five idle minutes.');
    } catch (err) {
      toast('bad', 'Could not unlock', message(err));
    } finally {
      setBusy(false);
    }
  };
  const toggleCi = async () => {
    if (ci()?.enabled) {
      const ok = await confirm({ title: 'Turn off CI access', consequences: ['Push deploys stop receiving production secrets.', 'Deploys from the CLI are not affected.'], verb: 'Turn off' });
      if (ok && (await act('CI access', () => cloud('/vault/ci-access', { method: 'DELETE', tenant: tenant() }), 'CI access off'))) void refetchCi();
      return;
    }
    const key = derivedKey(tenant());
    if (!key) return;
    if (await act('CI access', () => cloud('/vault/ci-access', { method: 'POST', tenant: tenant(), body: { derived_key: key } }), 'Push deploys can read secrets')) void refetchCi();
  };

  return (
    <>
      <Head title="Vault" />
      <div class="fx-page">
        <section class="fx-row cx-vault-top">
          <FxCell label="State" meta={<Show when={status()?.initialized}><span>{total()} secrets</span></Show>}>
            <div class="cx-state">
              <Sq tone={status()?.initialized ? (unlocked() ? 'ok' : 'idle') : 'warn'} />
              <span class="fx-big cx-mid">{!status() ? '…' : !status()!.initialized ? 'not set up' : unlocked() ? 'unlocked' : 'locked'}</span>
            </div>
            <p class="cx-note">
              {!status()?.initialized
                ? 'Set the vault up once with `spky cloud vault init`; every env value is encrypted with it.'
                : unlocked()
                  ? 'Values can be revealed and invitations share the vault. Locks after five idle minutes.'
                  : 'Every env value is encrypted with this workspace key. Unlock to reveal values; setting one never needs it.'}
            </p>
            <Show when={status()?.initialized}>
              <Show
                when={!unlocked()}
                fallback={<div class="cx-actions"><button class="btn btn-sm" onClick={lock}>Lock now</button></div>}
              >
                <form class="cx-unlock" onSubmit={doUnlock}>
                  <input type="password" placeholder="Vault passphrase" value={pass()} onInput={(e) => setPass(e.currentTarget.value)} autocomplete="current-password" />
                  <button class="btn btn-sm btn-primary" type="submit" disabled={busy() || !pass()}>{busy() ? 'Unlocking' : 'Unlock'}</button>
                </form>
                <p class="cx-note">The passphrase never leaves this page: it is stretched here (argon2id) and only the result is sent.</p>
              </Show>
            </Show>
          </FxCell>
          <FxCell label="Push deploys" meta={<Show when={ci()}><span classList={{ acc: !!ci()?.enabled }}>{ci()!.enabled ? 'on' : 'off'}</span></Show>}>
            <div class="cx-state">
              <Sq tone={ci()?.enabled ? 'ok' : 'idle'} />
              <span class="fx-big cx-mid">{ci() === undefined ? '…' : ci() === null ? 'unknown' : ci()!.enabled ? 'can read secrets' : 'no secrets'}</span>
            </div>
            <p class="cx-note">Lets git-linked builds read production values without a passphrase. The CLI never needs this.</p>
            <Show when={ci()}>
              <div class="cx-actions">
                <button class="btn btn-sm" disabled={!ci()!.enabled && !unlocked()} onClick={() => void toggleCi()}>
                  {ci()!.enabled ? 'Turn off' : 'Turn on'}
                </button>
                <Show when={!ci()!.enabled && !unlocked()}><span class="cap dim">unlock first</span></Show>
              </div>
            </Show>
          </FxCell>
        </section>
        <section class="fx-row">
          <FxCell label="Secrets by project" meta={<span>{props.workspace.projects.length} projects</span>}>
            <table class="fx-list cx-secrets">
              <thead><tr><th class="cap">Project</th><th class="cap r">Variables</th><th class="cap r">Development</th><th class="cap r">Production</th><th class="cap" /></tr></thead>
              <tbody>
                <For each={secrets() ?? []} fallback={<tr><td colspan="5" class="dim">{secrets.loading ? 'Loading' : 'No projects yet'}</td></tr>}>
                  {(s) => (
                    <tr>
                      <td><span class={`fx-sq ${projectTone(s.project)}`} /> <b>{s.project.slug}</b></td>
                      <td class="r">{s.rows ? s.rows.length : '—'}</td>
                      <td class="r dim">{s.rows ? s.rows.filter((r) => r.has_dev).length : '—'}</td>
                      <td class="r dim">{s.rows ? s.rows.filter((r) => r.has_prod).length : '—'}</td>
                      <td class="r"><button class="btn btn-sm" onClick={() => cx?.go(`/${props.workspace.slug}/${s.project.slug}/settings`)}>Open</button></td>
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
                  <span class="cx-line-main"><b class="cx-sha">{k.prefix}…</b><span class="cap dim">created {formatStamp(k.created_at)}</span></span>
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
                  <span class="cx-line-main"><b>{t.name}</b><span class="cap dim">{t.prefix}… · {t.last_used_at ? <>used <Stamp at={t.last_used_at} /></> : 'never used'}</span></span>
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

/** A user agent as people say it: `Chrome · macOS`. */
function browser(ua: string | null | undefined): string {
  if (!ua) return 'Unknown browser';
  const name = /Edg\//.test(ua) ? 'Edge' : /Firefox\//.test(ua) ? 'Firefox' : /Chrome\//.test(ua) ? 'Chrome' : /Safari\//.test(ua) ? 'Safari' : /curl|spky/i.test(ua) ? ua.split(' ')[0] : 'Browser';
  const os = /iPhone/.test(ua) ? 'iPhone' : /iPad/.test(ua) ? 'iPad' : /Android/.test(ua) ? 'Android' : /Mac OS X|Macintosh/.test(ua) ? 'macOS' : /Windows/.test(ua) ? 'Windows' : /Linux/.test(ua) ? 'Linux' : '';
  return os ? `${name} · ${os}` : name;
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
            <table class="fx-list cx-activity">
              <thead><tr><th class="cap">When</th><th class="cap">Who</th><th class="cap">What</th><th class="cap">Project</th><th class="cap">From</th></tr></thead>
              <tbody>
                <For each={events() ?? []} fallback={<tr><td colspan="5" class="dim">{events.loading ? 'Loading' : events.error ? message(events.error) : 'Nothing yet'}</td></tr>}>
                  {(e) => (
                    <tr>
                      <td><Stamp at={e.at} /></td>
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
            <table class="fx-list cx-sessions">
              <thead><tr><th class="cap">Browser</th><th class="cap">From</th><th class="cap">Signed in</th><th class="cap">Last seen</th><th class="cap" /></tr></thead>
              <tbody>
                <For each={sessions() ?? []}>
                  {(s) => (
                    <tr>
                      <td class="cx-who"><b title={s.user_agent ?? undefined}>{browser(s.user_agent)}</b> <Show when={s.current}><span class="cx-chip live">This one</span></Show></td>
                      <td class="dim">{s.ip ?? ''}</td>
                      <td><Stamp at={s.created_at} /></td>
                      <td><Stamp at={s.last_seen_at} /></td>
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
