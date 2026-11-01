/**
 * A project's pages that come from Sp00ky Cloud rather than its scheduler:
 * Deployments (what is live, what is rolling out, what came before) and
 * Settings (env, domains, git link, scale, tokens, danger zone). They work
 * whether or not the project's own dashboard could open.
 */

import { For, Match, Show, Switch, createMemo, createResource, createSignal, onCleanup, type JSX } from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { PageHead } from '../components/Chrome';
import { FxCell, Sq } from '../components/Frame';
import { confirm, toast } from '../components/Actions';
import { formatDuration, formatStamp } from '../lib/format';
import { useOverview } from '../ProjectApp';
import { cloud, consoleApi, type DeploymentRow, type PendingBuild, type Workspace, type WorkspaceProject } from './cloud';
import { Stamp, ago, useConsoleChrome } from './chrome';
import { isUnlocked, loadValues, unlock } from './vault';

interface Ctx {
  workspace: Workspace;
  project: WorkspaceProject;
}

const pid = (p: WorkspaceProject) => encodeURIComponent(p.id);

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Run a cloud action with a toast either way; resolves true on success. */
async function act(label: string, run: () => Promise<unknown>, done?: string): Promise<boolean> {
  try {
    await run();
    toast('ok', done ?? `${label} requested`);
    return true;
  } catch (err) {
    toast('bad', `${label} failed`, message(err));
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* Deployments                                                          */
/* ------------------------------------------------------------------ */

const DEPLOY_STEPS = ['pending', 'provisioning', 'migrating', 'deploying_apps', 'running'] as const;
const BUILD_STEPS = ['pending', 'cloning', 'building', 'uploading', 'deploying'] as const;
const IN_FLIGHT = new Set(['pending', 'provisioning', 'migrating', 'deploying_apps']);

function took(d: DeploymentRow): string {
  if (!d.deployed_at) return IN_FLIGHT.has(d.status) ? '…' : '—';
  return formatDuration(Date.parse(d.deployed_at) - Date.parse(d.created_at));
}

function statusChip(d: DeploymentRow): { label: string; tone: string } {
  if (d.live) return { label: d.status === 'degraded' ? 'Live · degraded' : 'Live', tone: 'live' };
  if (IN_FLIGHT.has(d.status)) return { label: d.status.replace('_', ' '), tone: 'run' };
  if (d.status === 'failed') return { label: 'Failed', tone: 'fail' };
  if (d.status === 'superseded') return { label: d.live_from ? 'Replaced' : 'Never live', tone: 'dim' };
  return { label: d.status, tone: 'dim' };
}

function source(d: DeploymentRow): JSX.Element {
  if (d.build) {
    return (
      <>
        git <span class="cx-sha">{d.build.commit_sha.slice(0, 7)}</span>{' '}
        <span class="dim">{d.build.commit_message?.split('\n')[0] ?? ''}</span>
      </>
    );
  }
  switch (d.source) {
    case 'rollback':
      return <>rollback <span class="dim">of v{d.rollback_of ?? '?'}</span></>;
    case 'auto_rollback':
      return <>auto rollback <span class="dim">after a failed deploy</span></>;
    case 'console':
      return <>console</>;
    case 'ci':
      return <>ci <span class="dim">build key</span></>;
    case 'git':
      return <>git</>;
    case 'cli':
      return <>cli <span class="dim">spky deploy</span></>;
    default:
      return <span class="dim">—</span>;
  }
}

/** One line of a deployment's infra versions, e.g. scheduler canary.369. */
function images(d: DeploymentRow | undefined): [string, string][] {
  if (!d?.images || typeof d.images !== 'object') return [];
  return Object.entries(d.images)
    .filter(([, v]) => typeof v === 'string' && v)
    .map(([k, v]) => [k, String(v).replace(/^0\.0\.1-/, '')]);
}

interface CloudDeploymentStatus {
  deployment: { id: string; version: number; status: string } | null;
  vms: { role: string; status: string; name: string }[];
  urls: Record<string, string>;
}

export function Deployments(props: Ctx) {
  const cx = useConsoleChrome();
  const overview = useOverview();
  const [data, { refetch }] = createResource(() => consoleApi.deployments(props.project.id));
  const [status, { refetch: refetchStatus }] = createResource(() =>
    cloud<CloudDeploymentStatus>(`/projects/${pid(props.project)}/deployment`, { tenant: props.workspace.id }).catch(() => null),
  );
  const [link] = createResource(() =>
    cloud<{ status: string }>(`/projects/${pid(props.project)}/link`, { tenant: props.workspace.id }).catch(() => ({ status: 'not_linked' })),
  );

  const list = () => data()?.deployments ?? [];
  const live = createMemo(() => list().find((d) => d.live));
  const rolling = createMemo(() => list().find((d) => IN_FLIGHT.has(d.status)));
  const pendingBuild = createMemo<PendingBuild | undefined>(() => data()?.builds?.[0]);
  const busy = () => !!rolling() || !!pendingBuild();
  /** The deployment `POST /deploy/rollback` would bring back: the newest replaced one. */
  const rollbackTarget = createMemo(() =>
    list()
      .filter((d) => d.status === 'superseded' && !d.live)
      .sort((a, b) => (b.version ?? 0) - (a.version ?? 0))[0],
  );

  // Fast while something is moving, slow at rest.
  let timer: ReturnType<typeof setTimeout>;
  const loop = () => {
    timer = setTimeout(() => {
      void refetch();
      void refetchStatus();
      loop();
    }, busy() ? 3000 : 15000);
  };
  loop();
  onCleanup(() => clearTimeout(timer));

  const tenant = () => props.workspace.id;
  const restart = async (roles: string[] | undefined, upgrade: boolean) => {
    const ok = await confirm({
      title: upgrade ? 'Upgrade and restart' : roles ? `Restart ${roles.join(', ')}` : 'Restart the project',
      consequences: [
        upgrade ? 'Pulls the newest images for the scheduler and SSPs, then restarts them.' : 'Restarts the containers in place.',
        'Blue/green where the scheduler supports it: no downtime expected.',
      ],
      verb: upgrade ? 'Upgrade' : 'Restart',
    });
    if (!ok) return;
    await act('Restart', () =>
      cloud(`/projects/${pid(props.project)}/restart`, { method: 'POST', tenant: tenant(), body: { roles, upgrade } }),
    );
    void refetch();
  };
  const rollback = async () => {
    const t = rollbackTarget();
    if (!t) return;
    const ok = await confirm({
      title: `Roll back to v${t.version}`,
      consequences: [`Deploys v${t.version}'s configuration and images as a new version.`, 'The live deployment keeps serving until the new one is up.'],
      verb: 'Roll back',
    });
    if (!ok) return;
    await act('Rollback', () => cloud(`/projects/${pid(props.project)}/deploy/rollback`, { method: 'POST', tenant: tenant() }));
    void refetch();
  };
  const rebuild = () =>
    void act('Build', () => cloud(`/projects/${pid(props.project)}/link/trigger`, { method: 'POST', tenant: tenant() }), 'Build started').then(
      () => void refetch(),
    );

  const [menu, setMenu] = createSignal(false);

  const vmsByRole = (role: string) => (status()?.vms ?? []).filter((v) => v.role === role);
  const healthy = (role: string) => vmsByRole(role).filter((v) => v.status === 'running').length;

  return (
    <>
      <PageHead
        title="Deployments"
        actions={
          <>
            <span class="cx-menu-wrap">
              <button class="btn btn-sm" onClick={() => setMenu(!menu())}>Manage ▾</button>
              <Show when={menu()}>
                <div class="cx-menu small" role="menu" onClick={() => setMenu(false)}>
                  <Show when={rollbackTarget()}>
                    <button class="cx-mi" onClick={() => void rollback()}>Roll back to v{rollbackTarget()!.version}</button>
                  </Show>
                  <button class="cx-mi" onClick={() => void restart(undefined, false)}>Restart everything</button>
                  <button class="cx-mi" onClick={() => void restart(['scheduler', 'ssp'], false)}>Restart scheduler and SSPs</button>
                  <button class="cx-mi" onClick={() => void restart(['backend', 'frontend'], false)}>Restart apps only</button>
                  <button class="cx-mi" onClick={() => void restart(['scheduler', 'ssp'], true)}>Upgrade to the newest images</button>
                </div>
              </Show>
            </span>
            <Show when={link()?.status === 'linked'}>
              <button class="btn btn-sm btn-primary" disabled={busy()} onClick={rebuild}>Rebuild</button>
            </Show>
          </>
        }
      />
      <div class="fx-page">
        <Show when={data.error}>
          <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{message(data.error)}</span></div></FxCell></section>
        </Show>

        <section class="fx-row cx-top">
          <FxCell
            label="Live deployment"
            meta={<Show when={live()?.live_from}><span title={`since ${formatStamp(live()!.live_from)}`}>up {ago(live()!.live_from).replace(/ ago$/, '')}</span></Show>}
          >
            <Show when={live()} fallback={<div class="fx-empty cap">{data.loading ? 'Loading' : 'Nothing is live yet'}</div>}>
              {(d) => (
                <>
                  <div class="fx-big">v{d().version ?? '?'}<small classList={{ warn: d().status === 'degraded' }}>{d().status === 'degraded' ? 'degraded' : 'serving'}</small></div>
                  <div class="cx-facts cap">
                    <span>rolled out in {took(d())}</span>
                    <Show when={d().actor ?? d().build?.commit_author}><span>by {d().actor ?? d().build?.commit_author}</span></Show>
                  </div>
                  <div class="cx-commit">
                    <span>{source(d())}</span>
                  </div>
                  <div class="cx-kv">
                    <For each={images(d())} fallback={<div><span class="cap">Images</span><b>defaults</b></div>}>
                      {([role, tag]) => (
                        <div><span class="cap">{role}</span><b>{tag}</b></div>
                      )}
                    </For>
                  </div>
                </>
              )}
            </Show>
          </FxCell>
          <FxCell label="Health" meta={<span>{overview() ? 'live' : 'from the control plane'}</span>}>
            <div class="cx-hl">
              <Show
                when={overview()}
                fallback={
                  <For each={['scheduler', 'ssp', 'backend', 'surrealdb']}>
                    {(role) => (
                      <Show when={vmsByRole(role).length}>
                        <div>
                          <Sq tone={healthy(role) === vmsByRole(role).length ? 'ok' : 'warn'} />
                          <span class="cap">{role}</span>
                          <span class="v">{healthy(role)} / {vmsByRole(role).length} running</span>
                        </div>
                      </Show>
                    )}
                  </For>
                }
              >
                {(o) => (
                  <>
                    <div><Sq tone={o().totals.ssps_ready === o().totals.ssps ? 'ok' : 'warn'} /><span class="cap">SSPs</span><span class="v">{o().totals.ssps_ready} / {o().totals.ssps} ready</span></div>
                    <div><Sq tone={o().totals.backends_healthy === o().totals.backends ? 'ok' : 'warn'} /><span class="cap">Backends</span><span class="v">{o().totals.backends_healthy} / {o().totals.backends} healthy</span></div>
                    <div>
                      <Sq tone={o().scheduler?.heartbeat?.stale ? 'warn' : 'ok'} />
                      <span class="cap">Round trip</span>
                      <span class="v">{o().scheduler?.heartbeat?.last_e2e_ms != null ? `${o().scheduler!.heartbeat!.last_e2e_ms} ms` : '—'}</span>
                    </div>
                    <div>
                      <Sq tone={o().incidents?.open ? 'warn' : 'ok'} />
                      <span class="cap">Incidents</span>
                      <span class="v" classList={{ warn: !!o().incidents?.open }}>{o().incidents?.open ?? 0} open</span>
                    </div>
                  </>
                )}
              </Show>
              <Show when={Object.keys(status()?.urls ?? {}).length}>
                <div class="cx-urls">
                  <span class="cap">Open</span>
                  <div class="cx-links">
                    <For each={Object.entries(status()!.urls).filter(([k, v]) => /^https:/.test(v) && k !== 'admin')}>
                      {([name, url]) => <a href={url} target="_blank" rel="noopener noreferrer">{name} ↗</a>}
                    </For>
                  </div>
                </div>
              </Show>
            </div>
          </FxCell>
        </section>

        <Show when={rolling() || pendingBuild()}>
          <section class="fx-row">
            <FxCell
              label={<span class="cx-head">Rollout <span class="cx-chip run">{rolling() ? `v${rolling()!.version ?? '?'}` : 'build'} · in progress</span></span>}
              meta={<span>{ago(rolling()?.created_at ?? pendingBuild()?.created_at)}</span>}
            >
              <Show when={pendingBuild() ?? rolling()?.build}>
                {(b) => (
                  <div class="cx-commit">
                    <span>git <span class="cx-sha">{b().commit_sha.slice(0, 7)}</span> <span class="dim">{b().commit_message?.split('\n')[0] ?? ''}</span></span>
                    <span class="cap dim" style={{ 'margin-left': 'auto' }}>{b().commit_author ?? ''}</span>
                  </div>
                )}
              </Show>
              <div class="cx-steps">
                <Stage
                  title="Build"
                  steps={BUILD_STEPS as unknown as string[]}
                  at={pendingBuild()?.status ?? (rolling()?.build ? 'done' : 'skip')}
                />
                <Stage title="Deploy" steps={DEPLOY_STEPS as unknown as string[]} at={rolling()?.status ?? 'waiting'} />
              </div>
            </FxCell>
          </section>
        </Show>

        <section class="fx-row">
          <FxCell label="History" meta={<span>{list().length} versions</span>}>
            <table class="fx-list cx-history">
              <thead>
                <tr><th class="cap">Ver</th><th class="cap">Status</th><th class="cap">Source</th><th class="cap">By</th><th class="cap">Started</th><th class="cap r">Took</th><th class="cap" /></tr>
              </thead>
              <tbody>
                <For each={list()} fallback={<tr><td colspan="7" class="dim">{data.loading ? 'Loading' : 'No deployments yet'}</td></tr>}>
                  {(d) => (
                    <tr>
                      <td><b>v{d.version ?? '?'}</b></td>
                      <td><span class={`cx-chip ${statusChip(d).tone}`}>{statusChip(d).label}</span></td>
                      <td class="cx-src" title={d.error ?? undefined}>
                        {source(d)}
                        <Show when={d.error}><span class="warn"> · {d.error}</span></Show>
                      </td>
                      <td>{d.actor ?? d.build?.commit_author ?? '—'}</td>
                      <td><Stamp at={d.created_at} /></td>
                      <td class="r">{took(d)}</td>
                      <td class="r">
                        <Show when={rollbackTarget()?.id === d.id}>
                          <button class="btn btn-sm" onClick={() => void rollback()}>Roll back</button>
                        </Show>
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
        </section>
        <Show when={cx?.limited()}>
          <section class="fx-row"><FxCell><p class="cx-note">This project's live dashboard is not available; restarts here go through Sp00ky Cloud.</p></FxCell></section>
        </Show>
      </div>
    </>
  );
}

/** One stage of a rollout as segments: done, current, waiting. */
function Stage(props: { title: string; steps: string[]; at: string }) {
  const idx = () => (props.at === 'done' ? props.steps.length : props.steps.indexOf(props.at));
  const label = () => (props.at === 'done' ? 'done' : props.at === 'skip' ? 'not built here' : props.at === 'waiting' ? 'waiting' : props.at.replace('_', ' '));
  return (
    <div class="cx-stage" classList={{ idle: props.at === 'skip' || props.at === 'waiting' }}>
      <div class="cap cx-stage-head"><span>{props.title}</span><span classList={{ acc: idx() >= 0 && idx() < props.steps.length }}>{label()}</span></div>
      <div class="cx-segs">
        <For each={props.steps}>{(_, i) => <i classList={{ done: i() < idx(), now: i() === idx() }} />}</For>
      </div>
      <div class="cx-sub cap"><For each={props.steps}>{(s) => <span>{s.replace('_', ' ')}</span>}</For></div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Settings                                                             */
/* ------------------------------------------------------------------ */

const TABS: { id: string; label: string }[] = [
  { id: 'env', label: 'Environment' },
  { id: 'domains', label: 'Domains' },
  { id: 'git', label: 'Git link' },
  { id: 'scale', label: 'Scale' },
  { id: 'access', label: 'Access tokens' },
  { id: 'danger', label: 'Danger zone' },
];

export function Settings(props: Ctx) {
  const params = useParams();
  const cx = useConsoleChrome();
  const tab = () => params.tab ?? 'env';
  return (
    <>
        <PageHead title="Settings" />
        <SettingsTabs active={tab()} limited={!!cx?.limited()} />
        <div class="fx-page">
          <Switch>
            <Match when={tab() === 'env'}><EnvTab {...props} /></Match>
            <Match when={tab() === 'domains'}><DomainsTab {...props} /></Match>
            <Match when={tab() === 'git'}><GitTab {...props} /></Match>
            <Match when={tab() === 'scale'}><ScaleTab {...props} /></Match>
            <Match when={tab() === 'danger'}><DangerTab {...props} /></Match>
          </Switch>
        </div>
    </>
  );
}

function SettingsTabs(props: { active: string; limited: boolean }) {
  // Access tokens are the scheduler's own page (it mints them), so that tab
  // goes there, and is absent when the scheduler is not reachable.
  const href = (id: string) => (id === 'env' ? '/settings' : id === 'access' ? '/access' : `/settings/${id}`);
  return (
    <nav class="cx-tabs cap">
      <For each={TABS.filter((t) => !(props.limited && t.id === 'access'))}>
        {(t) => (
          <A
            href={href(t.id)}
            classList={{ on: props.active === t.id }}
            ref={(el) => props.active === t.id && queueMicrotask(() => el.scrollIntoView({ block: 'nearest', inline: 'nearest' }))}
          >
            {t.label}
          </A>
        )}
      </For>
    </nav>
  );
}

/* ---------- environment ---------- */

interface EnvRow {
  name: string;
  has_dev: boolean;
  has_prod: boolean;
  updated_at: string;
}

function EnvTab(props: Ctx) {
  const tenant = props.workspace.id;
  const [rows, { refetch }] = createResource(() => cloud<EnvRow[]>(`/projects/${pid(props.project)}/envs`, { tenant }));
  const [values, setValues] = createSignal<{ development: Record<string, string>; production: Record<string, string> } | null>(null);
  const [pass, setPass] = createSignal('');
  const [busy, setBusy] = createSignal(false);
  const [name, setName] = createSignal('');
  const [value, setValue] = createSignal('');
  const [target, setTarget] = createSignal<'both' | 'prod' | 'dev'>('both');

  const doUnlock = async (e: Event) => {
    e.preventDefault();
    setBusy(true);
    try {
      await unlock(tenant, props.project.id, pass());
      setPass('');
      await reveal();
    } catch (err) {
      toast('bad', 'Could not unlock', message(err));
    } finally {
      setBusy(false);
    }
  };
  const reveal = async () => {
    const [development, production] = await Promise.all([
      loadValues(tenant, props.project.id, 'development'),
      loadValues(tenant, props.project.id, 'production'),
    ]);
    setValues({ development, production });
  };
  const save = async (e: Event) => {
    e.preventDefault();
    const n = name().trim().toUpperCase();
    if (!n) return;
    const body = target() === 'both' ? { dev_value: value(), both: true } : target() === 'prod' ? { prod_value: value() } : { dev_value: value() };
    const ok = await act('Saving ' + n, () => cloud(`/projects/${pid(props.project)}/envs/${encodeURIComponent(n)}`, { method: 'PUT', tenant, body }), `${n} saved`);
    if (ok) {
      setName('');
      setValue('');
      void refetch();
      if (values()) void reveal();
    }
  };
  const remove = async (n: string) => {
    const ok = await confirm({ title: `Delete ${n}`, consequences: ['Removes it from development and production.', 'Takes effect on the next deploy or restart.'], verb: 'Delete' });
    if (ok && (await act('Deleting ' + n, () => cloud(`/projects/${pid(props.project)}/envs/${encodeURIComponent(n)}`, { method: 'DELETE', tenant }), `${n} deleted`))) void refetch();
  };
  let form: HTMLFormElement | undefined;
  const edit = (n: string) => {
    setName(n);
    setValue('');
    form?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    form?.querySelector<HTMLInputElement>('input[type=password]')?.focus({ preventScroll: true });
  };
  const show = (env: 'development' | 'production', n: string, has: boolean) => {
    if (!has) return <span class="dim">—</span>;
    const v = values()?.[env]?.[n];
    return v === undefined ? <span class="cx-mask">••••••••</span> : <span class="cx-val">{v}</span>;
  };

  return (
    <>
      <section class="fx-row">
        <FxCell>
          <Show
            when={values()}
            fallback={
              <form class="cx-unlock" onSubmit={doUnlock}>
                <span class="fx-what">
                  {isUnlocked(tenant) ? 'Vault unlocked.' : 'Vault locked. Unlock to reveal values; setting one never needs it.'}
                </span>
                <Show when={!isUnlocked(tenant)} fallback={<button class="btn btn-sm btn-primary" type="button" onClick={() => void reveal()}>Reveal</button>}>
                  <input type="password" placeholder="Vault passphrase" value={pass()} onInput={(e) => setPass(e.currentTarget.value)} autocomplete="current-password" />
                  <button class="btn btn-sm btn-primary" type="submit" disabled={busy() || !pass()}>{busy() ? 'Unlocking' : 'Unlock'}</button>
                </Show>
              </form>
            }
          >
            <div class="cx-unlock"><span class="fx-what">Values shown; the vault locks itself after five idle minutes.</span><button class="btn btn-sm" onClick={() => setValues(null)}>Hide</button></div>
          </Show>
        </FxCell>
      </section>
      <section class="fx-row">
        <FxCell label="Variables" meta={<span>{rows()?.length ?? 0}</span>}>
          <table class="fx-list cx-env">
            <thead><tr><th class="cap">Name</th><th class="cap">Development</th><th class="cap">Production</th><th class="cap">Updated</th><th class="cap" /></tr></thead>
            <tbody>
              <For each={rows() ?? []} fallback={<tr><td colspan="5" class="dim">{rows.loading ? 'Loading' : rows.error ? message(rows.error) : 'No variables yet'}</td></tr>}>
                {(r) => (
                  <tr>
                    <td class="cx-env-name"><b>{r.name}</b></td>
                    <td data-label="dev">{show('development', r.name, r.has_dev)}</td>
                    <td data-label="prod">{show('production', r.name, r.has_prod)}</td>
                    <td><Stamp at={r.updated_at} /></td>
                    <td class="r cx-acts">
                      <button class="btn btn-sm" onClick={() => edit(r.name)}>Edit</button>
                      <button class="btn btn-sm" onClick={() => void remove(r.name)}>Delete</button>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </FxCell>
      </section>
      <section class="fx-row">
        <FxCell label="Set a variable" meta={<span>encrypted with the workspace key</span>}>
          <form class="cx-form cx-setvar" ref={form} onSubmit={save}>
            <label class="li-field"><span>Name</span><input value={name()} onInput={(e) => setName(e.currentTarget.value)} placeholder="STRIPE_SECRET_KEY" /></label>
            <label class="li-field"><span>Value</span><input type="password" value={value()} onInput={(e) => setValue(e.currentTarget.value)} autocomplete="off" /></label>
            <div class="fx-range" role="radiogroup">
              <For each={[['both', 'Both'], ['prod', 'Production'], ['dev', 'Development']] as const}>
                {([id, label]) => (
                  <button type="button" classList={{ on: target() === id }} onClick={() => setTarget(id)}>{label}</button>
                )}
              </For>
            </div>
            <button class="btn btn-primary" type="submit" disabled={!name().trim()}>Save</button>
          </form>
        </FxCell>
      </section>
    </>
  );
}

/* ---------- domains ---------- */

interface Domain {
  id: string;
  domain: string;
  target_kind: string;
  target_name: string;
  status: string;
  verification_error?: string;
  dns_target?: string;
  validation_records?: { type: string; name: string; value: string }[];
}

function DomainsTab(props: Ctx) {
  const tenant = props.workspace.id;
  const [rows, { refetch }] = createResource(() => cloud<Domain[]>(`/projects/${pid(props.project)}/domains`, { tenant }));
  const [domain, setDomain] = createSignal('');
  const [app, setApp] = createSignal('');
  const add = async (e: Event) => {
    e.preventDefault();
    const body: Record<string, string> = { domain: domain().trim() };
    if (app().trim()) body.app = app().trim();
    if (await act('Adding ' + body.domain, () => cloud(`/projects/${pid(props.project)}/domains`, { method: 'POST', tenant, body }), `${body.domain} added`)) {
      setDomain('');
      void refetch();
    }
  };
  const remove = async (d: Domain) => {
    const ok = await confirm({ title: `Remove ${d.domain}`, consequences: ['Traffic to it stops being served.'], verb: 'Remove' });
    if (ok && (await act('Removing ' + d.domain, () => cloud(`/projects/${pid(props.project)}/domains/${encodeURIComponent(d.id)}`, { method: 'DELETE', tenant })))) void refetch();
  };
  return (
    <>
      <section class="fx-row">
        <FxCell label="Custom domains">
          <table class="fx-list cx-domains">
            <thead><tr><th class="cap">Domain</th><th class="cap">Serves</th><th class="cap">Status</th><th class="cap">DNS</th><th class="cap" /></tr></thead>
            <tbody>
              <For each={rows() ?? []} fallback={<tr><td colspan="5" class="dim">{rows.loading ? 'Loading' : rows.error ? message(rows.error) : 'No custom domains'}</td></tr>}>
                {(d) => (
                  <tr>
                    <td><b>{d.domain}</b></td>
                    <td>{d.target_kind} {d.target_name}</td>
                    <td><span class={`cx-chip ${d.status === 'active' ? 'live' : d.status === 'failed' ? 'fail' : 'run'}`}>{d.status.replace('_', ' ')}</span></td>
                    <td class="cx-dns">
                      <Show when={d.dns_target}><div>CNAME → {d.dns_target}</div></Show>
                      <For each={d.validation_records ?? []}>{(r) => <div>{r.type} {r.name} → {r.value}</div>}</For>
                      <Show when={d.verification_error}><div class="warn">{d.verification_error}</div></Show>
                    </td>
                    <td class="r"><button class="btn btn-sm" onClick={() => void remove(d)}>Remove</button></td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </FxCell>
      </section>
      <section class="fx-row">
        <FxCell label="Add a domain">
          <form class="cx-form" onSubmit={add}>
            <label class="li-field"><span>Domain</span><input value={domain()} onInput={(e) => setDomain(e.currentTarget.value)} placeholder="app.example.com" /></label>
            <label class="li-field"><span>App (optional)</span><input value={app()} onInput={(e) => setApp(e.currentTarget.value)} placeholder="web" /></label>
            <button class="btn btn-primary" type="submit" disabled={!domain().trim()}>Add</button>
          </form>
        </FxCell>
      </section>
    </>
  );
}

/* ---------- git link ---------- */

interface Link {
  status: string;
  repo_owner?: string;
  repo_name?: string;
  branch?: string;
  auto_deploy?: boolean;
  config_path?: string;
}

interface BuildRun {
  id: string;
  commit_sha: string;
  commit_message?: string;
  commit_author?: string;
  status: string;
  error?: string;
  created_at: string;
  completed_at?: string;
}

function GitTab(props: Ctx) {
  const tenant = props.workspace.id;
  const [link, { refetch }] = createResource(() => cloud<Link>(`/projects/${pid(props.project)}/link`, { tenant }));
  const [runs, { refetch: refetchRuns }] = createResource(
    () => link()?.status === 'linked',
    () => cloud<BuildRun[]>(`/projects/${pid(props.project)}/link/runs`, { tenant }),
  );
  const toggleAuto = async () => {
    const next = !link()?.auto_deploy;
    if (await act('Auto deploy', () => cloud(`/projects/${pid(props.project)}/link`, { method: 'PATCH', tenant, body: { auto_deploy: next } }), next ? 'Pushes deploy automatically' : 'Pushes no longer deploy')) void refetch();
  };
  const trigger = async () => {
    if (await act('Build', () => cloud(`/projects/${pid(props.project)}/link/trigger`, { method: 'POST', tenant }), 'Build started')) void refetchRuns();
  };
  return (
    <Show
      when={link()?.status === 'linked'}
      fallback={
        <section class="fx-row">
          <FxCell label="Git link">
            <p class="cx-note">
              {link.loading ? 'Loading' : link()?.status === 'pending_repo_selection' ? 'The GitHub App is installed; pick a repository with `spky link`.' : 'Not linked. Run `spky link` in the project to deploy on every push.'}
            </p>
          </FxCell>
        </section>
      }
    >
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(3, minmax(0, 1fr))' }}>
        <FxCell label="Repository"><div class="fx-big cx-mid">{link()!.repo_owner}/{link()!.repo_name}</div></FxCell>
        <FxCell label="Branch"><div class="fx-big cx-mid">{link()!.branch}</div></FxCell>
        <FxCell label="On push">
          <div class="cx-row">
            <span class="fx-big cx-mid">{link()!.auto_deploy ? 'deploy' : 'nothing'}</span>
            <button class="btn btn-sm" onClick={() => void toggleAuto()}>{link()!.auto_deploy ? 'Turn off' : 'Turn on'}</button>
          </div>
        </FxCell>
      </section>
      <section class="fx-row">
        <FxCell label="Builds" meta={<button class="btn btn-sm btn-primary" onClick={() => void trigger()}>Build now</button>}>
          <table class="fx-list cx-builds">
            <thead><tr><th class="cap">Commit</th><th class="cap">Author</th><th class="cap">Status</th><th class="cap">Started</th></tr></thead>
            <tbody>
              <For each={runs() ?? []} fallback={<tr><td colspan="4" class="dim">No builds yet</td></tr>}>
                {(r) => (
                  <tr>
                    <td><span class="cx-sha">{r.commit_sha.slice(0, 7)}</span> <span class="dim">{r.commit_message?.split('\n')[0]}</span></td>
                    <td>{r.commit_author ?? '—'}</td>
                    <td><span class={`cx-chip ${r.status === 'running' ? 'live' : r.status === 'failed' ? 'fail' : r.status === 'cancelled' ? 'dim' : 'run'}`}>{r.status}</span></td>
                    <td><Stamp at={r.created_at} /></td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </FxCell>
      </section>
    </Show>
  );
}

/* ---------- scale ---------- */

function ScaleTab(props: Ctx) {
  const tenant = props.workspace.id;
  const overview = useOverview();
  const [count, setCount] = createSignal(overview()?.totals.ssps ?? 1);
  const apply = async (e: Event) => {
    e.preventDefault();
    await act('Scaling', () => cloud(`/projects/${pid(props.project)}/scale`, { method: 'POST', tenant, body: { ssp_count: count() } }), `Scaling to ${count()} SSPs`);
  };
  return (
    <section class="fx-row">
      <FxCell label="Sync processors" meta={<span>now {overview()?.totals.ssps ?? '?'}</span>}>
        <form class="cx-form" onSubmit={apply}>
          <label class="li-field"><span>SSPs</span><input type="number" min="1" max="20" value={count()} onInput={(e) => setCount(Number(e.currentTarget.value) || 1)} /></label>
          <button class="btn btn-primary" type="submit">Scale</button>
        </form>
        <p class="cx-note">Starter runs one SSP, Pro up to three. More SSPs spread views and survive one failing.</p>
      </FxCell>
    </section>
  );
}

/* ---------- danger zone ---------- */

function DangerTab(props: Ctx) {
  const cx = useConsoleChrome();
  const tenant = props.workspace.id;
  const teardown = async () => {
    const ok = await confirm({
      title: `Tear down ${props.project.slug}`,
      consequences: ['Stops and removes every container of the live deployment.', 'The project, its env and its backups remain; the next deploy brings it back.'],
      verb: 'Tear down',
      typeToConfirm: props.project.slug,
    });
    if (ok) await act('Tear down', () => cloud(`/projects/${pid(props.project)}/deploy`, { method: 'DELETE', tenant }), 'Tearing down');
  };
  const destroy = async () => {
    const ok = await confirm({
      title: `Destroy ${props.project.slug}`,
      consequences: ['Removes the project, its deployments and its data volumes.', 'This cannot be undone. Take a backup first if you may need the data.'],
      verb: 'Destroy',
      typeToConfirm: props.project.slug,
      warning: 'Permanent',
    });
    if (ok && (await act('Destroy', () => cloud(`/projects/${pid(props.project)}`, { method: 'DELETE', tenant }), 'Project destroyed'))) {
      cx?.go(`/${props.workspace.slug}/-`);
    }
  };
  return (
    <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1fr))' }}>
      <FxCell label="Tear down the deployment">
        <p class="cx-note">Stops everything that runs; keeps the project, env and backups.</p>
        <button class="btn" onClick={() => void teardown()}>Tear down</button>
      </FxCell>
      <FxCell label="Destroy the project">
        <p class="cx-note">Deletes the project and its data for good.</p>
        <button class="btn cx-danger" onClick={() => void destroy()}>Destroy project</button>
      </FxCell>
    </section>
  );
}
