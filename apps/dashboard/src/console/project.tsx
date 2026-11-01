/**
 * A project's pages that come from Sp00ky Cloud rather than its scheduler:
 * Deployments (what is live and where, what is rolling out, what came before) and
 * Settings (env, domains, git link, scale, tokens, danger zone). They work
 * whether or not the project's own dashboard could open.
 */

import { For, Match, Show, Switch, createEffect, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js';
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
/** History rows and addresses shown before "Show all". */
const HISTORY_ROWS = 10;
const URL_ROWS = 5;

const sha7 = (sha: string) => sha.slice(0, 7);
const firstLine = (s: string | null | undefined) => s?.split('\n')[0]?.trim() ?? '';
const upper1 = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

/** How long a rollout took, or has taken so far. */
function took(d: DeploymentRow): string {
  if (d.deployed_at) return formatDuration(Date.parse(d.deployed_at) - Date.parse(d.created_at));
  return IN_FLIGHT.has(d.status) ? formatDuration(Date.now() - Date.parse(d.created_at)) : '—';
}

/** A deployment's state as a square and a word. */
function state(d: DeploymentRow): { label: string; tone: string } {
  if (d.live) return d.status === 'degraded' ? { label: 'Degraded', tone: 'warn' } : { label: 'Live', tone: 'ok' };
  if (IN_FLIGHT.has(d.status)) return { label: d.status === 'pending' ? 'Queued' : upper1(d.status.replace('_', ' ')), tone: 'run' };
  if (d.status === 'failed') return { label: 'Failed', tone: 'bad' };
  if (d.status === 'superseded') return { label: d.live_from ? 'Replaced' : 'Never live', tone: 'idle' };
  return { label: upper1(d.status), tone: 'idle' };
}

/** Where a deployment came from, in a word or two. */
function origin(d: DeploymentRow): string {
  if (d.build) return 'git push';
  switch (d.source) {
    case 'rollback':
      return 'rollback';
    case 'auto_rollback':
      return 'auto rollback';
    case 'cli':
      return 'spky deploy';
    case null:
      return 'deploy';
    default:
      return d.source;
  }
}

/** What a deployment is: its commit, or what made it. */
function what(d: DeploymentRow): string {
  if (d.build) return firstLine(d.build.commit_message) || `Commit ${sha7(d.build.commit_sha)}`;
  switch (d.source) {
    case 'rollback':
      return `Rolled back to v${d.rollback_of ?? '?'}`;
    case 'auto_rollback':
      return 'Rolled back after a failed deploy';
    case 'console':
      return 'Deployed from the console';
    case 'ci':
      return 'Deployed by CI with a build key';
    case 'git':
      return 'Deployed from git';
    case 'cli':
      return 'Deployed from the CLI';
    default:
      return 'Deployment';
  }
}

/** A deployment's infra versions, e.g. scheduler canary.369. */
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

/** A public address of the project. */
interface Endpoint {
  url: string;
  host: string;
  /** What answers there: `app web`, `api chat`, `database`. */
  serves: string;
  custom: boolean;
  /** A browser page lives there, so it can be previewed. */
  web: boolean;
  status: string;
  tone: string;
  rank: number;
}

/** What answers at an address: `app`, `api · chat`, `database`. */
function serves(role: string, name: string): string {
  const what = role === 'frontend' ? 'app' : role === 'backend' ? 'api' : role === 'surrealdb' ? 'database' : role;
  if (!what) return name;
  return name && name !== what && role !== 'surrealdb' ? `${what} · ${name}` : what;
}

function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

/**
 * Every public address of a project, the main one first: an active custom
 * domain on a web app, else the web app's own URL, then APIs, the database,
 * and domains still waiting on DNS. The main one is what Visit opens and the
 * preview shows. The scheduler's admin URL is left out; this is its console.
 */
function endpoints(status: CloudDeploymentStatus | null | undefined, domains: Domain[] | undefined): Endpoint[] {
  const vms = new Map<string, { role: string; status: string }>();
  for (const vm of status?.vms ?? []) {
    // A redeploy briefly runs an old and a new VM under one name.
    if (!vms.has(vm.name) || vm.status === 'running') vms.set(vm.name, vm);
  }
  const out: Endpoint[] = [];
  for (const d of domains ?? []) {
    const active = d.status === 'active';
    const web = d.target_kind === 'frontend';
    out.push({
      url: `https://${d.domain}`,
      host: d.domain,
      serves: serves(web ? 'frontend' : d.target_kind, d.target_name),
      custom: true,
      web,
      status: active ? 'active' : d.status.replace(/_/g, ' '),
      tone: active ? 'ok' : d.status === 'failed' ? 'bad' : 'warn',
      rank: !active ? 6 : web ? 0 : 2,
    });
  }
  for (const [name, url] of Object.entries(status?.urls ?? {})) {
    if (!/^https:/.test(url) || name === 'admin') continue;
    const vm = vms.get(name);
    const role = vm?.role ?? (name === 'surrealdb' ? 'surrealdb' : name === 'app' || name === 'web' ? 'frontend' : '');
    const running = !vm || vm.status === 'running';
    out.push({
      url,
      host: hostOf(url),
      serves: serves(role, name),
      custom: false,
      web: role === 'frontend',
      status: vm?.status ?? 'running',
      tone: running ? 'ok' : 'warn',
      rank: role === 'frontend' ? (name === 'app' || name === 'web' ? 1 : 1.5) : role === 'backend' ? 3 : role === 'surrealdb' ? 5 : 4,
    });
  }
  return out.sort((a, b) => a.rank - b.rank || a.host.localeCompare(b.host));
}

/** The address a visitor would type: never the database or a domain still on its way. */
const mainOf = (list: Endpoint[]) => (list[0] && list[0].rank < 5 ? list[0] : undefined);

export function Deployments(props: Ctx) {
  const cx = useConsoleChrome();
  const overview = useOverview();
  const tenant = () => props.workspace.id;
  const [data, { refetch }] = createResource(() => consoleApi.deployments(props.project.id));
  const [status, { refetch: refetchStatus }] = createResource(() =>
    cloud<CloudDeploymentStatus>(`/projects/${pid(props.project)}/deployment`, { tenant: tenant() }).catch(() => null),
  );
  const [domains, { refetch: refetchDomains }] = createResource(() =>
    cloud<Domain[]>(`/projects/${pid(props.project)}/domains`, { tenant: tenant() }).catch(() => [] as Domain[]),
  );
  const [link] = createResource(() =>
    cloud<Link>(`/projects/${pid(props.project)}/link`, { tenant: tenant() }).catch((): Link => ({ status: 'not_linked' })),
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
  const urls = createMemo(() => endpoints(status(), domains()));
  const main = createMemo(() => mainOf(urls()));
  const linked = () => link()?.status === 'linked';
  const commitUrl = (sha: string) => {
    const l = link();
    return l?.repo_owner && l.repo_name ? `https://github.com/${l.repo_owner}/${l.repo_name}/commit/${sha}` : undefined;
  };

  // Fast while something is moving, slow at rest.
  let timer: ReturnType<typeof setTimeout>;
  let rounds = 0;
  const loop = () => {
    timer = setTimeout(() => {
      void refetch();
      void refetchStatus();
      if (++rounds % 4 === 0) void refetchDomains();
      loop();
    }, busy() ? 3000 : 15000);
  };
  loop();
  onCleanup(() => clearTimeout(timer));

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
  const [allUrls, setAllUrls] = createSignal(false);
  const [allHistory, setAllHistory] = createSignal(false);
  let urlsCell: HTMLDivElement | undefined;
  const showUrls = () => {
    setAllUrls(true);
    urlsCell?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  /** Health in a strip: the live overview when the project's dashboard is open, else the cloud's VM states. */
  const health = createMemo(() => {
    const o = overview();
    if (o) {
      const e2e = o.scheduler?.heartbeat?.last_e2e_ms;
      return [
        { label: 'SSPs', value: `${o.totals.ssps_ready}/${o.totals.ssps}`, unit: 'ready', tone: o.totals.ssps_ready === o.totals.ssps ? 'ok' : 'warn' },
        { label: 'Backends', value: `${o.totals.backends_healthy}/${o.totals.backends}`, unit: 'healthy', tone: o.totals.backends_healthy === o.totals.backends ? 'ok' : 'warn' },
        { label: 'Round trip', value: e2e != null ? String(e2e) : '—', unit: 'ms', tone: o.scheduler?.heartbeat?.stale ? 'warn' : 'ok' },
        { label: 'Incidents', value: String(o.incidents?.open ?? 0), unit: 'open', tone: o.incidents?.open ? 'warn' : 'ok' },
      ];
    }
    const vms = status()?.vms ?? [];
    return (
      [
        ['frontend', 'Apps'],
        ['backend', 'Backends'],
        ['ssp', 'SSPs'],
        ['scheduler', 'Scheduler'],
        ['surrealdb', 'Database'],
      ] as const
    )
      .map(([role, label]) => {
        const of = vms.filter((v) => v.role === role);
        const up = of.filter((v) => v.status === 'running').length;
        return { label, value: `${up}/${of.length}`, unit: 'running', tone: up === of.length ? 'ok' : 'warn', n: of.length };
      })
      .filter((h) => h.n > 0);
  });

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
            <Show when={linked()}>
              <button class="btn btn-sm" disabled={busy()} onClick={rebuild}>Rebuild</button>
            </Show>
            <Show when={main()}>
              {(m) => (
                <a class="btn btn-sm btn-primary" href={m().url} target="_blank" rel="noopener noreferrer">Visit ↗</a>
              )}
            </Show>
          </>
        }
      />
      <div class="fx-page">
        <Show when={data.error}>
          <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{message(data.error)}</span></div></FxCell></section>
        </Show>

        {/* Production: what a visitor sees, where, and what made it. */}
        <section class="fx-row cx-prod">
          <FxCell label="Preview" meta={<PreviewToggle />} class="cx-prod-shot">
            <Preview target={main()} loading={status.loading || domains.loading} />
          </FxCell>
          <FxCell
            label="Production"
            meta={
              <Show when={live()?.live_from}>
                <span title={`live since ${formatStamp(live()!.live_from)}`}>up {ago(live()!.live_from).replace(/ ago$/, '')}</span>
              </Show>
            }
          >
            <div class="cx-main">
              <span class="cap">{main()?.custom ? 'Domain' : 'URL'}</span>
              <Show
                when={main()}
                fallback={<span class="cx-main-none">{status.loading ? 'Loading' : 'No public address yet'}</span>}
              >
                {(m) => (
                  <a class="cx-main-url" href={m().url} target="_blank" rel="noopener noreferrer">
                    {m().host}<span class="cx-arrow">↗</span>
                  </a>
                )}
              </Show>
              <Show when={urls().length > 1}>
                <div class="cx-alts">
                  <For each={urls().slice(1, 3)}>
                    {(u) => <a href={u.url} target="_blank" rel="noopener noreferrer">{u.host}</a>}
                  </For>
                  <Show when={urls().length > 3}>
                    <button class="cx-more" onClick={showUrls}>+{urls().length - 3} more</button>
                  </Show>
                </div>
              </Show>
            </div>
            <Show
              when={live()}
              fallback={<div class="fx-empty cap">{data.loading ? 'Loading' : rolling() ? `v${rolling()!.version} is rolling out` : 'Nothing is live yet'}</div>}
            >
              {(d) => (
                <>
                  <div class="cx-facts">
                    <div>
                      <span class="cap">Status</span>
                      <b class={`cx-st ${state(d()).tone}`}><Sq tone={state(d()).tone} />{state(d()).label}<small>v{d().version ?? '?'}</small></b>
                    </div>
                    <div>
                      <span class="cap">Created</span>
                      <b title={formatStamp(d().created_at)}>
                        {ago(d().created_at)}
                        <Show when={d().actor ?? d().build?.commit_author}><small>by {d().actor ?? d().build?.commit_author}</small></Show>
                      </b>
                    </div>
                    <div>
                      <span class="cap">Rolled out in</span>
                      <b>{took(d())}</b>
                    </div>
                  </div>
                  <div class="cx-commit">
                    <Show when={d().build} fallback={<span>{origin(d())} <span class="dim">{what(d())}</span></span>}>
                      {(b) => (
                        <span>
                          <Show when={linked() && link()?.branch}><span class="cx-branch"><BranchIcon />{link()!.branch}</span>{' '}</Show>
                          <Sha sha={b().commit_sha} href={commitUrl(b().commit_sha)} />{' '}
                          <span class="dim">{firstLine(b().commit_message)}</span>
                        </span>
                      )}
                    </Show>
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
        </section>

        <Show when={health().length}>
          <section class="fx-row cx-strip" style={{ 'grid-template-columns': `repeat(${health().length}, minmax(0, 1fr))` }}>
            <For each={health()}>
              {(h) => (
                <FxCell label={h.label} meta={<Sq tone={h.tone} />}>
                  <div class="cx-stat" classList={{ warn: h.tone === 'warn' }}>{h.value}<small>{h.unit}</small></div>
                </FxCell>
              )}
            </For>
          </section>
        </Show>

        <Show when={rolling() || pendingBuild()}>
          <section class="fx-row">
            <FxCell
              label={<span class="cx-head">Rollout <span class="cx-chip run">{rolling() ? `v${rolling()!.version ?? '?'}` : 'build'} · in progress</span></span>}
              meta={<span>{ago(rolling()?.created_at ?? pendingBuild()?.created_at)}</span>}
            >
              <Show when={pendingBuild() ?? rolling()?.build}>
                {(b) => (
                  <div class="cx-commit">
                    <span>
                      <Show when={linked() && link()?.branch}><span class="cx-branch"><BranchIcon />{link()!.branch}</span>{' '}</Show>
                      <Sha sha={b().commit_sha} href={commitUrl(b().commit_sha)} /> <span class="dim">{firstLine(b().commit_message)}</span>
                    </span>
                    <span class="cap dim">{b().commit_author ?? ''}</span>
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

        <Show when={urls().length}>
          <section class="fx-row">
            <div class="fx-cell" ref={urlsCell} style={{ 'scroll-margin-top': '64px' }}>
              <div class="fx-head">
                <span class="cap">Domains</span>
                <span class="fx-meta cap">
                  <span>{urls().length} {urls().length === 1 ? 'address' : 'addresses'}</span>
                  <A href="/settings/domains">Add a domain</A>
                </span>
              </div>
              <div class="cx-urls">
                <For each={allUrls() ? urls() : urls().slice(0, URL_ROWS)}>
                  {(u) => (
                    <div class="cx-url" classList={{ main: u === main() }}>
                      <a class="cx-url-host" href={u.url} target="_blank" rel="noopener noreferrer">
                        <span>{u.host}</span>
                        <Show when={u === main()}><span class="cx-chip live">main</span></Show>
                      </a>
                      <span class="cx-url-serves">{u.serves}</span>
                      <span class="cap cx-url-kind">{u.custom ? 'custom domain' : 'sp00ky url'}</span>
                      <span class={`cx-st ${u.tone}`}><Sq tone={u.tone} />{u.status}</span>
                      <span class="cx-url-acts">
                        <button
                          class="btn btn-sm"
                          onClick={() => {
                            void navigator.clipboard?.writeText(u.url);
                            toast('ok', 'Copied', u.url);
                          }}
                        >
                          Copy
                        </button>
                        <a class="btn btn-sm" href={u.url} target="_blank" rel="noopener noreferrer">Open ↗</a>
                      </span>
                    </div>
                  )}
                </For>
                <Show when={urls().length > URL_ROWS}>
                  <button class="cx-showall cap" onClick={() => setAllUrls(!allUrls())}>
                    {allUrls() ? 'Show fewer' : `Show all ${urls().length}`}
                  </button>
                </Show>
              </div>
            </div>
          </section>
        </Show>

        <section class="fx-row">
          <FxCell label="History" meta={<span>{list().length} {list().length === 1 ? 'version' : 'versions'}</span>}>
            <div class="cx-deps">
              <For
                each={allHistory() ? list() : list().slice(0, HISTORY_ROWS)}
                fallback={<div class="cx-dep-empty dim">{data.loading ? 'Loading' : 'No deployments yet'}</div>}
              >
                {(d) => {
                  const s = state(d);
                  const who = d.actor ?? d.build?.commit_author;
                  return (
                    <div class="cx-dep" classList={{ live: d.live, run: s.tone === 'run', failed: s.tone === 'bad' }}>
                      <div class="cx-dep-v">
                        <b>v{d.version ?? '?'}</b>
                        <span class="cap">{origin(d)}</span>
                      </div>
                      <div class="cx-dep-st">
                        <span class={`cx-st ${s.tone}`}><Sq tone={s.tone} />{s.label}</span>
                        <span class="cap">{took(d)}</span>
                      </div>
                      <div class="cx-dep-what" title={d.error ?? undefined}>
                        <span class="t">{what(d)}</span>
                        <span class="m">
                          <Show when={d.build}>
                            {(b) => (
                              <>
                                <Show when={linked() && link()?.branch}><span class="cx-branch"><BranchIcon />{link()!.branch}</span></Show>
                                <Sha sha={b().commit_sha} href={commitUrl(b().commit_sha)} />
                              </>
                            )}
                          </Show>
                          <Show
                            when={d.error}
                            fallback={
                              <Show when={!d.build}>
                                <span>{images(d).map(([k, v]) => `${k} ${v}`).join(' · ') || 'default images'}</span>
                              </Show>
                            }
                          >
                            <span class="warn">{d.error}</span>
                          </Show>
                        </span>
                      </div>
                      <div class="cx-dep-when">
                        <span title={formatStamp(d.created_at)}>{ago(d.created_at)}</span>
                        <span class="cx-who-by">{who ? `by ${who}` : ''}</span>
                      </div>
                      <div class="cx-dep-act">
                        <Switch>
                          <Match when={d.live}><span class="cx-chip live">current</span></Match>
                          <Match when={rollbackTarget()?.id === d.id}>
                            <button class="btn btn-sm" onClick={() => void rollback()}>Roll back</button>
                          </Match>
                        </Switch>
                      </div>
                    </div>
                  );
                }}
              </For>
              <Show when={list().length > HISTORY_ROWS}>
                <button class="cx-showall cap" onClick={() => setAllHistory(!allHistory())}>
                  {allHistory() ? 'Show fewer' : `Show all ${list().length}`}
                </button>
              </Show>
            </div>
          </FxCell>
        </section>
        <Show when={cx?.limited()}>
          <section class="fx-row"><FxCell><p class="cx-note">This project's live dashboard is not available; restarts here go through Sp00ky Cloud.</p></FxCell></section>
        </Show>
      </div>
    </>
  );
}

function BranchIcon() {
  return (
    <svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true">
      <circle cx="4.5" cy="3.5" r="1.5" />
      <circle cx="4.5" cy="12.5" r="1.5" />
      <circle cx="11.5" cy="5.5" r="1.5" />
      <path d="M4.5 5v6M11.5 7c0 2.5-2 3-5.5 4" />
    </svg>
  );
}

/** A commit's short hash, linked to GitHub when the project is. */
function Sha(props: { sha: string; href?: string }) {
  return (
    <Show when={props.href} fallback={<span class="cx-sha">{sha7(props.sha)}</span>}>
      <a class="cx-sha" href={props.href} target="_blank" rel="noopener noreferrer" title={props.sha}>{sha7(props.sha)}</a>
    </Show>
  );
}

/* ---------- preview ---------- */

const PREVIEW_KEY = 'spky.console.preview';
/** The viewport the site is drawn at before it is scaled into the card. */
const PREVIEW_W = 1280;
const [previewOn, setPreviewOn] = createSignal(localStorage.getItem(PREVIEW_KEY) !== 'off');

function PreviewToggle() {
  return (
    <button
      class="cx-toggle cap"
      onClick={() => {
        localStorage.setItem(PREVIEW_KEY, previewOn() ? 'off' : 'on');
        setPreviewOn(!previewOn());
      }}
    >
      {previewOn() ? 'Hide' : 'Show'}
    </button>
  );
}

/**
 * The main address, live, as a thumbnail: the real site in a frame drawn at a
 * desktop width and scaled into the card. Sandboxed and credentialless (its
 * own throwaway storage, no cookies, so it shows what a new visitor sees),
 * never focused or clicked; a click opens the site. Turning it off is kept
 * per browser, for an app too heavy to keep a second copy of open.
 */
function Preview(props: { target: Endpoint | undefined; loading: boolean }) {
  const [phase, setPhase] = createSignal<'loading' | 'ready' | 'failed'>('loading');
  let screen!: HTMLDivElement;
  let host!: HTMLDivElement;
  // Keyed on the address alone: a refetch hands back an equal target.
  const src = createMemo(() => (previewOn() && props.target?.web ? props.target.url : undefined));

  onMount(() => {
    // Width sets the scale; the frame is as tall as the card, so the preview
    // fills whatever height the row gives it.
    const fit = () => {
      const scale = screen.clientWidth / PREVIEW_W;
      screen.style.setProperty('--s', String(scale));
      if (scale > 0) screen.style.setProperty('--h', `${Math.ceil(screen.clientHeight / scale)}px`);
    };
    const ro = new ResizeObserver(fit);
    ro.observe(screen);
    onCleanup(() => ro.disconnect());
  });

  createEffect(() => {
    const url = src();
    if (!url) return;
    setPhase('loading');
    const frame = document.createElement('iframe');
    // Set before `src`: credentialless only applies to a frame's first load.
    frame.setAttribute('credentialless', '');
    frame.setAttribute('sandbox', 'allow-scripts allow-same-origin');
    frame.setAttribute('referrerpolicy', 'no-referrer');
    frame.setAttribute('tabindex', '-1');
    frame.setAttribute('aria-hidden', 'true');
    frame.className = 'cx-shot-frame';
    const slow = setTimeout(() => setPhase((p) => (p === 'loading' ? 'failed' : p)), 15_000);
    frame.addEventListener('load', () => {
      clearTimeout(slow);
      setPhase('ready');
    });
    frame.src = url;
    host.append(frame);
    onCleanup(() => {
      clearTimeout(slow);
      frame.remove();
    });
  });

  const note = (): { title: string; detail?: string } | undefined => {
    const t = props.target;
    if (!t) return props.loading ? undefined : { title: 'Nothing to preview', detail: 'No public address yet' };
    if (!t.web) return { title: 'No page to preview', detail: `${t.host} serves an API` };
    if (!previewOn()) return { title: 'Preview off', detail: 'Show it to load the site here' };
    if (phase() === 'failed') return { title: 'The site did not load', detail: `${t.host} took longer than 15 s` };
    return undefined;
  };

  return (
    <div class="cx-shot" classList={{ ready: !!src() && phase() === 'ready' }}>
      <div class="cx-shot-bar">
        <i /><i /><i />
        <span>{props.target?.host ?? ''}</span>
      </div>
      <div class="cx-shot-screen" ref={screen}>
        {/* The frame lives here, outside what Solid renders. */}
        <div class="cx-shot-host" ref={host} />
        <Show when={note()} fallback={<Show when={phase() !== 'ready' || !src()}><span class="sk cx-shot-sk" /></Show>}>
          {(n) => (
            <div class="cx-shot-note">
              <b>{n().title}</b>
              <Show when={n().detail}><span class="cap">{n().detail}</span></Show>
            </div>
          )}
        </Show>
        <Show when={props.target}>
          {(t) => (
            <a class="cx-shot-open" href={t().url} target="_blank" rel="noopener noreferrer" aria-label={`Open ${t().host}`}>
              <span class="cap">Open ↗</span>
            </a>
          )}
        </Show>
      </div>
    </div>
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
