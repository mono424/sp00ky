/**
 * The Sp00ky Cloud console (console.sp00ky.cloud).
 *
 * Addresses are `/{workspace}/{project}/{page}` for a project and
 * `/{workspace}/-/{page}` for the workspace itself. The workspace list comes
 * from the control plane in one call; a project's dashboard comes from that
 * project's own admin API, which the browser calls directly: the control plane
 * mints a one-minute grant, the project's scheduler exchanges it for a
 * 15-minute session, and the console mints a fresh one before it ends. The
 * session token lives in memory only.
 *
 * A project whose scheduler cannot take a grant yet (older than the console,
 * or a Cloudflare project whose SSP does not speak it) still gets the cloud's
 * own pages, Deployments and Settings, and a note on how to upgrade.
 */

import {
  ErrorBoundary,
  For,
  Match,
  Show,
  Switch,
  createEffect,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
  onMount,
  type JSX,
} from 'solid-js';
import { A, Route, Router } from '@solidjs/router';
import { fetchConfig, onUnauthorized, setConsoleTarget } from '../api/client';
import type { LoginResponse } from '../api/types';
import { ConfirmHost, ToastHost } from '../components/Actions';
import { Logo, PageHead } from '../components/Chrome';
import { Shell } from '../components/Shell';
import { ProjectApp } from '../ProjectApp';
import { CloudError, consoleApi, signIn, signOut, type Grant, type Workspace, type WorkspaceProject } from './cloud';
import { ConsoleChromeProvider, type ConsoleChrome } from './chrome';
import { Deployments, Settings } from './project';
import { Activity, Billing, Keys, Members, Projects, Sessions, Vault } from './workspace';
import { lock } from './vault';

const LAST_KEY = 'spky.console.last';

type Where =
  | { kind: 'root' }
  | { kind: 'workspace'; ws: string }
  | { kind: 'project'; ws: string; project: string };

function parse(path: string): Where {
  const seg = path.split('/').filter(Boolean);
  if (seg.length === 0) return { kind: 'root' };
  if (seg.length === 1 || seg[1] === '-') return { kind: 'workspace', ws: seg[0] };
  return { kind: 'project', ws: seg[0], project: seg[1] };
}

export function ConsoleApp() {
  document.title = 'Sp00ky Console';
  const [path, setPath] = createSignal(window.location.pathname);
  const onPop = () => setPath(window.location.pathname);
  window.addEventListener('popstate', onPop);
  onCleanup(() => window.removeEventListener('popstate', onPop));

  const go = (to: string, replace = false) => {
    if (replace) window.history.replaceState(null, '', to);
    else window.history.pushState(null, '', to);
    setPath(to);
  };

  const [me] = createResource(async () => {
    try {
      return await consoleApi.me();
    } catch (err) {
      if (err instanceof CloudError && err.status === 401) return null;
      throw err;
    }
  });
  const [workspaces, { refetch }] = createResource(
    () => me(),
    () => consoleApi.workspaces(),
  );
  // Project states in the switcher stay fresh without a socket per project.
  const tick = setInterval(() => me() && void refetch(), 60_000);
  onCleanup(() => clearInterval(tick));

  const where = createMemo(() => parse(path()));
  const workspace = createMemo(() => {
    const w = where();
    return w.kind === 'root' ? undefined : workspaces()?.find((x) => x.slug === w.ws);
  });
  const project = createMemo(() => {
    const w = where();
    return w.kind === 'project' ? workspace()?.projects.find((p) => p.slug === w.project) : undefined;
  });

  // `/` goes where the operator last was, else the first project there is.
  const landing = createMemo(() => {
    const list = workspaces();
    if (!list || where().kind !== 'root') return;
    const last = localStorage.getItem(LAST_KEY);
    if (last) {
      const l = parse(last);
      if (l.kind === 'project' && list.some((w) => w.slug === l.ws && w.projects.some((p) => p.slug === l.project))) return last;
    }
    const first = list.find((w) => w.projects.length) ?? list[0];
    if (!first) return;
    return first.projects[0] ? `/${first.slug}/${first.projects[0].slug}` : `/${first.slug}/-`;
  });
  createEffect(() => {
    const to = landing();
    if (to) go(to, true);
  });

  const [limited, setLimited] = createSignal(false);
  const chrome: ConsoleChrome = {
    me: () => me() ?? undefined,
    workspaces: () => workspaces() ?? [],
    workspace,
    project,
    limited,
    go: (to) => go(to),
    signOut: () => {
      lock();
      void signOut().then(() => window.location.assign('/'));
    },
  };

  return (
    <>
      <Switch fallback={<Splash text="Loading" />}>
        <Match when={me.state === 'ready' && me() === null}>
          <Landing />
        </Match>
        <Match when={me.error || workspaces.error}>
          <Splash text={(me.error ?? workspaces.error)?.message ?? 'Sp00ky Cloud is not answering'} />
        </Match>
        <Match when={workspaces()}>
          <ConsoleChromeProvider value={chrome}>
            <Switch fallback={<Splash text="Nothing here" detail="This workspace or project does not exist, or you are not a member of it." />}>
              <Match when={where().kind === 'project' && project()}>
                {/* Keyed: switching projects is a fresh session on another host. */}
                <Show when={project()} keyed>
                  {(p) => <ProjectView workspace={workspace()!} project={p} setLimited={setLimited} />}
                </Show>
              </Match>
              <Match when={where().kind === 'workspace' && workspace()}>
                <Show when={workspace()} keyed>
                  {(w) => <WorkspaceView workspace={w} />}
                </Show>
              </Match>
              <Match when={where().kind === 'root' && workspaces()!.length === 0}>
                <Splash text="No workspaces yet" detail="Create one with `spky cloud tenants create`, or ask to be invited." />
              </Match>
            </Switch>
          </ConsoleChromeProvider>
        </Match>
      </Switch>
      <ConfirmHost />
      <ToastHost />
    </>
  );
}

/* ------------------------------------------------------------------ */
/* Signed out, loading                                                  */
/* ------------------------------------------------------------------ */

function Landing() {
  const error = new URLSearchParams(window.location.search).get('signin_error');
  return (
    <div class="cx-landing">
      <div class="cx-landing-card">
        <Logo />
        <h1>Sign in to the console</h1>
        <p>Your workspaces and projects, live: deployments, sync, jobs and the rest of every Sp00ky Cloud project you are a member of.</p>
        <Show when={error}>
          <div class="fx-log bad"><span class="fx-what">{error}</span></div>
        </Show>
        <button class="btn btn-primary" onClick={() => signIn()}>Sign in with GitHub</button>
      </div>
    </div>
  );
}

function Splash(props: { text: string; detail?: string }) {
  return (
    <div class="cx-landing">
      <div class="cx-landing-card">
        <Logo />
        <h1>{props.text}</h1>
        <Show when={props.detail}><p>{props.detail}</p></Show>
        <Show when={props.text === 'Nothing here'}>
          <a class="btn" href="/">Back to the console</a>
        </Show>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* A project: grant, session, dashboard                                 */
/* ------------------------------------------------------------------ */

/** Why the project's own dashboard cannot be shown; the cloud's pages still can. */
class Limited extends Error {}

const RENEW_MARGIN_S = 120;

function ProjectView(props: { workspace: Workspace; project: WorkspaceProject; setLimited: (v: boolean) => void }) {
  type State =
    | { kind: 'connecting' }
    | { kind: 'ready'; session: LoginResponse }
    | { kind: 'limited'; reason: string };
  const [state, setState] = createSignal<State>({ kind: 'connecting' });
  let token: string | null = null;
  let adminUrl = '';
  let renewTimer: ReturnType<typeof setTimeout> | undefined;
  const store = { get: () => token, set: (t: string | null) => (token = t) };
  const base = `/${props.workspace.slug}/${props.project.slug}`;
  localStorage.setItem(LAST_KEY, base);

  const exchange = async (fresh?: Grant): Promise<LoginResponse> => {
    const g = fresh ?? (await consoleApi.grant(props.project.id));
    adminUrl = g.admin_url;
    const res = await fetch(`${g.admin_url}/admin/api/session`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grant: g.grant }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new Limited(body.error ?? `The project refused the sign-in (${res.status})`);
    const login = body as LoginResponse;
    token = login.token;
    clearTimeout(renewTimer);
    renewTimer = setTimeout(() => void renew(), Math.max(30, login.expires_in_secs - RENEW_MARGIN_S) * 1000);
    return login;
  };

  /** A fresh session before this one ends; null when the project refuses. */
  const renew = async (): Promise<string | null> => {
    try {
      await exchange();
      return token;
    } catch {
      return null;
    }
  };

  onMount(async () => {
    try {
      const g = await consoleApi.grant(props.project.id);
      const config = await fetchConfig(g.admin_url).catch(() => null);
      if (!config) throw new Limited(`The project's admin API (${new URL(g.admin_url).host}) is not answering.`);
      if (!config.cloud_login) {
        throw new Limited(
          g.admin === 'ssp'
            ? "This Cloudflare project's SSP does not take console sign-in yet, so its live dashboard cannot open here. Deployments and Settings below work as usual."
            : 'This project runs a scheduler from before the console, so its live dashboard cannot open here yet. Upgrade it with `spky restart --upgrade` (or redeploy) and reload.',
        );
      }
      setConsoleTarget(g.admin_url, config, store);
      const login = await exchange(g);
      setState({ kind: 'ready', session: login });
      props.setLimited(false);
    } catch (err) {
      setState({ kind: 'limited', reason: err instanceof Error ? err.message : 'The project could not be opened.' });
      props.setLimited(true);
    }
  });

  // A request the project refused for an expired session: mint a new one.
  onCleanup(onUnauthorized(() => void renew()));
  onCleanup(() => {
    clearTimeout(renewTimer);
    token = null;
  });

  const cloudRoutes = (): JSX.Element => (
    <>
      <Route path="/deployments" component={() => <Deployments workspace={props.workspace} project={props.project} />} />
      <Route path="/settings/:tab?" component={() => <Settings workspace={props.workspace} project={props.project} />} />
    </>
  );

  return (
    <Switch>
      <Match when={state().kind === 'connecting'}>
        <Splash text={`Opening ${props.project.slug}`} />
      </Match>
      <Match when={state().kind === 'ready' && state()}>
        {(s) => (
          <ProjectApp
            session={(s() as { session: LoginResponse }).session}
            base={base}
            scope={`${props.project.id}@${adminUrl}`}
            renew={renew}
            onSignOut={() => undefined}
            routes={cloudRoutes()}
          />
        )}
      </Match>
      <Match when={state().kind === 'limited' && state()}>
        {(s) => <CloudOnly base={base} reason={(s() as { reason: string }).reason} routes={cloudRoutes()} />}
      </Match>
    </Switch>
  );
}

/** The cloud's own pages for a project whose dashboard cannot open. */
function CloudOnly(props: { base: string; reason: string; routes: JSX.Element }) {
  const session: LoginResponse = { token: '', subject: '', label: '', mode: 'cloud' as LoginResponse['mode'], expires_in_secs: 0 };
  const Notice = () => (
    <>
      <PageHead title="Overview" />
      <div class="fx-page">
        <section class="fx-row">
          <div class="fx-cell">
            <div class="fx-log"><span class="fx-what">{props.reason}</span></div>
            <p class="cx-note">Deployments and Settings come from Sp00ky Cloud and work as usual.</p>
          </div>
        </section>
      </div>
    </>
  );
  return (
    <Router base={props.base} root={(r) => <Shell session={session} overview={undefined} onSignOut={() => undefined}>{r.children}</Shell>}>
      <Route path="/" component={Notice} />
      {props.routes}
      <Route path="*" component={Notice} />
    </Router>
  );
}

/* ------------------------------------------------------------------ */
/* A workspace                                                          */
/* ------------------------------------------------------------------ */

const WORKSPACE_LINKS: { href: string; label: string; icon: string }[] = [
  { href: '/', label: 'Projects', icon: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z' },
  { href: '/members', label: 'Members', icon: 'M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z M2 21v-1a7 7 0 0 1 14 0v1 M16 3.5a4 4 0 0 1 0 7.5 M22 21v-1a7 7 0 0 0-4-6.3' },
  { href: '/billing', label: 'Billing & usage', icon: 'M3 6h18v12H3z M3 10h18 M7 15h4' },
  { href: '/vault', label: 'Vault', icon: 'M5 10h14v11H5z M8 10V6a4 4 0 0 1 8 0v4' },
  { href: '/keys', label: 'API keys', icon: 'M14 4a6 6 0 1 0 3 11.2L20 18v3h-3v-2h-2v-2l-1.2-1.2A6 6 0 0 0 14 4z' },
  { href: '/activity', label: 'Activity', icon: 'M3 12h4l3-8 4 16 3-8h4' },
  { href: '/sessions', label: 'Sessions', icon: 'M3 5h18v11H3z M8 20h8 M12 16v4' },
];

function WorkspaceView(props: { workspace: Workspace }) {
  return (
    <Router
      base={`/${props.workspace.slug}/-`}
      root={(r) => (
        <div class="shell settled">
          <nav class="sidebar">
            <div class="sb-me cx-brand"><Logo /><span class="cap">console</span></div>
            <div class="nav">
              <div class="nav-section">
                <div class="nav-heading">{props.workspace.name}</div>
                <For each={WORKSPACE_LINKS}>
                  {(l) => (
                    <A href={l.href} class="nav-link" activeClass="active" end={l.href === '/'}>
                      <span class="row" style={{ gap: '10px' }}>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d={l.icon} /></svg>
                        <span>{l.label}</span>
                      </span>
                    </A>
                  )}
                </For>
              </div>
            </div>
            <div class="sb-foot cap">console</div>
          </nav>
          <div class="main-frame">
            <main class="main">
              <ErrorBoundary fallback={(e) => <div class="fx-page"><div class="fx-log bad"><span class="fx-what">{String(e?.message ?? e)}</span></div></div>}>
                {r.children}
              </ErrorBoundary>
            </main>
          </div>
        </div>
      )}
    >
      <Route path="/" component={() => <Projects workspace={props.workspace} />} />
      <Route path="/members" component={() => <Members workspace={props.workspace} />} />
      <Route path="/billing" component={() => <Billing workspace={props.workspace} />} />
      <Route path="/vault" component={() => <Vault workspace={props.workspace} />} />
      <Route path="/keys" component={() => <Keys />} />
      <Route path="/activity" component={() => <Activity workspace={props.workspace} />} />
      <Route path="/sessions" component={() => <Sessions />} />
      <Route path="*" component={() => <Projects workspace={props.workspace} />} />
    </Router>
  );
}
