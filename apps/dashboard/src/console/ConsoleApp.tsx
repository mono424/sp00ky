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
import { A, Route, Router, useParams } from '@solidjs/router';
import { fetchConfig, onUnauthorized, setConsoleTarget } from '../api/client';
import type { LoginResponse } from '../api/types';
import { ConfirmHost, ToastHost } from '../components/Actions';
import { Logo, PageHead } from '../components/Chrome';
import { FrameSkeleton, PageSkeleton, Shell } from '../components/Shell';
import { ProjectApp } from '../ProjectApp';
import { SignInFrame } from '../routes/Login';
import { CloudError, consoleApi, rememberSignedIn, signInUrl, signOut, wasSignedIn, type Grant, type Workspace, type WorkspaceProject } from './cloud';
import { ConsoleChromeProvider, WORKSPACE_LINKS, type ConsoleChrome } from './chrome';
import { GitHubMark, Onboarding } from './onboarding';
import { Deployments, Settings } from './project';
import { Activity, Billing, Keys, Members, Projects, Sessions, Vault, WorkspaceSettings } from './workspace';
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
  createEffect(() => {
    if (me.state === 'ready') rememberSignedIn(me() !== null);
  });
  // A refresh that fails keeps what it had: a blip is not a reason to replace
  // the whole console with an error.
  const [workspaces, { refetch }] = createResource(
    () => me(),
    async (_me, info: { value: Workspace[] | undefined }): Promise<Workspace[]> => {
      try {
        return await consoleApi.workspaces();
      } catch (err) {
        if (info.value) return info.value;
        throw err;
      }
    },
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
  // What a page is keyed on: the minute's refetch brings new objects for the
  // same workspace and project, which must not remount the page (for a
  // project, a new session).
  const sameId = <T extends { id: string }>(a: T | undefined, b: T | undefined) => a?.id === b?.id;
  const workspaceKey = createMemo(workspace, undefined, { equals: sameId });
  const projectKey = createMemo(project, undefined, { equals: sameId });

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
  // A bare `/acme` is the workspace's projects; its router lives under `/acme/-`.
  createEffect(() => {
    const seg = path().split('/').filter(Boolean);
    if (seg.length === 1) go(`/${seg[0]}/-`, true);
  });

  const [limited, setLimited] = createSignal(false);
  const chrome: ConsoleChrome = {
    me: () => me() ?? undefined,
    workspaces: () => workspaces() ?? [],
    workspace,
    project,
    limited,
    go: (to) => go(to),
    refresh: () => Promise.resolve(refetch()),
    signOut: () => {
      lock();
      void signOut().then(() => window.location.assign('/'));
    },
  };

  return (
    <>
      <Switch fallback={wasSignedIn() ? <FrameSkeleton console /> : <div class="li-wrap" />}>
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
                <Show when={projectKey()} keyed>
                  {(p) => <ProjectView workspace={workspace()!} project={project() ?? p} setLimited={setLimited} />}
                </Show>
              </Match>
              <Match when={where().kind === 'workspace' && workspace()}>
                <Show when={workspaceKey()} keyed>
                  {(w) => <WorkspaceView workspace={workspace() ?? w} />}
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
    <SignInFrame docs="https://sp00ky.dev/docs/cloud/console" left={window.location.host} right="sp00ky cloud">
      <div class="li-eyebrow">Sp00ky Cloud</div>
      <h1 class="li-title">
        Sign in to
        <br />
        <span class="iri-text">your console</span>
      </h1>
      <div class="li-form">
        <Show when={error}>
          <div class="login-error li-error" role="alert">{error}</div>
        </Show>
        <a class="li-submit" href={signInUrl()}>
          <span class="li-submit-label li-social">
            <GitHubMark />
            Continue with GitHub
          </span>
          <span class="li-submit-arrow" aria-hidden="true">→</span>
        </a>
      </div>
      <div class="li-foot">
        <span>
          The same account as <span class="mono">spky login</span>. New here? This creates it.
        </span>
      </div>
    </SignInFrame>
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

/** The cloud's own pages run on the console cookie, not a project session. */
const CLOUD_SESSION: LoginResponse = { token: '', subject: '', label: '', mode: 'cloud', expires_in_secs: 0 };

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
        {/* The real frame while the session opens: switcher, rail, menu. */}
        <Router base={base} root={(r) => <Shell session={CLOUD_SESSION} overview={undefined} onSignOut={() => undefined}>{r.children}</Shell>}>
          <Route path="*rest" component={Opening} />
        </Router>
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

/** A project page while its session opens: its own head, a loading body. */
function Opening() {
  const params = useParams<{ rest?: string }>();
  const title = () => {
    const page = (params.rest ?? '').split('/')[0];
    if (!page) return 'Overview';
    return page === 'ssps' ? 'SSPs' : page[0].toUpperCase() + page.slice(1);
  };
  return <PageSkeleton head={<PageHead title={title()} />} />;
}

/** The cloud's own pages for a project whose dashboard cannot open. */
function CloudOnly(props: { base: string; reason: string; routes: JSX.Element }) {
  const Notice = () => (
    <>
      <PageHead title="Overview" />
      <div class="fx-page">
        <section class="fx-row">
          <div class="fx-cell cx-limited">
            <div class="fx-head"><span class="cap">Live dashboard</span><span class="fx-meta cap"><span class="warn">unavailable</span></span></div>
            <p class="cx-reason">{props.reason}</p>
            <div class="cx-actions">
              <A class="btn btn-sm" href="/deployments">Deployments</A>
              <A class="btn btn-sm" href="/settings">Settings</A>
            </div>
          </div>
        </section>
      </div>
    </>
  );
  return (
    <Router base={props.base} root={(r) => <Shell session={CLOUD_SESSION} overview={undefined} onSignOut={() => undefined}>{r.children}</Shell>}>
      <Route path="/" component={Notice} />
      {props.routes}
      <Route path="*" component={Notice} />
    </Router>
  );
}

/* ------------------------------------------------------------------ */
/* A workspace                                                          */
/* ------------------------------------------------------------------ */

function WorkspaceView(props: { workspace: Workspace }) {
  return (
    <Router
      base={`/${props.workspace.slug}/-`}
      root={(r) => (
        <Shell session={CLOUD_SESSION} overview={undefined} onSignOut={() => undefined} nav={{ heading: props.workspace.name, links: WORKSPACE_LINKS }}>
          <ErrorBoundary fallback={(e) => <div class="fx-page"><div class="fx-log bad"><span class="fx-what">{String(e?.message ?? e)}</span></div></div>}>
            {r.children}
          </ErrorBoundary>
        </Shell>
      )}
    >
      <Route path="/" component={() => <Projects workspace={props.workspace} />} />
      <Route path="/new" component={() => <Onboarding workspace={props.workspace} />} />
      <Route path="/members" component={() => <Members workspace={props.workspace} />} />
      <Route path="/billing" component={() => <Billing workspace={props.workspace} />} />
      <Route path="/vault" component={() => <Vault workspace={props.workspace} />} />
      <Route path="/keys" component={() => <Keys />} />
      <Route path="/activity" component={() => <Activity workspace={props.workspace} />} />
      <Route path="/sessions" component={() => <Sessions />} />
      <Route path="/settings" component={() => <WorkspaceSettings workspace={props.workspace} />} />
      <Route path="*" component={() => <Projects workspace={props.workspace} />} />
    </Router>
  );
}
