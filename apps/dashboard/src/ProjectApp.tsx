import { Show, createContext, createEffect, createMemo, createSignal, onCleanup, untrack, useContext, type Accessor, type JSX } from 'solid-js';
import { Route, Router } from '@solidjs/router';
import { api, currentMode, getToken, restarting, signalUnauthorized } from './api/client';
import { LiveProvider, createLiveStack, destroyLiveStack, liveOverview, type LiveStack } from './api/liveStack';
import type { LoginResponse, Overview as OverviewData } from './api/types';
import { Shell } from './components/Shell';
import { readStash, writeStash } from './lib/stash';
import { Overview } from './routes/Overview';
import { Ssps } from './routes/Ssps';
import { Backends, BackendDetailView } from './routes/Backends';
import { Runs, RunsOpen, RunsRedirect } from './routes/Runs';
import { Backups } from './routes/Backups';
import { Logs } from './routes/Logs';
import { ViewDetail, Views } from './routes/Views';
import { Incidents, IncidentDetail } from './routes/Incidents';
import { Access } from './routes/Access';
import { Resources } from './routes/Resources';

/** How often the overview is polled when the host has no live socket (or a
 *  proxy blocks it). Fast enough to feel live, slow enough that a wall
 *  display is not a load source. */
const OVERVIEW_INTERVAL = 3000;

/** The live overview changes every second; the paint-first stash only needs
 *  a recent frame. */
const STASH_EVERY = 10_000;

const OverviewContext = createContext<Accessor<OverviewData | undefined>>(() => undefined);

/** The project's overview (live or polled), for pages the routes do not pass it to. */
export function useOverview(): Accessor<OverviewData | undefined> {
  return useContext(OverviewContext);
}

/**
 * One project's dashboard: the overview (live socket, or a poll when the host
 * has none), the shell and every route. Mounted by the scheduler-hosted app
 * once someone signed in, and by the console once a grant opened a session.
 */
export function ProjectApp(props: {
  session: LoginResponse;
  /** Router base: '/admin' embedded, '' standalone, '/acme/whitepawn' in the console. */
  base: string;
  onSignOut: () => void;
  /** Keys the live cache by host. */
  scope: string;
  /** A fresh token before the session expires (the console re-mints one). */
  renew?: () => Promise<string | null>;
  /** Routes only this host has (the console's Deployments and Settings). */
  routes?: JSX.Element;
}) {
  const [overview, setOverview] = createSignal<OverviewData | undefined>();
  const [overviewError, setOverviewError] = createSignal<string | undefined>();
  /** True until the first live overview replaces what the stash painted. */
  const [stale, setStale] = createSignal(false);

  // Paint the last overview this host showed at once; the live state or the
  // first poll replaces it.
  const cached = readStash<OverviewData>('overview');
  if (cached) {
    setOverview(cached.value);
    setStale(true);
  }

  // The live stack: one socket, the topic collections and their cache. Built
  // for hosts that speak the live protocol; leaving disposes it, and signing
  // out (see the scheduler-hosted App) wipes the cache, which holds operator data.
  const [live, setLive] = createSignal<LiveStack | null>(null);
  createEffect(() => {
    const mode = untrack(currentMode);
    if (!mode.config?.live) return;
    let stack: LiveStack | null = null;
    let gone = false;
    void createLiveStack({
      baseUrl: mode.baseUrl,
      token: getToken,
      renew: props.renew,
      onUnauthorized: signalUnauthorized,
      scope: props.scope,
    }).then((made) => {
      if (gone) return void made.dispose();
      stack = made;
      setLive(made);
    });
    onCleanup(() => {
      gone = true;
      setLive(null);
      void stack?.dispose();
    });
  });

  /** True while the socket carries the overview; false means poll. */
  const liveCarries = () => {
    const st = live();
    return !!st && st.client.status() !== 'blocked';
  };

  const liveData = createMemo(() => {
    const st = live();
    return st ? liveOverview(st.topics) : undefined;
  });
  let stashedAt = 0;
  createEffect(() => {
    const next = liveData()?.();
    if (!next) return;
    setOverview(next);
    setStale(false);
    setOverviewError(undefined);
    if (Date.now() - stashedAt > STASH_EVERY) {
      stashedAt = Date.now();
      writeStash('overview', next);
    }
  });

  // One poll for the whole app when there is no socket: the shell's nav
  // counts, the overview, the SSP list and the log source picker read from it.
  const poll = async () => {
    if (restarting() || liveCarries()) return;
    try {
      const next = await api.get<OverviewData>('/overview');
      setOverview(next);
      setStale(false);
      writeStash('overview', next);
      setOverviewError(undefined);
    } catch (err) {
      setOverviewError(err instanceof Error ? err.message : 'Could not reach the scheduler');
    }
  };
  const timer = setInterval(poll, OVERVIEW_INTERVAL);
  onCleanup(() => clearInterval(timer));
  void poll();

  // A socket that never got through: the poll takes over at once.
  createEffect(() => {
    if (live() && !liveCarries()) void poll();
  });

  // Coming back from a restart: the token may or may not still be good, and
  // the overview is certainly stale. Ask, then poll.
  createEffect((was: string | null) => {
    const now = restarting();
    if (was && !now) void api.get('/me').then(poll, () => undefined);
    return now;
  }, null as string | null);

  return (
    <Show when={!restarting()} fallback={<Reconnecting message={restarting()!} />}>
      <LiveProvider value={live}>
        <OverviewContext.Provider value={overview}>
        <Router
          base={props.base}
          root={(r) => (
            <Shell session={props.session} overview={overview()} onSignOut={props.onSignOut}>
              {r.children}
            </Shell>
          )}
        >
          <Route path="/" component={() => <Overview data={overview()} error={overviewError()} refresh={poll} />} />
          <Route path="/ssps" component={() => <Ssps data={overview()} refresh={poll} />} />
          <Route path="/backends" component={() => <Backends overview={overview()} />} />
          <Route path="/backends/:name" component={() => <BackendDetailView overview={overview()} />} />
          {/* Schedules, workflows and jobs are one page now; their old
              addresses land on it, filtered or with the thing open. */}
          <Route path="/runs" component={Runs} />
          <Route path="/jobs" component={() => <RunsRedirect kind="job" />} />
          <Route path="/jobs/:id" component={() => <RunsOpen kind="job" />} />
          <Route path="/workflows" component={() => <RunsRedirect kind="workflow" />} />
          <Route path="/workflows/:id" component={() => <RunsOpen kind="run" />} />
          <Route path="/schedules" component={() => <RunsRedirect />} />
          <Route path="/schedules/:name" component={() => <RunsOpen kind="schedule" />} />
          <Route path="/backups" component={() => <Backups overview={overview()} refresh={poll} />} />
          <Route path="/views" component={Views} />
          <Route path="/views/:key" component={ViewDetail} />
          <Route path="/logs" component={() => <Logs overview={overview()} />} />
          <Route path="/resources" component={Resources} />
          <Route path="/incidents" component={Incidents} />
          <Route path="/incidents/:id" component={IncidentDetail} />
          <Route path="/access" component={Access} />
          {props.routes}
          <Route path="*" component={() => <Overview data={overview()} stale={stale()} refresh={poll} />} />
        </Router>
        </OverviewContext.Provider>
      </LiveProvider>
    </Show>
  );
}

/**
 * Shown while a restart the operator asked for is in flight. The route is
 * left untouched underneath: when the scheduler answers again the router
 * remounts on the same URL.
 */
function Reconnecting(props: { message: string }) {
  return (
    <div class="reconnect">
      <div class="reconnect-card">
        <div class="reconnect-title">Reconnecting</div>
        <div class="reconnect-msg">{props.message}</div>
        <div class="bar">
          <div class="bar-fill indeterminate" />
        </div>
      </div>
    </div>
  );
}

/** Sign-out for the scheduler-hosted app: forget everything cached. */
export function wipeLiveCache(): Promise<void> {
  return destroyLiveStack(null);
}
