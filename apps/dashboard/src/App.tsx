import { Show, createEffect, createMemo, createSignal, onCleanup, onMount, untrack } from 'solid-js';
import { Route, Router } from '@solidjs/router';
import {
  api,
  currentMode,
  getToken,
  logout,
  onUnauthorized,
  resolveMode,
  restarting,
  signalUnauthorized,
} from './api/client';
import {
  LiveProvider,
  createLiveStack,
  destroyLiveStack,
  liveOverview,
  type LiveStack,
} from './api/liveStack';
import type { LoginResponse, Overview as OverviewData } from './api/types';
import { Shell } from './components/Shell';
import { readStash, writeStash } from './lib/stash';
import { ConfirmHost, ToastHost } from './components/Actions';
import { Login } from './routes/Login';
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

export function App() {
  const [ready, setReady] = createSignal(false);
  const [session, setSession] = createSignal<LoginResponse | null>(null);
  const [overview, setOverview] = createSignal<OverviewData | undefined>();
  const [overviewError, setOverviewError] = createSignal<string | undefined>();
  /** True until the first live overview replaces what the stash painted. */
  const [stale, setStale] = createSignal(false);

  type Me = { subject: string; label: string; mode: LoginResponse['mode'] };

  const checkSession = async (token: string): Promise<boolean> => {
    try {
      const me = await api.get<Me>('/me');
      writeStash('me', me);
      setSession({
        token,
        subject: me.subject,
        label: me.label,
        mode: me.mode,
        expires_in_secs: 0,
      });
      return true;
    } catch {
      return false;
    }
  };

  onMount(async () => {
    await resolveMode();

    // A token in storage may be from a previous scheduler process. With a
    // cluster secret set the scheduler signs tokens and they survive a
    // restart; without one they do not. Either way the server decides.
    //
    // It decides AFTER the first paint, though: with a token and the last
    // session + overview in the stash, the shell renders immediately from
    // that, marked stale, and the `/me` round trip only ever takes it away.
    // Before this the whole app was blank for one round trip on every open.
    const token = getToken();
    if (token) {
      const me = readStash<Me>('me');
      const cached = readStash<OverviewData>('overview');
      if (me && cached) {
        setSession({
          token,
          subject: me.value.subject,
          label: me.value.label,
          mode: me.value.mode,
          expires_in_secs: 0,
        });
        setOverview(cached.value);
        setStale(true);
        setReady(true);
      }
      const ok = await checkSession(token);
      if (!ok) {
        setSession(null);
        setOverview(undefined);
        setStale(false);
      }
    }
    setReady(true);
  });

  // The server rejecting our token is the authority on being signed out.
  onCleanup(onUnauthorized(() => setSession(null)));

  // The live stack: one socket, the topic collections and their cache. Built
  // while a session exists on a host that speaks the live protocol; signing
  // out disposes it and wipes the cache, which holds operator data.
  const [live, setLive] = createSignal<LiveStack | null>(null);
  createEffect(() => {
    const s = session();
    const mode = untrack(currentMode);
    if (!s || !mode.config?.live) return;
    let stack: LiveStack | null = null;
    let gone = false;
    void createLiveStack({
      baseUrl: mode.baseUrl,
      token: getToken,
      onUnauthorized: signalUnauthorized,
      scope: mode.baseUrl || window.location.host,
    }).then((made) => {
      if (gone) return void made.dispose();
      stack = made;
      setLive(made);
    });
    onCleanup(() => {
      gone = true;
      setLive(null);
      if (session()) void stack?.dispose();
      else void destroyLiveStack(stack);
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

  // One poll for the whole app: the shell's nav counts, the overview, the SSP
  // list, the activity strip and the log source picker all read from it.
  const poll = async () => {
    if (!session() || restarting() || liveCarries()) return;
    try {
      const next = await api.get<OverviewData>('/overview');
      setOverview(next);
      setStale(false);
      writeStash('overview', next);
      setOverviewError(undefined);
    } catch (err) {
      setOverviewError(
        err instanceof Error ? err.message : 'Could not reach the scheduler',
      );
    }
  };

  const timer = setInterval(poll, OVERVIEW_INTERVAL);
  onCleanup(() => clearInterval(timer));

  // Poll once as soon as a session exists, rather than waiting out the first
  // interval. Without this, a reload with a stored token leaves the nav counts
  // and the whole overview blank for OVERVIEW_INTERVAL.
  createEffect(() => {
    if (session()) void poll();
  });

  // Coming back from a restart: the token may or may not still be good, and
  // the overview is certainly stale. Ask, then poll.
  createEffect((was: string | null) => {
    const now = restarting();
    if (was && !now) {
      const token = getToken();
      if (token) void checkSession(token).then(poll);
    }
    return now;
  }, null as string | null);

  const signIn = (s: LoginResponse) => {
    setSession(s);
  };

  const signOut = async () => {
    await logout();
    setSession(null);
    setOverview(undefined);
  };

  // A socket that never got through: the poll takes over at once.
  createEffect(() => {
    if (live() && !liveCarries()) void poll();
  });

  return (
    <>
      <Show when={ready()} fallback={<div class="li-wrap" />}>
        <Show when={!restarting()} fallback={<Reconnecting message={restarting()!} />}>
          <Show when={session()} fallback={<Login onSignedIn={signIn} />}>
            {(s) => (
              <LiveProvider value={live}>
              <Router
                // Embedded, the app is served under /admin; standalone (a dev
                // server or a static host) it sits at the root.
                base={currentMode().embedded ? '/admin' : ''}
                root={(props) => (
                  <Shell
                    session={s()}
                    overview={overview()}
                    onSignOut={signOut}
                  >
                    {props.children}
                  </Shell>
                )}
              >
                <Route
                  path="/"
                  component={() => (
                    <Overview
                      data={overview()}
                      error={overviewError()}
                      refresh={poll}
                    />
                  )}
                />
                <Route
                  path="/ssps"
                  component={() => <Ssps data={overview()} refresh={poll} />}
                />
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
                <Route
                  path="/backups"
                  component={() => <Backups overview={overview()} refresh={poll} />}
                />
                <Route path="/views" component={Views} />
                <Route path="/views/:key" component={ViewDetail} />
                <Route path="/logs" component={() => <Logs overview={overview()} />} />
                <Route path="/resources" component={Resources} />
                <Route path="/incidents" component={Incidents} />
                <Route path="/incidents/:id" component={IncidentDetail} />
                <Route path="/access" component={Access} />
                <Route path="*" component={() => <Overview data={overview()} stale={stale()} refresh={poll} />} />
              </Router>
              </LiveProvider>
            )}
          </Show>
        </Show>
      </Show>
      <ConfirmHost />
      <ToastHost />
    </>
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
