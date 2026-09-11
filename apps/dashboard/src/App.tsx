import { Show, createEffect, createSignal, onCleanup, onMount } from 'solid-js';
import { Route, Router } from '@solidjs/router';
import {
  api,
  currentMode,
  getToken,
  logout,
  onUnauthorized,
  resolveMode,
  restarting,
} from './api/client';
import type { LoginResponse, Overview as OverviewData } from './api/types';
import { Shell } from './components/Shell';
import { ConfirmHost, ToastHost } from './components/Actions';
import { Login } from './routes/Login';
import { Overview } from './routes/Overview';
import { Ssps } from './routes/Ssps';
import { Backends, BackendDetailView } from './routes/Backends';
import { Workflows } from './routes/Workflows';
import { WorkflowDetail } from './routes/WorkflowDetail';
import { ScheduleDetail, Schedules } from './routes/Schedules';
import { JobDetailView, Jobs } from './routes/Jobs';
import { Backups } from './routes/Backups';
import { Logs } from './routes/Logs';
import { ViewDetail, Views } from './routes/Views';
import { Access } from './routes/Access';

/** How often the overview is refreshed. Fast enough to feel live, slow enough
 *  that a wall display is not a load source. */
const OVERVIEW_INTERVAL = 3000;

export function App() {
  const [ready, setReady] = createSignal(false);
  const [session, setSession] = createSignal<LoginResponse | null>(null);
  const [overview, setOverview] = createSignal<OverviewData | undefined>();
  const [overviewError, setOverviewError] = createSignal<string | undefined>();

  const checkSession = async (token: string): Promise<boolean> => {
    try {
      const me = await api.get<{
        subject: string;
        label: string;
        mode: LoginResponse['mode'];
      }>('/me');
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
    const token = getToken();
    if (token) await checkSession(token);
    setReady(true);
  });

  // The server rejecting our token is the authority on being signed out.
  onCleanup(onUnauthorized(() => setSession(null)));

  // One poll for the whole app: the shell's nav counts, the overview, the SSP
  // list, the activity strip and the log source picker all read from it.
  const poll = async () => {
    if (!session() || restarting()) return;
    try {
      setOverview(await api.get<OverviewData>('/overview'));
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

  return (
    <>
      <Show when={ready()} fallback={<div class="login-wrap" />}>
        <Show when={!restarting()} fallback={<Reconnecting message={restarting()!} />}>
          <Show when={session()} fallback={<Login onSignedIn={signIn} />}>
            {(s) => (
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
                <Route path="/backends" component={Backends} />
                <Route path="/backends/:name" component={BackendDetailView} />
                <Route path="/workflows" component={Workflows} />
                <Route path="/workflows/:id" component={WorkflowDetail} />
                <Route path="/schedules" component={Schedules} />
                <Route path="/schedules/:name" component={ScheduleDetail} />
                <Route path="/jobs" component={Jobs} />
                <Route path="/jobs/:id" component={JobDetailView} />
                <Route
                  path="/backups"
                  component={() => <Backups overview={overview()} refresh={poll} />}
                />
                <Route path="/views" component={Views} />
                <Route path="/views/:key" component={ViewDetail} />
                <Route path="/logs" component={() => <Logs overview={overview()} />} />
                <Route path="/access" component={Access} />
                <Route path="*" component={() => <Overview data={overview()} refresh={poll} />} />
              </Router>
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
