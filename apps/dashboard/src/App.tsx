import { Show, createSignal, onCleanup, onMount } from 'solid-js';
import { api, currentMode, getToken, logout, onUnauthorized, resolveMode } from './api/client';
import type { LoginResponse } from './api/types';
import { readStash, writeStash } from './lib/stash';
import { ConfirmHost, ToastHost } from './components/Actions';
import { Login } from './routes/Login';
import { ProjectApp, wipeLiveCache } from './ProjectApp';
import { FrameSkeleton } from './components/Shell';

/**
 * The scheduler-hosted dashboard (and its standalone build): sign in to one
 * scheduler with the roster or the break-glass password, then the project's
 * dashboard. The Sp00ky Cloud console is `console/ConsoleApp.tsx`.
 */
export function App() {
  const [ready, setReady] = createSignal(false);
  const [session, setSession] = createSignal<LoginResponse | null>(null);

  type Me = { subject: string; label: string; mode: LoginResponse['mode'] };

  const checkSession = async (token: string): Promise<boolean> => {
    try {
      const me = await api.get<Me>('/me');
      writeStash('me', me);
      setSession({ token, subject: me.subject, label: me.label, mode: me.mode, expires_in_secs: 0 });
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
    // session in the stash, the shell renders immediately (the overview
    // paints from its own stash), and the `/me` round trip only ever takes it
    // away. Before this the whole app was blank for one round trip on every open.
    const token = getToken();
    if (token) {
      const me = readStash<Me>('me');
      if (me) {
        setSession({ token, subject: me.value.subject, label: me.value.label, mode: me.value.mode, expires_in_secs: 0 });
        setReady(true);
      }
      if (!(await checkSession(token))) setSession(null);
    }
    setReady(true);
  });

  // The server rejecting our token is the authority on being signed out, and
  // a signed-out browser keeps nothing it cached.
  onCleanup(
    onUnauthorized(() => {
      setSession(null);
      void wipeLiveCache();
    }),
  );

  const signOut = async () => {
    await logout();
    setSession(null);
    await wipeLiveCache();
  };

  return (
    <>
      {/* Signed in before, the frame paints at once; else the sign-in page's ground. */}
      <Show when={ready()} fallback={getToken() ? <FrameSkeleton /> : <div class="li-wrap" />}>
        <Show when={session()} fallback={<Login onSignedIn={setSession} />}>
          {(s) => (
            <ProjectApp
              session={s()}
              // Embedded, the app is served under /admin; standalone (a dev
              // server or a static host) it sits at the root.
              base={currentMode().embedded ? '/admin' : ''}
              scope={currentMode().baseUrl || window.location.host}
              onSignOut={signOut}
            />
          )}
        </Show>
      </Show>
      <ConfirmHost />
      <ToastHost />
    </>
  );
}
