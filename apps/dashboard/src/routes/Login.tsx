import { For, Show, createEffect, createMemo, createSignal, onCleanup, type JSX } from 'solid-js';
import { currentMode, login, setEndpoint } from '../api/client';
import type { LoginResponse } from '../api/types';
import { Logo } from '../components/Chrome';

/**
 * Sign-in.
 *
 * The endpoint field appears only when this bundle is NOT being served by a
 * scheduler. Embedded, the endpoint is where the page came from, and asking for it
 * again would be asking the operator to retype something we already know.
 */
export function Login(props: { onSignedIn: (session: LoginResponse) => void }) {
  const [endpoint, setEndpointValue] = createSignal(currentMode().baseUrl);
  const [username, setUsername] = createSignal('');
  const [password, setPassword] = createSignal('');
  const [breakglass, setBreakglass] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);
  const [config, setConfig] = createSignal(currentMode().config);

  const embedded = () => currentMode().embedded;

  // A standalone dashboard cannot know whether break-glass is offered until it
  // has reached a scheduler, so re-read the config whenever it changes.
  createEffect(() => {
    setConfig(currentMode().config);
  });

  const connect = async () => {
    setError(null);
    setBusy(true);
    try {
      setConfig(await setEndpoint(endpoint()));
    } catch {
      setError('Could not reach a scheduler at that address.');
      setConfig(null);
    } finally {
      setBusy(false);
    }
  };

  const submit = async (e: Event) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      // A standalone form where the operator typed an endpoint but never
      // pressed Connect should still just work.
      if (!embedded() && !config()) {
        setConfig(await setEndpoint(endpoint()));
      }
      const session = await login(
        password(),
        breakglass() ? undefined : username(),
      );
      props.onSignedIn(session);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Sign-in failed');
    } finally {
      setBusy(false);
    }
  };

  const name = () => config()?.project_slug ?? config()?.scheduler_id;

  return (
    <SignInFrame
      docs="https://sp00ky.dev/docs/reference/admin-dashboard"
      left={
        <Show when={config()} fallback="not connected">
          {(c) => (
            <>
              {c().scheduler_id}
              <span class="li-sep">/</span>
              {c().version.replace(/^0\.0\.1-/, '')}
            </>
          )}
        </Show>
      }
      right={config()?.cloud_linked ? 'sp00ky cloud' : 'self-hosted'}
    >
      <div class="li-eyebrow">
        {breakglass() ? 'Break-glass access' : 'Operator console'}
      </div>
      <h1 class="li-title">
        <Show when={name()} fallback={<>Connect to<br /><span class="iri-text">a scheduler</span></>}>
          {(n) => (
            <>
              Sign in to
              <br />
              <span class="iri-text">{n()}</span>
            </>
          )}
        </Show>
      </h1>
      <Show when={breakglass()}>
        <p class="li-lede">
          The shared admin password, for when no roster account can sign in. You act as
          Break-glass, not as yourself.
        </p>
      </Show>

      <form class="li-form" onSubmit={submit}>
        <Show when={!embedded()}>
          <div class="li-field">
            <label for="endpoint">Scheduler endpoint</label>
            <input
              id="endpoint"
              class="mono"
              placeholder="http://10.0.0.5:9668"
              value={endpoint()}
              onInput={(e) => setEndpointValue(e.currentTarget.value)}
              onBlur={connect}
              autocomplete="url"
            />
          </div>
        </Show>

        <Show when={!breakglass()}>
          <div class="li-field">
            <label for="username">Username</label>
            <input
              id="username"
              value={username()}
              onInput={(e) => setUsername(e.currentTarget.value)}
              autocomplete="username"
              autofocus
            />
          </div>
        </Show>

        <div class="li-field">
          <label for="password">{breakglass() ? 'Admin password' : 'Password'}</label>
          <input
            id="password"
            type="password"
            value={password()}
            onInput={(e) => setPassword(e.currentTarget.value)}
            autocomplete="current-password"
          />
        </div>

        <Show when={error()}>
          <div class="login-error li-error" role="alert">{error()}</div>
        </Show>

        <button class="li-submit" type="submit" disabled={busy()}>
          <span class="li-submit-label">{busy() ? 'Signing in…' : 'Sign in'}</span>
          <span class="li-submit-arrow" aria-hidden="true">→</span>
        </button>
      </form>

      <div class="li-foot">
        <Show when={!breakglass()}>
          <span>
            Get access with <span class="mono">spky admin add</span>
          </span>
        </Show>
        {/* Offered only when the scheduler says a break-glass password is
            configured, so the option never appears where it cannot work. */}
        <Show when={config()?.breakglass_available}>
          <button
            type="button"
            class="link-btn"
            onClick={() => {
              setBreakglass(!breakglass());
              setError(null);
            }}
          >
            {breakglass() ? 'Use an admin account' : 'Use the break-glass password'}
          </button>
        </Show>
      </div>
    </SignInFrame>
  );
}

/**
 * The page around signing in, here and on the Sp00ky Cloud console: the frame,
 * the rolling terminal, and whatever a host asks for in the column beside it.
 */
export function SignInFrame(props: { docs: string; left: JSX.Element; right: JSX.Element; children: JSX.Element }) {
  return (
    <div class="li-wrap">
      <div class="li-frame">
        <header class="li-top">
          <span class="li-brand">
            <Logo />
          </span>
          <a class="li-docs" href={props.docs} target="_blank" rel="noreferrer">
            Docs <span aria-hidden="true">↗</span>
          </a>
        </header>

        <main class="li-main">
          <section class="li-side">{props.children}</section>
          <Terminal />
        </main>

        <footer class="li-bottom mono">
          <span>{props.left}</span>
          <span>{props.right}</span>
        </footer>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* The terminal: real spky subcommands rolling past a fixed prompt     */
/* ------------------------------------------------------------------ */

const COMMANDS: [string, string][] = [
  ['deploy', 'ship the whole stack'],
  ['status', 'what runs where'],
  ['logs', 'tail every component'],
  ['jobs', 'the outbox queue, live'],
  ['workflows', 'runs, steps and retries'],
  ['schedules', 'cron, paused or firing'],
  ['backup', 'snapshot the database'],
  ['migrate', 'apply schema migrations'],
  ['dev', 'the whole stack, locally'],
];

/** How far ahead the queue of upcoming subcommands fades: the live line, then the next two. */
const FADE = [1, 0.2, 0.07];

const command = (k: number) => COMMANDS[((k % COMMANDS.length) + COMMANDS.length) % COMMANDS.length];

export function Terminal() {
  const [step, setStep] = createSignal(0);
  // The line just run (leaving upward), the live one, and what comes next.
  // Nothing behind it: the page opens on a command, not on a history.
  const rows = createMemo(() => Array.from({ length: FADE.length + 2 }, (_, i) => step() - 1 + i));

  // Every few seconds the queue moves up one line. The next subcommand is
  // already on screen, so it simply rises into place and brightens; nothing
  // is revealed that was not visible a moment ago. Still for an operator who
  // asked for less motion.
  if (!matchMedia('(prefers-reduced-motion: reduce)').matches) {
    const turn = window.setInterval(() => {
      if (!document.hidden) setStep((s) => s + 1);
    }, 2800);
    onCleanup(() => window.clearInterval(turn));
  }

  return (
    <aside class="li-art" aria-hidden="true">
      <div class="li-term mono">
        {/* A comment above the command, as a script would have it. Keyed by
            its text, so each new note mounts fresh and fades in. */}
        <For each={[command(step())[1]]}>{(note) => <div class="li-term-note"># {note}</div>}</For>
        <div class="li-term-line">
          {/* The prompt stands still; only the subcommand rolls. */}
          <span class="li-term-prompt">
            <span class="li-term-dollar">$</span> spky
          </span>
          <div class="li-term-slot">
            <div class="li-term-rows">
              <For each={rows()}>
                {(k) => {
                  const d = () => k - step();
                  return (
                    <div
                      class="li-term-row"
                      classList={{ active: d() === 0 }}
                      style={{
                        transform: `translateY(${(d() + 1) * 100}%)`,
                        opacity: d() < 0 ? 0 : FADE[d()] ?? 0,
                      }}
                    >
                      <span class="li-term-cmd">{command(k)[0]}</span>
                      <Show when={d() === 0}>
                        <span class="li-term-caret" />
                      </Show>
                    </div>
                  );
                }}
              </For>
            </div>
          </div>
        </div>
      </div>
    </aside>
  );
}
