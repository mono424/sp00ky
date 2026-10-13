import { Router, Route, A, useNavigate, type RouteSectionProps } from '@solidjs/router';
import { createSignal, Show } from 'solid-js';
import { Sp00kyProvider } from '@spooky-sync/client-solid';
import { AuthProvider, useAuth } from './lib/auth';
import { dbConfig } from './db';
import { AuthDialog } from './components/AuthDialog';
import { PendingMutationsIndicator } from './components/PendingMutationsIndicator';
import { createHotkey, createHotkeySequence, useShortcutsHelp } from './lib/keyboard';
import { ShortcutsHelp } from './components/ShortcutsHelp';

// Import routes
import Home from './routes/index';
import ThreadPage from './routes/thread/[id]';
import CreateThreadPage from './routes/create-thread';
import ProfilePage from './routes/profile';
import InvitePage from './routes/invite/[jwt]';
import { ProfilePicture } from './components/ProfilePicture';
import { Tooltip } from './components/Tooltip';
import { GhostMark, Wordmark, SP00KY_URL, SP00KY_REPO_URL } from './components/Brand';

function Layout(props: RouteSectionProps) {
  const auth = useAuth();
  const navigate = useNavigate();
  const { toggle: toggleHelp } = useShortcutsHelp();
  const [authDialog, setAuthDialog] = createSignal<'signin' | 'signup' | null>(null);

  createHotkey({ key: '/', shift: true }, () => toggleHelp());
  createHotkey('C', () => navigate('/create-thread'));
  createHotkeySequence(['G', 'H'], () => navigate('/'));
  createHotkeySequence(['G', 'P'], () => navigate('/profile'));


  return (
    <div class="min-h-screen bg-zinc-950 text-white font-sans selection:bg-zinc-700 selection:text-white flex flex-col">
      {/* Header */}
      <header class="bg-zinc-950/80 backdrop-blur-md border-b border-white/[0.06] sticky top-0 z-50 h-14">
        <div class="max-w-5xl mx-auto px-6 h-full">
          <div class="flex justify-between items-center h-full">
            <Tooltip text="Go home" kbd="g h">
              <a
                href="/"
                onClick={(e) => { e.preventDefault(); navigate('/'); }}
                class="text-xl hover:opacity-80 transition-opacity duration-150 cursor-pointer"
                aria-label="sp00ky threads home"
              >
                <Wordmark />
              </a>
            </Tooltip>

            <Show
              when={auth.userId()}
              fallback={
                <div class="flex items-center gap-3">
                  <button
                    onMouseDown={() => setAuthDialog('signin')}
                    class="text-sm text-zinc-400 hover:text-white transition-colors duration-150"
                  >
                    Sign in
                  </button>
                  <button
                    onMouseDown={() => setAuthDialog('signup')}
                    class="bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white text-sm font-medium px-4 py-1.5 rounded-lg transition-colors duration-150"
                  >
                    Sign up
                  </button>
                </div>
              }
            >
              <div class="flex items-center gap-3">
                <A
                  href="/profile"
                  class="flex items-center gap-2 hover:opacity-80 transition-opacity duration-150"
                >
                  <ProfilePicture
                    path={auth.user()?.profile_picture}
                    username={auth.user()?.username}
                  />
                </A>
                <button
                  onMouseDown={auth.signOut}
                  class="text-sm text-zinc-500 hover:text-white transition-colors duration-150"
                >
                  Sign out
                </button>
              </div>
            </Show>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main class="flex-grow">
        <Show
          when={auth.userId()}
          fallback={
            <div class="h-full flex items-center justify-center min-h-[80vh] px-6 py-12">
              <div class="text-center w-full max-w-md">
                {/* Logo mark */}
                <div class="w-16 h-16 rounded-2xl bg-zinc-900 border border-white/[0.06] flex items-center justify-center mx-auto mb-6">
                  <GhostMark class="w-8 h-8 text-white animate-ghost-float" />
                </div>

                <h2 class="text-3xl font-semibold mb-3 tracking-tight">Welcome to sp00ky threads</h2>
                <p class="text-zinc-400 text-sm mb-6 max-w-sm mx-auto leading-relaxed">
                  The live demo app for{' '}
                  <a href={SP00KY_URL} target="_blank" rel="noopener" class="text-zinc-200 hover:text-white underline decoration-white/20 underline-offset-2">
                    sp00ky
                  </a>
                  , local-first sync for SurrealDB. Threads, comments and every keystroke of the shared editor sync
                  live across tabs and devices, and keep working offline.
                </p>

                <ul class="flex flex-wrap justify-center gap-2 mb-8 text-xs text-zinc-400">
                  <li class="px-2.5 py-1 rounded-full border border-white/[0.06] bg-zinc-900">Real-time sync</li>
                  <li class="px-2.5 py-1 rounded-full border border-white/[0.06] bg-zinc-900">Offline-first</li>
                  <li class="px-2.5 py-1 rounded-full border border-white/[0.06] bg-zinc-900">Collaborative editing</li>
                </ul>

                <div class="flex flex-col gap-3 max-w-sm mx-auto">
                  <button
                    onMouseDown={() => setAuthDialog('signin')}
                    class="w-full bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white px-6 py-3 rounded-lg font-medium transition-colors duration-150"
                  >
                    Sign in
                  </button>
                  <button
                    onMouseDown={() => setAuthDialog('signup')}
                    class="w-full bg-surface hover:bg-surface-hover text-zinc-300 border border-white/[0.06] px-6 py-3 rounded-lg font-medium transition-colors duration-150"
                  >
                    Create account
                  </button>
                </div>

                <p class="mt-6 text-xs text-zinc-600 max-w-sm mx-auto leading-relaxed">
                  Accounts here are demo accounts for this app only. What you post is visible to other visitors, so
                  don't reuse a password or share anything private.
                </p>
              </div>
            </div>
          }
        >
          <div class="max-w-5xl mx-auto px-6 py-4 min-h-screen">{props.children}</div>
        </Show>
      </main>

      <footer class="border-t border-white/[0.06]">
        <div class="max-w-5xl mx-auto px-6 py-5 flex flex-wrap items-center justify-between gap-3 text-xs text-zinc-500">
          <span class="inline-flex items-center gap-2">
            <GhostMark class="w-3.5 h-3.5 text-zinc-400" />
            sp00ky threads is a demo of sp00ky, local-first sync for SurrealDB.
          </span>
          <span class="flex items-center gap-4">
            <a href={SP00KY_URL} target="_blank" rel="noopener" class="hover:text-white transition-colors duration-150">
              sp00ky.cloud
            </a>
            <a href={`${SP00KY_URL}/docs`} target="_blank" rel="noopener" class="hover:text-white transition-colors duration-150">
              Docs
            </a>
            <a href={SP00KY_REPO_URL} target="_blank" rel="noopener" class="hover:text-white transition-colors duration-150">
              GitHub
            </a>
          </span>
        </div>
      </footer>

      <Show when={!auth.userId() && authDialog()} keyed>
        {(mode) => <AuthDialog initialMode={mode} onClose={() => setAuthDialog(null)} />}
      </Show>

      <PendingMutationsIndicator />
      <ShortcutsHelp />
    </div>
  );
}

export default function App() {
  return (
    <Sp00kyProvider
      config={dbConfig}
      fallback={
        <div class="min-h-screen bg-zinc-950 text-white font-sans flex flex-col items-center justify-center gap-3">
          <GhostMark class="h-7 w-7 text-zinc-300 animate-ghost-float" />
          <span class="text-sm text-zinc-400">Connecting...</span>
        </div>
      }
      preload={(db) =>
        // Gate first-load UI on the thread list (+ authors) so Home paints from
        // cache instantly. Blocks only on the very first load; warm reloads
        // resolve immediately (nothing fetched — freshens when useQuery mounts).
        db.preload(db.query('thread').related('author').orderBy('title', 'asc').limit(10).build())
      }
      onError={(error) => console.error('Failed to initialize database:', error)}
      onReady={(db) => {
        // Expose the client for e2e diagnostics (e.g.
        // `liveRetryCount` regression in z-auth-bootstrap.spec.ts).
        // Harmless in prod and useful in the browser dev console too.
        (window as unknown as Record<string, unknown>).__sp00ky__ = db;
      }}
    >
      <AuthProvider>
        <Router root={Layout}>
          <Route path="/" component={Home} />
          <Route path="/thread/:id" component={ThreadPage} />
          <Route path="/create-thread" component={CreateThreadPage} />
          <Route path="/profile" component={ProfilePage} />
          <Route path="/invite/:jwt" component={InvitePage} />
        </Router>
      </AuthProvider>
    </Sp00kyProvider>
  );
}
