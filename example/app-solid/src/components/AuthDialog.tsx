import { createSignal, onMount, Show } from 'solid-js';
import { useAuth } from '../lib/auth';
import { createHotkey } from '../lib/keyboard';
import { Tooltip } from './Tooltip';

export function AuthDialog(props: { onClose: () => void; initialMode: 'signin' | 'signup' }) {
  const auth = useAuth();
  const [isSignUp, setIsSignUp] = createSignal(props.initialMode === 'signup');
  const [username, setUsername] = createSignal('');
  const [password, setPassword] = createSignal('');
  const [error, setError] = createSignal('');
  const [isLoading, setIsLoading] = createSignal(false);
  let usernameInput: HTMLInputElement | undefined;
  onMount(() => usernameInput?.focus());

  createHotkey('Escape', () => props.onClose(), { ignoreInputs: false });

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);
    try {
      await (isSignUp() ? auth.signUp : auth.signIn)(username(), password());
      props.onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setIsLoading(false);
    }
  };

  return (
    <div class="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-[100] p-4" onMouseDown={() => props.onClose()}>
      <div
        class="animate-slide-up bg-surface border border-white/[0.06] rounded-xl w-full max-w-md shadow-2xl"
        onMouseDown={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div class="flex justify-between items-center px-6 pt-6 pb-2">
          <h2 class="text-lg font-semibold">
            {isSignUp() ? 'Create account' : 'Sign in'}
          </h2>
          <Tooltip text="Close" kbd="Esc">
            <button
              onMouseDown={() => props.onClose()}
              class="text-zinc-500 hover:text-white transition-colors duration-150 p-1"
              aria-label="Close"
            >
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </Tooltip>
        </div>

        {/* Form */}
        <div class="px-6 pb-6 pt-4">
          <form onSubmit={handleSubmit} class="space-y-4">
            <div>
              <label for="username" class="block text-sm font-medium text-zinc-400 mb-1.5">
                Username
              </label>
              <input
                ref={usernameInput}
                id="username"
                type="text"
                value={username()}
                onInput={(e) => setUsername(e.currentTarget.value)}
                required
                class="w-full bg-zinc-950 border border-white/[0.06] rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-zinc-600 transition-colors duration-150 placeholder-zinc-600 text-sm"
                placeholder="Enter username"
                autocomplete="username"
              />
            </div>

            <div>
              <label for="password" class="block text-sm font-medium text-zinc-400 mb-1.5">
                Password
              </label>
              <input
                id="password"
                type="password"
                value={password()}
                onInput={(e) => setPassword(e.currentTarget.value)}
                required
                class="w-full bg-zinc-950 border border-white/[0.06] rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-zinc-600 transition-colors duration-150 placeholder-zinc-600 text-sm"
                placeholder="Enter password"
                autocomplete={isSignUp() ? 'new-password' : 'current-password'}
              />
            </div>

            <Show when={error()}>
              <div class="bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 p-3 text-sm">
                {error()}
              </div>
            </Show>

            <button
              type="submit"
              disabled={isLoading()}
              class="w-full bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white py-2.5 px-4 rounded-lg font-medium transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
            >
              {isLoading() ? 'Loading...' : isSignUp() ? 'Create account' : 'Sign in'}
            </button>
          </form>

          <div class="mt-6 text-center text-sm text-zinc-500">
            <button
              onMouseDown={() => setIsSignUp(!isSignUp())}
              class="hover:text-white transition-colors duration-150"
            >
              {isSignUp() ? 'Already have an account? Sign in' : "Don't have an account? Sign up"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
