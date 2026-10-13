import { For, Show } from 'solid-js';
import { useNavigate } from '@solidjs/router';
import { Tooltip } from './Tooltip';

interface ThreadSidebarProps {
  activeThreadId?: string;
  onNavigate?: (direction: 1 | -1) => void;
  threads: ReadonlyArray<{ id: string; title?: string | null }>;
  isLoading: boolean;
}

export function ThreadSidebar(props: ThreadSidebarProps) {
  const navigate = useNavigate();

  const threads = () => props.threads;

  const handleThreadClick = (threadId: string) => {
    const id = threadId.split(':')[1];
    navigate(`/thread/${id}`);
  };

  const isActive = (threadId: string) => {
    const id = threadId.split(':')[1];
    return id === props.activeThreadId;
  };

  const currentIndex = () => {
    if (!props.activeThreadId) return -1;
    return threads().findIndex((t) => t.id.split(':')[1] === props.activeThreadId);
  };
  const canGoUp = () => currentIndex() > 0;
  const canGoDown = () => {
    const idx = currentIndex();
    return idx !== -1 && idx < threads().length - 1;
  };

  return (
    <aside class="w-60 flex-shrink-0 hidden md:block h-full border-r border-white/[0.06]">
      <div class="flex flex-col h-full">
        {/* Header */}
        <div class="flex items-center justify-between px-4 py-3 border-b border-white/[0.06]">
          <h3 class="text-xs font-medium text-zinc-500 tracking-wide">Threads</h3>
          <div class="flex items-center gap-0.5">
            <Tooltip text="Next" kbd="j">
              <button
                onMouseDown={() => canGoDown() && props.onNavigate?.(1)}
                disabled={!canGoDown()}
                class={`p-1 rounded transition-colors duration-150 ${canGoDown() ? 'text-zinc-400 hover:text-white hover:bg-white/[0.06]' : 'text-zinc-700 cursor-not-allowed'}`}
              >
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 9l6 6 6-6" /></svg>
              </button>
            </Tooltip>
            <Tooltip text="Previous" kbd="k">
              <button
                onMouseDown={() => canGoUp() && props.onNavigate?.(-1)}
                disabled={!canGoUp()}
                class={`p-1 rounded transition-colors duration-150 ${canGoUp() ? 'text-zinc-400 hover:text-white hover:bg-white/[0.06]' : 'text-zinc-700 cursor-not-allowed'}`}
              >
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M18 15l-6-6-6 6" /></svg>
              </button>
            </Tooltip>
          </div>
        </div>

        {/* Thread list */}
        <nav class="flex-1 min-h-0 overflow-y-auto py-2">
          <For
            each={threads()}
            fallback={
              <Show when={!props.isLoading} fallback={
                <div class="px-4 py-8 text-center">
                  <div class="text-xs text-zinc-600">Loading...</div>
                </div>
              }>
                <div class="px-4 py-8 text-center">
                  <div class="text-xs text-zinc-600">No threads</div>
                </div>
              </Show>
            }
          >
            {(thread) => (
              <button
                onMouseDown={() => handleThreadClick(thread.id)}
                class={`w-full text-left px-4 py-2.5 text-sm transition-colors duration-150 border-l-2 ${
                  isActive(thread.id)
                    ? 'border-zinc-500 text-white bg-surface/50'
                    : 'border-transparent text-zinc-500 hover:text-zinc-300 hover:bg-surface/50'
                }`}
              >
                <div class="truncate">
                  {thread.title || 'Untitled'}
                </div>
              </button>
            )}
          </For>
        </nav>

        {/* Footer — back link */}
        <div class="px-4 py-3 border-t border-white/[0.06]">
          <Tooltip text="Back to feed" kbd="Esc" position="top">
            <button
              onMouseDown={() => navigate('/')}
              class="inline-flex items-center gap-1.5 text-xs text-zinc-600 hover:text-zinc-300 transition-colors duration-150 w-full"
            >
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
              </svg>
              Back to feed
            </button>
          </Tooltip>
        </div>
      </div>
    </aside>
  );
}
