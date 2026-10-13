import { For, Show, createSignal, createEffect, onCleanup } from 'solid-js';
import { useNavigate } from '@solidjs/router';
import { useQuery, useDb, createPreload } from '@spooky-sync/client-solid';
import { createHotkey } from '../lib/keyboard';
import type { schema } from '../schema.gen';
import { ProfilePicture } from './ProfilePicture';
import { ArrowDownAZ, ArrowUpZA, CalendarArrowDown, CalendarArrowUp, Plus } from 'lucide-solid';
import { Tooltip } from './Tooltip';

// Scroll the selected thread card into view
function scrollSelectedIntoView(index: number) {
  const el = document.querySelector(`[data-thread-index="${index}"]`);
  el?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

export function ThreadList() {
  const db = useDb<typeof schema>();
  const navigate = useNavigate();

  const [sort, setSort] = createSignal('a-z');
  const [sortOpen, setSortOpen] = createSignal(false);
  const [selectedIndex, setSelectedIndex] = createSignal(-1);

  // Close sort menu on outside click
  const handleClickOutside = (e: MouseEvent) => {
    const target = e.target as HTMLElement;
    if (!target.closest('[data-sort-menu]')) setSortOpen(false);
  };
  document.addEventListener('mousedown', handleClickOutside);
  onCleanup(() => document.removeEventListener('mousedown', handleClickOutside));

  const threadsResult = useQuery(() => {
    let q = db.query('thread').related('author');
    if (sort() === 'a-z') {
      q = q.orderBy('title', 'asc');
    } else if (sort() === 'z-a') {
      q = q.orderBy('title', 'desc');
    } else if (sort() === 'newest') {
      q = q.orderBy('created_at', 'desc');
    } else {
      q = q.orderBy('created_at', 'asc');
    }
    return q.limit(10).build();
  });

  const threads = () => threadsResult.data() || [];

  // Prewarm the home screen: snapshot every visible thread together with its
  // author and its 10 most-recent comments (each with their author) into the
  // local cache, mirroring the list's current sort so the same threads are
  // warmed. Opening any thread then paints its detail instantly from cache
  // instead of waiting on the network. Snapshot-only — no live view is
  // registered until the detail actually mounts.
  createPreload(db, () => {
    let q = db
      .query('thread')
      .related('author')
      .related('comments', (c) =>
        c.related('author').orderBy('created_at', 'desc').limit(10)
      );
    if (sort() === 'a-z') {
      q = q.orderBy('title', 'asc');
    } else if (sort() === 'z-a') {
      q = q.orderBy('title', 'desc');
    } else if (sort() === 'newest') {
      q = q.orderBy('created_at', 'desc');
    } else {
      q = q.orderBy('created_at', 'asc');
    }
    return q.limit(10).build();
  });

  // Reset selection when threads change
  createEffect(() => {
    threads();
    setSelectedIndex(-1);
  });

  const handleThreadClick = (threadId: string) => {
    navigate(`/thread/${threadId}`);
  };

  createHotkey('J', () => {
    const len = threads().length;
    if (len === 0) return;
    const next = Math.min(selectedIndex() + 1, len - 1);
    setSelectedIndex(next);
    scrollSelectedIntoView(next);
  });
  createHotkey('K', () => {
    const len = threads().length;
    if (len === 0) return;
    const prev = Math.max(selectedIndex() - 1, 0);
    setSelectedIndex(prev);
    scrollSelectedIntoView(prev);
  });
  createHotkey('Enter', () => {
    const idx = selectedIndex();
    const list = threads();
    if (idx >= 0 && idx < list.length) {
      handleThreadClick(list[idx].id.split(':')[1]);
    }
  });

  return (
    <div class="w-full">
      {/* Action Bar */}
      <div class="flex items-center mb-6 pt-2">
        <Tooltip text="New thread" kbd="c">
          <button
            onMouseDown={() => navigate('/create-thread')}
            class="inline-flex items-center gap-1.5 bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white px-3 py-1.5 rounded-lg text-xs font-medium transition-colors duration-150"
          >
            <Plus size={14} />
            New thread
          </button>
        </Tooltip>

        {/* Sort menu */}
        <div class="relative ml-auto" data-sort-menu>
          <Tooltip text="Sort" position="bottom">
          <button
            aria-label="Sort"
            onMouseDown={() => setSortOpen(!sortOpen())}
            class="inline-flex items-center justify-center w-8 h-8 text-zinc-500 hover:text-white rounded-lg transition-colors duration-150"
          >
            {sort() === 'a-z' ? (
              <ArrowDownAZ size={16} />
            ) : sort() === 'z-a' ? (
              <ArrowUpZA size={16} />
            ) : sort() === 'newest' ? (
              <CalendarArrowDown size={16} />
            ) : (
              <CalendarArrowUp size={16} />
            )}
          </button>
          </Tooltip>

          <Show when={sortOpen()}>
            <div class="absolute right-0 mt-1.5 w-44 bg-surface border border-white/[0.06] rounded-lg shadow-2xl z-50 py-1 animate-fade-in">
              <button
                onMouseDown={() => { setSort('a-z'); setSortOpen(false); }}
                class={`w-full flex items-center gap-2.5 px-3 py-2 text-sm transition-colors duration-150 ${sort() === 'a-z' ? 'text-white bg-surface-hover' : 'text-zinc-400 hover:text-white hover:bg-surface-hover'}`}
              >
                <ArrowDownAZ size={16} />
                A to Z
              </button>
              <button
                onMouseDown={() => { setSort('z-a'); setSortOpen(false); }}
                class={`w-full flex items-center gap-2.5 px-3 py-2 text-sm transition-colors duration-150 ${sort() === 'z-a' ? 'text-white bg-surface-hover' : 'text-zinc-400 hover:text-white hover:bg-surface-hover'}`}
              >
                <ArrowUpZA size={16} />
                Z to A
              </button>
              <button
                onMouseDown={() => { setSort('newest'); setSortOpen(false); }}
                class={`w-full flex items-center gap-2.5 px-3 py-2 text-sm transition-colors duration-150 ${sort() === 'newest' ? 'text-white bg-surface-hover' : 'text-zinc-400 hover:text-white hover:bg-surface-hover'}`}
              >
                <CalendarArrowDown size={16} />
                Newest
              </button>
              <button
                onMouseDown={() => { setSort('oldest'); setSortOpen(false); }}
                class={`w-full flex items-center gap-2.5 px-3 py-2 text-sm transition-colors duration-150 ${sort() === 'oldest' ? 'text-white bg-surface-hover' : 'text-zinc-400 hover:text-white hover:bg-surface-hover'}`}
              >
                <CalendarArrowUp size={16} />
                Oldest
              </button>
            </div>
          </Show>
        </div>
      </div>

      {/* Thread List */}
      <div class="space-y-3">
        <For
          each={threads()}
          fallback={
            <Show when={!threadsResult.isLoading()} fallback={
              <div class="bg-surface/50 rounded-xl border border-white/[0.06] py-16 text-center">
                <p class="text-zinc-500 text-sm">Loading threads...</p>
              </div>
            }>
              <div class="bg-surface/50 rounded-xl border border-white/[0.06] py-16 text-center">
                <p class="text-zinc-500 text-sm">No threads yet</p>
                <p class="text-zinc-600 text-xs mt-1">Create the first one to get started.</p>
              </div>
            </Show>
          }
        >
          {(thread, index) => {
            // Detail data (author + 10 comments) is prewarmed for the whole
            // home screen by the single `createPreload` above — no per-row
            // preload needed here.
            return (
            <div
              data-thread-index={index()}
              onMouseDown={() => handleThreadClick(thread.id.split(':')[1])}
              onMouseEnter={() => setSelectedIndex(index())}
              class={`border rounded-xl p-5 cursor-pointer transition-colors duration-150 group ${
                selectedIndex() === index()
                  ? 'bg-surface border-zinc-600 ring-1 ring-zinc-700'
                  : 'bg-surface/40 hover:bg-surface border-white/[0.06]'
              }`}
            >
              <div class="flex items-start gap-4">
                {/* Avatar */}
                <ProfilePicture
                  src={() => thread.author?.profile_picture}
                  username={() => thread.author?.username}
                  size="md"
                />

                <div class="flex-1 min-w-0">
                  {/* Author + time */}
                  <div class="flex items-center gap-2 mb-1">
                    <span class="text-sm font-medium text-zinc-300">
                      {thread.author?.username || 'Anonymous'}
                    </span>
                    <span class="text-zinc-700">&middot;</span>
                    <span class="text-xs text-zinc-600">
                      {new Date(thread.created_at ?? 0).toLocaleDateString(undefined, {
                        month: 'short',
                        day: '2-digit',
                        hour: '2-digit',
                        minute: '2-digit',
                      })}
                    </span>
                  </div>

                  {/* Title */}
                  <h2 class="text-[15px] font-semibold text-white group-hover:text-zinc-300 transition-colors duration-150 mb-1.5">
                    {thread.title || 'Untitled'}
                  </h2>

                  {/* Content preview: a `@crdt` column holds the document's
                      plain text, written by the server on every fold. */}
                  <p class="text-sm text-zinc-500 line-clamp-2 leading-relaxed">
                    {thread.content}
                  </p>
                </div>
              </div>
            </div>
            );
          }}
        </For>
      </div>
    </div>
  );
}
