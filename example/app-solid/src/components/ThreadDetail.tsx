import type { JSX } from 'solid-js';
import { createResource, createSignal, For, onCleanup, Show } from 'solid-js';
import { useNavigate, useParams } from '@solidjs/router';
import { useCrdtField, useDb, useQuery } from '@spooky-sync/client-solid';
import { Lock, MoreVertical } from 'lucide-solid';
import type { schema } from '../schema.gen';
import { useAuth } from '../lib/auth';
import { idString, toRecordId } from '../lib/ids';
import { createHotkey, isInputActive } from '../lib/keyboard';
import { CollaborativeEditor } from './CollaborativeEditor';
import { CommentBox } from './CommentBox';
import { CommentForm } from './CommentForm';
import { CrdtTextInput } from './CrdtTextInput';
import { ProfilePicture } from './ProfilePicture';
import { ShareDialog } from './ShareDialog';
import { ThreadSidebar } from './ThreadSidebar';
import { Tooltip } from './Tooltip';

interface Collaborator {
  /** The `collaborates_on` edge, what removing them deletes. */
  edge: string;
  id: string;
  username?: string;
  profile_picture?: string | null;
}

const focusReply = () => document.querySelector<HTMLTextAreaElement>('#comment-textarea')?.focus();

export function ThreadDetail() {
  const db = useDb<typeof schema>();
  const auth = useAuth();
  const params = useParams();
  const navigate = useNavigate();
  const [commentFilter, setCommentFilter] = createSignal<'all' | 'mine'>('all');
  const [shareOpen, setShareOpen] = createSignal(false);
  const [menuOpen, setMenuOpen] = createSignal(false);
  const me = () => idString(auth.user()?.id);

  // The thread with its author and newest comments, live.
  const threadResult = useQuery(() =>
    db
      .query('thread')
      .where({ id: `thread:${params.id}` })
      .related('author')
      .related('comments', (c) => {
        const newest = c.related('author').orderBy('created_at', 'desc');
        return commentFilter() === 'mine' ? newest.where({ author: me() }) : newest.limit(10);
      })
      .one()
      .build()
  );
  // useQuery keeps the previous thread's row until the next one arrives: show
  // nothing rather than the old thread under the new URL (j/k navigation).
  const thread = () => {
    const data = threadResult.data();
    return data && idString(data.id) === `thread:${params.id}` ? data : null;
  };

  // The sidebar list, which j/k step through.
  const threadsResult = useQuery(() => db.query('thread').orderBy('title', 'asc').limit(20).build());
  const threads = () => threadsResult.data() ?? [];

  // `title` (`@crdt text`) and `content` (`@crdt prosemirror @cursor`) are
  // edited through their Loro documents; the columns hold the server-written
  // plain text the lists show.
  const recordId = () => (thread() ? `thread:${params.id}` : undefined);
  const titleField = useCrdtField('thread', recordId, 'title');
  const contentField = useCrdtField('thread', recordId, 'content');

  // Who else may edit: the `collaborates_on` edges an accepted invite makes.
  // A relation table's `in`/`out` are not generated columns, so the query
  // builder cannot filter it; this one read goes to the server, once per
  // thread.
  const [collaborators, { mutate: setCollaborators }] = createResource(
    () => thread()?.id,
    async (id): Promise<Collaborator[]> => {
      try {
        const [rows] = await db.remoteQuery<[Array<{ id: unknown; user: Collaborator | null }>]>(
          'SELECT id, in.* AS user FROM collaborates_on WHERE out = $thread',
          { thread: toRecordId(id) }
        );
        return rows.flatMap((r) => (r.user ? [{ ...r.user, edge: idString(r.id), id: idString(r.user.id) }] : []));
      } catch (e) {
        console.error('[ThreadDetail] failed to load collaborators', e);
        return [];
      }
    },
    { initialValue: [] }
  );

  const isAuthor = () => !!me() && idString(thread()?.author?.id) === me();
  const canEdit = () => isAuthor() || collaborators().some((c) => c.id === me());

  const removeCollaborator = (edge: string) => {
    setCollaborators((list) => list.filter((c) => c.edge !== edge));
    void db.delete('collaborates_on', edge);
  };

  const deleteThread = async (id: string) => {
    if (!confirm('Delete this post? This cannot be undone.')) return;
    await db.delete('thread', id);
    navigate('/');
  };

  // j/k: the sidebar highlights the next row at once; the route follows once
  // the keys rest, so stepping fast does not mount an editor per row.
  const [pendingThreadId, setPendingThreadId] = createSignal<string | null>(null);
  let navTimer: ReturnType<typeof setTimeout> | undefined;
  const navigateToAdjacentThread = (direction: 1 | -1) => {
    const keys = threads().map((t) => t.id.split(':')[1]);
    const at = keys.indexOf(pendingThreadId() ?? params.id);
    const next = at === -1 ? undefined : keys[at + direction];
    if (!next) return;
    setPendingThreadId(next);
    clearTimeout(navTimer);
    navTimer = setTimeout(() => {
      setPendingThreadId(null);
      navigate(`/thread/${next}`);
    }, 200);
  };
  onCleanup(() => clearTimeout(navTimer));

  createHotkey('J', () => navigateToAdjacentThread(1));
  createHotkey('K', () => navigateToAdjacentThread(-1));
  createHotkey('R', focusReply);
  createHotkey(
    'Escape',
    () => (isInputActive() ? (document.activeElement as HTMLElement).blur() : navigate('/')),
    { ignoreInputs: false }
  );

  const closeMenuOutside = (e: MouseEvent) => {
    if (!(e.target as HTMLElement).closest('[data-thread-menu]')) setMenuOpen(false);
  };
  document.addEventListener('mousedown', closeMenuOutside);
  onCleanup(() => document.removeEventListener('mousedown', closeMenuOutside));

  return (
    <div class="fixed inset-0 top-14 z-40 bg-zinc-950">
      <div class="max-w-5xl mx-auto h-full flex">
        <ThreadSidebar
          activeThreadId={pendingThreadId() ?? params.id}
          onNavigate={navigateToAdjacentThread}
          threads={threads()}
          isLoading={threadsResult.isLoading()}
        />

        <div class="flex-1 overflow-y-auto">
          <div class="max-w-3xl mx-auto w-full px-6 py-6">
            <Show
              when={thread()}
              fallback={
                <div class="bg-surface/50 rounded-xl border border-white/[0.06] p-12 text-center">
                  {/* "Not found" is a verdict: only once the server has
                      answered and nothing is still fetching. */}
                  <Show
                    when={threadResult.isSettled() && !threadResult.error()}
                    fallback={
                      <Show
                        when={threadResult.error()}
                        fallback={<p class="text-zinc-400 font-medium mb-1">Loading thread...</p>}
                      >
                        <p class="text-zinc-400 font-medium mb-1">Couldn't load this thread</p>
                        <p class="text-sm text-zinc-600">Retrying in the background.</p>
                      </Show>
                    }
                  >
                    <p class="text-zinc-400 font-medium mb-1">Thread not found</p>
                    <p class="text-sm text-zinc-600">This thread may have been deleted or doesn't exist.</p>
                  </Show>
                </div>
              }
            >
              {(threadData) => (
                <div class="space-y-8">
                  <article>
                    {/* Author header */}
                    <div class="flex items-center gap-3 mb-5">
                      <ProfilePicture
                        path={threadData().author?.profile_picture}
                        username={threadData().author?.username}
                        size="md"
                      />
                      <div>
                        <div class="text-sm font-medium text-zinc-200">
                          {threadData().author?.username || 'Unknown'}
                        </div>
                        <div class="text-xs text-zinc-600">
                          {new Date(threadData().created_at ?? 0).toLocaleDateString(undefined, {
                            month: 'long',
                            day: 'numeric',
                            year: 'numeric',
                            hour: '2-digit',
                            minute: '2-digit',
                          })}
                        </div>
                      </div>
                      <div class="flex-1" />

                      <div class="flex -space-x-1.5">
                        <For each={collaborators()}>
                          {(c) => (
                            <div class="relative group">
                              <Tooltip text={c.username || 'Collaborator'}>
                                <div class="ring-2 ring-zinc-950 rounded-full">
                                  <ProfilePicture path={c.profile_picture} username={c.username} size="xs" />
                                </div>
                              </Tooltip>
                              <Show when={isAuthor()}>
                                <button
                                  onMouseDown={() => removeCollaborator(c.edge)}
                                  class="absolute -top-1 -right-1 w-4 h-4 rounded-full bg-zinc-900 border border-white/[0.06] text-zinc-500 hover:text-red-400 text-[10px] leading-none opacity-0 group-hover:opacity-100 transition-opacity"
                                  title="Remove collaborator"
                                  aria-label="Remove collaborator"
                                >
                                  ×
                                </button>
                              </Show>
                            </div>
                          )}
                        </For>
                      </div>

                      <div class="flex items-center gap-2">
                        <Show when={canEdit() && !threadData().published}>
                          <Tooltip text="Private — only members can view">
                            <span
                              class="inline-flex items-center justify-center w-6 h-6 text-zinc-500 bg-surface border border-white/[0.06] rounded-full"
                              aria-label="Private"
                            >
                              <Lock size={12} />
                            </span>
                          </Tooltip>
                        </Show>

                        <Show when={canEdit()}>
                          <div class="relative" data-thread-menu>
                            <button
                              onMouseDown={() => setMenuOpen(!menuOpen())}
                              class="inline-flex items-center justify-center w-8 h-8 text-zinc-500 hover:text-white rounded-lg transition-colors duration-150"
                              title="More"
                              aria-label="More options"
                            >
                              <MoreVertical size={16} />
                            </button>
                            <Show when={menuOpen()}>
                              <div class="absolute right-0 mt-1.5 w-40 bg-surface border border-white/[0.06] rounded-lg shadow-2xl z-50 py-1 animate-fade-in">
                                <Show when={isAuthor()}>
                                  <MenuItem
                                    onSelect={() => {
                                      setMenuOpen(false);
                                      setShareOpen(true);
                                    }}
                                  >
                                    Share
                                  </MenuItem>
                                </Show>
                                <MenuItem
                                  onSelect={() => {
                                    setMenuOpen(false);
                                    void db.update('thread', threadData().id, { published: !threadData().published });
                                  }}
                                >
                                  {threadData().published ? 'Unpublish' : 'Publish'}
                                </MenuItem>
                                <Show when={isAuthor()}>
                                  <MenuItem
                                    danger
                                    onSelect={() => {
                                      setMenuOpen(false);
                                      void deleteThread(threadData().id);
                                    }}
                                  >
                                    Delete post
                                  </MenuItem>
                                </Show>
                              </div>
                            </Show>
                          </div>
                        </Show>
                      </div>
                    </div>

                    {/* Title + content, both collaborative */}
                    <div class="bg-surface/40 rounded-xl border border-white/[0.06] p-6">
                      <CrdtTextInput
                        id="thread-title"
                        field={titleField()}
                        name="title"
                        fallback={threadData().title ?? ''}
                        readOnly={!canEdit()}
                        placeholder="Untitled"
                        maxLength={200}
                        class="w-full bg-transparent text-2xl font-semibold mb-4 leading-tight outline-none border-none focus:outline-none placeholder-zinc-600"
                      />

                      <Show
                        when={contentField()}
                        fallback={
                          <div class="text-[15px] text-zinc-300 leading-relaxed min-h-[120px]">
                            <p class="whitespace-pre-wrap">{threadData().content}</p>
                          </div>
                        }
                      >
                        {(field) => (
                          <CollaborativeEditor
                            field={field()}
                            placeholder="Write something..."
                            class="text-[15px] text-zinc-300 focus-within:text-white leading-relaxed min-h-[120px] [&_.ProseMirror]:outline-none [&_.ProseMirror]:min-h-[120px]"
                            editable={canEdit()}
                            username={auth.user()?.username}
                          />
                        )}
                      </Show>
                    </div>

                    <div class="flex items-center mt-3 px-1">
                      <Tooltip text="Reply" kbd="r">
                        <button
                          onMouseDown={() => focusReply()}
                          class="inline-flex items-center gap-1.5 text-xs text-zinc-500 hover:text-zinc-300 transition-colors duration-150"
                        >
                          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path
                              stroke-linecap="round"
                              stroke-linejoin="round"
                              stroke-width="1.5"
                              d="M12 20.25c4.97 0 9-3.694 9-8.25s-4.03-8.25-9-8.25S3 7.444 3 12c0 2.104.859 4.023 2.273 5.48.432.447.74 1.04.586 1.641a4.483 4.483 0 01-.923 1.785A5.969 5.969 0 006 21c1.282 0 2.47-.402 3.445-1.087.81.22 1.668.337 2.555.337z"
                            />
                          </svg>
                          {threadData().comments?.length ?? 0}
                        </button>
                      </Tooltip>
                    </div>
                  </article>

                  {/* Replies */}
                  <div>
                    <div class="flex items-center justify-between mb-4 pb-3 border-b border-white/[0.06]">
                      <h2 class="text-base font-semibold">
                        Replies
                        <span class="text-zinc-600 font-normal text-sm ml-1.5">
                          {threadData().comments?.length ?? 0}
                        </span>
                      </h2>

                      <div class="flex text-xs bg-surface rounded-lg border border-white/[0.06] overflow-hidden">
                        <For each={['all', 'mine'] as const}>
                          {(filter) => (
                            <button
                              onMouseDown={() => setCommentFilter(filter)}
                              class={`px-3 py-1.5 transition-colors duration-150 ${
                                commentFilter() === filter
                                  ? 'bg-surface-hover text-white'
                                  : 'text-zinc-500 hover:text-zinc-300'
                              }`}
                            >
                              {filter === 'all' ? 'All' : 'Mine'}
                            </button>
                          )}
                        </For>
                      </div>
                    </div>

                    <div class="mb-6">
                      <CommentForm threadId={threadData().id} />
                    </div>

                    <div class="space-y-1">
                      <For
                        each={threadData().comments ?? []}
                        fallback={
                          <div class="py-10 text-center text-sm text-zinc-600">
                            No replies yet. Be the first to respond.
                          </div>
                        }
                      >
                        {(comment) => <CommentBox comment={comment} />}
                      </For>
                    </div>
                  </div>
                </div>
              )}
            </Show>
          </div>
        </div>
      </div>

      <Show when={shareOpen() && thread()}>
        {(t) => (
          <ShareDialog threadId={t().id} published={!!t().published} onClose={() => setShareOpen(false)} />
        )}
      </Show>
    </div>
  );
}

function MenuItem(props: { onSelect: () => void; danger?: boolean; children: JSX.Element }) {
  return (
    <button
      onMouseDown={() => props.onSelect()}
      class={`w-full flex items-center gap-2.5 px-3 py-2 text-sm hover:bg-surface-hover transition-colors duration-150 ${
        props.danger ? 'text-red-400 hover:text-red-300' : 'text-zinc-300 hover:text-white'
      }`}
    >
      {props.children}
    </button>
  );
}
