import { createResource, createSignal, onMount, Show } from 'solid-js';
import { useNavigate } from '@solidjs/router';
import { Uuid, useDb } from '@spooky-sync/client-solid';
import type { schema } from '../schema.gen';
import { useAuth } from '../lib/auth';
import { idString } from '../lib/ids';
import { createHotkey } from '../lib/keyboard';
import { CollaborativeEditor } from './CollaborativeEditor';
import { Tooltip } from './Tooltip';

export function CreateThreadDialog(props: { onClose: () => void }) {
  const db = useDb<typeof schema>();
  const navigate = useNavigate();
  const auth = useAuth();
  const [title, setTitle] = createSignal('');
  const [contentText, setContentText] = createSignal('');
  const [error, setError] = createSignal('');
  const [publishing, setPublishing] = createSignal(false);
  // A draft document for `content`: the editor writes into it before the
  // thread exists, and on publish its edits become the thread's first ones.
  const [contentDraft] = createResource(() => db.getSp00ky().createCrdtDraft('thread', 'content'));
  const canPublish = () => !!title().trim() && !!contentText().trim() && !publishing();

  let titleInput: HTMLInputElement | undefined;
  onMount(() => titleInput?.focus());
  createHotkey('Escape', () => props.onClose(), { ignoreInputs: false });

  const publish = async () => {
    const draft = contentDraft();
    const user = auth.user();
    if (!canPublish() || !draft || !user) return;
    setError('');
    setPublishing(true);
    try {
      const key = Uuid.v4().toString().replace(/-/g, '');
      const id = `thread:${key}`;
      const sp00ky = db.getSp00ky();
      // `title` is `@crdt text` too: its document starts from the typed text.
      const titleText = title().trim();
      const titleDraft = await sp00ky.createCrdtDraft('thread', 'title');
      titleDraft.getDoc().getText('title').insert(0, titleText);
      titleDraft.getDoc().commit();
      // The create may set a `@crdt` column (later writes go through the
      // document), so lists show the text before the server's first fold.
      await db.create(id, {
        title: titleText,
        content: contentText().trim(),
        author: idString(user.id),
        active: true,
        published: false,
      });
      // The drafts' edits become the thread's first title and content edits.
      // Closing queues them in the outbox (offline it keeps them on the device).
      await sp00ky.openCrdtField('thread', id, 'title', titleDraft);
      await sp00ky.openCrdtField('thread', id, 'content', draft);
      await Promise.all([
        sp00ky.closeCrdtField('thread', id, 'title'),
        sp00ky.closeCrdtField('thread', id, 'content'),
      ]);
      // Straight to the new thread, in place of this form's history entry.
      navigate(`/thread/${key}`, { replace: true });
    } catch (err) {
      console.error('Failed to create thread:', err);
      setError(err instanceof Error ? err.message : 'Failed to create thread');
      setPublishing(false);
    }
  };

  return (
    <div class="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-[100] p-4" onMouseDown={() => props.onClose()}>
      <div
        class="animate-slide-up bg-surface border border-white/[0.06] rounded-xl w-full max-w-2xl shadow-2xl max-h-[90vh] flex flex-col"
        onMouseDown={(e) => e.stopPropagation()}
        onKeyDown={(e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            void publish();
          }
        }}
      >
        {/* Header */}
        <div class="flex justify-between items-center px-6 pt-6 pb-2 flex-shrink-0">
          <h2 class="text-lg font-semibold">New thread</h2>
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

        {/* Content */}
        <div class="px-6 pb-6 pt-4 overflow-y-auto">
          <form
            onSubmit={(e) => {
              e.preventDefault();
              void publish();
            }}
            class="space-y-4"
          >
            <div>
              <div class="flex justify-between items-end mb-1.5">
                <label for="create-title" class="text-sm font-medium text-zinc-400">
                  Title
                </label>
                <span class="text-xs text-zinc-600">{title().length}/200</span>
              </div>
              <input
                ref={titleInput}
                id="create-title"
                type="text"
                value={title()}
                onInput={(e) => setTitle(e.currentTarget.value)}
                required
                maxlength="200"
                class="w-full bg-zinc-950 border border-white/[0.06] rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-zinc-600 transition-colors duration-150 placeholder-zinc-600 text-sm"
                placeholder="Enter a title"
                autocomplete="off"
              />
            </div>

            <div>
              <label class="block text-sm font-medium text-zinc-400 mb-1.5">Content</label>
              <Show when={contentDraft()}>
                {(field) => (
                  <div class="w-full bg-zinc-950 border border-white/[0.06] rounded-lg p-4 focus-within:border-zinc-600 transition-colors duration-150">
                    <CollaborativeEditor
                      field={field()}
                      placeholder="What's on your mind?"
                      class="text-sm text-white leading-relaxed [&_.ProseMirror]:outline-none [&_.ProseMirror]:min-h-[10rem] [&_.ProseMirror_p.is-editor-empty:first-child::before]:text-zinc-600 [&_.ProseMirror_p.is-editor-empty:first-child::before]:content-[attr(data-placeholder)] [&_.ProseMirror_p.is-editor-empty:first-child::before]:float-left [&_.ProseMirror_p.is-editor-empty:first-child::before]:pointer-events-none [&_.ProseMirror_p.is-editor-empty:first-child::before]:h-0"
                      username={auth.user()?.username}
                      onUpdate={setContentText}
                    />
                  </div>
                )}
              </Show>
            </div>

            <Show when={error()}>
              <div class="bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 p-3 text-sm">{error()}</div>
            </Show>

            <div class="flex justify-end gap-3 pt-2">
              <Tooltip text="Close" kbd="Esc" position="top">
                <button
                  type="button"
                  onMouseDown={() => props.onClose()}
                  class="px-5 py-2.5 text-sm text-zinc-400 hover:text-white transition-colors duration-150"
                >
                  Cancel
                </button>
              </Tooltip>
              <Tooltip text="Publish" kbd={`${navigator.platform.includes('Mac') ? '⌘' : 'Ctrl'}+↵`} position="top">
                <button
                  type="submit"
                  disabled={!canPublish()}
                  class="bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white px-6 py-2.5 rounded-lg font-medium transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
                >
                  {publishing() ? 'Publishing...' : 'Publish'}
                </button>
              </Tooltip>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
