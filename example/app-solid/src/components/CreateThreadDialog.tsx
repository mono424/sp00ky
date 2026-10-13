import { createSignal, Show, createEffect } from 'solid-js';
import { useNavigate } from '@solidjs/router';
import { useAuth } from '../lib/auth';
import { Uuid, useDb } from '@spooky-sync/client-solid';
import { RecordId } from 'surrealdb';
import type { CrdtField } from '@spooky-sync/core';
import type { schema } from '../schema.gen';
import { createHotkey } from '../lib/keyboard';
import { Tooltip } from './Tooltip';
import { CollaborativeEditor } from './CollaborativeEditor';

interface CreateThreadDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

export function CreateThreadDialog(props: CreateThreadDialogProps) {
  const db = useDb<typeof schema>();
  const navigate = useNavigate();
  const auth = useAuth();
  const [title, setTitle] = createSignal('');
  const [contentText, setContentText] = createSignal('');
  const [error, setError] = createSignal('');
  const [isLoading, setIsLoading] = createSignal(false);
  // A fresh draft document for `content` per dialog open: the editor writes
  // into it before the thread exists, and on submit it is handed to the new
  // thread's CRDT field, whose first edits it becomes. `title` is a plain
  // `TYPE string` column, so it ships as the typed text directly.
  const [contentField, setContentField] = createSignal<CrdtField | null>(null);

  createEffect(() => {
    if (props.isOpen) {
      setTitle('');
      setContentText('');
      setError('');
      void db.getSp00ky().createCrdtDraft('thread', 'content').then(setContentField);
    } else {
      setContentField(null);
    }
  });

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    if (!title().trim() || !contentText().trim() || isLoading()) return;

    setError('');
    setIsLoading(true);

    try {
      const user = auth.user();
      if (!user) {
        throw new Error('You must be logged in to create a thread');
      }

      const cField = contentField();
      if (!cField) throw new Error('Editor not ready');

      const genId = Uuid.v4().toString().replace(/-/g, '');
      const threadId = `thread:${genId}`;
      await db.create(threadId, {
        title: title().trim(),
        author: new RecordId('user', user.id.toString().split(':')[1]),
        active: true,
        published: false,
      });
      // The draft's edits become the thread's first content edits.
      const sp00ky = db.getSp00ky();
      await sp00ky.openCrdtField('thread', threadId, 'content', cField);
      await sp00ky.closeCrdtField('thread', threadId, 'content');

      handleClose();
      navigate(`/thread/${genId}`);
    } catch (err) {
      console.error('Failed to create thread:', err);
      setError(err instanceof Error ? err.message : 'Failed to create thread');
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    setTitle('');
    setContentText('');
    setError('');
    props.onClose();
  };

  let titleInputRef: HTMLInputElement | undefined = undefined;

  // Autofocus title input on open
  createEffect(() => {
    if (props.isOpen) {
      requestAnimationFrame(() => titleInputRef?.focus());
    }
  });

  // Escape to close
  createHotkey('Escape', () => handleClose(), () => ({ enabled: props.isOpen, ignoreInputs: false }));

  // Cmd+Enter to submit
  const handleKeyDown = (e: KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      if (title().trim() && contentText().trim() && !isLoading()) {
        handleSubmit(e);
      }
    }
  };

  return (
    <Show when={props.isOpen}>
      <div class="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-[100] p-4" onMouseDown={handleClose}>
        <div
          class="animate-slide-up bg-surface border border-white/[0.06] rounded-xl w-full max-w-2xl shadow-2xl max-h-[90vh] flex flex-col"
          onMouseDown={(e) => e.stopPropagation()}
          onKeyDown={handleKeyDown}
        >
          {/* Header */}
          <div class="flex justify-between items-center px-6 pt-6 pb-2 flex-shrink-0">
            <h2 class="text-lg font-semibold">New thread</h2>
            <Tooltip text="Close" kbd="Esc">
              <button
                onMouseDown={handleClose}
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
            <form onSubmit={handleSubmit} class="space-y-4">
              <div>
                <div class="flex justify-between items-end mb-1.5">
                  <label for="create-title" class="text-sm font-medium text-zinc-400">
                    Title
                  </label>
                  <span class="text-xs text-zinc-600">{title().length}/200</span>
                </div>
                <input
                  ref={(el) => titleInputRef = el}
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
                <Show when={contentField()}>
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
                <div class="bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 p-3 text-sm">
                  {error()}
                </div>
              </Show>

              <div class="flex justify-end gap-3 pt-2">
                <Tooltip text="Close" kbd="Esc" position="top">
                  <button
                    type="button"
                    onMouseDown={handleClose}
                    class="px-5 py-2.5 text-sm text-zinc-400 hover:text-white transition-colors duration-150"
                  >
                    Cancel
                  </button>
                </Tooltip>
                <Tooltip text="Publish" kbd={`${navigator.platform.includes('Mac') ? '⌘' : 'Ctrl'}+↵`} position="top">
                  <button
                    type="submit"
                    disabled={isLoading() || !title().trim() || !contentText().trim()}
                    class="bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white px-6 py-2.5 rounded-lg font-medium transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
                  >
                    {isLoading() ? 'Publishing...' : 'Publish'}
                  </button>
                </Tooltip>
              </div>
            </form>
          </div>
        </div>
      </div>
    </Show>
  );
}
