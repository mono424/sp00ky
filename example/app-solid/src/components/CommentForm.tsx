import { createSignal, Show } from 'solid-js';
import { Uuid, useDb } from '@spooky-sync/client-solid';
import type { schema } from '../schema.gen';
import { useAuth } from '../lib/auth';
import { ProfilePicture } from './ProfilePicture';
import { Tooltip } from './Tooltip';

export function CommentForm(props: { threadId: string }) {
  const db = useDb<typeof schema>();
  const auth = useAuth();
  const [content, setContent] = createSignal('');
  const [isFocused, setIsFocused] = createSignal(false);
  const expanded = () => isFocused() || content().length > 0;

  const submit = async () => {
    const text = content().trim();
    const me = auth.userId();
    if (!text || !me) return;
    // Optimistic: the comment shows at once and syncs in the background. A
    // plain `table:key` id (see `idString`).
    await db.create(`comment:${Uuid.v4().toString().replace(/-/g, '')}`, {
      thread: props.threadId,
      content: text,
      author: me,
    });
    setContent('');
  };

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
      class="w-full"
    >
      <div class="flex gap-3">
        <ProfilePicture path={auth.user()?.profile_picture} username={auth.user()?.username} size="sm" />

        <div class="flex-1">
          <textarea
            id="comment-textarea"
            value={content()}
            onInput={(e) => setContent(e.currentTarget.value)}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            onKeyDown={(e) => {
              if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
                e.preventDefault();
                void submit();
              }
            }}
            placeholder="Write a comment... (Tap r to focus)"
            rows={expanded() ? '3' : '1'}
            class="w-full bg-surface text-white px-4 py-2.5 border border-white/[0.06] focus:border-zinc-600 outline-none resize-none rounded-xl placeholder-zinc-600 text-sm leading-relaxed block transition-all duration-150"
            required
          />

          <Show when={expanded()}>
            <div class="flex justify-between items-center mt-2">
              <span class="text-xs text-zinc-600">
                {content().length > 0 ? `${content().length} characters` : ''}
              </span>

              <Tooltip text="Reply" kbd={`${navigator.platform.includes('Mac') ? '⌘' : 'Ctrl'}+↵`} position="top">
                <button
                  type="submit"
                  disabled={!content().trim()}
                  class="bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white px-4 py-1.5 rounded-lg text-sm font-medium transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Reply
                </button>
              </Tooltip>
            </div>
          </Show>
        </div>
      </div>
    </form>
  );
}
