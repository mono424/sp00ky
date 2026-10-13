import type { Accessor} from 'solid-js';
import { createSignal } from 'solid-js';
import { useAuth } from '../lib/auth';
import { Uuid, useDb } from '@spooky-sync/client-solid';
import type { schema } from '../schema.gen';
import { ProfilePicture } from './ProfilePicture';
import { Tooltip } from './Tooltip';

interface CommentFormProps {
  thread: Accessor<{ id: string }>;
  onCommentAdded?: () => void;
}

export function CommentForm(props: CommentFormProps) {
  const db = useDb<typeof schema>();
  const auth = useAuth();
  const [content, setContent] = createSignal('');
  const [isLoading, setIsLoading] = createSignal(false);
  const [isFocused, setIsFocused] = createSignal(false);

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    if (!content().trim() || isLoading()) return;

    setIsLoading(true);
    try {
      const user = auth.user();
      if (!user) {
        throw new Error('You must be logged in to post a comment');
      }

      // A plain `table:key` string: `RecordId.toString()` brackets a key that
      // starts with a digit, and the brackets would end up in the stored id.
      const commentId = `comment:${Uuid.v4().toString().replace(/-/g, '')}`;

      await db.create(commentId, {
        thread: props.thread().id,
        content: content().trim(),
        author: user.id,
      });

      console.log('[CommentForm] Comment created with ID:', commentId);

      setContent('');
      props.onCommentAdded?.();
    } catch (error) {
      console.error('Failed to create comment:', error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} class="w-full">
      <div class="flex gap-3">
        {/* User avatar */}
        <ProfilePicture
          src={() => auth.user()?.profile_picture}
          username={() => auth.user()?.username}
          size="sm"
        />

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
                if (content().trim() && !isLoading()) handleSubmit(e);
              }
            }}
            placeholder="Write a comment... (Tap r to focus)"
            rows={isFocused() || content().length > 0 ? '3' : '1'}
            class="w-full bg-surface text-white px-4 py-2.5 border border-white/[0.06] focus:border-zinc-600 outline-none resize-none rounded-xl placeholder-zinc-600 text-sm leading-relaxed block transition-all duration-150"
            required
          />

          {(isFocused() || content().length > 0) && (
            <div class="flex justify-between items-center mt-2">
              <span class="text-xs text-zinc-600">
                {content().length > 0 ? `${content().length} characters` : ''}
              </span>

              <Tooltip text="Reply" kbd={`${navigator.platform.includes('Mac') ? '\u2318' : 'Ctrl'}+\u21B5`} position="top">
                <button
                  type="submit"
                  disabled={isLoading() || !content().trim()}
                  class="bg-surface hover:bg-surface-hover border border-white/[0.06] text-zinc-300 hover:text-white px-4 py-1.5 rounded-lg text-sm font-medium transition-colors duration-150 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isLoading() ? 'Posting...' : 'Reply'}
                </button>
              </Tooltip>
            </div>
          )}
        </div>
      </div>
    </form>
  );
}
