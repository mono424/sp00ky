import { Show } from 'solid-js';
import type { GetTable, TableModel } from '@spooky-sync/client-solid';
import { useDb } from '@spooky-sync/client-solid';
import type { SchemaDefinition, schema } from '../schema.gen';
import { useAuth } from '../lib/auth';
import { idString } from '../lib/ids';
import { ProfilePicture } from './ProfilePicture';

type AugmentedComment = Omit<TableModel<GetTable<SchemaDefinition, 'comment'>>, 'author'> & {
  author?: TableModel<GetTable<SchemaDefinition, 'user'>>;
};

export function CommentBox(props: { comment: AugmentedComment }) {
  const db = useDb<typeof schema>();
  const auth = useAuth();

  const isMine = () => !!auth.userId() && auth.userId() === idString(props.comment.author?.id);

  return (
    <div class="group flex gap-3 py-4 hover:bg-surface/20 -mx-2 px-2 rounded-xl transition-colors duration-150">
      {/* Avatar */}
      <ProfilePicture
        path={props.comment.author?.profile_picture}
        username={props.comment.author?.username}
        size="sm"
      />

      <div class="flex-1 min-w-0">
        {/* Name + time + actions */}
        <div class="flex items-center gap-2 mb-1">
          <span class="text-sm font-medium text-zinc-200">
            {props.comment.author?.username || 'Unknown'}
          </span>
          <span class="text-zinc-700">&middot;</span>
          <span class="text-xs text-zinc-600">
            {new Date(props.comment.created_at ?? 0).toLocaleDateString(undefined, {
              month: 'short',
              day: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
            })}
          </span>

          <Show when={isMine()}>
            <button
              onClick={() => db.delete('comment', props.comment.id)}
              class="ml-auto text-xs text-zinc-600 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-all duration-150"
            >
              Delete
            </button>
          </Show>
        </div>

        {/* Content */}
        <p class="text-[14px] text-zinc-400 whitespace-pre-wrap leading-relaxed">
          {props.comment.content}
        </p>
      </div>
    </div>
  );
}
