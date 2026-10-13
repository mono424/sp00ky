import { BucketImage } from '@spooky-sync/client-solid';

const sizeClasses = {
  xs: 'w-7 h-7 text-xs',
  sm: 'w-8 h-8 text-xs',
  md: 'w-10 h-10 text-sm',
  lg: 'w-20 h-20 text-2xl',
} as const;

/** A user's avatar from the `profile_pictures` bucket, over their initial. */
export function ProfilePicture(props: {
  path: string | null | undefined;
  username: string | null | undefined;
  size?: keyof typeof sizeClasses;
}) {
  return (
    <BucketImage
      bucket="profile_pictures"
      path={props.path}
      alt=""
      class={`${sizeClasses[props.size ?? 'xs']} rounded-full overflow-hidden flex-shrink-0`}
      fallback={
        <div class="w-full h-full bg-zinc-800 text-zinc-400 flex items-center justify-center font-semibold">
          {props.username?.charAt(0).toUpperCase() || '?'}
        </div>
      }
    />
  );
}
