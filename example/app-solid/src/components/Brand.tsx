/** The sp00ky ghost: its eyes are holes, so it sits on any background. */
export function GhostMark(props: { class?: string }) {
  return (
    <svg class={props.class} viewBox="0 0 32 32" fill="currentColor" aria-hidden="true">
      <path
        fill-rule="evenodd"
        d="M6 10V28L10 24L14 28L18 24L22 28L26 24V10C26 4.47715 21.5228 0 16 0C10.4772 0 6 4.47715 6 10ZM10 8H14V12H10ZM18 8H22V12H18Z"
      />
    </svg>
  );
}

/** "sp00ky threads", the app's name as it appears in the header. */
export function Wordmark(props: { class?: string }) {
  return (
    <span
      class={`inline-flex items-center gap-2 lowercase ${props.class ?? ''}`}
      style="font-family: 'Space Grotesk', sans-serif; font-weight: 700; letter-spacing: -0.04em;"
    >
      <GhostMark class="w-[1.1em] h-[1.1em] -mt-0.5" />
      <span>
        sp00ky<span class="text-zinc-500"> threads</span>
      </span>
    </span>
  );
}

export const SP00KY_URL = 'https://sp00ky.cloud';
export const SP00KY_REPO_URL = 'https://github.com/mono424/sp00ky';
