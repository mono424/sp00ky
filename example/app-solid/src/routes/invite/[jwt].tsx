import { createResource, Show } from 'solid-js';
import { useNavigate, useParams } from '@solidjs/router';
import { useDb } from '@spooky-sync/client-solid';
import * as jose from 'jose';
import type { schema } from '../../schema.gen';

/** The thread a link names in `sub`. Whether the link is valid is the database's call. */
function threadKey(jwt: string): string {
  try {
    const sub = jose.decodeJwt(jwt).sub ?? '';
    const colon = sub.indexOf(':');
    if (colon > 0) return sub.slice(colon + 1);
  } catch {
    /* falls through */
  }
  throw new Error('This invite link is malformed.');
}

// The layout renders routes only for a signed-in user, so this runs signed in.
export default function InvitePage() {
  const params = useParams();
  const navigate = useNavigate();
  const db = useDb<typeof schema>();

  // Accepting is a write the database checks itself: the `share_accept` event
  // joins this user to the thread only for an unexpired link its author made,
  // and throws otherwise. No backend, so it works on a deployment without one.
  const [accept] = createResource(
    () => params.jwt,
    async (jwt) => {
      const key = threadKey(jwt);
      await db.remoteQuery('CREATE share_accept SET jwt = $jwt RETURN NONE', { jwt });
      navigate(`/thread/${key}`, { replace: true });
    }
  );
  const errorMessage = () => {
    const message = String(accept.error?.message ?? accept.error ?? '');
    return /invalid or has expired/.test(message) ? 'This invite link is invalid or has expired.' : message;
  };

  return (
    <div class="min-h-[60vh] flex items-center justify-center px-6">
      <div class="bg-surface/50 rounded-xl border border-white/[0.06] p-10 text-center max-w-sm">
        <Show
          when={accept.error}
          fallback={
            <>
              <p class="text-zinc-200 font-medium mb-2">Joining thread...</p>
              <p class="text-sm text-zinc-500">Verifying invite.</p>
            </>
          }
        >
          <p class="text-zinc-200 font-medium mb-2">Invite error</p>
          <p class="text-sm text-zinc-500">{errorMessage()}</p>
        </Show>
      </div>
    </div>
  );
}
