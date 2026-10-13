import { createEffect, createSignal, Show } from 'solid-js';
import { useNavigate, useParams } from '@solidjs/router';
import { useDb } from '@spooky-sync/client-solid';
import * as jose from 'jose';
import { useAuth } from '../../lib/auth';
import type { schema } from '../../schema.gen';

export default function InvitePage() {
  const params = useParams();
  const navigate = useNavigate();
  const db = useDb<typeof schema>();
  const auth = useAuth();
  const [error, setError] = createSignal<string | null>(null);
  const [accepted, setAccepted] = createSignal(false);

  createEffect(async () => {
    const userId = auth.userId();
    const jwt = params.jwt;
    if (!userId || !jwt || accepted()) return;

    // The link names its thread in `sub`. Whether it is valid is the
    // database's call, not this decode's.
    let thread: string;
    try {
      const claims = jose.decodeJwt(jwt);
      if (typeof claims.sub !== 'string' || !claims.sub.includes(':')) throw new Error('no thread');
      thread = claims.sub;
    } catch {
      setError('This invite link is malformed.');
      return;
    }

    // Accepting is a write the database checks itself: the `share_accept`
    // event joins this user to the thread only for an unexpired link its
    // author created, and throws otherwise. No backend involved, so it works
    // on a deployment that runs none.
    try {
      await db.useRemote((s) => s.query('CREATE share_accept SET jwt = $jwt RETURN NONE', { jwt }));
    } catch (e: any) {
      const msg = String(e?.message || e);
      setError(/invalid or has expired/.test(msg) ? 'This invite link is invalid or has expired.' : msg);
      return;
    }

    setAccepted(true);
    const suffix = thread.split(':').slice(1).join(':');
    navigate(`/thread/${suffix}`, { replace: true });
  });

  return (
    <div class="min-h-[60vh] flex items-center justify-center px-6">
      <div class="bg-surface/50 rounded-xl border border-white/[0.06] p-10 text-center max-w-sm">
        <Show
          when={auth.userId()}
          fallback={
            <>
              <p class="text-zinc-200 font-medium mb-2">You've been invited to a thread.</p>
              <p class="text-sm text-zinc-500">Sign in to accept the invite.</p>
            </>
          }
        >
          <Show
            when={!error()}
            fallback={
              <>
                <p class="text-zinc-200 font-medium mb-2">Invite error</p>
                <p class="text-sm text-zinc-500">{error()}</p>
              </>
            }
          >
            <p class="text-zinc-200 font-medium mb-2">Joining thread...</p>
            <p class="text-sm text-zinc-500">Verifying invite.</p>
          </Show>
        </Show>
      </div>
    </div>
  );
}
