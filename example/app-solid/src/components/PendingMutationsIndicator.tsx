import { createSignal, onCleanup, Show } from 'solid-js';
import { useDb } from '@spooky-sync/client-solid';

export function PendingMutationsIndicator() {
  const db = useDb();
  const [count, setCount] = createSignal(db.pendingMutationCount);
  const [visible, setVisible] = createSignal(count() > 0);

  let showTimeout: ReturnType<typeof setTimeout> | undefined;
  let hideTimeout: ReturnType<typeof setTimeout> | undefined;

  // Shown only when writes stay queued for a moment (offline, slow server).
  const unsub = db.subscribeToPendingMutations((newCount) => {
    setCount(newCount);
    if (newCount > 0) {
      clearTimeout(hideTimeout);
      showTimeout = setTimeout(() => setVisible(true), 400);
    } else {
      clearTimeout(showTimeout);
      hideTimeout = setTimeout(() => setVisible(false), 400);
    }
  });
  onCleanup(() => {
    unsub();
    clearTimeout(hideTimeout);
    clearTimeout(showTimeout);
  });

  return (
    <Show when={visible()}>
      <div class={`pending-mutations-pill ${count() === 0 ? 'pending-mutations-exit' : ''}`}>
        <span class="pending-mutations-dot" />
        <span class="text-xs font-medium tracking-wide">{count()} pending</span>
      </div>
    </Show>
  );
}
