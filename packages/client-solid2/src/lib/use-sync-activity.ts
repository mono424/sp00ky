import { createEffect, createSignal, onCleanup, type Accessor } from 'solid-js';
import { useDb } from './context';
import { fromSubscription } from './from-subscription';
import type { SyncedDb } from '../index';
import type { SchemaStructure } from '@spooky-sync/query-builder';

export interface UseSyncActivityOptions {
  /**
   * How long queries must be fetching before `isDownloading()` turns on.
   * Filters the sub-frame fetches a warm cache produces, so the indicator only
   * shows work the user could notice. Default 200 ms.
   */
  downloadDelayMs?: number;
  /**
   * `isUploading()` turns on once MORE than this many mutations are waiting in
   * the outbox. Default 0: any unacknowledged write is upsync worth showing.
   */
  uploadThreshold?: number;
  /**
   * How long the outbox must stay above `uploadThreshold` before
   * `isUploading()` turns on. The mirror of `downloadDelayMs`: a write the
   * server acknowledges within a round trip never lights the indicator, a
   * write that is actually waiting does. Default 150 ms.
   */
  uploadDelayMs?: number;
}

export interface UseSyncActivity {
  /** Queries inside a fetch cycle right now (core `fetchingQueryCount`). */
  fetchingQueries: Accessor<number>;
  /** Locally committed writes the server has not acknowledged yet. */
  pendingMutations: Accessor<number>;
  /** Fetching for longer than `downloadDelayMs`. Drives a "downloading" mark. */
  isDownloading: Accessor<boolean>;
  /**
   * More than `uploadThreshold` writes have been queued for longer than
   * `uploadDelayMs`. Drives an "uploading" mark.
   */
  isUploading: Accessor<boolean>;
}

/**
 * The two directions of sync traffic, for an indicator in the app chrome.
 *
 * `fetchingQueries` is one subscription on the engine's aggregate fetch count,
 * not one per query; `pendingMutations` is the outbox depth. Both
 * `isDownloading` and `isUploading` are debounced ON by their delay (and off at
 * once), so a local-first page that answers from cache and confirms with the
 * server in a few milliseconds never flickers. Must be used within a `<Sp00kyProvider>`, or
 * pass the `SyncedDb` explicitly.
 */
export function useSyncActivity<S extends SchemaStructure = any>(
  dbOrOptions?: SyncedDb<S> | UseSyncActivityOptions,
  maybeOptions?: UseSyncActivityOptions
): UseSyncActivity {
  const explicitDb =
    dbOrOptions && typeof (dbOrOptions as SyncedDb<S>).subscribeToFetchActivity === 'function'
      ? (dbOrOptions as SyncedDb<S>)
      : undefined;
  const options = (explicitDb ? maybeOptions : (dbOrOptions as UseSyncActivityOptions)) ?? {};
  const db = explicitDb ?? useDb<S>();
  const downloadDelayMs = options.downloadDelayMs ?? 200;
  const uploadThreshold = options.uploadThreshold ?? 0;
  const uploadDelayMs = options.uploadDelayMs ?? 150;

  const fetchingQueries = fromSubscription<number>(
    (cb) => db.subscribeToFetchActivity(cb),
    db.fetchingQueryCount
  );
  const pendingMutations = fromSubscription<number>(
    (cb) => db.subscribeToPendingMutations(cb),
    db.pendingMutationCount
  );

  const isDownloading = delayedOn(() => fetchingQueries() > 0, downloadDelayMs);
  const isUploading = delayedOn(() => pendingMutations() > uploadThreshold, uploadDelayMs);

  return { fetchingQueries, pendingMutations, isDownloading, isUploading };
}

/**
 * `busy` held for `delayMs`, dropped the instant it clears. Both directions of
 * traffic use it, so a burst too short to see never flickers the indicator and
 * neither one can latch on after the work is done.
 */
function delayedOn(busy: Accessor<boolean>, delayMs: number): Accessor<boolean> {
  // Written from a timer, hence ownedWrite.
  const [on, setOn] = createSignal(false, { ownedWrite: true });
  let timer: ReturnType<typeof setTimeout> | undefined;
  createEffect(busy, (isBusy) => {
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (!isBusy) {
      setOn(false);
      return;
    }
    if (delayMs <= 0) {
      setOn(true);
      return;
    }
    timer = setTimeout(() => {
      timer = undefined;
      setOn(true);
    }, delayMs);
  });
  onCleanup(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
  return on;
}
