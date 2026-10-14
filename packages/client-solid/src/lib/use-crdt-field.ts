import { createEffect, createMemo, createSignal, on, onCleanup, useContext, type Accessor } from 'solid-js';
import { Sp00kyContext } from './context';
import type { CrdtField } from '@spooky-sync/core';

/**
 * Open a `@crdt` field of the record `recordId()` names, for an editor to bind
 * to (`field()?.getDoc()`). Re-opens when the id changes; closes on cleanup.
 *
 * The accessor stays `null` until the field is loaded (`CrdtField.isLoaded`):
 * this device had the document, or the server answered. Until then the
 * document may be empty only because nothing has arrived yet, so show the
 * column's text instead of binding an editor to it.
 */
export function useCrdtField(
  table: string,
  recordId: () => string | undefined,
  field: string,
): Accessor<CrdtField | null> {
  const db = useContext(Sp00kyContext);
  if (!db) {
    throw new Error('useCrdtField must be used within a <Sp00kyProvider>');
  }

  const [crdtField, setCrdtField] = createSignal<CrdtField | null>(null);
  // Only a different id reopens: `recordId` may re-run on every live update.
  const id = createMemo(recordId);

  createEffect(
    on(id, (current) => {
      if (!current) return;
      const sp00ky = db.getSp00ky();
      let active = true;
      let opened = false;
      let offLoaded = () => {};
      sp00ky
        .openCrdtField(table, current, field)
        .then((cf) => {
          opened = true;
          if (!active) {
            void sp00ky.closeCrdtField(table, current, field);
            return;
          }
          offLoaded = cf.onLoaded(() => {
            if (active) setCrdtField(cf);
          });
        })
        .catch((err) => {
          // A silent rejection leaves the consumer's `Show when={field()}` on
          // its fallback for good, with no trail. Surface the cause (missing
          // `@crdt` annotation, schema codegen drift, local DB failure, ...).
          console.error(`[useCrdtField] Failed to open CRDT field ${table}.${field} on ${current}:`, err);
        });
      // Runs before the next id opens, and on unmount. Closing a field whose
      // open is still in flight is left to the `then` above.
      onCleanup(() => {
        active = false;
        offLoaded();
        setCrdtField(null);
        if (opened) void sp00ky.closeCrdtField(table, current, field);
      });
    }),
  );

  return crdtField;
}
