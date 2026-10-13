import { createEffect, createSignal, type Accessor } from 'solid-js';
import { useDb } from './context';
import type { CrdtField } from '@spooky-sync/core';

/**
 * Open a `@crdt` field of the record `recordId()` names, for an editor to bind
 * to (`field()?.getDoc()`). Re-opens when the id changes; closes on cleanup.
 */
export function useCrdtField(
  table: string,
  recordId: () => string | undefined,
  field: string
): Accessor<CrdtField | null> {
  const db = useDb();
  const [crdtField, setCrdtField] = createSignal<CrdtField | null>(null, { ownedWrite: true });

  // Two-arg Solid 2 effect: the compute tracks `recordId`, the apply owns the
  // open/close lifecycle and returns the cleanup — which runs both when the id
  // changes (before the next apply) and on unmount. That replaces the Solid 1
  // version's manual currentId/initialized bookkeeping.
  createEffect(
    () => recordId(),
    (id) => {
      if (!id) {
        setCrdtField(null);
        return;
      }
      const sp00ky = db.getSp00ky();
      let superseded = false;
      sp00ky
        .openCrdtField(table, id, field)
        .then((cf) => {
          if (!superseded) {
            setCrdtField(cf);
          } else {
            sp00ky.closeCrdtField(table, id, field);
          }
        })
        .catch((err) => {
          // Silent rejections here leave the consumer's `Show when={field()}`
          // permanently stuck on its fallback (typically a static `<p>` with
          // no editing UI), with no error trail. Surface the failure so the
          // root cause (missing `@crdt` annotation, schema codegen drift,
          // local DB query failure, etc.) is visible in the console instead
          // of silently breaking collaborative fields.
          console.error(`[useCrdtField] Failed to open CRDT field ${table}.${field} on ${id}:`, err);
        });
      return () => {
        superseded = true;
        if (crdtField()) {
          sp00ky.closeCrdtField(table, id, field);
          setCrdtField(null);
        }
      };
    }
  );

  return crdtField;
}
