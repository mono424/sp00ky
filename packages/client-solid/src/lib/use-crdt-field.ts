import { createEffect, createSignal, onCleanup, useContext, type Accessor } from 'solid-js';
import { Sp00kyContext } from './context';
import type { CrdtField } from '@spooky-sync/core';

/**
 * Open a `@crdt` field of the record `recordId()` names, for an editor to bind
 * to (`field()?.getDoc()`). Re-opens when the id changes; closes on cleanup.
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
  let currentId: string | undefined;
  let initialized = false;

  createEffect(() => {
    const id = recordId();

    // Skip if the ID hasn't changed (but allow the first non-undefined value through)
    if (initialized && id === currentId) return;

    // Close previous field
    if (currentId && crdtField()) {
      db.getSp00ky().closeCrdtField(table, currentId, field);
      setCrdtField(null);
    }

    currentId = id;
    initialized = true;

    if (!id) return;

    const sp00ky = db.getSp00ky();
    sp00ky
      .openCrdtField(table, id, field)
      .then((cf) => {
        if (currentId === id) {
          setCrdtField(cf);
        }
      })
      .catch((err) => {
        // Silent rejections here leave the consumer's `Show when={field()}`
        // permanently stuck on its fallback (typically a static `<p>` with
        // no editing UI), with no error trail. Surface the failure so the
        // root cause (missing `@crdt` annotation, schema codegen drift,
        // local DB query failure, etc.) is visible in the console instead
        // of silently breaking collaborative fields.
        console.error(
          `[useCrdtField] Failed to open CRDT field ${table}.${field} on ${id}:`,
          err,
        );
      });
  });

  onCleanup(() => {
    if (currentId && crdtField()) {
      db.getSp00ky().closeCrdtField(table, currentId, field);
      setCrdtField(null);
    }
  });

  return crdtField;
}
