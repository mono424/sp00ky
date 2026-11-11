import type { Signal } from '@angular/core';
import { describe, expect, it } from 'vitest';
import {
  injectChannel,
  injectFileUpload,
  injectQuery,
  type SyncedDb,
} from '@spooky-sync/client-angular';

// Type-level checks: `pnpm type-check` runs tsc over this file, the body never executes.
const schema = {
  tables: [
    {
      name: 'message' as const,
      columns: {
        id: { type: 'string' as const, optional: false, recordId: true },
        text: { type: 'string' as const, optional: false },
        typing: { type: 'number' as const, optional: true, ephemeral: true },
      },
      channel: true as const,
      primaryKey: ['id'] as const,
    },
  ],
  relationships: [],
  backends: {},
} as const;

function typed(db: SyncedDb<typeof schema>, convo: Signal<string>) {
  // A list is never null, built from a function or passed as is.
  const list = injectQuery(() => db.query('message').where({ text: convo() }).build());
  const texts: string[] = list.data().map((m) => m.text);
  const fixed = injectQuery(db.query('message').build());
  void fixed.data().length;
  // `.one()` is null until the row exists.
  const one = injectQuery(() => db.query('message').one().build());
  // @ts-expect-error possibly null
  void one.data().text;
  void one.data()?.text;
  // A function may skip with null.
  injectQuery(() => (convo() ? db.query('message').build() : null));
  injectQuery(() => db.query('message').build(), {
    enabled: () => true,
    deregisterOnCleanup: true,
  });
  // @ts-expect-error results are read-only signals
  list.isLoading.set(true);

  const ch = injectChannel(
    () => db.query('message').where({ text: convo() }),
    () => ['user:a', 'user:b']
  );
  void ch.update('message', 'message:1', { typing: Date.now() });
  void ch.create('message:1', { text: 'hi' });
  // @ts-expect-error unknown field
  void ch.update('message', 'message:1', { nope: 1 });
  injectChannel(null, []);
  injectChannel([db.query('message')], []);
  return texts;
}

describe('types', () => {
  it('compiles', () => {
    expect(typeof typed).toBe('function');
  });
});

// File functions take any bucket name until the schema is named, then only its buckets.
const withBuckets = {
  ...schema,
  buckets: [{ name: 'avatars' as const, maxSize: 1024 }],
} as const;
export function files() {
  injectFileUpload('anything');
  injectFileUpload<typeof withBuckets>('avatars');
  // @ts-expect-error not a bucket of the schema
  injectFileUpload<typeof withBuckets>('covers');
}
