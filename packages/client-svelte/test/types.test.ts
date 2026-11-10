import { describe, expect, it } from 'vitest';
import type { SyncedDb } from '../src/db.js';
import { useQuery } from '../src/query.svelte.js';
import { useChannel } from '../src/realtime.svelte.js';

// Type-level checks: svelte-check runs over this file, the body never executes.
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

function typed(db: SyncedDb<typeof schema>, convo: () => string) {
  // A list is never null, built from a getter or passed as is.
  const list = useQuery(() => db.query('message').where({ text: convo() }).build());
  const texts: string[] = list.data.map((m) => m.text);
  const fixed = useQuery(db.query('message').build());
  void fixed.data.length;
  // `.one()` is null until the row exists.
  const one = useQuery(() => db.query('message').one().build());
  // @ts-expect-error possibly null
  void one.data.text;
  void one.data?.text;
  // A getter may skip with null.
  useQuery(() => (convo() ? db.query('message').build() : null));
  useQuery(db, () => db.query('message').build(), {
    enabled: () => true,
    deregisterOnCleanup: true,
  });
  // @ts-expect-error results are read-only
  list.isLoading = true;

  const ch = useChannel(
    () => db.query('message').where({ text: convo() }),
    () => ['user:a', 'user:b']
  );
  void ch.update('message', 'message:1', { typing: Date.now() });
  void ch.create('message:1', { text: 'hi' });
  // @ts-expect-error unknown field
  void ch.update('message', 'message:1', { nope: 1 });
  useChannel(null, []);
  useChannel([db.query('message')], []);
  return texts;
}

describe('types', () => {
  it('compiles', () => {
    expect(typeof typed).toBe('function');
  });
});
