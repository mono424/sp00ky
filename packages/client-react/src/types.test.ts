import { describe, expect, it } from 'vitest';
import type { SyncedDb } from './db';
import { useQuery } from './query';
import { useChannel } from './realtime';

// Type-level checks: `tsc` runs over this file, the body never executes.
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
    {
      name: 'note' as const,
      columns: { id: { type: 'string' as const, optional: false, recordId: true } },
      primaryKey: ['id'] as const,
    },
  ],
  relationships: [],
  backends: {},
} as const;

function typed(db: SyncedDb<typeof schema>) {
  // A list is never null: map without a guard.
  const list = useQuery(db.query('message').build());
  const texts: string[] = list.data.map((m) => m.text);
  // `.one()` is null until the row exists.
  const one = useQuery(db.query('message').one().build());
  // @ts-expect-error possibly null
  void one.data.text;
  void one.data?.text;
  // null skips.
  useQuery(null);
  useQuery(db, db.query('message').build(), { enabled: false, deregisterOnCleanup: true });

  const ch = useChannel(db.query('message').where({ text: 'x' }), ['user:a', 'user:b']);
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
