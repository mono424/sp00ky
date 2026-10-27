import { describe, expect, it } from 'vitest';
import type { SyncedDb } from '../..';
import { useChannel } from '../use-channel';

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
  const ch = useChannel(db.query('message').where({ text: 'x' }), ['user:a', 'user:b']);
  void ch.update('message', 'message:1', { typing: Date.now() });
  void ch.create('message:1', { text: 'hi' });
  void ch.delete('message', 'message:1');
  // @ts-expect-error unknown field
  void ch.update('message', 'message:1', { nope: 1 });
  // @ts-expect-error the id must name a carried table
  void ch.create('note:1', {});
  // @ts-expect-error `note` is not a -- @channel table
  useChannel(db.query('note'), []);
  // Accessor, built query, a list, and the explicit opt-out all type-check.
  useChannel(() => db.query('message').where({ text: 'x' }), () => ['user:b']);
  useChannel(db.query('message').build(), []);
  useChannel([db.query('message')], []);
  useChannel(db.query('message').limit(50).ignoreUnusedArgs(), []);
  // @ts-expect-error a table name is not a channel any more
  useChannel('message', []);
}

describe('useChannel types', () => {
  it('compiles', () => {
    expect(typeof typed).toBe('function');
  });
});
