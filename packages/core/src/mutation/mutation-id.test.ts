import { describe, it, expect } from 'vitest';
import { mintMutationId, mutationOwnerTabId } from './mutation-id';

describe('mutationOwnerTabId (raw ids)', () => {
  it('mints with the fallback tab id when none is given', () => {
    const id = mintMutationId();
    expect(id.startsWith('_00_pending_mutations:')).toBe(true);
    expect(mutationOwnerTabId(id)).toMatch(/^[a-z0-9]+$/);
  });
  it('accepts an id without the table prefix', () => {
    expect(mutationOwnerTabId('1700000000000_0001_tab9')).toBe('tab9');
    expect(mutationOwnerTabId('1700000000000')).toBeNull();
  });
});

// The old `_00_pending_mutations:${Date.now()}` collided within a millisecond
// (guaranteed once multiple tabs share a store) and never actually ordered the
// drain. These pin the new id's three properties: unique, sortable, routable.
describe('mutation ids', () => {
  it('never collides within a burst', () => {
    const ids = new Set(Array.from({ length: 1000 }, () => mintMutationId('tabx')));
    expect(ids.size).toBe(1000);
  });

  it('sorts lexicographically in mint order (ORDER BY id is chronological)', () => {
    const ids = Array.from({ length: 200 }, () => mintMutationId('tabx'));
    expect([...ids].sort()).toEqual(ids);
  });

  it('routes back to the owning tab', () => {
    expect(mutationOwnerTabId(mintMutationId('tab-abc'))).toBe('tab-abc');
  });

  it('returns null for a legacy numeric id instead of misrouting', () => {
    expect(mutationOwnerTabId('_00_pending_mutations:1753875000000')).toBeNull();
  });
});
