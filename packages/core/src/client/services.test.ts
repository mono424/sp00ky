import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { leaveReloadSalt, takeReloadSalt } from './services';

class MemoryStorage {
  private items = new Map<string, string>();
  getItem(key: string): string | null {
    return this.items.get(key) ?? null;
  }
  setItem(key: string, value: string): void {
    this.items.set(key, value);
  }
  removeItem(key: string): void {
    this.items.delete(key);
  }
}

describe('reload salt', () => {
  const g = globalThis as { sessionStorage?: unknown };
  beforeEach(() => {
    g.sessionStorage = new MemoryStorage();
  });
  afterEach(() => {
    delete g.sessionStorage;
  });

  it('a salt left on the way out is taken once, by the same principal', () => {
    leaveReloadSalt('s1', 'user:a');
    expect(takeReloadSalt('user:a')).toEqual({ salt: 's1', sessionIds: [], owner: null });
    // Taken, not read: a tab duplicated from this page finds nothing.
    expect(takeReloadSalt('user:a')).toBeNull();
  });

  it("the page's socket session ids travel with the salt", () => {
    // The reloaded page's views keep edges stamped by these sockets, and the
    // anonymous LIVE only hears sessions it knows.
    leaveReloadSalt('s1', null, ['sid-1', 'sid-2']);
    expect(takeReloadSalt(null)).toEqual({ salt: 's1', sessionIds: ['sid-1', 'sid-2'], owner: null });
    (g.sessionStorage as MemoryStorage).setItem('sp00ky:salt', JSON.stringify({ salt: 's2', authId: null, sessionIds: ['ok', 7, null] }));
    expect(takeReloadSalt(null)).toEqual({ salt: 's2', sessionIds: ['ok'], owner: null });
  });

  it("the page's owner secret travels with the salt, so the reloaded page still owns its anonymous views", () => {
    leaveReloadSalt('s1', null, [], 'secret-1');
    expect(takeReloadSalt(null)).toEqual({ salt: 's1', sessionIds: [], owner: 'secret-1' });
    // Gone with the salt: a duplicated tab mints its own.
    expect(takeReloadSalt(null)).toBeNull();
    // A different principal re-salts, and the secret is not handed over either.
    leaveReloadSalt('s2', 'user:a', [], 'secret-2');
    expect(takeReloadSalt(null)).toBeNull();
    for (const owner of ['', 7, null]) {
      (g.sessionStorage as MemoryStorage).setItem('sp00ky:salt', JSON.stringify({ salt: 's3', authId: null, owner }));
      expect(takeReloadSalt(null)?.owner).toBeNull();
    }
  });

  it('another principal, or none, mints a fresh salt', () => {
    leaveReloadSalt('s1', 'user:a');
    expect(takeReloadSalt('user:b')).toBeNull();
    leaveReloadSalt('s2', null);
    expect(takeReloadSalt(null)?.salt).toBe('s2');
    leaveReloadSalt('s3', null);
    expect(takeReloadSalt('user:a')).toBeNull();
  });

  it('garbage or missing storage is a fresh salt, never a throw', () => {
    (g.sessionStorage as MemoryStorage).setItem('sp00ky:salt', '{not json');
    expect(takeReloadSalt(null)).toBeNull();
    (g.sessionStorage as MemoryStorage).setItem('sp00ky:salt', JSON.stringify({ salt: '', authId: null }));
    expect(takeReloadSalt(null)).toBeNull();
    delete g.sessionStorage;
    expect(takeReloadSalt(null)).toBeNull();
    expect(() => leaveReloadSalt('s1', null)).not.toThrow();
  });
});
