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
    expect(takeReloadSalt('user:a')).toBe('s1');
    // Taken, not read: a tab duplicated from this page finds nothing.
    expect(takeReloadSalt('user:a')).toBeNull();
  });

  it('another principal, or none, mints a fresh salt', () => {
    leaveReloadSalt('s1', 'user:a');
    expect(takeReloadSalt('user:b')).toBeNull();
    leaveReloadSalt('s2', null);
    expect(takeReloadSalt(null)).toBe('s2');
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
