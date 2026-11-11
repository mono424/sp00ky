import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { describe, expect, it, vi } from 'vitest';
import { injectChannel, injectCrdtField, SyncedDb } from '@spooky-sync/client-angular';
import { inject, provideDb, tick } from './helpers';

describe('injectCrdtField', () => {
  it('opens once loaded, re-opens on a new id, closes the previous one', async () => {
    const opened: string[] = [];
    const closed: string[] = [];
    const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
    (db as any).sp00ky = {
      openCrdtField: vi.fn(async (_t: string, id: string) => {
        opened.push(id);
        return { id, onLoaded: (cb: () => void) => (cb(), () => {}) };
      }),
      closeCrdtField: vi.fn(async (_t: string, id: string) => {
        closed.push(id);
      }),
    };
    provideDb(db);
    const id = signal<string | null>('post:1');
    const field = inject(() => injectCrdtField('post', id, 'body'));
    expect(field()).toBeNull();
    await tick();
    expect((field() as any)?.id).toBe('post:1');

    id.set('post:2');
    TestBed.tick();
    expect(field()).toBeNull();
    await tick();
    expect(closed).toEqual(['post:1']);
    expect((field() as any)?.id).toBe('post:2');
    expect(opened).toEqual(['post:1', 'post:2']);
  });
});

describe('injectChannel', () => {
  it('opens for a query, re-peers in place, closes when the query goes null', () => {
    const subs = new Set<(s: any) => void>();
    const ch = {
      subscribe: vi.fn((cb: (s: any) => void) => {
        subs.add(cb);
        return () => subs.delete(cb);
      }),
      close: vi.fn(),
      setQueries: vi.fn(),
      setPeers: vi.fn(),
      update: vi.fn(async () => {}),
    };
    const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
    const open = vi.fn(() => ch);
    (db as any).channel = open;
    provideDb(db);

    const q = signal<any>({ hash: 'q1' });
    const peers = signal(['user:a', 'user:b']);
    const channel = inject(() => injectChannel(q, peers));
    expect(open).toHaveBeenCalledTimes(1);
    expect(channel.status()).toBe('idle');
    for (const cb of subs) cb({ status: 'connected', peers: ['user:b'] });
    expect(channel.status()).toBe('connected');
    expect(channel.peers()).toEqual(['user:b']);

    peers.set(['user:a', 'user:c']);
    TestBed.tick();
    expect(ch.setPeers).toHaveBeenLastCalledWith(['user:a', 'user:c']);
    expect(open).toHaveBeenCalledTimes(1);

    void (channel.update as any)('message', 'message:1', { draft: 'x' });
    expect(ch.update).toHaveBeenCalled();

    q.set(null);
    TestBed.tick();
    expect(ch.close).toHaveBeenCalled();
    expect(channel.status()).toBe('idle');
    expect(() => (channel.update as any)('message', 'message:1', {})).toThrow(/closed/);
  });
});
