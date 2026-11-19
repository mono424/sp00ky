import { describe, it, expect } from 'vitest';
import { AheadStore, type AheadHost, type YieldChannel } from './rows-yield';
import { ROWS_PREWARM_OPEN_ID } from './rows-prewarm';

/** BroadcastChannels of one name: a message reaches every channel but the one that sent it. */
function hub(): () => YieldChannel {
  const channels: YieldChannel[] = [];
  return () => {
    const channel: YieldChannel = {
      onmessage: null,
      postMessage: (data) => {
        for (const other of channels) if (other !== channel) other.onmessage?.({ data } as MessageEvent);
      },
    };
    channels.push(channel);
    return channel;
  };
}

/** A worker as `AheadStore` sees it: an open bucket, a queue of ops, a page. */
function worker(channel: (() => YieldChannel) | null, bucket: string | null = 'user_a') {
  const state = {
    bucket,
    queue: [] as (() => void)[],
    closed: 0,
    told: [] as { id: number; ok: false; error: string }[],
    /** Run what waits in the op queue. */
    run() {
      for (const fn of state.queue.splice(0)) fn();
    },
  };
  const host: AheadHost = {
    channel: () => channel?.() ?? null,
    openBucket: () => state.bucket,
    enqueue: (fn) => void state.queue.push(fn),
    closeStore: () => {
      state.closed++;
      state.bucket = null;
    },
    tellPage: (message) => void state.told.push(message),
  };
  return { state, store: new AheadStore(host) };
}

describe('a store opened ahead of its page', () => {
  it('is given to the worker that leads, and the page is told under the open\'s id', () => {
    const channel = hub();
    const held = worker(channel);
    const leader = worker(channel, null);
    held.store.openedAhead();
    leader.store.ask('user_a');
    // In turn with the ops: nothing is closed under a read that is running.
    expect(held.state.closed).toBe(0);
    held.state.run();
    expect(held.state.closed).toBe(1);
    expect(held.state.told).toEqual([{ id: ROWS_PREWARM_OPEN_ID, ok: false, error: expect.stringContaining('gave the store') }]);
    // Asked again (the leader's next attempt): there is nothing left to give.
    leader.store.ask('user_a');
    held.state.run();
    expect(held.state.closed).toBe(1);
    expect(held.state.told.length).toBe(1);
  });

  it('stays with a page that has claimed it', () => {
    const channel = hub();
    const held = worker(channel);
    const leader = worker(channel, null);
    held.store.openedAhead();
    held.store.claim();
    leader.store.ask('user_a');
    held.state.run();
    expect(held.state.closed).toBe(0);
    expect(held.state.told).toEqual([]);
  });

  it('stays with a page that claims it while the hand-over waits its turn', () => {
    const channel = hub();
    const held = worker(channel);
    const leader = worker(channel, null);
    held.store.openedAhead();
    leader.store.ask('user_a');
    expect(held.state.queue.length).toBe(1);
    held.store.claim();
    held.state.run();
    expect(held.state.closed).toBe(0);
    expect(held.state.told).toEqual([]);
  });

  it('is not given up for another bucket, nor by a worker whose page opened it itself', () => {
    const channel = hub();
    const held = worker(channel);
    const owned = worker(channel);
    const leader = worker(channel, null);
    held.store.openedAhead();
    // `owned` never opened ahead: its store was asked for by its page.
    owned.store.claim();
    owned.store.openedAhead();
    leader.store.ask('user_b');
    held.state.run();
    owned.state.run();
    expect(held.state.closed).toBe(0);
    leader.store.ask('user_a');
    held.state.run();
    owned.state.run();
    expect(held.state.closed).toBe(1);
    expect(owned.state.closed).toBe(0);
  });

  it('is asked for again when the first asking came while it was still being opened', () => {
    const channel = hub();
    const held = worker(channel, null);
    const leader = worker(channel, null);
    // The open is under way: no store yet, and nobody listening.
    held.store.openedAhead();
    leader.store.ask('user_a');
    held.state.run();
    expect(held.state.closed).toBe(0);
    held.state.bucket = 'user_a';
    held.store.openedAhead();
    // The leader's next failed attempt.
    leader.store.ask('user_a');
    held.state.run();
    expect(held.state.closed).toBe(1);
  });

  it('does nothing where there is no channel to ask on', () => {
    const held = worker(null);
    held.store.openedAhead();
    expect(() => held.store.ask('user_a')).not.toThrow();
    held.state.run();
    expect(held.state.closed).toBe(0);
    // Nor when the channel throws.
    const broken = worker(() => ({
      onmessage: null,
      postMessage: () => {
        throw new Error('closed');
      },
    }));
    expect(() => broken.store.ask('user_a')).not.toThrow();
  });
});
