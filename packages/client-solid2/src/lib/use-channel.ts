import { createEffect, createSignal, onCleanup, untrack } from 'solid-js';
import type { ChannelPeer, ChannelTableNames, SchemaStructure } from '@spooky-sync/query-builder';
import type { ChannelQuery, ChannelState, ChannelStatus, Sp00kyChannel } from '@spooky-sync/core';
import type { SyncedDb } from '..';
import { useDb } from './context';

/** Who a channel connects to: a list, or an accessor of one (re-read when it changes). */
export type ChannelPeers = Iterable<ChannelPeer> | (() => Iterable<ChannelPeer> | null | undefined);

/** What a channel carries: the rows a query (or several) matches. */
export type ChannelQueries<S extends SchemaStructure, T extends ChannelTableNames<S>> =
  | ChannelQuery<S, T>
  | ReadonlyArray<ChannelQuery<S, T>>;

/** An open channel, for as long as the component lives. */
export interface UseChannel<S extends SchemaStructure, T extends string> {
  /** `idle` | `connecting` | `connected` | `failed` | `unsupported`. */
  status: () => ChannelStatus;
  /** Users with a tab connected. */
  peers: () => ReadonlyArray<string>;
  /** Writes that go to THIS channel's peers only (when the row matches its query), plus the database as always. */
  update: Sp00kyChannel<S, T>['update'];
  create: Sp00kyChannel<S, T>['create'];
  delete: Sp00kyChannel<S, T>['delete'];
}

const IDLE: ChannelState = { status: 'idle', peers: [] };

/**
 * Open a peer to peer channel to `peers` for the rows `query` matches, for
 * the lifetime of the component. The query must be on a `-- @channel` table
 * and only its `where` counts; it is checked against each row, never
 * registered. Writes to matching rows (`db.update` / `db.create` /
 * `db.delete`, or the channel's own) reach the peers over WebRTC in tens of
 * ms; `-- @ephemeral` fields travel only this way. A connection forms when
 * both sides list each other; your own id is skipped.
 *
 * ```ts
 * const ch = useChannel(
 *   () => db.query('message').where({ conversation: convoId() }),
 *   () => [me(), other()]
 * );
 * ```
 *
 * `query` may be an accessor (re-read when it changes; `null` closes the
 * channel until it returns a query again) or a list of queries.
 */
export function useChannel<S extends SchemaStructure, T extends ChannelTableNames<S>>(
  query: ChannelQueries<S, T> | (() => ChannelQueries<S, T> | null | undefined),
  peers: ChannelPeers
): UseChannel<S, T> {
  const db = useDb() as SyncedDb<any>;
  const queries = query as ChannelQueries<any, any> | (() => ChannelQueries<any, any> | null | undefined);
  const readQueries = () => (typeof queries === 'function' ? queries() : queries);
  const readPeers = () => (typeof peers === 'function' ? (peers() ?? []) : peers);
  const [state, setState] = createSignal<ChannelState>(IDLE, { ownedWrite: true });

  let channel: Sp00kyChannel<any, any> | null = null;
  let stop = () => {};
  const close = () => {
    stop();
    channel?.close();
    channel = null;
    setState(IDLE);
  };
  const sync = (q: ChannelQueries<any, any> | null | undefined) => {
    if (!q) return close();
    if (channel) return channel.setQueries(q);
    channel = (db as SyncedDb<any>).channel(q, untrack(readPeers));
    stop = channel.subscribe(setState);
  };
  sync(untrack(readQueries));

  // Two-arg Solid 2 effects: the compute tracks, the apply acts (its first
  // run repeats what the channel was opened with, which is harmless).
  if (typeof queries === 'function') createEffect(() => queries(), (q) => sync(q));
  if (typeof peers === 'function') createEffect(() => [...readPeers()], (next) => channel?.setPeers(next));

  onCleanup(close);

  const open = () => {
    if (!channel) throw new Error('This channel is closed (its query accessor returned nothing).');
    return channel;
  };
  return {
    status: () => state().status,
    peers: () => state().peers,
    update: ((...args: Parameters<Sp00kyChannel<any, any>['update']>) => open().update(...args)) as UseChannel<S, T>['update'],
    create: ((...args: Parameters<Sp00kyChannel<any, any>['create']>) => open().create(...args)) as UseChannel<S, T>['create'],
    delete: ((...args: Parameters<Sp00kyChannel<any, any>['delete']>) => open().delete(...args)) as UseChannel<S, T>['delete'],
  };
}
