import { untrack } from 'svelte';
import type { ChannelPeer, ChannelTableNames, SchemaStructure } from '@spooky-sync/query-builder';
import type {
  ChannelQuery,
  ChannelState,
  ChannelStatus,
  CrdtField,
  Sp00kyChannel,
} from '@spooky-sync/core';
import type { SyncedDb } from './db.js';
import { read, useDb } from './context.js';
import type { ReactiveValue } from './status.svelte.js';
import type { MaybeGetter } from './types.js';

/**
 * Open a `@crdt` field of the record `recordId` names, for an editor to bind
 * to (`field.current?.getDoc()`). Re-opens when the id changes, closes with the
 * component. Pass the id as a getter (`() => postId`) to follow state.
 *
 * `.current` stays `null` until the field is loaded (`CrdtField.isLoaded`):
 * this device had the document, or the server answered. Until then show the
 * column's text instead of binding an editor to a document still arriving.
 */
export function useCrdtField(
  table: string,
  recordId: MaybeGetter<string | null | undefined>,
  field: string,
  db: SyncedDb<any> = useDb()
): ReactiveValue<CrdtField | null> {
  let current = $state.raw<CrdtField | null>(null);

  $effect(() => {
    const id = read(recordId);
    if (!id) return;
    const sp00ky = db.getSp00ky();
    let superseded = false;
    let isOpen = false;
    let offLoaded = () => {};
    sp00ky
      .openCrdtField(table, id, field)
      .then((cf) => {
        isOpen = true;
        if (superseded) {
          void sp00ky.closeCrdtField(table, id, field);
          return;
        }
        // Handed out once loaded: an empty document before the server answered
        // is "not arrived yet", not "empty".
        offLoaded = cf.onLoaded(() => {
          if (!superseded) current = cf;
        });
      })
      .catch((err) => {
        // A silent rejection leaves the editor stuck on its fallback with no
        // trail; surface the root cause (missing `@crdt` annotation, codegen
        // drift, a local query failure).
        // oxlint-disable-next-line no-console
        console.error(`[useCrdtField] Failed to open CRDT field ${table}.${field} on ${id}:`, err);
      });
    return () => {
      superseded = true;
      offLoaded();
      if (isOpen) void sp00ky.closeCrdtField(table, id, field);
      current = null;
    };
  });

  return {
    get current() {
      return current;
    },
  };
}

/** Who a channel connects to. */
export type ChannelPeers = Iterable<ChannelPeer>;

/** What a channel carries: the rows a query (or several) matches. */
export type ChannelQueries<S extends SchemaStructure, T extends ChannelTableNames<S>> =
  | ChannelQuery<S, T>
  | ReadonlyArray<ChannelQuery<S, T>>;

/** An open channel, for as long as the component lives. Fields are reactive getters. */
export interface UseChannel<S extends SchemaStructure, T extends string> {
  /** `idle` | `connecting` | `connected` | `failed` | `unsupported`. */
  readonly status: ChannelStatus;
  /** Users with a tab connected. */
  readonly peers: ReadonlyArray<string>;
  /** Writes that go to THIS channel's peers only (when the row matches its query), plus the database as always. */
  update: Sp00kyChannel<S, T>['update'];
  create: Sp00kyChannel<S, T>['create'];
  delete: Sp00kyChannel<S, T>['delete'];
}

const IDLE: ChannelState = { status: 'idle', peers: [] };

/** Identity of a channel's queries: their hashes (a builder is built first). */
function queriesKey(q: ChannelQueries<any, any> | null | undefined): string | null {
  if (!q) return null;
  const list = (Array.isArray(q) ? q : [q]) as ReadonlyArray<any>;
  return list
    .map((item) =>
      String('build' in item && typeof item.build === 'function' ? item.build().hash : item.hash)
    )
    .join(',');
}

const peersKey = (peers: ChannelPeers): string => [...peers].map((p) => String(p)).join('\u0000');

/**
 * Open a peer to peer channel to `peers` for the rows `query` matches, for the
 * lifetime of the component. The query must be on a `-- @channel` table and
 * only its `where` counts; it is checked against each row, never registered.
 * Writes to matching rows (`db.update` / `db.create` / `db.delete`, or the
 * channel's own) reach the peers over WebRTC in tens of ms; `-- @ephemeral`
 * fields travel only this way. A connection forms when both sides list each
 * other; your own id is skipped.
 *
 * ```ts
 * const ch = useChannel(
 *   () => db.query('message').where({ conversation: convoId }),
 *   () => [me, other]
 * );
 * ```
 *
 * Both arguments may be getters (re-read when what they read changes) and are
 * compared by value, so rebuilding them is free. A `null` query closes the
 * channel until a query comes back.
 */
export function useChannel<S extends SchemaStructure, T extends ChannelTableNames<S>>(
  query: MaybeGetter<ChannelQueries<S, T> | null | undefined>,
  peers: MaybeGetter<ChannelPeers | null | undefined>,
  db: SyncedDb<any> = useDb()
): UseChannel<S, T> {
  let state = $state.raw<ChannelState>(IDLE);
  let channel: Sp00kyChannel<any, any> | null = null;

  const readQuery = () => read(query) as ChannelQueries<any, any> | null | undefined;
  const readPeers = () => read(peers) ?? [];
  const qKey = $derived(queriesKey(readQuery()));
  const pKey = $derived(peersKey(readPeers()));
  const isOpen = $derived(qKey !== null);

  $effect(() => {
    if (!isOpen) return;
    const ch = db.channel(untrack(readQuery)!, untrack(readPeers));
    channel = ch;
    const stop = ch.subscribe((s) => {
      state = s;
    });
    return () => {
      stop();
      ch.close();
      if (channel === ch) channel = null;
      state = IDLE;
    };
  });

  // Re-scope and re-peer an open channel in place. Their first run repeats
  // what the channel was just opened with, which is harmless.
  $effect(() => {
    void qKey;
    const q = untrack(readQuery);
    if (q) channel?.setQueries(q);
  });
  $effect(() => {
    void pKey;
    channel?.setPeers(untrack(readPeers));
  });

  const open = () => {
    if (!channel)
      throw new Error('This channel is closed (its query is null, or the component is gone).');
    return channel;
  };
  return {
    get status() {
      return state.status;
    },
    get peers() {
      return state.peers;
    },
    update: ((...args: Parameters<Sp00kyChannel<any, any>['update']>) =>
      open().update(...args)) as UseChannel<S, T>['update'],
    create: ((...args: Parameters<Sp00kyChannel<any, any>['create']>) =>
      open().create(...args)) as UseChannel<S, T>['create'],
    delete: ((...args: Parameters<Sp00kyChannel<any, any>['delete']>) =>
      open().delete(...args)) as UseChannel<S, T>['delete'],
  };
}
