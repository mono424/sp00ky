import { computed, effect, type Signal, signal, untracked } from '@angular/core';
import type { ChannelPeer, ChannelTableNames, SchemaStructure } from '@spooky-sync/query-builder';
import type {
  ChannelQuery,
  ChannelState,
  ChannelStatus,
  CrdtField,
  Sp00kyChannel,
} from '@spooky-sync/core';
import { engineDb, inContext, read } from './internal';
import type { InjectOptions, MaybeSignal } from './types';

/**
 * Open a `@crdt` field of the record `recordId` names, for an editor to bind
 * to (`field()?.getDoc()`). Re-opens when the id changes, closes with the
 * injection context. Pass the id as a signal (an `input()`) or a function to
 * follow state.
 *
 * The signal stays `null` until the field is loaded (`CrdtField.isLoaded`):
 * this device had the document, or the server answered. Until then show the
 * column's text instead of binding an editor to a document still arriving.
 */
export function injectCrdtField(
  table: string,
  recordId: MaybeSignal<string | null | undefined>,
  field: string,
  options?: InjectOptions
): Signal<CrdtField | null> {
  return inContext(injectCrdtField, options?.injector, () => {
    const sp00ky = engineDb('injectCrdtField').getSp00ky();
    const current = signal<CrdtField | null>(null);

    effect(
      (onCleanup) => {
        const id = read(recordId);
        if (!id) return;
        let superseded = false;
        let isOpen = false;
        let offLoaded = () => {};
        onCleanup(() => {
          superseded = true;
          offLoaded();
          if (isOpen) void sp00ky.closeCrdtField(table, id, field);
          current.set(null);
        });
        sp00ky
          .openCrdtField(table, id, field)
          .then((cf) => {
            isOpen = true;
            if (superseded) {
              void sp00ky.closeCrdtField(table, id, field);
              return;
            }
            // Handed out once loaded: an empty document before the server
            // answered is "not arrived yet", not "empty".
            offLoaded = cf.onLoaded(() => {
              if (!superseded) current.set(cf);
            });
          })
          .catch((err) => {
            // A silent rejection leaves the editor stuck on its fallback with
            // no trail; surface the root cause (missing `@crdt` annotation,
            // codegen drift, a local query failure).
            // oxlint-disable-next-line no-console
            console.error(
              `[injectCrdtField] Failed to open CRDT field ${table}.${field} on ${id}:`,
              err
            );
          });
      },
      { debugName: 'injectCrdtField' }
    );

    return current.asReadonly();
  });
}

/** Who a channel connects to. */
export type ChannelPeers = Iterable<ChannelPeer>;

/** What a channel carries: the rows a query (or several) matches. */
export type ChannelQueries<S extends SchemaStructure, T extends ChannelTableNames<S>> =
  | ChannelQuery<S, T>
  | ReadonlyArray<ChannelQuery<S, T>>;

/** An open channel, for as long as the injection context lives. */
export interface ChannelRef<S extends SchemaStructure, T extends string> {
  /** `idle` | `connecting` | `connected` | `failed` | `unsupported`. */
  readonly status: Signal<ChannelStatus>;
  /** Users with a tab connected. */
  readonly peers: Signal<ReadonlyArray<string>>;
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
 * lifetime of the injection context. The query must be on a `-- @channel`
 * table and only its `where` counts; it is checked against each row, never
 * registered. Writes to matching rows (`db.update` / `db.create` /
 * `db.delete`, or the channel's own) reach the peers over WebRTC in tens of
 * ms; `-- @ephemeral` fields travel only this way. A connection forms when both
 * sides list each other; your own id is skipped.
 *
 * ```ts
 * readonly channel = injectChannel(
 *   () => this.db.query('message').where({ conversation: this.convoId() }),
 *   () => [this.me(), this.other()]
 * );
 * ```
 *
 * Both arguments may be signals or functions and are compared by value, so
 * rebuilding them is free. A `null` query closes the channel until a query
 * comes back.
 */
export function injectChannel<S extends SchemaStructure, T extends ChannelTableNames<S>>(
  query: MaybeSignal<ChannelQueries<S, T> | null | undefined>,
  peers: MaybeSignal<ChannelPeers | null | undefined>,
  options?: InjectOptions
): ChannelRef<S, T> {
  return inContext(injectChannel, options?.injector, () => {
    const db = engineDb('injectChannel');
    const state = signal<ChannelState>(IDLE);
    let channel: Sp00kyChannel<any, any> | null = null;

    const readQuery = () => read(query) as ChannelQueries<any, any> | null | undefined;
    const readPeers = () => read(peers) ?? [];
    const qKey = computed(() => queriesKey(readQuery()));
    const pKey = computed(() => peersKey(readPeers()));
    const isOpen = computed(() => qKey() !== null);

    effect(
      (onCleanup) => {
        if (!isOpen()) return;
        const ch = untracked(() => db.channel(readQuery()!, readPeers()));
        channel = ch;
        const stop = ch.subscribe((s) => state.set(s));
        onCleanup(() => {
          stop();
          ch.close();
          if (channel === ch) channel = null;
          state.set(IDLE);
        });
      },
      { debugName: 'injectChannel' }
    );

    // Re-scope and re-peer an open channel in place. Their first run repeats
    // what the channel was just opened with, which is harmless.
    effect(() => {
      qKey();
      const q = untracked(readQuery);
      if (q) channel?.setQueries(q);
    });
    effect(() => {
      pKey();
      channel?.setPeers(untracked(readPeers));
    });

    const open = () => {
      if (!channel)
        throw new Error('This channel is closed (its query is null, or its context is destroyed).');
      return channel;
    };
    return {
      status: computed(() => state().status),
      peers: computed(() => state().peers),
      update: ((...args: Parameters<Sp00kyChannel<any, any>['update']>) =>
        open().update(...args)) as ChannelRef<S, T>['update'],
      create: ((...args: Parameters<Sp00kyChannel<any, any>['create']>) =>
        open().create(...args)) as ChannelRef<S, T>['create'],
      delete: ((...args: Parameters<Sp00kyChannel<any, any>['delete']>) =>
        open().delete(...args)) as ChannelRef<S, T>['delete'],
    };
  });
}
