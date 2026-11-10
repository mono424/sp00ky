import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChannelPeer, ChannelTableNames, SchemaStructure } from '@spooky-sync/query-builder';
import type {
  ChannelQuery,
  ChannelState,
  ChannelStatus,
  CrdtField,
  Sp00kyChannel,
} from '@spooky-sync/core';
import type { SyncedDb } from './db';
import { useDb, useLatest } from './provider';

/**
 * Open a `@crdt` field of the record `recordId` names, for an editor to bind
 * to (`field?.getDoc()`). Re-opens when the id changes, closes on unmount.
 *
 * `null` until the field is loaded (`CrdtField.isLoaded`): this device had the
 * document, or the server answered. Until then show the column's text instead
 * of binding an editor to a document still arriving.
 */
export function useCrdtField(
  table: string,
  recordId: string | null | undefined,
  field: string
): CrdtField | null {
  const db = useDb();
  const key = recordId ? `${table}\u0000${recordId}\u0000${field}` : null;
  // Tagged with the key it was opened for, so a render after the id changed
  // never hands out the previous record's document.
  const [opened, setOpened] = useState<{ key: string; field: CrdtField } | null>(null);

  useEffect(() => {
    if (!key || !recordId) return;
    const sp00ky = db.getSp00ky();
    let superseded = false;
    let isOpen = false;
    let offLoaded = () => {};
    sp00ky
      .openCrdtField(table, recordId, field)
      .then((cf) => {
        isOpen = true;
        if (superseded) {
          void sp00ky.closeCrdtField(table, recordId, field);
          return;
        }
        // Handed out once loaded: an empty document before the server answered
        // is "not arrived yet", not "empty".
        offLoaded = cf.onLoaded(() => {
          if (!superseded) setOpened({ key, field: cf });
        });
      })
      .catch((err) => {
        // A silent rejection leaves the editor stuck on its fallback with no
        // trail; surface the root cause (missing `@crdt` annotation, codegen
        // drift, a local query failure).
        // oxlint-disable-next-line no-console
        console.error(
          `[useCrdtField] Failed to open CRDT field ${table}.${field} on ${recordId}:`,
          err
        );
      });
    return () => {
      superseded = true;
      offLoaded();
      if (isOpen) void sp00ky.closeCrdtField(table, recordId, field);
      setOpened(null);
    };
  }, [db, key]);

  return opened && opened.key === key ? opened.field : null;
}

/** Who a channel connects to. */
export type ChannelPeers = Iterable<ChannelPeer>;

/** What a channel carries: the rows a query (or several) matches. */
export type ChannelQueries<S extends SchemaStructure, T extends ChannelTableNames<S>> =
  | ChannelQuery<S, T>
  | ReadonlyArray<ChannelQuery<S, T>>;

/** An open channel, for as long as the component is mounted. */
export interface UseChannel<S extends SchemaStructure, T extends string> {
  /** `idle` | `connecting` | `connected` | `failed` | `unsupported`. */
  status: ChannelStatus;
  /** Users with a tab connected. */
  peers: ReadonlyArray<string>;
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
 * Open a peer to peer channel to `peers` for the rows `query` matches, while
 * the component is mounted. The query must be on a `-- @channel` table and
 * only its `where` counts; it is checked against each row, never registered.
 * Writes to matching rows (`db.update` / `db.create` / `db.delete`, or the
 * channel's own) reach the peers over WebRTC in tens of ms; `-- @ephemeral`
 * fields travel only this way. A connection forms when both sides list each
 * other; your own id is skipped.
 *
 * ```tsx
 * const ch = useChannel(db.query('message').where({ conversation: convoId }), [me, other]);
 * ```
 *
 * `query` may be `null` (closes the channel until a query is passed again) or
 * a list of queries. Query and peers are compared by value, so building them
 * inline is fine.
 */
export function useChannel<S extends SchemaStructure, T extends ChannelTableNames<S>>(
  query: ChannelQueries<S, T> | null | undefined,
  peers: ChannelPeers
): UseChannel<S, T> {
  const db = useDb();
  const [state, setState] = useState<ChannelState>(IDLE);
  const channel = useRef<Sp00kyChannel<any, any> | null>(null);

  const qKey = queriesKey(query as ChannelQueries<any, any> | null | undefined);
  const pKey = peersKey(peers);
  // The latest inputs, for the effects below, which key on their values.
  const latest = useLatest({ query, peers });

  const isOpen = qKey !== null;
  useEffect(() => {
    if (!isOpen) return;
    const { query: q, peers: p } = latest.current;
    const ch = (db as SyncedDb<any>).channel(q as any, p);
    channel.current = ch;
    const stop = ch.subscribe(setState);
    return () => {
      stop();
      ch.close();
      if (channel.current === ch) channel.current = null;
      setState(IDLE);
    };
  }, [db, isOpen]);

  // Re-scope and re-peer an open channel in place. Their first run repeats
  // what the channel was just opened with, which is harmless.
  useEffect(() => {
    const q = latest.current.query;
    if (q) channel.current?.setQueries(q as ChannelQueries<any, any>);
  }, [qKey]);
  useEffect(() => {
    channel.current?.setPeers(latest.current.peers);
  }, [pKey]);

  const open = useCallback(() => {
    if (!channel.current) throw new Error('This channel is closed (no query was passed).');
    return channel.current;
  }, []);
  const writes = useMemo(
    () => ({
      update: ((...args: Parameters<Sp00kyChannel<any, any>['update']>) =>
        open().update(...args)) as UseChannel<S, T>['update'],
      create: ((...args: Parameters<Sp00kyChannel<any, any>['create']>) =>
        open().create(...args)) as UseChannel<S, T>['create'],
      delete: ((...args: Parameters<Sp00kyChannel<any, any>['delete']>) =>
        open().delete(...args)) as UseChannel<S, T>['delete'],
    }),
    [open]
  );

  return { status: state.status, peers: state.peers, ...writes };
}
