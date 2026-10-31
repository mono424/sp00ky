/**
 * The live stack for one signed-in session: a {@link LiveClient}, the cache,
 * and the topic collections, plus the adapters screens read through.
 *
 * Built when a session exists and the host advertises the live protocol
 * (`/config.live`); torn down, cache included, on sign-out.
 */

import { createContext, createMemo, createSignal, onCleanup, useContext, type Accessor } from 'solid-js';
import { LiveClient, liveUrl } from './live';
import { TopicCollections, TOPICS, loadPersistedOptions, type DocRow, type TopicCollection } from './collections';
import { clearPersistence, getPersistence } from './persistence';
import type { Overview } from './types';

export interface LiveStack {
  client: LiveClient;
  topics: TopicCollections;
  /** Whether rows survive a reload on this page. */
  cached: boolean;
  dispose(): Promise<void>;
}

export interface LiveStackOptions {
  /** `''` for this page's own origin, otherwise the host's base URL. */
  baseUrl: string;
  token: () => string | null;
  renew?: () => Promise<string | null>;
  onUnauthorized?: () => void;
  /** Keys the cache by host: an endpoint, or a project id in the console. */
  scope: string;
}

export async function createLiveStack(opts: LiveStackOptions): Promise<LiveStack> {
  const persistence = await getPersistence();
  if (persistence) await loadPersistedOptions();
  const client = new LiveClient({
    url: () => liveUrl(opts.baseUrl),
    token: opts.token,
    renew: opts.renew,
    onUnauthorized: opts.onUnauthorized,
  });
  const topics = new TopicCollections(client, opts.scope, persistence);
  return {
    client,
    topics,
    cached: persistence !== null,
    dispose: async () => {
      await topics.dispose();
      client.dispose();
    },
  };
}

/** Tear a stack down and forget everything it cached (sign-out). */
export async function destroyLiveStack(stack: LiveStack | null): Promise<void> {
  await stack?.dispose();
  await clearPersistence();
}

/* ------------------------------------------------------------------ */
/* Reading collections reactively                                       */
/* ------------------------------------------------------------------ */

/**
 * A collection's rows as a Solid signal. Subscribing is what starts the
 * topic's sync; the subscription ends with the owning scope.
 */
export function rowsSignal<T>(collection: TopicCollection): Accessor<T[]> {
  const read = () => [...collection.values()] as unknown as T[];
  const [rows, setRows] = createSignal<T[]>(read(), { equals: false });
  const sub = collection.subscribeChanges(() => setRows(read()), { includeInitialState: true });
  onCleanup(() => sub.unsubscribe());
  return rows;
}

/** A doc topic's value as a Solid signal. */
export function docSignal<T>(collection: TopicCollection): Accessor<T | undefined> {
  const rows = rowsSignal<DocRow<T>>(collection);
  return createMemo(() => rows().find((r) => r.id === 'doc')?.data);
}

/* ------------------------------------------------------------------ */
/* The overview, assembled from topics                                  */
/* ------------------------------------------------------------------ */

type Keyed<T> = Record<string, T>;

/** The `overview` topic: `/overview` with entity lists keyed by id. */
type OverviewDoc = Omit<Overview, 'ssps' | 'backends' | 'operations' | 'server_time_ms'> & {
  ssps?: Keyed<Overview['ssps'][number]>;
  backends?: Keyed<Overview['backends'][number]>;
  operations?: Keyed<NonNullable<Overview['operations']>[number]>;
};

const byId = <T extends { id?: unknown }>(m: Keyed<T> | undefined): T[] =>
  Object.values(m ?? {}).sort((a, b) =>
    String(a.id ?? '').localeCompare(String(b.id ?? ''), undefined, { numeric: true }),
  );

const byKey = <T>(rows: T[], key: string): T[] =>
  [...rows].sort((a, b) => Number((a as Record<string, unknown>)[key]) - Number((b as Record<string, unknown>)[key]));

/**
 * The `Overview` every screen already reads, rebuilt from the `overview` doc
 * and the three `metrics` series. Pure apart from the clock: the host drops
 * its clock readings from the topic (they change every tick and describe
 * nothing), so `server_time_ms` is this browser's.
 */
export function assembleOverview(
  doc: OverviewDoc,
  sync: unknown[],
  presence: unknown[],
  jobs: unknown[],
  now = Date.now(),
): Overview {
  const scheduler = doc.scheduler
    ? {
        ...doc.scheduler,
        heartbeat: doc.scheduler.heartbeat
          ? { ...doc.scheduler.heartbeat, samples: byKey(sync, 'ts') }
          : doc.scheduler.heartbeat,
      }
    : null;
  return {
    ...doc,
    scheduler,
    ssps: byId(doc.ssps),
    backends: byId(doc.backends),
    operations: Object.values(doc.operations ?? {}).sort((a, b) => b.started_at - a.started_at),
    presence: doc.presence ? { ...doc.presence, samples: byKey(presence, 'ts') } : doc.presence,
    jobs: doc.jobs ? { ...doc.jobs, samples: byKey(jobs, 't') } : doc.jobs,
    incidents: doc.incidents ? { ...doc.incidents, server_time_ms: now } : doc.incidents,
    server_time_ms: now,
  } as Overview;
}

/** The live overview: one doc and three series, assembled on change. */
export function liveOverview(topics: TopicCollections): Accessor<Overview | undefined> {
  const doc = docSignal<OverviewDoc>(topics.get(TOPICS.overview));
  const sync = rowsSignal<unknown>(topics.get(TOPICS.syncMetrics));
  const presence = rowsSignal<unknown>(topics.get(TOPICS.presenceMetrics));
  const jobs = rowsSignal<unknown>(topics.get(TOPICS.jobMetrics));
  return createMemo(() => {
    const d = doc();
    return d ? assembleOverview(d, sync(), presence(), jobs()) : undefined;
  });
}

/* ------------------------------------------------------------------ */
/* Context                                                              */
/* ------------------------------------------------------------------ */

const LiveContext = createContext<Accessor<LiveStack | null>>(() => null);

export const LiveProvider = LiveContext.Provider;

/** The session's live stack, or null when polling. */
export function useLive(): Accessor<LiveStack | null> {
  return useContext(LiveContext);
}
