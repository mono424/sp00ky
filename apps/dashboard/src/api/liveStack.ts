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
import type { JobSummary, JobsListResponse, LogLine, Overview, WorkflowRun } from './types';

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

/* ------------------------------------------------------------------ */
/* Live views for the pages that used to stream or poll on their own    */
/* ------------------------------------------------------------------ */

/**
 * A live view while the socket carries data, `undefined` otherwise (no live
 * host, or a socket that never got through): the page then keeps its own
 * stream or poll. Re-evaluated only when that answer flips.
 */
export function useLiveView<T>(make: (topics: TopicCollections) => Accessor<T>): Accessor<Accessor<T> | undefined> {
  const live = useLive();
  const usable = createMemo(() => {
    const st = live();
    return !!st && st.client.status() !== 'blocked';
  });
  return createMemo(() => (usable() ? make(live()!.topics) : undefined));
}

const newestFirst = <T>(rows: T[], key: string): T[] =>
  [...rows].sort((a, b) => {
    const x = (a as Record<string, unknown>)[key];
    const y = (b as Record<string, unknown>)[key];
    const nx = typeof x === 'number' ? x : Date.parse(String(x ?? ''));
    const ny = typeof y === 'number' ? y : Date.parse(String(y ?? ''));
    return (Number.isFinite(ny) ? ny : 0) - (Number.isFinite(nx) ? nx : 0);
  });

/** `GET /jobs` as the `jobs` topic and its sparkline series serve it. */
export function liveJobsView(topics: TopicCollections): Accessor<JobsListResponse | undefined> {
  const col = topics.get(TOPICS.jobs);
  const rows = rowsSignal<JobSummary>(col);
  const samples = rowsSignal<unknown>(topics.get(TOPICS.jobMetrics));
  return createMemo(() => {
    const meta = col.utils.meta() as Partial<JobsListResponse> & { totals?: JobsListResponse['totals'] };
    if (!meta.totals) return undefined;
    return {
      ...meta,
      jobs: newestFirst(rows(), 'created_at'),
      totals: { ...meta.totals, samples: byKey(samples(), 't') },
    } as JobsListResponse;
  });
}

/** The newest workflow runs, newest first. */
export function liveRunsView(topics: TopicCollections): Accessor<WorkflowRun[]> {
  const rows = rowsSignal<WorkflowRun>(topics.get(TOPICS.runs));
  return createMemo(() => newestFirst(rows(), 'created_at'));
}

/** The newest incidents with the summary the topic carries as meta. */
export function liveIncidentsView(topics: TopicCollections) {
  const col = topics.get(TOPICS.incidents);
  const rows = rowsSignal<{ started_at: number }>(col);
  return createMemo(() => {
    const meta = col.utils.meta() as { total?: number; storage_error?: string | null };
    const list = newestFirst(rows(), 'started_at');
    return {
      incidents: list,
      total: meta.total ?? list.length,
      offset: 0,
      limit: list.length,
      storage_error: meta.storage_error ?? null,
      server_time_ms: Date.now(),
    };
  });
}

/** The scheduler's own log tail, oldest first, with the count the host dropped. */
export function liveLogsView(topics: TopicCollections) {
  const col = topics.get(TOPICS.logs);
  const rows = rowsSignal<LogLine & { n: number }>(col);
  return createMemo(() => ({
    lines: byKey(rows(), 'n') as (LogLine & { n: number })[],
    dropped: Number((col.utils.meta() as { dropped?: number }).dropped ?? 0),
  }));
}
