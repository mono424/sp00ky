import {
  createContext,
  useContext,
  createSignal,
  createEffect,
  onCleanup,
  onMount,
  type ParentComponent,
} from 'solid-js';
import { createStore } from 'solid-js/store';
import {
  DEFAULT_VERSIONS,
  type DevToolsState,
  type BackendDevToolsState,
  type TabType,
  type ChromeMessage,
  type QueryMark,
  type StorageInfo,
  type FlagsSnapshot,
  type ImpersonationStatus,
  type ImpersonationUser,
  type Sp00kyFrame,
} from '../types/devtools';
import { useChromeConnection } from '../hooks/useChromeConnection';
import { useRunInHostPage } from '../hooks/useRunInHostPage';
import { adaptBackendState } from '../utils/state-adapter';

interface McpStatus {
  enabled: boolean;
  connected: boolean;
  port: number;
}

export type QueryRows = { data?: unknown; localArray?: unknown; remoteArray?: unknown };

interface DevToolsContextValue {
  // State
  state: DevToolsState;
  activeTab: () => TabType;
  queryMarks: () => QueryMark[];
  selectedQueryHash: () => number | null;
  selectedTable: () => string | null;
  isSp00kyAvailable: () => boolean;
  mcpStatus: () => McpStatus;
  setMcpEnabled: (enabled: boolean) => void;

  /** Every frame in the tab running a Sp00ky client (main document + iframes). */
  frames: () => Sp00kyFrame[];
  /** Which one the panel is inspecting; 0 is the main document. */
  activeFrameId: () => number;
  activeFrame: () => Sp00kyFrame | undefined;

  // Actions
  /** Point the panel at another client. Wipes the previous frame's state. */
  selectFrame: (frameId: number) => void;
  setActiveTab: (tab: TabType) => void;
  setSelectedQueryHash: (hash: number | null) => void;
  setSelectedTable: (table: string | null) => void;
  clearEvents: () => void;
  /**
   * Toolbar Refresh. Scoped to the active tab; `{ full: true }` (Shift+click)
   * refreshes everything. Options bag, not a positional boolean — see the
   * implementation for why that matters.
   */
  refresh: (opts?: { full?: boolean }) => void;
  /** True while any refresh-driven fetch is in flight (drives the spinner). */
  isRefreshing: () => boolean;
  /** Bumped to make the Database tab refetch its list and rows. */
  dbRefreshNonce: () => number;
  isFetchingRows: () => boolean;
  /** Written only by TableView, the sole row fetcher. */
  setFetchingRows: (value: boolean) => void;
  refreshVersions: () => void;
  fetchTableData: (tableName: string) => void;
  /** Rows of one active query, on demand (the pushed state has counts only). */
  fetchQueryRows: (queryHash: number) => Promise<QueryRows | null>;
  updateTableRow: (tableName: string, recordId: string, updates: Record<string, unknown>) => void;
  deleteTableRow: (tableName: string, recordId: string) => void;
  runQuery?: (query: string, target: 'local' | 'remote') => Promise<any>;
  fetchSchema?: () => Promise<void>;
  fetchTables?: (target?: 'local' | 'remote') => Promise<void>;

  // Storage tab
  storageInfo: () => StorageInfo | null;
  storageInfoError: () => string | null;
  isFetchingStorage: () => boolean;
  fetchStorageInfo: (opts?: { tableCounts?: boolean }) => Promise<void>;
  requestPersistentStorage: () => Promise<boolean>;

  // Access tab
  flagsSnapshot: () => FlagsSnapshot | null;
  flagsError: () => string | null;
  isFetchingFlags: () => boolean;
  /** Flag key currently being mutated, so its row can be disabled. */
  isMutatingFlag: () => string | null;
  fetchFlags: () => Promise<void>;
  setFlagEnabled: (key: string, enabled: boolean) => Promise<void>;
  /** `userId` (a `user:xxx` record id) targets someone else; omitted = the signed-in user. */
  setFlagUserVariant: (
    key: string,
    variant: string,
    remove: boolean,
    userId?: string
  ) => Promise<void>;
  setFlagOverride: (key: string, variant: string | null) => Promise<void>;
  clearFlagOverrides: () => Promise<void>;
  impersonation: () => ImpersonationStatus | null;
  impersonationError: () => string | null;
  /** True while a start/stop is in flight. */
  isImpersonationBusy: () => boolean;
  fetchImpersonation: () => Promise<void>;
  searchImpersonationUsers: (search: string) => Promise<ImpersonationUser[]>;
  startImpersonation: (target: string, reason: string) => Promise<boolean>;
  stopImpersonation: () => Promise<void>;
}

/** Union of two table-name lists, preserving `a`'s order then appending new `b`. */
function unionTables(a: string[], b: string[]): string[] {
  const seen = new Set(a);
  const extra = b.filter((t) => !seen.has(t));
  return extra.length === 0 ? a : [...a, ...extra];
}

const DevToolsContext = createContext<DevToolsContextValue>();

export const DevToolsProvider: ParentComponent = (props) => {
  // Store for DevTools state
  // oxlint-disable-next-line no-shadow -- intentionally matching interface field name
  const [state, setState] = createStore<DevToolsState>({
    events: [],
    activeQueries: [],
    auth: {
      isAuthenticated: false,
      user: null,
      lastAuthCheck: Date.now(),
    },
    database: {
      tables: [],
      remoteTables: [],
      tableData: {},
    },
    versions: DEFAULT_VERSIONS,
  });

  // UI state
  const [activeTab, setActiveTab] = createSignal<TabType>('queries');
  // Timeline marks for the Queries tab. Accumulated here (not in the backend)
  // by diffing each activeQueries snapshot — the backend only carries the
  // latest lastUpdate per query and its event history is capped at 100.
  const [queryMarks, setQueryMarks] = createSignal<QueryMark[]>([]);
  // queryHash -> last seen `lastUpdate`; non-reactive on purpose.
  const seenQueryUpdates = new Map<number, number>();
  const MAX_QUERY_MARKS = 2000;
  const [selectedQueryHash, setSelectedQueryHash] = createSignal<number | null>(null);
  const [selectedTable, setSelectedTable] = createSignal<string | null>(null);
  const [isSp00kyAvailable, setIsSp00kyAvailable] = createSignal(false);
  // Every frame in the tab that announced a client, and which one the panel is
  // inspecting. The main document is frameId 0 and is the default; an iframe is
  // only ever selected explicitly.
  const [frames, setFrames] = createSignal<Sp00kyFrame[]>([]);
  const [activeFrameId, setActiveFrameIdSignal] = createSignal(0);
  // Falls back to the pinned entry so a client that is momentarily absent
  // (navigating, or its iframe being rebuilt) still has a name in the picker
  // instead of blanking out mid-use.
  const activeFrame = (): Sp00kyFrame | undefined =>
    frames().find((f) => f.frameId === activeFrameId()) ??
    (activeFrameId() !== 0 && pinnedFrameUrl()
      ? { frameId: activeFrameId(), url: pinnedFrameUrl()! }
      : undefined);
  // Undefined for the main document — `useRunInHostPage` then evaluates against
  // the top frame, which is the pre-iframe behavior.
  const activeFrameUrl = () => (activeFrameId() === 0 ? undefined : activeFrame()?.url);
  // Storage tab: on-demand diagnostics snapshot (the heavyweight OPFS walk +
  // quota estimate lives behind an explicit fetch, not the push channel).
  const [storageInfo, setStorageInfo] = createSignal<StorageInfo | null>(null);
  const [storageInfoError, setStorageInfoError] = createSignal<string | null>(null);
  const [isFetchingStorage, setIsFetchingStorage] = createSignal(false);
  // Access tab: on-demand too. Definitions aren't synced to the client, so this
  // is a remote read and can't ride the push channel.
  const [flagsSnapshot, setFlagsSnapshot] = createSignal<FlagsSnapshot | null>(null);
  const [flagsError, setFlagsError] = createSignal<string | null>(null);
  const [isFetchingFlags, setIsFetchingFlags] = createSignal(false);
  const [isMutatingFlag, setIsMutatingFlag] = createSignal<string | null>(null);
  // Access tab, impersonation half: also an on-demand remote read.
  const [impersonation, setImpersonation] = createSignal<ImpersonationStatus | null>(null);
  const [impersonationError, setImpersonationError] = createSignal<string | null>(null);
  const [isImpersonationBusy, setIsImpersonationBusy] = createSignal(false);
  const [mcpStatus, setMcpStatus] = createSignal<McpStatus>({ enabled: false, connected: false, port: 9315 });

  // Bumped by the toolbar Refresh when the Database tab is active. Read by
  // DatabaseTab's table-list effect and TableView's row-fetch effect. Both are
  // unmounted while another tab is showing, so a bump then is simply inert —
  // remounting re-runs them from scratch anyway.
  const [dbRefreshNonce, setDbRefreshNonce] = createSignal(0);
  const bumpDbRefresh = () => setDbRefreshNonce((n) => n + 1);

  // Rows are fetched by TableView, but the toolbar spinner needs to know about
  // them, so the flag lives here. TableView is its only writer.
  const [isFetchingRows, setFetchingRows] = createSignal(false);

  // Refresh fires several independent page evals that have no guards of their
  // own (checkSp00ky, refreshVersions). Counted so the button stays busy until
  // the last one lands.
  const [refreshPending, setRefreshPending] = createSignal(0);
  const [minSpin, setMinSpin] = createSignal(false);
  let minSpinTimer: ReturnType<typeof setTimeout> | undefined;
  const REFRESH_MIN_SPIN_MS = 400;
  const REFRESH_OP_TIMEOUT_MS = 20_000;
  // One probe interval on the server side, so the reading is never more than
  // about a cycle behind while the Versions tab is being watched.
  const VERSIONS_POLL_MS = 30_000;

  /**
   * Start a refresh op; returns its idempotent `end`.
   *
   * The watchdog is not paranoia: `chrome.devtools.inspectedWindow.eval` drops
   * its callback if the page navigates mid-eval, and `checkSp00kyAvailable` has
   * no error channel at all — so without it the button would strand disabled
   * with no way back.
   */
  function beginOp(): () => void {
    setRefreshPending((n) => n + 1);
    let settled = false;
    const end = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      setRefreshPending((n) => Math.max(0, n - 1));
    };
    const timer = setTimeout(end, REFRESH_OP_TIMEOUT_MS);
    return end;
  }

  /** True while anything the toolbar Refresh can trigger is in flight. */
  const isRefreshing = () =>
    minSpin() ||
    refreshPending() > 0 ||
    isFetchingStorage() ||
    isFetchingFlags() ||
    isFetchingRows();

  // Custom hooks
  const { sendMessage: sendRaw } = useChromeConnection({
    onMessage: handleMessage,
    onConnect: () => {
      console.log('[DevTools] Chrome connection established');
      checkSp00ky();
      sendMessage({ type: 'GET_MCP_STATUS' });
      // The panel can attach after the page loaded, in which case every
      // detection announcement is already gone; ask for the current list.
      sendRaw({ type: 'GET_FRAMES' });
    },
    onDisconnect: () => {
      console.log('[DevTools] Chrome connection lost');
      setIsSp00kyAvailable(false);
    },
  });

  /**
   * Stamp the inspected frame on everything the panel sends. The content script
   * runs in every frame, so an unaddressed message is answered by all of them.
   */
  const sendMessage = (message: ChromeMessage) =>
    sendRaw({ ...message, frameId: activeFrameId() });

  const requestState = () => sendMessage({ type: 'GET_SP00KY_STATE' });

  /** URL an eval last failed to address, cleared once the frame list explains it. */
  let lostFrameUrl: string | null = null;
  /** Last known URL of the frame the user picked. Its ORIGIN is what identifies
   *  the client across document swaps and iframe re-creation, since neither the
   *  frame id nor the full URL survives those. */
  const [pinnedFrameUrl, setPinnedFrameUrl] = createSignal<string | undefined>(undefined);

  /** Origin of a URL, or undefined if it isn't one (a frame that never
   *  reported a URL). Used to match a client across frame ids. */
  const originOf = (url: string | undefined): string | undefined => {
    if (!url) return undefined;
    try {
      return new URL(url).origin;
    } catch {
      return undefined;
    }
  };

  /**
   * Whether the inspected client can be reached with `inspectedWindow.eval`.
   *
   * Only the main document can, reliably: a cross-origin iframe runs in its own
   * renderer process, and eval's `frameURL` option does not cross that boundary
   * (it answers E_NOTFOUND). Everything for a non-main frame therefore goes
   * over the content-script message channel, which is frameId-addressed and
   * process-agnostic — the same channel the frame list already arrives on.
   */
  const isMainFrame = () => activeFrameId() === 0;

  const hostPage = useRunInHostPage(activeFrameUrl, (lostUrl) => {
    // The frame we were addressing no longer answers to that URL. Almost always
    // a route change inside the iframe, whose new URL the content script is
    // already reporting — so pull the fresh list rather than giving up on the
    // frame. `applyFrames` re-checks when the URL moved, and falls back to the
    // main document if the frame is genuinely gone.
    console.log('[DevTools] Frame URL no longer matches, re-syncing frames:', lostUrl);
    lostFrameUrl = lostUrl;
    setIsSp00kyAvailable(false);
    sendRaw({ type: 'GET_FRAMES' });
  });

  // In-flight guards for the panel's own optional schema queries (kept for
  // manual/debug use). The table list itself comes from the backend now.
  let tablesInFlight = false;
  let remoteTablesInFlight = false;
  let schemaInFlight = false;

  /**
   * Handle messages from background script
   */
  // Stores pending query requests: requestId -> { resolve, reject }
  const pendingQueries = new Map<
    string,
    { resolve: (data: any) => void; reject: (err: string) => void }
  >();

  /** Settle a requestId-correlated response (query or storage op). */
  function settlePending(msg: {
    type: string;
    requestId?: string;
    success?: boolean;
    data?: any;
    error?: string;
  }) {
    console.log('[DevTools] RAW RESPONSE:', msg);
    if (msg.requestId && pendingQueries.has(msg.requestId)) {
      // oxlint-disable-next-line no-non-null-assertion -- guarded by .has() check above
      const { resolve, reject } = pendingQueries.get(msg.requestId)!;
      pendingQueries.delete(msg.requestId);
      if (msg.success) {
        resolve(msg.data);
      } else {
        console.error('[DevTools] Rejecting with error:', msg.error);
        reject(msg.error || 'Unknown error from response (msg.error was falsy)');
      }
    } else {
      console.warn('[DevTools] Response for unknown/expired requestId:', msg.requestId);
    }
  }

  /**
   * Handle messages from background script
   */
  function handleMessage(message: ChromeMessage) {
    console.log('[DevTools] Processing message:', message);

    // The frame registry and the MCP/lifecycle messages are tab-wide; state
    // pushes belong to exactly one frame.
    if (message.type === 'SP00KY_FRAMES') {
      applyFrames(message.frames ?? []);
      return;
    }

    // Drop pushes from frames the panel is not inspecting. `frameId` is absent
    // on panel-internal messages (MCP_STATUS, PAGE_RELOADED), which are not
    // frame-scoped and must not be filtered.
    if (message.frameId !== undefined && message.frameId !== activeFrameId()) {
      console.log('[DevTools] Ignoring message from frame', message.frameId);
      return;
    }

    switch (message.type) {
      case 'SP00KY_DETECTED':
        setIsSp00kyAvailable(true);
        // If state is included in the detection message, use it
        if (message.data && (message.data as any).state) {
          console.log('[DevToolsContext] Initialized with state from detection');
          updateState((message.data as any).state);
        } else {
          console.log('[DevToolsContext] Sp00ky detected, requesting state...');
          requestState();
        }
        break;

      case 'SP00KY_STATE_CHANGED':
        if (message.state) {
          console.log(
            '[DevToolsContext] State updated. Tables:',
            message.state.database?.tables?.length || 0
          );
          updateState(message.state);
        }
        break;

      case 'SP00KY_TABLE_DATA_RESPONSE':
        if (message.tableName && message.data) {
          setState(
            'database',
            'tableData',
            message.tableName,
            message.data as Record<string, unknown>[]
          );
        }
        break;

      case 'SP00KY_QUERY_RESPONSE':
      case 'SP00KY_STORAGE_INFO_RESPONSE':
      case 'SP00KY_FLAG_RESPONSE':
      case 'SP00KY_IMPERSONATE_RESPONSE':
        settlePending(message as any);
        break;

      case 'MCP_STATUS':
        setMcpStatus({
          enabled: (message as any).enabled ?? false,
          connected: (message as any).connected ?? false,
          port: (message as any).port ?? 9315,
        });
        break;

      case 'PAGE_RELOADED':
        console.log('[DevTools] Page reloaded, checking for Sp00ky...');
        // Fresh page → fresh timeline (Chrome network-tab behavior without
        // "Preserve log").
        seenQueryUpdates.clear();
        setQueryMarks([]);
        // The flag snapshot is tied to the old page's auth session and local
        // store, so it's stale by definition. Drop it rather than render it.
        setFlagsSnapshot(null);
        setFlagsError(null);
        setImpersonation(null);
        setImpersonationError(null);
        setTimeout(() => {
          checkSp00ky();
        }, 500);
        // Clear pending queries on reload
        pendingQueries.forEach(({ reject }) => reject('Page reloaded'));
        pendingQueries.clear();
        // A navigation orphans any eval callbacks still outstanding, which
        // would otherwise leave the toolbar spinner stuck until its watchdog.
        setRefreshPending(0);
        break;

      default:
        console.log('[DevTools] Unknown message type:', message.type);
    }
  }

  /** Adopt a frame list from the background script. */
  function applyFrames(next: Sp00kyFrame[]) {
    const previousUrl = activeFrame()?.url;
    setFrames(next);

    // Same frame, new URL: the iframe routed somewhere else. Evals now address
    // it correctly again, so re-read state — a panel that failed while the URL
    // was stale comes back without the user touching anything.
    const currentUrl = next.find((f) => f.frameId === activeFrameId())?.url;
    if (activeFrameId() !== 0 && currentUrl && previousUrl && currentUrl !== previousUrl) {
      console.log('[DevTools] Inspected frame navigated, re-reading state');
      lostFrameUrl = null;
      checkSp00ky();
    } else if (lostFrameUrl && currentUrl === lostFrameUrl) {
      // An eval failed against this URL and a fresh list still reports the same
      // one, so the frame is really gone rather than merely re-routed (the host
      // page can drop an iframe without its document getting a chance to say
      // so). Nothing can be evaluated there — go back to the main document.
      console.log('[DevTools] Inspected frame is unreachable, falling back to the main document');
      lostFrameUrl = null;
      selectFrame(0);
      return;
    }
    if (activeFrameId() === 0) return;

    const live = next.some((f) => f.frameId === activeFrameId());

    // The frame id can change under the user without them doing anything: a
    // navigation inside the iframe replaces its document, and the host page
    // REPLACING the <iframe> element gets a brand-new frame id altogether. So
    // the panel follows the CLIENT — the same app on the same origin — rather
    // than an id, and never throws the selection away on its own.
    if (!live) {
      const origin = originOf(pinnedFrameUrl());
      const successor = origin
        ? next.find((f) => f.frameId !== 0 && originOf(f.url) === origin)
        : undefined;

      if (successor) {
        console.log('[DevTools] Client reappeared as frame', successor.frameId);
        setActiveFrameIdSignal(successor.frameId);
        setPinnedFrameUrl(successor.url);
        lostFrameUrl = null;
        setIsSp00kyAvailable(true);
        // A new document means the previous one's timeline is finished; keep
        // the view, refresh its contents.
        checkSp00ky();
        return;
      }

      // Gone for now. Hold the selection and say so — the picker keeps showing
      // it, so the user's place survives whatever the page is doing.
      setIsSp00kyAvailable(false);
      return;
    }

    const current = next.find((f) => f.frameId === activeFrameId());
    if (current) setPinnedFrameUrl(current.url);
    if (!isSp00kyAvailable()) {
      console.log('[DevTools] Inspected frame is back, re-reading state');
      lostFrameUrl = null;
      setIsSp00kyAvailable(true);
      checkSp00ky();
      return;
    }
  }

  /**
   * Point the whole panel at another Sp00ky client.
   *
   * Everything on screen — events, queries, tables, flags, storage, versions —
   * belongs to ONE client, so switching wipes it rather than blending two
   * clients' state into one view. The new frame's data arrives from the
   * re-check + state request below.
   */
  function selectFrame(frameId: number) {
    if (frameId === activeFrameId()) return;
    setActiveFrameIdSignal(frameId);
    // Remember WHAT was picked, not just which frame slot: the id is the part
    // that goes stale (see applyFrames).
    setPinnedFrameUrl(frames().find((f) => f.frameId === frameId)?.url);
    lostFrameUrl = null;

    setIsSp00kyAvailable(false);
    setState('events', []);
    setState('activeQueries', []);
    setState('auth', { isAuthenticated: false, user: null, lastAuthCheck: Date.now() });
    setState('database', { tables: [], remoteTables: [], tableData: {} });
    setState('versions', DEFAULT_VERSIONS);
    seenQueryUpdates.clear();
    setQueryMarks([]);
    setSelectedQueryHash(null);
    setSelectedTable(null);
    setStorageInfo(null);
    setStorageInfoError(null);
    setFlagsSnapshot(null);
    setFlagsError(null);
    // In-flight requests were addressed to the previous frame; their responses
    // are filtered out on arrival, so settle them here or they leak forever.
    pendingQueries.forEach(({ reject }) => reject('Switched to another frame'));
    pendingQueries.clear();
    setRefreshPending(0);

    // Re-reads availability AND the full state, both against the new frame.
    checkSp00ky();
    // Wakes the tab-level effects (table list, rows) that key off the nonce.
    bumpDbRefresh();
  }

  /**
   * Update state from Sp00ky - accepts backend state format
   */
  function updateState(backendState: BackendDevToolsState | DevToolsState) {
    console.log('[DevTools] Received state:', backendState);

    // Check if it's backend format (has eventsHistory) or frontend format (has events)
    const frontendState =
      'eventsHistory' in backendState
        ? adaptBackendState(backendState as BackendDevToolsState)
        : (backendState as DevToolsState);

    console.log('[DevTools] Adapted state:', frontendState);

    // Update events
    if (frontendState.events) {
      setState('events', frontendState.events);
    }

    // Update active queries
    if (frontendState.activeQueries) {
      setState('activeQueries', frontendState.activeQueries);
      recordQueryMarks(frontendState.activeQueries);
    }

    // Update auth
    if (frontendState.auth) {
      setState('auth', frontendState.auth);
    }

    // Merge (union) the backend table list with what we already have. Newer core
    // enumerates every local table (incl. internal `_00_*`); older core sends
    // only app tables. Merging means the panel's own `fetchTables()` result
    // (which always sees `_00_*`) isn't clobbered by a subsequent backend push.
    if (frontendState.database?.tables) {
      const incoming = frontendState.database.tables;
      setState('database', 'tables', (prev) => unionTables(incoming, prev));
    }

    // Local-store durability (pushed by core on every health change). Without
    // this copy the Database tab's Storage line and the Storage tab's live
    // banner never see it.
    if (frontendState.database?.storage) {
      setState('database', 'storage', frontendState.database.storage);
    }

    // Update component versions
    if (frontendState.versions) {
      setState('versions', frontendState.versions);
    }
  }

  /**
   * Diff an activeQueries snapshot against what we've seen and append timeline
   * marks: a `registered` mark the first time a query appears, an `updated`
   * mark whenever its lastUpdate advances.
   */
  function recordQueryMarks(queries: DevToolsState['activeQueries']) {
    const fresh: QueryMark[] = [];
    for (const q of queries) {
      const seen = seenQueryUpdates.get(q.queryHash);
      if (seen === undefined) {
        fresh.push({ queryHash: q.queryHash, timestamp: q.createdAt, kind: 'registered' });
        if (q.lastUpdate > q.createdAt) {
          fresh.push({ queryHash: q.queryHash, timestamp: q.lastUpdate, kind: 'updated' });
        }
      } else if (q.lastUpdate > seen) {
        fresh.push({ queryHash: q.queryHash, timestamp: q.lastUpdate, kind: 'updated' });
      }
      seenQueryUpdates.set(q.queryHash, q.lastUpdate);
    }
    if (fresh.length === 0) return;
    setQueryMarks((prev) => {
      const next = [...prev, ...fresh];
      return next.length > MAX_QUERY_MARKS ? next.slice(next.length - MAX_QUERY_MARKS) : next;
    });
  }

  /**
   * Check if Sp00ky is available on the page
   *
   * `onSettled` fires exactly once, on every path, so the toolbar's busy
   * counter can be released. Note `checkSp00kyAvailable` has no error channel
   * at all (`useRunInHostPage.ts`), so if the page navigates mid-eval its
   * callback simply never fires — hence the watchdog in `beginOp`.
   */
  function checkSp00ky(onSettled?: () => void) {
    const done = onSettled ?? (() => {});

    // An iframe cannot be evaluated in when it is cross-origin: Chrome puts it
    // in its own process, and `inspectedWindow.eval` addresses frames by URL
    // within the inspected process only. The message channel reaches every
    // frame regardless, so for a non-main frame availability comes from the
    // frame having announced itself, and state comes from asking it.
    if (!isMainFrame()) {
      const present = frames().some((f) => f.frameId === activeFrameId());
      setIsSp00kyAvailable(present);
      if (present) requestState();
      done();
      return;
    }

    hostPage.checkSp00kyAvailable((available) => {
      console.log('[DevTools] Sp00ky available:', available);
      setIsSp00kyAvailable(available);

      if (!available) {
        done();
        return;
      }

      hostPage.getSp00kyState(
        (sp00kyState) => {
          if (sp00kyState) {
            updateState(sp00kyState);
          }
          done();
        },
        (error) => {
          console.error('[DevTools] Error getting Sp00ky state:', error);
          done();
        }
      );
    });
  }

  /**
   * Clear all events - clears both local state and backend history
   */
  function clearEvents() {
    // Clear backend history first
    if (isMainFrame()) {
      hostPage.clearHistory(
        (result) => {
          console.log('[DevTools] Clear history result:', result);
        },
        (error) => {
          console.error('[DevTools] Error clearing history:', error);
        }
      );
    } else {
      sendMessage({ type: 'CLEAR_HISTORY', payload: { requestId: `clear-${Date.now()}` } } as any);
    }
    // Clear local state immediately for responsive UI
    setState('events', []);
  }

  /**
   * The toolbar Refresh. Scoped to the active tab; Shift+click does everything.
   *
   * `opts` is an options bag rather than a positional boolean on purpose. The
   * button used to be wired as `onClick={refresh}`, which hands the handler a
   * MouseEvent — with `refresh(full?: boolean)` that event is truthy, so every
   * plain click would silently trigger a full refresh (remote version
   * discovery + a 15s OPFS walk + a 30s remote flag read). `MouseEvent.full` is
   * undefined, so the worst case here is a scoped refresh. The parameter shape
   * IS the guard; don't "simplify" it to a boolean.
   */
  function refresh(opts?: { full?: boolean }) {
    const full = opts?.full === true;

    // A page eval round-trips in ~20ms. Without a floor the spinner is a
    // flicker that reads as "the button did nothing".
    setMinSpin(true);
    if (minSpinTimer) clearTimeout(minSpinTimer);
    minSpinTimer = setTimeout(() => setMinSpin(false), REFRESH_MIN_SPIN_MS);

    if (full) {
      checkSp00ky(beginOp());
      refreshVersions(beginOp());
      bumpDbRefresh();
      void fetchStorageInfo();
      void fetchFlags();
      void fetchImpersonation();
      sendMessage({ type: 'GET_MCP_STATUS' });
      return;
    }

    refreshScoped(activeTab());
  }

  /**
   * One tab's worth of refresh.
   *
   * `checkSp00ky()` is the baseline for every tab, not just the cheap ones: it
   * is a single `window.__00__.getState()` eval (no network, no DB), it is the
   * only writer of `isSp00kyAvailable()` — which renders the connected dot in
   * the very toolbar this button lives in — and one call feeds events,
   * activeQueries, auth, the table list and storage health at once. There is no
   * tab where skipping it buys anything.
   *
   * `refreshVersions()` is deliberately NOT baseline: it is a remote fetch and
   * irrelevant to seven of the eight tabs.
   *
   * Keep in sync with REFRESH_SCOPE in components/Tabs.tsx, which is the
   * user-facing description of exactly this mapping.
   */
  function refreshScoped(tab: TabType) {
    checkSp00ky(beginOp());

    switch (tab) {
      case 'events':
      case 'queries':
      case 'timing':
        // Fully covered by the baseline — all three render slices of getState().
        break;
      case 'database':
        // Table list (DatabaseTab's effect) and rows (TableView's effect).
        bumpDbRefresh();
        break;
      case 'storage':
        void fetchStorageInfo();
        break;
      case 'access':
        // The session half rides the baseline getState(); the flag snapshot is
        // a separate remote read, as is the impersonation status.
        void fetchFlags();
        void fetchImpersonation();
        break;
      case 'versions':
        refreshVersions(beginOp());
        break;
      case 'mcp':
        // Otherwise requested exactly once, at onConnect — so a bridge that
        // connects after the panel opened shows a stale badge until reopen.
        sendMessage({ type: 'GET_MCP_STATUS' });
        break;
      default: {
        // Adding a TabType member without deciding what Refresh does for it is
        // a compile error here (and in REFRESH_SCOPE), not a silent default.
        const exhaustive: never = tab;
        void exhaustive;
      }
    }
  }

  /**
   * Re-run backend version discovery in the page. Core's refreshVersions()
   * re-fetches the ssp/scheduler/surrealdb versions and posts a state change,
   * which arrives back through the normal SP00KY_STATE_CHANGED channel.
   */
  function refreshVersions(onSettled?: () => void) {
    const done = onSettled ?? (() => {});

    if (!isMainFrame()) {
      // Fire-and-forget over the message channel: the result arrives as a state
      // push, exactly as it does for the eval path below.
      sendMessage({
        type: 'REFRESH_VERSIONS',
        payload: { requestId: `versions-${Date.now()}` },
      } as any);
      done();
      return;
    }

    hostPage.run(
      `(async function() {
        if (window.__00__ && window.__00__.refreshVersions) {
          await window.__00__.refreshVersions();
          return { success: true };
        }
        return { success: false };
      })()`,
      {
        onSuccess: () => done(),
        onError: (error) => {
          console.error('[DevTools] Error refreshing versions:', error);
          done();
        },
      }
    );
  }

  /**
   * Keep the Versions tab's reading current while it is on screen.
   *
   * Everything else in the panel is push-driven, but `/info` is a remote fetch
   * that only happens on Refresh — so the heartbeat it carries was rendering a
   * snapshot from whenever you last refreshed. A reading captured during an
   * outage kept showing `stale` long after the pipeline recovered, which is
   * indistinguishable from the pipeline still being broken.
   *
   * Scoped deliberately: only while the Versions tab is the active one AND the
   * panel is visible, so a backgrounded DevTools window costs nothing. Each
   * tick is one `fn::spooky::info()` — a SurrealDB-side `http::get` to the
   * scheduler — which is why this is not panel-wide.
   */
  createEffect(() => {
    if (activeTab() !== 'versions') return;

    let timer: ReturnType<typeof setInterval> | undefined;
    let inFlight = false;

    const tick = () => {
      if (inFlight || document.visibilityState !== 'visible') return;
      inFlight = true;
      refreshVersions(() => {
        inFlight = false;
      });
    };

    const start = () => {
      if (timer) return;
      timer = setInterval(tick, VERSIONS_POLL_MS);
    };
    const stop = () => {
      if (!timer) return;
      clearInterval(timer);
      timer = undefined;
    };

    const onVisibility = () => {
      if (document.visibilityState === 'visible') {
        // Coming back from hidden: the reading is stale by definition.
        tick();
        start();
      } else {
        stop();
      }
    };

    tick();
    start();
    document.addEventListener('visibilitychange', onVisibility);

    onCleanup(() => {
      stop();
      document.removeEventListener('visibilitychange', onVisibility);
    });
  });

  /**
   * Fetch table data from the page
   */
  function fetchTableData(tableName: string) {
    console.log('[DevTools] Fetching table data for:', tableName);
    if (!isMainFrame()) {
      // The response comes back as SP00KY_TABLE_DATA_RESPONSE, the same message
      // the eval path's page-side code posts.
      sendMessage({ type: 'GET_TABLE_DATA', payload: { tableName } } as any);
      return;
    }
    hostPage.getTableData(
      tableName,
      (result) => {
        console.log('[DevTools] Table data fetch result:', result);
      },
      (error) => {
        console.error('[DevTools] Error fetching table data:', error);
      }
    );
  }

  /**
   * Rows of one active query. The pushed state carries counts and capped ids
   * only, so the Data tab asks for the rows when it is actually opened.
   */
  function fetchQueryRows(queryHash: number): Promise<QueryRows | null> {
    if (!isMainFrame()) {
      const requestId = `rows-${queryHash}-${Date.now()}`;
      return new Promise((resolve, reject) => {
        pendingQueries.set(requestId, {
          resolve: (data: any) => resolve((data ?? null) as QueryRows | null),
          reject: (err: any) => reject(err),
        });
        sendMessage({ type: 'GET_QUERY_ROWS', payload: { queryHash, requestId } } as any);
      });
    }
    return new Promise((resolve, reject) => {
      hostPage.getQueryRows(queryHash, (rows) => resolve((rows ?? null) as QueryRows | null), reject);
    });
  }

  /**
   * Update a table row
   */
  function updateTableRow(tableName: string, recordId: string, updates: Record<string, unknown>) {
    console.log('[DevTools] Updating row:', { tableName, recordId, updates });
    if (!isMainFrame()) {
      const requestId = `update-${Date.now()}`;
      sendMessage({
        type: 'UPDATE_TABLE_ROW',
        payload: { tableName, recordId, updates, requestId },
      } as any);
      // The write settles as a SP00KY_BRIDGE_RESPONSE; re-read once it lands.
      pendingQueries.set(requestId, {
        resolve: () => fetchTableData(tableName),
        reject: (err) => console.error('[DevTools] Update failed:', err),
      });
      return;
    }
    hostPage.updateTableRow(
      tableName,
      recordId,
      updates,
      (result) => {
        console.log('[DevTools] Update result:', result);
        if (result.success) {
          // Refresh table data after successful update
          fetchTableData(tableName);
        } else {
          console.error('[DevTools] Update failed:', result.error);
        }
      },
      (error) => {
        console.error('[DevTools] Error updating row:', error);
      }
    );
  }

  /**
   * Delete a table row
   */
  function deleteTableRow(tableName: string, recordId: string) {
    console.log('[DevTools] Deleting row:', { tableName, recordId });
    if (!isMainFrame()) {
      const requestId = `delete-${Date.now()}`;
      sendMessage({
        type: 'DELETE_TABLE_ROW',
        payload: { tableName, recordId, requestId },
      } as any);
      pendingQueries.set(requestId, {
        resolve: () => fetchTableData(tableName),
        reject: (err) => console.error('[DevTools] Delete failed:', err),
      });
      return;
    }
    hostPage.deleteTableRow(
      tableName,
      recordId,
      (result) => {
        console.log('[DevTools] Delete result:', result);
        if (result.success) {
          // Refresh table data after successful delete
          fetchTableData(tableName);
        } else {
          console.error('[DevTools] Delete failed:', result.error);
        }
      },
      (error) => {
        console.error('[DevTools] Error deleting row:', error);
      }
    );
  }

  // Check for Sp00ky on mount
  onMount(() => {
    setTimeout(() => {
      checkSp00ky();
    }, 500);

    // Listen for window messages (table data responses)
    const handleWindowMessage = (event: MessageEvent) => {
      if (event.data.source === 'sp00ky-devtools-page') {
        handleMessage(event.data as ChromeMessage);
      }
    };

    window.addEventListener('message', handleWindowMessage);

    return () => {
      window.removeEventListener('message', handleWindowMessage);
    };
  });

  const runQuery = (query: string, target: 'local' | 'remote') => {
    return new Promise<{ success: boolean; data: any; error?: string }>((resolve, reject) => {
      const requestId = Math.random().toString(36).substring(7);

      // Timeout handling
      const timeoutId = setTimeout(() => {
        if (pendingQueries.has(requestId)) {
          pendingQueries.delete(requestId);
          console.error('[DevToolsContext] Query timed out:', requestId);
          reject('Query timed out (10s)');
        }
      }, 10000); // 10s timeout

      pendingQueries.set(requestId, {
        resolve: (data) => {
          clearTimeout(timeoutId);
          resolve(data);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          const safeErr = err || 'Undefined error passed to pendingQueries.reject';
          console.error('[DevToolsContext] Rejecting query', requestId, 'with:', safeErr);
          reject(safeErr);
        },
      });

      // A non-main frame is unreachable by eval when it is cross-origin, so the
      // request travels the content-script channel instead. Same requestId,
      // same SP00KY_QUERY_RESPONSE — only the delivery differs.
      if (!isMainFrame()) {
        sendMessage({ type: 'RUN_QUERY', payload: { query, target, requestId } } as any);
        return;
      }

      // Use eval to trigger the event directly in the page
      // This bypasses potential message dropping in background script
      console.log(
        '[DevToolsContext] Triggering RUN_QUERY via hostPage.runQuery (eval event dispatch)',
        requestId
      );

      hostPage.runQuery(
        query,
        target,
        requestId,
        (result) => {
          if (result && !result.success) {
            clearTimeout(timeoutId);
            pendingQueries.delete(requestId);
            const safeErr = result.error || 'Failed to dispatch query event';
            console.error('[DevToolsContext] Event dispatch failed:', safeErr);
            reject(safeErr);
          }
        },
        (err) => {
          clearTimeout(timeoutId);
          pendingQueries.delete(requestId);
          const errorStr = err instanceof Error ? err.message : String(err);
          console.error('[DevToolsContext] Eval error:', errorStr);
          reject(errorStr);
        }
      );
    });
  };

  /**
   * Dispatch a storage op into the page (same eval → CustomEvent → postMessage
   * round-trip as runQuery) and await the correlated response. 15s timeout:
   * the OPFS walk can be slow on large origins.
   */
  const storageOpRequest = (op: 'info' | 'persist', options?: { tableCounts?: boolean }) => {
    return new Promise<any>((resolve, reject) => {
      const requestId = Math.random().toString(36).substring(7);

      const timeoutId = setTimeout(() => {
        if (pendingQueries.has(requestId)) {
          pendingQueries.delete(requestId);
          reject('Storage request timed out (15s)');
        }
      }, 15000);

      pendingQueries.set(requestId, {
        resolve: (data) => {
          clearTimeout(timeoutId);
          resolve(data);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err || 'Undefined error passed to pendingQueries.reject');
        },
      });

      if (!isMainFrame()) {
        sendMessage({ type: 'STORAGE_OP', payload: { op, requestId, options } } as any);
        return;
      }

      hostPage.storageOp(
        op,
        requestId,
        options,
        (result) => {
          if (result && !result.success) {
            clearTimeout(timeoutId);
            pendingQueries.delete(requestId);
            reject(result.error || 'Failed to dispatch storage event');
          }
        },
        (err) => {
          clearTimeout(timeoutId);
          pendingQueries.delete(requestId);
          reject(err instanceof Error ? err.message : String(err));
        }
      );
    });
  };

  const fetchStorageInfo = async (opts?: { tableCounts?: boolean }) => {
    if (isFetchingStorage()) return;
    setIsFetchingStorage(true);
    try {
      const data = await storageOpRequest('info', opts);
      setStorageInfo(data as StorageInfo);
      setStorageInfoError(null);
    } catch (e) {
      setStorageInfoError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsFetchingStorage(false);
    }
  };

  /**
   * Dispatch an Access-tab op into the page and await the correlated response.
   *
   * 30s, not the 15s storage budget or the 10s query one: a flag write calls
   * `fn::feature::materialize`, which re-evaluates the flag for EVERY user and
   * upserts a row each, and an impersonation start switches the page's whole
   * local store. Both are long single operations.
   */
  const pageOpRequest = (
    channel: 'flag' | 'impersonate',
    op: string,
    args?: Record<string, unknown>
  ) => {
    return new Promise<any>((resolve, reject) => {
      const requestId = Math.random().toString(36).substring(7);
      const label = channel === 'flag' ? 'Flag' : 'Impersonation';

      const timeoutId = setTimeout(() => {
        if (pendingQueries.has(requestId)) {
          pendingQueries.delete(requestId);
          reject(`${label} request timed out (30s)`);
        }
      }, 30000);

      pendingQueries.set(requestId, {
        resolve: (data) => {
          clearTimeout(timeoutId);
          resolve(data);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err || 'Undefined error passed to pendingQueries.reject');
        },
      });

      if (!isMainFrame()) {
        const type = channel === 'flag' ? 'FLAG_OP' : 'IMPERSONATE_OP';
        sendMessage({ type, payload: { op, requestId, args } } as any);
        return;
      }

      const dispatch = channel === 'flag' ? hostPage.flagOp : hostPage.impersonateOp;
      dispatch(
        op as never,
        requestId,
        args,
        (result) => {
          if (result && !result.success) {
            clearTimeout(timeoutId);
            pendingQueries.delete(requestId);
            reject(result.error || `Failed to dispatch ${channel} event`);
          }
        },
        (err) => {
          clearTimeout(timeoutId);
          pendingQueries.delete(requestId);
          reject(err instanceof Error ? err.message : String(err));
        }
      );
    });
  };

  const flagOpRequest = (
    op: 'list' | 'setEnabled' | 'setUserVariant' | 'setOverride' | 'clearOverrides',
    args?: Record<string, unknown>
  ) => pageOpRequest('flag', op, args);

  /** Impersonation ops answer `{ success, error }` inside `data`; unwrap it. */
  const impersonateOpRequest = async (
    op: 'status' | 'listUsers' | 'start' | 'stop',
    args?: Record<string, unknown>
  ): Promise<ImpersonationStatus & { users?: ImpersonationUser[] }> => {
    const result = await pageOpRequest('impersonate', op, args);
    if (!result?.success) throw new Error(result?.error || `Impersonation ${op} failed`);
    return result;
  };

  const fetchImpersonation = async () => {
    try {
      const status = await impersonateOpRequest('status');
      setImpersonation(status);
      setImpersonationError(null);
    } catch (e) {
      setImpersonationError(e instanceof Error ? e.message : String(e));
    }
  };

  const searchImpersonationUsers = async (search: string) => {
    const result = await impersonateOpRequest('listUsers', { search });
    return result.users ?? [];
  };

  const startImpersonation = async (target: string, reason: string) => {
    if (isImpersonationBusy()) return false;
    setIsImpersonationBusy(true);
    try {
      await impersonateOpRequest('start', { target, reason });
      setImpersonationError(null);
      return true;
    } catch (e) {
      setImpersonationError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setIsImpersonationBusy(false);
      // Who the page is changed, so both halves of the tab are stale.
      checkSp00ky();
      void fetchImpersonation();
      setFlagsSnapshot(null);
      void fetchFlags();
    }
  };

  const stopImpersonation = async () => {
    if (isImpersonationBusy()) return;
    setIsImpersonationBusy(true);
    try {
      await impersonateOpRequest('stop');
      setImpersonationError(null);
    } catch (e) {
      setImpersonationError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsImpersonationBusy(false);
      checkSp00ky();
      void fetchImpersonation();
      setFlagsSnapshot(null);
      void fetchFlags();
    }
  };

  const fetchFlags = async () => {
    if (isFetchingFlags()) return;
    setIsFetchingFlags(true);
    try {
      const data = (await flagOpRequest('list')) as FlagsSnapshot;
      setFlagsSnapshot(data);
      // `snapshot.error` is a per-section failure (not migrated, remote read
      // denied) that still carries a usable local half — surface it without
      // discarding the snapshot.
      setFlagsError(data?.error ?? null);
    } catch (e) {
      setFlagsError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsFetchingFlags(false);
    }
  };

  /**
   * Run a remote flag mutation, then re-read.
   *
   * Serialized on `isMutatingFlag`: `_00_user_feature` has a UNIQUE (user, key)
   * index, so two overlapping materializes on one flag can collide, and a
   * failed statement inside the function's FOR loop aborts the rest of it.
   */
  const mutateFlag = async (
    key: string,
    op: 'setEnabled' | 'setUserVariant',
    args: Record<string, unknown>
  ) => {
    if (isMutatingFlag()) return;
    setIsMutatingFlag(key);
    try {
      const result = await flagOpRequest(op, { key, ...args });
      if (result && result.success === false) {
        setFlagsError(result.error || 'Flag update failed');
      } else {
        setFlagsError(null);
      }
    } catch (e) {
      setFlagsError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsMutatingFlag(null);
      await fetchFlags();
    }
  };

  const setFlagEnabled = (key: string, enabled: boolean) =>
    mutateFlag(key, 'setEnabled', { enabled });

  // `userId` is forwarded to core's `setFlagUserVariant`, which defaults it to
  // the signed-in user. Passing `undefined` therefore keeps the "for me" path
  // identical rather than sending an empty target.
  const setFlagUserVariant = (key: string, variant: string, remove: boolean, userId?: string) =>
    mutateFlag(key, 'setUserVariant', { variant, remove, userId });

  /** Local-only: no auth, no network, works signed out. */
  const setFlagOverride = async (key: string, variant: string | null) => {
    try {
      const result = await flagOpRequest('setOverride', { key, variant });
      setFlagsSnapshot((prev) =>
        prev ? { ...prev, overrides: result?.overrides ?? prev.overrides } : prev
      );
    } catch (e) {
      setFlagsError(e instanceof Error ? e.message : String(e));
    }
  };

  const clearFlagOverrides = async () => {
    try {
      const result = await flagOpRequest('clearOverrides');
      setFlagsSnapshot((prev) =>
        prev ? { ...prev, overrides: result?.overrides ?? {} } : prev
      );
    } catch (e) {
      setFlagsError(e instanceof Error ? e.message : String(e));
    }
  };

  const requestPersistentStorage = async (): Promise<boolean> => {
    try {
      const result = await storageOpRequest('persist');
      // `persisted` flips in the estimate/persisted section — re-snapshot.
      void fetchStorageInfo();
      return !!result?.granted;
    } catch (e) {
      setStorageInfoError(e instanceof Error ? e.message : String(e));
      return false;
    }
  };

  /**
   * Cheap: fetch just the table list via a single `INFO FOR DB`. Safe to call
   * often (Refresh, Database tab open, "show internal" toggle) — guarded so
   * overlapping calls don't pile up on the query channel.
   */
  const fetchTables = async (target: 'local' | 'remote' = 'local') => {
    if (target === 'local' ? tablesInFlight : remoteTablesInFlight) return;
    if (target === 'local') tablesInFlight = true;
    else remoteTablesInFlight = true;

    // Remote enumeration can be denied (the session has no permission to run
    // `INFO FOR DB` remotely) or simply unreachable. When it fails, mirror the
    // local table list into `remoteTables` so the Remote picker still shows
    // something instead of an empty list.
    const fallbackRemoteToLocal = () => {
      if (target !== 'remote') return;
      console.warn(
        '[DevToolsContext] Remote table enumeration failed; falling back to local tables'
      );
      setState('database', 'remoteTables', state.database.tables);
    };

    try {
      const infoRes = await runQuery('INFO FOR DB', target);

      // Handle SurrealDB response format: [{ status: 'OK', result: { tables: ... } }] or [[{ tables: ... }]]
      if (!Array.isArray(infoRes) || !infoRes[0]) {
        console.warn('[DevToolsContext] INFO FOR DB failed or invalid format', infoRes);
        fallbackRemoteToLocal();
        return;
      }

      let info: any = null;
      if ('result' in infoRes[0]) {
        info = infoRes[0].result;
      } else if (Array.isArray(infoRes[0])) {
        info = infoRes[0][0]; // Unwrap nested array
      } else {
        info = infoRes[0]; // Fallback
      }

      if (!info || !info.tables) {
        console.warn('[DevToolsContext] No tables found in INFO FOR DB result', info);
        fallbackRemoteToLocal();
        return;
      }

      const tables = Object.keys(info.tables);
      if (target === 'remote') {
        // Remote isn't pushed by the backend, so this fetch is the source of
        // truth — replace (don't union) so nonexistent tables don't linger.
        setState('database', 'remoteTables', tables);
      } else {
        // Merge so a later backend push (which may omit `_00_*`) can't drop them.
        setState('database', 'tables', (prev) => unionTables(prev, tables));
      }
    } catch (e) {
      console.error(`[DevToolsContext] fetchTables(${target}) failed:`, e);
      fallbackRemoteToLocal();
    } finally {
      if (target === 'local') tablesInFlight = false;
      else remoteTablesInFlight = false;
    }
  };

  /**
   * Full schema: table list + per-table field lists (`INFO FOR TABLE`). This is
   * the heavy one (a query per table) — run once when Sp00ky becomes available,
   * NOT on every UI interaction. Guarded against overlapping runs.
   */
  const fetchSchema = async () => {
    if (schemaInFlight) return;
    schemaInFlight = true;
    try {
      console.log('[DevToolsContext] Fetching DB Schema...');
      await fetchTables();
      const tables = state.database.tables;

      const schema: Record<string, string[]> = {};

      // For each table, get columns via INFO FOR TABLE. Batched so we don't fire
      // dozens of concurrent queries at the single WASM connection.
      const BATCH = 4;
      for (let i = 0; i < tables.length; i += BATCH) {
        await Promise.all(
          tables.slice(i, i + BATCH).map(async (table) => {
            try {
              const tableRes = await runQuery(`INFO FOR TABLE ${table}`, 'local');

              if (Array.isArray(tableRes) && tableRes[0]) {
                // Normalize nested vs wrapped
                const tableInfo =
                  'result' in tableRes[0]
                    ? tableRes[0].result
                    : Array.isArray(tableRes[0])
                      ? tableRes[0][0]
                      : tableRes[0];

                if (tableInfo && tableInfo.fields) {
                  schema[table] = Object.keys(tableInfo.fields);
                } else {
                  schema[table] = []; // No explicit fields
                }
              }
            } catch (e) {
              console.error(`[DevToolsContext] Failed to fetch info for table ${table}`, e);
              schema[table] = [];
            }
          })
        );
      }

      console.log('[DevToolsContext] Schema fetched:', schema);
      setState('database', 'schema', schema);
    } catch (e) {
      console.error('[DevToolsContext] fetchSchema failed:', e);
    } finally {
      schemaInFlight = false;
    }
  };

  function setMcpEnabledAction(enabled: boolean) {
    sendMessage({ type: 'SET_MCP_ENABLED', enabled } as any);
  }

  const contextValue: DevToolsContextValue = {
    state,
    activeTab,
    queryMarks,
    selectedQueryHash,
    selectedTable,
    isSp00kyAvailable,
    mcpStatus,
    setMcpEnabled: setMcpEnabledAction,
    frames,
    activeFrameId,
    activeFrame,
    selectFrame,
    setActiveTab,
    setSelectedQueryHash,
    setSelectedTable,
    clearEvents,
    refresh,
    isRefreshing,
    dbRefreshNonce,
    isFetchingRows,
    setFetchingRows,
    refreshVersions,
    fetchTableData,
    fetchQueryRows,
    updateTableRow,
    deleteTableRow,
    runQuery: runQuery as any, // Cast to match interface if needed
    fetchSchema,
    fetchTables,
    storageInfo,
    storageInfoError,
    isFetchingStorage,
    fetchStorageInfo,
    requestPersistentStorage,
    flagsSnapshot,
    flagsError,
    isFetchingFlags,
    isMutatingFlag,
    fetchFlags,
    setFlagEnabled,
    setFlagUserVariant,
    setFlagOverride,
    clearFlagOverrides,
    impersonation,
    impersonationError,
    isImpersonationBusy,
    fetchImpersonation,
    searchImpersonationUsers,
    startImpersonation,
    stopImpersonation,
  };

  return <DevToolsContext.Provider value={contextValue}>{props.children}</DevToolsContext.Provider>;
};

export function useDevTools() {
  const context = useContext(DevToolsContext);
  if (!context) {
    throw new Error('useDevTools must be used within DevToolsProvider');
  }
  return context;
}
