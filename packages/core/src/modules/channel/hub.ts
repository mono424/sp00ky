import type { Logger } from '../../services/logger/index';
import type { ChannelTransport, PeerRef, TransportHooks } from './index';
import { roomSupported, type RoomLink, type RoomLinkFactory, type RoomPeer } from './room';

/**
 * One link per browser, shared by its tabs.
 *
 * Every tab of a user runs this transport. One of them, the hub, holds the
 * browser's link to the user's other browsers (`RoomLink`); which one is
 * decided by a Web Lock, so when the hub's tab closes the next one takes over
 * and connects. The other tabs reach the hub over a `BroadcastChannel`.
 *
 * To the `ChannelManager` nothing changes: a peer is still one tab. The hubs
 * tell each other which tabs they speak for, every tab is told about the
 * tabs of the other browsers, and a message names the tab it comes from and
 * the tabs it is for. So a value a tab wrote still disappears with that tab,
 * not with its browser.
 *
 * Tabs of one browser are not peers of each other here: they may share one
 * local store, where a peer's write would be undone under the tab that made
 * it.
 *
 * Where there are no Web Locks or no `BroadcastChannel`, every tab is a hub
 * with a link of its own.
 */

type Status = 'connecting' | 'connected' | 'failed';

/** What tabs of one browser say to each other. */
type Bus =
  | { k: 'here'; tab: string }
  | { k: 'bye'; tab: string }
  | { k: 'hub'; tab: string }
  | { k: 'remote'; browsers: Record<RoomPeer, string[]>; status: Status }
  | { k: 'out'; tab: string; to: string[]; m: unknown }
  | { k: 'in'; from: string; to: string[] | null; m: unknown };

/** What hubs say to each other over the link. */
type Between = { h: 'tabs'; tabs: string[] } | { h: 'm'; from: string; to: string[] | null; m: unknown };

/** The part of `navigator.locks` this needs. */
export interface HubLocks {
  request(name: string, callback: () => Promise<void>): Promise<unknown>;
  request(name: string, options: { mode: 'shared' | 'exclusive' }, callback: () => Promise<void> | void): Promise<unknown>;
}

/** The part of a `BroadcastChannel` this needs. */
export interface HubBus {
  postMessage(message: unknown): void;
  onmessage: ((event: { data: unknown }) => void) | null;
  close(): void;
}

export interface HubOptions {
  /** Names the room of one user in this browser; the lock and the bus carry it. */
  scope: (user: string) => string;
  /** Builds the browser's link. Called in the tab that holds it. */
  link: RoomLinkFactory;
  logger?: Pick<Logger, 'debug' | 'warn'>;
  /** `null` forces a link per tab. Default `navigator.locks`. */
  locks?: HubLocks | null;
  /** `null` forces a link per tab. Default `BroadcastChannel`. */
  bus?: ((name: string) => HubBus) | null;
  supported?: boolean;
}

const MAX_TABS = 64;
const isTabId = (v: unknown): v is string => typeof v === 'string' && v.length > 0 && v.length <= 64;
const tabList = (v: unknown): string[] | null => (Array.isArray(v) && v.length <= MAX_TABS && v.every(isTabId) ? (v as string[]) : null);

const defaultLocks = (): HubLocks | null => (globalThis as { navigator?: { locks?: HubLocks } }).navigator?.locks ?? null;
const defaultBus = (): ((name: string) => HubBus) | null => {
  const BC = (globalThis as { BroadcastChannel?: new (name: string) => HubBus }).BroadcastChannel;
  return BC ? (name) => new BC(name) : null;
};

export function hubTransport(hooks: TransportHooks, opts: HubOptions): ChannelTransport {
  const locks = opts.locks === undefined ? defaultLocks() : opts.locks;
  const openBus = opts.bus === undefined ? defaultBus() : opts.bus;
  const log = opts.logger;

  /** This tab while it takes part, else null. */
  let me: PeerRef | null = null;
  /** Counts starts, so a lock granted for an earlier one does nothing. */
  let run = 0;
  let bus: HubBus | null = null;
  let isHub = false;
  let link: RoomLink | null = null;
  let releaseHub: (() => void) | null = null;
  let releaseTab: (() => void) | null = null;
  /** The hub's: the other tabs of this browser. */
  const localTabs = new Set<string>();
  /** The other browsers and the tabs each speaks for. The hub's is the truth, the other tabs hold a copy. */
  const browsers = new Map<RoomPeer, Set<string>>();
  /** The tabs of other browsers this tab has reported as up. */
  const up = new Set<string>();
  let status: Status = 'connecting';
  let reported: Status | null = null;
  /** Messages of this turn, each with the tabs it goes to: one frame per message, not one per tab. */
  const pending = new Map<unknown, string[]>();
  let flushing = false;

  const post = (message: Bus): void => {
    try {
      bus?.postMessage(message);
    } catch (error) {
      log?.debug({ error, Category: 'sp00ky-client::channel' }, 'channel bus post failed');
    }
  };

  /** Tell the manager which tabs of other browsers came and went, and how the link is doing. */
  const apply = (): void => {
    const self = me;
    if (!self) return;
    const now = new Set<string>();
    for (const tabs of browsers.values()) for (const t of tabs) if (t !== self.peer) now.add(t);
    for (const t of [...up]) {
      if (now.has(t)) continue;
      up.delete(t);
      hooks.onPeer({ user: self.user, peer: t }, false);
    }
    for (const t of now) {
      if (up.has(t)) continue;
      up.add(t);
      hooks.onPeer({ user: self.user, peer: t }, true);
    }
    if (status !== reported) {
      reported = status;
      hooks.onStatus(self.user, status);
    }
  };

  // ---- the hub -----------------------------------------------------------------

  const publish = (): void => {
    apply();
    const out: Record<RoomPeer, string[]> = {};
    for (const [b, tabs] of browsers) out[b] = [...tabs];
    post({ k: 'remote', browsers: out, status });
  };

  /** The tabs this browser speaks for changed: the other hubs hear it. */
  const announceTabs = (to: ReadonlyArray<RoomPeer> | null): void => {
    if (me) link?.send(to, { h: 'tabs', tabs: [me.peer, ...localTabs] } satisfies Between);
  };

  const hubSend = (from: string, to: string[], m: unknown): void => {
    const dest: RoomPeer[] = [];
    for (const [b, tabs] of browsers) if (to.some((t) => tabs.has(t))) dest.push(b);
    if (dest.length > 0) link?.send(dest.length === browsers.size ? null : dest, { h: 'm', from, to, m } satisfies Between);
  };

  /** A message of another browser's tab: to this tab, and on to the others it names. */
  const deliver = (from: string, to: string[] | null, m: unknown): void => {
    const self = me;
    if (!self) return;
    if (localTabs.size > 0 && (to === null || to.some((t) => localTabs.has(t)))) post({ k: 'in', from, to, m });
    if (to === null || to.includes(self.peer)) hooks.onMessage({ user: self.user, peer: from }, m);
  };

  /** Hold on to a tab of this browser until it is gone, however it goes. */
  const watch = (tab: string, at: number): void => {
    if (!locks) return;
    // Every tab holds a lock under its own name for as long as it lives, so
    // this request is granted the moment that tab is gone.
    void locks
      .request(tabLock(tab), { mode: 'shared' }, () => {
        if (at === run && isHub && localTabs.delete(tab)) announceTabs(null);
      })
      .catch(() => {});
  };

  const becomeHub = (self: PeerRef): void => {
    isHub = true;
    browsers.clear();
    status = 'connecting';
    link = opts.link({
      self: () => (me ? { user: me.user, peer: me.peer } : null),
      onPeer: (browser, isUp) => {
        if (isUp) {
          browsers.set(browser, new Set());
          announceTabs([browser]);
        } else if (!browsers.delete(browser)) return;
        publish();
      },
      onMessage: (browser, data) => {
        const tabs = browsers.get(browser);
        if (!tabs || data === null || typeof data !== 'object') return;
        const d = data as Partial<Between> & Record<string, unknown>;
        if (d.h === 'tabs') {
          const list = tabList(d.tabs);
          if (!list) return;
          browsers.set(browser, new Set(list));
          publish();
        } else if (d.h === 'm') {
          const to = d.to === null ? null : tabList(d.to);
          // A hub speaks for its own tabs only.
          if (!isTabId(d.from) || !tabs.has(d.from) || (d.to !== null && to === null)) return;
          deliver(d.from, to, d.m);
        }
      },
      onStatus: (next) => {
        status = next;
        publish();
      },
    });
    post({ k: 'hub', tab: self.peer });
    publish();
  };

  // ---- every tab ---------------------------------------------------------------

  const hubLock = (): string => `sp00ky-channels:${opts.scope(me?.user ?? '')}`;
  const tabLock = (tab: string): string => `${hubLock()}:tab:${tab}`;

  const onBus = (data: unknown): void => {
    const self = me;
    if (!self || data === null || typeof data !== 'object') return;
    const msg = data as Bus;
    switch (msg.k) {
      case 'here':
        if (!isHub || !isTabId(msg.tab) || msg.tab === self.peer) return;
        if (!localTabs.has(msg.tab)) {
          localTabs.add(msg.tab);
          watch(msg.tab, run);
          announceTabs(null);
        }
        publish();
        return;
      case 'bye':
        if (isHub && localTabs.delete(msg.tab)) announceTabs(null);
        return;
      case 'hub':
        // A tab took the link over: it starts out knowing nobody.
        if (!isHub) post({ k: 'here', tab: self.peer });
        return;
      case 'remote': {
        if (isHub || msg.browsers === null || typeof msg.browsers !== 'object') return;
        browsers.clear();
        for (const [b, tabs] of Object.entries(msg.browsers)) {
          const list = tabList(tabs);
          if (list) browsers.set(b, new Set(list));
        }
        status = msg.status;
        apply();
        return;
      }
      case 'out':
        if (isHub && localTabs.has(msg.tab) && tabList(msg.to)) hubSend(msg.tab, msg.to, msg.m);
        return;
      case 'in':
        if (!isHub && (msg.to === null || (Array.isArray(msg.to) && msg.to.includes(self.peer)))) {
          hooks.onMessage({ user: self.user, peer: msg.from }, msg.m);
        }
        return;
    }
  };

  const flush = (): void => {
    flushing = false;
    const self = me;
    const batch = [...pending];
    pending.clear();
    if (!self) return;
    for (const [m, to] of batch) {
      if (isHub) hubSend(self.peer, to, m);
      else post({ k: 'out', tab: self.peer, to, m });
    }
  };

  const start = (self: PeerRef): void => {
    me = self;
    const at = ++run;
    status = 'connecting';
    reported = null;
    if (!locks || !openBus) {
      becomeHub(self);
      return;
    }
    bus = openBus(hubLock());
    bus.onmessage = (event) => onBus(event.data);
    // The tab's own lock first: a hub that hears `here` waits on it to learn
    // when this tab is gone, so it has to be held by then.
    void locks
      .request(
        tabLock(self.peer),
        () =>
          new Promise<void>((resolve) => {
            if (at !== run) return resolve();
            releaseTab = resolve;
            if (!isHub) post({ k: 'here', tab: self.peer });
          })
      )
      .catch(() => {});
    void locks
      .request(
        hubLock(),
        () =>
          new Promise<void>((resolve) => {
            if (at !== run) return resolve();
            releaseHub = resolve;
            becomeHub(self);
          })
      )
      .catch(() => {});
  };

  const stop = (): void => {
    const self = me;
    if (!self) return;
    if (!isHub) post({ k: 'bye', tab: self.peer });
    run++;
    link?.close();
    link = null;
    isHub = false;
    releaseHub?.();
    releaseTab?.();
    releaseHub = null;
    releaseTab = null;
    try {
      bus?.close();
    } catch {
      // already closed
    }
    bus = null;
    localTabs.clear();
    browsers.clear();
    pending.clear();
    for (const t of [...up]) hooks.onPeer({ user: self.user, peer: t }, false);
    up.clear();
    me = null;
  };

  return {
    supported: opts.supported ?? roomSupported(),
    want(users) {
      const self = hooks.self();
      const wanted = self !== null && users.has(self.user);
      if (me && (!wanted || !self || self.user !== me.user || self.peer !== me.peer)) stop();
      if (wanted && self && !me) start(self);
    },
    send(to, message) {
      if (!me || to.user !== me.user) return;
      const list = pending.get(message);
      if (list) list.push(to.peer);
      else pending.set(message, [to.peer]);
      if (flushing) return;
      flushing = true;
      void Promise.resolve().then(flush);
    },
    close() {
      stop();
    },
  };
}
