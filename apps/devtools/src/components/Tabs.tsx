import { For, Show, createSignal, createMemo, createEffect, onMount, onCleanup } from 'solid-js';
import { useDevTools } from '../context/DevToolsContext';
import { FrameSelect } from './FrameSelect';
import { formatMs, formatRelativeTime } from '../utils/formatters';
import type { HeartbeatInfo, TabType } from '../types/devtools';

// Events moved to the end (was first); Queries is now the default landing tab.
// Reads, writes, then the log: the three things you watch while reproducing.
const tabs: { id: TabType; label: string }[] = [
  { id: 'queries', label: 'Queries' },
  { id: 'mutations', label: 'Mutations' },
  { id: 'logs', label: 'Logs' },
  { id: 'timing', label: 'Timing' },
  { id: 'database', label: 'Database' },
  { id: 'storage', label: 'Storage' },
  { id: 'access', label: 'Access' },
  { id: 'versions', label: 'Stack' },
  { id: 'mcp', label: 'MCP' },
  { id: 'events', label: 'Events' },
];

// Reserve for the » overflow button when it has to be shown.
const CHEVRON_W = 30;

// What a plain Refresh click actually refetches, per tab — every tab also gets
// a `getState()` resync, which is what covers Queries/Timing/Events (and the
// session half of Access).
//
// Typed as a full Record so adding a TabType member is a compile error here as
// well as in `refreshScoped` (context/DevToolsContext.tsx). The copy and the
// behavior can't drift apart.
const REFRESH_SCOPE: Record<TabType, string> = {
  queries: 'page state',
  mutations: 'the outbox and the failed-writes tray',
  logs: 'the log backlog',
  timing: 'page state',
  events: 'page state',
  access: 'feature flags',
  database: 'the table list and rows',
  storage: 'storage diagnostics',
  versions: 'version discovery',
  mcp: 'MCP bridge status',
};

// What the toolbar Clear empties, per tab (see `clearActive` in the context).
const clearLabel = (tab: TabType): string =>
  tab === 'logs' ? 'Clear logs' : tab === 'mutations' ? 'Clear mutation history' : 'Clear events';

function RefreshIcon() {
  return (
    <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M17.65 6.35A7.958 7.958 0 0 0 12 4a8 8 0 1 0 7.75 10h-2.08A6 6 0 1 1 12 6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z" />
    </svg>
  );
}

function ClearIcon() {
  return (
    <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zM4 12c0-4.42 3.58-8 8-8 1.85 0 3.55.63 4.9 1.69L5.69 16.9A7.902 7.902 0 0 1 4 12zm8 8c-1.85 0-3.55-.63-4.9-1.69L18.31 7.1A7.902 7.902 0 0 1 20 12c0 4.42-3.58 8-8 8z" />
    </svg>
  );
}

function DoubleChevronIcon() {
  return (
    <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M6.41 6 5 7.41 9.58 12 5 16.59 6.41 18l6-6z" />
      <path d="m13 6-1.41 1.41L16.17 12l-4.58 4.59L13 18l6-6z" />
    </svg>
  );
}

export function Tabs() {
  const { state, activeTab, setActiveTab, frames, activeFrameId, refresh, isRefreshing, clearActive } =
    useDevTools();

  // E2E sync latency from the scheduler entity, shown in the toolbar so it is
  // visible from every tab. Absent (not zero) whenever the probe is off or the
  // scheduler is unreachable — an unknown latency must not read as a fast one.
  const heartbeat = (): HeartbeatInfo | undefined => {
    const scheduler = state.versions.entities?.find((e) => e.entity === 'scheduler');
    const hb = scheduler?.heartbeat;
    return hb?.enabled ? hb : undefined;
  };

  const hbFailing = (): boolean => {
    const hb = heartbeat();
    return !!hb && (hb.stale || hb.blocked === true || hb.consecutive_failures > 0);
  };

  const hbLabel = (): string => {
    const hb = heartbeat();
    if (!hb) return '';
    return hbFailing() ? 'e2e !' : formatMs(hb.last_e2e_ms);
  };

  // There is no poller: the value is as fresh as the last version discovery.
  // Say so rather than implying it is live.
  const hbTitle = (): string => {
    const hb = heartbeat();
    if (!hb) return '';
    const parts = ['End-to-end sync latency (scheduler probe)'];
    if (hb.last_ok_epoch_ms) parts.push(`last ok ${formatRelativeTime(hb.last_ok_epoch_ms)}`);
    else parts.push('no successful probe yet');
    if (hb.consecutive_failures > 0) parts.push(`${hb.consecutive_failures} consecutive failures`);
    if (hb.stale) parts.push('stale');
    parts.push('updates on Refresh');
    parts.push('click to open Stack');
    return parts.join(' · ');
  };

  const refreshLabel = () =>
    isRefreshing()
      ? 'Refreshing…'
      : `Refresh ${REFRESH_SCOPE[activeTab()]} — Shift+click to refresh everything`;

  let tabsEl: HTMLDivElement | undefined;
  let statusEl: HTMLDivElement | undefined;
  let actionsEl: HTMLDivElement | undefined;
  let overflowEl: HTMLDivElement | undefined;
  const tabRefs: Partial<Record<TabType, HTMLButtonElement>> = {};

  const [visibleCount, setVisibleCount] = createSignal(tabs.length);
  const [menuOpen, setMenuOpen] = createSignal(false);

  // Tab labels are static, so their intrinsic widths only need measuring once
  // (while every tab is still mounted on first paint).
  const tabWidths: Partial<Record<TabType, number>> = {};
  let measured = false;

  const measure = (): boolean => {
    if (measured) return true;
    for (const t of tabs) {
      const el = tabRefs[t.id];
      if (!el) return false;
      tabWidths[t.id] = el.offsetWidth;
    }
    measured = true;
    return true;
  };

  const recompute = () => {
    if (!tabsEl || !measure()) return;
    const containerW = tabsEl.clientWidth;
    const reserved = (statusEl?.offsetWidth ?? 0) + (actionsEl?.offsetWidth ?? 0) + 6;
    const availAll = containerW - reserved;
    const total = tabs.reduce((s, t) => s + (tabWidths[t.id] ?? 0), 0);
    if (total <= availAll) {
      setVisibleCount(tabs.length);
      return;
    }
    const avail = availAll - CHEVRON_W;
    let sum = 0;
    let n = 0;
    for (const t of tabs) {
      const w = tabWidths[t.id] ?? 0;
      if (sum + w <= avail) {
        sum += w;
        n++;
      } else break;
    }
    setVisibleCount(n);
  };

  // Split into visible + overflow, always keeping the active tab visible (pull
  // it out of the overflow menu, Chrome-style, displacing the last visible tab).
  const split = createMemo(() => {
    const n = visibleCount();
    let visible = tabs.slice(0, n);
    let overflow = tabs.slice(n);
    const act = activeTab();
    if (overflow.some((t) => t.id === act)) {
      // oxlint-disable-next-line no-non-null-assertion -- guarded by .some() above
      const actTab = overflow.find((t) => t.id === act)!;
      const rest = overflow.filter((t) => t.id !== act);
      if (visible.length > 0) {
        const displaced = visible[visible.length - 1];
        visible = [...visible.slice(0, -1), actTab];
        overflow = [displaced, ...rest];
      } else {
        visible = [actTab];
        overflow = rest;
      }
    }
    return { visible, overflow };
  });

  // The badge widens the right-hand group, which `recompute` treats as
  // reserved space. It appears asynchronously (first version discovery), so
  // without this the tab split keeps using the pre-badge reservation.
  createEffect(() => {
    const present = !!heartbeat();
    const width = hbLabel().length;
    void present;
    void width;
    requestAnimationFrame(recompute);
  });

  // Same story on the left: the frame picker is a bare dot until a second
  // client shows up, then grows a label. That widens `statusEl`, which is also
  // reserved space in `recompute`.
  createEffect(() => {
    void frames().length;
    void activeFrameId();
    requestAnimationFrame(recompute);
  });

  onMount(() => {
    requestAnimationFrame(recompute);

    const ro = new ResizeObserver(() => recompute());
    if (tabsEl) ro.observe(tabsEl);

    const onDocClick = (e: MouseEvent) => {
      if (menuOpen() && overflowEl && !overflowEl.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setMenuOpen(false);
    };
    document.addEventListener('click', onDocClick);
    document.addEventListener('keydown', onKey);

    onCleanup(() => {
      ro.disconnect();
      document.removeEventListener('click', onDocClick);
      document.removeEventListener('keydown', onKey);
    });
  });

  const pickOverflow = (id: TabType) => {
    setActiveTab(id);
    setMenuOpen(false);
  };

  return (
    <div class="tabs" ref={tabsEl}>
      <div class="toolbar-group" ref={statusEl}>
        <FrameSelect />
      </div>

      <For each={split().visible}>
        {(tab) => (
          <button
            class="tab-btn"
            ref={(el) => (tabRefs[tab.id] = el)}
            classList={{ active: activeTab() === tab.id }}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        )}
      </For>

      <Show when={split().overflow.length > 0}>
        <div class="tab-overflow" ref={overflowEl}>
          <button
            class="tab-overflow-btn"
            classList={{ open: menuOpen() }}
            title="More tabs"
            aria-label="More tabs"
            aria-expanded={menuOpen()}
            onClick={() => setMenuOpen((v) => !v)}
          >
            <DoubleChevronIcon />
          </button>
          <Show when={menuOpen()}>
            <div class="tab-overflow-menu" role="menu">
              <For each={split().overflow}>
                {(tab) => (
                  <button
                    class="tab-overflow-item"
                    classList={{ active: activeTab() === tab.id }}
                    role="menuitem"
                    onClick={() => pickOverflow(tab.id)}
                  >
                    {tab.label}
                  </button>
                )}
              </For>
            </div>
          </Show>
        </div>
      </Show>

      <div class="toolbar-group-right" ref={actionsEl}>
        {/* A button, not a readout: the number is the summary of the chart on
            the Stack tab, so clicking it goes there rather than making you find
            the tab that explains it. */}
        <Show when={heartbeat()}>
          <button
            type="button"
            class="hb-badge"
            classList={{ failing: hbFailing(), active: activeTab() === 'versions' }}
            title={hbTitle()}
            aria-label={hbTitle()}
            onClick={() => setActiveTab('versions')}
          >
            <span class="hb-badge-icon" aria-hidden="true">
              ♥
            </span>
            {hbLabel()}
          </button>
        </Show>
        {/* Shift+click = refresh everything. Note a keyboard activation reports
            shiftKey:false in Chrome, so there is no keyboard path to a full
            refresh — acceptable for a power-user escape hatch. */}
        <button
          class="icon-btn"
          classList={{ 'is-busy': isRefreshing() }}
          title={refreshLabel()}
          aria-label={refreshLabel()}
          disabled={isRefreshing()}
          onClick={(e) => refresh({ full: e.shiftKey })}
        >
          <RefreshIcon />
        </button>
        <button
          class="icon-btn"
          title={clearLabel(activeTab())}
          aria-label={clearLabel(activeTab())}
          onClick={clearActive}
        >
          <ClearIcon />
        </button>
      </div>
    </div>
  );
}
