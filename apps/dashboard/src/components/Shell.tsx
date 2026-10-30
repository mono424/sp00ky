import { For, Show, createEffect, createMemo, createSignal, onCleanup, type JSX } from 'solid-js';
import { A, useLocation, useNavigate } from '@solidjs/router';
import { currentMode, servedByNode } from '../api/client';
import { formatCount } from '../lib/format';
import { Logo } from './Chrome';
import type { LoginResponse, Overview } from '../api/types';

interface NavLink {
  href: string;
  label: string;
  count?: string;
  /** Amber count: something there needs looking at. */
  warn?: boolean;
  /** Section heading the link sits under; links keep their order. */
  group?: string;
}

/** Two letters for an avatar: initials of a name, or the first two of a slug. */
function initials(text: string | undefined): string {
  const words = (text ?? '').trim().split(/[\s._@-]+/).filter(Boolean);
  if (words.length === 0) return '?';
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return (words[0][0] + words[1][0]).toUpperCase();
}

// Icons inherit link color; text remains the accessible navigation name.
const NAV_PATHS: Record<string, string> = {
  '/': 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  '/ssps': 'M4 3h16v7H4z M4 14h16v7H4z M7 6.5h1 M7 17.5h1 M16 6.5h1 M16 17.5h1',
  '/backends': 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M4 12c0 4 16 4 16 0',
  '/views': 'M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z M9 12a3 3 0 1 0 6 0 3 3 0 1 0-6 0',
  '/runs': 'M3 3h6v6H3z M15 15h6v6h-6z M6 9v9h9 M9 6h9v9',
  '/backups': 'M4 4h16v5H4z M6 9v12h12V9 M9 13h6',
  '/incidents': 'M12 3 2 21h20L12 3z M12 9v5 M12 17v1',
  '/logs': 'M5 3h14v18H5z M8 7h8 M8 11h8 M8 15h5',
  '/access': 'M5 10h14v11H5z M8 10V6a4 4 0 0 1 8 0v4 M12 14v3',
  '/resources': 'M6 6h12v12H6z M9 9h6v6H9z M9 3v3 M15 3v3 M9 18v3 M15 18v3 M3 9h3 M3 15h3 M18 9h3 M18 15h3',
};

function NavIcon(props: { href: string }) {
  return <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
    stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"
    aria-hidden="true" style={{ 'flex-shrink': '0' }}>
    <path d={NAV_PATHS[props.href]} />
  </svg>;
}

/**
 * Sidebar + content frame.
 *
 * The sidebar sits on the dark frame; the routed page lives in `.main`, one
 * rounded card inset from the frame (see theme.css, LAYOUT). The card is the
 * scroll container, which is what lets each page's header stick to its top.
 *
 * Nav counts come from the overview poll the app already runs, so the rail is
 * live without a request of its own.
 *
 * Below 860px the rail becomes an off-canvas drawer behind a top bar: 224px of
 * a 375px viewport is more than half the screen spent on navigation. The
 * drawer is CSS-driven; this component owns only the open state and the
 * things JS has to do — close on navigation, close on Escape, and stop the
 * page behind it scrolling.
 */
export function Shell(props: {
  session: LoginResponse;
  overview: Overview | undefined;
  onSignOut: () => void;
  children: JSX.Element;
}) {
  const location = useLocation();
  const [open, setOpen] = createSignal(false);

  // Pages only a scheduler can fill: an SSP serving the API alone has no
  // presence sampler, incident log, log ring or backups.
  const clusterOnly = new Set(['/views', '/backups', '/incidents', '/logs']);
  const links = (): NavLink[] => allLinks().filter((l) => !servedByNode() || !clusterOnly.has(l.href));
  const allLinks = (): NavLink[] => [
    // Where an operator starts: the cluster, what went wrong, what it said.
    { href: '/', label: 'Overview' },
    {
      href: '/incidents',
      label: 'Incidents',
      count: props.overview?.incidents?.open ? formatCount(props.overview.incidents.open) : undefined,
      warn: !!props.overview?.incidents?.open,
    },
    { href: '/logs', label: 'Logs' },
    {
      href: '/ssps',
      label: 'SSPs',
      group: 'Sync',
      count: props.overview
        ? `${props.overview.totals.ssps_ready}/${props.overview.totals.ssps}`
        : undefined,
    },
    {
      href: '/backends',
      label: 'Backends',
      group: 'Sync',
      count: props.overview
        ? `${props.overview.totals.backends_healthy}/${props.overview.totals.backends}`
        : undefined,
    },
    {
      href: '/views',
      label: 'Views',
      group: 'Sync',
      // Off the presence block the overview poll already carries. Absent until
      // the scheduler's sampler has run once, which is not the same as "no
      // views are registered".
      count: props.overview?.presence?.ready
        ? formatCount(props.overview.presence.totals.views)
        : undefined,
    },
    {
      href: '/runs',
      label: 'Runs',
      group: 'Automation',
      // Work outstanding, not work done: pending plus in flight is the number
      // that means "there is something to look at". Absent until the job
      // sampler has run once, which is not the same as "the queue is empty".
      count: props.overview?.jobs?.ready
        ? formatCount(
            props.overview.jobs.counts.pending +
              props.overview.jobs.counts.processing,
          )
        : undefined,
    },
    { href: '/backups', label: 'Backups', group: 'Data' },
    // Sp00ky Cloud only: the scheduler cannot measure its neighbours or the
    // database's volume, the control plane can. Hidden, not disabled, when
    // unlinked: there is nothing on that page a self-hosted cluster could use.
    ...(props.overview?.cloud_linked ? [{ href: '/resources', label: 'Resources', group: 'Data' }] : []),
    { href: '/access', label: 'Access', group: 'Data' },
  ];

  /** Consecutive links with the same group, in order. */
  const sections = () => {
    const out: { group?: string; links: NavLink[] }[] = [];
    for (const l of links()) {
      const last = out[out.length - 1];
      if (last && last.group === l.group) last.links.push(l);
      else out.push({ group: l.group, links: [l] });
    }
    return out;
  };

  const slug = () => currentMode().config?.project_slug ?? 'sp00ky';

  // Navigating closes the drawer — otherwise tapping a link leaves it sitting
  // open over the page it just moved to.
  createEffect(() => {
    location.pathname;
    setOpen(false);
  });

  /**
   * Entrance animations play once, when a page is opened, and never again.
   * Polled lists rebuild their rows on every refresh (each poll is new
   * objects, so `<For>` mounts new nodes), and without this every refresh
   * replayed the rise-in. A second after navigating the shell is marked
   * settled; the stylesheet switches entrance animations off under it.
   */
  const [settled, setSettled] = createSignal(false);
  let settleTimer: ReturnType<typeof setTimeout> | undefined;
  createEffect(() => {
    location.pathname;
    setSettled(false);
    clearTimeout(settleTimer);
    settleTimer = setTimeout(() => setSettled(true), 1000);
  });
  onCleanup(() => clearTimeout(settleTimer));

  // An open drawer must not let the page behind it scroll under the scrim.
  createEffect(() => {
    document.body.style.overflow = open() ? 'hidden' : '';
  });
  onCleanup(() => {
    document.body.style.overflow = '';
  });

  const atRoot = () => /^(\/admin)?\/?$/.test(location.pathname);
  const [menuOpen, setMenuOpen] = createSignal(false);
  const [paletteOpen, setPaletteOpen] = createSignal(false);
  createEffect(() => {
    location.pathname;
    setMenuOpen(false);
  });
  // A click anywhere outside the user menu closes it.
  const onDocClick = (e: MouseEvent) => {
    if (menuOpen() && !(e.target as Element).closest('.sb-me')) setMenuOpen(false);
  };
  document.addEventListener('click', onDocClick);
  onCleanup(() => document.removeEventListener('click', onDocClick));

  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      setOpen(false);
      setMenuOpen(false);
    }
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      setPaletteOpen(!paletteOpen());
    }
  };
  window.addEventListener('keydown', onKey);
  onCleanup(() => window.removeEventListener('keydown', onKey));

  return (
    <div class="shell" classList={{ settled: settled() }}>
      {/* Mobile only; `display:none` above the breakpoint. */}
      <header class="mobile-bar">
        <button
          class="burger"
          classList={{ open: open() }}
          aria-label={open() ? 'Close navigation' : 'Open navigation'}
          aria-expanded={open()}
          onClick={() => setOpen(!open())}
        >
          <span />
          <span />
          <span />
        </button>
        <Logo />
      </header>

      <Show when={open()}>
        <div class="scrim show" onClick={() => setOpen(false)} />
      </Show>

      <nav class="sidebar" classList={{ open: open() }}>
        {/* Who is signed in. The menu holds what used to sit at the foot:
            access and signing out. */}
        <div class="sb-me">
          <button
            class="sb-me-btn"
            aria-haspopup="menu"
            aria-expanded={menuOpen()}
            onClick={() => setMenuOpen(!menuOpen())}
          >
            <span class="sb-av" aria-hidden="true">{initials(props.session.label)}</span>
            <span class="sb-me-name" title={props.session.subject}>{props.session.label}</span>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
              stroke-width="1.6" aria-hidden="true">
              <path d="M7 10l5 5 5-5" />
            </svg>
          </button>
          <Show when={menuOpen()}>
            <div class="sb-menu" role="menu">
              <div class="sb-menu-sub">
                {slug()} · {props.session.mode === 'breakglass' ? 'break-glass' : props.session.subject}
              </div>
              <A href="/access" role="menuitem" onClick={() => setMenuOpen(false)}>
                Access and tokens
              </A>
              <button role="menuitem" onClick={props.onSignOut}>
                Sign out
              </button>
            </div>
          </Show>
        </div>

        <button class="sb-jump cap" onClick={() => setPaletteOpen(true)}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            stroke-width="1.6" aria-hidden="true">
            <path d="M10.5 4a6.5 6.5 0 1 0 0 13 6.5 6.5 0 0 0 0-13z M20 20l-4.5-4.5" />
          </svg>
          Jump to
          <kbd>⌘K</kbd>
        </button>

        <div class="nav">
          <For each={sections()}>
            {(section) => (
              <div class="nav-section">
                <Show when={section.group}>
                  <div class="nav-heading">{section.group}</div>
                </Show>
                <For each={section.links}>
                  {(link) => (
                    // `activeClass` + `end` rather than comparing pathnames by
                    // hand: the router applies the base ("/admin") to every
                    // href, so a hand-rolled `location.pathname === href`
                    // never matches and the rail silently highlights nothing.
                    // `end` keeps "/" from matching every route as a prefix.
                    <A
                      href={link.href}
                      class="nav-link"
                      activeClass="active"
                      end={link.href === '/'}
                      // The scheduler serves the app at "/admin/", which the
                      // router's exact match for "/admin" does not count.
                      // Only on that link: the router merges this over its own
                      // active class, so `active: false` elsewhere would erase it.
                      classList={link.href === '/' ? { active: atRoot() } : undefined}
                    >
                      <span class="row" style={{ gap: '10px' }}><NavIcon href={link.href} /><span>{link.label}</span></span>
                      <Show when={link.count}>
                        <span class="nav-count" classList={{ warn: link.warn }}>{link.count}</span>
                      </Show>
                    </A>
                  )}
                </For>
              </div>
            )}
          </For>
        </div>

        <div class="sb-foot cap">
          {props.overview?.scheduler?.version?.replace(/^0\.0\.1-/, '') ?? currentMode().config?.version?.replace(/^0\.0\.1-/, '') ?? ''}
        </div>
      </nav>

      <Show when={paletteOpen()}>
        <Palette links={links()} onClose={() => setPaletteOpen(false)} />
      </Show>

      <div class="main-frame">
        <main class="main">
          {props.children}
          <Show when={props.session.mode === 'breakglass'}>
            <div class="page-body">
              <div class="banner" style={{ 'margin-top': '16px' }}>
                <span class="dot warn" />
                Signed in with the break-glass password — the{' '}
                <span class="dim mono">_00_admin</span> roster was bypassed.
              </div>
            </div>
          </Show>
        </main>
      </div>
    </div>
  );
}

/**
 * Jump to any page by typing part of its name. ⌘K (Ctrl-K) opens it from
 * anywhere; arrows move, Enter goes, Escape closes.
 */
function Palette(props: { links: NavLink[]; onClose: () => void }) {
  const navigate = useNavigate();
  const [query, setQuery] = createSignal('');
  const [index, setIndex] = createSignal(0);
  const matches = createMemo(() => {
    const q = query().trim().toLowerCase();
    return q ? props.links.filter((l) => `${l.label} ${l.group ?? ''}`.toLowerCase().includes(q)) : props.links;
  });
  createEffect(() => {
    query();
    setIndex(0);
  });

  const go = (l: NavLink | undefined) => {
    if (!l) return;
    props.onClose();
    navigate(l.href);
  };

  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setIndex((i) => Math.min(i + 1, matches().length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      go(matches()[index()]);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      props.onClose();
    }
  };

  return (
    <div class="palette-scrim" onClick={(e) => e.target === e.currentTarget && props.onClose()}>
      <div class="palette" role="dialog" aria-label="Jump to a page">
        <input
          ref={(el) => queueMicrotask(() => el.focus())}
          placeholder="Jump to…"
          value={query()}
          onInput={(e) => setQuery(e.currentTarget.value)}
          onKeyDown={onKey}
        />
        <div class="palette-list">
          <For each={matches()} fallback={<div class="palette-empty cap">No page matches</div>}>
            {(l, i) => (
              <button
                class="palette-item"
                classList={{ on: i() === index() }}
                onMouseEnter={() => setIndex(i())}
                onClick={() => go(l)}
              >
                <NavIcon href={l.href} />
                {l.label}
                <span class="cap">{l.group ?? ''}</span>
              </button>
            )}
          </For>
        </div>
      </div>
    </div>
  );
}
