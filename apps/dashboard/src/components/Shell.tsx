import { For, Show, createEffect, createSignal, onCleanup, type JSX } from 'solid-js';
import { A, useLocation } from '@solidjs/router';
import { formatCount } from '../lib/format';
import { Logo } from './Chrome';
import type { LoginResponse, Overview } from '../api/types';

interface NavLink {
  href: string;
  label: string;
  count?: string;
}

// Icons inherit link color; text remains the accessible navigation name.
const NAV_PATHS: Record<string, string> = {
  '/': 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  '/ssps': 'M4 3h16v7H4z M4 14h16v7H4z M7 6.5h1 M7 17.5h1 M16 6.5h1 M16 17.5h1',
  '/backends': 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M4 12c0 4 16 4 16 0',
  '/views': 'M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z M9 12a3 3 0 1 0 6 0 3 3 0 1 0-6 0',
  '/workflows': 'M3 3h6v6H3z M15 15h6v6h-6z M6 9v9h9 M9 6h9v9',
  '/schedules': 'M5 4h14v17H5z M8 2v4 M16 2v4 M5 9h14 M8 13h2 M14 13h2 M8 17h2',
  '/jobs': 'M5 6h14v15H5z M9 6V3h6v3 M8 11h8 M8 15h8',
  '/backups': 'M4 4h16v5H4z M6 9v12h12V9 M9 13h6',
  '/incidents': 'M12 3 2 21h20L12 3z M12 9v5 M12 17v1',
  '/logs': 'M5 3h14v18H5z M8 7h8 M8 11h8 M8 15h5',
  '/access': 'M5 10h14v11H5z M8 10V6a4 4 0 0 1 8 0v4 M12 14v3',
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

  const links = (): NavLink[] => [
    { href: '/', label: 'Overview' },
    {
      href: '/ssps',
      label: 'SSPs',
      count: props.overview
        ? `${props.overview.totals.ssps_ready}/${props.overview.totals.ssps}`
        : undefined,
    },
    {
      href: '/backends',
      label: 'Backends',
      count: props.overview
        ? `${props.overview.totals.backends_healthy}/${props.overview.totals.backends}`
        : undefined,
    },
    {
      href: '/views',
      label: 'Views',
      // Off the presence block the overview poll already carries. Absent until
      // the scheduler's sampler has run once, which is not the same as "no
      // views are registered".
      count: props.overview?.presence?.ready
        ? formatCount(props.overview.presence.totals.views)
        : undefined,
    },
    { href: '/workflows', label: 'Workflows' },
    { href: '/schedules', label: 'Schedules' },
    {
      href: '/jobs',
      label: 'Jobs',
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
    { href: '/backups', label: 'Backups' },
    { href: '/incidents', label: 'Incidents' },
    { href: '/logs', label: 'Logs' },
    { href: '/access', label: 'Access' },
  ];

  // Navigating closes the drawer — otherwise tapping a link leaves it sitting
  // open over the page it just moved to.
  createEffect(() => {
    location.pathname;
    setOpen(false);
  });

  // An open drawer must not let the page behind it scroll under the scrim.
  createEffect(() => {
    document.body.style.overflow = open() ? 'hidden' : '';
  });
  onCleanup(() => {
    document.body.style.overflow = '';
  });

  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'Escape') setOpen(false);
  };
  window.addEventListener('keydown', onKey);
  onCleanup(() => window.removeEventListener('keydown', onKey));

  return (
    <div class="shell">
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
        <span class="brand-tag">admin</span>
      </header>

      <Show when={open()}>
        <div class="scrim show" onClick={() => setOpen(false)} />
      </Show>

      <nav class="sidebar" classList={{ open: open() }}>
        <div class="brand">
          <Logo />
          <span class="brand-tag">admin</span>
        </div>

        <div class="nav">
          <For each={links()}>
            {(link) => (
              // `activeClass` + `end` rather than comparing pathnames by hand:
              // the router applies the base ("/admin") to every href, so a
              // hand-rolled `location.pathname === href` never matches and the
              // rail silently highlights nothing. `end` keeps "/" from
              // matching every route as a prefix.
              <A
                href={link.href}
                class="nav-link"
                activeClass="active"
                end={link.href === '/'}
              >
                <span class="row" style={{ gap: '10px' }}><NavIcon href={link.href} /><span>{link.label}</span></span>
                <Show when={link.count}>
                  <span class="nav-count">{link.count}</span>
                </Show>
              </A>
            )}
          </For>
        </div>

        <div class="sidebar-foot">
          <div class="spread">
            <span class="truncate" title={props.session.subject}>
              {props.session.label}
            </span>
            <button class="link-btn" onClick={props.onSignOut}>
              Sign out
            </button>
          </div>
          <Show when={props.overview?.scheduler?.version}>
            <div class="sidebar-version" title="Scheduler version">
              scheduler v{props.overview!.scheduler!.version}
            </div>
          </Show>
        </div>
      </nav>

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
