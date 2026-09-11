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
                <span>{link.label}</span>
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
