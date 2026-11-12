import { For, Show, createEffect, createMemo, createSignal, onCleanup, type JSX } from 'solid-js';
import { A, useLocation, useNavigate, useResolvedPath } from '@solidjs/router';
import { currentMode, servedByNode } from '../api/client';
import { formatCount } from '../lib/format';
import { Logo } from './Chrome';
import type { LoginResponse, Overview } from '../api/types';
import { GEAR, WORKSPACE_LINKS, WorkspaceMenu, projectLine, projectTone, useConsoleChrome } from '../console/chrome';

export interface NavLink {
  href: string;
  label: string;
  /** SVG path data; the page's own icon by default. */
  icon?: string;
  count?: string;
  /** Amber count: something there needs looking at. */
  warn?: boolean;
  /** Section heading the link sits under; links keep their order. */
  group?: string;
  /** A console address outside this router (another project, the workspace). */
  go?: boolean;
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
  '/database': 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M4 12c0 4 16 4 16 0',
  '/users': 'M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2 M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z M22 21v-2a4 4 0 0 0-3-3.9 M16 3.1a4 4 0 0 1 0 7.8',
  '/flags': 'M5 21V3 M5 4h12l-2.5 4.5L17 13H5',
  '/storage': 'M3 6h6l2 2h10v11H3z',
  '/resources': 'M6 6h12v12H6z M9 9h6v6H9z M9 3v3 M15 3v3 M9 18v3 M15 18v3 M3 9h3 M3 15h3 M18 9h3 M18 15h3',
  '/deployments': 'M12 3l9 5-9 5-9-5 9-5z M3 13l9 5 9-5',
  '/audience': 'M4 20V11 M10 20V4 M16 20v-7 M21 20H3',
  '/settings/env': 'M4 7h16 M4 12h16 M4 17h10 M18 15l3 3-3 3',
  '/settings/domains': 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z M3 12h18 M12 3c3 3.5 3 14.5 0 18 M12 3c-3 3.5-3 14.5 0 18',
  '/settings/git': 'M6 3v12 M18 9a3 3 0 1 0 0-6 3 3 0 0 0 0 6z M6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6z M18 9a9 9 0 0 1-9 9',
  '/settings/scale': 'M4 21v-7 M4 10V3 M12 21v-9 M12 8V3 M20 21v-5 M20 12V3 M1 14h6 M9 8h6 M17 16h6',
  '/settings/database': 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M12 18v-7 M9 14l3-3 3 3',
  '/settings/danger': 'M12 9v4 M12 17h.01 M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z',
  project: 'M4 4h16v16H4z M9 9h6v6H9z',
  workspace: 'M3 21h18 M5 21V7l7-4 7 4v14 M9 21v-6h6v6',
  '/settings': 'M12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6z M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z',
};

function NavIcon(props: { href: string; icon?: string }) {
  return <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
    stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"
    aria-hidden="true" style={{ 'flex-shrink': '0' }}>
    <path d={props.icon ?? NAV_PATHS[props.href]} />
  </svg>;
}

/**
 * The console rail's icons: solid glyphs, so the rail reads at a glance and the
 * labels carry the weight. `f` is filled (even-odd, so holes cut through), `s`
 * an optional stroke drawn over it.
 */
const RAIL_ICONS: Record<string, { f: string; s?: string }> = {
  '/': { f: 'M11.2 2.8a1.2 1.2 0 0 1 1.6 0l7.8 6.9c.3.2.4.6.4.9v9.4a1.5 1.5 0 0 1-1.5 1.5h-4.3v-5.6a1 1 0 0 0-1-1h-4.4a1 1 0 0 0-1 1v5.6H4.5A1.5 1.5 0 0 1 3 20v-9.4c0-.3.1-.7.4-.9z' },
  '/activity': { f: 'M12 2a10 10 0 1 1 0 20 10 10 0 0 1 0-20z M11 6.5v6.1l4.4 2.7.95-1.55-3.55-2.15V6.5z' },
  '/deployments': { f: 'M12 2a10 10 0 1 1 0 20 10 10 0 0 1 0-20z M12 6.2 7.3 10.9l1.3 1.3 2.5-2.5v7.3h1.8V9.7l2.5 2.5 1.3-1.3z' },
  '/audience': { f: 'M4.5 12h3a.5.5 0 0 1 .5.5v8a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-8a.5.5 0 0 1 .5-.5z M10.5 3h3a.5.5 0 0 1 .5.5v17a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-17a.5.5 0 0 1 .5-.5z M16.5 8h3a.5.5 0 0 1 .5.5v12a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-12a.5.5 0 0 1 .5-.5z' },
  '/incidents': { f: 'M10.3 3.4a2 2 0 0 1 3.4 0l8.3 14.6A2 2 0 0 1 20.3 21H3.7A2 2 0 0 1 2 18L10.3 3.4z M11.1 9v5.2h1.8V9z M11.1 15.8v1.9h1.8v-1.9z' },
  '/logs': { f: 'M6.5 2h11A2.5 2.5 0 0 1 20 4.5v15a2.5 2.5 0 0 1-2.5 2.5h-11A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z M8 6.6v1.7h8V6.6z M8 10.6v1.7h8v-1.7z M8 14.6v1.7h5v-1.7z' },
  '/ssps': { f: 'M4.5 3h15A1.5 1.5 0 0 1 21 4.5v5a1.5 1.5 0 0 1-1.5 1.5h-15A1.5 1.5 0 0 1 3 9.5v-5A1.5 1.5 0 0 1 4.5 3z M4.5 13h15a1.5 1.5 0 0 1 1.5 1.5v5a1.5 1.5 0 0 1-1.5 1.5h-15A1.5 1.5 0 0 1 3 19.5v-5A1.5 1.5 0 0 1 4.5 13z M6 6.1v1.8h4V6.1z M6 16.1v1.8h4v-1.8z M16 6.1v1.8h2V6.1z M16 16.1v1.8h2v-1.8z' },
  '/backends': { f: 'M11.5 2.6a1.2 1.2 0 0 1 1 0l8.6 4.3c.7.4.7 1.4 0 1.8l-8.6 4.3a1.2 1.2 0 0 1-1 0L2.9 8.7c-.7-.4-.7-1.4 0-1.8z M4.6 11.3l6.9 3.5c.3.2.7.2 1 0l6.9-3.5 1.7.9c.7.4.7 1.4 0 1.8l-8.6 4.3a1.2 1.2 0 0 1-1 0L2.9 14c-.7-.4-.7-1.4 0-1.8z M4.6 16.4l6.9 3.5c.3.2.7.2 1 0l6.9-3.5 1.7.9c.7.4.7 1.4 0 1.8l-8.6 4.3a1.2 1.2 0 0 1-1 0l-8.6-4.3c-.7-.4-.7-1.4 0-1.8z' },
  '/views': { f: 'M12 4.5c5.6 0 9.6 5.2 10 7.5-.4 2.3-4.4 7.5-10 7.5S2.4 14.3 2 12c.4-2.3 4.4-7.5 10-7.5z M12 8.3a3.7 3.7 0 1 0 0 7.4 3.7 3.7 0 0 0 0-7.4z M12 10.2a1.8 1.8 0 1 1 0 3.6 1.8 1.8 0 0 1 0-3.6z' },
  '/runs': { f: 'M6 2.5a3.2 3.2 0 1 1 0 6.4 3.2 3.2 0 0 1 0-6.4z M18 15.1a3.2 3.2 0 1 1 0 6.4 3.2 3.2 0 0 1 0-6.4z', s: 'M9.5 5.7h7a3.15 3.15 0 0 1 0 6.3h-9a3.15 3.15 0 0 0 0 6.3h5' },
  '/database': { f: 'M12 2c4.4 0 8 1.4 8 3.2S16.4 8.4 12 8.4 4 7 4 5.2 7.6 2 12 2z M4 7.9c1.7 1.3 4.7 2 8 2s6.3-.7 8-2v4c0 1.8-3.6 3.2-8 3.2S4 13.7 4 11.9z M4 14.6c1.7 1.3 4.7 2 8 2s6.3-.7 8-2v4.2c0 1.8-3.6 3.2-8 3.2s-8-1.4-8-3.2z' },
  '/users': { f: 'M12 2.8a4.6 4.6 0 1 1 0 9.2 4.6 4.6 0 0 1 0-9.2z M3.8 20.2c0-3.9 3.7-6.6 8.2-6.6s8.2 2.7 8.2 6.6v.3a.7.7 0 0 1-.7.7H4.5a.7.7 0 0 1-.7-.7z' },
  '/members': { f: 'M9 3a4.2 4.2 0 1 1 0 8.4A4.2 4.2 0 0 1 9 3z M1.8 19.6c0-3.6 3.2-6 7.2-6s7.2 2.4 7.2 6v.7a.7.7 0 0 1-.7.7H2.5a.7.7 0 0 1-.7-.7z M15.6 3.5a4 4 0 0 1 0 7.4 5.6 5.6 0 0 0 0-7.4z M17.4 13.8c2.8.6 4.8 2.7 4.8 5.6v.9a.7.7 0 0 1-.7.7h-3.8c.2-.3.3-.7.3-1.1v-.8c0-2.1-.2-3.6-.6-5.3z' },
  '/flags': { f: 'M4.6 2.5h1.8V22H4.6z M7.6 3.5h11.5c.5 0 .8.6.5 1l-2.7 4.3 2.7 4.3c.3.4 0 1-.5 1H7.6z' },
  '/storage': { f: 'M3.5 4h5.8c.4 0 .8.2 1.1.5L12.3 6.6h8.2A1.5 1.5 0 0 1 22 8.1v10.4a1.5 1.5 0 0 1-1.5 1.5h-17A1.5 1.5 0 0 1 2 18.5v-13A1.5 1.5 0 0 1 3.5 4z' },
  '/backups': { f: 'M3.2 3.5h17.6c.7 0 1.2.5 1.2 1.2v3.1c0 .7-.5 1.2-1.2 1.2H3.2C2.5 9 2 8.5 2 7.8V4.7c0-.7.5-1.2 1.2-1.2z M3.6 10.5h16.8v8A2.5 2.5 0 0 1 17.9 21H6.1a2.5 2.5 0 0 1-2.5-2.5z M9.2 13.2v1.7h5.6v-1.7z' },
  '/resources': { f: 'M7.5 5h9A2.5 2.5 0 0 1 19 7.5v9a2.5 2.5 0 0 1-2.5 2.5h-9A2.5 2.5 0 0 1 5 16.5v-9A2.5 2.5 0 0 1 7.5 5z M9.6 9.6v4.8h4.8V9.6z', s: 'M9 2v3 M15 2v3 M9 19v3 M15 19v3 M2 9h3 M2 15h3 M19 9h3 M19 15h3' },
  '/billing': { f: 'M4.5 4h15A2.5 2.5 0 0 1 22 6.5V8H2V6.5A2.5 2.5 0 0 1 4.5 4z M2 10.2h20v7.3a2.5 2.5 0 0 1-2.5 2.5h-15A2.5 2.5 0 0 1 2 17.5z M5 14.3V16h5v-1.7z' },
  '/vault': { f: 'M7.6 10V7.2a4.4 4.4 0 0 1 8.8 0V10h-2V7.2a2.4 2.4 0 0 0-4.8 0V10z M5.5 10h13a1.5 1.5 0 0 1 1.5 1.5v8.5a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 4 20v-8.5A1.5 1.5 0 0 1 5.5 10z M11.1 14v3.4h1.8V14z' },
  '/keys': { f: 'M15 2a7 7 0 1 1-2.6 13.5l-1.7 1.7H8.8v1.9H6.9V21H3.6a.6.6 0 0 1-.6-.6v-2.7c0-.2.1-.3.2-.4l5.9-5.9A7 7 0 0 1 15 2z M16.4 5.4a2.2 2.2 0 1 0 0 4.4 2.2 2.2 0 0 0 0-4.4z' },
  '/settings': { f: GEAR },
};
RAIL_ICONS['/access'] = RAIL_ICONS['/keys'];
RAIL_ICONS['/settings/env'] = RAIL_ICONS['/settings'];
RAIL_ICONS['/settings/domains'] = { f: 'M12 2a10 10 0 1 1 0 20 10 10 0 0 1 0-20z', s: 'M2.5 12h19 M12 2.5c2.8 3 2.8 16 0 19 M12 2.5c-2.8 3-2.8 16 0 19' };
RAIL_ICONS['/settings/git'] = { f: 'M6 2.5a2.6 2.6 0 1 1 0 5.2 2.6 2.6 0 0 1 0-5.2z M6 16.3a2.6 2.6 0 1 1 0 5.2 2.6 2.6 0 0 1 0-5.2z M18 4.3a2.6 2.6 0 1 1 0 5.2 2.6 2.6 0 0 1 0-5.2z', s: 'M6 7.5v9 M18 9.5c0 4.5-5 5-11 7.5' };
RAIL_ICONS['/settings/scale'] = { f: 'M3 4h4v16H3z M10 9h4v11h-4z M17 13h4v7h-4z' };
RAIL_ICONS['/settings/database'] = RAIL_ICONS['/database'];
RAIL_ICONS['/settings/danger'] = RAIL_ICONS['/incidents'];

function RailIcon(props: { href: string; icon?: string }) {
  const glyph = () => RAIL_ICONS[props.href];
  return (
    <Show when={glyph()} fallback={<NavIcon href={props.href} icon={props.icon} />}>
      {(g) => (
        <svg class="rail-ic" width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
          <path d={g().f} fill="currentColor" fill-rule="evenodd" />
          <Show when={g().s}>
            <path d={g().s} fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />
          </Show>
        </svg>
      )}
    </Show>
  );
}

/** Small solid triangle, the rail's group caret. */
function Tri(props: { dir: 'down' | 'right' }) {
  return (
    <svg class="rail-tri" width="8" height="8" viewBox="0 0 8 8" aria-hidden="true">
      <path d={props.dir === 'down' ? 'M1 2.5h6L4 6z' : 'M2.5 1v6L6 4z'} fill="currentColor" />
    </svg>
  );
}

/** Persisted rail state: folded groups and the icon-only width. */
function stored<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(key);
    return v == null ? fallback : (JSON.parse(v) as T);
  } catch {
    return fallback;
  }
}
function store(key: string, v: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(v));
  } catch {
    /* private mode */
  }
}
const [railFolded, setRailFolded] = createSignal<string[]>(stored('spky.rail.folded', []));
const toggleFold = (group: string) => {
  const next = railFolded().includes(group) ? railFolded().filter((g) => g !== group) : [...railFolded(), group];
  setRailFolded(next);
  store('spky.rail.folded', next);
};
const [railMin, setRailMinSig] = createSignal<boolean>(stored('spky.rail.min', false));
const setRailMin = (v: boolean) => {
  setRailMinSig(v);
  store('spky.rail.min', v);
};

/**
 * A project's rail, counts off the overview where it has them. Quick access
 * first (what an operator opens most), then the cluster by layer. The console
 * adds Deployments and the project's Settings pages; `limited` is a project
 * whose own dashboard cannot open there, so only Sp00ky Cloud's pages remain.
 */
export function projectNav(console: boolean, overview?: Overview, limited = false): NavLink[] {
  const o = overview;
  const settings: NavLink[] = console
    ? [
        { href: '/settings', label: 'Environment', group: 'Settings', icon: NAV_PATHS['/settings/env'] },
        { href: '/settings/domains', label: 'Domains', group: 'Settings' },
        { href: '/settings/git', label: 'Git & builds', group: 'Settings' },
        { href: '/settings/scale', label: 'Scale', group: 'Settings' },
        { href: '/settings/database', label: 'Database', group: 'Settings' },
        ...(limited ? [] : [{ href: '/access', label: 'Access tokens', group: 'Settings' }]),
        { href: '/settings/danger', label: 'Danger zone', group: 'Settings' },
      ]
    : [];
  if (limited) return [{ href: '/', label: 'Overview' }, { href: '/deployments', label: 'Deployments' }, { href: '/audience', label: 'Audience' }, ...settings];
  return [
    // Where an operator starts: the cluster, what shipped, what went wrong, what it said.
    { href: '/', label: 'Overview' },
    ...(console ? [{ href: '/deployments', label: 'Deployments' }, { href: '/audience', label: 'Audience' }] : []),
    {
      href: '/incidents',
      label: 'Incidents',
      count: o?.incidents?.open ? formatCount(o.incidents.open) : undefined,
      warn: !!o?.incidents?.open,
    },
    { href: '/logs', label: 'Logs' },
    {
      href: '/ssps',
      label: 'SSPs',
      group: 'Sync',
      count: o ? `${o.totals.ssps_ready}/${o.totals.ssps}` : undefined,
    },
    {
      href: '/backends',
      label: 'Backends',
      group: 'Sync',
      count: o ? `${o.totals.backends_healthy}/${o.totals.backends}` : undefined,
    },
    {
      href: '/views',
      label: 'Views',
      group: 'Sync',
      // Off the presence block the overview poll already carries. Absent until
      // the scheduler's sampler has run once, which is not the same as "no
      // views are registered".
      count: o?.presence?.ready ? formatCount(o.presence.totals.views) : undefined,
    },
    {
      href: '/runs',
      label: 'Runs',
      group: 'Automation',
      // Work outstanding, not work done: pending plus in flight is the number
      // that means "there is something to look at". Absent until the job
      // sampler has run once, which is not the same as "the queue is empty".
      count: o?.jobs?.ready ? formatCount(o.jobs.counts.pending + o.jobs.counts.processing) : undefined,
    },
    // The project's own data. In the console first; a host says whether it
    // can serve it, since the console also opens older schedulers.
    ...(console && currentMode().config?.data
      ? [
          { href: '/database', label: 'Database', group: 'Data' },
          { href: '/users', label: 'Users', group: 'Data' },
          // A host older than the flag routes still serves the rest of `/data`.
          ...(currentMode().config?.flags ? [{ href: '/flags', label: 'Feature flags', group: 'Data' }] : []),
          { href: '/storage', label: 'Storage', group: 'Data' },
        ]
      : []),
    { href: '/backups', label: 'Backups', group: 'Data' },
    // Sp00ky Cloud only: the scheduler cannot measure its neighbours or the
    // database's volume, the control plane can. Hidden, not disabled, when
    // unlinked: there is nothing on that page a self-hosted cluster could use.
    ...(o?.cloud_linked ? [{ href: '/resources', label: 'Resources', group: 'Data' }] : []),
    // In the console the tokens are one of the project's settings.
    ...(console ? settings : [{ href: '/access', label: 'Access', group: 'Data' }]),
  ];
}

/** Consecutive links with the same group, in order. */
function sectionsOf(links: NavLink[]): { group?: string; links: NavLink[] }[] {
  const out: { group?: string; links: NavLink[] }[] = [];
  for (const l of links) {
    const last = out[out.length - 1];
    if (last && last.group === l.group) last.links.push(l);
    else out.push({ group: l.group, links: [l] });
  }
  return out;
}

/** One rail link, with its count. */
function NavItem(props: { link: NavLink; atRoot: () => boolean; rail?: boolean }) {
  const link = props.link;
  return (
    // `activeClass` + `end` rather than comparing pathnames by hand: the
    // router applies the base ("/admin") to every href, so a hand-rolled
    // `location.pathname === href` never matches and the rail silently
    // highlights nothing. `end` keeps "/" (and "/settings") from matching
    // every route under it as a prefix.
    <A
      href={link.href}
      class="nav-link"
      activeClass="active"
      end={link.href === '/' || link.href === '/settings'}
      // The scheduler serves the app at "/admin/", which the router's exact
      // match for "/admin" does not count. Only on that link: the router
      // merges this over its own active class, so `active: false` elsewhere
      // would erase it.
      classList={link.href === '/' ? { active: props.atRoot() } : undefined}
      title={props.rail ? link.label : undefined}
    >
      <span class="row" style={{ gap: '10px' }}>
        {props.rail ? <RailIcon href={link.href} icon={link.icon} /> : <NavIcon href={link.href} icon={link.icon} />}
        <span class="nav-label">{link.label}</span>
      </span>
      <Show when={link.count}>
        <span class="nav-count" classList={{ warn: link.warn }}>{link.count}</span>
      </Show>
    </A>
  );
}

/** Which console layer the rail showed last, so a change slides the right way. */
let lastLayer: 'workspace' | 'project' | undefined;

/**
 * The console's rail, in two layers. The workspace layer lists the
 * workspace's projects between its quick links and its own pages; opening a
 * project drills into the project layer: a way back, its quick links, then
 * the cluster and the project's settings in groups. The project switcher heads
 * both; the foot is one menu for the account and its workspaces.
 */
function ConsoleRail(props: { links: NavLink[]; workspaceLayer: boolean; atRoot: () => boolean; onJump: () => void }) {
  const cx = useConsoleChrome()!;
  const layer = props.workspaceLayer ? 'workspace' : 'project';
  const dir = lastLayer && lastLayer !== layer ? (layer === 'project' ? 'in' : 'out') : '';
  lastLayer = layer;
  const ws = () => cx.workspace();
  const quick = () => sectionsOf(props.links).filter((x) => !x.group);
  const grouped = () => sectionsOf(props.links).filter((x) => x.group);
  const location = useLocation();
  const Section = (sp: { group: string; count?: number; children: JSX.Element }) => {
    const folded = () => railFolded().includes(sp.group);
    return (
      <div class="nav-section" classList={{ folded: folded() }}>
        <button class="rail-head" aria-expanded={!folded()} onClick={() => toggleFold(sp.group)}>
          <span>{sp.group}</span>
          <Tri dir={folded() ? 'right' : 'down'} />
          <Show when={sp.count != null}><span class="rail-head-n">{sp.count}</span></Show>
        </button>
        <Show when={!folded()}>{sp.children}</Show>
      </div>
    );
  };
  return (
    <>
      <div class="cx-brand">
        <Logo />
        <span class="rail-tools">
          <button class="rail-tool" onClick={props.onJump} title="Search (⌘K)" aria-label="Search">
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">
              <path d="M10.5 3.5a7 7 0 1 0 0 14 7 7 0 0 0 0-14z M20.5 20.5l-5-5" />
            </svg>
          </button>
          <button class="rail-tool rail-min-btn" onClick={() => setRailMin(!railMin())} title={railMin() ? 'Expand sidebar (⌘B)' : 'Collapse sidebar (⌘B)'} aria-label={railMin() ? 'Expand sidebar' : 'Collapse sidebar'}>
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
              <rect x="3" y="4" width="18" height="16" rx="3" />
              <path d="M9.5 4v16" />
            </svg>
          </button>
        </span>
      </div>
      <div
        class={`nav cx-layer ${dir}`}
        ref={(el) => {
          // Keep the page you are on in view when the rail is taller than the screen.
          createEffect(() => {
            location.pathname;
            setTimeout(() => el.querySelector('.nav-link.active')?.scrollIntoView({ block: 'nearest' }), 60);
          });
        }}
      >
        <For each={quick()}>
          {(section) => (
            <div class="nav-section cx-quick">
              <For each={section.links}>{(link) => <NavItem link={link} atRoot={props.atRoot} rail />}</For>
            </div>
          )}
        </For>
        <Show when={props.workspaceLayer && ws()}>
          {(w) => (
            <Section group="Projects" count={w().projects.length}>
              <For each={w().projects}>
                {(p) => (
                  <button class="nav-link cx-pl" title={`${p.slug} · ${projectLine(p)}`} onClick={() => cx.go(`/${w().slug}/${p.slug}`)}>
                    <span class="row" style={{ gap: '10px', 'min-width': '0' }}>
                      <span class="rail-sq"><span class={`fx-sq ${projectTone(p)}`} /></span>
                      <span class="cx-pl-name nav-label">{p.slug}</span>
                    </span>
                    <span class="cx-pl-r">
                      <Show when={p.platform === 'cloudflare'}><span class="cx-tag cf">CF</span></Show>
                    </span>
                  </button>
                )}
              </For>
            </Section>
          )}
        </Show>
        <For each={grouped()}>
          {(section) => (
            <Section group={section.group!}>
              <For each={section.links}>{(link) => <NavItem link={link} atRoot={props.atRoot} rail />}</For>
            </Section>
          )}
        </For>
      </div>
      <div class="sb-foot cx-foot">
        <WorkspaceMenu />
      </div>
    </>
  );
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
  /** A rail of its own instead of a project's pages: a console workspace. */
  nav?: { heading: string; links: NavLink[] };
  children: JSX.Element;
}) {
  const location = useLocation();
  const [open, setOpen] = createSignal(false);
  // The top bar sticks above the page. While the page is at its top, the
  // squares on the bar's line show whole; once it moves they pass under.
  const [scrolled, setScrolled] = createSignal(false);
  // In the console the workspace menu (the rail's foot) holds the account,
  // and Deployments and Settings join the rail; elsewhere this is undefined.
  const cx = useConsoleChrome();

  // Pages only a scheduler can fill: an SSP serving the API alone has no
  // presence sampler, incident log, log ring or backups.
  const clusterOnly = new Set(['/views', '/backups', '/incidents', '/logs']);
  const links = (): NavLink[] => {
    if (props.nav) return props.nav.links;
    return projectNav(!!cx, props.overview, !!cx?.limited()).filter((l) => !servedByNode() || !clusterOnly.has(l.href));
  };

  const sections = () => sectionsOf(links());

  // ⌘K reaches past this router in the console: other projects, the
  // workspace's own pages, other workspaces.
  const paletteLinks = (): NavLink[] => {
    const w = cx?.workspace();
    if (!cx || !w) return links();
    const extra: NavLink[] = [];
    for (const p of w.projects) {
      if (p.id !== cx.project()?.id) extra.push({ href: `/${w.slug}/${p.slug}`, label: p.slug, group: 'Projects', icon: NAV_PATHS.project, go: true });
    }
    if (!props.nav) {
      for (const l of WORKSPACE_LINKS) extra.push({ href: `/${w.slug}/-${l.href === '/' ? '' : l.href}`, label: l.label, icon: l.icon, group: w.name, go: true });
    }
    for (const o of cx.workspaces()) {
      if (o.id !== w.id) extra.push({ href: `/${o.slug}/-`, label: o.name, group: 'Workspaces', icon: NAV_PATHS.workspace, go: true });
    }
    return [...links(), ...extra];
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

  // The router's exact match misses "/admin/" for "/admin"; compare the
  // root's own href, base included, with the trailing slash dropped.
  const rootPath = useResolvedPath(() => '/');
  const atRoot = () => location.pathname.replace(/\/$/, '') === (rootPath() ?? '').replace(/\/$/, '');
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
    // ⌘B folds the console rail to its icons, except while typing (an editor may want it).
    if (cx && (e.metaKey || e.ctrlKey) && !e.shiftKey && !e.altKey && e.key.toLowerCase() === 'b') {
      const t = e.target;
      if (t instanceof Element && t.closest('input, textarea, select, [contenteditable=""], [contenteditable="true"], .cm-editor')) return;
      e.preventDefault();
      setRailMin(!railMin());
    }
  };
  window.addEventListener('keydown', onKey);
  onCleanup(() => window.removeEventListener('keydown', onKey));

  return (
    <div class="shell" classList={{ settled: settled(), 'rail-min': !!cx && railMin() }}>
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
        {/* Where this is; the drawer holds the switchers. */}
        <Show when={cx}>
          <button class="mb-ctx" onClick={() => setOpen(true)}>
            <Show when={cx!.project()} fallback={<span class="cx-av">{(cx!.workspace()?.name ?? '?').slice(0, 2).toUpperCase()}</span>}>
              {(p) => <span class={`fx-sq ${projectTone(p())}`} />}
            </Show>
            <b>{cx!.project()?.slug ?? cx!.workspace()?.name ?? ''}</b>
          </button>
        </Show>
      </header>

      <Show when={open()}>
        <div class="scrim show" onClick={() => setOpen(false)} />
      </Show>

      <nav class="sidebar" classList={{ open: open(), 'cx-rail': !!cx }}>
        {/* Who is signed in. The menu holds what used to sit at the foot:
            access and signing out. In the console the account lives in the
            workspace menu, and the rail starts with the wordmark. */}
        <Show when={cx}>
          <ConsoleRail
            links={links()}
            workspaceLayer={!!props.nav}
            atRoot={atRoot}
            onJump={() => setPaletteOpen(true)}
          />
        </Show>
        <Show when={!cx}>
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
                <For each={section.links}>{(link) => <NavItem link={link} atRoot={atRoot} />}</For>
              </div>
            )}
          </For>
        </div>

        {/* The foot of the rail: the version, pinned to the bottom. */}
        <div class="sb-foot">
          <div class="sb-ver cap">
            {props.overview?.scheduler?.version?.replace(/^0\.0\.1-/, '') ?? currentMode().config?.version?.replace(/^0\.0\.1-/, '') ?? ''}
          </div>
        </div>
        </Show>
      </nav>

      <Show when={paletteOpen()}>
        <Palette links={paletteLinks()} onClose={() => setPaletteOpen(false)} />
      </Show>

      <div class="main-frame">
        <main class="main" classList={{ scrolled: scrolled() }} onScroll={(e) => setScrolled(e.currentTarget.scrollTop > 0)}>
          {props.children}
          <Show when={props.session.mode === 'breakglass'}>
            <div class="page-body">
              <div class="banner" style={{ 'margin-top': '16px' }}>
                <span class="dot warn" />
                {/* One element: the banner is a flex row, which would split the sentence. */}
                <span>Signed in with the break-glass password, so the <span class="dim mono">_00_admin</span> roster was bypassed.</span>
              </div>
            </div>
          </Show>
        </main>
      </div>
    </div>
  );
}

/**
 * A page while it loads: the frame's rows, shimmering, under the real head
 * when there is one. Every loading state draws this, so nothing around the
 * page moves when it arrives.
 */
export function PageSkeleton(props: { head?: JSX.Element }) {
  return (
    <>
      {props.head ?? (
        <div class="page-head">
          <div class="ph-crumb cap">
            <span class="sk sk-cap" style={{ width: '84px' }} />
            <span class="ph-sep">/</span>
            <span class="sk sk-cap" style={{ width: '64px' }} />
          </div>
        </div>
      )}
      <div class="fx-page sk-page" aria-busy="true" aria-label="Loading">
        <section class="fx-row sk-tiles">
          <For each={[0, 1, 2, 3]}>
            {(i) => (
              <div class="fx-cell" style={{ '--i': String(i) }}>
                <span class="sk sk-cap" />
                <span class="sk sk-big" />
                <span class="sk sk-line" />
              </div>
            )}
          </For>
        </section>
        <section class="fx-row">
          <div class="fx-cell">
            <span class="sk sk-cap" />
            <span class="sk sk-area" />
          </div>
        </section>
        <div class="skeleton-list">
          <For each={[0, 1, 2, 3, 4, 5]}>{(i) => <div class="skeleton-row" style={{ '--i': String(i) }} />}</For>
        </div>
      </div>
    </>
  );
}

/**
 * The whole frame before a router or a session exists: the rail with its
 * real pages (not clickable yet), the top bar, and a loading page. It paints
 * from the entry chunk, before the app's own code has loaded. In the console
 * the address says whether a workspace or a project is opening.
 */
export function FrameSkeleton(props: { console?: boolean }) {
  const seg = window.location.pathname.split('/').filter(Boolean);
  const workspace = props.console && seg.length > 0 && (seg.length === 1 || seg[1] === '-');
  const links = (): NavLink[] =>
    workspace ? WORKSPACE_LINKS : projectNav(!!props.console).filter((l) => l.group !== 'Settings');
  return (
    <div class="shell settled">
      <header class="mobile-bar">
        <span class="burger" aria-hidden="true"><span /><span /><span /></span>
        <Logo />
      </header>
      <nav class="sidebar" aria-hidden="true">
        <Show
          when={props.console}
          fallback={
            <div class="sb-me">
              <span class="sb-me-btn"><span class="sb-av sk" /><span class="sk sk-cap" style={{ width: '96px' }} /></span>
            </div>
          }
        >
          <div class="cx-brand"><Logo /><span class="cap">console</span></div>
          <div class="cx-rail-head"><span class="cx-prow"><span class="fx-sq idle" /><span class="sk sk-cap" style={{ width: '96px' }} /></span></div>
        </Show>
        <div class="sb-jump cap">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
            <path d="M10.5 4a6.5 6.5 0 1 0 0 13 6.5 6.5 0 0 0 0-13z M20 20l-4.5-4.5" />
          </svg>
          Jump to
          <kbd>⌘K</kbd>
        </div>
        <div class="nav">
          <For each={sectionsOf(links())}>
            {(section) => (
              <div class="nav-section">
                <Show when={section.group}>
                  <div class="nav-heading">{section.group}</div>
                </Show>
                <For each={section.links}>
                  {(link) => (
                    <span class="nav-link">
                      <span class="row" style={{ gap: '10px' }}><NavIcon href={link.href} icon={link.icon} /><span>{link.label}</span></span>
                    </span>
                  )}
                </For>
              </div>
            )}
          </For>
        </div>
        <div class="sb-foot" classList={{ 'cx-foot': props.console }}>
          <Show when={props.console}>
            <span class="cx-me">
              <span class="cx-face sk" />
              <span class="cx-wsw-text"><span class="sk sk-cap" style={{ width: '72px' }} /><span class="sk sk-cap" style={{ width: '52px' }} /></span>
            </span>
          </Show>
          <div class="sb-ver cap" />
        </div>
      </nav>
      <div class="main-frame">
        <main class="main">
          <PageSkeleton />
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
  const cx = useConsoleChrome();
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
    if (l.go && cx) cx.go(l.href);
    else navigate(l.href);
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
                <NavIcon href={l.href} icon={l.icon} />
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
