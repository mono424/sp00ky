/**
 * The console's chrome: which workspace and project the page is in, the
 * project switcher (the rail's head) and the workspace menu (the rail's foot,
 * which is also the account's). Shared through a context so the scheduler's
 * own components (Shell, PageHead) can show it without knowing about Sp00ky
 * Cloud; outside the console the context is empty and they render as they
 * always did.
 */

import { For, Show, createContext, createMemo, createSignal, onCleanup, useContext, type Accessor } from 'solid-js';
import { useNavigate, useResolvedPath } from '@solidjs/router';
import { formatDuration, formatStamp } from '../lib/format';
import type { Me, Workspace, WorkspaceProject } from './cloud';

export interface ConsoleChrome {
  me: Accessor<Me | undefined>;
  workspaces: Accessor<Workspace[]>;
  workspace: Accessor<Workspace | undefined>;
  project: Accessor<WorkspaceProject | undefined>;
  /** Only the cloud's pages: the project's scheduler is not reachable or too old. */
  limited: Accessor<boolean>;
  /** Go to a console path (`/acme/whitepawn/deployments`). */
  go: (path: string) => void;
  /** Load the workspaces again, after one of them changed. */
  refresh: () => Promise<unknown>;
  signOut: () => void;
}

const Ctx = createContext<ConsoleChrome | undefined>(undefined);
export const ConsoleChromeProvider = Ctx.Provider;

/** The console's chrome, or undefined outside the console. */
export function useConsoleChrome(): ConsoleChrome | undefined {
  return useContext(Ctx);
}

/**
 * Go to a console path from inside a page's router. A path under this router
 * goes through it (a bare `history.pushState` would change the address but
 * not the page); anything else, another project or workspace, through the
 * console.
 */
export function useGo(): (to: string) => void {
  const c = useConsoleChrome()!;
  const navigate = useNavigate();
  const root = useResolvedPath(() => '/');
  return (to) => {
    const base = (root() ?? '').replace(/\/$/, '');
    if (base && (to === base || to.startsWith(`${base}/`))) navigate(to.slice(base.length) || '/');
    else c.go(to);
  };
}

export function initials(text: string | undefined | null): string {
  const words = (text ?? '').trim().split(/[\s._@-]+/).filter(Boolean);
  if (words.length === 0) return '?';
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return (words[0][0] + words[1][0]).toUpperCase();
}

/** A project's state as one square: ok, warn (degraded, rolling out) or idle (stopped). */
export function projectTone(p: WorkspaceProject): string {
  if (p.status !== 'active') return 'idle';
  switch (p.deployment_status) {
    case 'running':
      return 'ok';
    case 'degraded':
    case 'failed':
      return 'warn';
    case null:
    case undefined:
      return 'idle';
    default:
      return 'accent';
  }
}

export function projectLine(p: WorkspaceProject): string {
  if (p.status !== 'active') return p.status;
  const v = p.deployment_version != null ? `v${p.deployment_version}` : 'not deployed';
  switch (p.deployment_status) {
    case 'running':
      return `${v} live`;
    case null:
    case undefined:
      return 'not deployed';
    default:
      return `${v} ${p.deployment_status.replace('_', ' ')}`;
  }
}

/** Close a popover on an outside click or Escape. */
function usePopover(selector: string): [Accessor<boolean>, (v: boolean) => void] {
  const [open, setOpen] = createSignal(false);
  const onClick = (e: MouseEvent) => {
    if (open() && !(e.target as Element).closest(selector)) setOpen(false);
  };
  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'Escape') setOpen(false);
  };
  document.addEventListener('click', onClick);
  window.addEventListener('keydown', onKey);
  onCleanup(() => {
    document.removeEventListener('click', onClick);
    window.removeEventListener('keydown', onKey);
  });
  return [open, setOpen];
}

const caret = (up: boolean) => (
  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
    <path d={up ? 'M7 14l5-5 5 5' : 'M7 10l5 5 5-5'} />
  </svg>
);

export const GEAR =
  'M12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6z M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z';

/** A workspace's own pages in the rail; its projects sit between the two groups. */
export const WORKSPACE_LINKS: { href: string; label: string; icon: string; group?: string }[] = [
  { href: '/', label: 'Overview', icon: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z' },
  { href: '/activity', label: 'Activity', icon: 'M3 12h4l3-8 4 16 3-8h4' },
  { href: '/members', label: 'Members', group: 'Workspace', icon: 'M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z M2 21v-1a7 7 0 0 1 14 0v1 M16 3.5a4 4 0 0 1 0 7.5 M22 21v-1a7 7 0 0 0-4-6.3' },
  { href: '/billing', label: 'Billing & usage', group: 'Workspace', icon: 'M3 6h18v12H3z M3 10h18 M7 15h4' },
  { href: '/vault', label: 'Vault', group: 'Workspace', icon: 'M5 10h14v11H5z M8 10V6a4 4 0 0 1 8 0v4' },
  { href: '/keys', label: 'API keys', group: 'Workspace', icon: 'M14 4a6 6 0 1 0 3 11.2L20 18v3h-3v-2h-2v-2l-1.2-1.2A6 6 0 0 0 14 4z' },
  { href: '/settings', label: 'Settings', group: 'Workspace', icon: GEAR },
];

/**
 * The project switcher: a button that opens every project in the workspace.
 * ⌘P. In the rail it heads both layers; on a workspace page it offers the
 * projects without one being open.
 */
export function ProjectSwitcher(props: { rail?: boolean }) {
  const c = useConsoleChrome()!;
  const go = useGo();
  const [open, setOpen] = usePopover('.cx-proj-wrap');
  const [q, setQ] = createSignal('');
  const onKey = (e: KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'p') {
      e.preventDefault();
      setOpen(!open());
    }
  };
  window.addEventListener('keydown', onKey);
  onCleanup(() => window.removeEventListener('keydown', onKey));

  const projects = createMemo(() => {
    const needle = q().trim().toLowerCase();
    const all = c.workspace()?.projects ?? [];
    return needle ? all.filter((p) => p.slug.toLowerCase().includes(needle)) : all;
  });

  return (
    <span class="cx-proj-wrap" classList={{ 'in-rail': !!props.rail }}>
      <Show
        when={props.rail}
        fallback={
          <button class="cx-proj" classList={{ on: open() }} onClick={() => setOpen(!open())} aria-haspopup="menu" aria-expanded={open()}>
            <Show when={c.project()} fallback={<b>{c.workspace()?.name ?? 'sp00ky'}</b>}>
              {(p) => (
                <>
                  <span class={`fx-sq ${projectTone(p())}`} />
                  <b>{p().slug}</b>
                </>
              )}
            </Show>
            {caret(open())}
          </button>
        }
      >
        <button class="cx-prow" classList={{ on: open() }} onClick={() => setOpen(!open())} aria-haspopup="menu" aria-expanded={open()} title="Switch project (⌘P)">
          <Show
            when={c.project()}
            fallback={
              <svg class="cx-prow-ic" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
                <path d="M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z" />
              </svg>
            }
          >
            {(p) => <span class={`fx-sq ${projectTone(p())}`} />}
          </Show>
          <span class="cx-prow-text">
            <b classList={{ 'cx-prow-none': !c.project() }}>{c.project()?.slug ?? 'Open a project'}</b>
            <small>{c.project() ? projectLine(c.project()!) : `${c.workspace()?.projects.length ?? 0} in ${c.workspace()?.name ?? 'this workspace'}`}</small>
          </span>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
            <path d="M8 9l4-4 4 4 M8 15l4 4 4-4" />
          </svg>
        </button>
      </Show>
      <Show when={open()}>
        <div class="cx-pop" role="menu">
          <label class="cx-find cap">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
              <path d="M10.5 4a6.5 6.5 0 1 0 0 13 6.5 6.5 0 0 0 0-13z M20 20l-4.5-4.5" />
            </svg>
            <input placeholder="Find a project" value={q()} onInput={(e) => setQ(e.currentTarget.value)} autofocus />
            <kbd>⌘P</kbd>
          </label>
          <div class="cx-grp">
            <div class="cap cx-h">
              {c.workspace()?.name} · {c.workspace()?.projects.length ?? 0} projects
            </div>
            <For each={projects()} fallback={<div class="cx-empty cap">No project matches</div>}>
              {(p) => (
                <button
                  class="cx-pi"
                  classList={{ on: p.id === c.project()?.id }}
                  role="menuitem"
                  onClick={() => {
                    setOpen(false);
                    go(`/${c.workspace()!.slug}/${p.slug}`);
                  }}
                >
                  <span class={`fx-sq ${projectTone(p)}`} />
                  <span class="cx-pi-text">
                    <b>{p.slug}</b>
                    <small>{projectLine(p)}</small>
                  </span>
                  <span class="cx-tag" classList={{ cf: p.platform === 'cloudflare' }}>
                    {p.platform === 'cloudflare' ? 'CF' : 'VM'}
                  </span>
                </button>
              )}
            </For>
          </div>
          <div class="cx-pfoot cap">
            <button onClick={() => { setOpen(false); go(`/${c.workspace()!.slug}/-`); }}>All projects</button>
            <span>⌘K jumps anywhere</span>
          </div>
        </div>
      </Show>
    </span>
  );
}

/** The signed-in person's GitHub picture over their initials; the initials stay if it cannot load. */
function UserAvatar(props: { small?: boolean }) {
  const c = useConsoleChrome()!;
  const login = () => c.me()?.account.github_login;
  const [failed, setFailed] = createSignal(false);
  const who = () => login() ?? c.me()?.account.email ?? '';
  return (
    <span class="cx-uav" classList={{ sm: !!props.small }} aria-hidden="true">
      {initials(who())}
      <Show when={login() && !failed()}>
        <img src={`https://avatars.githubusercontent.com/${encodeURIComponent(login()!)}?s=64`} alt="" loading="lazy" onError={() => setFailed(true)} />
      </Show>
    </span>
  );
}

/**
 * The rail's foot: who is signed in and which workspace they are in, as one
 * menu. It holds the account, every workspace (each with a way to its
 * settings), the account's sessions and signing out. ⌘⇧W opens it; arrows
 * move through the workspaces, Enter opens one.
 */
export function WorkspaceMenu() {
  const c = useConsoleChrome()!;
  const [open, setOpen] = usePopover('.cx-who-wrap');
  const [q, setQ] = createSignal('');
  const [hot, setHot] = createSignal(0);
  const account = () => c.me()?.account;
  const who = () => account()?.github_login ?? account()?.email ?? 'Account';
  const ws = () => c.workspace();
  const list = createMemo(() => {
    const needle = q().trim().toLowerCase();
    const all = c.workspaces();
    return needle ? all.filter((w) => `${w.name} ${w.slug}`.toLowerCase().includes(needle)) : all;
  });
  const toggle = (v = !open()) => {
    if (v) {
      setQ('');
      setHot(Math.max(0, c.workspaces().findIndex((w) => w.id === ws()?.id)));
    }
    setOpen(v);
  };
  const go = useGo();
  const leave = (to: string) => {
    setOpen(false);
    go(to);
  };
  const onGlobalKey = (e: KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key.toLowerCase() === 'w') {
      e.preventDefault();
      toggle();
    }
  };
  window.addEventListener('keydown', onGlobalKey);
  onCleanup(() => window.removeEventListener('keydown', onGlobalKey));

  const onKey = (e: KeyboardEvent) => {
    const n = list().length;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setHot((i) => Math.min(i + 1, n - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHot((i) => Math.max(i - 1, 0));
    } else if (e.key === 'Enter' && list()[hot()]) {
      e.preventDefault();
      leave(`/${list()[hot()].slug}/-`);
    }
  };
  const kind = (w: Workspace) => (w.type === 'personal' ? 'Personal' : 'Team');
  const count = (n: number) => `${n} ${n === 1 ? 'project' : 'projects'}`;

  return (
    <div class="cx-who-wrap">
      <Show when={open()}>
        <div
          class="cx-menu cx-who-menu"
          role="menu"
          tabIndex={-1}
          onKeyDown={onKey}
          ref={(el) => queueMicrotask(() => (el.querySelector<HTMLInputElement>('.cx-find input') ?? el).focus())}
        >
          <div class="cx-who-acct">
            <UserAvatar />
            <span class="cx-mi-text">
              <b>{who()}</b>
              <small>{account()?.github_login && account()?.email ? account()!.email : 'Signed in with GitHub'}</small>
            </span>
          </div>
          <div class="cx-who-sec">
            <div class="cap cx-h"><span>Workspaces</span><kbd>⌘⇧W</kbd></div>
            <Show when={c.workspaces().length > 5}>
              <label class="cx-find cap">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
                  <path d="M10.5 4a6.5 6.5 0 1 0 0 13 6.5 6.5 0 0 0 0-13z M20 20l-4.5-4.5" />
                </svg>
                <input placeholder="Find a workspace" value={q()} onInput={(e) => { setQ(e.currentTarget.value); setHot(0); }} />
              </label>
            </Show>
            <div class="cx-who-list">
              <For each={list()} fallback={<div class="cx-empty cap">No workspace matches</div>}>
                {(w, i) => (
                  <div class="cx-wrow" classList={{ on: w.id === ws()?.id, hot: i() === hot() }} onMouseEnter={() => setHot(i())}>
                    <button class="cx-wrow-main" role="menuitemradio" aria-checked={w.id === ws()?.id} onClick={() => leave(`/${w.slug}/-`)}>
                      <span class="cx-av2">{initials(w.name)}</span>
                      <span class="cx-mi-text">
                        <b>{w.name}</b>
                        <small>{kind(w)} · {count(w.projects.length)}{w.role === 'admin' ? ' · admin' : ''}</small>
                      </span>
                      <Show when={w.id === ws()?.id}>
                        <svg class="cx-check" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-label="Current workspace">
                          <path d="M5 12.5l4.5 4.5L19 7.5" />
                        </svg>
                      </Show>
                    </button>
                    <button class="cx-wrow-gear" title={`${w.name} settings`} aria-label={`${w.name} settings`} onClick={() => leave(`/${w.slug}/-/settings`)}>
                      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                        <path d={GEAR} />
                      </svg>
                    </button>
                  </div>
                )}
              </For>
            </div>
          </div>
          <div class="cx-who-foot">
            <button class="cx-mi" role="menuitem" onClick={() => leave(`/${ws()?.slug ?? c.workspaces()[0]?.slug ?? '-'}/-/sessions`)}>
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M3 5h18v11H3z M8 20h8 M12 16v4" /></svg>
              Sessions
            </button>
            <button class="cx-mi" role="menuitem" onClick={() => { setOpen(false); c.signOut(); }}>
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M14 4h5v16h-5 M10 8l-4 4 4 4 M6 12h10" /></svg>
              Sign out
            </button>
          </div>
        </div>
      </Show>
      <button class="cx-who" classList={{ on: open() }} onClick={() => toggle()} aria-haspopup="menu" aria-expanded={open()} title="Account and workspaces (⌘⇧W)">
        <span class="cx-av">{initials(ws()?.name)}</span>
        <span class="cx-wsw-text">
          <b>{ws()?.name ?? 'Workspace'}</b>
          <small class="cx-who-user"><UserAvatar small /><span>{who()}</span></small>
        </span>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
          <path d="M8 9l4-4 4 4 M8 15l4 4 4-4" />
        </svg>
      </button>
    </div>
  );
}

/** How long ago, as `3.2h ago`; `—` for no time. */
export function ago(iso: string | null | undefined): string {
  if (!iso) return '—';
  const ms = Date.now() - Date.parse(iso);
  if (!Number.isFinite(ms)) return '—';
  if (ms < 0) return `in ${formatDuration(-ms)}`;
  return ms < 60_000 ? 'just now' : `${formatDuration(ms)} ago`;
}

/** A time in a list: the full stamp, and on a phone how long ago. */
export function Stamp(props: { at: string | null | undefined }) {
  return (
    <span title={props.at ? formatStamp(props.at) : undefined}>
      <span class="cx-abs">{props.at ? formatStamp(props.at) : '—'}</span>
      <span class="cx-rel">{ago(props.at)}</span>
    </span>
  );
}
