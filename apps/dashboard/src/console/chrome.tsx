/**
 * The console's chrome: which workspace and project the page is in, the
 * project switcher (the breadcrumb, top-left) and the workspace menu
 * (top-right). Shared through a context so the scheduler's own components
 * (Shell, PageHead) can show it without knowing about Sp00ky Cloud; outside
 * the console the context is empty and they render as they always did.
 */

import { For, Show, createContext, createMemo, createSignal, onCleanup, useContext, type Accessor } from 'solid-js';
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
  signOut: () => void;
}

const Ctx = createContext<ConsoleChrome | undefined>(undefined);
export const ConsoleChromeProvider = Ctx.Provider;

/** The console's chrome, or undefined outside the console. */
export function useConsoleChrome(): ConsoleChrome | undefined {
  return useContext(Ctx);
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

/** A workspace's own rail in the console. */
export const WORKSPACE_LINKS: { href: string; label: string; icon: string }[] = [
  { href: '/', label: 'Projects', icon: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z' },
  { href: '/members', label: 'Members', icon: 'M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z M2 21v-1a7 7 0 0 1 14 0v1 M16 3.5a4 4 0 0 1 0 7.5 M22 21v-1a7 7 0 0 0-4-6.3' },
  { href: '/billing', label: 'Billing & usage', icon: 'M3 6h18v12H3z M3 10h18 M7 15h4' },
  { href: '/vault', label: 'Vault', icon: 'M5 10h14v11H5z M8 10V6a4 4 0 0 1 8 0v4' },
  { href: '/keys', label: 'API keys', icon: 'M14 4a6 6 0 1 0 3 11.2L20 18v3h-3v-2h-2v-2l-1.2-1.2A6 6 0 0 0 14 4z' },
  { href: '/activity', label: 'Activity', icon: 'M3 12h4l3-8 4 16 3-8h4' },
  { href: '/sessions', label: 'Sessions', icon: 'M3 5h18v11H3z M8 20h8 M12 16v4' },
];

/** The breadcrumb's project: a button that opens every project in the workspace. ⌘P. */
export function ProjectSwitcher() {
  const c = useConsoleChrome()!;
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
    <span class="cx-proj-wrap">
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
                    c.go(`/${c.workspace()!.slug}/${p.slug}`);
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
            <button onClick={() => { setOpen(false); c.go(`/${c.workspace()!.slug}/-`); }}>All projects</button>
            <span>Other workspaces: top right</span>
          </div>
        </div>
      </Show>
    </span>
  );
}

/** The workspace selector (top-right): workspaces, this workspace's pages, the account. ⌘⇧W. */
export function WorkspaceMenu(props: { shortcut?: boolean }) {
  const c = useConsoleChrome()!;
  const [open, setOpen] = usePopover('.cx-ws-wrap');
  const onKey = (e: KeyboardEvent) => {
    if (props.shortcut === false) return;
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key.toLowerCase() === 'w') {
      e.preventDefault();
      setOpen(!open());
    }
  };
  window.addEventListener('keydown', onKey);
  onCleanup(() => window.removeEventListener('keydown', onKey));

  const go = (path: string) => {
    setOpen(false);
    c.go(path);
  };
  const ws = () => c.workspace();
  const account = () => c.me()?.account;

  return (
    <span class="cx-ws-wrap">
      <button class="cx-ws" classList={{ on: open() }} onClick={() => setOpen(!open())} aria-haspopup="menu" aria-expanded={open()}>
        <span class="cx-av">{initials(ws()?.name)}</span>
        <b>{ws()?.name ?? 'Workspace'}</b>
        {caret(open())}
      </button>
      <Show when={open()}>
        <div class="cx-menu" role="menu">
          <div class="cx-sec">
            <div class="cap cx-h"><span>Workspaces</span><span>⌘⇧W</span></div>
            <For each={c.workspaces()}>
              {(w) => (
                <button class="cx-mi" classList={{ on: w.id === ws()?.id }} role="menuitem" onClick={() => go(`/${w.slug}`)}>
                  <span class="cx-av2">{initials(w.name)}</span>
                  <span class="cx-mi-text">
                    {w.name}
                    <small>{w.type === 'personal' ? 'Personal' : 'Organization'} · {w.projects.length} projects</small>
                  </span>
                  <span class="cx-r" classList={{ acc: w.id === ws()?.id }}>{w.role}</span>
                  <Show when={w.id === ws()?.id}><span class="cx-check">✓</span></Show>
                </button>
              )}
            </For>
          </div>
          <Show when={ws()}>
            {(w) => (
              <div class="cx-sec">
                <div class="cap cx-h"><span>{w().name}</span></div>
                <button class="cx-mi" role="menuitem" onClick={() => go(`/${w().slug}/-`)}>Projects<span class="cx-r">{w().projects.length}</span></button>
                <button class="cx-mi" role="menuitem" onClick={() => go(`/${w().slug}/-/members`)}>Members</button>
                <button class="cx-mi" role="menuitem" onClick={() => go(`/${w().slug}/-/billing`)}>Billing &amp; usage</button>
                <button class="cx-mi" role="menuitem" onClick={() => go(`/${w().slug}/-/vault`)}>Vault</button>
                <button class="cx-mi" role="menuitem" onClick={() => go(`/${w().slug}/-/keys`)}>API keys &amp; MCP tokens</button>
                <button class="cx-mi" role="menuitem" onClick={() => go(`/${w().slug}/-/activity`)}>Activity</button>
              </div>
            )}
          </Show>
          <div class="cx-sec">
            <div class="cap cx-h">
              <span>{account()?.email ?? account()?.github_login ?? ''}</span>
              <span>GitHub</span>
            </div>
            <button class="cx-mi" role="menuitem" onClick={() => go(`/${ws()?.slug ?? '-'}/-/sessions`)}>Sessions</button>
            <button class="cx-mi" role="menuitem" onClick={() => { setOpen(false); c.signOut(); }}>Sign out</button>
          </div>
        </div>
      </Show>
    </span>
  );
}
