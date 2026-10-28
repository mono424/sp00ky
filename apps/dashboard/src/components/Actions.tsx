import {
  For,
  Show,
  createEffect,
  createSignal,
  createUniqueId,
  onCleanup,
  onMount,
} from 'solid-js';
import { Portal } from 'solid-js/web';
import { api, ApiError } from '../api/client';
import { formatDuration } from '../lib/format';
import type { OpKind, Operation } from '../api/types';

/**
 * The action layer: how an operator makes the scheduler DO something, and how
 * they find out what happened.
 *
 * Four pieces, each small on purpose:
 *
 *  - `ActionMenu`: a split button. The primary verb is one click; the menu
 *    behind the chevron lists every mode with its consequence on the same
 *    line, and an option that is not available here is shown muted with the
 *    reason rather than hidden. An operator on a self-hosted scheduler should
 *    learn that "Upgrade images" exists and why it is off, not wonder whether
 *    the feature exists at all.
 *  - `confirm()` + `ConfirmHost`: one modal for the whole app. Destructive
 *    modes ask for a typed word; the word is the consequence ("clean",
 *    "restore"), never "yes".
 *  - `toast()` + `ToastHost`: the server's own message, verbatim. The API
 *    answers errors as `{error}` with a sentence written for a human, and the
 *    toast is where that sentence is read.
 *  - `OpBadge`: a running operation next to the thing it targets. The full
 *    progress card for every running operation lives in `OpCard.tsx`.
 */

/* ------------------------------------------------------------------ */
/* Toasts                                                               */
/* ------------------------------------------------------------------ */

export type ToastTone = 'ok' | 'bad' | 'warn' | 'idle';

interface Toast {
  id: number;
  tone: ToastTone;
  title: string;
  body?: string;
}

const [toasts, setToasts] = createSignal<Toast[]>([]);
let toastSeq = 0;

/** Show a transient notice. Errors stay longer, because they get read. */
export function toast(tone: ToastTone, title: string, body?: string) {
  const id = ++toastSeq;
  setToasts((t) => [...t, { id, tone, title, body }]);
  const ttl = tone === 'bad' ? 9000 : 4500;
  setTimeout(() => dismissToast(id), ttl);
}

function dismissToast(id: number) {
  setToasts((t) => t.filter((x) => x.id !== id));
}

export function ToastHost() {
  return (
    <div class="toasts" aria-live="polite">
      <For each={toasts()}>
        {(t) => (
          <div class="toast" classList={{ [t.tone]: true }}>
            <span class="dot" classList={{ [t.tone]: true }} />
            <div class="toast-text">
              <div class="toast-title">{t.title}</div>
              <Show when={t.body}>
                <div class="toast-body">{t.body}</div>
              </Show>
            </div>
            <button
              class="toast-close"
              aria-label="Dismiss"
              onClick={() => dismissToast(t.id)}
            >
              ×
            </button>
          </div>
        )}
      </For>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Confirm dialog                                                       */
/* ------------------------------------------------------------------ */

export interface ConfirmSpec {
  title: string;
  /** What will happen, one line each. Read before the button is pressed. */
  consequences: string[];
  /** The verb on the primary button: "Restart", "Restore", "Wipe and restart". */
  verb: string;
  /** Require this word to be typed before the verb is enabled. */
  typeToConfirm?: string;
  /** A warning above the consequences, e.g. "not supervised". */
  warning?: string;
}

interface PendingConfirm extends ConfirmSpec {
  resolve: (ok: boolean) => void;
}

const [pending, setPending] = createSignal<PendingConfirm | null>(null);

/** Ask the operator. Resolves false on cancel, Escape, or scrim click. */
export function confirm(spec: ConfirmSpec): Promise<boolean> {
  // A second confirm while one is open is a programming error, not a queue:
  // refuse it rather than stack modals.
  if (pending()) return Promise.resolve(false);
  return new Promise((resolve) => setPending({ ...spec, resolve }));
}

export function ConfirmHost() {
  let dialog: HTMLDialogElement | undefined;
  let input: HTMLInputElement | undefined;
  const [typed, setTyped] = createSignal('');

  const close = (ok: boolean) => {
    const p = pending();
    if (!p) return;
    setPending(null);
    setTyped('');
    p.resolve(ok);
  };

  createEffect(() => {
    const p = pending();
    if (!dialog) return;
    if (p && !dialog.open) {
      dialog.showModal();
      // Focus the word field when there is one; otherwise the cancel button,
      // so a stray Enter cannot confirm a destructive action.
      queueMicrotask(() => input?.focus());
    } else if (!p && dialog.open) {
      dialog.close();
    }
  });

  const armed = () => {
    const p = pending();
    if (!p) return false;
    if (!p.typeToConfirm) return true;
    return typed().trim() === p.typeToConfirm;
  };

  return (
    <dialog
      ref={dialog}
      class="confirm"
      onCancel={(e) => {
        e.preventDefault();
        close(false);
      }}
      onClick={(e) => {
        // The backdrop is the dialog element itself; the card is its child.
        if (e.target === dialog) close(false);
      }}
    >
      <Show when={pending()}>
        {(p) => (
          <form
            class="confirm-card"
            method="dialog"
            onSubmit={(e) => {
              e.preventDefault();
              if (armed()) close(true);
            }}
          >
            <div class="tag">Confirm</div>
            <h2 class="confirm-title">{p().title}</h2>

            <Show when={p().warning}>
              <div class="confirm-warning">{p().warning}</div>
            </Show>

            <ul class="confirm-list">
              <For each={p().consequences}>{(c) => <li>{c}</li>}</For>
            </ul>

            <Show when={p().typeToConfirm}>
              <label class="confirm-type">
                <span class="tag">
                  Type <span class="val">{p().typeToConfirm}</span> to continue
                </span>
                <input
                  ref={input}
                  value={typed()}
                  onInput={(e) => setTyped(e.currentTarget.value)}
                  autocomplete="off"
                  spellcheck={false}
                />
              </label>
            </Show>

            <div class="confirm-actions">
              <button type="button" class="btn" onClick={() => close(false)}>
                Cancel
              </button>
              <button type="submit" class="btn btn-primary" disabled={!armed()}>
                {p().verb}
              </button>
            </div>
          </form>
        )}
      </Show>
    </dialog>
  );
}

/* ------------------------------------------------------------------ */
/* Running an action                                                    */
/* ------------------------------------------------------------------ */

export interface RunSpec<T> {
  /** Shown as the toast title on success. */
  label: string;
  confirm?: ConfirmSpec;
  request: () => Promise<T>;
  /** Success toast body, from the response. */
  success?: (value: T) => string | undefined;
  /** Runs after success: refetch, navigate, wait for a restart. */
  after?: (value: T) => void | Promise<void>;
}

/**
 * Confirm (if asked), call, report.
 *
 * Returns the response, or `undefined` when cancelled or failed. Failures are
 * reported here rather than thrown, because every caller would otherwise write
 * the same catch: the server's sentence, in a toast.
 */
export async function runAction<T>(spec: RunSpec<T>): Promise<T | undefined> {
  if (spec.confirm && !(await confirm(spec.confirm))) return undefined;
  try {
    const value = await spec.request();
    toast('ok', spec.label, spec.success?.(value));
    await spec.after?.(value);
    return value;
  } catch (err) {
    const message =
      err instanceof ApiError
        ? err.message
        : err instanceof Error
          ? err.message
          : 'Request failed';
    toast('bad', `${spec.label} failed`, message);
    return undefined;
  }
}

/** `runAction` for the common shape: a POST with a JSON body. */
export function post<T>(path: string, body?: unknown): () => Promise<T> {
  return () => api.post<T>(path, body);
}

/* ------------------------------------------------------------------ */
/* Action menu                                                          */
/* ------------------------------------------------------------------ */

export interface ActionEntry {
  id: string;
  title: string;
  /** One line: what this does, said plainly. */
  consequence: string;
  /** Present = shown muted with this reason, and not clickable. */
  disabledReason?: string;
  /** Draws the entry's title in the fault colour: the irreversible ones. */
  destructive?: boolean;
  onSelect: () => void;
}

/**
 * A split button.
 *
 * `primary` is the entry the left half runs. Without one, the whole control is
 * a single button that opens the menu. Below 860px the primary half is hidden
 * and the toggle reads "Actions"; the menu is the same.
 */
/**
 * Which menu is open, app-wide, by id. Kept outside the component because a
 * menu inside a polled table row is re-created on every poll (the row's data
 * object is new each time), and state that lives in the component dies with
 * it. Keyed state survives the remount; the new instance sees its own id and
 * reopens where it was.
 */
const [openMenuId, setOpenMenuId] = createSignal<string | null>(null);

export function ActionMenu(props: {
  entries: ActionEntry[];
  primary?: ActionEntry;
  /** Label for the toggle when there is no primary, and on mobile. */
  label?: string;
  size?: 'sm';
  /** Open the menu to the left of the toggle. Default right-aligned. */
  align?: 'left' | 'right';
  /** Stable identity across re-renders; defaults to one per instance. */
  menuId?: string;
}) {
  const fallbackId = createUniqueId();
  const myId = () => props.menuId ?? fallbackId;
  const open = () => openMenuId() === myId();
  const setOpen = (v: boolean) => setOpenMenuId(v ? myId() : openMenuId() === myId() ? null : openMenuId());
  // Viewport coordinates for the menu, computed when it opens. Null means
  // "let the stylesheet place it", which is the bottom-sheet rule on phones.
  const [place, setPlace] = createSignal<Record<string, string> | null>(null);
  let root: HTMLDivElement | undefined;
  let toggle: HTMLButtonElement | undefined;
  let menuEl: HTMLDivElement | undefined;

  /**
   * The menu lives in a portal on `document.body` and is positioned from the
   * toggle's rectangle. It used to be an absolutely positioned child of the
   * split button, which was fine everywhere except inside a scrolling table
   * (`.table-scroll` has overflow: auto), where the panel clipped it and the
   * operator saw two entries and a cut edge. A fixed-position menu is clipped
   * by nothing.
   */
  const measure = () => {
    if (!toggle) return;
    if (window.matchMedia('(max-width: 560px)').matches) {
      setPlace(null);
      return;
    }
    const r = toggle.getBoundingClientRect();
    const gap = 4;
    const coords: Record<string, string> = {};
    if (props.align === 'left') coords.left = `${Math.round(r.left)}px`;
    else coords.right = `${Math.round(window.innerWidth - r.right)}px`;
    // Flip above the toggle when the menu would run past the bottom edge.
    // The height is known only once rendered, so this runs after mount too.
    const height = menuEl?.offsetHeight ?? 0;
    const below = r.bottom + gap;
    if (height > 0 && below + height > window.innerHeight - 8 && r.top - gap - height > 8) {
      coords.top = `${Math.round(r.top - gap - height)}px`;
    } else {
      coords.top = `${Math.round(below)}px`;
    }
    setPlace(coords);
  };

  const openMenu = () => {
    measure();
    setOpen(true);
    // Second pass with the real height, for the flip decision.
    requestAnimationFrame(measure);
  };

  onMount(() => {
    // Remounted while open (a polled row re-created around us): measure
    // against the new toggle so the menu lands where it was.
    if (open()) requestAnimationFrame(measure);

    const onDoc = (e: MouseEvent) => {
      if (!open()) return;
      const t = e.target as Node;
      if (root?.contains(t) || menuEl?.contains(t)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    // A menu pinned to viewport coordinates must follow the layout it was
    // measured against: on scroll or resize it is re-measured, and closed
    // only once its toggle has left the viewport entirely.
    const onMove = () => {
      if (!open() || !toggle) return;
      const r = toggle.getBoundingClientRect();
      if (r.bottom < 0 || r.top > window.innerHeight) setOpen(false);
      else measure();
    };
    document.addEventListener('mousedown', onDoc);
    document.addEventListener('keydown', onKey);
    window.addEventListener('scroll', onMove, true);
    window.addEventListener('resize', onMove);
    onCleanup(() => {
      document.removeEventListener('mousedown', onDoc);
      document.removeEventListener('keydown', onKey);
      window.removeEventListener('scroll', onMove, true);
      window.removeEventListener('resize', onMove);
    });
  });

  const pick = (entry: ActionEntry) => {
    if (entry.disabledReason) return;
    setOpen(false);
    entry.onSelect();
  };

  return (
    <div
      ref={root}
      class="split"
      classList={{
        'split-sm': props.size === 'sm',
        'split-solo': !props.primary,
        open: open(),
      }}
      onClick={(e) => e.stopPropagation()}
    >
      <Show when={props.primary}>
        {(p) => (
          <button
            class="btn split-primary"
            classList={{ 'btn-sm': props.size === 'sm' }}
            disabled={!!p().disabledReason}
            title={p().disabledReason ?? p().consequence}
            onClick={() => pick(p())}
          >
            {p().title}
          </button>
        )}
      </Show>
      <button
        ref={toggle}
        class="btn split-toggle"
        classList={{ 'btn-sm': props.size === 'sm' }}
        aria-haspopup="menu"
        aria-expanded={open()}
        onClick={() => (open() ? setOpen(false) : openMenu())}
      >
        <span class="split-label">{props.label ?? 'Actions'}</span>
        <span class="chev" aria-hidden="true" />
      </button>

      <Show when={open()}>
        <Portal>
          <div
            ref={menuEl}
            class="menu menu-fixed"
            classList={{ 'menu-left': props.align === 'left' }}
            style={place() ?? undefined}
            role="menu"
            onClick={(e) => e.stopPropagation()}
          >
            <For each={props.entries}>
              {(entry) => (
                <button
                  role="menuitem"
                  class="menu-item"
                  classList={{
                    disabled: !!entry.disabledReason,
                    destructive: !!entry.destructive,
                  }}
                  aria-disabled={!!entry.disabledReason}
                  onClick={() => pick(entry)}
                >
                  <span class="menu-title">{entry.title}</span>
                  <span class="menu-sub">
                    {entry.disabledReason ?? entry.consequence}
                  </span>
                </button>
              )}
            </For>
          </div>
        </Portal>
      </Show>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Activity strip                                                       */
/* ------------------------------------------------------------------ */

export const OP_LABELS: Record<OpKind, string> = {
  ssp_restart: 'Restarting SSP',
  ssp_clean: 'Clean-restarting SSP',
  ssp_reload: 'Reloading SSP',
  rolling_restart: 'Rolling restart',
  scheduler_restart: 'Restarting scheduler',
  reclone: 'Recloning replica',
  rehash: 'Rehashing snapshot',
  cloud_restart: 'Cloud restart',
  backup_create: 'Backing up',
  backup_restore: 'Restoring',
};

export function opLabel(kind: string): string {
  return OP_LABELS[kind as OpKind] ?? kind;
}

/** A ticking clock signal so elapsed readouts move without a refetch. */
function useNow(intervalMs = 1000) {
  const [now, setNow] = createSignal(Date.now());
  const t = setInterval(() => setNow(Date.now()), intervalMs);
  onCleanup(() => clearInterval(t));
  return now;
}

/** A finished-or-not badge for one target, e.g. next to an SSP's name. */
export function OpBadge(props: { operations: Operation[] | undefined; target: string }) {
  const now = useNow();
  const op = () =>
    (props.operations ?? []).find(
      (o) => o.status === 'running' && o.target === props.target,
    );
  return (
    <Show when={op()}>
      {(o) => (
        <span class="pill warn" title={o().message ?? undefined}>
          <span class="dot warn pulse" />
          {opLabel(o().kind).toLowerCase()} ·{' '}
          {formatDuration(Math.max(0, now() - o().started_at))}
        </span>
      )}
    </Show>
  );
}
