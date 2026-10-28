import {
  For,
  Show,
  createMemo,
  createSignal,
  onCleanup,
  type JSX,
} from 'solid-js';
import { A } from '@solidjs/router';

/**
 * A trace waterfall: a tree of work on the left, the same rows as bars on one
 * shared time axis on the right.
 *
 *   Steps                          0s     10s     20s   ·now
 *   ▾ search_job   2 pending        ▬▬ ▬  ▬▬▬▬
 *       /jobs/reindex  ssp-0           ▬▬▬▬▬▬▬▬▬▬░░    12s
 *       /jobs/reindex                       ╱╱╱╱╱╱     waiting
 *
 * Used by a workflow run (steps, with dependency hand-offs drawn as elbows)
 * and by the jobs list (jobs grouped by outbox table on a rolling window).
 *
 * Rules worth keeping:
 *  - One axis for every row, so lengths compare honestly.
 *  - A row that is waiting is hatched, never drawn as work.
 *  - A running row has no end: it reaches the now line and keeps growing.
 *  - A failed row is red and carries a cross; nothing else is red.
 *  - Live traces move by re-rendering `now` once a second with a one-second
 *    linear transition, so motion is continuous but the DOM is touched once a
 *    second, not sixty times.
 */

export type TraceState = 'done' | 'running' | 'waiting' | 'failed' | 'skipped';

export interface TraceRow {
  id: string;
  label: string;
  /** Quiet text after the label: the model in the reference, our backend. */
  sub?: string;
  depth?: number;
  /** A collapsible header row: draws its children's spans on one line. */
  group?: boolean;
  /** For rows inside a group, the group's id. */
  parent?: string;
  /** Categorical colour of the bar (a CSS colour). */
  color?: string;
  /** One or two letters in the row badge. */
  badge?: string;
  state: TraceState;
  /** Epoch ms the work started; null = has not started. */
  start: number | null;
  /** Epoch ms it ended; null while running. */
  end: number | null;
  /** Epoch ms it was queued: hatched from here to `start` (or to now). */
  queued?: number | null;
  /** Start was inferred, not recorded: the bar gets an open left edge. */
  estimated?: boolean;
  /** Right of the bar: "12s · 2 attempts". */
  meta?: string;
  /** Ids of rows this one waited for; drawn as hand-off elbows. */
  after?: string[];
  href?: string;
  /** Expanded under the row on click. */
  detail?: () => JSX.Element;
  /** Small controls at the end of the row, shown on hover. */
  actions?: () => JSX.Element;
}

export interface TraceStat {
  label: string;
  value: JSX.Element;
  tone?: 'warn' | 'bad';
}

/** A muted categorical palette: hues for telling rows apart, not status. */
const PALETTE = ['#6b8fd6', '#9b84d8', '#d9925a', '#4fa89b', '#c97aa6', '#8fa564', '#c9a54f', '#6aa6c9'];

/** A stable colour per name, so the same table or step keeps its hue. */
export function hueOf(name: string): string {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) | 0;
  return PALETTE[Math.abs(h) % PALETTE.length];
}

/** `0s`, `850ms`, `14s`, `3m 05s`, `2h 10m`: short, for bars and ticks. */
export function short(ms: number): string {
  if (ms <= 0) return '0s';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const s = ms / 1000;
  if (s < 10) return `${s.toFixed(1)}s`;
  if (s < 60) return `${Math.round(s)}s`;
  const m = Math.floor(s / 60);
  const rs = Math.round(s % 60);
  if (m < 60) return rs ? `${m}m ${String(rs).padStart(2, '0')}s` : `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 48) return m % 60 ? `${h}h ${String(m % 60).padStart(2, '0')}m` : `${h}h`;
  return `${Math.floor(h / 24)}d`;
}

const ROW_H = 34;

export interface TraceWindow {
  key: string;
  label: string;
  ms: number;
}

export function Trace(props: {
  title: string;
  stats?: TraceStat[];
  rows: TraceRow[];
  /**
   * Fixed bounds (a finished run). Omit for a rolling window ending at now,
   * sized by `windows`.
   */
  from?: number | null;
  to?: number | null;
  /** True while anything can still move: the now line shows and ticks. */
  live?: boolean;
  /** The clock to call now; defaults to Date.now. Lets a list follow the server's clock. */
  clock?: () => number;
  /** Rolling-window choices; the first is the default. */
  windows?: TraceWindow[];
  /** Right side of the header: search, filters. */
  tools?: JSX.Element;
  /** Heading over the tree column. */
  treeLabel?: string;
  /** Shown instead of rows when there are none. */
  empty?: JSX.Element;
  /** Legend entries to show; default all. */
  legend?: ('run' | 'wait' | 'handoff' | 'failed' | 'skipped')[];
}) {
  const clock = () => (props.clock ?? Date.now)();

  // ---- the clock: a signal ticking once a second while live and not paused
  const [paused, setPaused] = createSignal(false);
  const [now, setNow] = createSignal(clock());
  const tick = setInterval(() => {
    if (props.live && !paused()) setNow(clock());
  }, 1000);
  onCleanup(() => clearInterval(tick));

  const [windowKey, setWindowKey] = createSignal<string | null>(null);
  const win = () => {
    const ws = props.windows ?? [];
    return ws.find((w) => w.key === windowKey()) ?? ws[0];
  };

  // ---- the axis
  const span = createMemo(() => {
    const t = props.live ? now() : clock();
    if (props.from != null) {
      const from = props.from;
      const ends = props.rows
        .map((r) => r.end ?? (r.start != null ? t : null))
        .filter((v): v is number => v != null);
      const rawTo = props.to ?? Math.max(t, ...ends, from + 1000);
      // While live, leave a little room after now so the line is visible.
      const to = props.to == null ? rawTo + (rawTo - from) * 0.08 : rawTo;
      return { from, to, total: Math.max(1, to - from) };
    }
    const w = win()?.ms ?? 5 * 60_000;
    const to = t + w * 0.06;
    const from = to - w;
    return { from, to, total: w };
  });

  const x = (t: number) => ((t - span().from) / span().total) * 100;
  const clampPct = (v: number) => Math.min(100, Math.max(0, v));

  const ticks = createMemo(() => {
    const { from, total } = span();
    const target = total / 4;
    const steps = [1e3, 2e3, 5e3, 1e4, 15e3, 3e4, 6e4, 12e4, 3e5, 6e5, 9e5, 18e5, 36e5, 72e5, 216e5, 432e5, 864e5];
    const step = steps.find((s) => s >= target) ?? target;
    const out: { at: number; label: string }[] = [];
    // Fixed bounds count up from the run's start; a rolling window counts
    // back from now.
    if (props.from != null) {
      for (let v = 0; v <= total; v += step) out.push({ at: (v / total) * 100, label: short(v) });
    } else {
      const t = props.live ? now() : clock();
      for (let v = 0; t - v >= from; v += step) {
        out.push({ at: x(t - v), label: v === 0 ? 'now' : `-${short(v)}` });
      }
    }
    return out;
  });

  // ---- collapse state, keyed by group id
  const [collapsed, setCollapsed] = createSignal<Set<string>>(new Set());
  const toggle = (id: string) =>
    setCollapsed((s) => {
      const n = new Set(s);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  const [open, setOpen] = createSignal<string | null>(null);

  const visible = createMemo(() =>
    props.rows.filter((r) => !r.parent || !collapsed().has(r.parent)),
  );

  const tNow = () => (props.live ? now() : clock());

  /** Geometry of one bar, in percent of the track. Null = nothing to draw. */
  const bar = (r: TraceRow) => {
    if (r.start == null) return null;
    const end = r.end ?? tNow();
    const l = x(r.start);
    const rr = x(end);
    if (rr < 0 || l > 100) return { off: rr < 0 ? ('left' as const) : ('right' as const), end };
    const left = clampPct(l);
    return {
      left,
      width: Math.max(0.5, clampPct(rr) - left),
      clippedLeft: l < 0,
    };
  };

  const wait = (r: TraceRow) => {
    if (r.queued == null) return null;
    const until = r.start ?? tNow();
    const l = clampPct(x(r.queued));
    const rr = clampPct(x(until));
    if (rr <= 0 || l >= 100 || rr - l <= 0) return null;
    return { left: l, width: Math.max(0.5, rr - l) };
  };

  /** A group row's line: every child's span, thin. */
  const childSpans = (id: string) =>
    props.rows
      .filter((r) => r.parent === id)
      .map((r) => ({ r, g: bar(r) }))
      .filter((v) => v.g && !('off' in v.g!));

  /** Elbows from each dependency's end down to the dependent's start. */
  const handoffs = createMemo(() => {
    const rows = visible();
    const index = new Map(rows.map((r, i) => [r.id, i]));
    const out: { d: string; key: string }[] = [];
    rows.forEach((r, i) => {
      if (!r.after?.length || r.start == null) return;
      const sx = clampPct(x(r.start));
      for (const dep of r.after) {
        const j = index.get(dep);
        if (j === undefined) continue;
        const d = rows[j];
        if (d.start == null) continue;
        const ex = clampPct(x(d.end ?? tNow()));
        const y1 = j * ROW_H + ROW_H / 2 + 7;
        const y2 = i * ROW_H + ROW_H / 2;
        out.push({ key: `${dep}>${r.id}`, d: `M ${ex} ${y1} L ${ex} ${y2} L ${sx} ${y2}` });
      }
    });
    return out;
  });

  const legend = () => props.legend ?? ['run', 'wait', 'handoff', 'failed'];

  return (
    <section class="trace">
      <header class="trace-head">
        <span class="trace-title">{props.title}</span>
        <Show when={props.tools}>
          <div class="trace-tools">{props.tools}</div>
        </Show>
      </header>

      <Show when={props.stats?.length}>
        <div class="trace-stats">
          <For each={props.stats}>
            {(s) => (
              <div class="trace-stat">
                <span class="trace-stat-label">{s.label}</span>
                <span class="trace-stat-value" classList={{ [`tone-${s.tone}`]: !!s.tone }}>
                  {s.value}
                </span>
              </div>
            )}
          </For>
        </div>
      </Show>

      <div class="trace-grid">
        {/* axis header */}
        <div class="trace-axis-label">
          <svg viewBox="0 0 16 16" aria-hidden="true">
            <path d="M3 3h4v4H3zM9 9h4v4H9zM5 7v4h4" />
          </svg>
          {props.treeLabel ?? 'Steps'}
        </div>
        <div class="trace-axis">
          <For each={ticks()}>
            {(t) => (
              <span class="trace-tick" style={{ left: `${t.at}%` }}>
                {t.label}
              </span>
            )}
          </For>
          <Show when={props.live}>
            <span class="trace-now-dot" style={{ left: `${clampPct(x(tNow()))}%` }} />
          </Show>
        </div>

        {/* rows */}
        <div class="trace-tree">
          <Show when={visible().length > 0} fallback={<div class="trace-empty">{props.empty ?? 'Nothing here.'}</div>}>
            <For each={visible()}>
              {(r) => {
                const isOpen = () => open() === r.id;
                const clickable = () => !!r.detail || !!r.href || !!r.group;
                const onClick = () => {
                  if (r.group) toggle(r.id);
                  else if (r.detail) setOpen(isOpen() ? null : r.id);
                };
                const name = () => (
                  <>
                    <Show when={r.group}>
                      <svg class="trace-caret" classList={{ shut: collapsed().has(r.id) }} viewBox="0 0 16 16" aria-hidden="true">
                        <path d="M4 6l4 4 4-4" />
                      </svg>
                    </Show>
                    <span class="trace-badge" style={{ '--c': r.color ?? 'var(--ink-faint)' }}>
                      {r.badge ?? r.label.replace(/^\W+/, '').slice(0, 1).toUpperCase()}
                    </span>
                    <span class="trace-label" classList={{ group: !!r.group }}>{r.label}</span>
                    <Show when={r.sub}>
                      <span class="trace-sub">{r.sub}</span>
                    </Show>
                  </>
                );
                return (
                  <div
                    class="trace-name"
                    classList={{ clickable: clickable(), open: isOpen(), [r.state]: true }}
                    style={{ '--d': String(r.depth ?? 0) }}
                    onClick={onClick}
                  >
                    <For each={Array.from({ length: r.depth ?? 0 })}>
                      {(_, k) => <span class="trace-guide" style={{ left: `${14 + k() * 16}px` }} />}
                    </For>
                    <Show when={r.href && !r.group} fallback={name()}>
                      <A href={r.href!} class="trace-name-link" onClick={(e) => e.stopPropagation()}>
                        {name()}
                      </A>
                    </Show>
                    <Show when={r.actions}>
                      <span class="trace-actions" onClick={(e) => e.stopPropagation()}>
                        {r.actions!()}
                      </span>
                    </Show>
                  </div>
                );
              }}
            </For>
          </Show>
        </div>

        <div class="trace-track" style={{ '--rows': String(visible().length) }}>
          {/* Overlays share the lanes' box, so a percentage means the same
              instant on a rule, a bar and the now line. */}
          <div class="trace-layer">
            <For each={ticks()}>
              {(t) => <span class="trace-rule" style={{ left: `${t.at}%` }} />}
            </For>
            <Show when={props.live}>
              <span class="trace-now" style={{ left: `${clampPct(x(tNow()))}%` }} />
            </Show>
          </div>

          <svg class="trace-handoffs" viewBox={`0 0 100 ${Math.max(1, visible().length * ROW_H)}`} preserveAspectRatio="none" aria-hidden="true">
            <For each={handoffs()}>{(h) => <path d={h.d} />}</For>
          </svg>

          <For each={visible()}>
            {(r) => {
              const g = () => bar(r);
              const w = () => wait(r);
              return (
                <div class="trace-lane" classList={{ [r.state]: true }}>
                  <Show
                    when={!r.group}
                    fallback={
                      <For each={childSpans(r.id)}>
                        {(c) => (
                          <span
                            class="trace-bar mini"
                            classList={{ [c.r.state]: true }}
                            style={{
                              left: `${(c.g as { left: number }).left}%`,
                              width: `${(c.g as { width: number }).width}%`,
                              '--c': c.r.state === 'failed' ? 'var(--bad)' : (c.r.color ?? r.color ?? 'var(--ink-dim)'),
                            }}
                          />
                        )}
                      </For>
                    }
                  >
                    <Show when={w()}>
                      {(ww) => <span class="trace-wait" style={{ left: `${ww().left}%`, width: `${ww().width}%` }} />}
                    </Show>
                    <Show when={g()}>
                      {(gg) => (
                        <Show
                          when={!('off' in gg())}
                          fallback={
                            <span class="trace-off" classList={{ right: (gg() as { off: string }).off === 'right' }}>
                              {(gg() as { off: string }).off === 'left'
                                ? `◂ ${short(tNow() - (gg() as { end: number }).end)} ago`
                                : 'later ▸'}
                              {/* Off the window, the row's note rides with the marker. */}
                              <Show when={r.meta}>
                                <span class="trace-off-meta">{r.meta}</span>
                              </Show>
                            </span>
                          }
                        >
                          {(() => {
                            const v = gg() as { left: number; width: number; clippedLeft: boolean };
                            return (
                              <span
                                class="trace-bar"
                                classList={{
                                  [r.state]: true,
                                  estimated: !!r.estimated,
                                  clipped: v.clippedLeft,
                                }}
                                style={{
                                  left: `${v.left}%`,
                                  width: `${v.width}%`,
                                  // Red is status, so a failure overrides the row's hue.
                                  '--c': r.state === 'failed' ? 'var(--bad)' : (r.color ?? 'var(--ink-dim)'),
                                }}
                              >
                                <Show when={r.state === 'failed'}>
                                  <svg class="trace-x" viewBox="0 0 16 16" aria-hidden="true">
                                    <path d="M5 5l6 6m0-6-6 6" />
                                  </svg>
                                </Show>
                                <Show when={r.state === 'running'}>
                                  <svg class="trace-spin" viewBox="0 0 16 16" aria-hidden="true">
                                    <path d="M8 3a5 5 0 1 1-5 5" />
                                  </svg>
                                </Show>
                              </span>
                            );
                          })()}
                        </Show>
                      )}
                    </Show>
                    <Show when={(r.meta || (r.state === 'running' && r.start != null)) && !(g() && 'off' in g()!)}>
                      <span
                        class="trace-meta"
                        style={{
                          left: `${clampPct(
                            Math.max(
                              g() && !('off' in g()!) ? (g() as { left: number; width: number }).left + (g() as { width: number }).width : 0,
                              w() ? w()!.left + w()!.width : 0,
                            ),
                          )}%`,
                        }}
                      >
                        {r.state === 'running' && r.start != null
                          ? [short(tNow() - r.start), r.meta].filter(Boolean).join(' · ')
                          : r.meta}
                      </span>
                    </Show>
                  </Show>
                </div>
              );
            }}
          </For>
        </div>

        {/* The opened row's detail, across both columns under the trace. */}
        <Show when={open()}>
          {(id) => {
            const r = () => props.rows.find((x) => x.id === id());
            return (
              <Show when={r()?.detail}>
                <div class="trace-detail">
                  <div class="trace-detail-head">
                    <span class="trace-badge" style={{ '--c': r()!.color ?? 'var(--ink-faint)' }}>
                      {r()!.badge ?? r()!.label.slice(0, 1).toUpperCase()}
                    </span>
                    <span class="trace-label">{r()!.label}</span>
                    <button class="icon-btn" aria-label="Close" onClick={() => setOpen(null)}>
                      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
                        <path d="m4.5 4.5 7 7m0-7-7 7" />
                      </svg>
                    </button>
                  </div>
                  {r()!.detail!()}
                </div>
              </Show>
            );
          }}
        </Show>
      </div>

      <footer class="trace-foot">
        <Show when={props.live || props.windows?.length}>
          <div class="trace-controls">
            <Show when={props.live}>
              <button
                class="trace-ctl"
                aria-label={paused() ? 'Resume live updates' : 'Pause live updates'}
                title={paused() ? 'Resume' : 'Pause'}
                onClick={() => {
                  setPaused(!paused());
                  if (!paused()) setNow(clock());
                }}
              >
                <Show
                  when={paused()}
                  fallback={
                    <svg viewBox="0 0 16 16" aria-hidden="true">
                      <path d="M6 4v8M10 4v8" />
                    </svg>
                  }
                >
                  <svg viewBox="0 0 16 16" aria-hidden="true">
                    <path d="M5.5 4l6 4-6 4z" />
                  </svg>
                </Show>
              </button>
            </Show>
            <Show when={props.windows?.length}>
              <select
                class="trace-zoom"
                value={win()?.key}
                onChange={(e) => setWindowKey(e.currentTarget.value)}
                aria-label="Time window"
              >
                <For each={props.windows}>{(w) => <option value={w.key}>{w.label}</option>}</For>
              </select>
            </Show>
          </div>
        </Show>
        <div class="trace-legend">
          <Show when={legend().includes('run')}>
            <span><i class="lg run" />Running</span>
          </Show>
          <Show when={legend().includes('wait')}>
            <span><i class="lg wait" />Waiting</span>
          </Show>
          <Show when={legend().includes('handoff')}>
            <span><i class="lg handoff" />Hand-off</span>
          </Show>
          <Show when={legend().includes('skipped')}>
            <span><i class="lg skipped" />Skipped</span>
          </Show>
          <Show when={legend().includes('failed')}>
            <span><i class="lg failed" />Failed</span>
          </Show>
        </div>
      </footer>
    </section>
  );
}
