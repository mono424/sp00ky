import { For, Index, Show, createEffect, createMemo, createSignal, onCleanup } from 'solid-js';
import { useSearchParams } from '@solidjs/router';
import { openStream } from '../api/client';
import { PageHead } from '../components/Chrome';
import { FilterMenu, Sq, type FilterValue } from '../components/Frame';
import { formatClock, formatCount } from '../lib/format';
import type { LogLine, Overview } from '../api/types';

/** Above this many lines the oldest are dropped, so a long tail cannot grow
 *  without bound in the tab's memory. */
const MAX_LINES = 5000;

/** A clock to the second, for the strip's axis. */
const hms = (ms: number) => formatClock(ms).replace(/\.\d+$/, '');

/** Columns of the activity strip above the stream. */
const BUCKETS = 120;

const LEVELS = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];

type Entry =
  | { kind: 'line'; line: LogLine; seq: number }
  | { kind: 'dropped'; count: number; seq: number };

/**
 * Live log tail for the scheduler or any SSP.
 *
 * Top to bottom: where the lines come from and which to show; an activity
 * strip over the whole buffer (warnings and errors stacked in their colours,
 * click a column to jump there); then the stream itself, filling the rest of
 * the window and following the tail until the reader scrolls up.
 *
 * Backends are deliberately absent from the sources: the scheduler reaches
 * them with HTTP health checks and has no pipe to their output, so offering
 * them here would be offering something that cannot work.
 */
export function Logs(props: { overview: Overview | undefined }) {
  const [params, setParams] = useSearchParams<{ source?: string }>();
  const [entries, setEntries] = createSignal<Entry[]>([]);
  const [connected, setConnected] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [follow, setFollow] = createSignal(true);
  const [filters, setFilters] = createSignal<FilterValue>({});
  const levels = () => filters().level ?? [];
  const targets = () => filters().target ?? [];
  const [text, setText] = createSignal('');

  let viewport: HTMLDivElement | undefined;
  let seq = 0;

  const source = () => params.source || 'scheduler';

  const sources = () => [
    { value: 'scheduler', label: 'scheduler', tone: props.overview?.scheduler?.status === 'ready' || !props.overview?.scheduler ? '' : 'warn' },
    ...(props.overview?.ssps ?? []).map((s) => ({
      value: `ssp:${s.id}`,
      label: s.id,
      tone: s.status === 'ready' ? '' : 'warn',
    })),
  ];

  // Re-open the stream whenever the source changes. `createEffect` tracking
  // `source()` plus `onCleanup` inside it is what makes switching sources tear
  // the old stream down rather than accumulating them.
  createEffect(() => {
    const src = source();
    setEntries([]);
    setConnected(false);
    setError(null);
    setFollow(true);
    seq = 0;

    const close = openStream(`/logs?source=${encodeURIComponent(src)}&tail=true`, {
      onOpen: () => {
        setConnected(true);
        setError(null);
      },
      onEvent: (event, data) => {
        if (event === 'line') {
          try {
            const line = JSON.parse(data) as LogLine;
            push({ kind: 'line', line, seq: seq++ });
          } catch {
            /* skip a malformed line rather than dropping the stream */
          }
        } else if (event === 'dropped') {
          push({ kind: 'dropped', count: Number(data) || 0, seq: seq++ });
        }
      },
      onError: (err) => {
        setConnected(false);
        setError(err instanceof Error ? err.message : 'Stream disconnected');
      },
    });
    onCleanup(close);
  });

  const push = (entry: Entry) => {
    setEntries((prev) => {
      const next = prev.length >= MAX_LINES ? prev.slice(prev.length - MAX_LINES + 1) : prev.slice();
      next.push(entry);
      return next;
    });
    if (follow()) {
      // After paint, so the height is the post-append height.
      queueMicrotask(() => {
        if (viewport) viewport.scrollTop = viewport.scrollHeight;
      });
    }
  };

  const lines = createMemo(() =>
    entries().flatMap((e) => (e.kind === 'line' ? [e as Extract<Entry, { kind: 'line' }>] : [])),
  );

  /** Lines per level and per target in the buffer, for the filter menu. */
  const counts = createMemo(() => {
    const c: Record<string, number> = {};
    for (const e of lines()) c[e.line.level] = (c[e.line.level] ?? 0) + 1;
    return c;
  });
  const targetCounts = createMemo(() => {
    const c = new Map<string, number>();
    for (const e of lines()) c.set(e.line.target, (c.get(e.line.target) ?? 0) + 1);
    return [...c.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  });

  const matches = (line: LogLine) => {
    if (targets().length && !targets().includes(line.target)) return false;
    const q = text().trim().toLowerCase();
    if (!q) return true;
    return (
      line.message.toLowerCase().includes(q) ||
      line.target.toLowerCase().includes(q) ||
      (line.fields ?? '').toLowerCase().includes(q)
    );
  };

  const visible = createMemo(() => {
    const lv = levels();
    return entries().filter((e) => e.kind === 'dropped' || ((!lv.length || lv.includes(e.line.level)) && matches(e.line)));
  });

  /**
   * The buffer cut into equal slices of time: how many lines each, and how
   * many of them were warnings or errors. Follows the search and targets, not
   * the levels, so the strip still shows where the errors are while reading info.
   */
  const activity = createMemo(() => {
    const ls = lines().filter((e) => matches(e.line));
    if (ls.length < 2) return null;
    const t0 = ls[0].line.ts;
    const t1 = ls[ls.length - 1].line.ts;
    const span = Math.max(1, t1 - t0);
    const cols = Array.from({ length: BUCKETS }, () => ({ all: 0, warn: 0, error: 0, seq: -1 }));
    for (const e of ls) {
      const b = Math.min(BUCKETS - 1, Math.floor(((e.line.ts - t0) / span) * BUCKETS));
      const col = cols[b];
      col.all++;
      if (e.line.level === 'WARN') col.warn++;
      if (e.line.level === 'ERROR') col.error++;
      if (col.seq < 0) col.seq = e.seq;
    }
    const hi = Math.max(1, ...cols.map((c) => c.all));
    return { cols, hi, t0, t1 };
  });

  // Following means "stick to the bottom". Scrolling up turns it off, which is
  // what a reader inspecting older lines expects.
  const onScroll = () => {
    if (!viewport) return;
    const atBottom = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight < 40;
    if (atBottom !== follow()) setFollow(atBottom);
  };

  const jumpToEnd = () => {
    setFollow(true);
    if (viewport) viewport.scrollTop = viewport.scrollHeight;
  };

  /** Scroll the stream to the first line at or after a column of the strip. */
  const jumpTo = (seqAt: number) => {
    if (seqAt < 0 || !viewport) return;
    const el = viewport.querySelector(`[data-seq="${seqAt}"]`) ?? [...viewport.querySelectorAll<HTMLElement>('[data-seq]')].find((n) => Number(n.dataset.seq) >= seqAt);
    if (!el) return;
    setFollow(false);
    viewport.scrollTop = (el as HTMLElement).offsetTop - 8;
    // Mark where the jump landed, briefly.
    el.classList.remove('flash');
    void (el as HTMLElement).offsetWidth;
    el.classList.add('flash');
  };

  const shown = () => visible().filter((e) => e.kind === 'line').length;

  return (
    <>
      <PageHead
        title="Logs"
        actions={
          <>
            <span class="cap lx-live" classList={{ on: connected() }}>
              <span class="lx-pulse" />
              {connected() ? 'Streaming' : error() ? 'Disconnected' : 'Connecting'}
            </span>
            <span class="cap">
              <b style={{ color: 'var(--ink-dim)' }}>{formatCount(shown())}</b> lines
            </span>
          </>
        }
      />

      <div class="lx-page">
        {/* where from, which levels, what text */}
        <section class="fx-row lx-filters">
          <div class="fx-cell">
            <span class="cap">Source</span>
            <div class="fx-range lx-sources" role="group" aria-label="Source"
              title="Scheduler and SSPs only: backends are health-checked over HTTP, their output never reaches the scheduler.">
              <For each={sources()}>
                {(s) => (
                  <button classList={{ on: source() === s.value }} aria-pressed={source() === s.value}
                    onClick={() => setParams({ source: s.value === 'scheduler' ? undefined : s.value })}>
                    <Sq tone={s.tone} />
                    {s.label}
                  </button>
                )}
              </For>
            </div>
          </div>
          <div class="fx-cell">
            <FilterMenu
              value={filters()}
              onChange={setFilters}
              fields={[
                {
                  key: 'level',
                  label: 'Level',
                  options: LEVELS.filter((l) => counts()[l]).map((l) => ({
                    value: l,
                    label: l.toLowerCase(),
                    count: counts()[l],
                    tone: l === 'ERROR' ? 'bad' : l === 'WARN' ? 'warn' : l === 'INFO' ? '' : 'idle',
                  })),
                },
                {
                  key: 'target',
                  label: 'Target',
                  options: targetCounts().map(([t, n]) => ({ value: t, label: t.replace(/^scheduler::/, ''), count: n })),
                },
              ]}
            />
          </div>
          <div class="fx-cell lx-find">
            <input type="search" aria-label="Search" placeholder="Search messages, targets, fields"
              value={text()} onInput={(e) => setText(e.currentTarget.value)} />
          </div>
        </section>

        {/* the whole buffer at a glance */}
        <section class="fx-row">
          <div class="fx-cell lx-activity-cell">
            <Show when={activity()} fallback={<div class="lx-activity empty" />}>
              {(a) => (
                <>
                  <div class="lx-activity" aria-label="Lines over time">
                    <Index each={a().cols}>
                      {(c) => (
                        <button
                          class="lx-col"
                          disabled={c().all === 0}
                          title={c().all ? `${c().all} lines${c().error ? `, ${c().error} errors` : ''}${c().warn ? `, ${c().warn} warnings` : ''}` : undefined}
                          onClick={() => jumpTo(c().seq)}
                        >
                          <i class="all" style={{ height: `${(c().all / a().hi) * 100}%` }}>
                            <i class="warn" style={{ height: `${c().all ? ((c().warn + c().error) / c().all) * 100 : 0}%` }}>
                              <i class="err" style={{ height: `${c().warn + c().error ? (c().error / (c().warn + c().error)) * 100 : 0}%` }} />
                            </i>
                          </i>
                        </button>
                      )}
                    </Index>
                  </div>
                  <div class="fx-axis cap">
                    <span>{hms(a().t0)}</span>
                    <span>{hms(a().t0 + (a().t1 - a().t0) / 2)}</span>
                    <span>{hms(a().t1)}</span>
                  </div>
                </>
              )}
            </Show>
          </div>
        </section>

        <Show when={error()}>
          <div class="fx-log bad lx-error">
            <span class="fx-what">{error()}</span>
          </div>
        </Show>

        {/* the stream */}
        <div class="lx-stream" ref={viewport} onScroll={onScroll}>
          <Show
            when={visible().length > 0}
            fallback={<div class="fx-empty cap lx-empty">{connected() ? 'No lines match' : 'Waiting for the stream'}</div>}
          >
            <For each={visible()}>
              {(entry) =>
                entry.kind === 'dropped' ? (
                  <div class="lx-dropped cap">{formatCount(entry.count)} lines dropped · this viewer fell behind</div>
                ) : (
                  <div class={`lx-line ${entry.line.level}`} data-seq={entry.seq}>
                    <span class="lx-ts">{formatClock(entry.line.ts)}</span>
                    <span class="lx-lvl">{entry.line.level}</span>
                    <span class="lx-target" title={entry.line.target}>{entry.line.target.replace(/^scheduler::/, '')}</span>
                    <span class="lx-msg">
                      {entry.line.message}
                      <Show when={entry.line.fields}>
                        <span class="lx-fields">{entry.line.fields}</span>
                      </Show>
                    </span>
                  </div>
                )
              }
            </For>
          </Show>
        </div>

        {/* Scrolled up to read: one click back to the live tail. */}
        <Show when={!follow()}>
          <button class="btn btn-sm btn-primary lx-jump" onClick={jumpToEnd}>
            Jump to latest ↓
          </button>
        </Show>
      </div>
    </>
  );
}
