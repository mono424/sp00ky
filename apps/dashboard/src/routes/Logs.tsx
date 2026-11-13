import { For, Index, Show, batch, createEffect, createMemo, createSignal, on, onCleanup, onMount, untrack, type JSX } from 'solid-js';
import { useSearchParams } from '@solidjs/router';
import { openStream } from '../api/client';
import { PageHead } from '../components/Chrome';
import { FilterMenu, Sq, type FilterValue } from '../components/Frame';
import { formatCount, formatRelativeTime } from '../lib/format';
import { liveLogsView, useLiveView } from '../api/liveStack';
import { LogRowsSkeleton, LogTotalSkeleton, PlotSkeleton } from '../skeletons/Logs';
import type { Overview } from '../api/types';
import {
  appendNew,
  clock,
  count,
  day,
  frameEnd,
  frameFor,
  lasting,
  lowerBound,
  parseFields,
  prependOlder,
  rangeLabel,
  shape,
  span,
  tick,
  type Columns,
  type Frame,
  type Line,
  type Win,
} from '../lib/logscope';

/** Lines the tab keeps, and asks for when it opens: the whole of the server's ring. */
const MAX_LINES = 10_000;

const LEVELS = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];

/** Table row height. Every row is one line tall, so the table can draw only what is on screen. */
const ROW = 28;
/** Rows drawn past either edge of the table, so a fast scroll never shows a gap. */
const OVERSCAN = 12;

/** The histogram: the band the pill floats in, then the bars. */
const CHART_H = 132;
const PILL_ZONE = 38;
/** Narrowest a column may get, in CSS pixels. */
const MIN_COL = 6;
const MAX_COLS = 180;
/** A new frame's bars grow in, left to right. */
const GROW_MS = 340;
const STAGGER_MS = 160;

const sameWin = (a: Win | null, b: Win | null) => a === b || (!!a && !!b && a.t0 === b.t0 && a.t1 === b.t1);
const sameFrame = (a: Frame | null, b: Frame | null) =>
  a === b || (!!a && !!b && a.t0 === b.t0 && a.step === b.step && a.cols === b.cols);
/** Local time's offset from UTC, so minute, hour and day columns start where a clock says they do. */
const localOffset = (t: number) => -new Date(t).getTimezoneOffset() * 60_000;
const shortTarget = (t: string) => t.replace(/^(scheduler|ssp)::/, '');
const levelTone = (l: string) => (l === 'ERROR' ? 'bad' : l === 'WARN' ? 'warn' : l === 'INFO' ? '' : 'idle');

const parse = (data: string): Line | null => {
  try {
    return JSON.parse(data) as Line;
  } catch {
    return null; // skip a malformed line rather than dropping the stream
  }
};

/**
 * Log explorer for the scheduler or any SSP.
 *
 * Top to bottom: the volume of lines over time as a histogram you drag across
 * to pick a window (the pill above it reads the window out and zooms into
 * it, or back out); the search, where the window sits as a `time` chip beside
 * the level and target filters; then the lines themselves, newest first, with
 * the one picked open beside them.
 *
 * Everything below the chart is the same sorted buffer seen through binary
 * search, so dragging the window, zooming and typing re-read the table in the
 * same frame. The table draws only the rows on screen.
 *
 * On opening, the page asks for the server's whole ring (10,000 lines) once,
 * then follows the tail: over the live socket for the scheduler when there is
 * one, else over the same stream that delivered the history. Lines both carry
 * are merged, not doubled.
 *
 * Backends are deliberately absent from the sources: the scheduler reaches
 * them with HTTP health checks and has no pipe to their output, so offering
 * them here would be offering something that cannot work.
 */
export function Logs(props: { overview: Overview | undefined }) {
  const [params, setParams] = useSearchParams<{ source?: string }>();
  const source = () => params.source || 'scheduler';

  const sources = () => [
    { value: 'scheduler', label: 'scheduler', tone: props.overview?.scheduler?.status === 'ready' || !props.overview?.scheduler ? '' : 'warn' },
    ...(props.overview?.ssps ?? []).map((s) => ({
      value: `ssp:${s.id}`,
      label: s.id,
      tone: s.status === 'ready' ? '' : 'warn',
    })),
  ];

  /* ---- the buffer: sorted by time, mutated in place, `ver` says when ---- */

  let buf: Line[] = [];
  const [ver, setVer] = createSignal(0);
  const [dropped, setDropped] = createSignal(0);
  const [connected, setConnected] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  // Something has arrived, or the source answered and had nothing: the skeleton can go.
  const [settled, setSettled] = createSignal(false);
  const [growAt, setGrowAt] = createSignal(0);

  const grow = () => setGrowAt(performance.now());
  const changed = () => {
    if (buf.length > MAX_LINES) buf.splice(0, buf.length - MAX_LINES);
    setVer((v) => v + 1);
  };
  const ingest = (lines: readonly Line[]) => {
    const was = buf.length;
    if (!appendNew(buf, lines)) return;
    batch(() => {
      changed();
      setSettled(true);
      if (!was) grow();
    });
  };

  // A stream's lines are applied in batches: thousands arrive at once when it
  // opens, and the page should redraw a few times a second, not per line.
  let pending: Line[] = [];
  let flushing: ReturnType<typeof setTimeout> | undefined;
  const queue = (line: Line) => {
    pending.push(line);
    if (pending.length > MAX_LINES) pending.splice(0, pending.length - MAX_LINES);
    flushing ??= setTimeout(() => {
      flushing = undefined;
      const lines = pending;
      pending = [];
      ingest(lines);
    }, 60);
  };
  onCleanup(() => clearTimeout(flushing));

  /* ---- where the lines come from ---- */

  const live = useLiveView(liveLogsView);
  let shown = '';
  createEffect(() => {
    const src = source();
    const socket = src === 'scheduler' ? live() : undefined;
    if (src !== shown) {
      shown = src;
      buf = [];
      pending = [];
      batch(() => {
        setVer((v) => v + 1);
        setDropped(0);
        setConnected(false);
        setError(null);
        setSettled(false);
        setZoom([]);
        setSel(null);
        setPicked(null);
      });
    }

    if (socket) {
      // The socket carries the last 500 lines and everything after; the rest
      // of the ring comes once, over a stream that closes on its own.
      const history: Line[] = [];
      const close = openStream(`/logs?source=scheduler&tail=false&backfill=${MAX_LINES}`, {
        onEvent: (event, data) => {
          const line = event === 'line' ? parse(data) : null;
          if (line) history.push(line);
        },
        onDone: () => {
          const was = buf.length;
          buf = prependOlder(buf, history);
          if (buf.length === was) return;
          batch(() => {
            changed();
            setSettled(true);
            grow();
          });
        },
        // The socket still has the tail; the page is only shallower.
        onError: () => undefined,
      });
      onCleanup(close);
      createEffect(() => {
        const { lines, dropped } = socket();
        ingest(lines);
        batch(() => {
          setDropped(dropped);
          setConnected(true);
          setError(null);
          if (!lines.length) setSettled(true);
        });
      });
      return;
    }

    setConnected(false);
    let quiet: ReturnType<typeof setTimeout> | undefined;
    const close = openStream(`/logs?source=${encodeURIComponent(src)}&tail=true&backfill=${MAX_LINES}`, {
      onOpen: () => {
        setConnected(true);
        setError(null);
        // The backfill follows the headers at once; nothing by now means an empty ring.
        quiet = setTimeout(() => setSettled(true), 400);
      },
      onEvent: (event, data) => {
        if (event === 'line') {
          const line = parse(data);
          if (line) queue(line);
        } else if (event === 'dropped') {
          setDropped((d) => d + (Number(data) || 0));
        }
      },
      onError: (err) => {
        setConnected(false);
        setSettled(true);
        setError(err instanceof Error ? err.message : 'Stream disconnected');
      },
    });
    onCleanup(() => {
      clearTimeout(quiet);
      close();
    });
  });

  /* ---- what to show ---- */

  const [filters, setFilters] = createSignal<FilterValue>({});
  const [text, setText] = createSignal('');
  const query = createMemo(() => text().trim().toLowerCase());

  /** The lines every filter but time lets through: the buffer itself when there are none. */
  const matched = createMemo(
    () => {
      ver();
      const lv = filters().level ?? [];
      const tg = filters().target ?? [];
      const q = query();
      if (!lv.length && !tg.length && !q) return buf;
      return buf.filter(
        (l) =>
          (!lv.length || lv.includes(l.level)) &&
          (!tg.length || tg.includes(l.target)) &&
          (!q || l.message.toLowerCase().includes(q) || l.target.toLowerCase().includes(q) || (!!l.fields && l.fields.toLowerCase().includes(q))),
      );
    },
    buf,
    { equals: false },
  );

  /** Lines per level and per target in the buffer, for the filter menu. */
  const tallies = createMemo(() => {
    ver();
    const levels: Record<string, number> = {};
    const targets = new Map<string, number>();
    for (const l of buf) {
      levels[l.level] = (levels[l.level] ?? 0) + 1;
      targets.set(l.target, (targets.get(l.target) ?? 0) + 1);
    }
    return { levels, targets: [...targets.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])) };
  });

  /* ---- time: the buffer's extent, the zoom stack, the brushed window ---- */

  const extent = createMemo<Win | null>(
    () => {
      ver();
      return buf.length ? { t0: buf[0].ts, t1: buf[buf.length - 1].ts + 1 } : null;
    },
    null,
    { equals: sameWin },
  );
  /** Windows zoomed into, innermost last. Zooming out returns to the one before. */
  const [zoom, setZoom] = createSignal<Win[]>([]);
  const zoomed = () => zoom().length > 0;
  const domain = () => zoom()[zoom().length - 1] ?? extent();
  const [sel, setSel] = createSignal<Win | null>(null, { equals: sameWin });
  /** The time filter the table reads: the brushed window, else the zoomed one. */
  const win = () => sel() ?? (zoomed() ? domain() : null);

  const [chartW, setChartW] = createSignal(0);
  const frame = createMemo<Frame | null>(
    () => {
      const d = domain();
      const w = chartW();
      if (!d || w <= 0) return null;
      return frameFor(d, Math.min(MAX_COLS, Math.floor(w / MIN_COL)), zoomed() ? null : localOffset(d.t0));
    },
    null,
    { equals: sameFrame },
  );
  const inView = createMemo(() => span(matched(), domain()), undefined, { equals: false });
  const cols = createMemo<Columns | null>(() => {
    const f = frame();
    const [lo, hi] = inView();
    return f ? count(matched(), lo, hi, f) : null;
  });

  /** The table's rows, `[lo, hi)` of `matched`, read newest first. */
  const rows = createMemo(() => span(matched(), win()), undefined, { equals: false });
  const rowCount = () => rows()[1] - rows()[0];
  const rowAt = (i: number) => matched()[rows()[1] - 1 - i];
  /** Where a line sits in the table, or -1 once it has left. */
  const rowOf = (line: Line) => {
    const m = matched();
    const [lo, hi] = rows();
    for (let j = Math.max(lo, lowerBound(m, line.ts)); j < hi && m[j].ts === line.ts; j++) {
      if (m[j] === line) return hi - 1 - j;
    }
    return -1;
  };

  const zoomIn = () => {
    const s = sel();
    if (!s) return;
    batch(() => {
      setZoom((z) => [...z, s]);
      setSel(null);
      grow();
    });
  };
  const zoomOut = () => {
    const z = zoom();
    if (!z.length) return;
    // Back out to the window before, with the one just left still marked.
    batch(() => {
      setZoom(z.slice(0, -1));
      setSel(z[z.length - 1]);
      grow();
    });
  };
  const clearTime = () =>
    batch(() => {
      if (zoomed()) grow();
      setSel(null);
      setZoom([]);
    });
  const clearPill = () => (sel() ? setSel(null) : clearTime());

  /* ---- the line read beside the table ---- */

  const [picked, setPicked] = createSignal<Line | null>(null);
  // A line the buffer has dropped, or a filter now hides, closes.
  createEffect(() => {
    const p = picked();
    if (p && rowOf(p) < 0) setPicked(null);
  });

  const around = (line: Line) => {
    const half = 30_000;
    const w = { t0: line.ts - half, t1: line.ts + half };
    const d = zoom()[zoom().length - 1];
    batch(() => {
      if (d && (w.t0 < d.t0 || w.t1 > d.t1)) {
        setZoom([]);
        grow();
      }
      setSel(w);
    });
  };

  let search: HTMLInputElement | undefined;
  const onKey = (e: KeyboardEvent) => {
    const t = e.target as HTMLElement | null;
    if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return;
    if (e.key === '/' && !e.metaKey && !e.ctrlKey) {
      e.preventDefault();
      search?.focus();
    } else if (e.key === 'Escape') {
      if (picked()) setPicked(null);
      else if (sel()) setSel(null);
      else if (zoomed()) zoomOut();
    }
  };
  document.addEventListener('keydown', onKey);
  onCleanup(() => document.removeEventListener('keydown', onKey));

  const loading = () => !settled() && !error();
  const viewCount = () => inView()[1] - inView()[0];
  const viewSpan = () => {
    const d = domain();
    return d ? lasting(d.t1 - d.t0) : '';
  };

  return (
    <>
      <PageHead
        title="Logs"
        actions={
          <span class="cap lx-live" classList={{ on: connected() }}>
            <span class="lx-pulse" />
            {connected() ? 'Streaming' : error() ? 'Disconnected' : 'Connecting'}
          </span>
        }
      />

      <div class="lx-page">
        {/* where from, how many, and when */}
        <section class="fx-row">
          <div class="fx-cell lx-chart-cell">
            <div class="lx-chart-top">
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
              <div class="lx-total">
                <Show when={!loading()} fallback={<LogTotalSkeleton />}>
                  <b>{formatCount(viewCount())}</b>
                  <span class="cap">
                    lines<Show when={viewSpan()}> · {viewSpan()}</Show>
                    <Show when={dropped() > 0}>
                      <span class="lx-dropped" title="This viewer fell behind the stream and skipped these lines.">
                        {' '}· {formatCount(dropped())} dropped
                      </span>
                    </Show>
                  </span>
                </Show>
              </div>
            </div>
            <Histogram
              frame={frame()}
              cols={cols()}
              domain={domain()}
              sel={sel()}
              zoomed={zoomed()}
              loading={loading()}
              growAt={growAt()}
              selCount={rowCount()}
              viewCount={viewCount()}
              onWidth={setChartW}
              onSelect={setSel}
              onZoomIn={zoomIn}
              onZoomOut={zoomOut}
              onClear={clearPill}
            />
          </div>
        </section>

        {/* the window as a chip, then the words */}
        <section class="fx-row lx-query">
          <div class="fx-cell lx-query-cell">
            <label class="lx-search">
              <svg width="13" height="13" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true">
                <circle cx="6" cy="6" r="4.5" />
                <path d="M9.5 9.5L13 13" />
              </svg>
              <Show when={win()}>
                {(w) => (
                  <span class="fm-chip lx-time-chip">
                    <span class="fm-chip-body">
                      <span class="fm-chip-k">time</span>
                      <span class="fm-chip-v">{rangeLabel(w())}</span>
                    </span>
                    <button type="button" class="fm-chip-x" aria-label="Clear the time window" onClick={(e) => (e.preventDefault(), clearTime())}>
                      <XIcon />
                    </button>
                  </span>
                )}
              </Show>
              <input
                ref={search}
                type="search"
                aria-label="Search"
                placeholder="Search messages, targets, fields"
                value={text()}
                onInput={(e) => setText(e.currentTarget.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Backspace' && !e.currentTarget.value && win()) clearTime();
                  else if (e.key === 'Escape') e.currentTarget.blur();
                }}
              />
              <kbd class="lx-kbd">/</kbd>
            </label>
            <FilterMenu
              value={filters()}
              onChange={setFilters}
              fields={[
                {
                  key: 'level',
                  label: 'Level',
                  options: LEVELS.filter((l) => tallies().levels[l]).map((l) => ({
                    value: l,
                    label: l.toLowerCase(),
                    count: tallies().levels[l],
                    tone: levelTone(l),
                  })),
                },
                {
                  key: 'target',
                  label: 'Target',
                  options: tallies().targets.map(([t, n]) => ({ value: t, label: shortTarget(t), count: n })),
                },
              ]}
            />
          </div>
        </section>

        <Show when={error()}>
          <div class="fx-log bad lx-error">
            <span class="fx-what">{error()}</span>
          </div>
        </Show>

        <div class="lx-body" classList={{ open: !!picked() }}>
          <LogTable
            count={rowCount()}
            at={rowAt}
            rowOf={rowOf}
            version={ver()}
            reset={[source(), filters(), query(), win()]}
            picked={picked()}
            onPick={setPicked}
            query={query()}
            loading={loading()}
            empty={
              <div class="lx-empty">
                <span class="cap">
                  {!buf.length ? (connected() ? 'No lines yet' : 'Waiting for the stream') : win() ? 'No lines in this window' : 'No lines match'}
                </span>
                <Show when={buf.length > 0}>
                  <div class="row">
                    <Show when={win()}>
                      <button class="btn btn-sm" onClick={clearTime}>Clear time</button>
                    </Show>
                    <Show when={query() || (filters().level ?? []).length || (filters().target ?? []).length}>
                      <button class="btn btn-sm" onClick={() => batch(() => (setText(''), setFilters({})))}>Clear filters</button>
                    </Show>
                  </div>
                </Show>
              </div>
            }
          />
          <Show when={picked()}>
            {(line) => (
              <Inspector
                line={line()}
                source={source()}
                lines={() => (ver(), buf)}
                onClose={() => setPicked(null)}
                onAround={() => around(line())}
                onTarget={() => setFilters((f) => ({ ...f, target: [line().target] }))}
              />
            )}
          </Show>
        </div>
      </div>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* the histogram                                                       */
/* ------------------------------------------------------------------ */

type Drag = 'new' | 'move' | 'w' | 'e';

/**
 * Lines over time on one canvas, with a window brushed across it.
 *
 * Drag on the bars to pick a window, drag inside it to move it, drag its
 * edges to resize; a click picks one column. Edges snap to columns, so the
 * window always holds whole bars. The pill over the window reads it out and
 * zooms in, back out, or clears it.
 *
 * Keys, with the chart focused: arrows move the window a column (with Shift
 * they stretch it), Enter zooms in, Backspace zooms out, Escape clears.
 */
function Histogram(props: {
  frame: Frame | null;
  cols: Columns | null;
  domain: Win | null;
  sel: Win | null;
  zoomed: boolean;
  loading: boolean;
  growAt: number;
  selCount: number;
  viewCount: number;
  onWidth: (w: number) => void;
  onSelect: (w: Win | null) => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onClear: () => void;
}) {
  let host!: HTMLDivElement;
  let canvas!: HTMLCanvasElement;
  const [w, setW] = createSignal(0);
  const [hover, setHover] = createSignal<number | null>(null);
  const [mode, setMode] = createSignal<Drag | null>(null);
  const [aim, setAim] = createSignal<Drag | null>(null);
  const [pillW, setPillW] = createSignal(0);

  onMount(() => {
    const ro = new ResizeObserver(([e]) => {
      const cw = Math.floor(e.contentRect.width);
      setW(cw);
      props.onWidth(cw);
    });
    ro.observe(host);
    onCleanup(() => ro.disconnect());
  });

  const total = (f: Frame) => f.cols * f.step;
  const px = (f: Frame, t: number) => ((t - f.t0) / total(f)) * w();
  const colAt = (f: Frame, x: number) => Math.min(f.cols - 1, Math.max(0, Math.floor((x / w()) * f.cols)));
  /** The column edge nearest a point: 0 to cols. */
  const edgeAt = (f: Frame, x: number) => Math.min(f.cols, Math.max(0, Math.round((x / w()) * f.cols)));
  /** A window kept inside the zoomed one: a zoomed frame's last column can run past its end. */
  const within = (s: Win): Win => {
    const d = props.zoomed ? props.domain : null;
    return d ? { t0: Math.max(d.t0, s.t0), t1: Math.min(d.t1, s.t1) } : s;
  };
  const cell = (f: Frame, c0: number, c1: number): Win => within({ t0: f.t0 + c0 * f.step, t1: f.t0 + (c1 + 1) * f.step });

  const hit = (x: number): Drag | null => {
    const f = props.frame;
    const s = props.sel;
    if (!f || !s) return null;
    const a = px(f, s.t0);
    const b = px(f, s.t1);
    if (Math.abs(x - a) <= 6) return 'w';
    if (Math.abs(x - b) <= 6) return 'e';
    return x > a && x < b ? 'move' : null;
  };

  let start: { x: number; c: number; sel: Win | null; mode: Drag; moved: boolean } | null = null;
  const localX = (e: PointerEvent | MouseEvent) => e.clientX - host.getBoundingClientRect().left;

  const onDown = (e: PointerEvent) => {
    const f = props.frame;
    if (e.button !== 0 || !f) return;
    const x = localX(e);
    start = { x, c: colAt(f, x), sel: props.sel, mode: hit(x) ?? 'new', moved: false };
    try {
      host.setPointerCapture(e.pointerId);
    } catch {
      /* a pointer that is already gone; the drag simply ends at the edge */
    }
    host.focus({ preventScroll: true });
    e.preventDefault();
  };

  const onMove = (e: PointerEvent) => {
    const f = props.frame;
    if (!f) return;
    const x = localX(e);
    if (!start) {
      setHover(colAt(f, x));
      setAim(hit(x));
      return;
    }
    if (!start.moved && Math.abs(x - start.x) < 3) return;
    if (!start.moved) {
      start.moved = true;
      setMode(start.mode);
      setHover(null);
    }
    const s0 = start.sel;
    if (start.mode === 'new' || !s0) {
      const c = colAt(f, x);
      props.onSelect(cell(f, Math.min(start.c, c), Math.max(start.c, c)));
    } else if (start.mode === 'move') {
      const width = s0.t1 - s0.t0;
      let t0 = s0.t0 + (colAt(f, x) - start.c) * f.step;
      t0 = Math.max(f.t0, Math.min(frameEnd(f) - width, t0));
      props.onSelect(within({ t0, t1: t0 + width }));
    } else {
      const t = f.t0 + edgeAt(f, x) * f.step;
      // Dragged past the other edge, the window flips rather than vanishing.
      const other = start.mode === 'w' ? s0.t1 : s0.t0;
      const t0 = Math.min(t, other);
      const t1 = Math.max(t, other);
      props.onSelect(within(t1 - t0 >= f.step ? { t0, t1 } : start.mode === 'w' ? { t0: other - f.step, t1: other } : { t0: other, t1: other + f.step }));
    }
  };

  const onUp = () => {
    const f = props.frame;
    const s = start;
    start = null;
    setMode(null);
    if (!f || !s || s.moved) return;
    // A click inside the window leaves it be, so a double-click can zoom it.
    if (s.mode === 'move' || s.mode === 'w' || s.mode === 'e') return;
    props.onSelect(cell(f, s.c, s.c));
  };

  const onKey = (e: KeyboardEvent) => {
    const f = props.frame;
    if (!f) return;
    const s = props.sel;
    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
      e.preventDefault();
      const d = e.key === 'ArrowLeft' ? -f.step : f.step;
      if (!s) props.onSelect(cell(f, f.cols - 1, f.cols - 1));
      else if (e.shiftKey) props.onSelect(s.t1 + d - s.t0 >= f.step ? within({ t0: s.t0, t1: Math.min(frameEnd(f), s.t1 + d) }) : s);
      else {
        const t0 = Math.max(f.t0, Math.min(frameEnd(f) - (s.t1 - s.t0), s.t0 + d));
        props.onSelect(within({ t0, t1: t0 + (s.t1 - s.t0) }));
      }
    } else if (e.key === 'Enter') {
      props.onZoomIn();
    } else if (e.key === 'Backspace' || e.key === '-') {
      props.onZoomOut();
    }
  };

  /* ---- drawing: one canvas, redrawn at most once a frame ---- */

  let raf = 0;
  const schedule = () => {
    raf ||= requestAnimationFrame(() => {
      raf = 0;
      draw();
    });
  };
  onCleanup(() => cancelAnimationFrame(raf));
  createEffect(() => {
    props.frame;
    props.cols;
    props.sel;
    props.growAt;
    hover();
    w();
    schedule();
  });

  let palette: { ink: string; warn: string; bad: string } | null = null;
  const colors = () => {
    if (palette) return palette;
    const css = getComputedStyle(host);
    palette = {
      ink: css.getPropertyValue('--ink').trim() || '#e4e4e4',
      warn: css.getPropertyValue('--warn').trim() || '#f1b440',
      bad: css.getPropertyValue('--bad').trim() || '#ff6369',
    };
    return palette;
  };

  const draw = () => {
    const W = w();
    if (!W || !canvas) return;
    const dpr = window.devicePixelRatio || 1;
    const bw = Math.round(W * dpr);
    const bh = Math.round(CHART_H * dpr);
    if (canvas.width !== bw || canvas.height !== bh) {
      canvas.width = bw;
      canvas.height = bh;
    }
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, CHART_H);
    const base = CHART_H - 1;
    ctx.fillStyle = 'rgba(255, 255, 255, 0.08)';
    ctx.fillRect(0, base, W, 1);

    const f = props.frame;
    const c = props.cols;
    if (!f || !c || !c.hi) return;
    const { ink, warn, bad } = colors();
    const colW = W / f.cols;
    const gap = colW >= 9 ? 2 : colW >= 4 ? 1 : 0;
    const barW = Math.max(1, colW - gap);
    const room = base - PILL_ZONE;
    const s = props.sel;
    const s0 = s ? px(f, s.t0) : 0;
    const s1 = s ? px(f, s.t1) : 0;
    const hov = hover();

    if (s) {
      ctx.fillStyle = 'rgba(255, 255, 255, 0.025)';
      ctx.fillRect(s0, PILL_ZONE - 6, s1 - s0, base - PILL_ZONE + 6);
    }

    const age = performance.now() - props.growAt;
    let growing = false;
    for (let i = 0; i < f.cols; i++) {
      const n = c.all[i];
      if (!n) continue;
      let p = (age - (i / f.cols) * STAGGER_MS) / GROW_MS;
      if (p < 1) growing = true;
      p = p <= 0 ? 0 : p >= 1 ? 1 : 1 - Math.pow(1 - p, 3);
      if (!p) continue;
      const h = Math.max(2, (n / c.hi) * room) * p;
      const x = i * colW + gap / 2;
      const mid = (i + 0.5) * colW;
      const inside = !!s && mid > s0 && mid < s1;
      const lit = hov === i;
      // Unpicked bars recede once there is a window; the window's bars come forward.
      const a = s ? (inside ? (lit ? 1 : 0.88) : lit ? 0.3 : 0.13) : lit ? 0.62 : 0.3;
      const tint = s ? (inside ? 1 : 0.32) : 0.85;
      const e = c.error[i];
      const wn = c.warn[i];
      // An error always shows, however few; warnings take their share and no more,
      // or a warning or two in every column would underline the whole chart.
      const he = e ? Math.min(h, Math.max(1.5, (e / n) * h)) : 0;
      const hw = wn ? Math.min(h - he, (wn / n) * h) : 0;
      ctx.globalAlpha = a;
      ctx.fillStyle = ink;
      ctx.fillRect(x, base - h, barW, h - he - hw);
      if (hw) {
        ctx.globalAlpha = tint;
        ctx.fillStyle = warn;
        ctx.fillRect(x, base - he - hw, barW, hw);
      }
      if (he) {
        ctx.globalAlpha = tint;
        ctx.fillStyle = bad;
        ctx.fillRect(x, base - he, barW, he);
      }
    }
    ctx.globalAlpha = 1;

    // The window's edges: hairlines up into the pill's band, a grip halfway down.
    if (s) {
      ctx.fillStyle = ink;
      const grip = PILL_ZONE + room / 2;
      for (const ex of [s0, s1]) {
        const x = Math.round(Math.min(W - 1, Math.max(0, ex)));
        ctx.fillRect(x, PILL_ZONE - 8, 1, base - PILL_ZONE + 9);
        ctx.fillRect(x - 2, grip - 7, 5, 14);
      }
    }
    if (growing) schedule();
  };

  /* ---- the pill, the hover readout and the axis ---- */

  const pillWin = () => props.sel ?? (props.zoomed ? props.domain : null);
  const pillLeft = () => {
    const f = props.frame;
    const pw = pillWin();
    if (!f || !pw) return 0;
    const W = w();
    const mid = props.sel ? (px(f, props.sel.t0) + px(f, props.sel.t1)) / 2 : W / 2;
    return Math.max(0, Math.min(W - pillW(), mid - pillW() / 2));
  };

  const hoverInfo = createMemo(() => {
    const f = props.frame;
    const c = props.cols;
    const i = hover();
    if (!f || !c || i === null || mode()) return null;
    const t0 = f.t0 + i * f.step;
    const parts = [`${formatCount(c.all[i])} ${c.all[i] === 1 ? 'line' : 'lines'}`];
    if (c.warn[i]) parts.push(`${c.warn[i]} warn`);
    if (c.error[i]) parts.push(`${c.error[i]} error`);
    return { x: (i + 0.5) * (w() / f.cols), text: `${tick(t0, f.step)} · ${parts.join(' · ')}` };
  });

  const cursor = () => {
    const m = mode() ?? aim();
    return m === 'move' ? (mode() ? 'grabbing' : 'grab') : m === 'w' || m === 'e' ? 'ew-resize' : 'crosshair';
  };

  return (
    <div class="lx-chart">
      <div
        ref={host}
        class="lx-plot"
        classList={{ loading: props.loading }}
        style={{ height: `${CHART_H}px`, cursor: props.frame ? cursor() : 'default' }}
        tabIndex={0}
        role="slider"
        aria-label="Lines over time: drag to pick a window"
        aria-valuetext={pillWin() ? rangeLabel(pillWin()!) : 'whole buffer'}
        onPointerDown={onDown}
        onPointerMove={onMove}
        onPointerUp={onUp}
        onPointerCancel={onUp}
        onPointerLeave={() => !start && (setHover(null), setAim(null))}
        onDblClick={(e) => hit(localX(e)) && props.onZoomIn()}
        onKeyDown={onKey}
      >
        <canvas ref={canvas} aria-hidden="true" />
        <Show when={props.loading}>
          <PlotSkeleton />
        </Show>
        <Show when={pillWin()}>
          {(pw) => (
            <div
              class="lx-pill"
              ref={(el) => {
                const ro = new ResizeObserver(() => setPillW(el.offsetWidth));
                ro.observe(el);
                onCleanup(() => ro.disconnect());
              }}
              style={{ transform: `translateX(${pillLeft()}px)` }}
              onPointerDown={(e) => e.stopPropagation()}
              onDblClick={(e) => e.stopPropagation()}
            >
              <span class="lx-pill-range">{rangeLabel(pw())}</span>
              <span class="lx-pill-n">{formatCount(props.sel ? props.selCount : props.viewCount)} lines</span>
              <span class="lx-pill-acts">
                <button type="button" title="Zoom in (Enter)" aria-label="Zoom in" disabled={!props.sel} onClick={() => props.onZoomIn()}>
                  <ZoomIcon plus />
                </button>
                <button type="button" title="Zoom out (Backspace)" aria-label="Zoom out" disabled={!props.zoomed} onClick={() => props.onZoomOut()}>
                  <ZoomIcon />
                </button>
                <button type="button" title="Clear (Esc)" aria-label="Clear" onClick={() => props.onClear()}>
                  <XIcon />
                </button>
              </span>
            </div>
          )}
        </Show>
      </div>
      <div class="lx-axis cap">
        <Show when={props.frame} fallback={<span>&nbsp;</span>}>
          {(f) => (
            <>
              <span>{tick(f().t0, f().step)}</span>
              <span>{tick(f().t0 + total(f()) / 2, f().step)}</span>
              <span>{tick(frameEnd(f()), f().step)}</span>
            </>
          )}
        </Show>
        <Show when={hoverInfo()}>
          {(h) => (
            <span class="lx-hover" style={{ left: `${Math.max(70, Math.min(w() - 70, h().x))}px` }}>
              {h().text}
            </span>
          )}
        </Show>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* the table                                                           */
/* ------------------------------------------------------------------ */

/**
 * The lines, newest first, one row each, only the ones on screen drawn.
 *
 * At the top the table follows the tail: new lines land above. Scrolled down
 * to read, the line at the top of the view stays put as lines arrive above
 * it, and a button counts them and goes back up. Changing what is shown (a
 * filter, the window, the source) starts again at the top.
 *
 * Keys, with the table focused: ↑/↓ or k/j move the picked line, Escape closes it.
 */
function LogTable(props: {
  count: number;
  at: (i: number) => Line;
  rowOf: (line: Line) => number;
  version: number;
  reset: unknown[];
  picked: Line | null;
  onPick: (line: Line | null) => void;
  query: string;
  loading: boolean;
  empty: JSX.Element;
}) {
  let vp!: HTMLDivElement;
  const [top, setTop] = createSignal(0);
  const [vh, setVh] = createSignal(600);
  const [fresh, setFresh] = createSignal(0);
  let anchor: { line: Line; row: number; offset: number } | null = null;

  onMount(() => {
    const ro = new ResizeObserver(([e]) => setVh(e.contentRect.height));
    ro.observe(vp);
    onCleanup(() => ro.disconnect());
  });

  const remember = () => {
    const t = vp.scrollTop;
    setTop(t);
    if (t < ROW / 2) {
      anchor = null;
      setFresh(0);
      return;
    }
    const row = Math.floor(t / ROW);
    anchor = row < props.count ? { line: props.at(row), row, offset: t - row * ROW } : null;
  };

  const scrollTo = (t: number) => {
    vp.scrollTop = t;
    setTop(vp.scrollTop);
  };

  // What is shown changed: back to the top. Only new lines: hold the reader's place.
  createEffect(
    on(
      () => props.reset,
      () => {
        anchor = null;
        setFresh(0);
        scrollTo(0);
      },
      { defer: true },
    ),
  );
  createEffect(
    on(
      () => props.version,
      () => {
        const a = anchor;
        if (!a) return;
        const row = props.rowOf(a.line);
        if (row < 0) {
          anchor = null;
          return;
        }
        if (row === a.row) return;
        if (row > a.row) setFresh((n) => n + row - a.row);
        anchor = { ...a, row };
        scrollTo(row * ROW + a.offset);
      },
      { defer: true },
    ),
  );

  const first = () => Math.max(0, Math.floor(top() / ROW) - OVERSCAN);
  const slots = createMemo(() => {
    props.version;
    const end = Math.min(props.count, Math.ceil((top() + vh()) / ROW) + OVERSCAN);
    const out: number[] = [];
    for (let i = first(); i < end; i++) out.push(i);
    return out;
  });

  const move = (by: number) => {
    if (!props.count) return;
    const cur = props.picked ? props.rowOf(props.picked) : -1;
    const next = Math.max(0, Math.min(props.count - 1, cur < 0 ? 0 : cur + by));
    props.onPick(props.at(next));
    const y = next * ROW;
    if (y < vp.scrollTop) scrollTo(y);
    else if (y + ROW > vp.scrollTop + vp.clientHeight) scrollTo(y + ROW - vp.clientHeight);
  };

  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'ArrowDown' || e.key === 'j') move(1);
    else if (e.key === 'ArrowUp' || e.key === 'k') move(-1);
    else if (e.key === 'Home') move(-props.count);
    else if (e.key === 'End') move(props.count);
    else return;
    e.preventDefault();
  };

  return (
    <div class="lx-table">
      <div class="lx-row lx-thead cap" aria-hidden="true">
        <span>Time</span>
        <span>Level</span>
        <span class="lx-target">Target</span>
        <span>Message</span>
      </div>
      <div class="lx-vp" ref={vp} onScroll={remember} tabIndex={0} onKeyDown={onKey} role="grid" aria-rowcount={props.count} aria-label="Log lines, newest first">
        <Show when={!props.loading} fallback={<LogRowsSkeleton />}>
          <Show when={props.count > 0} fallback={props.empty}>
            <div class="lx-spacer" style={{ height: `${props.count * ROW}px` }}>
              <div class="lx-rows" style={{ transform: `translateY(${first() * ROW}px)` }}>
                <Index each={slots()}>
                  {(i) => {
                    const line = createMemo(() => props.at(i()));
                    return (
                      <div
                        class={`lx-row ${line()?.level ?? ''}${props.picked === line() ? ' on' : ''}`}
                        role="row"
                        onClick={() => props.onPick(props.picked === line() ? null : line())}
                      >
                        <span class="lx-ts">{line() ? clock(line().ts, 'ms') : ''}</span>
                        <span class="lx-lvl">{line()?.level}</span>
                        <span class="lx-target" title={line()?.target}>{line() ? shortTarget(line().target) : ''}</span>
                        <span class="lx-msg">
                          <Marked text={line()?.message ?? ''} q={props.query} />
                          <Show when={line()?.fields}>
                            <span class="lx-fields">
                              <Marked text={line().fields!} q={props.query} />
                            </span>
                          </Show>
                        </span>
                      </div>
                    );
                  }}
                </Index>
              </div>
            </div>
          </Show>
        </Show>
      </div>
      <Show when={fresh() > 0}>
        <button class="lx-fresh" onClick={() => (setFresh(0), (anchor = null), scrollTo(0))}>
          ↑ {formatCount(fresh())} new {fresh() === 1 ? 'line' : 'lines'}
        </button>
      </Show>
    </div>
  );
}

/** `text` with the first match of `q` marked. */
function Marked(props: { text: string; q: string }) {
  const at = () => (props.q ? props.text.toLowerCase().indexOf(props.q) : -1);
  return (
    <Show when={at() >= 0} fallback={props.text}>
      {props.text.slice(0, at())}
      <mark>{props.text.slice(at(), at() + props.q.length)}</mark>
      {props.text.slice(at() + props.q.length)}
    </Show>
  );
}

/* ------------------------------------------------------------------ */
/* one line, read in full                                              */
/* ------------------------------------------------------------------ */

const shapes = new WeakMap<Line, string>();
const shapeOf = (l: Line) => {
  let s = shapes.get(l);
  if (s === undefined) shapes.set(l, (s = shape(l.message)));
  return s;
};

/** Columns in the "seen before" strip. */
const KIN_COLS = 48;

/**
 * The picked line beside the table: the message whole, when, its fields one
 * per row, and how often a line like it shows up in the buffer, which is what
 * says whether this is news or the usual.
 */
function Inspector(props: {
  line: Line;
  source: string;
  lines: () => Line[];
  onClose: () => void;
  onAround: () => void;
  onTarget: () => void;
}) {
  const fields = createMemo(() => parseFields(props.line.fields));
  const [copied, setCopied] = createSignal(false);

  /** Lines of the same shape as this one, over the whole buffer. */
  const kin = createMemo(() => {
    const all = props.lines();
    const me = shapeOf(props.line);
    if (!all.length) return null;
    const t0 = all[0].ts;
    const width = Math.max(1, all[all.length - 1].ts + 1 - t0);
    const bins = new Array<number>(KIN_COLS).fill(0);
    let n = 0;
    let first = 0;
    let last = 0;
    for (const l of all) {
      if (l.target !== props.line.target || shapeOf(l) !== me) continue;
      if (!n) first = l.ts;
      last = l.ts;
      n++;
      bins[Math.min(KIN_COLS - 1, Math.floor(((l.ts - t0) / width) * KIN_COLS))]++;
    }
    const at = Math.min(KIN_COLS - 1, Math.floor(((props.line.ts - t0) / width) * KIN_COLS));
    return { n, first, last, bins, hi: Math.max(1, ...bins), at, every: n > 1 ? (last - first) / (n - 1) : 0, span: width };
  });

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(untrack(() => props.line)));
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {
      /* the clipboard needs a secure context; nothing to do without one */
    }
  };

  return (
    <aside class="lx-insp" aria-label="Log line">
      <header class="lx-insp-head">
        <span class={`lx-insp-lvl ${props.line.level}`}>
          <Sq tone={levelTone(props.line.level)} />
          {props.line.level}
        </span>
        <span class="lx-insp-target" title={props.line.target}>{props.line.target}</span>
        <button class="sheet-x" aria-label="Close (Esc)" title="Close (Esc)" onClick={props.onClose}>
          <XIcon />
        </button>
      </header>

      <div class="lx-insp-msg">{props.line.message}</div>

      <dl class="lx-kv">
        <dt class="cap">Time</dt>
        <dd>
          {clock(props.line.ts, 'ms')} <span class="lx-dim">{day(props.line.ts)}</span>
        </dd>
        <dt class="cap">Age</dt>
        <dd>{formatRelativeTime(props.line.ts)}</dd>
        <dt class="cap">Source</dt>
        <dd>{props.source}</dd>
        <dt class="cap">Target</dt>
        <dd class="lx-wrap">{props.line.target}</dd>
      </dl>

      <Show when={fields().length}>
        <div class="lx-insp-sec">
          <span class="cap">Fields</span>
          <dl class="lx-kv">
            <For each={fields()}>
              {([k, v]) => (
                <>
                  <dt>{k || '·'}</dt>
                  <dd class="lx-wrap">{v}</dd>
                </>
              )}
            </For>
          </dl>
        </div>
      </Show>

      <Show when={kin()}>
        {(k) => (
          <div class="lx-insp-sec">
            <span class="cap">Seen before</span>
            <div class="lx-kin-n">
              <b>{k().n === 1 ? 'Once' : `${formatCount(k().n)} times`}</b>
              <span class="lx-dim"> in {lasting(k().span)}</span>
              <Show when={k().n > 1}>
                <span class="lx-dim"> · about every {lasting(k().every)}</span>
              </Show>
            </div>
            <div class="lx-kin" aria-hidden="true">
              <Index each={k().bins}>
                {(b, i) => <i classList={{ on: i === k().at, none: !b() }} style={{ height: `${b() ? Math.max(12, (b() / k().hi) * 100) : 100}%` }} />}
              </Index>
            </div>
            <div class="fx-axis cap">
              <span>first {clock(k().first)}</span>
              <span>last {clock(k().last)}</span>
            </div>
          </div>
        )}
      </Show>

      <div class="lx-insp-acts">
        <button class="btn btn-sm" onClick={props.onAround}>Lines around this</button>
        <button class="btn btn-sm" onClick={props.onTarget}>Only {shortTarget(props.line.target)}</button>
        <button class="btn btn-sm" onClick={copy}>{copied() ? 'Copied' : 'Copy JSON'}</button>
      </div>
    </aside>
  );
}

/* ------------------------------------------------------------------ */
/* icons                                                               */
/* ------------------------------------------------------------------ */

function XIcon() {
  return (
    <svg width="10" height="10" viewBox="0 0 10 10" stroke="currentColor" stroke-width="1.3" aria-hidden="true">
      <path d="M2 2l6 6M8 2l-6 6" />
    </svg>
  );
}

function ZoomIcon(props: { plus?: boolean }) {
  return (
    <svg width="13" height="13" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.3" aria-hidden="true">
      <circle cx="6" cy="6" r="4.5" />
      <path d="M9.5 9.5L13 13M4 6h4" />
      <Show when={props.plus}>
        <path d="M6 4v4" />
      </Show>
    </svg>
  );
}
