import { For, Show, createEffect, createSignal, onCleanup } from 'solid-js';
import { useSearchParams } from '@solidjs/router';
import { openStream } from '../api/client';
import { Empty, PageHead, StatusDot } from '../components/Chrome';
import { formatClock, formatCount } from '../lib/format';
import type { LogLine, Overview } from '../api/types';

/** Above this many lines the oldest are dropped, so a long tail cannot grow
 *  without bound in the tab's memory. */
const MAX_LINES = 5000;

type Entry =
  | { kind: 'line'; line: LogLine; seq: number }
  | { kind: 'dropped'; count: number; seq: number };

/**
 * Live log tail for the scheduler or any SSP.
 *
 * Backends are deliberately absent from the source list: the scheduler reaches
 * them with HTTP health checks and has no pipe to their output, so offering
 * them here would be offering something that cannot work.
 */
export function Logs(props: { overview: Overview | undefined }) {
  const [params, setParams] = useSearchParams<{ source?: string }>();
  const [entries, setEntries] = createSignal<Entry[]>([]);
  const [connected, setConnected] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [follow, setFollow] = createSignal(true);
  const [levelFilter, setLevelFilter] = createSignal('');
  const [text, setText] = createSignal('');

  let viewport: HTMLDivElement | undefined;
  let seq = 0;

  const source = () => params.source || 'scheduler';

  const sources = () => [
    { value: 'scheduler', label: 'Scheduler' },
    ...(props.overview?.ssps ?? []).map((s) => ({
      value: `ssp:${s.id}`,
      label: `SSP · ${s.id}`,
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
    seq = 0;

    const close = openStream(
      `/logs?source=${encodeURIComponent(src)}&tail=true`,
      {
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
      },
    );
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

  const visible = () => {
    const lvl = levelFilter();
    const q = text().trim().toLowerCase();
    return entries().filter((e) => {
      if (e.kind === 'dropped') return true;
      if (lvl && e.line.level !== lvl) return false;
      if (!q) return true;
      return (
        e.line.message.toLowerCase().includes(q) ||
        e.line.target.toLowerCase().includes(q) ||
        (e.line.fields ?? '').toLowerCase().includes(q)
      );
    });
  };

  // Following means "stick to the bottom". Scrolling up turns it off, which is
  // what a reader inspecting older lines expects.
  const onScroll = () => {
    if (!viewport) return;
    const atBottom =
      viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight < 40;
    if (atBottom !== follow()) setFollow(atBottom);
  };

  const jumpToEnd = () => {
    setFollow(true);
    if (viewport) viewport.scrollTop = viewport.scrollHeight;
  };

  return (
    <>
      <PageHead
        title="Logs"
        actions={
          <div class="lg-controls">
            <select
              class="lg-source"
              aria-label="Source"
              title="Scheduler and SSPs only: backends are health-checked over HTTP, their output never reaches the scheduler."
              value={source()}
              onChange={(e) => setParams({ source: e.currentTarget.value })}
            >
              <For each={sources()}>
                {(s) => <option value={s.value}>{s.label}</option>}
              </For>
            </select>
            <select
              class="lg-level"
              aria-label="Level"
              value={levelFilter()}
              onChange={(e) => setLevelFilter(e.currentTarget.value)}
            >
              <option value="">All levels</option>
              <option value="ERROR">Error</option>
              <option value="WARN">Warn</option>
              <option value="INFO">Info</option>
              <option value="DEBUG">Debug</option>
              <option value="TRACE">Trace</option>
            </select>
            <input
              class="lg-search"
              type="search"
              aria-label="Search"
              placeholder="Search"
              value={text()}
              onInput={(e) => setText(e.currentTarget.value)}
            />
          </div>
        }
      />

      <div class="page-body lg-page">
        <Show when={error()}>
          <div class="banner">
            <span class="dot bad" />
            {error()}
          </div>
        </Show>

        <div class="lg-frame">
          <div class="lg-bar">
            <span class="lg-status" classList={{ on: connected() }}>
              <StatusDot tone={connected() ? 'ok' : 'idle'} pulse={connected()} />
              {connected() ? 'Streaming' : 'Connecting'}
            </span>
            <span class="lg-count">
              {formatCount(visible().filter((e) => e.kind === 'line').length)} lines
            </span>
          </div>

          <div class="lg-view" ref={viewport} onScroll={onScroll}>
            <Show
              when={visible().length > 0}
              fallback={
                <Empty>
                  {connected() ? 'No lines match.' : 'Waiting for the stream.'}
                </Empty>
              }
            >
              <For each={visible()}>
                {(entry) =>
                  entry.kind === 'dropped' ? (
                    <div class="lg-dropped">
                      {formatCount(entry.count)} lines dropped, this viewer fell behind
                    </div>
                  ) : (
                    <div class={`lg-line ${entry.line.level}`}>
                      <span class="lg-ts">{formatClock(entry.line.ts)}</span>
                      <span class="lg-lvl">{entry.line.level}</span>
                      <span class="lg-msg">
                        <span class="lg-target">{entry.line.target}</span>
                        {entry.line.message}
                        <Show when={entry.line.fields}>
                          <span class="lg-fields"> {entry.line.fields}</span>
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
            <button class="btn btn-sm lg-jump" onClick={jumpToEnd}>
              Jump to latest
            </button>
          </Show>
        </div>
      </div>
    </>
  );
}
