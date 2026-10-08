import { For, Show, createEffect, createMemo, createSignal, on } from 'solid-js';
import { useDevTools } from '../../context/DevToolsContext';
import { formatTime } from '../../utils/formatters';
import { JsonView } from '../ui/JsonView';
import type { LogEntry, LogLevelName } from '../../types/devtools';

type Shown = 'trace' | 'debug' | 'info' | 'warn' | 'error';

const LEVEL_NAMES: LogLevelName[] = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'];
const SHOWN: Shown[] = ['trace', 'debug', 'info', 'warn', 'error'];

/** Pino's numeric level to its name; `error` covers `fatal` for filtering. */
const levelName = (level: number): LogLevelName =>
  level >= 60 ? 'fatal' : level >= 50 ? 'error' : level >= 40 ? 'warn' : level >= 30 ? 'info' : level >= 20 ? 'debug' : 'trace';
const shownBucket = (level: number): Shown => {
  const name = levelName(level);
  return name === 'fatal' ? 'error' : name;
};

interface Parsed {
  category?: string;
  /** Everything but the fields the row already shows. */
  extra?: Record<string, unknown>;
  /** The line did not parse (cut off at the page's cap): show it raw. */
  raw?: string;
}

// Lines are immutable once captured, so each is parsed at most once.
const parsedCache = new WeakMap<LogEntry, Parsed>();

function parse(entry: LogEntry): Parsed {
  const hit = parsedCache.get(entry);
  if (hit) return hit;
  let result: Parsed;
  try {
    const { time: _t, level: _l, msg: _m, Category, service, ...rest } = JSON.parse(entry.line);
    const category = typeof Category === 'string' ? Category : typeof service === 'string' ? service : undefined;
    result = { category, extra: Object.keys(rest).length ? rest : undefined };
  } catch {
    result = { raw: entry.line };
  }
  parsedCache.set(entry, result);
  return result;
}

/** `sp00ky-client::DevToolsService::init` reads as `DevToolsService::init`. */
const shortCategory = (c: string) => c.replace(/^sp00ky-client::/, '');

export function LogsTab() {
  const { state, isSp00kyAvailable, logEntries, logsDropped, logsError, logsLoaded, fetchLogs, setLogCaptureLevel } =
    useDevTools();
  const [filter, setFilter] = createSignal('');
  const [levels, setLevels] = createSignal<Set<Shown>>(new Set(SHOWN));
  const [category, setCategory] = createSignal('');
  const [expanded, setExpanded] = createSignal<Set<number>>(new Set());
  const [follow, setFollow] = createSignal(true);
  let listEl: HTMLDivElement | undefined;

  // The backlog from before the panel attached: pulled once per page.
  createEffect(() => {
    if (isSp00kyAvailable() && !logsLoaded()) void fetchLogs();
  });

  const categories = createMemo(() => {
    const seen = new Set<string>();
    for (const e of logEntries()) {
      const c = parse(e).category;
      if (c) seen.add(c);
    }
    return [...seen].toSorted();
  });

  const visible = createMemo(() => {
    const shown = levels();
    const cat = category();
    const term = filter().trim().toLowerCase();
    return logEntries().filter((e) => {
      if (!shown.has(shownBucket(e.level))) return false;
      if (cat && parse(e).category !== cat) return false;
      return !term || e.line.toLowerCase().includes(term);
    });
  });

  const counts = createMemo(() => {
    const c: Record<Shown, number> = { trace: 0, debug: 0, info: 0, warn: 0, error: 0 };
    for (const e of logEntries()) c[shownBucket(e.level)]++;
    return c;
  });

  const toggleLevel = (l: Shown) =>
    setLevels((prev) => {
      const next = new Set(prev);
      if (next.has(l)) next.delete(l);
      else next.add(l);
      return next;
    });

  const toggleRow = (seq: number) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(seq)) next.delete(seq);
      else next.add(seq);
      return next;
    });

  // Stick to the newest line, like the console, until the user scrolls up.
  createEffect(
    on(visible, () => {
      if (follow() && listEl) queueMicrotask(() => listEl && (listEl.scrollTop = listEl.scrollHeight));
    })
  );
  const onScroll = () => {
    if (!listEl) return;
    setFollow(listEl.scrollHeight - listEl.scrollTop - listEl.clientHeight < 24);
  };

  const meta = () => state.logs;

  return (
    <div class="queries-container">
      <div class="mt-toolbar">
        <input
          class="dt-filter-input mt-filter"
          type="text"
          placeholder="Filter text"
          value={filter()}
          onInput={(e) => setFilter(e.currentTarget.value)}
        />
        <For each={SHOWN}>
          {(l) => (
            <button
              class={`filter-chip log-chip log-${l}`}
              classList={{ active: levels().has(l) }}
              onClick={() => toggleLevel(l)}
            >
              {l}
              <span class="chip-count">{counts()[l]}</span>
            </button>
          )}
        </For>
        <select
          class="db-source-select"
          value={category()}
          title="Only lines from this category / service"
          onChange={(e) => setCategory(e.currentTarget.value)}
        >
          <option value="">All categories</option>
          <For each={categories()}>{(c) => <option value={c}>{shortCategory(c)}</option>}</For>
        </select>
        <span class="toolbar-spacer" />
        <Show when={meta()}>
          {(m) => (
            <label
              class="log-capture"
              title={`The console prints ${m().consoleLevel} and up (logLevel in the client config). This picks what the page records for this panel: lower it to see debug lines without flooding the console. It holds for this tab's session, so a reload records its boot at this level too.`}
            >
              Record
              <select
                class="db-source-select"
                value={m().captureLevel}
                onChange={(e) => void setLogCaptureLevel(e.currentTarget.value as LogLevelName)}
              >
                <For each={LEVEL_NAMES}>{(l) => <option value={l}>{l}+</option>}</For>
              </select>
            </label>
          )}
        </Show>
      </div>

      <Show when={logsError()}>
        <div class="log-notice error">Could not read the page's logs: {logsError()}</div>
      </Show>
      <Show when={logsDropped() > 0}>
        <div class="log-notice">
          {logsDropped()} older lines were overwritten in the page's buffer before they were read.
        </div>
      </Show>

      <div class="log-list" ref={(el) => (listEl = el)} onScroll={onScroll}>
        <Show
          when={visible().length > 0}
          fallback={
            <div class="empty-state">
              {!isSp00kyAvailable()
                ? 'Waiting for Sp00ky…'
                : !meta() && logsError()
                  ? 'This client does not capture its logs. Update @spooky-sync/core to see them here.'
                  : logEntries().length > 0
                    ? 'No lines match the filters'
                    : 'Nothing logged yet at this level.'}
            </div>
          }
        >
          <For each={visible()}>
            {(e) => {
              const p = parse(e);
              const open = () => expanded().has(e.seq);
              return (
                <div class={`log-row log-${shownBucket(e.level)}`} classList={{ open: open() }}>
                  <div class="log-line" onClick={() => toggleRow(e.seq)}>
                    <span class="log-caret">{p.extra || p.raw ? (open() ? '▾' : '▸') : ''}</span>
                    <span class="log-time">{formatTime(e.time)}</span>
                    <span class="log-level">{levelName(e.level)}</span>
                    <Show when={p.category}>
                      {(c) => (
                        <span class="log-category" title={c()}>
                          {shortCategory(c())}
                        </span>
                      )}
                    </Show>
                    <span class="log-msg">{e.msg || (p.raw ? '(line cut off)' : '')}</span>
                  </div>
                  <Show when={open() && (p.extra || p.raw)}>
                    <Show when={p.extra} fallback={<pre class="code-block log-raw">{p.raw}</pre>}>
                      <JsonView class="code-block log-extra" value={p.extra} />
                    </Show>
                  </Show>
                </div>
              );
            }}
          </For>
        </Show>
      </div>

      <div class="queries-statusbar">
        <span>
          {visible().length === logEntries().length
            ? `${logEntries().length} lines`
            : `${visible().length} / ${logEntries().length} lines`}
        </span>
        <Show when={meta()}>
          {(m) => (
            <>
              <span class="statusbar-sep" />
              <span>
                recording {m().captureLevel}+, console prints {m().consoleLevel}+
              </span>
            </>
          )}
        </Show>
        <Show when={!follow()}>
          <span class="statusbar-sep" />
          <button
            class="btn log-follow"
            onClick={() => {
              setFollow(true);
              if (listEl) listEl.scrollTop = listEl.scrollHeight;
            }}
          >
            ↓ Jump to newest
          </button>
        </Show>
      </div>
    </div>
  );
}
