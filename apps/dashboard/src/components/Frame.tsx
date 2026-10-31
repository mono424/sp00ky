import { For, Index, Show, createEffect, createMemo, createResource, createSignal, onCleanup, type JSX } from 'solid-js';
import { Portal } from 'solid-js/web';
import { api } from '../api/client';
import type { CloudService, CloudStats, IncidentBrief } from '../api/types';

/**
 * The frame's building blocks: the pieces every screen is drawn from. The
 * matching styles are the `fx-` rules in styles/frame.css.
 */

/** A section of a row: head (label, meta on the right), then its content. */
export function FxCell(props: {
  label?: JSX.Element;
  meta?: JSX.Element;
  class?: string;
  style?: JSX.CSSProperties;
  children: JSX.Element;
}) {
  return (
    <div class={`fx-cell ${props.class ?? ''}`} style={props.style}>
      <Show when={props.label || props.meta}>
        <div class="fx-head">
          <Show when={props.label}>
            <span class="cap">{props.label}</span>
          </Show>
          <Show when={props.meta}>
            <span class="fx-meta cap">{props.meta}</span>
          </Show>
        </div>
      </Show>
      {props.children}
    </div>
  );
}

/** Status as a square: grey when fine, amber or red when not. */
export function Sq(props: { tone?: string }) {
  const t = () => (props.tone === 'ok' || !props.tone ? '' : props.tone);
  return <span class={`fx-sq ${t()}`} />;
}

/** Usage in segments; amber once it reaches 80%. `pct` is 0..100, null when unknown. */
export function Seg(props: { pct: number | null | undefined; wide?: boolean }) {
  const p = () => (props.pct == null ? 0 : Math.max(0, Math.min(100, props.pct)));
  return (
    <span
      class="fx-seg"
      classList={{ wide: props.wide, hot: p() >= 80 }}
      style={{ '--p': p() }}
      aria-hidden="true"
    >
      <i />
    </span>
  );
}

/**
 * The last 24 hours of one component, one block an hour: amber or red for an
 * hour an incident of it was open in, hollow for hours before it existed.
 */
export function DayTicks(props: {
  component: string;
  incidents: IncidentBrief[] | undefined;
  /** Seconds this component has been up; hours before that are hollow. */
  uptimeSecs?: number | null;
  now: number;
}) {
  const hours = createMemo(() => {
    const out: string[] = Array.from({ length: 24 }, () => '');
    const start = props.now - 24 * 3_600_000;
    for (const inc of props.incidents ?? []) {
      if (inc.component !== props.component) continue;
      const from = Math.max(inc.started_at, start);
      const to = Math.min(inc.ended_at ?? props.now, props.now);
      if (to < start) continue;
      if (inc.severity === 'info') continue;
      const tone = inc.severity === 'critical' || inc.severity === 'error' ? 'bad' : 'warn';
      for (let h = Math.floor((from - start) / 3_600_000); h <= Math.min(23, Math.floor((to - start) / 3_600_000)); h++) {
        if (out[h] !== 'bad') out[h] = tone;
      }
    }
    const up = props.uptimeSecs;
    if (up != null && up < 24 * 3600) {
      const born = 23 - Math.floor(up / 3600);
      for (let h = 0; h < born; h++) if (!out[h]) out[h] = 'off';
    }
    if (!out[23]) out[23] = 'now';
    return out;
  });
  return (
    <div class="fx-ticks" title="Last 24 hours, one block an hour">
      <Index each={hours()}>{(c) => <i class={c()} />}</Index>
    </div>
  );
}

/** A small area chart on a fixed 0..100 scale. */
export function Spark(props: { values: number[] }) {
  const path = createMemo(() => {
    const v = props.values;
    if (v.length < 2) return null;
    const W = 300;
    const H = 40;
    const pts = v.map((y, i) => [(i / (v.length - 1)) * W, H - (Math.max(0, Math.min(100, y)) / 100) * H]);
    const line = pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join(' ');
    return { line, area: `${line} L${W} ${H} L0 ${H} Z` };
  });
  return (
    <svg class="fx-spark" viewBox="0 0 300 40" preserveAspectRatio="none" aria-hidden="true">
      <Show when={path()}>
        {(p) => (
          <>
            <path d={p().area} fill="rgba(156,165,211,0.12)" />
            <path d={p().line} fill="none" stroke="#9ca5d3" stroke-width="1.2" vector-effect="non-scaling-stroke" />
          </>
        )}
      </Show>
    </svg>
  );
}

/** One gauge: label and limit, the value, its share as segments, the recent trend. */
export function Gauge(props: {
  label: string;
  of?: string;
  value: string | null;
  unit?: string;
  pct: number | null;
  series?: number[];
  /** In place of the trend, for a figure that has none. */
  note?: JSX.Element;
}) {
  return (
    <div class="fx-gauge">
      <div class="fx-g-top">
        <span class="cap">{props.label}</span>
        <Show when={props.of}>
          <span class="cap">of {props.of}</span>
        </Show>
      </div>
      <Show
        when={props.value != null}
        fallback={<div class="fx-gv none">—<small>not reported</small></div>}
      >
        <div class="fx-gv">
          {props.value}
          <Show when={props.unit}>
            <small>{props.unit}</small>
          </Show>
        </div>
      </Show>
      <Seg pct={props.pct} wide />
      <Show when={props.note} fallback={<Spark values={props.series ?? []} />}>
        <div class="fx-gnote cap">{props.note}</div>
      </Show>
    </div>
  );
}

const MIB = 1024 * 1024;

/** CPU and memory of one container, in the shapes the gauges and meters want. */
export function usage(s: CloudService | undefined) {
  if (!s) return null;
  const limit = s.sample?.mem_limit_bytes || s.memory_mb * MIB;
  const cores = Math.max(1, s.vcpus);
  return {
    cpuPct: s.sample ? s.sample.cpu_pct / cores : null,
    cpuSeries: s.cpu_series.map((v) => v / cores),
    memBytes: s.sample?.mem_bytes ?? null,
    memPct: s.sample ? (s.sample.mem_bytes / limit) * 100 : null,
    memSeries: s.mem_series.map((v) => (v / limit) * 100),
    memLimit: limit,
    vcpus: s.vcpus,
    diskWriteBps: s.sample?.disk_write_bps ?? null,
    // The room a container has is what it takes plus what is still free on
    // the disk it shares: a host filling up shows here as well.
    diskBytes: s.disk?.used_bytes ?? null,
    diskFree: s.disk?.free_bytes ?? null,
    diskRoom: s.disk ? s.disk.used_bytes + s.disk.free_bytes : null,
    diskPct: s.disk && s.disk.used_bytes + s.disk.free_bytes > 0
      ? (s.disk.used_bytes / (s.disk.used_bytes + s.disk.free_bytes)) * 100
      : null,
    diskAt: s.disk?.measured_at ?? null,
    since: s.since,
  };
}

/** Bytes in the unit a gauge reads best in: GB from one gigabyte, else MB. */
export function memText(bytes: number): { value: string; unit: string } {
  if (bytes >= 1024 * MIB) return { value: (bytes / (1024 * MIB)).toFixed(1), unit: 'GB' };
  return { value: String(Math.round(bytes / MIB)), unit: 'MB' };
}

/** A meter for a list row: segments and the figure beside it. */
export function Meter(props: { pct: number | null | undefined; children?: JSX.Element }) {
  return (
    <span class="fx-meter" classList={{ none: props.pct == null }}>
      <Seg pct={props.pct} />
      <b classList={{ warn: (props.pct ?? 0) >= 80 }}>{props.pct == null ? '—' : props.children}</b>
    </span>
  );
}

/** Running against a limit, one slot each. */
export function Slots(props: { used: number; limit: number }) {
  const n = () => Math.max(1, Math.min(12, props.limit));
  return (
    <span class="fx-slots" title={`${props.used} of ${props.limit} running`}>
      <Index each={Array.from({ length: n() })}>{(_, i) => <i classList={{ on: i < props.used }} />}</Index>
    </span>
  );
}

/** "-24h", "-6h", "-30m": how far back an axis point is. */
function ago(ms: number): string {
  if (ms <= 0) return 'now';
  const m = Math.round(ms / 60_000);
  if (m < 60) return `-${m}m`;
  const h = Math.round(m / 60);
  if (h < 48) return `-${h}h`;
  return `-${Math.round(h / 24)}d`;
}

/** Clean steps for a y axis: 1, 2 or 5 times a power of ten. */
function niceStep(max: number): number {
  const raw = max / 3;
  const pow = 10 ** Math.floor(Math.log10(raw || 1));
  const f = raw / pow;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * pow;
}

export interface LinePoint {
  ts: number;
  /** Null for a failed sample. */
  v: number | null;
}

/**
 * A line over its samples on the dotted canvas, with gridlines, a time axis,
 * the peak marked when it stands out, failed samples as red ticks on the
 * floor, and a crosshair that reads any sample under the pointer.
 */
export function LineChart(props: {
  points: LinePoint[];
  format: (v: number) => string;
  now: number;
}) {
  const W = 1000;
  const H = 150;
  const [hover, setHover] = createSignal<number | null>(null);

  const model = createMemo(() => {
    const pts = props.points;
    const vals = pts.map((p) => p.v).filter((v): v is number => v != null);
    if (pts.length < 2 || vals.length === 0) return null;
    const peak = Math.max(...vals);
    const step = niceStep(peak * 1.1);
    const top = Math.ceil((peak * 1.1) / step) * step;
    const t0 = pts[0].ts;
    const span = Math.max(1, pts[pts.length - 1].ts - t0);
    const x = (ts: number) => ((ts - t0) / span) * W;
    const y = (v: number) => H - (v / top) * H;
    let line = '';
    let pen = false;
    for (const p of pts) {
      if (p.v == null) {
        pen = false;
        continue;
      }
      line += `${pen ? 'L' : 'M'}${x(p.ts).toFixed(1)} ${y(p.v).toFixed(1)}`;
      pen = true;
    }
    const sorted = [...vals].sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];
    const peakIdx = pts.findIndex((p) => p.v === peak);
    const grid = [step, step * 2, step * 3].filter((g) => g < top);
    const axis = [0, 0.25, 0.5, 0.75, 1].map((f) => ago(props.now - (t0 + span * f)));
    return { pts, x, y, line, grid, axis, peakIdx, showPeak: peak > median * 2.5 && peak > 0 };
  });

  const onMove = (e: MouseEvent) => {
    const m = model();
    if (!m) return;
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const fx = ((e.clientX - r.left) / r.width) * W;
    let best = 0;
    for (let i = 1; i < m.pts.length; i++) if (Math.abs(m.x(m.pts[i].ts) - fx) < Math.abs(m.x(m.pts[best].ts) - fx)) best = i;
    setHover(best);
  };

  const clock = (ts: number) =>
    new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });

  return (
    <Show when={model()} fallback={<div class="fx-canvas fx-empty cap">No samples yet</div>}>
      {(m) => (
        <>
          <div class="fx-canvas" onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
            <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden="true">
              <For each={m().grid}>
                {(g) => (
                  <line x1="0" x2={W} y1={m().y(g)} y2={m().y(g)} stroke="rgba(255,255,255,0.06)" vector-effect="non-scaling-stroke" />
                )}
              </For>
              <For each={m().pts}>
                {(p, i) => (
                  <Show
                    when={p.v != null}
                    fallback={<line x1={m().x(p.ts)} x2={m().x(p.ts)} y1={H - 10} y2={H} stroke="var(--bad)" stroke-width="1.5" vector-effect="non-scaling-stroke" />}
                  >
                    <line
                      x1={m().x(p.ts)}
                      x2={m().x(p.ts)}
                      y1={m().y(p.v!)}
                      y2={H}
                      stroke={`rgba(156,165,211,${hover() != null && i() >= hover()! ? 0.28 : 0.1})`}
                      vector-effect="non-scaling-stroke"
                    />
                  </Show>
                )}
              </For>
              <path d={m().line} fill="none" stroke="#c6cbea" stroke-width="1.3" stroke-linejoin="round" vector-effect="non-scaling-stroke" />
            </svg>
            <For each={m().grid}>
              {(g) => (
                <span class="fx-ylab cap" style={{ top: `${(m().y(g) / H) * 100}%` }}>
                  {props.format(g)}
                </span>
              )}
            </For>
            <Show when={m().showPeak}>
              <span
                class="fx-mark warn"
                style={{ left: `${(m().x(m().pts[m().peakIdx].ts) / W) * 100}%`, top: `${(m().y(m().pts[m().peakIdx].v!) / H) * 100}%` }}
              />
            </Show>
            <Show when={hover() != null && m().pts[hover()!]}>
              {(p) => (
                <>
                  <span
                    style={{
                      position: 'absolute',
                      top: 0,
                      bottom: 0,
                      left: `${(m().x(p().ts) / W) * 100}%`,
                      'border-left': '1px dashed rgba(255,255,255,0.35)',
                      'pointer-events': 'none',
                    }}
                  />
                  <Show when={p().v != null}>
                    <span class="fx-mark" style={{ left: `${(m().x(p().ts) / W) * 100}%`, top: `${(m().y(p().v!) / H) * 100}%` }} />
                  </Show>
                  <div
                    class="fx-tip"
                    style={{ left: `${(m().x(p().ts) / W) * 100}%`, top: `${((p().v != null ? m().y(p().v!) : H) / H) * 100}%` }}
                  >
                    <span>{clock(p().ts)}</span>
                    {p().v != null ? props.format(p().v!) : 'failed'}
                  </div>
                </>
              )}
            </Show>
          </div>
          <div class="fx-axis cap">
            <For each={m().axis}>{(a) => <span>{a}</span>}</For>
          </div>
        </>
      )}
    </Show>
  );
}

/**
 * Samples bucketed into columns of seven pixels each, the newest column in the
 * accent: how many people were here, hour by hour.
 */
export function Matrix(props: { values: number[]; columns?: number }) {
  const cols = createMemo(() => {
    const v = props.values;
    if (v.length === 0) return [];
    const n = Math.min(props.columns ?? 24, v.length);
    const out: number[] = [];
    for (let c = 0; c < n; c++) {
      const from = Math.floor((c * v.length) / n);
      const to = Math.max(from + 1, Math.floor(((c + 1) * v.length) / n));
      out.push(Math.max(...v.slice(from, to)));
    }
    const hi = Math.max(1, ...out);
    return out.map((x) => (x <= 0 ? 0 : Math.max(1, Math.round((x / hi) * 7))));
  });
  return (
    <div class="fx-matrix" aria-hidden="true">
      <For each={cols()}>
        {(lit, c) => (
          <div classList={{ now: c() === cols().length - 1 }}>
            <Index each={Array.from({ length: 7 })}>{(_, r) => <i classList={{ on: 7 - r <= lit }} />}</Index>
          </div>
        )}
      </For>
    </div>
  );
}

/** One thin bar per sample; `warn` marks the samples where something failed. */
export function Barcode(props: { values: number[]; warn?: boolean[] }) {
  const hi = () => Math.max(1, ...props.values);
  return (
    <div class="fx-barcode" aria-hidden="true">
      <For each={props.values}>
        {(v, i) => (
          <i
            classList={{ warn: !!props.warn?.[i()], now: i() === props.values.length - 1 }}
            style={{ height: `${Math.max(4, (v / hi()) * 100)}%` }}
          />
        )}
      </For>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Filters: one button, a popover of fields, then their values           */
/* ------------------------------------------------------------------ */

export interface FilterOption {
  value: string;
  label: string;
  count?: number;
  /** Status tone for the option's square: '', 'warn', 'bad', 'idle'. */
  tone?: string;
}

export interface FilterField {
  key: string;
  label: string;
  options: FilterOption[];
  /** One value at a time: picking another replaces it. */
  single?: boolean;
}

export type FilterValue = Record<string, string[]>;

type MenuItem =
  | { type: 'field'; field: FilterField }
  | { type: 'opt'; field: FilterField; opt: FilterOption; inline?: boolean };

/**
 * A Linear-style filter menu.
 *
 * "Filter" opens a popover listing the fields. Typing searches fields and
 * every value at once; choosing a field shows its values, each a checkbox, so
 * several can be picked without the menu closing. Applied filters sit beside
 * the button as chips ("State is recovered, failed"), each removable, and
 * clicking one reopens its values.
 *
 * Keys: arrows move, Enter picks, Backspace on an empty search steps back to
 * the fields, Escape steps back and then closes.
 */
export function FilterMenu(props: {
  fields: FilterField[];
  value: FilterValue;
  onChange: (next: FilterValue) => void;
}) {
  const [open, setOpen] = createSignal(false);
  const [fieldKey, setFieldKey] = createSignal<string | null>(null);
  const [query, setQuery] = createSignal('');
  const [index, setIndex] = createSignal(0);
  let root: HTMLDivElement | undefined;
  let input: HTMLInputElement | undefined;

  const picked = (k: string) => props.value[k] ?? [];
  const field = () => props.fields.find((f) => f.key === fieldKey()) ?? null;
  const active = () => props.fields.filter((f) => picked(f.key).length > 0);

  const toggle = (k: string, v: string) => {
    const cur = picked(k);
    const single = props.fields.find((f) => f.key === k)?.single;
    const next = cur.includes(v) ? cur.filter((x) => x !== v) : single ? [v] : [...cur, v];
    props.onChange({ ...props.value, [k]: next });
  };
  const clear = (k: string) => props.onChange({ ...props.value, [k]: [] });

  const items = createMemo<MenuItem[]>(() => {
    const q = query().trim().toLowerCase();
    const f = field();
    if (f) return f.options.filter((o) => !q || o.label.toLowerCase().includes(q)).map((opt) => ({ type: 'opt', field: f, opt }));
    const fields = props.fields.filter((x) => x.options.length > 0);
    if (!q) return fields.map((x) => ({ type: 'field', field: x }));
    const out: MenuItem[] = [];
    for (const x of fields) if (x.label.toLowerCase().includes(q)) out.push({ type: 'field', field: x });
    for (const x of fields)
      for (const opt of x.options) if (opt.label.toLowerCase().includes(q)) out.push({ type: 'opt', field: x, opt, inline: true });
    return out;
  });

  createEffect(() => {
    items();
    setIndex((i) => Math.min(i, Math.max(0, items().length - 1)));
  });

  const show = (key: string | null = null) => {
    setFieldKey(key);
    setQuery('');
    setIndex(0);
    setOpen(true);
    queueMicrotask(() => input?.focus());
  };
  const close = () => {
    setOpen(false);
    setFieldKey(null);
    setQuery('');
  };

  const choose = (item: MenuItem | undefined) => {
    if (!item) return;
    if (item.type === 'field') {
      setFieldKey(item.field.key);
      setQuery('');
      setIndex(0);
      input?.focus();
    } else {
      toggle(item.field.key, item.opt.value);
      // Keep the keys working: a click would otherwise leave focus on the item.
      input?.focus();
    }
  };

  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setIndex((i) => Math.min(i + 1, items().length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      choose(items()[index()]);
    } else if (e.key === 'Backspace' && !query() && fieldKey()) {
      e.preventDefault();
      setFieldKey(null);
      setIndex(0);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      if (fieldKey()) {
        setFieldKey(null);
        setQuery('');
        setIndex(0);
      } else close();
    }
  };

  const onDoc = (e: MouseEvent) => {
    if (open() && root && !root.contains(e.target as Node)) close();
  };
  document.addEventListener('mousedown', onDoc);
  onCleanup(() => document.removeEventListener('mousedown', onDoc));

  /** "recovered, failed" for a few; "any of 4" for many. */
  const summary = (f: FilterField) => {
    const vals = picked(f.key);
    const names = vals.map((v) => f.options.find((o) => o.value === v)?.label ?? v);
    return names.length <= 2 ? names.join(', ') : `any of ${names.length}`;
  };

  return (
    <div class="fm" ref={root}>
      <button type="button" class="btn btn-sm fm-btn" classList={{ on: open() }} aria-haspopup="dialog" aria-expanded={open()} onClick={() => (open() ? close() : show())}>
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
          <path d="M4 6h16 M7 12h10 M10 18h4" />
        </svg>
        Filter
      </button>

      <For each={active()}>
        {(f) => (
          <span class="fm-chip">
            <button type="button" class="fm-chip-body" onClick={() => show(f.key)}>
              <span class="fm-chip-k">{f.label}</span>
              <span class="fm-chip-op">{picked(f.key).length > 1 ? 'is any of' : 'is'}</span>
              <span class="fm-chip-v">{summary(f)}</span>
            </button>
            <button type="button" class="fm-chip-x" aria-label={`Remove the ${f.label} filter`} onClick={() => clear(f.key)}>
              <svg width="10" height="10" viewBox="0 0 10 10" stroke="currentColor" stroke-width="1.4" aria-hidden="true">
                <path d="M2 2l6 6M8 2l-6 6" />
              </svg>
            </button>
          </span>
        )}
      </For>
      <Show when={active().length > 1}>
        <button type="button" class="fm-clear cap" onClick={() => props.onChange({})}>
          Clear
        </button>
      </Show>

      <Show when={open()}>
        <div class="fm-pop" role="dialog" aria-label="Filter" onKeyDown={onKey}>
          <div class="fm-search">
            <Show when={field()}>
              {(f) => (
                <button type="button" class="fm-back cap" onClick={() => { setFieldKey(null); setQuery(''); input?.focus(); }}>
                  {f().label}
                </button>
              )}
            </Show>
            <input
              ref={input}
              placeholder={field() ? `Filter by ${field()!.label.toLowerCase()}…` : 'Filter…'}
              value={query()}
              onInput={(e) => { setQuery(e.currentTarget.value); setIndex(0); }}
            />
          </div>
          <div class="fm-list" role="listbox" aria-multiselectable={!!field()}>
            <For each={items()} fallback={<div class="fm-empty cap">No match</div>}>
              {(item, i) => {
                const on = () => item.type === 'opt' && picked(item.field.key).includes(item.opt.value);
                return (
                  <button
                    type="button"
                    class="fm-item"
                    classList={{ hi: i() === index(), on: on() }}
                    role={item.type === 'opt' ? 'option' : undefined}
                    aria-selected={item.type === 'opt' ? on() : undefined}
                    tabIndex={-1}
                    onMouseDown={(e) => e.preventDefault()}
                    onMouseEnter={() => setIndex(i())}
                    onClick={() => choose(item)}
                  >
                    <Show
                      when={item.type === 'opt'}
                      fallback={
                        <>
                          <span class="fm-label">{item.field.label}</span>
                          <Show when={picked(item.field.key).length}>
                            <span class="fm-count">{picked(item.field.key).length} picked</span>
                          </Show>
                          <span class="fm-arrow" aria-hidden="true">›</span>
                        </>
                      }
                    >
                      <span class="fm-check" classList={{ radio: !!item.field.single }} aria-hidden="true" />
                      <Show when={(item as { inline?: boolean }).inline}>
                        <span class="fm-field cap">{item.field.label}</span>
                      </Show>
                      <Show when={(item as { opt: FilterOption }).opt.tone !== undefined}>
                        <Sq tone={(item as { opt: FilterOption }).opt.tone} />
                      </Show>
                      <span class="fm-label">{(item as { opt: FilterOption }).opt.label}</span>
                      <Show when={(item as { opt: FilterOption }).opt.count != null}>
                        <span class="fm-count">{(item as { opt: FilterOption }).opt.count}</span>
                      </Show>
                    </Show>
                  </button>
                );
              }}
            </For>
          </div>
          <div class="fm-foot cap">
            <span>↑↓ move</span>
            <span>↵ {items()[index()]?.type === 'opt' ? 'toggle' : 'open'}</span>
            <span>esc {field() ? 'back' : 'close'}</span>
          </div>
        </div>
      </Show>
    </div>
  );
}

/**
 * Container metrics from Sp00ky Cloud, polled while `linked` holds. Empty when
 * the project is not linked or the control plane has no metrics: callers show
 * "not reported" rather than zero.
 */
export function useCloudServices(linked: () => boolean | undefined, everyMs = 15_000) {
  const [res, { refetch }] = createResource(linked, (on) =>
    on ? api.getResult<CloudStats>('/cloud/stats') : Promise.resolve(null),
  );
  const timer = setInterval(() => {
    if (linked() && !document.hidden) void refetch();
  }, everyMs);
  onCleanup(() => clearInterval(timer));
  const services = createMemo(() => {
    const r = res();
    return r && r.ok && r.value.metrics_available ? r.value.services : [];
  });
  return {
    services,
    /** CPU and memory of the container with this name, if reported. */
    byName: (name: string) => usage(services().find((s) => s.name === name)),
    byRole: (role: string) => usage(services().find((s) => s.role === role)),
  };
}

/**
 * A sheet that slides in from the right over the page: for the internals of
 * one thing, read alongside the list it came from. Escape, the close button or
 * a click on the page behind it closes it.
 */
export function Sheet(props: {
  open: boolean;
  /** Room for a timeline: 760px rather than 520px. */
  wide?: boolean;
  title: JSX.Element;
  meta?: JSX.Element;
  onClose: () => void;
  children: JSX.Element;
}) {
  return (
    <Show when={props.open}>
      <Portal>
        <SheetPanel {...props} />
      </Portal>
    </Show>
  );
}

function SheetPanel(props: { wide?: boolean; title: JSX.Element; meta?: JSX.Element; onClose: () => void; children: JSX.Element }) {
  let panel: HTMLElement | undefined;
  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'Escape') props.onClose();
  };
  document.addEventListener('keydown', onKey);
  onCleanup(() => document.removeEventListener('keydown', onKey));
  queueMicrotask(() => panel?.focus());
  return (
    <div class="sheet-scrim" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <aside class="sheet" classList={{ wide: props.wide }} role="dialog" aria-modal="true" tabIndex={-1} ref={panel}>
        <header class="sheet-head">
          <span class="sheet-title">{props.title}</span>
          <Show when={props.meta}>
            <span class="cap sheet-meta">{props.meta}</span>
          </Show>
          <button class="sheet-x" aria-label="Close" onClick={props.onClose}>
            <svg width="12" height="12" viewBox="0 0 10 10" stroke="currentColor" stroke-width="1.3" aria-hidden="true">
              <path d="M2 2l6 6M8 2l-6 6" />
            </svg>
          </button>
        </header>
        <div class="sheet-body">{props.children}</div>
      </aside>
    </div>
  );
}

/** One section of a sheet: a label, then its content, divided by a hairline. */
export function SheetSection(props: { label: string; meta?: JSX.Element; children: JSX.Element }) {
  return (
    <section class="sheet-sec">
      <div class="fx-head">
        <span class="cap">{props.label}</span>
        <Show when={props.meta}>
          <span class="fx-meta cap">{props.meta}</span>
        </Show>
      </div>
      {props.children}
    </section>
  );
}

/**
 * Sort as a small popover in the filter menu's style: one button showing the
 * current order, a list to pick another. Arrows move, Enter picks, Escape closes.
 */
export function SortMenu<T extends string>(props: {
  options: { value: T; label: string }[];
  value: T;
  onChange: (v: T) => void;
}) {
  const [open, setOpen] = createSignal(false);
  const [index, setIndex] = createSignal(0);
  let root: HTMLDivElement | undefined;
  let list: HTMLDivElement | undefined;
  const current = () => props.options.find((o) => o.value === props.value);
  const show = () => {
    setIndex(Math.max(0, props.options.findIndex((o) => o.value === props.value)));
    setOpen(true);
    queueMicrotask(() => list?.focus());
  };
  const pick = (v: T) => {
    props.onChange(v);
    setOpen(false);
  };
  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setIndex((i) => Math.min(i + 1, props.options.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      pick(props.options[index()].value);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      setOpen(false);
    }
  };
  const onDoc = (e: MouseEvent) => {
    if (open() && root && !root.contains(e.target as Node)) setOpen(false);
  };
  document.addEventListener('mousedown', onDoc);
  onCleanup(() => document.removeEventListener('mousedown', onDoc));

  return (
    <div class="fm" ref={root}>
      <button type="button" class="btn btn-sm fm-btn" classList={{ on: open() }} aria-haspopup="listbox" aria-expanded={open()} onClick={() => (open() ? setOpen(false) : show())}>
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
          <path d="M8 4v16 M4 8l4-4 4 4 M16 20V4 M12 16l4 4 4-4" />
        </svg>
        Sort
        <span class="fm-sort-v">{current()?.label}</span>
      </button>
      <Show when={open()}>
        <div class="fm-pop fm-pop-sm" role="listbox" tabIndex={-1} ref={list} onKeyDown={onKey}>
          <div class="fm-list">
            <For each={props.options}>
              {(o, i) => (
                <button
                  type="button"
                  class="fm-item"
                  classList={{ hi: i() === index(), on: o.value === props.value }}
                  role="option"
                  aria-selected={o.value === props.value}
                  tabIndex={-1}
                  onMouseDown={(e) => e.preventDefault()}
                  onMouseEnter={() => setIndex(i())}
                  onClick={() => pick(o.value)}
                >
                  <span class="fm-check radio" aria-hidden="true" />
                  <span class="fm-label">{o.label}</span>
                </button>
              )}
            </For>
          </div>
        </div>
      </Show>
    </div>
  );
}
