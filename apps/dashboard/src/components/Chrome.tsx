import { For, Show, type JSX } from 'solid-js';
import { A } from '@solidjs/router';

/** Small shared pieces of the panel chrome. */

export function PageHead(props: {
  crumb?: string;
  title: string;
  subtitle?: JSX.Element;
  actions?: JSX.Element;
}) {
  return (
    <div class="page-head">
      <div style={{ 'min-width': '0' }}>
        <Show when={props.crumb}>
          <div class="crumb">{props.crumb}</div>
        </Show>
        <h1>{props.title}</h1>
        <Show when={props.subtitle}>
          <div class="id" style={{ 'margin-top': '5px' }}>
            {props.subtitle}
          </div>
        </Show>
      </div>
      <Show when={props.actions}>
        <div class="row">{props.actions}</div>
      </Show>
    </div>
  );
}

export function StatusDot(props: { tone: string; pulse?: boolean }) {
  return (
    <span
      class="dot"
      classList={{ [props.tone]: true, pulse: !!props.pulse }}
    />
  );
}

export function Pill(props: {
  tone?: string;
  dot?: boolean;
  pulse?: boolean;
  children: JSX.Element;
}) {
  return (
    <span class="pill" classList={{ [props.tone ?? '']: !!props.tone }}>
      <Show when={props.dot}>
        <StatusDot tone={props.tone ?? 'idle'} pulse={props.pulse} />
      </Show>
      {props.children}
    </span>
  );
}

/** One readout in a metric rail. */
export function Cell(props: {
  label: string;
  value: JSX.Element;
  unit?: string;
  foot?: JSX.Element;
  tone?: string;
  pulse?: boolean;
}) {
  return (
    <div class="rail-cell">
      <div class="rail-label">
        <Show when={props.tone}>
          <StatusDot tone={props.tone!} pulse={props.pulse} />
        </Show>
        {props.label}
      </div>
      <div class="rail-value">
        {props.value}
        <Show when={props.unit}>
          <span class="rail-unit">{props.unit}</span>
        </Show>
      </div>
      <Show when={props.foot}>
        <div class="rail-foot">{props.foot}</div>
      </Show>
    </div>
  );
}

export function Rail(props: { children: JSX.Element }) {
  return <div class="rail">{props.children}</div>;
}

export function Panel(props: {
  title?: string;
  sub?: JSX.Element;
  actions?: JSX.Element;
  flush?: boolean;
  children: JSX.Element;
}) {
  return (
    <div class="panel">
      <Show when={props.title || props.actions}>
        <div class="panel-head">
          <div>
            <Show when={props.title}>
              <h2>{props.title}</h2>
            </Show>
            <Show when={props.sub}>
              <div class="panel-sub">{props.sub}</div>
            </Show>
          </div>
          <Show when={props.actions}>
            <div class="row">{props.actions}</div>
          </Show>
        </div>
      </Show>
      {props.flush ? props.children : <div class="panel-body">{props.children}</div>}
    </div>
  );
}

export function Empty(props: { children: JSX.Element }) {
  return <div class="empty">{props.children}</div>;
}

/** An identifier with a copy affordance — ids here are long and get pasted. */
export function CopyId(props: { value: string }) {
  return (
    <span class="row" style={{ gap: '0' }}>
      <span class="id">{props.value}</span>
      <button
        class="copy"
        title="Copy"
        onClick={(e) => {
          e.stopPropagation();
          void navigator.clipboard?.writeText(props.value);
          const el = e.currentTarget;
          const original = el.textContent;
          el.textContent = 'copied';
          setTimeout(() => (el.textContent = original), 1200);
        }}
      >
        copy
      </button>
    </span>
  );
}

export function KeyValue(props: { rows: [string, JSX.Element][] }) {
  return (
    <dl class="kv">
      {props.rows.map(([k, v]) => (
        <>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </>
      ))}
    </dl>
  );
}

/* ------------------------------------------------------------------ */
/* Recorded causes                                                       */
/* ------------------------------------------------------------------ */

/** Keys `Reason` renders itself; everything else falls through to the detail. */
const REASON_KEYS = ['code', 'reason', 'message', 'error'];

/**
 * One recorded cause — a `{ code, reason, … }` object as the engine writes it.
 *
 * Rendered as a sentence rather than as JSON because that is how it was written:
 * `schedule-core` puts a stable `code` and a human sentence on every failure,
 * skip and kill it records, precisely so a reader does not have to interpret a
 * payload. The remaining keys are the actionable part (which run holds a key,
 * which step failed), so they are shown as pairs, with the raw object one click
 * away for the shapes this does not anticipate.
 *
 * `tone` defaults to bad. A skip is not a failure, so callers pass 'warn'.
 */
export function Reason(props: {
  error: unknown;
  tone?: string;
  /** Render ids as links (the dashboard knows how to route some of them). */
  link?: (key: string, value: string) => JSX.Element | null;
}) {
  const obj = () =>
    props.error && typeof props.error === 'object' && !Array.isArray(props.error)
      ? (props.error as Record<string, unknown>)
      : null;

  // A bare string is a legitimate shape: `_00_schedule.last_error` is one.
  const text = () => {
    const o = obj();
    if (!o) return typeof props.error === 'string' ? props.error : null;
    for (const key of ['reason', 'message', 'error']) {
      const v = o[key];
      if (typeof v === 'string' && v) return v;
    }
    return null;
  };

  const code = () => {
    const c = obj()?.code;
    return typeof c === 'string' || typeof c === 'number' ? String(c) : null;
  };

  const extras = () =>
    Object.entries(obj() ?? {}).filter(
      ([k, v]) =>
        !REASON_KEYS.includes(k) &&
        v !== null &&
        v !== undefined &&
        // A value the sentence already quotes — a timestamp, a step name — is
        // not worth a second line under it. The writer of the cause decides
        // what belongs in prose; this only avoids echoing the decision.
        !(typeof v !== 'object' && (text() ?? '').includes(String(v))),
    );

  /**
   * One extra value. A list of step names is the common case and reads as prose,
   * not as a payload, so only genuinely nested values fall back to JSON.
   */
  const show = (key: string, value: unknown): JSX.Element => {
    if (typeof value === 'string') return props.link?.(key, value) ?? value;
    if (Array.isArray(value) && value.every((v) => typeof v !== 'object')) {
      return value.join(', ');
    }
    return typeof value === 'object' ? JSON.stringify(value) : String(value);
  };

  return (
    <Show when={props.error} fallback={null}>
      <div class="reason" classList={{ [props.tone ?? 'bad']: true }}>
        <div class="reason-head">
          <Show when={code()}>
            <span class="reason-code">{code()}</span>
          </Show>
          <span class="reason-text">{text() ?? 'No reason recorded.'}</span>
        </div>
        <Show when={extras().length > 0}>
          <dl class="reason-kv">
            <For each={extras()}>
              {([key, value]) => (
                <>
                  <dt>{key.replace(/_/g, ' ')}</dt>
                  <dd>{show(key, value)}</dd>
                </>
              )}
            </For>
          </dl>
        </Show>
      </div>
    </Show>
  );
}

/**
 * The one-line form, for a table cell. Same source, same precedence — a row in a
 * list should not disagree with the page it links to.
 */
export function ReasonLine(props: { error: unknown; tone?: string }) {
  const o = () =>
    props.error && typeof props.error === 'object'
      ? (props.error as Record<string, unknown>)
      : null;
  const text = () => {
    const obj = o();
    if (!obj) return typeof props.error === 'string' ? props.error : null;
    for (const key of ['reason', 'message', 'error']) {
      const v = obj[key];
      if (typeof v === 'string' && v) return v;
    }
    const code = obj.code;
    return typeof code === 'string' ? code : null;
  };
  return (
    <Show when={text()}>
      {(line) => (
        <span class="reason-line" classList={{ [props.tone ?? 'bad']: true }} title={line()}>
          {line()}
        </span>
      )}
    </Show>
  );
}

/* ------------------------------------------------------------------ */
/* Brand                                                                 */
/* ------------------------------------------------------------------ */

/**
 * The Sp00ky wordmark, inlined from `apps/landing-page/public/logo.svg`.
 *
 * Inline rather than an `<img>` so it draws in `currentColor`: the same
 * component sits on the dark frame in the sidebar and on the mobile bar.
 */
export function Logo() {
  return (
    <span class="brand-logo" aria-label="Sp00ky">
        <svg viewBox="0 0 150 45.4" height="16" width="53" fill="currentColor" aria-hidden="true">
          <path d="m62.9 3.5c-6.8 0-12.3 4.9-12.3 17 0 7.8 2.5 15.7 12.3 15.7 7.8 0 11.4-5.1 11.4-15.7 0-8.5-2.2-17-11.4-17zm8.9 18.2c-1.8-0.9-3.7-0.1-4.1 2.4-0.5 3.1-1.1 7-4.9 7-3 0-4.2-2.7-4.7-6.7-0.2-1.9-1.9-4.2-4.8-2.7 0.8-2.2 2.3-3.5 4.4-4.4 0.3-3.2 1.3-8.1 5-8.1 2.8 0 4.6 2.8 5.1 8.1 1.7 0.6 3.3 2.2 4 3.8l0.2 0.5-0.2 0.1z" />
          <path d="m89 3.5c-6.8 0-12.3 4.9-12.3 17 0.4 8.4 2.9 15.6 12.4 15.6 7.8 0 11.9-4.8 11.9-15.6-0.2-8-2.4-17-12-17zm9.5 18.2c-1.8-0.9-4.2-0.3-4.7 2.6-0.4 3.1-1.3 6.7-5 6.8-3.4 0-4.8-2.8-5.3-7-0.3-2-2.2-3.7-4.9-2.4 0.7-1.9 2.3-3.5 4.5-4.4 0.3-3.2 1.5-8 5.8-8 2.8 0 4.8 2.7 5.3 8 1.9 0.7 3.6 2.1 4.4 4.4h-0.1z" />
          <path d="m16.2 19.3 3.3-4.7c-2.8-2.3-6.4-3.6-11.6-2.7-2.8 0.5-4.1 1.7-5.2 3.9-0.8 1.5-1.8 6.6 2.7 9 3.8 2.1 8.6 2.5 8.6 4.5 0 1.1-1.4 2-3.7 1.9-2.2-0.1-4.3-1.2-5.8-3.4l-3.2 4.4c2.1 2.1 5.2 3.9 9.8 3.9 5.9 0 9.5-2.6 9.4-7.6-0.4-4.4-4.1-6.1-9.1-7.5-2-0.5-3.9-1.1-3.9-2.7 0-0.9 1.6-1.8 3.2-1.8 2.2 0 3.9 0.9 5.5 2.8z" />
          <path d="m37.4 11.5c-3.1 0-5.4 1.2-7.4 3.8l-0.6-3.2h-7c0.3 1.3 0.7 4 0.7 5.2l-0.1 27 6.8-2.1 0.1-9.3c1.9 1.7 3.8 3.2 7.5 3.2 5.3-0.1 10.5-2.6 10.8-11.3 0.2-6-3.1-13.3-10.8-13.3zm-1.5 19.3c-3.1 0-5.9-2.1-6.2-6.1-0.2-4.6 2.5-7.7 6.2-7.7 2.8 0 5.4 2.1 5.4 6.6 0.1 4-1.6 7-5.4 7.2z" />
          <path d="m118.2 12.1c0.2 3.6-2.5 7.2-7.1 9v-21.1l-7.2 2.1v33.4h7.1l0.1-8.6 1.9-0.9 2.4 2.8c0.9 1 2.1 3.7 3.5 6.7h8.6l-9.5-12.4c3.8-3 7.1-6.2 7.5-11h-7.8z" />
          <path d="m143.7 12.1c-1.7 3.8-2.2 7.1-4.5 15l-4.8-13.2-0.1-0.3-0.1-0.4 0.1 0.4-0.1-0.4-0.1-0.2v-0.1l-0.2-0.8h-6.7l9.1 21.9c-1.8 3.7-2.9 5.3-6 4.2l-0.7-0.2-3.1 4.9c1.3 0.7 3.1 1.2 5.3 1.2 5.1-0.1 7.8-3.7 9.6-8.2l8.6-24c-2.1-0.1-4 0.2-6.3 0.2z" />
        </svg>
    </span>
  );
}

/* ------------------------------------------------------------------ */
/* Bento                                                                 */
/* ------------------------------------------------------------------ */

export function Bento(props: { children: JSX.Element }) {
  return <div class="bento">{props.children}</div>;
}

/**
 * One tile of the bento.
 *
 * `span`/`rows` place it on the 12-column grid; `i` is its index in reading
 * order and only drives the staggered entrance. `tone` puts a status dot in
 * the label and, for `warn`/`bad`, a glow in the corner: the point is that a
 * wrong tile is the first thing the eye lands on. `to` links to the page the
 * tile summarises.
 */
export function Tile(props: {
  i?: number;
  span?: 3 | 4 | 6 | 8 | 9 | 12;
  rows?: 1 | 2;
  hero?: boolean;
  /** No body padding; the head keeps its own. For a table. */
  flush?: boolean;
  label?: string;
  /** The label is an identifier: keep its case and set it in mono. */
  raw?: boolean;
  sub?: JSX.Element;
  tone?: string;
  pulse?: boolean;
  actions?: JSX.Element;
  to?: string;
  class?: string;
  children: JSX.Element;
}) {
  return (
    <section
      class="tile"
      classList={{
        [`span-${props.span ?? 12}`]: true,
        'rows-2': props.rows === 2,
        hero: !!props.hero,
        flush: !!props.flush,
        [props.tone ?? '']: !!props.tone,
        [props.class ?? '']: !!props.class,
      }}
      style={{ '--i': String(props.i ?? 0) }}
    >
      <Show when={props.label || props.actions}>
        <header class="tile-head">
          <div style={{ 'min-width': '0' }}>
            <Show when={props.label}>
              <div class="tile-label" classList={{ raw: !!props.raw }}>
                <Show when={props.tone}>
                  <StatusDot tone={props.tone!} pulse={props.pulse} />
                </Show>
                {props.label}
              </div>
            </Show>
            <Show when={props.sub}>
              <div class="tile-sub">{props.sub}</div>
            </Show>
          </div>
          <Show when={props.actions || props.to}>
            <div class="row tile-actions">
              {props.actions}
              <Show when={props.to}>
                <A href={props.to!} class="tile-go">
                  open
                </A>
              </Show>
            </div>
          </Show>
        </header>
      </Show>
      {props.children}
    </section>
  );
}

/** The number in a tile. */
export function Readout(props: { value: JSX.Element; unit?: string }) {
  return (
    <div class="readout">
      <span class="readout-value">{props.value}</span>
      <Show when={props.unit}>
        <span class="readout-unit">{props.unit}</span>
      </Show>
    </div>
  );
}

/**
 * One segment per member of a fleet, coloured by status. With eight backends
 * it reads as a bar of eight; with one SSP, as a single indicator. An empty
 * fleet is one idle segment, so the row does not collapse.
 */
export function Segments(props: {
  items: { id: string; tone: string; title?: string }[];
}) {
  const items = () =>
    props.items.length ? props.items : [{ id: 'none', tone: 'idle', title: 'none' }];
  return (
    <div
      class="segs"
      style={{ 'grid-template-columns': `repeat(${items().length}, minmax(0, 1fr))` }}
      role="img"
      aria-label={items()
        .map((it) => `${it.id} ${it.tone}`)
        .join(', ')}
    >
      <For each={items()}>
        {(it) => (
          <span class="seg" classList={{ [it.tone]: true }} title={it.title ?? it.id} />
        )}
      </For>
    </div>
  );
}

/** The overview's own shape while the first poll is in flight. */
export function SkeletonBento() {
  const shape: { span: 3 | 4 | 6 | 8 | 12; rows?: 2 }[] = [
    { span: 6, rows: 2 },
    { span: 3 },
    { span: 3 },
    { span: 3 },
    { span: 3 },
    { span: 3 },
    { span: 3 },
    { span: 3 },
    { span: 3 },
    { span: 8 },
    { span: 4 },
  ];
  return (
    <Bento>
      <For each={shape}>
        {(t, i) => (
          <div
            class="tile skeleton"
            classList={{ [`span-${t.span}`]: true, 'rows-2': t.rows === 2 }}
            style={{ '--i': String(i()) }}
            aria-hidden="true"
          />
        )}
      </For>
    </Bento>
  );
}
