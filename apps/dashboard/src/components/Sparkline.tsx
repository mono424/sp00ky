import { For, Show, createMemo, createUniqueId } from 'solid-js';
import { formatMs, splitValue } from '../lib/format';

/**
 * A latency sparkline: area + line, with failures drawn as gaps.
 *
 * Adapted from `apps/devtools/src/components/versions/HeartbeatSparkline.tsx`,
 * whose central decision is worth restating because it is easy to undo: a
 * failed probe has NO latency, so it must be a break in the line rather than a
 * bar of some height. Drawing failures full-height (the obvious thing) makes an
 * outage look like the slowest possible response instead of the absence of one.
 * The y-axis starts at zero for the same reason — height stays proportional to
 * actual time.
 *
 * Hand-rolled SVG on purpose: the dashboard ships no charting library, and this
 * is the only chart it needs.
 */

export interface Point {
  /** Epoch ms. */
  ts: number;
  /**
   * The plotted value. Latency in ms for the probe charts this was written
   * for; a plain count for the presence charts, which pass their own
   * `format`. Null when the sample represents a failure — drawn as a gap.
   */
  ms: number | null;
  ok: boolean;
}

/** Plot box in user units; rendered with preserveAspectRatio="none". */
const W = 240;
const H = 44;

export function Sparkline(props: {
  points: Point[];
  /** Accent colour override, e.g. a backend's status colour. */
  stroke?: string;
  height?: number;
  /** Hide the min / last / max readout under the plot. */
  bare?: boolean;
  /**
   * How to render the min / last / max readout. Defaults to milliseconds,
   * which is what every probe chart wants; a count chart passes
   * `formatCount` and reuses everything else unchanged.
   */
  format?: (value: number | null | undefined) => string;
  /** What the plot is of, for screen readers. */
  ariaLabel?: string;
  /**
   * Grow to fill a flex column instead of taking a fixed height. Used by the
   * bento tiles, whose height is set by the grid row rather than the chart.
   */
  fill?: boolean;
}) {
  const gradientId = createUniqueId();

  const stats = createMemo(() => {
    const pts = props.points;
    const oks = pts.filter((p) => p.ok && typeof p.ms === 'number');
    const values = oks.map((p) => p.ms as number);
    return {
      count: pts.length,
      max: values.length ? Math.max(...values) : 0,
      min: values.length ? Math.min(...values) : null,
      last: values.length ? values[values.length - 1]! : null,
      failures: pts.length - oks.length,
    };
  });

  /** x for index i, y for a latency. Peak is padded so the line never clips. */
  const scale = createMemo(() => {
    const { count, max } = stats();
    const span = Math.max(1, count - 1);
    const ceiling = max > 0 ? max * 1.15 : 1;
    return {
      x: (i: number) => (i / span) * W,
      y: (ms: number) => H - (ms / ceiling) * H,
    };
  });

  /**
   * Split into runs of consecutive successes. Each run is its own path, which
   * is what produces a visible gap wherever a probe failed.
   */
  const runs = createMemo(() => {
    const { x, y } = scale();
    const out: { line: string; area: string }[] = [];
    let current: { i: number; ms: number }[] = [];

    const flush = () => {
      if (current.length === 0) return;
      // A lone point has no line to draw; give it a hairline segment so it is
      // still visible rather than silently absent.
      const pts = current.map((p) => `${x(p.i).toFixed(2)},${y(p.ms).toFixed(2)}`);
      const line =
        current.length === 1
          ? `M ${pts[0]} L ${(x(current[0]!.i) + 0.6).toFixed(2)},${y(current[0]!.ms).toFixed(2)}`
          : `M ${pts.join(' L ')}`;
      const first = x(current[0]!.i).toFixed(2);
      const last = x(current[current.length - 1]!.i).toFixed(2);
      out.push({
        line,
        area: `M ${first},${H} L ${pts.join(' L ')} L ${last},${H} Z`,
      });
      current = [];
    };

    props.points.forEach((p, i) => {
      if (p.ok && typeof p.ms === 'number') current.push({ i, ms: p.ms });
      else flush();
    });
    flush();
    return out;
  });

  const failureMarks = createMemo(() => {
    const { x } = scale();
    return props.points
      .map((p, i) => ({ p, i }))
      .filter(({ p }) => !p.ok || typeof p.ms !== 'number')
      .map(({ i }) => x(i));
  });

  const colour = () => props.stroke ?? 'var(--accent)';
  const fmt = (v: number | null | undefined) => (props.format ?? formatMs)(v);

  return (
    <Show
      when={stats().count > 0}
      fallback={<div class="empty" style={{ padding: '18px 0' }}>No samples yet</div>}
    >
      <div classList={{ 'spark-fill': !!props.fill }}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          style={{
            width: '100%',
            height: props.fill ? undefined : `${props.height ?? H}px`,
            display: 'block',
          }}
          role="img"
          aria-label={`${props.ariaLabel ?? 'Latency'} over the last ${stats().count} samples`}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color={colour()} stop-opacity="0.12" />
              <stop offset="70%" stop-color={colour()} stop-opacity="0" />
            </linearGradient>
          </defs>

          <For each={runs()}>
            {(run) => (
              <>
                <path d={run.area} fill={`url(#${gradientId})`} />
                <path
                  d={run.line}
                  fill="none"
                  stroke={colour()}
                  stroke-width="1.5"
                  stroke-linejoin="round"
                  stroke-linecap="round"
                  /* Opt out of the non-uniform scaling so the stroke stays
                     the same weight however wide the container is. */
                  vector-effect="non-scaling-stroke"
                />
              </>
            )}
          </For>

          {/* Failures sit on the baseline: present, but with no height, which
              is the honest shape for "no measurement". */}
          <For each={failureMarks()}>
            {(x) => (
              <line
                x1={x}
                x2={x}
                y1={H - 3}
                y2={H}
                stroke="var(--bad)"
                stroke-width="2"
                vector-effect="non-scaling-stroke"
              />
            )}
          </For>
        </svg>

        {/* The three numbers a line alone cannot give: the plot is scaled to
            its own peak, so "high" or "low" means nothing without them. */}
        <Show when={!props.bare && stats().last !== null}>
          <div class="spark-read">
            <span class="tag">
              min<span class="val">{fmt(stats().min)}</span>
            </span>
            <span class="tag">
              last<span class="val">{fmt(stats().last)}</span>
            </span>
            <span class="tag">
              max<span class="val">{fmt(stats().max)}</span>
            </span>
          </div>
        </Show>
      </div>
    </Show>
  );
}

/** The sparkline plus its headline reading, as used on the overview. */
export function LatencyCard(props: {
  points: Point[];
  label: string;
  sub?: string;
  value: number | null | undefined;
}) {
  const parts = () => splitValue(formatMs(props.value));
  const failures = () => props.points.filter((p) => !p.ok).length;

  return (
    <div class="card">
      <div class="card-head">
        <div>
          <h2>{props.label}</h2>
          <Show when={props.sub}>
            <div class="card-sub">{props.sub}</div>
          </Show>
        </div>
        <Show when={failures() > 0}>
          <span class="pill bad">
            {failures()} failed {failures() === 1 ? 'cycle' : 'cycles'}
          </span>
        </Show>
      </div>
      <div class="stat-value num" style={{ 'margin-bottom': '10px' }}>
        {parts().value}
        <span class="stat-unit">{parts().unit}</span>
      </div>
      <Sparkline points={props.points} height={64} />
    </div>
  );
}
