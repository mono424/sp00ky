import { Index } from 'solid-js';
import { FilterMenu, Sq } from '../components/Frame';
import { Sk } from '../components/Skeleton';

/** The plot's height, as `CHART_H` in the Logs route. */
const PLOT_H = 132;

/** Heights of the loading bars: a plausible skyline, fixed so it does not flicker. */
const SKYLINE = Array.from({ length: 64 }, (_, i) => 18 + Math.round(30 * (1 + Math.sin(i / 4.3)) * (0.6 + 0.4 * Math.cos(i / 9))));

/** The count over the chart while nothing has arrived: the figure and its caption line. */
export function LogTotalSkeleton() {
  return (
    <>
      {/* A block, not a bar in a <b>: the figure's line-height is 1 and a bar's line box is taller. */}
      <Sk block w={60} h={30} />
      <span class="cap"><Sk w="8em" /></span>
    </>
  );
}

/** Bars standing in for the histogram, inside its plot. */
export function PlotSkeleton() {
  return (
    <div class="lx-plot-sk" aria-hidden="true">
      <Index each={SKYLINE}>{(h, i) => <span class="sk" style={{ height: `${h()}%`, '--i': i % 12 }} />}</Index>
    </div>
  );
}

/** Log rows before the stream has said anything. */
export function LogRowsSkeleton() {
  return (
    <div class="lx-sk-rows" aria-hidden="true">
      <Index each={Array.from({ length: 14 })}>
        {(_, i) => (
          <div class="lx-row">
            <span class="sk" style={{ width: '84px', '--i': i }} />
            <span class="sk" style={{ width: '34px', '--i': i }} />
            <span class="sk lx-target" style={{ width: '90px', '--i': i }} />
            <span class="sk" style={{ width: `${38 + ((i * 37) % 45)}%`, '--i': i }} />
          </div>
        )}
      </Index>
    </div>
  );
}

/** Logs before the page exists: sources and count, the histogram, the search, the table. */
export function LogsSkeleton() {
  return (
    <div class="lx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell lx-chart-cell">
          <div class="lx-chart-top">
            <div class="fx-range lx-sources" role="group" aria-label="Source">
              <button class="on" tabIndex={-1}><Sq />scheduler</button>
              <button tabIndex={-1}><Sq /><Sk w="5em" /></button>
            </div>
            <div class="lx-total"><LogTotalSkeleton /></div>
          </div>
          <div class="lx-chart">
            <div class="lx-plot loading" style={{ height: `${PLOT_H}px` }}>
              <PlotSkeleton />
            </div>
            <div class="lx-axis cap"><span>&nbsp;</span></div>
          </div>
        </div>
      </section>
      <section class="fx-row lx-query">
        <div class="fx-cell lx-query-cell">
          <label class="lx-search">
            <svg width="13" height="13" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true">
              <circle cx="6" cy="6" r="4.5" />
              <path d="M9.5 9.5L13 13" />
            </svg>
            <input type="search" aria-label="Search" placeholder="Search messages, targets, fields" disabled />
            <kbd class="lx-kbd">/</kbd>
          </label>
          <FilterMenu value={{}} onChange={() => undefined} fields={[]} />
        </div>
      </section>
      <div class="lx-body">
        <div class="lx-table">
          <div class="lx-row lx-thead cap" aria-hidden="true">
            <span>Time</span>
            <span>Level</span>
            <span class="lx-target">Target</span>
            <span>Message</span>
          </div>
          <div class="lx-vp">
            <LogRowsSkeleton />
          </div>
        </div>
      </div>
    </div>
  );
}
