/**
 * Loading skeletons that take the real page's size.
 *
 * A page's skeleton is that page's own markup: the same rows, cells, tables
 * and classes, with the static words (titles, column heads) left in and only
 * the data swapped for shimmer bars. `Sk` is a bar sized in `em` and set
 * inline, so it sits on the line box of whatever text it stands in for and
 * the row comes out exactly as tall as it will be when the data lands.
 *
 * Skeleton bodies live in `src/skeletons/`, one file per page, importing only
 * this kit, so the entry chunk can draw the right one before the app loads.
 */

import { For, type JSX } from 'solid-js';

/** One bar of shimmer standing in for a word or a number. */
export function Sk(props: {
  /** Width: a number is px, a string any CSS length. */
  w?: number | string;
  /** Height for a block (a chart, an image); text bars take the line's. */
  h?: number | string;
  /** A block on its own line, full width by default (charts, areas). */
  block?: boolean;
  /** Stagger index, so a list shimmers in a wave. */
  i?: number;
  class?: string;
  style?: JSX.CSSProperties;
}) {
  const len = (v: number | string | undefined) => (typeof v === 'number' ? `${v}px` : v);
  if (props.block) {
    return (
      <span
        class={`sk sk-b ${props.class ?? ''}`}
        style={{ width: len(props.w), height: len(props.h), '--i': String(props.i ?? 0), ...props.style }}
        aria-hidden="true"
      />
    );
  }
  // The zero-width space gives the wrapper a line box in any context, a flex
  // item included, so it is exactly as tall as the text it stands in for.
  return (
    <span class={`sk-w ${props.class ?? ''}`} style={{ width: len(props.w), ...props.style }} aria-hidden="true">
      <span class="sk sk-t" style={{ height: len(props.h), '--i': String(props.i ?? 0) }} />
      {'\u200b'}
    </span>
  );
}

/** `n` copies of a row, each told its index. */
export function SkRepeat(props: { n: number; children: (i: number) => JSX.Element }) {
  return <For each={Array.from({ length: props.n }, (_, i) => i)}>{(i) => props.children(i)}</For>;
}

/** A width that varies a little row to row, so a list does not look stamped. */
export function vary(i: number, min: number, spread: number): string {
  return `${min + ((i * 37) % spread)}px`;
}
