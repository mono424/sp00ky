<script lang="ts" module>
  import type { ClassValue } from 'svelte/elements';

  export interface BlurhashProps {
    /** The blurhash string. Nullish paints nothing (transparent canvas). */
    hash: string | null | undefined;
    /**
     * Decode resolution. 32x32 is plenty: a blurhash carries at most 9x9 DCT
     * components, and the canvas is meant to be CSS-scaled to fill.
     */
    width?: number;
    height?: number;
    /** Contrast punch, see the blurhash reference decoder. Defaults to 1. */
    punch?: number;
    class?: ClassValue;
    style?: string;
  }
</script>

<script lang="ts">
  import { decodeBlurhash } from '@spooky-sync/core';

  let { hash, width = 32, height = 32, punch = 1, class: className, style }: BlurhashProps =
    $props();

  // An attachment re-runs whenever a prop it reads changes, so the canvas is
  // repainted once per hash (or size) change and never on unrelated renders.
  function paint(canvas: HTMLCanvasElement) {
    canvas.width = width;
    canvas.height = height;
    if (!hash) return;
    try {
      const pixels = decodeBlurhash(hash, width, height, punch);
      const ctx = canvas.getContext('2d');
      if (!ctx) return;
      const imageData = ctx.createImageData(width, height);
      imageData.data.set(pixels);
      ctx.putImageData(imageData, 0, 0);
    } catch {
      // An invalid hash paints nothing; the layer below stays visible.
    }
  }
</script>

<!--
  A blurhash painted onto a canvas. Size it with `class` / `style` (e.g.
  `absolute inset-0 w-full h-full`); the decode resolution stays tiny.
-->
<canvas class={className} {style} {@attach paint}></canvas>
