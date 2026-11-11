import { afterRenderEffect, Directive, ElementRef, inject, input } from '@angular/core';
import { decodeBlurhash } from '@spooky-sync/core';

/**
 * Paints a blurhash onto the host `<canvas>`:
 * `<canvas [sp00kyBlurhash]="hash" class="absolute inset-0 h-full w-full"></canvas>`.
 *
 * Size the canvas with CSS; the decode resolution stays tiny (32x32 is plenty:
 * a blurhash carries at most 9x9 DCT components). Repaints once per hash (or
 * size) change, after render, and never during a server render.
 */
@Directive({
  selector: 'canvas[sp00kyBlurhash]',
})
export class Blurhash {
  /** The blurhash string. Nullish paints nothing (a transparent canvas). */
  readonly hash = input<string | null | undefined>(undefined, { alias: 'sp00kyBlurhash' });
  /** Decode width in pixels. Defaults to 32. */
  readonly blurhashWidth = input(32);
  /** Decode height in pixels. Defaults to 32. */
  readonly blurhashHeight = input(32);
  /** Contrast punch, see the blurhash reference decoder. Defaults to 1. */
  readonly blurhashPunch = input(1);

  constructor() {
    const canvas = inject<ElementRef<HTMLCanvasElement>>(ElementRef).nativeElement;
    afterRenderEffect({
      write: () => {
        const width = this.blurhashWidth();
        const height = this.blurhashHeight();
        const hash = this.hash();
        const punch = this.blurhashPunch();
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
      },
    });
  }
}
