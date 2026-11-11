import { afterRenderEffect, Directive, ElementRef, inject, input } from '@angular/core';
import type { BucketImageRef } from './files';

/**
 * Wires an `<img>` to `injectBucketImage`: sets `src` to the image's url and
 * flips its `ready` signal once the bitmap is decoded, re-arming on every new
 * url. Do not also bind `src`.
 *
 * ```html
 * <img [sp00kyImageGate]="cover" alt="" [style.opacity]="cover.ready() ? 1 : 0" />
 * ```
 */
@Directive({
  selector: 'img[sp00kyImageGate]',
})
export class ImageGate {
  /** The result of `injectBucketImage(...)`. */
  readonly image = input.required<BucketImageRef>({ alias: 'sp00kyImageGate' });

  constructor() {
    const img = inject<ElementRef<HTMLImageElement>>(ElementRef).nativeElement;
    afterRenderEffect({
      write: (onCleanup) => onCleanup(this.image().gate(img)),
    });
  }
}
