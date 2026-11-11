import {
  afterNextRender,
  ChangeDetectionStrategy,
  Component,
  computed,
  DestroyRef,
  effect,
  ElementRef,
  inject,
  input,
  signal,
} from '@angular/core';
import { Blurhash } from './blurhash';
import { bucketImage, type BucketImageOptions } from './files';
import { ImageGate } from './image-gate';
import { engineDb } from './internal';

const LAYER = 'position:absolute;inset:0;width:100%;height:100%;';

/** `prefers-reduced-motion: reduce`, live. */
function prefersReducedMotion() {
  const reduced = signal(false);
  if (typeof matchMedia !== 'function') return reduced.asReadonly();
  const query = matchMedia('(prefers-reduced-motion: reduce)');
  reduced.set(query.matches);
  const onChange = (e: MediaQueryListEvent) => reduced.set(e.matches);
  query.addEventListener?.('change', onChange);
  inject(DestroyRef).onDestroy(() => query.removeEventListener?.('change', onChange));
  return reduced.asReadonly();
}

/**
 * A bucket image that never pops in. Bottom to top: the projected content (your
 * own plate or skeleton), the stored blurhash, and the real image, which stays
 * transparent until its bitmap is DECODED and then crossfades over the
 * placeholders. The placeholders leave the DOM once the fade is over and come
 * back when the path changes.
 *
 * ```html
 * <sp00ky-bucket-image bucket="avatars" [path]="avatar()" class="h-12 w-12 rounded-full">
 *   <div class="h-full w-full bg-neutral-200"></div>
 * </sp00ky-bucket-image>
 * ```
 *
 * The host is the container: size and position it with `class` / `style`. It
 * becomes `display: block` and `position: relative` unless you set otherwise.
 */
@Component({
  selector: 'sp00ky-bucket-image',
  imports: [Blurhash, ImageGate],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (!settled()) {
      <div [style]="layer">
        <div [style]="layer"><ng-content /></div>
        <canvas [sp00kyBlurhash]="image.blurhash()" [style]="layer"></canvas>
      </div>
    }
    <img
      [class]="imgClass()"
      [alt]="alt()"
      decoding="async"
      [style]="imgStyle()"
      [sp00kyImageGate]="image"
    />
  `,
})
export class BucketImage {
  /** Bucket name from the schema. */
  readonly bucket = input.required<string>();
  /** Path within the bucket. Nullish renders only the placeholder layers. */
  readonly path = input<string | null | undefined>(null);
  readonly alt = input('');
  /** Classes for the inner `<img>` (the layout styles are inline). */
  readonly imgClass = input<string>();
  /** `object-fit` for the image. Defaults to `cover`. */
  readonly fit = input<'cover' | 'contain' | 'fill' | 'none' | 'scale-down'>('cover');
  /** Crossfade duration in ms. Defaults to 300. */
  readonly transition = input(300);
  /** Crossfade easing. Defaults to an ease-out-expo curve. */
  readonly easing = input('cubic-bezier(0.16, 1, 0.3, 1)');
  /** Resolve the blurhash sidecar. Defaults to true. */
  readonly blurhash = input(true);
  /** Download tuning, as for `injectDownloadFile`. Read when a fetch starts. */
  readonly options = input<BucketImageOptions>();

  protected readonly layer = LAYER;
  protected readonly image = bucketImage(engineDb('BucketImage'), this.bucket, this.path, () => ({
    ...this.options(),
    blurhash: this.blurhash(),
  }));
  protected readonly settled = signal(false);
  private readonly reducedMotion = prefersReducedMotion();

  protected readonly imgStyle = computed(() => {
    const fade = this.reducedMotion() ? 'none' : `opacity ${this.transition()}ms ${this.easing()}`;
    return (
      LAYER + `object-fit:${this.fit()};opacity:${this.image.ready() ? 1 : 0};transition:${fade};`
    );
  });

  constructor() {
    effect((onCleanup) => {
      if (!this.image.ready()) {
        this.settled.set(false);
        return;
      }
      const wait = (this.reducedMotion() ? 0 : this.transition()) + 120;
      const timer = setTimeout(() => this.settled.set(true), wait);
      onCleanup(() => clearTimeout(timer));
    });

    // The layers are absolutely positioned: give them an anchor without
    // stomping on a caller class that already positions or displays the host.
    const host = inject<ElementRef<HTMLElement>>(ElementRef).nativeElement;
    afterNextRender(() => {
      const { display, position } = getComputedStyle(host);
      if (!display || display === 'inline') host.style.display = 'block';
      if (!position || position === 'static') host.style.position = 'relative';
    });
  }
}
