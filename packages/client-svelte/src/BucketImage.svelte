<script lang="ts" module>
  import type { Snippet } from 'svelte';
  import type { ClassValue } from 'svelte/elements';
  import type { UseBucketImageOptions } from './files.svelte.js';

  export interface BucketImageProps {
    /** Bucket name from the schema. */
    bucket: string;
    /** Path within the bucket. Nullish renders only the placeholder layers. */
    path: string | null | undefined;
    alt?: string;
    /** Classes for the container element (sizing / positioning). */
    class?: ClassValue;
    /** Classes for the inner `<img>` (the layout styles are inline). */
    imgClass?: ClassValue;
    /** `object-fit` for the image. Defaults to `cover`. */
    fit?: 'cover' | 'contain' | 'fill' | 'none' | 'scale-down';
    /**
     * Bottom placeholder layer (your own plate / skeleton), shown until the
     * image settles. The blurhash paints on top of it once the sidecar resolves.
     */
    fallback?: Snippet;
    /** Crossfade duration in ms. Defaults to 300. */
    transition?: number;
    /** Crossfade easing. Defaults to an ease-out-expo curve. */
    easing?: string;
    /** Resolve the blurhash sidecar. Defaults to true. */
    blurhash?: boolean;
    /** Download tuning, forwarded to `useDownloadFile`. Read once. */
    options?: UseBucketImageOptions;
  }
</script>

<script lang="ts">
  import { untrack } from 'svelte';
  import { prefersReducedMotion } from 'svelte/motion';
  import type { BucketNames, SchemaStructure } from '@spooky-sync/query-builder';
  import { useBucketImage } from './files.svelte.js';
  import Blurhash from './Blurhash.svelte';

  let {
    bucket,
    path,
    alt = '',
    class: className,
    imgClass,
    fit = 'cover',
    fallback,
    transition = 300,
    easing = 'cubic-bezier(0.16, 1, 0.3, 1)',
    blurhash = true,
    options,
  }: BucketImageProps = $props();

  const LAYER = 'position:absolute;inset:0;width:100%;height:100%;';

  // Bucket and options are read once, like the hook's own arguments; the path
  // follows the prop.
  const image = untrack(() =>
    useBucketImage(bucket as BucketNames<SchemaStructure>, () => path, { ...options, blurhash })
  );

  // Placeholders leave the DOM once the fade is over (a shelf of covers should
  // not composite three layers each forever) and come back when the path
  // changes mid-life (`ready` re-arms with the url).
  let settled = $state(false);
  $effect(() => {
    if (!image.ready) {
      settled = false;
      return;
    }
    const wait = (prefersReducedMotion.current ? 0 : transition) + 120;
    const timer = setTimeout(() => (settled = true), wait);
    return () => clearTimeout(timer);
  });

  const imgStyle = $derived(
    LAYER +
      `object-fit:${fit};opacity:${image.ready ? 1 : 0};` +
      `transition:${prefersReducedMotion.current ? 'none' : `opacity ${transition}ms ${easing}`};`
  );

  // Layers are absolutely positioned: give them an anchor without stomping on
  // a caller class that already positions the container.
  function anchor(node: HTMLDivElement) {
    if (getComputedStyle(node).position === 'static') node.style.position = 'relative';
  }
</script>

<!--
  A bucket image that never pops in: bottom to top, your `fallback` plate, the
  stored blurhash, and the real image, which stays transparent until its bitmap
  is DECODED and then crossfades over the placeholders.
-->
<div class={className} {@attach anchor}>
  {#if !settled}
    <div style={LAYER}>
      {#if fallback}
        <div style={LAYER}>{@render fallback()}</div>
      {/if}
      <Blurhash hash={image.blurhash} style={LAYER} />
    </div>
  {/if}
  <img class={imgClass} {alt} decoding="async" style={imgStyle} {@attach image.gate} />
</div>
