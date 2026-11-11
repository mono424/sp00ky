import { DestroyRef, effect, inject, type Signal, signal, untracked } from '@angular/core';
import type { BucketNames, SchemaStructure } from '@spooky-sync/query-builder';
import {
  fileToUint8Array,
  type BlobUrlLease,
  type BucketPutOptions,
  type BucketPutResult,
} from '@spooky-sync/core';
import type { SyncedDb } from './db';
import { engineDb, inContext, read, toError } from './internal';
import type { InjectOptions, MaybeSignal } from './types';

type PathArg = MaybeSignal<string | null | undefined>;

export interface FileUpload {
  readonly isUploading: Signal<boolean>;
  readonly error: Signal<Error | null>;
  clearError: () => void;
  /** Validates against the bucket's `maxSize` / `allowedExtensions` first. */
  upload: (
    path: string,
    file: File | Blob,
    options?: BucketPutOptions
  ) => Promise<BucketPutResult | void>;
  /** An object URL for the file (revoked with the injection context), or `null`. */
  download: (path: string) => Promise<string | null>;
  remove: (path: string) => Promise<void>;
  exists: (path: string) => Promise<boolean>;
}

/**
 * Upload, download, remove and probe files in a bucket, with `isUploading`
 * and `error` signals. `bucket` may be a signal; it is read on each call.
 */
export function injectFileUpload<S extends SchemaStructure = any>(
  bucket: MaybeSignal<BucketNames<S>>,
  options?: InjectOptions
): FileUpload {
  return inContext(injectFileUpload, options?.injector, () => {
    const db = engineDb('injectFileUpload') as SyncedDb<S>;
    const isUploading = signal(false);
    const error = signal<Error | null>(null);

    const objectUrls: string[] = [];
    inject(DestroyRef).onDestroy(() => {
      for (const url of objectUrls) URL.revokeObjectURL(url);
    });

    const validate = (name: string, file: File | Blob): void => {
      const config = db.getBucketConfig(name);
      if (!config) return;
      if (config.maxSize !== null && config.maxSize !== undefined && file.size > config.maxSize) {
        const maxMB = (config.maxSize / (1024 * 1024)).toFixed(1);
        throw new Error(`File exceeds maximum size of ${maxMB} MB.`);
      }
      if (config.allowedExtensions && config.allowedExtensions.length > 0) {
        const fileName = (file as File).name;
        if (fileName) {
          const ext = fileName.split('.').pop()?.toLowerCase();
          if (!ext || !config.allowedExtensions.includes(ext)) {
            throw new Error(
              `File type not allowed. Accepted: ${config.allowedExtensions.join(', ')}.`
            );
          }
        }
      }
    };

    return {
      isUploading: isUploading.asReadonly(),
      error: error.asReadonly(),
      clearError: () => error.set(null),
      upload: async (path, file, putOptions) => {
        const name = untracked(() => read(bucket));
        error.set(null);
        try {
          validate(name as string, file);
        } catch (e) {
          error.set(toError(e));
          return undefined;
        }
        isUploading.set(true);
        try {
          const bytes = await fileToUint8Array(file);
          return await db.bucket(name).put(path, bytes, putOptions);
        } catch (e) {
          error.set(toError(e));
          return undefined;
        } finally {
          isUploading.set(false);
        }
      },
      download: async (path) => {
        error.set(null);
        try {
          const content = await db.bucket(untracked(() => read(bucket))).get(path);
          if (!content) return null;
          const objectUrl = URL.createObjectURL(new Blob([content as BlobPart]));
          objectUrls.push(objectUrl);
          return objectUrl;
        } catch (e) {
          error.set(toError(e));
          return null;
        }
      },
      remove: async (path) => {
        error.set(null);
        try {
          await db.bucket(untracked(() => read(bucket))).delete(path);
        } catch (e) {
          error.set(toError(e));
        }
      },
      exists: async (path) => {
        error.set(null);
        try {
          return await db.bucket(untracked(() => read(bucket))).exists(path);
        } catch (e) {
          error.set(toError(e));
          return false;
        }
      },
    };
  });
}

export interface DownloadFileOptions extends InjectOptions {
  /**
   * Master switch, default `true`. `false` gives every caller its own private
   * object URL fetched fresh from the bucket and revoked with its context: no
   * sharing, no persistence, no reuse.
   */
  cache?: boolean;
  /**
   * Keep the bytes in OPFS so they survive a reload and are available offline.
   * Default `true`. The in-tab object URL is still shared between components
   * rendering the same path either way.
   */
  persist?: boolean;
  /** Exempt this file from pressure eviction. Pinned bytes never expire. */
  pin?: boolean;
  /**
   * `'never'` (default) treats a bucket path as immutable, which is how paths
   * are written (`crypto.randomUUID() + ext`). `'head'` spends a remote `head()`
   * to compare sizes before trusting the cached copy, for paths the app
   * overwrites in place.
   */
  revalidate?: 'never' | 'head';
}

export interface DownloadFile {
  /** Object URL for the file, `null` while none is resolved (or no path). */
  readonly url: Signal<string | null>;
  readonly isLoading: Signal<boolean>;
  readonly error: Signal<Error | null>;
  /** Bypass every cache layer once and fetch again. */
  refetch: () => void;
}

/**
 * An object URL for a bucket file that follows `path` (a signal or function).
 * Cached URLs are refcounted and shared between every component showing the
 * same path; the previous URL stays valid until the next one is ready, so an
 * `<img>` never flashes broken on a path change.
 */
export function injectDownloadFile<S extends SchemaStructure = any>(
  bucket: MaybeSignal<BucketNames<S>>,
  path: PathArg,
  options: DownloadFileOptions = {}
): DownloadFile {
  return inContext(injectDownloadFile, options.injector, () =>
    downloadFile(engineDb('injectDownloadFile'), bucket, path, () => options)
  );
}

/**
 * `injectDownloadFile` without the context check, with options read lazily
 * (on each fetch, untracked), so a component can pass its inputs.
 */
function downloadFile(
  db: SyncedDb<any>,
  bucket: MaybeSignal<string>,
  path: PathArg,
  options: () => DownloadFileOptions
): DownloadFile {
  const url = signal<string | null>(null);
  const isLoading = signal(false);
  const error = signal<Error | null>(null);
  const refetchTick = signal(0);
  /** Consumed by the next fetch, so `refetch()` bypasses every layer once. */
  let reloadOnce = false;

  // What backs the current `url`: a refcounted lease on the shared cache entry,
  // or a private URL this caller minted and must revoke itself.
  let held: { lease?: BlobUrlLease; privateUrl?: string } | null = null;
  const release = () => {
    held?.lease?.release();
    if (held?.privateUrl) URL.revokeObjectURL(held.privateUrl);
    held = null;
  };
  inject(DestroyRef).onDestroy(release);

  effect(
    (onCleanup) => {
      refetchTick();
      const name = read(bucket);
      const filePath = read(path);
      if (!filePath) {
        release();
        url.set(null);
        isLoading.set(false);
        error.set(null);
        return;
      }

      const opts = untracked(options);
      const reload = reloadOnce;
      reloadOnce = false;
      let cancelled = false;
      onCleanup(() => {
        cancelled = true;
      });
      isLoading.set(true);
      error.set(null);

      const handle = db.bucket(name);
      const next: Promise<{ lease?: BlobUrlLease; privateUrl?: string; url: string } | null> =
        opts.cache !== false
          ? handle
              .url(filePath, {
                persist: opts.persist !== false,
                pin: opts.pin,
                revalidate: opts.revalidate,
                reload,
              })
              .then((lease) => (lease ? { lease, url: lease.url } : null))
          : handle.read(filePath, { persist: false, reload: true }).then((blob) => {
              if (!blob) return null;
              const privateUrl = URL.createObjectURL(blob);
              return { privateUrl, url: privateUrl };
            });

      next.then(
        (result) => {
          if (cancelled) {
            // Destroyed, or the path changed mid-flight: hand the reference
            // straight back, or the entry never drops to zero and its object
            // URL leaks for the life of the tab.
            result?.lease?.release();
            if (result?.privateUrl) URL.revokeObjectURL(result.privateUrl);
            return;
          }
          release();
          held = result;
          url.set(result?.url ?? null);
          isLoading.set(false);
        },
        (err) => {
          if (cancelled) return;
          error.set(toError(err));
          isLoading.set(false);
        }
      );
    },
    { debugName: 'injectDownloadFile' }
  );

  return {
    url: url.asReadonly(),
    isLoading: isLoading.asReadonly(),
    error: error.asReadonly(),
    refetch: () => {
      reloadOnce = true;
      refetchTick.update((n) => n + 1);
    },
  };
}

export interface BlurhashResult {
  /** The stored blurhash for the path, `null` while loading or when none exists. */
  readonly hash: Signal<string | null>;
  readonly isLoading: Signal<boolean>;
}

/**
 * The blurhash sidecar for a bucket image (written by `bucket.put`, see
 * `Sp00kyConfig.blurhash`). Resolves from OPFS instantly on warm clients; a
 * miss is remembered per tab. `injectBucketImage` / `<sp00ky-bucket-image>`
 * bundle it with the download; use this when the hash belongs to another
 * rendition.
 */
export function injectBlurhash<S extends SchemaStructure = any>(
  bucket: MaybeSignal<BucketNames<S>>,
  path: PathArg,
  options?: InjectOptions
): BlurhashResult {
  return inContext(injectBlurhash, options?.injector, () =>
    blurhashOf(engineDb('injectBlurhash'), bucket, path)
  );
}

function blurhashOf(db: SyncedDb<any>, bucket: MaybeSignal<string>, path: PathArg): BlurhashResult {
  const hash = signal<string | null>(null);
  const isLoading = signal(false);

  effect(
    (onCleanup) => {
      const name = read(bucket);
      const filePath = read(path);
      if (!filePath) {
        hash.set(null);
        isLoading.set(false);
        return;
      }
      let cancelled = false;
      onCleanup(() => {
        cancelled = true;
      });
      isLoading.set(true);
      db.bucket(name)
        .blurhash(filePath)
        .then(
          (result) => {
            if (cancelled) return;
            hash.set(result);
            isLoading.set(false);
          },
          () => {
            if (cancelled) return;
            hash.set(null);
            isLoading.set(false);
          }
        );
    },
    { debugName: 'injectBlurhash' }
  );

  return { hash: hash.asReadonly(), isLoading: isLoading.asReadonly() };
}

export interface BucketImageOptions extends DownloadFileOptions {
  /**
   * Also resolve the image's blurhash sidecar (see `Sp00kyConfig.blurhash`).
   * Default `true`; read before the bytes, so the tiny sidecar tends to land
   * first on the serialized remote chain.
   */
  blurhash?: boolean;
}

export interface BucketImageRef extends DownloadFile {
  /** Blurhash for the same path, or `null` (off, missing, still loading). */
  readonly blurhash: Signal<string | null>;
  /** True once the current `url` has been decoded and is safe to paint. */
  readonly ready: Signal<boolean>;
  /**
   * Drive an `<img>`: set its `src` to `url` and flip `ready` once that bitmap
   * is DECODED (failures count too, so a broken blob degrades to
   * paint-on-load instead of hiding forever). Reads `url`, so call it inside
   * an effect to re-arm on every new url; returns that run's cleanup. The
   * `sp00kyImageGate` directive does exactly this: prefer it.
   */
  gate: (img: HTMLImageElement) => () => void;
}

/**
 * Everything needed to render a bucket image without a pop-in: the refcounted
 * object URL, the blurhash placeholder, and a decode gate so the bitmap is only
 * revealed once it can paint in full. `<sp00ky-bucket-image>` wraps it; use
 * this with `<img [sp00kyImageGate]="image">` for custom markup.
 */
export function injectBucketImage<S extends SchemaStructure = any>(
  bucket: MaybeSignal<BucketNames<S>>,
  path: PathArg,
  options: BucketImageOptions = {}
): BucketImageRef {
  return inContext(injectBucketImage, options.injector, () =>
    bucketImage(engineDb('injectBucketImage'), bucket, path, () => options)
  );
}

/** `injectBucketImage` with options read lazily, for `<sp00ky-bucket-image>`'s inputs. */
export function bucketImage(
  db: SyncedDb<any>,
  bucket: MaybeSignal<string>,
  path: PathArg,
  options: () => BucketImageOptions
): BucketImageRef {
  // Created BEFORE the download so the sidecar read enters the serialized
  // remote queue first: the placeholder should never wait behind the bytes it
  // stands in for.
  const sidecar = blurhashOf(db, bucket, () => (options().blurhash !== false ? read(path) : null));
  const file = downloadFile(db, bucket, path, options);
  const ready = signal(false);

  const gate = (img: HTMLImageElement): (() => void) => {
    const src = file.url();
    ready.set(false);
    if (!src) {
      img.removeAttribute('src');
      return () => {};
    }
    img.src = src;
    let cancelled = false;
    const reveal = () => {
      if (!cancelled) ready.set(true);
    };
    if (typeof img.decode === 'function') img.decode().then(reveal, reveal);
    else if (img.complete) reveal();
    else {
      img.onload = reveal;
      img.onerror = reveal;
    }
    return () => {
      cancelled = true;
    };
  };

  return {
    ...file,
    blurhash: sidecar.hash,
    ready: ready.asReadonly(),
    gate,
  };
}
