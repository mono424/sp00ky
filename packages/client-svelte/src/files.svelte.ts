import type { Attachment } from 'svelte/attachments';
import type { BucketNames, SchemaStructure } from '@spooky-sync/query-builder';
import {
  fileToUint8Array,
  type BlobUrlLease,
  type BucketPutOptions,
  type BucketPutResult,
} from '@spooky-sync/core';
import { SyncedDb } from './db.js';
import { read, toError, useDb } from './context.js';
import type { MaybeGetter } from './types.js';

type PathArg = MaybeGetter<string | null | undefined>;

export interface FileUploadResult {
  readonly isUploading: boolean;
  readonly error: Error | null;
  clearError: () => void;
  /** Validates against the bucket's `maxSize` / `allowedExtensions` first. */
  upload: (
    path: string,
    file: File | Blob,
    options?: BucketPutOptions
  ) => Promise<BucketPutResult | void>;
  /** An object URL for the file (revoked when the component is destroyed), or `null`. */
  download: (path: string) => Promise<string | null>;
  remove: (path: string) => Promise<void>;
  exists: (path: string) => Promise<boolean>;
}

/**
 * Upload, download, remove and probe files in a bucket, with `isUploading`
 * and `error` state. Reactive fields are getters: keep the object
 * (`const files = useFileUpload('avatars')`) and read `files.isUploading`.
 */
export function useFileUpload<S extends SchemaStructure>(
  bucketName: BucketNames<S>
): FileUploadResult;
export function useFileUpload<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>
): FileUploadResult;
export function useFileUpload<S extends SchemaStructure>(
  dbOrBucketName: SyncedDb<S> | BucketNames<S>,
  maybeBucketName?: BucketNames<S>
): FileUploadResult {
  const db = dbOrBucketName instanceof SyncedDb ? dbOrBucketName : useDb<S>();
  const bucketName = (
    dbOrBucketName instanceof SyncedDb ? maybeBucketName : dbOrBucketName
  ) as BucketNames<S>;

  let isUploading = $state(false);
  let error = $state.raw<Error | null>(null);

  const objectUrls: string[] = [];
  $effect(() => () => {
    for (const url of objectUrls) URL.revokeObjectURL(url);
  });

  const validate = (file: File | Blob): void => {
    const config = db.getBucketConfig(bucketName as string);
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
    get isUploading() {
      return isUploading;
    },
    get error() {
      return error;
    },
    clearError: () => {
      error = null;
    },
    upload: async (path, file, options) => {
      error = null;
      try {
        validate(file);
      } catch (e) {
        error = toError(e);
        return;
      }
      isUploading = true;
      try {
        const bytes = await fileToUint8Array(file);
        return await db.bucket(bucketName).put(path, bytes, options);
      } catch (e) {
        error = toError(e);
      } finally {
        isUploading = false;
      }
    },
    download: async (path) => {
      error = null;
      try {
        const content = await db.bucket(bucketName).get(path);
        if (!content) return null;
        const objectUrl = URL.createObjectURL(new Blob([content as BlobPart]));
        objectUrls.push(objectUrl);
        return objectUrl;
      } catch (e) {
        error = toError(e);
        return null;
      }
    },
    remove: async (path) => {
      error = null;
      try {
        await db.bucket(bucketName).delete(path);
      } catch (e) {
        error = toError(e);
      }
    },
    exists: async (path) => {
      error = null;
      try {
        return await db.bucket(bucketName).exists(path);
      } catch (e) {
        error = toError(e);
        return false;
      }
    },
  };
}

export interface UseDownloadFileOptions {
  /**
   * Master switch, default `true`. `false` gives every hook instance its own
   * private object URL fetched fresh from the bucket and revoked on destroy:
   * no sharing, no persistence, no reuse.
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

export interface UseDownloadFileResult {
  /** Object URL for the file, `null` while none is resolved (or no path). */
  readonly url: string | null;
  readonly isLoading: boolean;
  readonly error: Error | null;
  /** Bypass every cache layer once and fetch again. */
  refetch: () => void;
}

/**
 * An object URL for a bucket file that follows `path` (pass a getter). Cached
 * URLs are refcounted and shared between every component showing the same
 * path; the previous URL stays valid until the next one is ready, so an
 * `<img>` never flashes broken on a path change.
 */
export function useDownloadFile<S extends SchemaStructure>(
  bucketName: BucketNames<S>,
  path: PathArg,
  options?: UseDownloadFileOptions
): UseDownloadFileResult;
export function useDownloadFile<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>,
  path: PathArg,
  options?: UseDownloadFileOptions
): UseDownloadFileResult;
export function useDownloadFile<S extends SchemaStructure>(
  dbOrBucketName: SyncedDb<S> | BucketNames<S>,
  bucketNameOrPath?: BucketNames<S> | PathArg,
  pathOrOptions?: PathArg | UseDownloadFileOptions,
  maybeOptions?: UseDownloadFileOptions
): UseDownloadFileResult {
  const explicit = dbOrBucketName instanceof SyncedDb;
  const db = explicit ? dbOrBucketName : useDb<S>();
  const bucketName = (explicit ? bucketNameOrPath : dbOrBucketName) as BucketNames<S>;
  const path = (explicit ? pathOrOptions : bucketNameOrPath) as PathArg;
  const options = ((explicit ? maybeOptions : pathOrOptions) ?? {}) as UseDownloadFileOptions;
  const useCache = options.cache !== false;

  let url = $state<string | null>(null);
  let isLoading = $state(false);
  let error = $state.raw<Error | null>(null);
  let refetchTick = $state(0);
  /** Consumed by the next fetch, so `refetch()` bypasses every layer once. */
  let reloadOnce = false;

  // What backs the current `url`: a refcounted lease on the shared cache entry,
  // or a private URL this instance minted and must revoke itself.
  let held: { lease?: BlobUrlLease; privateUrl?: string } | null = null;
  const release = () => {
    held?.lease?.release();
    if (held?.privateUrl) URL.revokeObjectURL(held.privateUrl);
    held = null;
  };
  $effect(() => release);

  $effect(() => {
    void refetchTick;
    const filePath = read(path);
    if (!filePath) {
      release();
      url = null;
      isLoading = false;
      error = null;
      return;
    }

    const reload = reloadOnce;
    reloadOnce = false;
    let cancelled = false;
    isLoading = true;
    error = null;

    const bucket = db.bucket(bucketName);
    const next: Promise<{ lease?: BlobUrlLease; privateUrl?: string; url: string } | null> =
      useCache
        ? bucket
            .url(filePath, {
              persist: options.persist !== false,
              pin: options.pin,
              revalidate: options.revalidate,
              reload,
            })
            .then((lease) => (lease ? { lease, url: lease.url } : null))
        : bucket.read(filePath, { persist: false, reload: true }).then((blob) => {
            if (!blob) return null;
            const privateUrl = URL.createObjectURL(blob);
            return { privateUrl, url: privateUrl };
          });

    next.then(
      (result) => {
        if (cancelled) {
          // Destroyed, or the path changed mid-flight: hand the reference
          // straight back, or the entry never drops to zero and its object URL
          // leaks for the life of the tab.
          result?.lease?.release();
          if (result?.privateUrl) URL.revokeObjectURL(result.privateUrl);
          return;
        }
        release();
        held = result;
        url = result?.url ?? null;
        isLoading = false;
      },
      (err) => {
        if (cancelled) return;
        error = toError(err);
        isLoading = false;
      }
    );

    return () => {
      cancelled = true;
    };
  });

  return {
    get url() {
      return url;
    },
    get isLoading() {
      return isLoading;
    },
    get error() {
      return error;
    },
    refetch: () => {
      reloadOnce = true;
      refetchTick += 1;
    },
  };
}

export interface UseBlurhashResult {
  /** The stored blurhash for the path, `null` while loading or when none exists. */
  readonly hash: string | null;
  readonly isLoading: boolean;
}

/**
 * The blurhash sidecar for a bucket image (written by `bucket.put`, see
 * `Sp00kyConfig.blurhash`). Resolves from OPFS instantly on warm clients; a
 * miss is remembered per tab. `useBucketImage` / `<BucketImage>` bundle it with
 * the download; use this when the hash belongs to another rendition.
 */
export function useBlurhash<S extends SchemaStructure>(
  bucketName: BucketNames<S>,
  path: PathArg
): UseBlurhashResult;
export function useBlurhash<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>,
  path: PathArg
): UseBlurhashResult;
export function useBlurhash<S extends SchemaStructure>(
  dbOrBucketName: SyncedDb<S> | BucketNames<S>,
  bucketNameOrPath?: BucketNames<S> | PathArg,
  maybePath?: PathArg
): UseBlurhashResult {
  const explicit = dbOrBucketName instanceof SyncedDb;
  const db = explicit ? dbOrBucketName : useDb<S>();
  const bucketName = (explicit ? bucketNameOrPath : dbOrBucketName) as BucketNames<S>;
  const path = (explicit ? maybePath : bucketNameOrPath) as PathArg;

  let hash = $state<string | null>(null);
  let isLoading = $state(false);

  $effect(() => {
    const filePath = read(path);
    if (!filePath) {
      hash = null;
      isLoading = false;
      return;
    }
    let cancelled = false;
    isLoading = true;
    db.bucket(bucketName)
      .blurhash(filePath)
      .then(
        (result) => {
          if (cancelled) return;
          hash = result;
          isLoading = false;
        },
        () => {
          if (cancelled) return;
          hash = null;
          isLoading = false;
        }
      );
    return () => {
      cancelled = true;
    };
  });

  return {
    get hash() {
      return hash;
    },
    get isLoading() {
      return isLoading;
    },
  };
}

export interface UseBucketImageOptions extends UseDownloadFileOptions {
  /**
   * Also resolve the image's blurhash sidecar (see `Sp00kyConfig.blurhash`).
   * Default `true`; read before the bytes, so the tiny sidecar tends to land
   * first on the serialized remote chain.
   */
  blurhash?: boolean;
}

export interface UseBucketImageResult extends UseDownloadFileResult {
  /** Blurhash for the same path, or `null` (off, missing, still loading). */
  readonly blurhash: string | null;
  /** True once the current `url` has been decoded and is safe to paint. */
  readonly ready: boolean;
  /**
   * Attachment for the `<img>`: `<img {@attach image.gate} alt="" />`. It sets
   * `src` to `url` itself and flips `ready` once that bitmap is DECODED
   * (failures count too, so a broken blob degrades to paint-on-load instead of
   * hiding forever). Re-arms on every new url. Do not also bind `src`.
   */
  gate: Attachment<HTMLImageElement>;
}

/**
 * Everything needed to render a bucket image without a pop-in: the refcounted
 * object URL, the blurhash placeholder, and a decode gate so the bitmap is only
 * revealed once it can paint in full. `<BucketImage>` wraps it; use the hook
 * for custom markup.
 */
export function useBucketImage<S extends SchemaStructure>(
  bucketName: BucketNames<S>,
  path: PathArg,
  options?: UseBucketImageOptions
): UseBucketImageResult;
export function useBucketImage<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>,
  path: PathArg,
  options?: UseBucketImageOptions
): UseBucketImageResult;
export function useBucketImage<S extends SchemaStructure>(
  dbOrBucketName: SyncedDb<S> | BucketNames<S>,
  bucketNameOrPath?: BucketNames<S> | PathArg,
  pathOrOptions?: PathArg | UseBucketImageOptions,
  maybeOptions?: UseBucketImageOptions
): UseBucketImageResult {
  const explicit = dbOrBucketName instanceof SyncedDb;
  const db = explicit ? dbOrBucketName : useDb<S>();
  const bucketName = (explicit ? bucketNameOrPath : dbOrBucketName) as BucketNames<S>;
  const path = (explicit ? pathOrOptions : bucketNameOrPath) as PathArg;
  const options = ((explicit ? maybeOptions : pathOrOptions) ?? {}) as UseBucketImageOptions;

  // Declared BEFORE the download so the sidecar read enters the serialized
  // remote queue first: the placeholder should never wait behind the bytes it
  // stands in for.
  const wantHash = options.blurhash !== false;
  const sidecar = useBlurhash(db, bucketName, () => (wantHash ? read(path) : null));
  const file = useDownloadFile(db, bucketName, path, options);

  let ready = $state(false);

  const gate: Attachment<HTMLImageElement> = (img) => {
    const src = file.url;
    ready = false;
    if (!src) {
      img.removeAttribute('src');
      return;
    }
    img.src = src;
    let cancelled = false;
    const reveal = () => {
      if (!cancelled) ready = true;
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
    get url() {
      return file.url;
    },
    get isLoading() {
      return file.isLoading;
    },
    get error() {
      return file.error;
    },
    refetch: file.refetch,
    get blurhash() {
      return sidecar.hash;
    },
    get ready() {
      return ready;
    },
    gate,
  };
}
