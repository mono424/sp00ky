import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
} from 'react';
import type { BucketNames, SchemaStructure } from '@spooky-sync/query-builder';
import {
  decodeBlurhash,
  fileToUint8Array,
  type BlobUrlLease,
  type BucketPutOptions,
  type BucketPutResult,
} from '@spooky-sync/core';
import { SyncedDb } from './db';
import { useIsomorphicLayoutEffect, useOptionalDb } from './provider';

const toError = (e: unknown) => (e instanceof Error ? e : new Error(String(e)));

/** `(bucket, ...rest)` with the provider's db, or `(db, bucket, ...rest)`. */
function useDbAndArgs<S extends SchemaStructure, Rest extends unknown[]>(
  hook: string,
  args: unknown[]
): [SyncedDb<S>, BucketNames<S>, ...Rest] {
  const contextDb = useOptionalDb();
  if (args[0] instanceof SyncedDb) return args as [SyncedDb<S>, BucketNames<S>, ...Rest];
  if (!contextDb)
    throw new Error(`${hook} must be used within a <Sp00kyProvider>, or pass the SyncedDb.`);
  return [contextDb as SyncedDb<S>, ...(args as [BucketNames<S>, ...Rest])];
}

// ---------------------------------------------------------------------------
// useFileUpload
// ---------------------------------------------------------------------------

export interface FileUploadResult {
  isUploading: boolean;
  error: Error | null;
  clearError: () => void;
  upload: (
    path: string,
    file: File | Blob,
    options?: BucketPutOptions
  ) => Promise<BucketPutResult | void>;
  /** Object URL of the file, revoked when the component unmounts. */
  download: (path: string) => Promise<string | null>;
  remove: (path: string) => Promise<void>;
  exists: (path: string) => Promise<boolean>;
}

/**
 * Upload to, read from and delete in a bucket, with `isUploading` / `error`
 * state. Validates `maxSize` and `allowedExtensions` from the schema before
 * uploading. The returned functions are stable across renders.
 */
export function useFileUpload<S extends SchemaStructure>(
  bucketName: BucketNames<S>
): FileUploadResult;
export function useFileUpload<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>
): FileUploadResult;
export function useFileUpload(...args: unknown[]): FileUploadResult {
  const [db, bucketName] = useDbAndArgs<any, []>('useFileUpload', args);
  const [uploads, setUploads] = useState(0);
  const [error, setError] = useState<Error | null>(null);

  const objectUrls = useRef<string[]>([]);
  useEffect(
    () => () => {
      for (const url of objectUrls.current) URL.revokeObjectURL(url);
      objectUrls.current = [];
    },
    []
  );

  const clearError = useCallback(() => setError(null), []);

  const upload = useCallback(
    async (path: string, file: File | Blob, options?: BucketPutOptions) => {
      setError(null);
      const config = db.getBucketConfig(bucketName as string);
      if (config?.maxSize !== null && config?.maxSize !== undefined && file.size > config.maxSize) {
        const maxMB = (config.maxSize / (1024 * 1024)).toFixed(1);
        setError(new Error(`File exceeds maximum size of ${maxMB} MB.`));
        return;
      }
      const allowed = config?.allowedExtensions;
      const fileName = (file as File).name;
      if (allowed && allowed.length > 0 && fileName) {
        const ext = fileName.split('.').pop()?.toLowerCase();
        if (!ext || !allowed.includes(ext)) {
          setError(new Error(`File type not allowed. Accepted: ${allowed.join(', ')}.`));
          return;
        }
      }
      setUploads((n) => n + 1);
      try {
        const bytes = await fileToUint8Array(file);
        return await db.bucket(bucketName).put(path, bytes, options);
      } catch (e) {
        setError(toError(e));
      } finally {
        setUploads((n) => n - 1);
      }
    },
    [db, bucketName]
  );

  const download = useCallback(
    async (path: string) => {
      setError(null);
      try {
        const content = await db.bucket(bucketName).get(path);
        if (!content) return null;
        const url = URL.createObjectURL(new Blob([content as BlobPart]));
        objectUrls.current.push(url);
        return url;
      } catch (e) {
        setError(toError(e));
        return null;
      }
    },
    [db, bucketName]
  );

  const remove = useCallback(
    async (path: string) => {
      setError(null);
      try {
        await db.bucket(bucketName).delete(path);
      } catch (e) {
        setError(toError(e));
      }
    },
    [db, bucketName]
  );

  const exists = useCallback(
    async (path: string) => {
      setError(null);
      try {
        return await db.bucket(bucketName).exists(path);
      } catch (e) {
        setError(toError(e));
        return false;
      }
    },
    [db, bucketName]
  );

  return { isUploading: uploads > 0, error, clearError, upload, download, remove, exists };
}

// ---------------------------------------------------------------------------
// useDownloadFile
// ---------------------------------------------------------------------------

export interface UseDownloadFileOptions {
  /**
   * Master switch, default `true`. `false` gives every hook instance its own
   * private object URL fetched fresh from the bucket and revoked on unmount.
   */
  cache?: boolean;
  /** Keep the bytes in OPFS so they survive a reload and work offline. Default `true`. */
  persist?: boolean;
  /** Exempt this file from pressure eviction. */
  pin?: boolean;
  /**
   * `'never'` (default) treats a bucket path as immutable. `'head'` spends a
   * remote `head()` to compare sizes first, for paths the app overwrites.
   */
  revalidate?: 'never' | 'head';
}

export interface UseDownloadFileResult {
  url: string | null;
  isLoading: boolean;
  error: Error | null;
  /** Bypass every cache layer once and fetch again. */
  refetch: () => void;
}

interface DownloadState {
  /** The request this state belongs to; a stale one reads as idle. */
  request: string | null;
  url: string | null;
  isLoading: boolean;
  error: Error | null;
}

/**
 * Object URL for a bucket file, shared and refcounted across every component
 * showing the same path, persisted to OPFS by default. `null` path is idle.
 */
export function useDownloadFile<S extends SchemaStructure>(
  bucketName: BucketNames<S>,
  path: string | null | undefined,
  options?: UseDownloadFileOptions
): UseDownloadFileResult;
export function useDownloadFile<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>,
  path: string | null | undefined,
  options?: UseDownloadFileOptions
): UseDownloadFileResult;
export function useDownloadFile(...args: unknown[]): UseDownloadFileResult {
  const [db, bucketName, path, options = {}] = useDbAndArgs<
    any,
    [string | null | undefined, UseDownloadFileOptions?]
  >('useDownloadFile', args);
  const useCache = options.cache !== false;
  const persist = options.persist !== false;
  const { pin, revalidate } = options;

  const [tick, setTick] = useState(0);
  const reloadOnce = useRef(false);
  const request = path ? `${String(bucketName)}\u0000${path}\u0000${tick}` : null;
  const [state, setState] = useState<DownloadState>({
    request: null,
    url: null,
    isLoading: false,
    error: null,
  });

  useEffect(() => {
    if (!path || !request) return;
    const reload = reloadOnce.current;
    reloadOnce.current = false;
    let cancelled = false;
    // Exactly one of these is held: a refcounted lease on the shared cache
    // entry, or a private URL this effect minted and must revoke itself.
    let lease: BlobUrlLease | null = null;
    let privateUrl: string | null = null;
    setState((s) => ({ request, url: s.url, isLoading: true, error: null }));

    const bucket = db.bucket(bucketName);
    const resolve: Promise<string | null> = useCache
      ? bucket.url(path, { persist, pin, revalidate, reload }).then((acquired) => {
          if (!acquired) return null;
          if (cancelled) {
            // Unmounted or the path changed mid-flight: hand the reference
            // straight back, or its object URL leaks for the life of the tab.
            acquired.release();
            return null;
          }
          lease = acquired;
          return acquired.url;
        })
      : bucket.read(path, { persist: false, reload: true }).then((blob) => {
          if (!blob || cancelled) return null;
          privateUrl = URL.createObjectURL(blob);
          return privateUrl;
        });

    resolve.then(
      (url) => {
        if (!cancelled) setState({ request, url, isLoading: false, error: null });
      },
      (err) => {
        if (!cancelled) setState({ request, url: null, isLoading: false, error: toError(err) });
      }
    );

    return () => {
      cancelled = true;
      lease?.release();
      if (privateUrl) URL.revokeObjectURL(privateUrl);
    };
  }, [db, request, useCache, persist, pin, revalidate]);

  const refetch = useCallback(() => {
    reloadOnce.current = true;
    setTick((n) => n + 1);
  }, []);

  // A path that went away (or a request not started yet) reads as idle, so a
  // render never shows the previous path's URL under a null path.
  if (!request) return { url: null, isLoading: false, error: null, refetch };
  if (state.request !== request) return { url: state.url, isLoading: true, error: null, refetch };
  return { url: state.url, isLoading: state.isLoading, error: state.error, refetch };
}

// ---------------------------------------------------------------------------
// useBlurhash / useBucketImage
// ---------------------------------------------------------------------------

export interface UseBlurhashResult {
  /** The stored blurhash for the path, or null while loading / when none exists. */
  hash: string | null;
  isLoading: boolean;
}

/**
 * The blurhash sidecar for a bucket image (written automatically by
 * `bucket.put`, see `Sp00kyConfig.blurhash`). Resolves from OPFS instantly on
 * warm clients. `useBucketImage` / `BucketImage` bundle it with the download.
 */
export function useBlurhash<S extends SchemaStructure>(
  bucketName: BucketNames<S>,
  path: string | null | undefined
): UseBlurhashResult;
export function useBlurhash<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>,
  path: string | null | undefined
): UseBlurhashResult;
export function useBlurhash(...args: unknown[]): UseBlurhashResult {
  const [db, bucketName, path] = useDbAndArgs<any, [string | null | undefined]>(
    'useBlurhash',
    args
  );
  const request = path ? `${String(bucketName)}\u0000${path}` : null;
  const [state, setState] = useState<{ request: string | null; hash: string | null }>({
    request: null,
    hash: null,
  });

  useEffect(() => {
    if (!path || !request) return;
    let cancelled = false;
    db.bucket(bucketName)
      .blurhash(path)
      .then(
        (hash) => {
          if (!cancelled) setState({ request, hash });
        },
        () => {
          if (!cancelled) setState({ request, hash: null });
        }
      );
    return () => {
      cancelled = true;
    };
  }, [db, request]);

  if (!request) return { hash: null, isLoading: false };
  if (state.request !== request) return { hash: null, isLoading: true };
  return { hash: state.hash, isLoading: false };
}

export interface UseBucketImageOptions extends UseDownloadFileOptions {
  /** Also resolve the image's blurhash sidecar. Default `true`. */
  blurhash?: boolean;
}

export interface UseBucketImageResult extends UseDownloadFileResult {
  /** Blurhash for the same path, or null (off, missing, still loading). */
  blurhash: string | null;
  /** True once the current `url` has been decoded and is safe to paint. */
  ready: boolean;
  /**
   * Ref callback for the `<img>` rendering `url`: flips `ready` once the
   * bitmap is decoded (on failure too, so a broken blob degrades to
   * paint-on-load instead of hiding the image forever). Its identity follows
   * `url`, so React re-runs it when the url changes.
   */
  gate: (img: HTMLImageElement | null) => void;
}

/**
 * Everything to render a bucket image without a pop-in: the refcounted object
 * URL, the blurhash placeholder, and a decode gate. `BucketImage` wraps this
 * into a drop-in component; use the hook for custom markup.
 */
export function useBucketImage<S extends SchemaStructure>(
  bucketName: BucketNames<S>,
  path: string | null | undefined,
  options?: UseBucketImageOptions
): UseBucketImageResult;
export function useBucketImage<S extends SchemaStructure>(
  db: SyncedDb<S>,
  bucketName: BucketNames<S>,
  path: string | null | undefined,
  options?: UseBucketImageOptions
): UseBucketImageResult;
export function useBucketImage(...args: unknown[]): UseBucketImageResult {
  const [db, bucketName, path, options = {}] = useDbAndArgs<
    any,
    [string | null | undefined, UseBucketImageOptions?]
  >('useBucketImage', args);
  // Called BEFORE the download so the sidecar read enters the serialized
  // remote queue first: the placeholder should never wait behind its bytes.
  const { hash } = useBlurhash(db, bucketName, options.blurhash !== false ? path : null);
  const file = useDownloadFile(db, bucketName, path, options);

  // The url whose bitmap is decoded. A new url is undecoded until its own
  // gate fires, so `ready` re-arms without an effect.
  const [decodedUrl, setDecodedUrl] = useState<string | null>(null);
  const url = file.url;
  const gate = useCallback(
    (img: HTMLImageElement | null) => {
      if (!img || !url) return;
      const done = () => setDecodedUrl(url);
      if (typeof img.decode === 'function') {
        img.decode().then(done, done);
      } else if (img.complete) {
        done();
      } else {
        img.onload = done;
        img.onerror = done;
      }
    },
    [url]
  );

  return { ...file, blurhash: hash, ready: url !== null && decodedUrl === url, gate };
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

export interface BlurhashProps {
  /** The blurhash string. Nullish paints nothing (transparent canvas). */
  hash: string | null | undefined;
  /** Decode resolution. 32x32 is plenty; size the canvas with CSS. */
  width?: number;
  height?: number;
  /** Contrast punch, see the blurhash reference decoder. Defaults to 1. */
  punch?: number;
  className?: string;
  style?: CSSProperties;
}

/** A blurhash painted onto a canvas, once per hash change. */
export function Blurhash({
  hash,
  width = 32,
  height = 32,
  punch = 1,
  className,
  style,
}: BlurhashProps) {
  const canvas = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const el = canvas.current;
    if (!el) return;
    el.width = width;
    el.height = height;
    if (!hash) return;
    try {
      const pixels = decodeBlurhash(hash, width, height, punch);
      const ctx = el.getContext('2d');
      if (!ctx) return;
      const imageData = ctx.createImageData(width, height);
      imageData.data.set(pixels);
      ctx.putImageData(imageData, 0, 0);
    } catch {
      // An invalid hash paints nothing; the layer below stays visible.
    }
  }, [hash, width, height, punch]);
  return <canvas ref={canvas} className={className} style={style} />;
}

export interface BucketImageProps {
  /** Bucket name from the schema. */
  bucket: string;
  /** Path within the bucket. Nullish renders only the fallback layers. */
  path: string | null | undefined;
  alt?: string;
  /** Class for the container (sizing/positioning). */
  className?: string;
  style?: CSSProperties;
  /** Class for the inner `<img>` (the layout styles are inline). */
  imgClassName?: string;
  /** `object-fit` for the image. Defaults to `cover`. */
  fit?: CSSProperties['objectFit'];
  /** Bottom placeholder layer, shown until the image settles. */
  fallback?: ReactNode;
  /** Crossfade duration in ms. Defaults to 300. */
  transition?: number;
  /** Crossfade easing. Defaults to an ease-out-expo curve. */
  easing?: string;
  /** Resolve the blurhash sidecar. Defaults to true. */
  blurhash?: boolean;
  /** Download tuning, forwarded to `useDownloadFile`. */
  options?: UseBucketImageOptions;
}

const LAYER: CSSProperties = { position: 'absolute', inset: 0, width: '100%', height: '100%' };

function prefersReducedMotion(): boolean {
  return typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/**
 * A bucket image that never pops in. Layers, bottom to top: your `fallback`,
 * the stored blurhash, and the real image, which stays transparent until its
 * bitmap is DECODED and then crossfades in. The placeholder layers unmount
 * once the fade settles. Respects prefers-reduced-motion. The container is
 * made `position: relative` unless your class positions it already.
 */
export function BucketImage(props: BucketImageProps) {
  const image = useBucketImage(props.bucket as BucketNames<SchemaStructure>, props.path, {
    ...props.options,
    blurhash: props.blurhash !== false,
  });
  const [reducedMotion] = useState(prefersReducedMotion);
  const transition = props.transition ?? 300;

  const root = useRef<HTMLDivElement>(null);
  // Layers are absolutely positioned; give them an anchor without stomping on
  // a class that already positions the container.
  useIsomorphicLayoutEffect(() => {
    const el = root.current;
    if (el && getComputedStyle(el).position === 'static') el.style.position = 'relative';
  }, []);

  // Placeholders leave the DOM once the fade is over (a shelf of covers should
  // not composite three layers each forever) and come back with a new url.
  const [settledUrl, setSettledUrl] = useState<string | null>(null);
  useEffect(() => {
    if (!image.ready || !image.url) return;
    const url = image.url;
    const timer = setTimeout(() => setSettledUrl(url), (reducedMotion ? 0 : transition) + 120);
    return () => clearTimeout(timer);
  }, [image.ready, image.url, reducedMotion, transition]);
  const settled = image.ready && settledUrl === image.url;

  return (
    <div ref={root} className={props.className} style={props.style}>
      {!settled && (
        <div style={LAYER}>
          <div style={LAYER}>{props.fallback}</div>
          <Blurhash hash={image.blurhash} style={LAYER} />
        </div>
      )}
      <img
        ref={image.gate}
        src={image.url ?? undefined}
        alt={props.alt ?? ''}
        decoding="async"
        className={props.imgClassName}
        style={{
          ...LAYER,
          objectFit: props.fit ?? 'cover',
          opacity: image.ready ? 1 : 0,
          transition: reducedMotion
            ? 'none'
            : `opacity ${transition}ms ${props.easing ?? 'cubic-bezier(0.16, 1, 0.3, 1)'}`,
        }}
      />
    </div>
  );
}
