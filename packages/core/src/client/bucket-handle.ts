import type { RemoteDatabaseService } from '../services/database/remote';
import type { BlobCache, BlobReadOptions, BlobUrlLease } from '../services/blobs/index';
import {
  blurhashSidecarPath,
  encodeImageToBlurhash,
  isImagePath,
  isBlurhashValid,
  type BlurhashSetting,
  type BlurhashEncodeOptions,
} from '../utils/blurhash';

/** Coerce whatever the `.get()` RPC hands back into a Blob. */
export function bucketContentToBlob(content: unknown): Blob | null {
  if (content == null) return null;
  if (content instanceof Blob) return content;
  if (typeof content === 'string') return new Blob([content]);
  if (content instanceof ArrayBuffer) return new Blob([content]);
  // Uint8Array and friends. The cast sidesteps `ArrayBufferLike` vs
  // `ArrayBuffer` (SharedArrayBuffer) in lib.dom's BlobPart.
  if (ArrayBuffer.isView(content)) return new Blob([content as unknown as BlobPart]);
  return null;
}

export interface BucketPutOptions {
  /** Override the client-level {@link Sp00kyConfig.blurhash} setting for this put. */
  blurhash?: BlurhashSetting;
}

export interface BucketPutResult {
  /** The computed blurhash when the content was a hashable image; else null. */
  blurhash: string | null;
}

interface BucketHandleSettings {
  blurhash?: BlurhashSetting;
  logger?: { warn: (obj: unknown, msg?: string) => void };
  /** The bucket's backend as generated (`'r2'`: files live with the SSP). */
  backend?: string;
  /** {@link Sp00kyConfig.files}: where `r2` bucket files are read and written. */
  files?: { endpoint?: string };
}

/** A handle's settings from the client config and the bucket's generated schema. */
export function bucketSettings(
  config: { blurhash?: BlurhashSetting; files?: { endpoint?: string }; schema: { buckets?: readonly { name: string; backend?: string }[] } },
  name: string,
  logger: { warn: (obj: unknown, msg?: string) => void }
): BucketHandleSettings {
  const backend = config.schema.buckets?.find((b) => b.name === name)?.backend;
  return { blurhash: config.blurhash, logger, backend, files: config.files };
}

/** Where a bucket's bytes go: SurrealDB itself, or the SSP for `r2` buckets. */
interface BucketIo {
  put(path: string, content: string | Uint8Array | Blob): Promise<void>;
  /** The file, or null when it does not exist. */
  get(path: string): Promise<unknown>;
  delete(path: string): Promise<void>;
  exists(path: string): Promise<boolean>;
  head(path: string): Promise<Record<string, unknown>>;
  copy(sourcePath: string, targetPath: string): Promise<void>;
  rename(sourcePath: string, targetPath: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
}

/** `f"bucket:/path"` statements over the database connection. */
function surrealIo(bucket: string, remote: RemoteDatabaseService): BucketIo {
  const file = (path: string) => `f"${bucket}:/${path}"`;
  return {
    async put(path, content) {
      await remote.query(`RETURN ${file(path)}.put($content);`, { content });
    },
    async get(path) {
      const [result] = await remote.query<[unknown]>(`RETURN ${file(path)}.get();`);
      return result;
    },
    async delete(path) {
      await remote.query(`RETURN ${file(path)}.delete();`);
    },
    async exists(path) {
      const [result] = await remote.query<[boolean]>(`RETURN ${file(path)}.exists();`);
      return result;
    },
    async head(path) {
      const [result] = await remote.query<[Record<string, unknown>]>(`RETURN ${file(path)}.head();`);
      return result;
    },
    async copy(sourcePath, targetPath) {
      await remote.query(`RETURN ${file(sourcePath)}.copy($target);`, { target: targetPath });
    },
    async rename(sourcePath, targetPath) {
      await remote.query(`RETURN ${file(sourcePath)}.rename($target);`, { target: targetPath });
    },
    async list(prefix) {
      const [result] = await remote.query<[string[]]>(`RETURN ${file(prefix)}.list();`);
      return result;
    },
  };
}

/**
 * `BACKEND "r2"` buckets: the SSP keeps the files (R2 on Cloudflare, a
 * directory under `spky dev`) and serves them at `/bucket/<name>/<key>`,
 * after asking SurrealDB, as the signed-in user, whether the bucket's
 * PERMISSIONS clause allows it. The request carries the session's token.
 */
function sspIo(bucket: string, remote: RemoteDatabaseService, endpoint: string | undefined): BucketIo {
  const base = () => {
    if (!endpoint) {
      throw new Error(
        `Bucket '${bucket}' is stored with the SSP (BACKEND "r2"): set \`files.endpoint\` in the client config to the SSP's URL.`
      );
    }
    return endpoint.replace(/\/+$/, '');
  };
  const url = (path: string) =>
    `${base()}/bucket/${encodeURIComponent(bucket)}/${path
      .split('/')
      .filter(Boolean)
      .map(encodeURIComponent)
      .join('/')}`;
  const send = async (method: string, path: string, body?: BodyInit, contentType?: string): Promise<Response> => {
    const headers: Record<string, string> = {};
    const token = remote.getAuthToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    if (contentType) headers['Content-Type'] = contentType;
    const response = await fetch(url(path), { method, headers, body, credentials: 'omit' });
    if (!response.ok && response.status !== 404) {
      let message = response.statusText;
      try {
        message = ((await response.json()) as { error?: string }).error ?? message;
      } catch {
        // Not JSON: keep the status text.
      }
      throw new Error(`Bucket '${bucket}' ${method} ${path} failed (${response.status}): ${message}`);
    }
    return response;
  };
  const op = async <T>(path: string, body: Record<string, unknown>): Promise<T | null> => {
    const response = await send('POST', path, JSON.stringify(body), 'application/json');
    if (response.status === 404) return null;
    return (await response.json()) as T;
  };
  const missing = (path: string) => new Error(`Bucket '${bucket}': no file at ${path}`);
  return {
    async put(path, content) {
      const type = content instanceof Blob ? content.type : undefined;
      await send('PUT', path, content as BodyInit, type || 'application/octet-stream');
    },
    async get(path) {
      const response = await send('GET', path);
      return response.status === 404 ? null : await response.blob();
    },
    async delete(path) {
      await send('DELETE', path);
    },
    async exists(path) {
      return (await op<boolean>(path, { op: 'exists' })) ?? false;
    },
    async head(path) {
      return (await op<Record<string, unknown>>(path, { op: 'head' })) as Record<string, unknown>;
    },
    async copy(sourcePath, targetPath) {
      if ((await op(sourcePath, { op: 'copy', target: targetPath })) === null) throw missing(sourcePath);
    },
    async rename(sourcePath, targetPath) {
      if ((await op(sourcePath, { op: 'rename', target: targetPath })) === null) throw missing(sourcePath);
    },
    async list() {
      // SurrealDB refuses `list` to record users; the SSP does too.
      throw new Error(`Bucket '${bucket}': listing is not available (BACKEND "r2")`);
    },
  };
}

/**
 * Paths known to have no blurhash sidecar, per tab. The blob cache has no
 * negative caching, so without this every mount of a hashless image would pay
 * one serialized remote read. Cleared when a put writes a sidecar or a delete
 * removes the image. Keyed `${bucket}:${path}` (the image path, not the sidecar).
 */
const missingBlurhash = new Set<string>();

export class BucketHandle {
  private io: BucketIo;

  constructor(
    private bucketName: string,
    remote: RemoteDatabaseService,
    /** Absent on the raw handle the cache itself reads through. */
    private blobs?: BlobCache | null,
    private settings?: BucketHandleSettings
  ) {
    this.io =
      settings?.backend === 'r2'
        ? sspIo(bucketName, remote, settings.files?.endpoint)
        : surrealIo(bucketName, remote);
  }

  /** Effective blurhash setting: per-call option > client config > default ON. */
  private resolveBlurhash(option?: BlurhashSetting): BlurhashEncodeOptions | null {
    const setting = option ?? this.settings?.blurhash ?? true;
    if (setting === false) return null;
    return setting === true ? {} : setting;
  }

  async put(
    path: string,
    content: string | Uint8Array | Blob,
    options?: BucketPutOptions
  ): Promise<BucketPutResult> {
    // Start hashing while the upload is in flight; both browser-side costs
    // overlap and the sidecar put only queues once the main put resolved.
    const encodeOptions = this.resolveBlurhash(options?.blurhash);
    const hashPromise =
      encodeOptions && isImagePath(path)
        ? encodeImageToBlurhash(content, encodeOptions)
        : Promise.resolve(null);

    await this.io.put(path, content);
    // A path can be overwritten, so anything cached under it is now wrong.
    await this.blobs?.invalidate({ bucket: this.bucketName, path });

    // The sidecar is best-effort: a hash or sidecar failure must never fail
    // the image put that triggered it.
    let hash: string | null = null;
    try {
      hash = await hashPromise;
      if (hash) {
        const sidecar = blurhashSidecarPath(path);
        await this.io.put(sidecar, hash);
        await this.blobs?.invalidate({ bucket: this.bucketName, path: sidecar });
        missingBlurhash.delete(`${this.bucketName}:${path}`);
      }
    } catch (error) {
      hash = null;
      this.settings?.logger?.warn({ error, path }, 'blurhash sidecar put failed');
    }
    return { blurhash: hash };
  }

  /**
   * The blurhash stored alongside an uploaded image (see
   * {@link blurhashSidecarPath}), or null when there is none. Reads through the
   * blob cache, so a warm client answers from OPFS without a network hop, and
   * misses are remembered per tab so a hashless image costs at most one
   * serialized remote read per session.
   */
  async blurhash(path: string): Promise<string | null> {
    const cacheKey = `${this.bucketName}:${path}`;
    if (missingBlurhash.has(cacheKey)) return null;
    try {
      const blob = await this.read(blurhashSidecarPath(path), { persist: true });
      if (!blob) {
        missingBlurhash.add(cacheKey);
        return null;
      }
      const hash = (await blob.text()).trim();
      if (!isBlurhashValid(hash).result) {
        this.settings?.logger?.warn({ path }, 'blurhash sidecar holds an invalid hash');
        missingBlurhash.add(cacheKey);
        return null;
      }
      return hash;
    } catch (error) {
      // Transient failure: do NOT negative-cache, the next mount may succeed.
      this.settings?.logger?.warn({ error, path }, 'blurhash sidecar read failed');
      return null;
    }
  }

  async get(path: string): Promise<unknown> {
    return this.io.get(path);
  }

  /**
   * Read through the local blob cache: OPFS first, the bucket second. Unlike
   * {@link get} this survives a reload and works offline. Returns null when the
   * file exists in neither place.
   */
  async read(path: string, options?: BlobReadOptions): Promise<Blob | null> {
    if (!this.blobs) return bucketContentToBlob(await this.get(path));
    return this.blobs.read({ bucket: this.bucketName, path }, options);
  }

  /**
   * A refcounted object URL for `path`, suitable for `<img src>`. The caller
   * MUST call `release()` when the URL goes off screen. Returns null when the
   * file does not exist, or when object URLs are unavailable (non-browser).
   */
  async url(path: string, options?: BlobReadOptions): Promise<BlobUrlLease | null> {
    if (!this.blobs) return null;
    return this.blobs.acquireUrl({ bucket: this.bucketName, path }, options);
  }

  /** Exempt `path` from pressure eviction. Pinned bytes never expire. */
  pin(path: string): void {
    this.blobs?.setPinned({ bucket: this.bucketName, path }, true);
  }

  unpin(path: string): void {
    this.blobs?.setPinned({ bucket: this.bucketName, path }, false);
  }

  /** Drop `path` from the local cache without touching the remote file. */
  async evict(path: string): Promise<void> {
    await this.blobs?.invalidate({ bucket: this.bucketName, path });
  }

  /** Warm the cache for offline use. Already-cached paths are skipped. */
  async prefetch(paths: string[]): Promise<void> {
    await this.blobs?.prefetch(paths.map((path) => ({ bucket: this.bucketName, path })));
  }

  async delete(path: string): Promise<void> {
    await this.io.delete(path);
    await this.blobs?.invalidate({ bucket: this.bucketName, path });
    // Symmetry with put: an image's blurhash sidecar dies with it. Best-effort,
    // the sidecar may simply not exist.
    if (isImagePath(path)) {
      const sidecar = blurhashSidecarPath(path);
      try {
        await this.io.delete(sidecar);
        await this.blobs?.invalidate({ bucket: this.bucketName, path: sidecar });
      } catch {
        // Nothing to clean up.
      }
      missingBlurhash.delete(`${this.bucketName}:${path}`);
    }
  }

  async exists(path: string): Promise<boolean> {
    return this.io.exists(path);
  }

  async head(path: string): Promise<Record<string, unknown>> {
    return this.io.head(path);
  }

  async copy(sourcePath: string, targetPath: string): Promise<void> {
    await this.io.copy(sourcePath, targetPath);
  }

  async rename(sourcePath: string, targetPath: string): Promise<void> {
    await this.io.rename(sourcePath, targetPath);
  }

  async list(prefix?: string): Promise<string[]> {
    return this.io.list(prefix ?? '');
  }
}

