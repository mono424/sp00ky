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
}

/**
 * Paths known to have no blurhash sidecar, per tab. The blob cache has no
 * negative caching, so without this every mount of a hashless image would pay
 * one serialized remote read. Cleared when a put writes a sidecar or a delete
 * removes the image. Keyed `${bucket}:${path}` (the image path, not the sidecar).
 */
const missingBlurhash = new Set<string>();

export class BucketHandle {
  constructor(
    private bucketName: string,
    private remote: RemoteDatabaseService,
    /** Absent on the raw handle the cache itself reads through. */
    private blobs?: BlobCache | null,
    private settings?: BucketHandleSettings
  ) {}

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

    await this.remote.query(`RETURN f"${this.bucketName}:/${path}".put($content);`, { content });
    // A path can be overwritten, so anything cached under it is now wrong.
    await this.blobs?.invalidate({ bucket: this.bucketName, path });

    // The sidecar is best-effort: a hash or sidecar failure must never fail
    // the image put that triggered it.
    let hash: string | null = null;
    try {
      hash = await hashPromise;
      if (hash) {
        const sidecar = blurhashSidecarPath(path);
        await this.remote.query(`RETURN f"${this.bucketName}:/${sidecar}".put($content);`, {
          content: hash,
        });
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
    const [result] = await this.remote.query<[unknown]>(
      `RETURN f"${this.bucketName}:/${path}".get();`
    );
    return result;
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
    await this.remote.query(`RETURN f"${this.bucketName}:/${path}".delete();`);
    await this.blobs?.invalidate({ bucket: this.bucketName, path });
    // Symmetry with put: an image's blurhash sidecar dies with it. Best-effort,
    // the sidecar may simply not exist.
    if (isImagePath(path)) {
      const sidecar = blurhashSidecarPath(path);
      try {
        await this.remote.query(`RETURN f"${this.bucketName}:/${sidecar}".delete();`);
        await this.blobs?.invalidate({ bucket: this.bucketName, path: sidecar });
      } catch {
        // Nothing to clean up.
      }
      missingBlurhash.delete(`${this.bucketName}:${path}`);
    }
  }

  async exists(path: string): Promise<boolean> {
    const [result] = await this.remote.query<[boolean]>(
      `RETURN f"${this.bucketName}:/${path}".exists();`
    );
    return result;
  }

  async head(path: string): Promise<Record<string, unknown>> {
    const [result] = await this.remote.query<[Record<string, unknown>]>(
      `RETURN f"${this.bucketName}:/${path}".head();`
    );
    return result;
  }

  async copy(sourcePath: string, targetPath: string): Promise<void> {
    await this.remote.query(`RETURN f"${this.bucketName}:/${sourcePath}".copy($target);`, {
      target: targetPath,
    });
  }

  async rename(sourcePath: string, targetPath: string): Promise<void> {
    await this.remote.query(`RETURN f"${this.bucketName}:/${sourcePath}".rename($target);`, {
      target: targetPath,
    });
  }

  async list(prefix?: string): Promise<string[]> {
    const p = prefix ?? '';
    const [result] = await this.remote.query<[string[]]>(
      `RETURN f"${this.bucketName}:/${p}".list();`
    );
    return result;
  }
}

