import { describe, expect, it, vi } from 'vitest';
import { BucketHandle, bucketContentToBlob } from './bucket-handle';

vi.mock('../utils/blurhash', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../utils/blurhash')>();
  return { ...actual, encodeImageToBlurhash: vi.fn(async () => 'LEHV6nWB2yk8pyo0adR*.7kCMdnj') };
});

function fakeRemote(answers: Record<string, unknown> = {}) {
  const calls: Array<[string, unknown]> = [];
  const remote = {
    query: async (sql: string, vars?: unknown) => {
      calls.push([sql, vars]);
      for (const [needle, answer] of Object.entries(answers)) if (sql.includes(needle)) return [answer];
      return [undefined];
    },
  } as any;
  return { remote, calls };
}

function fakeBlobs() {
  const log: string[] = [];
  return {
    log,
    blobs: {
      invalidate: async (k: { path: string }) => void log.push(`invalidate:${k.path}`),
      read: async (k: { path: string }) => (k.path.endsWith('.bh') ? new Blob(['LEHV6nWB2yk8pyo0adR*.7kCMdnj']) : new Blob(['x'])),
      acquireUrl: async (k: { path: string }) => ({ url: `blob:${k.path}`, release: () => undefined }),
      setPinned: (k: { path: string }, pinned: boolean) => void log.push(`pin:${k.path}:${pinned}`),
      prefetch: async (keys: Array<{ path: string }>) => void log.push(`prefetch:${keys.map((k) => k.path).join('+')}`),
    } as any,
  };
}

describe('bucketContentToBlob', () => {
  it('coerces strings, buffers and views; passes blobs through; rejects the rest', async () => {
    expect(bucketContentToBlob(null)).toBeNull();
    expect(bucketContentToBlob(42)).toBeNull();
    const b = new Blob(['a']);
    expect(bucketContentToBlob(b)).toBe(b);
    expect(await bucketContentToBlob('hi')!.text()).toBe('hi');
    expect((await bucketContentToBlob(new ArrayBuffer(3))!.arrayBuffer()).byteLength).toBe(3);
    expect((await bucketContentToBlob(new Uint8Array([1, 2]))!.arrayBuffer()).byteLength).toBe(2);
  });
});

describe('BucketHandle', () => {
  it('put: uploads, invalidates, writes the blurhash sidecar for images (best-effort), honours the setting', async () => {
    const { remote, calls } = fakeRemote();
    const { blobs, log } = fakeBlobs();
    const warn = vi.fn();
    const h = new BucketHandle('files', remote, blobs, { logger: { warn } });
    const res = await h.put('a/pic.png', new Uint8Array([1]));
    expect(res.blurhash).toBe('LEHV6nWB2yk8pyo0adR*.7kCMdnj');
    expect(calls.map(([s]) => s)).toEqual(['RETURN f"files:/a/pic.png".put($content);', 'RETURN f"files:/a/pic.png.bh".put($content);']);
    expect(log).toEqual(['invalidate:a/pic.png', 'invalidate:a/pic.png.bh']);
    const off = new BucketHandle('files', remote, blobs, { blurhash: false });
    expect((await off.put('b.png', 'x')).blurhash).toBeNull();
    const perCall = new BucketHandle('files', remote, blobs, { blurhash: { componentX: 2 } });
    expect((await perCall.put('c.txt', 'x', { blurhash: true })).blurhash).toBeNull();
    expect((await perCall.put('c.png', 'x', { blurhash: { componentY: 3 } })).blurhash).toBeTruthy();
    const failing = { query: async (sql: string) => { if (sql.includes('.bh')) throw new Error('sidecar'); return [undefined]; } } as any;
    const sidecarFail = new BucketHandle('files', failing, blobs, { logger: { warn } });
    expect((await sidecarFail.put('d.png', 'x')).blurhash).toBeNull();
    expect(warn).toHaveBeenCalled();
    const noBlobs = new BucketHandle('files', remote, null);
    expect((await noBlobs.put('e.png', 'x')).blurhash).toBeTruthy();
  });

  it('blurhash: reads the sidecar through the cache, negative-caches misses, warns on invalid or failing reads', async () => {
    const { remote } = fakeRemote();
    const warn = vi.fn();
    const { blobs } = fakeBlobs();
    const h = new BucketHandle('bucketA', remote, blobs, { logger: { warn } });
    expect(await h.blurhash('img.png')).toBe('LEHV6nWB2yk8pyo0adR*.7kCMdnj');
    const missing = new BucketHandle('bucketB', remote, { ...blobs, read: async () => null } as any, { logger: { warn } });
    expect(await missing.blurhash('none.png')).toBeNull();
    expect(await missing.blurhash('none.png')).toBeNull();
    const invalid = new BucketHandle('bucketC', remote, { ...blobs, read: async () => new Blob(['not-a-hash']) } as any, { logger: { warn } });
    expect(await invalid.blurhash('bad.png')).toBeNull();
    const throwing = new BucketHandle('bucketD', remote, { ...blobs, read: async () => { throw new Error('io'); } } as any, { logger: { warn } });
    expect(await throwing.blurhash('err.png')).toBeNull();
    expect(warn).toHaveBeenCalledTimes(2);
    await new BucketHandle('bucketB', remote, blobs).put('none.png', 'x');
    expect(await new BucketHandle('bucketB', remote, blobs).blurhash('none.png')).toBeTruthy();
  });

  it('get / read / url / pin / evict / prefetch / delete / exists / head / copy / rename / list', async () => {
    const { remote, calls } = fakeRemote({ '.get()': 'content', '.exists()': true, '.head()': { size: 1 }, '.list()': ['a'] });
    const { blobs, log } = fakeBlobs();
    const h = new BucketHandle('files', remote, blobs);
    expect(await h.get('p')).toBe('content');
    expect(await (await h.read('p'))!.text()).toBe('x');
    expect(await new BucketHandle('files', remote, null).read('p')).toBeInstanceOf(Blob);
    expect(await h.url('p')).toEqual({ url: 'blob:p', release: expect.any(Function) });
    expect(await new BucketHandle('files', remote, null).url('p')).toBeNull();
    h.pin('p');
    h.unpin('p');
    await h.evict('p');
    await h.prefetch(['a', 'b']);
    await h.delete('doc.txt');
    await h.delete('img.png');
    const flaky = { query: async (sql: string) => { if (sql.includes('.bh".delete')) throw new Error('nope'); return [undefined]; } } as any;
    await new BucketHandle('files', flaky, blobs).delete('img2.png');
    expect(await h.exists('p')).toBe(true);
    expect(await h.head('p')).toEqual({ size: 1 });
    await h.copy('a', 'b');
    await h.rename('a', 'b');
    expect(await h.list()).toEqual(['a']);
    expect(await h.list('pre/')).toEqual(['a']);
    expect(log).toContain('pin:p:true');
    expect(log).toContain('pin:p:false');
    expect(log).toContain('prefetch:a+b');
    expect(calls.some(([s]) => s === 'RETURN f"files:/img.png.bh".delete();')).toBe(true);
    expect(calls.some(([s]) => s === 'RETURN f"files:/pre/".list();')).toBe(true);
  });
});
