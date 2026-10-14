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

describe('r2 buckets (files with the SSP)', () => {
  function fakeSsp(files: Record<string, string> = {}) {
    const requests: Array<{ method: string; url: string; auth: string | null; body: unknown }> = [];
    const fetchMock = vi.fn(async (url: string, init: RequestInit) => {
      const headers = (init.headers ?? {}) as Record<string, string>;
      requests.push({ method: init.method ?? 'GET', url, auth: headers.Authorization ?? null, body: init.body });
      const key = decodeURIComponent(new URL(url).pathname.replace(/^\/bucket\/[^/]+\//, ''));
      const json = (status: number, value: unknown) => new Response(JSON.stringify(value), { status });
      if (headers.Authorization !== 'Bearer tok') return json(401, { error: 'invalid or expired token' });
      switch (init.method) {
        case 'PUT':
          files[key] = typeof init.body === 'string' ? init.body : 'bytes';
          return json(200, { ok: true });
        case 'GET':
          return key in files ? new Response(files[key], { status: 200 }) : json(404, { error: 'no such file' });
        case 'DELETE':
          delete files[key];
          return json(200, { ok: true });
        default: {
          const op = JSON.parse(String(init.body));
          if (op.op === 'exists') return json(200, key in files);
          if (op.op === 'head') return json(200, key in files ? { key: `/${key}`, size: files[key].length } : null);
          if (!(key in files)) return json(404, { error: 'no such file' });
          files[op.target] = files[key];
          if (op.op === 'rename') delete files[key];
          return json(200, { ok: true });
        }
      }
    });
    vi.stubGlobal('fetch', fetchMock);
    return { requests, files };
  }

  const remote = (token: string | null = 'tok') => ({ getAuthToken: () => token, query: vi.fn() }) as any;
  const r2 = (r = remote()) =>
    new BucketHandle('pics', r, null, { backend: 'r2', files: { endpoint: 'https://app.db.example/' }, blurhash: false });

  it('reads and writes over HTTP with the session token, never the database socket', async () => {
    const { requests, files } = fakeSsp();
    const db = remote();
    const h = r2(db);
    await h.put('u1/a b.png', 'PNG');
    expect(files['u1/a b.png']).toBe('PNG');
    expect(requests[0]).toMatchObject({ method: 'PUT', url: 'https://app.db.example/bucket/pics/u1/a%20b.png', auth: 'Bearer tok' });
    expect(await ((await h.get('u1/a b.png')) as Blob).text()).toBe('PNG');
    expect(await h.get('missing.png')).toBeNull();
    expect(await h.exists('u1/a b.png')).toBe(true);
    expect(await h.head('u1/a b.png')).toEqual({ key: '/u1/a b.png', size: 3 });
    await h.copy('u1/a b.png', 'u1/c.png');
    await h.rename('u1/c.png', 'u1/d.png');
    expect(Object.keys(files).sort()).toEqual(['u1/a b.png', 'u1/d.png']);
    await expect(h.copy('nope.png', 'x.png')).rejects.toThrow(/no file/);
    await h.delete('u1/a b.png');
    expect(await h.exists('u1/a b.png')).toBe(false);
    await expect(h.list()).rejects.toThrow(/listing/);
    expect(db.query).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it('surfaces refusals and a missing endpoint as errors', async () => {
    fakeSsp({ 'a.png': 'x' });
    await expect(r2(remote(null)).get('a.png')).rejects.toThrow(/401.*invalid or expired token/);
    const noEndpoint = new BucketHandle('pics', remote(), null, { backend: 'r2', files: {} });
    await expect(noEndpoint.get('a.png')).rejects.toThrow(/files\.endpoint/);
    vi.unstubAllGlobals();
  });

  it('a bucket without backend r2 keeps using the database', async () => {
    const db = { getAuthToken: () => 'tok', query: vi.fn(async () => ['content']) } as any;
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    const h = new BucketHandle('pics', db, null, { files: { endpoint: 'https://app.db.example' } });
    expect(await h.get('a.png')).toBe('content');
    expect(fetchMock).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });
});
