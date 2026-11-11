import { Component, provideZonelessChangeDetection, signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { describe, expect, it, vi } from 'vitest';
import {
  BucketImage,
  ImageGate,
  injectBucketImage,
  injectDownloadFile,
  injectFileUpload,
  SyncedDb,
} from '@spooky-sync/client-angular';
import { inject, provideDb, tick } from './helpers';

/** A db with one fake bucket whose leases the test can count. */
function bucketDb() {
  const leases = new Map<string, number>();
  const put = vi.fn(async (path: string) => ({ path }));
  const bucket = {
    url: vi.fn(async (path: string) => {
      leases.set(path, (leases.get(path) ?? 0) + 1);
      return {
        url: 'blob:' + path,
        release: () => leases.set(path, (leases.get(path) ?? 1) - 1),
      };
    }),
    blurhash: vi.fn(async () => 'LEHV6nWB2yk8pyo0adR*.7kCMdnj'),
    put,
  };
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  (db as any).sp00ky = { bucket: () => bucket };
  (db as any).config = { schema: { buckets: [{ name: 'avatars', maxSize: 10 }] } };
  return { db, bucket, leases, put };
}

describe('injectFileUpload', () => {
  it('validates maxSize before uploading', async () => {
    const b = bucketDb();
    provideDb(b.db);
    const files = inject(() => injectFileUpload('avatars'));
    await files.upload('big.png', new Blob(['x'.repeat(20)]));
    expect(files.error()?.message).toMatch(/maximum size/);
    expect(b.put).not.toHaveBeenCalled();

    await files.upload('small.png', new Blob(['x']));
    expect(files.error()).toBeNull();
    expect(b.put).toHaveBeenCalledTimes(1);
    expect(files.isUploading()).toBe(false);
  });
});

describe('injectDownloadFile', () => {
  it('follows the path, keeps the old url until the next is ready, releases leases', async () => {
    const b = bucketDb();
    provideDb(b.db);
    const path = signal<string | null>('a.png');
    const file = inject(() => injectDownloadFile('avatars', path));
    expect(file.isLoading()).toBe(true);
    await tick();
    expect(file.url()).toBe('blob:a.png');

    path.set('b.png');
    TestBed.tick();
    expect(file.url()).toBe('blob:a.png');
    await tick();
    expect(file.url()).toBe('blob:b.png');
    expect(b.leases.get('a.png')).toBe(0);

    path.set(null);
    TestBed.tick();
    expect(file.url()).toBeNull();
    expect(b.leases.get('b.png')).toBe(0);
  });
});

describe('injectBucketImage and the image gate', () => {
  it('sets src, then flips ready once the bitmap decodes', async () => {
    const b = bucketDb();
    const decode = vi
      .spyOn(HTMLImageElement.prototype, 'decode')
      .mockImplementation(() => Promise.resolve());

    @Component({
      imports: [ImageGate],
      template: `<img [sp00kyImageGate]="image" alt="" />`,
    })
    class Host {
      readonly image = injectBucketImage('avatars', () => 'a.png');
    }

    TestBed.configureTestingModule({
      providers: [provideZonelessChangeDetection(), { provide: SyncedDb, useValue: b.db }],
    });
    const fixture = TestBed.createComponent(Host);
    await fixture.whenStable();
    await tick();
    await fixture.whenStable();
    const img = fixture.nativeElement.querySelector('img') as HTMLImageElement;
    expect(img.getAttribute('src')).toBe('blob:a.png');
    expect(fixture.componentInstance.image.ready()).toBe(true);
    expect(fixture.componentInstance.image.blurhash()).toBe('LEHV6nWB2yk8pyo0adR*.7kCMdnj');
    decode.mockRestore();
  });
});

describe('<sp00ky-bucket-image>', () => {
  it('projects the fallback, paints the blurhash layer and gates the image', async () => {
    const b = bucketDb();
    const decode = vi
      .spyOn(HTMLImageElement.prototype, 'decode')
      .mockImplementation(() => Promise.resolve());

    @Component({
      imports: [BucketImage],
      template: `
        <sp00ky-bucket-image bucket="avatars" [path]="path()" alt="me" imgClass="rounded">
          <div class="plate"></div>
        </sp00ky-bucket-image>
      `,
    })
    class Host {
      readonly path = signal<string | null>('a.png');
    }

    TestBed.configureTestingModule({
      providers: [provideZonelessChangeDetection(), { provide: SyncedDb, useValue: b.db }],
    });
    const fixture = TestBed.createComponent(Host);
    await fixture.whenStable();
    await tick();
    await fixture.whenStable();

    const host = fixture.nativeElement.querySelector('sp00ky-bucket-image') as HTMLElement;
    const img = host.querySelector('img') as HTMLImageElement;
    expect(host.querySelector('.plate')).not.toBeNull();
    expect(host.querySelector('canvas')).not.toBeNull();
    expect(img.getAttribute('src')).toBe('blob:a.png');
    expect(img.getAttribute('alt')).toBe('me');
    expect(img.className).toBe('rounded');
    expect(img.style.opacity).toBe('1');
    expect(host.style.position).toBe('relative');

    fixture.componentInstance.path.set('b.png');
    await fixture.whenStable();
    await tick();
    await fixture.whenStable();
    expect(img.getAttribute('src')).toBe('blob:b.png');
    expect(b.leases.get('a.png')).toBe(0);
    decode.mockRestore();
  });
});
