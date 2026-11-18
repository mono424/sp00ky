import { describe, it, expect } from 'vitest';
import { MemoryFile, RowStore, cmpText, hash32, runId, type StoreIo, type StoreOp } from './rows-store';

const enc = new TextEncoder();
const dec = new TextDecoder();

/** Two in-memory files, reopenable: what a bucket's OPFS directory holds. */
class Disk implements StoreIo {
  files: Record<'a' | 'b', MemoryFile> = { a: new MemoryFile(), b: new MemoryFile() };
  opened: string[] = [];
  open(name: 'a' | 'b'): MemoryFile {
    this.opened.push(name);
    return this.files[name];
  }
  /** A copy as a crash would leave it, optionally cut to `size` bytes. */
  crashed(cut?: { file: 'a' | 'b'; size: number }): Disk {
    const copy = new Disk();
    for (const name of ['a', 'b'] as const) {
      let bytes = this.files[name].snapshot();
      if (cut?.file === name) bytes = bytes.slice(0, cut.size);
      copy.files[name] = MemoryFile.from(bytes);
    }
    return copy;
  }
}

const put = (table: string, id: string, value: unknown, rv = 0): StoreOp => ({
  t: 'put',
  table,
  id,
  rv,
  flags: 0,
  body: enc.encode(JSON.stringify(value)),
});
const del = (table: string, id: string): StoreOp => ({ t: 'del', table, id });

function read(store: RowStore, table: string, id: string): unknown {
  const hit = store.get(table, id);
  return hit ? JSON.parse(dec.decode(hit.body)) : null;
}

/** Every row of a table, in the store's order. */
function dump(store: RowStore, table: string): [string, unknown][] {
  const run = store.ordered(table);
  const out: [string, unknown][] = [];
  for (let i = 0; i < run.n; i++) {
    const body = new Uint8Array(run.len[i]);
    store.readInto(run.off[i], run.len[i], body, 0);
    out.push([runId(run, i), JSON.parse(dec.decode(body))]);
  }
  return out;
}

describe('RowStore', () => {
  it('reads back what it wrote, before and after a reopen', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write([put('game', 'game:b', { n: 2 }, 7), put('game', 'game:a', { n: 1 }, 3)]);
    store.write([put('user', 'user:x', { name: 'Zoë' })]);
    expect(read(store, 'game', 'game:a')).toEqual({ n: 1 });
    expect(store.find('game', 'game:b')?.rv).toBe(7);
    expect(read(store, 'game', 'game:missing')).toBeNull();
    expect(read(store, 'nope', 'x')).toBeNull();
    store.close();

    const again = await RowStore.open(disk);
    expect(dump(again, 'game')).toEqual([
      ['game:a', { n: 1 }],
      ['game:b', { n: 2 }],
    ]);
    expect(read(again, 'user', 'user:x')).toEqual({ name: 'Zoë' });
    expect(again.versions('game')).toEqual([
      ['game:a', 3],
      ['game:b', 7],
    ]);
    expect(again.tableNames()).toEqual(['game', 'user']);
  });

  it('opens one file when nothing was compacted', async () => {
    const disk = new Disk();
    (await RowStore.open(disk)).close();
    disk.opened = [];
    (await RowStore.open(disk)).close();
    expect(disk.opened).toEqual(['a']);
  });

  it('overwrites, deletes and clears', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write([put('t', 'a', 1), put('t', 'b', 2), put('t', 'c', 3)]);
    store.write([put('t', 'a', 10), del('t', 'b'), del('t', 'never')]);
    expect(dump(store, 't')).toEqual([
      ['a', 10],
      ['c', 3],
    ]);
    expect(store.count('t')).toBe(2);
    store.checkpoint();
    store.write([{ t: 'clear', table: 't' }, put('t', 'z', 26)]);
    expect(dump(store, 't')).toEqual([['z', 26]]);
    store.close();
    expect(dump(await RowStore.open(disk), 't')).toEqual([['z', 26]]);
  });

  it('applies the ops of one batch in order', async () => {
    const store = await RowStore.open(new Disk());
    store.write([put('t', 'a', 1), put('t', 'a', 2), del('t', 'b'), put('t', 'b', 3), put('t', 'c', 4), del('t', 'c')]);
    expect(dump(store, 't')).toEqual([
      ['a', 2],
      ['b', 3],
    ]);
  });

  it('orders ids by their UTF-8 bytes', async () => {
    const store = await RowStore.open(new Disk());
    // U+FFFF sorts above a surrogate pair in UTF-16 units and below it in UTF-8.
    const ids = ['b', 'a', '￿', '\u{1f600}', 'é', 'ab', 'A', ''];
    store.write(ids.map((id, i) => put('t', id, i)));
    const expected = [...ids].sort((x, y) => Buffer.compare(Buffer.from(x), Buffer.from(y)));
    expect(dump(store, 't').map(([id]) => id)).toEqual(expected);
    store.checkpoint();
    for (const id of ids) expect(read(store, 't', id)).toBe(ids.indexOf(id));
    expect([...ids].sort(cmpText)).toEqual(expected);
  });

  it('replays only the writes since the last checkpoint', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write(Array.from({ length: 500 }, (_, i) => put('t', `t:${i}`, { i })));
    expect(store.checkpoint()).toBe(true);
    expect(store.checkpoint()).toBe(false);
    expect(store.tailBytes).toBe(0);
    store.write([put('t', 't:7', { i: 'seven' }), del('t', 't:8'), put('u', 'u:1', 1)]);
    expect(store.tailBytes).toBeGreaterThan(0);
    store.close();

    const again = await RowStore.open(disk);
    expect(again.tailBytes).toBeGreaterThan(0);
    expect(again.tailBytes).toBeLessThan(200);
    expect(read(again, 't', 't:7')).toEqual({ i: 'seven' });
    expect(read(again, 't', 't:8')).toBeNull();
    expect(read(again, 't', 't:499')).toEqual({ i: 499 });
    expect(read(again, 'u', 'u:1')).toBe(1);
    expect(again.count('t')).toBe(499);
    expect(again.stats().rebuilt).toBe(false);
  });

  it('keeps every whole batch and no part of a torn one, wherever the file is cut', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    const sizes: number[] = [];
    const states: [string, unknown][][] = [];
    const batches: StoreOp[][] = [
      [put('t', 'a', 1), put('t', 'b', 2)],
      [put('t', 'a', 11), del('t', 'b'), put('t', 'c', 3)],
      [put('t', 'd', 'x'.repeat(300))],
      [del('t', 'a'), put('t', 'e', { deep: [1, 2, 3] })],
    ];
    sizes.push(disk.files.a.size());
    states.push([]);
    for (const batch of batches) {
      store.write(batch);
      sizes.push(disk.files.a.size());
      states.push(dump(store, 't'));
    }
    const full = disk.files.a.size();
    for (let size = sizes[0]; size <= full; size++) {
      const reopened = await RowStore.open(disk.crashed({ file: 'a', size }));
      // The batches that fit whole inside `size`. An extent ends in up to 7
      // bytes of padding, and a batch cut inside its padding is still whole.
      let whole = 0;
      while (whole + 1 < sizes.length && sizes[whole + 1] <= size) whole++;
      const got = dump(reopened, 't');
      const inPadding = whole + 1 < sizes.length && sizes[whole + 1] - size < 8;
      if (inPadding && JSON.stringify(got) === JSON.stringify(states[whole + 1])) whole++;
      expect(got, `cut at ${size}`).toEqual(states[whole]);
      // And the store keeps working after the cut.
      reopened.write([put('t', 'after', true)]);
      expect(read(reopened, 't', 'after')).toBe(true);
    }
  });

  it('drops a batch whose bytes were damaged in place', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write([put('t', 'a', 1)]);
    const good = disk.files.a.size();
    store.write([put('t', 'b', 2)]);
    const bytes = disk.files.a.snapshot();
    bytes[good + 20] ^= 0xff;
    const damaged = new Disk();
    damaged.files.a = MemoryFile.from(bytes);
    const reopened = await RowStore.open(damaged);
    expect(dump(reopened, 't')).toEqual([['a', 1]]);
    expect(damaged.files.a.size()).toBe(good);
  });

  it('survives a checkpoint torn before its superblock', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write([put('t', 'a', 1), put('t', 'b', 2)]);
    store.checkpoint();
    store.write([put('t', 'c', 3)]);
    const before = disk.crashed();
    store.checkpoint();
    // The blocks and directory of the second checkpoint reached the file, its superblock did not.
    const torn = disk.crashed();
    torn.files.a.write(before.files.a.snapshot().slice(0, 512), 0);
    const reopened = await RowStore.open(torn);
    expect(dump(reopened, 't')).toEqual([
      ['a', 1],
      ['b', 2],
      ['c', 3],
    ]);
    reopened.write([put('t', 'd', 4)]);
    reopened.checkpoint();
    reopened.close();
    expect((await RowStore.open(torn)).count('t')).toBe(4);
  });

  it('rebuilds the index from the log when a block does not match', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write(Array.from({ length: 50 }, (_, i) => put('t', `t:${String(i).padStart(2, '0')}`, i)));
    const logEnd = disk.files.a.size();
    store.checkpoint();
    store.write([put('t', 't:99', 99)]);
    store.close();
    const bytes = disk.files.a.snapshot();
    bytes[logEnd + 16 + 3] ^= 0x40; // inside the table block
    const damaged = new Disk();
    damaged.files.a = MemoryFile.from(bytes);
    const reopened = await RowStore.open(damaged);
    expect(reopened.stats().rebuilt).toBe(false);
    expect(reopened.count('t')).toBe(51);
    expect(reopened.stats().rebuilt).toBe(true);
    expect(read(reopened, 't', 't:07')).toBe(7);
    expect(read(reopened, 't', 't:99')).toBe(99);
    reopened.checkpoint();
    reopened.close();
    const healed = await RowStore.open(damaged);
    expect(healed.count('t')).toBe(51);
    expect(healed.stats().rebuilt).toBe(false);
  });

  it('rebuilds from the log when both superblocks are lost', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write([put('t', 'a', 1)]);
    store.checkpoint();
    store.write([put('u', 'b', 2)]);
    store.close();
    const bytes = disk.files.a.snapshot();
    bytes.fill(0, 0, 512);
    const damaged = new Disk();
    damaged.files.a = MemoryFile.from(bytes);
    const reopened = await RowStore.open(damaged);
    expect(reopened.stats().rebuilt).toBe(true);
    expect(read(reopened, 't', 'a')).toBe(1);
    expect(read(reopened, 'u', 'b')).toBe(2);
  });

  it('compacts into the other file and back, keeping every row', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    for (let round = 0; round < 20; round++) {
      store.write(Array.from({ length: 100 }, (_, i) => put('t', `t:${i}`, { round, i, pad: 'p'.repeat(100) }, round)));
    }
    store.write([put('small', 's:1', 'kept'), { t: 'clear', table: 'emptied' }]);
    store.checkpoint();
    const before = store.stats();
    expect(before.garbageBytes).toBeGreaterThan(before.liveBytes * 10);

    await store.compact();
    const after = store.stats();
    expect(after.file).toBe('b');
    expect(after.garbageBytes).toBeLessThan(2048);
    expect(after.fileBytes).toBeLessThan(before.fileBytes / 10);
    expect(disk.files.a.size()).toBe(4096);
    expect(read(store, 't', 't:42')).toEqual({ round: 19, i: 42, pad: 'p'.repeat(100) });
    expect(store.find('t', 't:42')?.rv).toBe(19);
    store.write([put('t', 't:new', 'after compaction')]);
    store.close();

    disk.opened = [];
    const reopened = await RowStore.open(disk);
    expect(disk.opened).toEqual(['a', 'b']);
    expect(reopened.count('t')).toBe(101);
    expect(read(reopened, 't', 't:new')).toBe('after compaction');
    expect(read(reopened, 'small', 's:1')).toBe('kept');
    expect(reopened.tableNames()).toEqual(['emptied', 'small', 't']);

    await reopened.compact();
    expect(reopened.stats().file).toBe('a');
    reopened.close();
    disk.opened = [];
    const third = await RowStore.open(disk);
    expect(disk.opened).toEqual(['a']);
    expect(third.count('t')).toBe(101);
    expect(read(third, 't', 't:0')).toEqual({ round: 19, i: 0, pad: 'p'.repeat(100) });
  });

  it('finds the rows whichever side of a compaction a crash lands on', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write(Array.from({ length: 30 }, (_, i) => put('t', `t:${i}`, i)));
    store.write(Array.from({ length: 30 }, (_, i) => put('t', `t:${i}`, i * 2)));
    const expected = dump(store, 't');
    const beforeA = disk.files.a.snapshot();
    await store.compact();

    // Crash before the commit: `b` is whole, `a` never learned about it.
    const early = new Disk();
    early.files.a = MemoryFile.from(beforeA);
    early.files.b = MemoryFile.from(disk.files.b.snapshot());
    expect(dump(await RowStore.open(early), 't')).toEqual(expected);

    // Crash after it.
    expect(dump(await RowStore.open(disk.crashed()), 't')).toEqual(expected);

    // A compaction that died halfway through writing `b`.
    const half = new Disk();
    half.files.a = MemoryFile.from(beforeA);
    half.files.b = MemoryFile.from(disk.files.b.snapshot().slice(0, 5000));
    expect(dump(await RowStore.open(half), 't')).toEqual(expected);
  });

  it('rebuilds a compacted file from its own extents', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    store.write([put('t', 'a', 1), put('u', 'b', 2), { t: 'clear', table: 'empty' }]);
    await store.compact();
    store.close();
    const bytes = disk.files.b.snapshot();
    bytes.fill(0, 0, 512);
    const damaged = new Disk();
    damaged.files.b = MemoryFile.from(bytes);
    const reopened = await RowStore.open(damaged);
    expect(reopened.stats().rebuilt).toBe(true);
    expect(read(reopened, 't', 'a')).toBe(1);
    expect(read(reopened, 'u', 'b')).toBe(2);
    expect(reopened.tableNames()).toEqual(['empty', 't', 'u']);
  });

  it('leaves no table behind when the write fails', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    const write = disk.files.a.write.bind(disk.files.a);
    disk.files.a.write = () => {
      throw new Error('QuotaExceededError');
    };
    expect(() => store.write([put('fresh', 'a', 1)])).toThrow('QuotaExceededError');
    expect(store.tableNames()).toEqual([]);
    disk.files.a.write = write;
    store.write([put('other', 'b', 2)]);
    store.close();
    const reopened = await RowStore.open(disk);
    expect(reopened.tableNames()).toEqual(['other']);
    expect(read(reopened, 'other', 'b')).toBe(2);
  });

  it('flushes at once only when asked to', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    const base = disk.files.a.flushes;
    store.write([put('t', 'a', 1)]);
    expect(disk.files.a.flushes).toBe(base);
    store.write([put('t', 'b', 2)], true);
    expect(disk.files.a.flushes).toBe(base + 1);
    store.flush();
    expect(disk.files.a.flushes).toBe(base + 1);
  });

  it('holds bodies of any size and keeps raw bytes intact', async () => {
    const disk = new Disk();
    const store = await RowStore.open(disk);
    const blob = new Uint8Array(3 << 20);
    for (let i = 0; i < blob.length; i++) blob[i] = (i * 31) & 255;
    store.write([{ t: 'put', table: 'blob', id: 'k', rv: 0, flags: 2, body: blob }]);
    store.write([put('t', 'a', 1)]);
    store.close();
    const reopened = await RowStore.open(disk);
    const hit = reopened.get('blob', 'k');
    expect(hit?.entry.flags).toBe(2);
    expect(hash32(hit!.body)).toBe(hash32(blob));
  });

  it('matches a model through random writes, checkpoints, compactions and reopens', async () => {
    let seed = 0x5eed;
    const rnd = (n: number): number => {
      seed = (Math.imul(seed, 1103515245) + 12345) & 0x7fffffff;
      return seed % n;
    };
    const disk = new Disk();
    let store = await RowStore.open(disk);
    const model = new Map<string, Map<string, { value: unknown; rv: number }>>();
    const tables = ['game', 'user', '_00_view'];
    for (let step = 0; step < 600; step++) {
      const ops: StoreOp[] = [];
      for (let k = rnd(6) + 1; k > 0; k--) {
        const table = tables[rnd(tables.length)];
        const rows = model.get(table) ?? model.set(table, new Map()).get(table)!;
        const id = `${table}:${rnd(40)}${rnd(4) === 0 ? 'é' : ''}`;
        const roll = rnd(10);
        if (roll === 0) {
          ops.push({ t: 'clear', table });
          rows.clear();
        } else if (roll < 3) {
          ops.push(del(table, id));
          rows.delete(id);
        } else {
          const value = { step, text: 'v'.repeat(rnd(200)) };
          const rv = rnd(1000);
          ops.push(put(table, id, value, rv));
          rows.set(id, { value, rv });
        }
      }
      store.write(ops);
      const action = rnd(40);
      if (action === 0) store.checkpoint();
      else if (action === 1) await store.compact();
      else if (action === 2) {
        store.close();
        store = await RowStore.open(disk);
      } else if (action === 3) {
        // Crash: reopen from the bytes as they are, without closing.
        store = await RowStore.open(disk.crashed());
        disk.files = (store as unknown as { io: Disk }).io.files;
      }
    }
    for (const [table, rows] of model) {
      const expected = [...rows.entries()]
        .sort(([x], [y]) => Buffer.compare(Buffer.from(x), Buffer.from(y)))
        .map(([id, row]) => [id, row.value]);
      expect(dump(store, table)).toEqual(expected);
      for (const [id, row] of rows) expect(store.find(table, id)?.rv).toBe(row.rv);
      expect(store.count(table)).toBe(rows.size);
    }
  });
});

describe('hash32', () => {
  it('gives one answer for the same bytes at any alignment', () => {
    const bytes = new Uint8Array(1031);
    for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 131 + 7) & 255;
    const expected = hash32(bytes);
    for (let shift = 1; shift < 8; shift++) {
      const moved = new Uint8Array(bytes.length + shift);
      moved.set(bytes, shift);
      expect(hash32(moved, shift, moved.length)).toBe(expected);
    }
    bytes[500] ^= 1;
    expect(hash32(bytes)).not.toBe(expected);
  });
});
