import { describe, it, expect, vi, afterEach } from 'vitest';
import { LogTap, createLogger, stringifyLog } from './index';

// Vitest runs in node, where `pino` resolves to its node build and ignores the
// `browser` options the tap hangs off. Apps bundle the browser build.
// The specifier is widened to `string`: pino ships no types for that entry.
vi.mock('pino', async () => ({ default: (await import('pino/browser.js' as string)).default }));

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function silentConsole() {
  return vi.spyOn(console, 'log').mockImplementation(() => {});
}

describe('LogTap', () => {
  it('records what the logger writes, child bindings included', () => {
    silentConsole();
    const tap = new LogTap('info');
    const logger = createLogger('info', undefined, tap);
    logger.child({ service: 'Database' }).warn({ Category: 'x' }, 'slow query');
    logger.debug('not at this level');
    const { entries, head } = tap.read();
    expect(entries).toHaveLength(1);
    expect(head).toBe(1);
    expect(entries[0]).toMatchObject({ seq: 1, level: 40, msg: 'slow query' });
    expect(JSON.parse(entries[0].line)).toMatchObject({ service: 'Database', Category: 'x' });
  });

  it('captures below the console level without printing it, on existing children too', () => {
    const printed = silentConsole();
    const tap = new LogTap('info');
    const logger = createLogger('info', undefined, tap);
    const child = logger.child({ service: 'Sync' });
    child.debug('before');
    tap.setCaptureLevel('debug');
    child.debug('after');
    logger.info('shown');
    expect(tap.read().entries.map((e) => e.msg)).toEqual(['after', 'shown']);
    expect(printed).toHaveBeenCalledTimes(1);
    expect(String(printed.mock.calls[0][0])).toContain('shown');
  });

  it('keeps the capture level for the tab session', () => {
    const store = new Map<string, string>();
    vi.stubGlobal('sessionStorage', {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, v),
      removeItem: (k: string) => void store.delete(k),
    });
    new LogTap('info').setCaptureLevel('trace');
    expect(new LogTap('info').captureLevel).toBe('trace');
    new LogTap('info').setCaptureLevel('info');
    expect(store.size).toBe(0);
  });

  it('is a bounded ring that reports what a slow reader missed', () => {
    silentConsole();
    const tap = new LogTap('info');
    const logger = createLogger('info', undefined, tap);
    for (let i = 0; i < LogTap.CAPACITY + 10; i++) logger.info(`line ${i}`);
    const all = tap.read();
    expect(all.entries).toHaveLength(LogTap.CAPACITY);
    expect(all.dropped).toBe(10);
    const delta = tap.read(all.head - 2);
    expect(delta.entries.map((e) => e.msg)).toEqual([`line ${LogTap.CAPACITY + 8}`, `line ${LogTap.CAPACITY + 9}`]);
    expect(delta.dropped).toBe(0);
  });

  it('does not count cleared lines as dropped', () => {
    silentConsole();
    const tap = new LogTap('info');
    const logger = createLogger('info', undefined, tap);
    logger.info('a');
    logger.info('b');
    tap.clear();
    logger.info('c');
    expect(tap.read(0)).toMatchObject({ dropped: 0, head: 3 });
    expect(tap.read(0).entries.map((e) => e.msg)).toEqual(['c']);
  });

  it('caps a huge line', () => {
    silentConsole();
    const tap = new LogTap('info');
    createLogger('info', undefined, tap).info({ blob: 'x'.repeat(LogTap.LINE_MAX * 2) }, 'big');
    const [entry] = tap.read().entries;
    expect(entry.line.length).toBe(LogTap.LINE_MAX + 1);
    expect(entry.msg).toBe('big');
  });

  it('notifies listeners and survives one that throws', () => {
    silentConsole();
    const tap = new LogTap('info');
    const logger = createLogger('info', undefined, tap);
    const seen: number[] = [];
    tap.subscribe(() => {
      throw new Error('listener bug');
    });
    tap.subscribe(() => seen.push(tap.head));
    expect(() => logger.info('x')).not.toThrow();
    expect(seen).toEqual([1]);
  });

  it('keeps the otel transmit at the configured level when capture goes lower', () => {
    silentConsole();
    const sent: string[] = [];
    const tap = new LogTap('info');
    const logger = createLogger('info', { send: (level) => void sent.push(level) }, tap);
    tap.setCaptureLevel('debug');
    logger.debug('d');
    logger.info('i');
    expect(sent).toEqual(['info']);
  });
});

describe('stringifyLog', () => {
  it('keeps errors readable and never throws on bigints or cycles', () => {
    const o: Record<string, unknown> = { err: new Error('boom'), n: 10n };
    o.self = o;
    const parsed = JSON.parse(stringifyLog(o));
    expect(parsed.err).toMatchObject({ type: 'Error', message: 'boom' });
    expect(parsed.n).toBe('10');
    expect(parsed.self).toBe('[Circular]');
  });

  it('does not mistake a shared reference for a cycle', () => {
    const shared = { a: 1 };
    expect(JSON.parse(stringifyLog({ x: shared, y: shared }))).toEqual({ x: { a: 1 }, y: { a: 1 } });
  });
});
