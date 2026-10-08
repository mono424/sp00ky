import pino, { type Level, type Logger as PinoLogger, type LoggerOptions } from 'pino';
import type { PinoTransmit } from '../../types';

export type Logger = PinoLogger;

/** One line the logger wrote, as the DevTools Logs tab and the MCP read it. */
export interface CapturedLog {
  /** Monotonic for the page's lifetime; a reader resumes after the last one it holds. */
  seq: number;
  time: number;
  /** Pino's numeric level: 10 trace … 60 fatal. */
  level: number;
  msg: string;
  /** The whole log object as JSON (what the console printed), capped at {@link LogTap.LINE_MAX}. */
  line: string;
}

export interface LogRead {
  entries: CapturedLog[];
  /** `seq` of the newest captured line; pass it back as `after` to resume. */
  head: number;
  /** Lines after `after` the ring overwrote before they were read (cleared ones excluded). */
  dropped: number;
  consoleLevel: Level;
  captureLevel: Level;
}

const LEVELS = pino.levels.values as Record<Level, number>;
const CAPTURE_KEY = 'sp00ky:devtools:logCapture';

function isLevel(value: unknown): value is Level {
  return typeof value === 'string' && value in LEVELS;
}

/**
 * JSON for a log object that can never throw into the call site: Errors keep
 * their message and stack (plain `JSON.stringify` prints `{}`), bigints become
 * strings, and a cycle prints `[Circular]` instead of throwing.
 */
export function stringifyLog(o: unknown): string {
  const ancestors: unknown[] = [];
  try {
    return JSON.stringify(o, function (this: unknown, _key: string, value: unknown) {
      if (typeof value === 'bigint') return value.toString();
      if (value instanceof Error) return { type: value.name, message: value.message, stack: value.stack };
      if (typeof value !== 'object' || value === null) return value;
      while (ancestors.length > 0 && ancestors.at(-1) !== this) ancestors.pop();
      if (ancestors.includes(value)) return '[Circular]';
      ancestors.push(value);
      return value;
    });
  } catch {
    const msg = (o as { msg?: unknown } | null)?.msg;
    return JSON.stringify({ msg: typeof msg === 'string' ? msg : '[unserializable log object]' });
  }
}

/**
 * The page-side log buffer behind the DevTools Logs tab. Always on and
 * bounded: a line costs the JSON the console write produces anyway, and a
 * panel opened after boot still sees the boot.
 *
 * It also owns the logger's level. The console prints at the configured level
 * only; `setCaptureLevel` changes what is RECORDED (debug without flooding the
 * console), live on every child logger, and keeps it for this tab's session so
 * a reload captures its boot at that level too.
 */
export class LogTap {
  static readonly CAPACITY = 500;
  static readonly LINE_MAX = 4000;

  private readonly ring: CapturedLog[] = [];
  private seq = 0;
  /** `seq` at the last `clear()`: lines up to here are gone on purpose, not dropped. */
  private clearedThrough = 0;
  private readonly listeners = new Set<() => void>();
  private readonly loggers: Array<WeakRef<Logger>> = [];
  private captureLevelValue: Level;

  constructor(readonly consoleLevel: Level) {
    this.captureLevelValue = consoleLevel;
    try {
      const saved = typeof sessionStorage !== 'undefined' ? sessionStorage.getItem(CAPTURE_KEY) : null;
      if (isLevel(saved)) this.captureLevelValue = saved;
    } catch {
      // Storage blocked: capture at the console level.
    }
  }

  /** `seq` of the newest captured line. */
  get head(): number {
    return this.seq;
  }

  get captureLevel(): Level {
    return this.captureLevelValue;
  }

  /** What pino has to produce for both the console and the buffer. */
  get effectiveLevel(): Level {
    return LEVELS[this.captureLevelValue] < LEVELS[this.consoleLevel] ? this.captureLevelValue : this.consoleLevel;
  }

  /** Whether the console prints a line at this numeric level. */
  printsToConsole(level: number): boolean {
    return level >= LEVELS[this.consoleLevel];
  }

  /** Remember a logger (the root and every child) so a level change reaches it. */
  track(logger: Logger): void {
    this.loggers.push(new WeakRef(logger));
  }

  setCaptureLevel(level: Level): void {
    if (!isLevel(level)) throw new Error(`unknown log level: ${String(level)}`);
    this.captureLevelValue = level;
    try {
      if (typeof sessionStorage !== 'undefined') {
        if (level === this.consoleLevel) sessionStorage.removeItem(CAPTURE_KEY);
        else sessionStorage.setItem(CAPTURE_KEY, level);
      }
    } catch {
      // Storage blocked: the change holds until the page goes away.
    }
    const effective = this.effectiveLevel;
    for (let i = this.loggers.length - 1; i >= 0; i--) {
      const logger = this.loggers[i].deref();
      if (logger) logger.level = effective;
      else this.loggers.splice(i, 1);
    }
  }

  record(o: Record<string, unknown>, line: string): void {
    const level = typeof o.level === 'number' ? o.level : LEVELS.info;
    if (level < LEVELS[this.captureLevelValue]) return;
    this.ring.push({
      seq: ++this.seq,
      time: typeof o.time === 'number' ? o.time : Date.now(),
      level,
      msg: typeof o.msg === 'string' ? o.msg : '',
      line: line.length > LogTap.LINE_MAX ? `${line.slice(0, LogTap.LINE_MAX)}…` : line,
    });
    if (this.ring.length > LogTap.CAPACITY) this.ring.shift();
    for (const cb of this.listeners) {
      try {
        cb();
      } catch {
        // A listener must never turn a log call into a throw.
      }
    }
  }

  /** Lines after `after` (0 = everything still buffered), oldest first. */
  read(after = 0, limit = LogTap.CAPACITY): LogRead {
    const first = this.ring[0]?.seq ?? this.seq + 1;
    const fresh = this.ring.filter((e) => e.seq > after);
    return {
      entries: fresh.slice(Math.max(0, fresh.length - limit)),
      head: this.seq,
      dropped: Math.max(0, first - Math.max(after, this.clearedThrough) - 1),
      consoleLevel: this.consoleLevel,
      captureLevel: this.captureLevelValue,
    };
  }

  clear(): void {
    this.ring.length = 0;
    this.clearedThrough = this.seq;
  }

  /** Called synchronously inside the log call: keep it cheap, never log from it. */
  subscribe(cb: () => void): () => void {
    this.listeners.add(cb);
    return () => void this.listeners.delete(cb);
  }
}

export function createLogger(level: Level = 'info', transmit?: PinoTransmit, tap?: LogTap): Logger {
  const browserConfig: LoggerOptions['browser'] = {
    asObject: true,
    write: (o: any) => {
      const line = stringifyLog(o);
      if (!tap || tap.printsToConsole(o?.level)) {
        // oxlint-disable-next-line no-console
        console.log(line);
      }
      tap?.record(o, line);
    },
  };

  if (transmit) {
    // Pinned to the configured level: a lower capture level for DevTools must
    // not start shipping debug lines to the collector.
    browserConfig.transmit = { ...transmit, level: transmit.level ?? level };
  }

  const logger = pino({
    level: tap ? tap.effectiveLevel : level,
    browser: browserConfig,
    onChild: (child) => tap?.track(child),
  });
  tap?.track(logger);
  return logger;
}
