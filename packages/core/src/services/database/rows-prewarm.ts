/**
 * Starting the row store ahead of the app. Kept free of imports: a build
 * script that only wants {@link ROWS_PREWARM_SNIPPET} for the page's `<head>`
 * loads this file and nothing else of the client.
 *
 * What a page load can have done before its bundle has run:
 *
 *   1. the worker started and the last bucket's store opened;
 *   2. the reads the app made in its first seconds last time, asked again
 *      ("read ahead"), their answers waiting in the page;
 *   3. the tables' indexes read into the worker's memory.
 *
 * The engine (`rows-cache-engine.ts`) adopts all three when it connects, and
 * starts its own worker when anything about them is wrong.
 */

/** localStorage key under which the engine leaves what {@link prewarmRows} needs. */
export const ROWS_PREWARM_KEY = 'sp00ky.rows.prewarm';

/** Message id of the open a prewarm sends. */
export const ROWS_PREWARM_OPEN_ID = -1;
/** Read `i` of a prewarm goes out as message `ROWS_AHEAD_ID - i`. The worker answers reads only under these ids. */
export const ROWS_AHEAD_ID = -10;

/** The worker ops a prewarm may ask ahead: the ones that answer rows and change nothing. */
export const ROWS_AHEAD_OPS: ReadonlySet<string> = new Set(['select', 'by-ids', 'relation']);

/** What the engine saves for the next page load. */
export interface PrewarmRecord {
  url: string;
  bucket: string;
  /** `[op, payload]` of the reads to ask ahead; JSON all the way down. */
  reads?: [string, unknown][];
}

/** A worker's reply as it arrives: the transport's envelope around an op's result. */
export interface PrewarmReply {
  id: number;
  ok: boolean;
  error?: string;
  [field: string]: unknown;
}

export interface Prewarmed {
  worker: Worker;
  bucket: string;
  failed: boolean;
  /** The worker's answer to the open it was started with, once it has come. */
  opened?: PrewarmReply;
  /** The reads asked ahead, and the answers that have come, by position. */
  reads: [string, unknown][];
  replies: Record<number, PrewarmReply>;
}

type PrewarmGlobal = { __sp00kyRowsPrewarm?: Prewarmed };

/**
 * Start the row store's worker and open the last bucket NOW, before the app
 * has loaded. A worker takes about 10 ms to start and the store a few more to
 * open; started from the first bytes of the page, that happens while the
 * bundle downloads and parses, and by the time the client connects the store
 * is open, its indexes are in memory and the answers to the app's first reads
 * are already in the page. The engine finds the worker and adopts it; if
 * anything about it is wrong (a new release, another bucket, a failed start)
 * it starts its own, as it would have without this.
 *
 * Call it as early as the app can, or inline {@link ROWS_PREWARM_SNIPPET} in
 * the page's `<head>`, which is this function without the bundle. A no-op on
 * a first visit: the worker's URL is learned from the engine's first open.
 */
export function prewarmRows(): void {
  try {
    const g = globalThis as PrewarmGlobal;
    const saved = JSON.parse(localStorage.getItem(ROWS_PREWARM_KEY) ?? 'null') as Partial<PrewarmRecord> | null;
    if (!saved?.url || !saved.bucket || g.__sp00kyRowsPrewarm) return;
    const reads = Array.isArray(saved.reads) ? saved.reads : [];
    const pre: Prewarmed = {
      worker: new Worker(saved.url, { type: 'module' }),
      bucket: saved.bucket,
      failed: false,
      reads,
      replies: {},
    };
    pre.worker.onerror = () => {
      pre.failed = true;
    };
    // Kept for the engine: with the worker's answers in hand it does not ask
    // again, and its first reads cost no trip to the worker at all.
    pre.worker.onmessage = (ev: MessageEvent) => {
      const data = ev.data as PrewarmReply | null;
      if (!data) return;
      if (data.id === ROWS_PREWARM_OPEN_ID) {
        if (data.ok === true) pre.opened = data;
      } else if (data.id <= ROWS_AHEAD_ID) pre.replies[ROWS_AHEAD_ID - data.id] = data;
    };
    // One attempt and no memory fallback: if the files are held (another tab
    // leads), the engine's own open sorts it out.
    pre.worker.postMessage({
      id: ROWS_PREWARM_OPEN_ID,
      type: 'open',
      payload: { dbName: saved.bucket, useOpfs: true, openOptions: { maxAttempts: 1, disallowMemoryFallback: true } },
    });
    for (let i = 0; i < reads.length; i++) pre.worker.postMessage({ id: ROWS_AHEAD_ID - i, type: reads[i][0], payload: reads[i][1] });
    pre.worker.postMessage({ id: -2, type: 'warm' });
    g.__sp00kyRowsPrewarm = pre;
  } catch {
    // No storage, no workers, a blocked URL: the engine starts its own worker.
  }
}

/** {@link prewarmRows} as a script to inline in `<head>`, ahead of the app's bundle. */
export const ROWS_PREWARM_SNIPPET =
  `(function(){try{var g=globalThis,p=JSON.parse(localStorage.getItem("${ROWS_PREWARM_KEY}")||"null");` +
  `if(!p||!p.url||!p.bucket||g.__sp00kyRowsPrewarm)return;` +
  `var r=Array.isArray(p.reads)?p.reads:[],s={worker:new Worker(p.url,{type:"module"}),bucket:p.bucket,failed:false,reads:r,replies:{}};` +
  `s.worker.onerror=function(){s.failed=true};` +
  `s.worker.onmessage=function(e){var d=e.data;if(!d)return;` +
  `if(d.id===${ROWS_PREWARM_OPEN_ID}){if(d.ok===true)s.opened=d}else if(d.id<=${ROWS_AHEAD_ID})s.replies[${ROWS_AHEAD_ID}-d.id]=d};` +
  `s.worker.postMessage({id:${ROWS_PREWARM_OPEN_ID},type:"open",payload:{dbName:p.bucket,useOpfs:true,openOptions:{maxAttempts:1,disallowMemoryFallback:true}}});` +
  `for(var i=0;i<r.length;i++)s.worker.postMessage({id:${ROWS_AHEAD_ID}-i,type:r[i][0],payload:r[i][1]});` +
  `s.worker.postMessage({id:-2,type:"warm"});g.__sp00kyRowsPrewarm=s}catch(e){}})();`;

/** The prewarmed worker, once: whoever takes it owns it. */
export function takePrewarmed(): Prewarmed | null {
  const g = globalThis as PrewarmGlobal;
  const pre = g.__sp00kyRowsPrewarm ?? null;
  delete g.__sp00kyRowsPrewarm;
  return pre;
}

/** How a read is told from another: its op and its payload, as JSON. */
export const aheadKey = (type: string, payload: unknown): string => `${type}\n${JSON.stringify(payload)}`;

/**
 * Whether `value` is the same thing after a trip through JSON. Only such a
 * payload may be saved and asked again: a `Date` would come back as a string,
 * and the worker would answer another question under the same key.
 */
export function jsonSafe(value: unknown): boolean {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value !== 'object') return false;
  if (Array.isArray(value)) return value.every(jsonSafe);
  const proto = Object.getPrototypeOf(value) as unknown;
  if (proto !== Object.prototype && proto !== null) return false;
  // An `undefined` property is one JSON leaves out, as if it had not been set.
  return Object.values(value).every((v) => v === undefined || jsonSafe(v));
}

/**
 * The answers a prewarmed worker gave, or is about to give, to the reads the
 * last session made at startup. The engine takes one when it is about to ask
 * the worker the very same thing, and so does not ask.
 *
 * Why the answer is the one the worker would give now: it read the store
 * after opening it and before this page asked for anything, and nothing has
 * written since. The engine drops everything left here the moment it sends a
 * write (or anything else that is not a plain read), and never uses this at
 * all when other tabs write through the same worker.
 */
export class ReadAhead {
  private wanted = new Map<string, number>();
  private waiting = new Map<number, (reply: PrewarmReply | null) => void>();

  /** Call once the transport has taken the worker: answers on their way pass through here first. */
  constructor(
    private pre: Prewarmed,
    private waitMs: number
  ) {
    pre.reads.forEach((read, i) => {
      if (Array.isArray(read) && ROWS_AHEAD_OPS.has(read[0])) this.wanted.set(aheadKey(read[0], read[1]), i);
    });
    const worker = pre.worker;
    const inner = worker.onmessage;
    worker.onmessage = (ev: MessageEvent) => {
      const data = ev.data as PrewarmReply | null;
      if (typeof data?.id === 'number' && data.id < 0) this.arrived(data);
      else inner?.call(worker, ev);
    };
  }

  private arrived(data: PrewarmReply): void {
    if (data.id > ROWS_AHEAD_ID) return;
    const i = ROWS_AHEAD_ID - data.id;
    const waiter = this.waiting.get(i);
    if (waiter) waiter(data.ok ? data : null);
    else if (this.wanted.size > 0) this.pre.replies[i] = data;
  }

  /**
   * The answer to this read if it was asked ahead: a promise of the reply,
   * or of `null` when it failed or does not come. `null` when it was not
   * asked ahead (or was taken already): ask the worker.
   */
  take(type: string, payload: unknown): Promise<PrewarmReply | null> | null {
    if (this.wanted.size === 0) return null;
    const key = aheadKey(type, payload);
    const i = this.wanted.get(key);
    if (i === undefined) return null;
    this.wanted.delete(key);
    const here = this.pre.replies[i];
    if (here) {
      delete this.pre.replies[i];
      return Promise.resolve(here.ok ? here : null);
    }
    // Sent before the app existed, so it is ahead of anything sent now:
    // waiting for it is never slower than asking again. Not forever, though.
    return new Promise((resolve) => {
      const settle = (reply: PrewarmReply | null): void => {
        clearTimeout(timer);
        this.waiting.delete(i);
        resolve(reply);
      };
      const timer = setTimeout(() => settle(null), this.waitMs);
      this.waiting.set(i, settle);
    });
  }

  /** Forget every answer not taken yet: the store is about to change, or the session has moved on. */
  drop(): void {
    this.wanted.clear();
    this.pre.replies = {};
    for (const settle of [...this.waiting.values()]) settle(null);
  }
}
