/**
 * A store opened ahead of its page, and how the tab that leads gets it back.
 *
 * `prewarmRows` (rows-prewarm.ts) starts the row worker from the page head and
 * opens the last bucket's store before the app has loaded. With shared tabs
 * the page cannot know then whether it will lead. When another tab does, that
 * tab's worker finds the files held by a worker nobody uses: its page becomes
 * a follower and ends it, but only once its bundle has loaded and it was told
 * its role, and the leader's ten attempts are over by then.
 *
 * So a worker that leads asks for the files each time an open fails, and a
 * worker that holds them unclaimed gives them up and tells its page. A page
 * claims its worker with the first message it sends under an id of its own
 * (everything a prewarm sends goes under a negative one).
 *
 * Kept apart from `rows-worker.ts`, which runs only in a worker on OPFS, so
 * this can be tested.
 */
import { ROWS_PREWARM_OPEN_ID } from './rows-prewarm';

/** What this needs of a BroadcastChannel. A channel never hears its own messages. */
export interface YieldChannel {
  postMessage(message: unknown): void;
  onmessage: ((ev: MessageEvent) => void) | null;
}

export interface AheadHost {
  /** The channel the workers of an origin ask each other on, or null where there is none. */
  channel(): YieldChannel | null;
  /** The bucket whose store is open in this worker, or null. */
  openBucket(): string | null;
  /** Run `fn` in turn with the worker's ops, so a store is not closed under a read. */
  enqueue(fn: () => void): void;
  closeStore(): void;
  /** Post to the page that started this worker. */
  tellPage(message: { id: number; ok: false; error: string }): void;
}

export class AheadStore {
  /** The open store was opened ahead of the page. */
  private ahead = false;
  /** The page has sent a message of its own: what is open here is its store from now on. */
  private claimed = false;
  private heard: YieldChannel | null = null;

  constructor(private host: AheadHost) {}

  /** A message from the page under an id of its own. */
  claim(): void {
    this.claimed = true;
  }

  /** The open a prewarm asked for has succeeded. */
  openedAhead(): void {
    if (this.claimed || this.host.openBucket() === null || !this.listen()) return;
    this.ahead = true;
  }

  /** This worker leads `bucket` and could not open its files: whoever holds them unclaimed is asked for them. */
  ask(bucket: string): void {
    try {
      this.listen()?.postMessage({ bucket });
    } catch {
      /* no channel: the retries are all there is */
    }
  }

  private wanted(bucket: unknown): boolean {
    return this.ahead && !this.claimed && this.host.openBucket() !== null && this.host.openBucket() === bucket;
  }

  private listen(): YieldChannel | null {
    if (this.heard) return this.heard;
    const channel = this.host.channel();
    if (!channel) return null;
    channel.onmessage = (ev) => {
      const bucket = (ev.data as { bucket?: unknown } | null)?.bucket;
      if (!this.wanted(bucket)) return;
      // The page may claim the store while this waits its turn.
      this.host.enqueue(() => {
        if (!this.wanted(bucket)) return;
        this.ahead = false;
        this.host.closeStore();
        // Under the open's id: the page then takes neither that open as answered
        // nor anything it had read ahead (`prewarmRows`, `ReadAhead`).
        this.host.tellPage({ id: ROWS_PREWARM_OPEN_ID, ok: false, error: 'rows worker: gave the store to the tab that leads' });
      });
    };
    this.heard = channel;
    return channel;
  }
}
