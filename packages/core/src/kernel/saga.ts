import type { Effect } from './effects';

/**
 * A saga is a generator that yields effects and receives their results. It
 * holds no references to adapters or state, so it can be driven by the real
 * interpreter or by `testing/run-pure` with canned results.
 */
export type Saga<R> = Generator<Effect, R, any>;
export type Interpret = (effect: Effect) => Promise<unknown>;

/**
 * Drive a saga to completion. A rejected effect is thrown back into the saga
 * at the `yield`, so sagas use ordinary `try/catch`; an error the saga does
 * not catch propagates to the caller.
 */
export async function runSaga<R>(saga: Saga<R>, interpret: Interpret): Promise<R> {
  let step = saga.next();
  while (!step.done) {
    let result: unknown;
    try {
      result = await interpret(step.value);
    } catch (error) {
      step = saga.throw(error);
      continue;
    }
    step = saga.next(result);
  }
  return step.value;
}

/**
 * Lanes bound concurrency without a queue object per lane:
 * - `serial`: runs strictly one at a time per key, in arrival order.
 * - `dedupe`: a request while one run is active joins that run instead of
 *   starting another.
 * The bookkeeping is a pure value so the policy is unit-testable; the runtime
 * owns the promises.
 */
export type Lane = { kind: 'serial'; key: string } | { kind: 'dedupe'; key: string };

export interface LaneState {
  readonly running: ReadonlySet<string>;
  readonly waiting: ReadonlyMap<string, number>;
}

export const emptyLanes = (): LaneState => ({ running: new Set(), waiting: new Map() });

export type LaneDecision = 'start' | 'wait' | 'join';

export function acquire(state: LaneState, lane: Lane): { decision: LaneDecision; state: LaneState } {
  if (!state.running.has(lane.key)) {
    const running = new Set(state.running);
    running.add(lane.key);
    return { decision: 'start', state: { running, waiting: state.waiting } };
  }
  if (lane.kind === 'dedupe') return { decision: 'join', state };
  const waiting = new Map(state.waiting);
  waiting.set(lane.key, (waiting.get(lane.key) ?? 0) + 1);
  return { decision: 'wait', state: { running: state.running, waiting } };
}

export function release(state: LaneState, key: string): { startNext: boolean; state: LaneState } {
  const pending = state.waiting.get(key) ?? 0;
  if (pending > 0) {
    const waiting = new Map(state.waiting);
    if (pending === 1) waiting.delete(key);
    else waiting.set(key, pending - 1);
    return { startNext: true, state: { running: state.running, waiting } };
  }
  const running = new Set(state.running);
  running.delete(key);
  return { startNext: false, state: { running, waiting: state.waiting } };
}
