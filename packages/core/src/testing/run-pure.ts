import { webcrypto } from 'node:crypto';
import type { Effect, EffectKind, Settled } from '../kernel/effects';
import type { OutEvent, RuntimeEvent } from '../kernel/events';
import type { Saga } from '../kernel/saga';
import { runSaga } from '../kernel/saga';
import type { ClientState } from '../state/client-state';
import { emptyState } from '../state/client-state';

export type EffectHandler = (effect: Effect, ctx: PureContext) => unknown | Promise<unknown>;

export interface PureContext {
  state: ClientState;
  now: number;
  log: Effect[];
  timers: Map<string, { ms: number; event: RuntimeEvent }>;
  emitted: OutEvent[];
  dispatched: RuntimeEvent[];
  ids: number;
}

export interface RunPureOptions {
  state?: ClientState;
  now?: number;
  /** What `local.epoch` answers; a store-side counter, so it is scripted like `now`. */
  epoch?: number;
  handlers?: Partial<Record<EffectKind, EffectHandler>>;
}

export interface RunPureResult<R> extends PureContext {
  result: R;
}

export class UnhandledEffectError extends Error {
  constructor(public readonly effect: Effect) {
    super(`runPure: no handler for effect '${effect.kind}'`);
    this.name = 'UnhandledEffectError';
  }
}

export async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const digest = await webcrypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/**
 * Drive a saga with canned effect results. `state.*`, `now`, `id`, `hash`,
 * `timer.*`, `emit`, `dispatch` and `local.epoch` are handled here; every other
 * adapter effect (`local.*`, `remote.*`, `ssp.*`) must have a handler or the run throws, so
 * a test can never pass by an effect silently returning `undefined`.
 */
export async function runPure<R>(saga: Saga<R>, opts: RunPureOptions = {}): Promise<RunPureResult<R>> {
  const ctx: PureContext = {
    state: opts.state ?? emptyState({ tabId: 'tab-test' }),
    now: opts.now ?? 1_700_000_000_000,
    log: [],
    timers: new Map(),
    emitted: [],
    dispatched: [],
    ids: 0,
  };
  const interpret = async (effect: Effect): Promise<unknown> => {
    ctx.log.push(effect);
    const custom = opts.handlers?.[effect.kind];
    if (custom) return custom(effect, ctx);
    switch (effect.kind) {
      case 'state.read':
        return effect.select(ctx.state);
      case 'state.update':
        ctx.state = effect.fn(ctx.state);
        return ctx.state;
      case 'state.wait':
        if (effect.until(ctx.state)) return undefined;
        throw new Error('runPure: state.wait would block; script a handler or prepare the state');
      case 'now':
        return ctx.now;
      case 'local.epoch':
        return opts.epoch ?? 0;
      case 'id':
        ctx.ids += 1;
        return `${effect.scope}-${ctx.ids}`;
      case 'hash':
        return sha256Hex(effect.input);
      case 'timer.set':
        ctx.timers.set(effect.key, { ms: effect.ms, event: effect.event });
        return undefined;
      case 'timer.clear':
        ctx.timers.delete(effect.key);
        return undefined;
      case 'emit':
        ctx.emitted.push(effect.event);
        return undefined;
      case 'dispatch':
        ctx.dispatched.push(effect.event);
        return undefined;
      case 'service':
        throw new UnhandledEffectError(effect);
      case 'all': {
        const results: Settled[] = [];
        for (const inner of effect.effects) {
          try {
            results.push({ ok: true, value: await interpret(inner) });
          } catch (error) {
            results.push({ ok: false, error });
          }
        }
        return results;
      }
      default:
        throw new UnhandledEffectError(effect);
    }
  };
  const result = await runSaga(saga, interpret);
  return { ...ctx, result };
}

/** Handler helper: answer effects by SQL prefix (first match wins). */
export function bySqlPrefix(
  table: Array<[prefix: string, answer: (effect: Effect & { sql: string }) => unknown]>
): EffectHandler {
  return (effect) => {
    const sql = (effect as { sql?: string }).sql ?? '';
    for (const [prefix, answer] of table) {
      if (sql.startsWith(prefix)) return answer(effect as Effect & { sql: string });
    }
    throw new Error(`no scripted answer for SQL: ${sql.slice(0, 80)}`);
  };
}
