import type { Effect, ServiceCalls, ServiceName } from '../kernel/effects';
import type { EffectHandler } from './run-pure';

type Impl = { [N in ServiceName]?: (...args: Parameters<ServiceCalls[N]>) => unknown };

/** Script the `service` effect by name and record every call. */
export function fakeServices(impl: Impl = {}) {
  const calls: Array<[ServiceName, unknown[]]> = [];
  const handler: EffectHandler = (effect: Effect) => {
    if (effect.kind !== 'service') throw new Error('not a service effect');
    calls.push([effect.name, effect.args]);
    const fn = impl[effect.name] as ((...a: unknown[]) => unknown) | undefined;
    if (!fn) return undefined;
    return fn(...effect.args);
  };
  return { handler, calls, names: () => calls.map(([n]) => n) };
}
