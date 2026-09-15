/**
 * Placeholder arguments for running a `q*` builder without real data.
 *
 * A `q*` function is pure: `(db, ...args) => db.query(...)...build()`. The
 * generator calls it with one of these proxies per argument. The proxy has to
 * survive whatever the function does to its argument on the way to the builder:
 * be passed straight through (`where({ id })`), be stringified
 * (`toRecordId(String(id))`), be used in arithmetic (`offset(window * page)`),
 * be treated as an array (`(opts.collections || []).map(String)`), have a
 * property or method read off it (`opts.since.getTime()`), or be spread.
 *
 * Every consequence is deterministic so two runs over the same source produce
 * the same surql:
 *
 *   - `Symbol.toPrimitive`: hint `number` -> 1, anything else -> `proxy:<path>`
 *   - `then` -> undefined, so the proxy is never mistaken for a thenable
 *   - `length` -> 1; `Symbol.iterator` yields ONE nested proxy
 *   - `map` / `filter` / `flatMap` / `slice` / `concat` -> `[nested]`
 *   - `some` / `every` / `includes` -> true; `join` -> 'proxy'
 *   - `toJSON` -> `proxy:<path>` (a subquery `where` inlines values via
 *     `JSON.stringify`, which would otherwise drop a function-typed value)
 *   - any other property -> a nested proxy at `<path>.<prop>`
 *   - `has` -> false, `ownKeys` -> [], `getOwnPropertyDescriptor` -> undefined,
 *     so `'_op' in value` is false (the builder binds it as a plain param) and
 *     `{ ...proxy }` / `Object.keys(proxy)` / `Object.entries(proxy)` are empty
 *   - calling the proxy returns a nested proxy at `<path>()`
 *
 * The target is an ARROW function on purpose: an ordinary function carries a
 * non-configurable `prototype`, which the `ownKeys: []` invariant would reject.
 */

const registry = new WeakSet<object>();

export type ArgProxy = any;

const ARRAY_TO_ARRAY = new Set(['map', 'filter', 'flatMap', 'slice', 'concat']);
const ARRAY_TO_TRUE = new Set(['some', 'every', 'includes']);

export function makeArgProxy(path: string): ArgProxy {
  const target = () => undefined;
  const text = `proxy:${path}`;

  const proxy: ArgProxy = new Proxy(target, {
    apply: () => makeArgProxy(`${path}()`),
    get: (_target, prop) => {
      if (prop === Symbol.toPrimitive) {
        return (hint: string) => (hint === 'number' ? 1 : text);
      }
      if (prop === Symbol.iterator) {
        return function* () {
          yield makeArgProxy(`${path}[0]`);
        };
      }
      if (typeof prop === 'symbol') return undefined;
      switch (prop) {
        case 'then':
          return undefined;
        case 'toString':
        case 'toJSON':
          return () => text;
        case 'valueOf':
          return () => 1;
        case 'length':
          return 1;
        case 'join':
          return () => 'proxy';
      }
      if (ARRAY_TO_ARRAY.has(prop)) return () => [makeArgProxy(`${path}[0]`)];
      if (ARRAY_TO_TRUE.has(prop)) return () => true;
      return makeArgProxy(`${path}.${prop}`);
    },
    has: () => false,
    ownKeys: () => [],
    getOwnPropertyDescriptor: () => undefined,
    set: () => true,
    deleteProperty: () => true,
  });

  registry.add(proxy);
  return proxy;
}

export function isArgProxy(value: unknown): boolean {
  return (
    (typeof value === 'function' || (typeof value === 'object' && value !== null)) &&
    registry.has(value as object)
  );
}
