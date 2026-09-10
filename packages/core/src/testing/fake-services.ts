import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { Services } from '../client/services';

const noop = () => {};

/**
 * The legacy services the facade constructs around, as inert stubs. Only the
 * members the facade touches at construction / close time exist; sagas never
 * see these (they go through the adapters' `service` calls).
 */
export function fakeServiceBundle<S extends SchemaStructure>(over: Partial<Services<S>> = {}): Services<S> & { authListeners: Array<(u: string | null) => void>; connectionListeners: Array<(s: string) => void>; receivers: unknown[] } {
  const logger: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop, child: () => logger };
  const authListeners: Array<(u: string | null) => void> = [];
  const connectionListeners: Array<(s: string) => void> = [];
  const receivers: unknown[] = [];
  const events = { subscribe: noop };
  const bundle = {
    logger,
    local: { getEvents: () => events, getClient: () => 'local-client', storageHealth: undefined, subscribeToStorageHealth: undefined, close: async () => undefined } as any,
    remote: { getEvents: () => events, getClient: () => ({ authenticate: async (t: string) => t, invalidate: async () => undefined }), setAuthToken: noop, query: async () => ['remote'], close: async () => undefined } as any,
    connectionSupervisor: { subscribe: (cb: (s: string) => void) => (connectionListeners.push(cb), noop), dispose: noop } as any,
    blobs: { stats: () => ({ entries: 0 }), close: async () => undefined } as any,
    persistence: { set: async () => undefined, get: async () => null, remove: async () => undefined },
    streamProcessor: { addReceiver: (r: unknown) => void receivers.push(r), checkpoint: async () => undefined, dispose: noop } as any,
    migrator: {} as any,
    crdt: { closeAll: noop, dispose: noop, open: async () => 'field', close: noop } as any,
    auth: { subscribe: (cb: (u: string | null) => void) => (authListeners.push(cb), noop), currentUser: null, isAuthenticated: false, eventSystem: { subscribe: noop } } as any,
    tabs: null,
    tabsUnsupportedReason: 'test',
    tabId: 'tab-test',
    ...over,
  } as Services<S>;
  return Object.assign(bundle, { authListeners, connectionListeners, receivers });
}
