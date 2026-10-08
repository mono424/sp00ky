import {
  DEFAULT_VERSIONS,
  type BackendDevToolsState,
  type DevToolsState,
  type Sp00kyEvent,
  type ActiveQuery,
  type AuthState,
} from '../types/devtools';

/**
 * Transforms backend DevTools state to frontend state structure
 */
export function adaptBackendState(backendState: BackendDevToolsState): DevToolsState {
  // Transform events
  const events: Sp00kyEvent[] = backendState.eventsHistory.map((event) => ({
    type: event.eventType,
    timestamp: event.timestamp,
    data: event.payload,
  }));

  // Transform activeQueries from Record to Array
  const activeQueries: ActiveQuery[] = Object.values(backendState.activeQueries);

  // Transform auth state
  const auth: AuthState = {
    isAuthenticated: backendState.auth.authenticated,
    user: backendState.auth.userId
      ? {
          email: backendState.auth.userId,
          roles: [],
        }
      : null,
    lastAuthCheck: backendState.auth.timestamp || Date.now(),
  };

  return {
    events,
    activeQueries,
    auth,
    database: backendState.database,
    versions: backendState.versions
      ? { ...backendState.versions, entities: backendState.versions.entities ?? [] }
      : DEFAULT_VERSIONS,
    mutations: backendState.mutations ?? null,
    logs: backendState.logs ?? null,
  };
}
