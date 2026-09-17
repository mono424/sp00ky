import type { EventDefinition, EventSystem } from '../../../events/index';
import { createEventSystem } from '../../../events/index';
import type { ImpersonationInfo } from '../impersonation';

export const AuthEventTypes = {
  AuthStateChanged: 'AUTH_STATE_CHANGED',
  ImpersonationChanged: 'IMPERSONATION_CHANGED',
} as const;

export type AuthEventTypeMap = {
  [AuthEventTypes.AuthStateChanged]: EventDefinition<
    typeof AuthEventTypes.AuthStateChanged,
    string | null
  >;
  [AuthEventTypes.ImpersonationChanged]: EventDefinition<
    typeof AuthEventTypes.ImpersonationChanged,
    ImpersonationInfo | null
  >;
};

export type AuthEventSystem = EventSystem<AuthEventTypeMap>;

export function createAuthEventSystem(): AuthEventSystem {
  return createEventSystem([AuthEventTypes.AuthStateChanged, AuthEventTypes.ImpersonationChanged]);
}
