import type { RemoteDatabaseService } from '../../services/database/remote';
import type {
  SchemaStructure,
  AccessDefinition,
  ColumnSchema,
  TypeNameToTypeMap,
} from '@spooky-sync/query-builder';
import type { Logger } from '../../services/logger/index';
export * from './events/index';
import { AuthEventTypes, createAuthEventSystem } from './events/index';
import type { PersistenceClient } from '../../types';
import { classifySyncError } from '../../utils/error-classification';
import {
  decodeTokenClaims,
  impersonationFromToken,
  sameImpersonation,
  ImpersonationTimers,
  ORIGIN_TOKEN_KEY,
} from './impersonation';
import type { ImpersonationInfo } from './impersonation';
export * from './impersonation';

/** What `fn::_00_impersonate::list_users` returns per row. */
export interface ImpersonationCandidate {
  id: string;
  is_admin: boolean;
  [field: string]: unknown;
}

/** An open session, as `fn::_00_impersonate::active` lists it. */
export interface ActiveImpersonation {
  session: string;
  target: string;
  admin: string;
  reason: string;
  started_at: unknown;
  expires_at: unknown;
}

// Helper to pretty print types
type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

// Map ColumnSchema (value type string) to actual Typescript type
type MapColumnType<T extends ColumnSchema> = T['optional'] extends true
  ? TypeNameToTypeMap[T['type']] | undefined
  : TypeNameToTypeMap[T['type']];

// Extract params object from SchemaStructure based on access name and method (signIn/signup)
type ExtractAccessParams<
  S extends SchemaStructure,
  Name extends keyof S['access'],
  Method extends 'signIn' | 'signup',
> = S['access'] extends undefined
  ? never
  : S['access'][Name] extends AccessDefinition
    ? Prettify<{
        [K in keyof S['access'][Name][Method]['params']]: MapColumnType<
          S['access'][Name][Method]['params'][K]
        >;
      }>
    : never;

function decodeAccessFromToken(token: string): string | null {
  return decodeTokenClaims(token).access;
}

export class AuthService<S extends SchemaStructure> {
  // State
  public token: string | null = null;
  public currentUser: any | null = null;
  public isAuthenticated: boolean = false;
  /**
   * The record-access method name for the current session (e.g. `"account"`),
   * derived from the token's `AC` claim. Consumed by the in-browser SSP's
   * permission injection so `$access`-gated table predicates resolve locally,
   * mirroring the server's `$access`. Null when logged out.
   */
  public access: string | null = null;
  public isLoading: boolean = true;

  private events = createAuthEventSystem();

  /** The impersonation last announced, for change detection. */
  private announcedImpersonation: ImpersonationInfo | null = null;
  /** Target of an impersonation that just ended, until the client purges its
   *  local bucket (see `consumeEndedImpersonation`). */
  private endedImpersonationTarget: string | null = null;
  private impersonationTimers = new ImpersonationTimers({
    current: () => this.impersonation,
    renew: () => this.renewImpersonation(),
    stillActive: async () => {
      const [row] = await this.remote.query<[unknown]>('RETURN fn::_00_impersonate::current()');
      return row !== null && row !== undefined;
    },
    endedRemotely: async (reason) => {
      this.logger.warn(
        { reason, Category: 'sp00ky-client::AuthService::impersonation' },
        'Impersonation ended on the server; returning to the admin session'
      );
      await this.stopImpersonating({ serverEnded: true });
    },
    isNetworkError: (error) => classifySyncError(error) === 'network',
  });

  public get eventSystem() {
    return this.events;
  }

  constructor(
    private schema: S,
    private remote: RemoteDatabaseService,
    private persistenceClient: PersistenceClient,
    private logger: Logger
  ) {}

  async init() {
    await this.check();
  }

  getAccessDefinition<Name extends keyof S['access']>(name: Name): AccessDefinition | undefined {
    return this.schema.access?.[name as string];
  }

  /**
   * Subscribe to auth state changes.
   * callback is called immediately with current value and whenever validation status changes.
   */
  subscribe(cb: (userId: string | null) => void): () => void {
    // Immediate callback
    cb(this.currentUser?.id || null);

    const id = this.events.subscribe(AuthEventTypes.AuthStateChanged, (event) => {
      cb(event.payload);
    });

    return () => {
      this.events.unsubscribe(id);
    };
  }

  private notifyListeners() {
    const userId = this.currentUser?.id || null;
    this.events.emit(AuthEventTypes.AuthStateChanged, userId);
    const impersonation = this.impersonation;
    if (!sameImpersonation(impersonation, this.announcedImpersonation)) {
      this.announcedImpersonation = impersonation;
      this.events.emit(AuthEventTypes.ImpersonationChanged, impersonation);
    }
    this.impersonationTimers.sync();
  }

  /**
   * The impersonation the current session is, or null. Derived from the
   * token in use, so it always describes the identity the server sees.
   */
  get impersonation(): ImpersonationInfo | null {
    return this.isAuthenticated ? impersonationFromToken(this.token) : null;
  }

  /** Subscribe to impersonation changes; called immediately with the current value. */
  subscribeImpersonation(cb: (info: ImpersonationInfo | null) => void): () => void {
    cb(this.impersonation);
    const id = this.events.subscribe(AuthEventTypes.ImpersonationChanged, (event) => {
      cb(event.payload);
    });
    return () => {
      this.events.unsubscribe(id);
    };
  }

  /**
   * Act as `targetId` (a record id such as `user:abc`). Admins only, and only
   * when the project enabled `impersonation` in sp00ky.yml: the server refuses
   * everything else. `reason` is recorded in the audit log.
   *
   * The admin's session is kept aside and restored by
   * {@link stopImpersonating}. Local data switches to the target's bucket
   * exactly as on a sign-in, and that bucket is removed on stop.
   */
  async impersonate(targetId: string, reason: string): Promise<ImpersonationInfo> {
    if (this.impersonation) throw new Error('Already impersonating; stop first.');
    if (!this.isAuthenticated || !this.token) throw new Error('Sign in as an admin first.');
    const originToken = this.token;
    const [started] = await this.remote.query<[{ token: string }]>(
      'RETURN fn::_00_impersonate::start($target, $reason)',
      { target: targetId, reason }
    );
    if (!started?.token) throw new Error('The server did not return an impersonation token.');

    await this.persistenceClient.set(ORIGIN_TOKEN_KEY, originToken);
    try {
      await this.remote.getClient().authenticate(started.token);
    } catch (error) {
      // The server refused the session it just issued (for example the
      // target is an admin, or no longer exists). Nothing changed locally
      // yet; put the admin's token back on the transport.
      await this.persistenceClient.remove(ORIGIN_TOKEN_KEY);
      this.remote.setAuthToken(originToken);
      await this.remote.getClient().authenticate(originToken).catch(() => undefined);
      throw error;
    }
    await this.check(started.token);
    // A method call, not the getter: TS narrowed the getter to null above.
    const info = impersonationFromToken(this.isAuthenticated ? this.token : null);
    if (!info) {
      await this.stopImpersonating();
      throw new Error('Impersonation could not be established.');
    }
    this.logger.info(
      { target: info.target, session: info.session, Category: 'sp00ky-client::AuthService::impersonate' },
      'Impersonation started'
    );
    return info;
  }

  /**
   * Return to the admin session. Always leaves the impersonated identity,
   * even when the server cannot be reached: the admin token is restored
   * locally first and verified afterwards, and a rejected admin token ends in
   * a full sign-out rather than staying on the target.
   */
  async stopImpersonating(opts: { serverEnded?: boolean } = {}): Promise<void> {
    const info = this.impersonation;
    if (!info) return;
    this.impersonationTimers.clear();
    if (!opts.serverEnded) {
      try {
        await this.remote.query('RETURN fn::_00_impersonate::stop($session)', { session: info.session });
      } catch (error) {
        // The token expires on its own; the local switch below is what matters.
        this.logger.warn(
          { error, Category: 'sp00ky-client::AuthService::stopImpersonating' },
          'Could not end the impersonation session on the server'
        );
      }
    }
    this.endedImpersonationTarget = info.target;
    const origin = await this.persistenceClient.get<string>(ORIGIN_TOKEN_KEY);
    await this.persistenceClient.remove(ORIGIN_TOKEN_KEY);
    if (!origin || impersonationFromToken(origin)) {
      await this.signOut();
      return;
    }
    const { access, userId } = decodeTokenClaims(origin);
    if (!userId) {
      await this.signOut();
      return;
    }
    // Optimistic switch back, as a warm boot does, then verify.
    this.token = origin;
    this.remote.setAuthToken(origin);
    this.currentUser = { id: userId };
    this.access = access ?? this.defaultAccessName();
    await this.persistenceClient.set('sp00ky_auth_token', origin);
    this.notifyListeners();
    await this.check(origin);
    this.logger.info(
      { target: info.target, Category: 'sp00ky-client::AuthService::stopImpersonating' },
      'Impersonation stopped'
    );
  }

  /** Re-sign the impersonation token before it expires. */
  async renewImpersonation(): Promise<void> {
    if (!this.impersonation) return;
    const [renewed] = await this.remote.query<[{ token: string }]>('RETURN fn::_00_impersonate::renew()');
    if (!renewed?.token) throw new Error('Impersonation renewal returned no token.');
    this.remote.setAuthToken(renewed.token);
    await this.remote.getClient().authenticate(renewed.token);
    this.token = renewed.token;
    await this.persistenceClient.set('sp00ky_auth_token', renewed.token);
    this.notifyListeners();
  }

  /** Users an admin may pick from. Admin-only on the server. */
  async searchImpersonationTargets(search: string): Promise<ImpersonationCandidate[]> {
    const [rows] = await this.remote.query<[ImpersonationCandidate[]]>(
      'RETURN fn::_00_impersonate::list_users($search)',
      { search }
    );
    return Array.isArray(rows) ? rows : [];
  }

  /** Open impersonation sessions. Admin-only on the server. */
  async listActiveImpersonations(): Promise<ActiveImpersonation[]> {
    const [rows] = await this.remote.query<[ActiveImpersonation[]]>('RETURN fn::_00_impersonate::active()');
    return Array.isArray(rows) ? rows : [];
  }

  /**
   * The target of an impersonation that ended, once. The bucket switch calls
   * this after leaving a bucket so it can delete that user's local data.
   */
  consumeEndedImpersonation(): string | null {
    const target = this.endedImpersonationTarget;
    this.endedImpersonationTarget = null;
    return target;
  }

  /**
   * Restore a session from the locally cached JWT, with NO network.
   *
   * This is what makes a warm boot paint instantly and what makes an offline
   * boot possible at all: the token is in local storage, and it already carries
   * both the access method and the `$auth.id` record id. Everything the client
   * needs to route queries (`setCurrentUserId`) and to satisfy `$auth`-gated
   * permission predicates in the in-browser SSP (`setSessionAuth`) is therefore
   * available before a socket exists.
   *
   * The session is OPTIMISTIC: the token is unverified here. `check()` runs
   * afterwards in the background and downgrades to a real sign-out if the
   * server rejects it. Nothing is trusted that the server has not also seen -
   * the local store only ever holds rows the server previously sent.
   *
   * Returns the restored user id, or null when there is no usable token.
   */
  async restoreSessionFromToken(): Promise<string | null> {
    const token = await this.persistenceClient.get<string>('sp00ky_auth_token');
    if (!token) return null;
    const { access, userId } = decodeTokenClaims(token);
    if (!userId) return null;

    this.token = token;
    // Hand it to the transport too, so a socket rebuilt from scratch later
    // (the supervisor's revive loop) comes back authenticated. Without this the
    // page kept reporting this user while its session was anonymous, and every
    // view registered afterwards was stamped with an empty identity.
    this.remote.setAuthToken(token);
    // Only the id: the full row is not in the token. It lands from the local
    // cache when the app's own `user` query paints, and is replaced wholesale
    // by `check()` once the server answers.
    this.currentUser = { id: userId };
    this.isAuthenticated = true;
    this.access = access ?? this.defaultAccessName();
    this.notifyListeners();
    this.logger.debug(
      { userId, Category: 'sp00ky-client::AuthService::restoreSessionFromToken' },
      'Session restored optimistically from cached token'
    );
    return userId;
  }

  /**
   * Check for existing session and validate
   */
  async check(accessToken?: string) {
    this.isLoading = true;

    let token: string | undefined;
    try {
      token = accessToken || (await this.persistenceClient.get<string>('sp00ky_auth_token')) || undefined;

      if (!token) {
        this.logger.debug(
          { Category: 'sp00ky-client::AuthService::check' },
          'No token found in storage or arguments'
        );
        this.isLoading = false;
        this.isAuthenticated = false;
        this.notifyListeners();
        return;
      }

      // Authenticate with the token, and record it for future connects so a
      // socket rebuilt from scratch reproduces this identity (see
      // `RemoteDatabaseService.setAuthToken`).
      this.remote.setAuthToken(token);
      await this.remote.getClient().authenticate(token);

      // Verify the session by fetching the full user record using $auth.id
      const result = await this.remote.query('SELECT * FROM ONLY $auth.id');

      const items = Array.isArray(result) && Array.isArray(result[0]) ? result[0] : result;
      const user = Array.isArray(items) ? items[0] : items;

      if (user && user.id) {
        this.logger.info(
          { user, Category: 'sp00ky-client::AuthService::check' },
          'Auth check complete (via $auth.id)'
        );
        await this.setSession(token, user);
      } else {
        this.logger.warn(
          { Category: 'sp00ky-client::AuthService::check' },
          '$auth.id empty, attempting manual user fetch'
        );

        const manualResult = await this.remote.query(
          'SELECT * FROM user WHERE id = $auth.id LIMIT 1'
        );
        const manualItems =
          Array.isArray(manualResult) && Array.isArray(manualResult[0])
            ? manualResult[0]
            : manualResult;
        const manualUser = Array.isArray(manualItems) ? manualItems[0] : manualItems;

        if (manualUser && manualUser.id) {
          this.logger.info(
            { user: manualUser, Category: 'sp00ky-client::AuthService::check' },
            'Auth check complete (via manual fetch)'
          );
          await this.setSession(token, manualUser);
        } else {
          this.logger.warn(
            { Category: 'sp00ky-client::AuthService::check' },
            'Token valid but user not found via fallback'
          );
          await this.rejectToken(token);
        }
      }
    } catch (error) {
      // A REACHABILITY failure is not a rejected token. This catch used to call
      // signOut() unconditionally, which deletes `sp00ky_auth_token` - so a
      // blip on boot silently logged the user out, and an offline boot could
      // never stay signed in. Only an application error (the server answered,
      // and the answer was "no") ends the session.
      if (classifySyncError(error) === 'network') {
        this.logger.warn(
          { error, Category: 'sp00ky-client::AuthService::check' },
          'Auth check unreachable; keeping the cached session and retrying later'
        );
      } else {
        this.logger.error(
          { error, stack: (error as Error).stack, Category: 'sp00ky-client::AuthService::check' },
          'Auth check failed'
        );
        await this.rejectToken(token);
      }
    } finally {
      this.isLoading = false;
    }
  }

  /**
   * The server refused `token`. A refused impersonation token (stopped,
   * expired, or revoked) returns to the admin session instead of signing the
   * admin out; anything else signs out.
   */
  private async rejectToken(token: string | undefined): Promise<void> {
    if (token && impersonationFromToken(token)) {
      if (!this.impersonation || this.token !== token) {
        // A boot or explicit check with an impersonation token that is not
        // the session in memory yet: adopt it so stop has something to leave.
        this.token = token;
        this.isAuthenticated = true;
      }
      await this.stopImpersonating({ serverEnded: true });
      return;
    }
    await this.signOut();
  }

  /**
   * Sign out and clear session. While impersonating this ends the
   * impersonation and signs the admin out too.
   */
  async signOut() {
    const impersonating = this.impersonation;
    this.impersonationTimers.clear();
    if (impersonating) {
      this.endedImpersonationTarget = impersonating.target;
      try {
        await this.remote.query('RETURN fn::_00_impersonate::stop($session)', {
          session: impersonating.session,
        });
      } catch (_e) {
        // The token expires on its own.
      }
    }
    await this.persistenceClient.remove(ORIGIN_TOKEN_KEY);
    this.token = null;
    this.remote.setAuthToken(null);
    this.currentUser = null;
    this.isAuthenticated = false;
    this.access = null;

    await this.persistenceClient.remove('sp00ky_auth_token');

    try {
      await this.remote.getClient().invalidate();
    } catch (_e) {
      // Ignore invalidation errors
    }

    this.notifyListeners();
  }

  private async setSession(token: string, user: any) {
    this.token = token;
    this.remote.setAuthToken(token);
    this.currentUser = user;
    this.isAuthenticated = true;
    // Resolve the access-method name (e.g. "account") for in-browser SSP
    // permission injection. Prefer the token's `AC` claim; fall back to the
    // schema's sole record-access method if the claim is absent.
    this.access = decodeAccessFromToken(token) ?? this.defaultAccessName();
    await this.persistenceClient.set('sp00ky_auth_token', token);
    this.notifyListeners();
  }

  /** Fallback when the token carries no `AC` claim: if the schema defines
   *  exactly one record-access method, assume the session used it. */
  private defaultAccessName(): string | null {
    const names = Object.keys(this.schema.access ?? {});
    return names.length === 1 ? names[0] : null;
  }

  async signUp<Name extends keyof S['access'] & string>(
    accessName: Name,
    params: ExtractAccessParams<S, Name, 'signup'>
  ) {
    const def = this.getAccessDefinition(accessName);
    if (!def) throw new Error(`Access definition '${accessName}' not found`);

    // Verify all required params are present
    // Safe cast params to Record<string, any> for runtime check
    const runtimeParams = params as Record<string, any>;

    const missingParams = Object.entries(def.signup.params)
      .filter(([name, schema]) => !schema.optional && !(name in runtimeParams))
      .map(([name]) => name);

    if (missingParams.length > 0) {
      throw new Error(
        `Missing required signup params for '${accessName}': ${missingParams.join(', ')}`
      );
    }

    this.logger.info(
      { accessName, runtimeParams, Category: 'sp00ky-client::AuthService::signUp' },
      'Attempting signup'
    );

    const { access } = await this.remote.getClient().signup({
      access: accessName,
      variables: runtimeParams,
    });

    this.logger.info(
      { Category: 'sp00ky-client::AuthService::signUp' },
      'Signup successful, token received'
    );

    // After signup, we usually get a token.
    // We should also fetch the user or trust the token works.
    // For now, let's just trigger a check() to fully hydrate state
    await this.check(access);
  }

  async signIn<Name extends keyof S['access'] & string>(
    accessName: Name,
    params: ExtractAccessParams<S, Name, 'signIn'>
  ) {
    const def = this.getAccessDefinition(accessName);
    if (!def) throw new Error(`Access definition '${accessName}' not found`);

    const runtimeParams = params as Record<string, any>;

    // Verify all required params are present
    const missingParams = Object.entries(def.signIn.params)
      .filter(([name, schema]) => !schema.optional && !(name in runtimeParams))
      .map(([name]) => name);

    if (missingParams.length > 0) {
      throw new Error(
        `Missing required signin params for '${accessName}': ${missingParams.join(', ')}`
      );
    }

    this.logger.info(
      { accessName, Category: 'sp00ky-client::AuthService::signIn' },
      'Attempting signin'
    );

    const { access } = await this.remote.getClient().signin({
      access: accessName,
      variables: runtimeParams,
    });

    await this.check(access);
  }
}
