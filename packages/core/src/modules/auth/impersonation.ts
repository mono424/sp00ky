/**
 * Admin impersonation, client side.
 *
 * The server does the enforcing (see `impersonation_remote.surql` in the CLI):
 * an admin calls `fn::_00_impersonate::start`, receives a token for the
 * `_00_impersonate` access method, and authenticates with it, so `$auth.id` is
 * the target user. Everything here is state and timers around that:
 *
 * - the admin's own token is kept under {@link ORIGIN_TOKEN_KEY} so Stop (and a
 *   reload in between) can return to it;
 * - the impersonation state is derived from the token's claims, never stored
 *   separately, so it cannot disagree with the session actually in use;
 * - the token is re-signed before it expires, and the session is polled so a
 *   stop or revocation made elsewhere reaches an open WebSocket quickly.
 */

/** Access method impersonation tokens are issued for. */
export const IMPERSONATION_ACCESS = '_00_impersonate';

/** Persistence key holding the admin's own token while impersonating. */
export const ORIGIN_TOKEN_KEY = 'sp00ky_auth_token_origin';

/** How often an active impersonation re-checks its session on the server. */
export const SESSION_POLL_MS = 60_000;

export interface ImpersonationInfo {
  /** `_00_impersonation:<id>` the session is bound to. */
  session: string;
  /** Record id being impersonated. */
  target: string;
  /** Record id of the admin. */
  admin: string;
  /** When the current token expires (it is renewed before then). */
  tokenExpiresAt: Date | null;
}

export interface TokenClaims {
  access: string | null;
  userId: string | null;
  exp: number | null;
  impersonation: { session: string; admin: string } | null;
}

/**
 * Read the claims of a SurrealDB record-access JWT WITHOUT verifying it. The
 * server still enforces the token on every request; this is only so the client
 * can act on what it already holds before a round trip completes.
 *
 * Returns nulls on any malformed input.
 */
export function decodeTokenClaims(token: string): TokenClaims {
  const none: TokenClaims = { access: null, userId: null, exp: null, impersonation: null };
  try {
    const payload = token.split('.')[1];
    if (!payload) return none;
    let b64 = payload.replace(/-/g, '+').replace(/_/g, '/');
    b64 += '='.repeat((4 - (b64.length % 4)) % 4);
    const json =
      typeof atob === 'function' ? atob(b64) : Buffer.from(b64, 'base64').toString('binary');
    const claims = JSON.parse(json) as Record<string, unknown>;
    const ac = claims.AC ?? claims.ac;
    const id = claims.ID ?? claims.id;
    const access = typeof ac === 'string' ? ac : null;
    const session = claims.spky_imp;
    const admin = claims.spky_admin;
    return {
      access,
      userId: typeof id === 'string' ? id : null,
      exp: typeof claims.exp === 'number' ? claims.exp : null,
      impersonation:
        access === IMPERSONATION_ACCESS && typeof session === 'string' && typeof admin === 'string'
          ? { session, admin }
          : null,
    };
  } catch {
    return none;
  }
}

/** The impersonation a token represents, or null for a regular session. */
export function impersonationFromToken(token: string | null): ImpersonationInfo | null {
  if (!token) return null;
  const claims = decodeTokenClaims(token);
  if (!claims.impersonation || !claims.userId) return null;
  return {
    session: claims.impersonation.session,
    admin: claims.impersonation.admin,
    target: claims.userId,
    tokenExpiresAt: claims.exp === null ? null : new Date(claims.exp * 1000),
  };
}

/** Same impersonation, for change detection (renewals only move the expiry). */
export function sameImpersonation(a: ImpersonationInfo | null, b: ImpersonationInfo | null): boolean {
  if (a === null || b === null) return a === b;
  return a.session === b.session && a.target === b.target && a.admin === b.admin;
}

/**
 * When to renew a token expiring at `expSecs`: at 80% of its remaining life,
 * never sooner than 5s from now, and immediately once it is inside that.
 */
export function renewDelayMs(expSecs: number, nowMs: number): number {
  const remaining = expSecs * 1000 - nowMs;
  if (remaining <= 5_000) return 0;
  return Math.max(5_000, Math.floor(remaining * 0.8));
}

export interface ImpersonationTimerHost {
  /** Current impersonation, or null. */
  current(): ImpersonationInfo | null;
  /** Re-sign the token. Rejects when the session has ended. */
  renew(): Promise<void>;
  /** Whether the server still considers the session live. */
  stillActive(): Promise<boolean>;
  /** Leave the impersonation because the server ended it. */
  endedRemotely(reason: string): Promise<void>;
  /** Whether a failure was a reachability problem (retry) rather than an answer. */
  isNetworkError(error: unknown): boolean;
}

/**
 * The two timers an active impersonation needs. `sync()` is called whenever
 * the auth state changes and arms or clears them to match.
 */
export class ImpersonationTimers {
  private renewTimer: ReturnType<typeof setTimeout> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private armedFor: string | null = null;

  constructor(private host: ImpersonationTimerHost) {}

  sync(): void {
    const info = this.host.current();
    if (!info) {
      this.clear();
      return;
    }
    const key = `${info.session}@${info.tokenExpiresAt?.getTime() ?? 'none'}`;
    if (key === this.armedFor) return;
    this.clear();
    this.armedFor = key;
    if (info.tokenExpiresAt) {
      const delay = renewDelayMs(info.tokenExpiresAt.getTime() / 1000, Date.now());
      this.renewTimer = setTimeout(() => void this.renew(), delay);
    }
    this.pollTimer = setInterval(() => void this.poll(), SESSION_POLL_MS);
  }

  clear(): void {
    if (this.renewTimer) clearTimeout(this.renewTimer);
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.renewTimer = null;
    this.pollTimer = null;
    this.armedFor = null;
  }

  private async renew(): Promise<void> {
    this.renewTimer = null;
    try {
      await this.host.renew();
    } catch (error) {
      if (this.host.isNetworkError(error)) {
        // Try again shortly; the token may still have some life left.
        this.renewTimer = setTimeout(() => void this.renew(), 10_000);
        return;
      }
      await this.host.endedRemotely('renew refused');
    }
  }

  private async poll(): Promise<void> {
    try {
      if (!(await this.host.stillActive())) await this.host.endedRemotely('session ended');
    } catch (error) {
      if (!this.host.isNetworkError(error)) await this.host.endedRemotely('session check refused');
    }
  }
}
