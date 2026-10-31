/**
 * The console's calls to Sp00ky Cloud.
 *
 * The console is served by the control plane on its own host, which routes
 * `/v1/*` to the API: every call is same-origin, authenticated by the
 * HttpOnly session cookie, and carries `X-Spky-Console: 1`, the proof that one
 * of the console's own pages made it (the API refuses the cookie without it).
 * Nothing here ever sees or stores a token for the cloud.
 */

export class CloudError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | undefined,
    message: string,
  ) {
    super(message);
    this.name = 'CloudError';
  }
}

/** Send the browser through GitHub, back to where it was. */
export function signIn(): never {
  const back = window.location.pathname + window.location.search;
  window.location.assign(`/v1/auth/web/login?return_to=${encodeURIComponent(back)}`);
  throw new CloudError(401, 'unauthorized', 'Signing in');
}

export async function signOut(): Promise<void> {
  await fetch('/v1/auth/web/logout', { method: 'POST', credentials: 'same-origin', headers: { 'X-Spky-Console': '1' } });
}

export interface CallOptions {
  method?: string;
  body?: unknown;
  /** The workspace a tenant-scoped route acts in (`X-Tenant-ID`). */
  tenant?: string;
}

export async function cloud<T>(path: string, opts: CallOptions = {}): Promise<T> {
  const headers: Record<string, string> = { 'X-Spky-Console': '1' };
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  if (opts.tenant) headers['X-Tenant-ID'] = opts.tenant;
  const res = await fetch(`/v1${path}`, {
    method: opts.method ?? 'GET',
    credentials: 'same-origin',
    headers,
    body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
  });
  if (res.status === 401) signIn();
  if (!res.ok) {
    let message = `Request failed (${res.status})`;
    let code: string | undefined;
    try {
      const body = await res.json();
      if (typeof body?.error === 'string') message = body.error;
      if (typeof body?.code === 'string') code = body.code;
    } catch {
      /* keep the default */
    }
    throw new CloudError(res.status, code, message);
  }
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

/* ------------------------------------------------------------------ */
/* Console routes (/v1/console)                                         */
/* ------------------------------------------------------------------ */

export interface Me {
  account: { id: string; github_login: string | null; email: string | null; created_at: string };
  session: string;
  grants: boolean;
}

export interface WorkspaceProject {
  id: string;
  slug: string;
  plan: string;
  platform: 'hetzner' | 'cloudflare' | string;
  status: string;
  deployment_status: string | null;
  deployment_version: number | null;
  admin: 'scheduler' | 'ssp';
}

export interface Workspace {
  id: string;
  name: string;
  slug: string;
  type: 'personal' | 'organization' | string;
  role: 'admin' | 'member' | string;
  projects: WorkspaceProject[];
}

export interface Grant {
  grant: string;
  admin_url: string;
  admin: 'scheduler' | 'ssp';
  scope: 'read' | 'full';
  expires_at: number;
}

export interface ConsoleSession {
  id: string;
  ip: string | null;
  user_agent: string | null;
  created_at: string;
  last_seen_at: string;
  expires_at: string;
  current: boolean;
}

export interface DeploymentRow {
  id: string;
  version: number;
  status: string;
  error: string | null;
  live: boolean;
  live_from: string | null;
  live_until: string | null;
  source: string | null;
  actor: string | null;
  rollback_of: number | null;
  /** Infra role -> image tag (scheduler, ssp, surrealdb, ssp-cf). */
  images: Record<string, unknown>;
  created_at: string;
  deployed_at: string | null;
  updated_at: string;
  build: { commit_sha: string; commit_message: string | null; commit_author: string | null; status: string } | null;
}

export interface PendingBuild {
  id: string;
  commit_sha: string;
  commit_message: string | null;
  commit_author: string | null;
  status: string;
  started_at: string | null;
  created_at: string;
}

export interface AuditEvent {
  id: number;
  at: string;
  actor: string;
  action: string;
  project: string | null;
  ip: string | null;
  detail: Record<string, unknown>;
}

export const consoleApi = {
  me: () => cloud<Me>('/console/me'),
  workspaces: () => cloud<{ workspaces: Workspace[] }>('/console/workspaces').then((r) => r.workspaces),
  grant: (projectId: string) => cloud<Grant>(`/console/projects/${encodeURIComponent(projectId)}/grant`, { method: 'POST' }),
  sessions: () => cloud<ConsoleSession[]>('/console/sessions'),
  revokeSession: (id: string) => cloud<void>(`/console/sessions/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  revokeOthers: () => cloud<void>('/console/sessions/revoke-others', { method: 'POST' }),
  deployments: (projectId: string) =>
    cloud<{ deployments: DeploymentRow[]; builds: PendingBuild[] }>(`/console/projects/${encodeURIComponent(projectId)}/deployments`),
  activity: (tenantId: string) =>
    cloud<{ events: AuditEvent[] }>(`/console/workspaces/${encodeURIComponent(tenantId)}/activity`).then((r) => r.events),
};
