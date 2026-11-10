/**
 * What the Git tab does with a project's link to a repository and an edit of
 * it: the request that saves the edit, what to confirm first, and what to say
 * about the branch. Nothing here fetches; the tab feeds it and renders the
 * answer. The link itself is the control plane's (internal/linking: GET, PATCH
 * and DELETE /v1/projects/{id}/link, GET /v1/github/branches).
 */

import type { ConfirmSpec } from '../components/Actions';

export interface Link {
  status: 'linked' | 'not_linked' | 'pending_repo_selection' | string;
  installation_id?: number;
  repo_owner?: string;
  repo_name?: string;
  branch?: string;
  auto_deploy?: boolean;
  config_path?: string;
}

/** A repository's branches, the default one first (`GET /v1/github/branches`). */
export interface BranchList {
  default_branch: string;
  branches: string[];
  truncated?: boolean;
}

/** The edit as it stands in the form. */
export interface Draft {
  /** The GitHub App installation the repository is read through. */
  installation?: number;
  /** owner/name */
  repo: string;
  branch: string;
  configPath: string;
  /** Only sent when connecting; a linked project keeps its own setting. */
  autoDeploy: boolean;
}

export const DEFAULT_CONFIG = 'sp00ky.yml';

/** owner/name of the linked repository, or undefined when there is none. */
export function linkedRepo(l: Link | undefined): string | undefined {
  return l?.status === 'linked' && l.repo_owner && l.repo_name ? `${l.repo_owner}/${l.repo_name}` : undefined;
}

/** Where an edit starts: the link as it stands, or an empty one on the first account. */
export function draftFrom(l: Link | undefined, firstInstallation?: number): Draft {
  const repo = linkedRepo(l);
  return repo
    ? { installation: l!.installation_id, repo, branch: l!.branch ?? '', configPath: l!.config_path || DEFAULT_CONFIG, autoDeploy: l!.auto_deploy ?? true }
    : { installation: firstInstallation, repo: '', branch: '', configPath: DEFAULT_CONFIG, autoDeploy: true };
}

/** Whether saving the draft points the project at another repository (or links it at all). */
export function moves(l: Link | undefined, d: Draft): boolean {
  const current = linkedRepo(l);
  return !current || d.repo.trim() !== current || (d.installation !== undefined && d.installation !== l!.installation_id);
}

/**
 * The PATCH body that saves a draft, or null when it changes nothing. A move
 * sends the whole link, because the server replaces the repository, branch and
 * manifest together; anything else sends only what changed.
 */
export function patchBody(l: Link | undefined, d: Draft): Record<string, unknown> | null {
  const repo = d.repo.trim();
  const branch = d.branch.trim();
  const config = d.configPath.trim() || DEFAULT_CONFIG;
  if (moves(l, d)) {
    const [repo_owner, repo_name] = repo.split('/');
    const body: Record<string, unknown> = { repo_owner, repo_name, branch, config_path: config };
    if (d.installation !== undefined) body.installation_id = d.installation;
    if (!linkedRepo(l)) body.auto_deploy = d.autoDeploy;
    return body;
  }
  const body: Record<string, unknown> = {};
  if (branch !== (l!.branch ?? '')) body.branch = branch;
  if (config !== (l!.config_path || DEFAULT_CONFIG)) body.config_path = config;
  return Object.keys(body).length ? body : null;
}

const REPO = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;

/**
 * A branch name git would take; mirrors `validBranch` in the control plane, so
 * the form says so before the server does.
 */
export function validBranch(name: string): boolean {
  if (!name || name.length > 255 || name === '@') return false;
  if (/^[-/]|[/.]$|\.lock$|\.\.|\/\/|@\{/.test(name)) return false;
  if (/[\x00-\x20\x7f~^:?*[\\]/.test(name)) return false;
  return name.split('/').every((part) => !part.startsWith('.'));
}

/** A file inside the checkout; mirrors `validConfigPath` in the control plane. */
export function validConfigPath(p: string): boolean {
  if (!p || p.length > 512 || p.startsWith('/') || p.includes('\\')) return false;
  return p.split('/').every((part) => part !== '' && part !== '.' && part !== '..');
}

/**
 * What stops the draft from saving, or null. `repos` is what the chosen
 * account's installation can see; undefined while it loads or for an account
 * the person cannot read through, in which case the repository is not judged.
 */
export function problem(l: Link | undefined, d: Draft, repos: string[] | undefined): string | null {
  const repo = d.repo.trim();
  const move = moves(l, d);
  if (move && d.installation === undefined) return 'Pick a GitHub account';
  if (!REPO.test(repo)) return 'Pick a repository';
  if (move && repos && !repos.includes(repo)) return `The app cannot see ${repo} on that account`;
  if (!validBranch(d.branch.trim())) return d.branch.trim() ? `"${d.branch.trim()}" is not a branch name` : 'Name a branch';
  const config = d.configPath.trim() || DEFAULT_CONFIG;
  if (!validConfigPath(config)) return 'The manifest must be a file inside the repository, like sp00ky.yml or apps/api/sp00ky.yml';
  return null;
}

/** A line under the branch field, or null for nothing to say. */
export function branchNote(branch: string, list: BranchList | undefined): { tone: 'dim' | 'warn'; text: string } | null {
  const name = branch.trim();
  if (!list || !name) return null;
  if (name === list.default_branch) return { tone: 'dim', text: 'The default branch' };
  if (list.branches.includes(name)) return null;
  if (list.truncated) return null;
  return { tone: 'warn', text: `No ${name} on GitHub yet. Pushes to it deploy once it exists.` };
}

/** What to ask before a save, or null for a save that needs no asking. Only a move does. */
export function saveConfirm(l: Link | undefined, d: Draft, project: string): ConfirmSpec | null {
  const current = linkedRepo(l);
  if (!current || !moves(l, d)) return null;
  const repo = d.repo.trim();
  const branch = d.branch.trim();
  return {
    title: `Deploy ${project} from ${repo}`,
    consequences: [
      `Pushes to ${current} (${l!.branch}) stop deploying ${project}.`,
      l!.auto_deploy
        ? `Pushes to ${repo} (${branch}) deploy ${project}.`
        : `Pushes to ${repo} (${branch}) build nothing until deploy on push is turned on.`,
      'What is live keeps running until the next build. Build history stays.',
    ],
    verb: 'Change repository',
  };
}

export function unlinkConfirm(l: Link, project: string): ConfirmSpec {
  return {
    title: `Unlink ${linkedRepo(l) ?? 'the repository'}`,
    consequences: [
      `Pushes no longer deploy ${project}.`,
      'Its build history is deleted.',
      'What is live keeps running; deploy again with spky deploy or by linking a repository.',
    ],
    verb: 'Unlink',
  };
}

/** The toast after a save. */
export function savedMessage(l: Link | undefined, d: Draft, body: Record<string, unknown>): string {
  const repo = d.repo.trim();
  if (!linkedRepo(l)) return `Linked to ${repo}`;
  if (moves(l, d)) return `Now deploying from ${repo}`;
  const parts: string[] = [];
  if ('branch' in body) parts.push(`following ${body.branch}`);
  if ('config_path' in body) parts.push(`reading ${body.config_path}`);
  const line = parts.join(', ');
  return line.charAt(0).toUpperCase() + line.slice(1);
}
