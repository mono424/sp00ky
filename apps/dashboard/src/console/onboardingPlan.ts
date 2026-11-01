/**
 * What the first-project page shows, as a function of what is known: what is
 * in the repository, and how far the person's commands have got. Nothing here
 * runs anything; the page feeds it fresh facts and renders the answer, so a
 * step can never disagree with the state it describes.
 */

import type { Inspection, WorkspaceProject } from './cloud';

/** The CLI's default; any other cloud has to be named to it. */
export const DEFAULT_CLOUD_API = 'https://api.sp00ky.cloud';

export type StepState = 'done' | 'active' | 'waiting';

export interface Facts {
  login?: string | null;
  cliLoginAt?: string | null;
  apiUrl?: string;
  /** Undefined until it has been read; null when starting without a repository. */
  inspection: Inspection | null | undefined;
  project?: WorkspaceProject;
  /** The workspace the page is in: the CLI deploys to the personal one. */
  workspaceType?: string;
  /** Whether pushes to the repository deploy the project. */
  link?: RepoLink;
  now: number;
}

export type RepoLink =
  | { state: 'unknown' }
  | { state: 'none' }
  | { state: 'working' }
  | { state: 'linked'; repo: string; branch: string }
  | { state: 'failed'; error: string };

export interface Check {
  label: string;
  state: StepState | 'bad';
  detail?: string;
  /** A click this row is waiting for. */
  action?: 'connect';
}

export interface Step {
  id: 'login' | 'config' | 'deploy';
  title: string;
  state: StepState;
  /**
   * What to run, a block to a paste: a line starting with `# ` is a note, the
   * rest are commands. A command that asks questions ends its block, because
   * whatever is pasted after it would become its answers.
   */
  blocks: string[][];
  /** What happened, once it has. */
  result?: string;
  /** What the page is watching for meanwhile. */
  watching?: string;
  checks?: Check[];
}

export function isLive(p: WorkspaceProject | undefined): boolean {
  return p?.deployment_status === 'running' || p?.deployment_status === 'degraded';
}

export function commit(sha: string | undefined): string {
  return (sha ?? '').slice(0, 7);
}

function ago(at: string | null | undefined, now: number): string {
  if (!at) return '';
  const s = Math.max(0, Math.round((now - new Date(at).getTime()) / 1000));
  if (s < 5) return 'just now';
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86_400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86_400)}d ago`;
}

/** How the CLI runs in this repository: its own dependency, or installed globally. */
function spky(ins: Inspection | null | undefined): string {
  if (!ins?.has_cli) return 'spky';
  return { pnpm: 'pnpm spky', yarn: 'yarn spky', bun: 'bunx spky' }[ins.package_manager ?? ''] ?? 'npx spky';
}

/** The CLI goes by the repository's `cloudApi`, else its default; a variable overrides both. */
function needsApiOverride(f: Facts): boolean {
  if (!f.apiUrl) return false;
  return (f.inspection?.config?.cloud_api ?? DEFAULT_CLOUD_API) !== f.apiUrl;
}

/**
 * A value from the repository, safe to paste into a shell. A branch or a
 * directory is anyone's choice, and a page handing out commands must not run
 * what they chose.
 */
export function shellQuote(value: string): string {
  return /^[\w@%+=:,./-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`;
}

/** Each block can be pasted into a fresh shell, so each one that talks to the cloud names it. */
function cloudLines(f: Facts): string[] {
  return needsApiOverride(f) ? ['# Point the CLI at this cloud', `export SP00KY_CLOUD_API=${f.apiUrl}`] : [];
}

function loginStep(f: Facts): Omit<Step, 'state'> {
  const run = spky(f.inspection);
  const lines: string[] = [];
  if (f.inspection?.has_cli) {
    lines.push(`# In your checkout of ${f.inspection.repo}`, `${f.inspection.package_manager ?? 'npm'} install`);
  } else {
    lines.push(f.inspection ? `# In your checkout of ${f.inspection.repo}` : '# Once per machine', 'npm i -g @spooky-sync/cli');
  }
  lines.push(...cloudLines(f), `${run} login`);
  return {
    id: 'login',
    title: 'Sign in the CLI',
    blocks: [lines],
    result: f.cliLoginAt ? `Signed in${f.login ? ` as ${f.login}` : ''}, ${ago(f.cliLoginAt, f.now)}` : 'Signed in',
    watching: `Waiting for ${run} login`,
  };
}

function configStep(ins: Inspection): Omit<Step, 'state'> {
  const base = {
    id: 'config' as const,
    title: 'Add sp00ky.yml',
    watching: `Watching ${ins.branch} for sp00ky.yml`,
    // Done without one on the branch: spky deploy ran from a checkout.
    result: 'Not pushed yet, deployed from your checkout',
  };
  if (ins.config?.valid) {
    return {
      ...base,
      blocks: [],
      result: `${ins.config.path} on ${ins.branch}${ins.sha ? ` at ${commit(ins.sha)}` : ''}`,
    };
  }
  if (ins.config) {
    return {
      ...base,
      title: 'Fix sp00ky.yml',
      blocks: [[`# ${ins.config.path} does not parse. The linter says where`, `${spky(ins)} lint`], ['git add -A', 'git commit -m "Fix sp00ky.yml"', `git push origin ${shellQuote(ins.branch)}`]],
      watching: `Watching ${ins.branch} for a sp00ky.yml that parses`,
    };
  }
  // `spky init` writes a directory of its own, so it is safe in a repository
  // that already holds an app.
  const name = ins.empty ? ins.repo.split('/').pop()! : 'spooky';
  const note = ins.empty
    ? `# Name it ${name}, choose "Full project", and say no to a new git repository`
    : `# Name it ${name}, choose "Schema only", and say no to a new git repository`;
  return {
    ...base,
    blocks: [[note, `${spky(ins)} init`], [`git add ${shellQuote(name)}`, 'git commit -m "Add Sp00ky"', `git push origin ${shellQuote(ins.branch)}`]],
  };
}

function deployChecks(f: Facts): Check[] {
  const p = f.project;
  const paid = !!p && p.status !== 'pending_payment';
  const status = p?.deployment_status;
  const checks: Check[] = [
    { label: 'Project', state: p ? 'done' : 'active', detail: p ? `${p.slug}, ${p.plan} plan` : 'spky deploy creates it' },
    {
      label: 'Checkout',
      state: paid ? 'done' : p ? 'active' : 'waiting',
      detail: paid ? 'Paid' : p ? 'Finish the checkout spky deploy opened' : undefined,
    },
    {
      label: 'Deployment',
      state: isLive(p) ? 'done' : status === 'failed' ? 'bad' : paid ? 'active' : 'waiting',
      detail:
        status === 'failed'
          ? 'Failed. See Deployments for why'
          : isLive(p)
            ? `Version ${p!.deployment_version ?? 1} is ${status === 'degraded' ? 'up, degraded' : 'live'}`
            : paid
              ? status && status !== 'superseded'
                ? status.replace(/_/g, ' ')
                : 'Starting'
              : undefined,
    },
  ];
  if (f.inspection) checks.push(pushCheck(f, f.inspection, paid));
  return checks;
}

function pushCheck(f: Facts, ins: Inspection, paid: boolean): Check {
  const label = 'Deploy on push';
  if (!paid) return { label, state: 'waiting', detail: 'Available once the project is paid for' };
  const link = f.link ?? { state: 'unknown' };
  switch (link.state) {
    case 'unknown':
      return { label, state: 'active', detail: 'Checking' };
    case 'working':
      return { label, state: 'active', detail: `Connecting ${ins.repo}` };
    case 'failed':
      return { label, state: 'bad', detail: link.error, action: 'connect' };
    case 'linked':
      return { label, state: 'done', detail: link.repo === ins.repo ? `Pushes to ${link.branch} deploy` : `Deploys from ${link.repo}` };
    case 'none':
      return ins.config?.valid
        ? { label, state: 'active', detail: `Deploy ${ins.repo} on every push to ${ins.branch}`, action: 'connect' }
        : { label, state: 'waiting', detail: `Push sp00ky.yml to ${ins.branch} first` };
  }
}

function deployStep(f: Facts): Omit<Step, 'state'> {
  const ins = f.inspection;
  const dir = ins?.config?.path.includes('/') ? ins.config.path.slice(0, ins.config.path.lastIndexOf('/')) : '';
  const blocks: string[][] = [];
  const lines: string[] = [];
  if (ins === null) {
    blocks.push(['# Name it my-app and choose "Full project"', 'spky init']);
    lines.push('cd my-app');
  }
  if (dir) lines.push(`cd ${shellQuote(dir)}`);
  lines.push(...cloudLines(f), '# Creates the project, opens checkout, then builds and ships it');
  if (f.workspaceType && f.workspaceType !== 'personal') lines.push('# It lands in your personal workspace: the CLI has no other yet');
  lines.push(`${spky(ins)} deploy`);
  blocks.push(lines);
  return {
    id: 'deploy',
    title: 'Deploy',
    blocks,
    result: f.project ? `${f.project.slug} is live` : undefined,
    watching: f.project ? `Following ${f.project.slug}` : 'Waiting for spky deploy',
    checks: deployChecks(f),
  };
}

/**
 * The steps, in order. A later step having happened means the earlier ones did,
 * however they got there: someone already signed in never re-runs `spky login`,
 * and a project that is live is live.
 */
export function plan(f: Facts): Step[] {
  const raw = [loginStep(f), ...(f.inspection ? [configStep(f.inspection)] : []), deployStep(f)];
  const done = (s: Omit<Step, 'state'>): boolean => {
    switch (s.id) {
      case 'login':
        return !!f.cliLoginAt || !!f.project;
      case 'config':
        return !!f.inspection?.config?.valid || !!f.project;
      case 'deploy':
        return isLive(f.project);
    }
  };
  const firstOpen = raw.findIndex((s) => !done(s));
  return raw.map((s, i) => ({ ...s, state: done(s) ? 'done' : i === firstOpen ? 'active' : 'waiting' }));
}

export interface Finding {
  tone: 'ok' | 'todo' | 'info';
  title: string;
  detail?: string;
  href?: string;
}

const GUIDES: Record<string, string> = {
  Solid: 'solid',
  SolidStart: 'solid',
  Flutter: 'flutter',
};

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n} ${n === 1 ? one : many}`;
}

/** What reading a repository turned up, most useful first. */
export function findings(ins: Inspection): Finding[] {
  const out: Finding[] = [];
  const cfg = ins.config;
  if (cfg?.valid) {
    const bits = [cfg.path, cfg.slug && `slug ${cfg.slug}`, cfg.mode && `${cfg.mode} mode`, cfg.apps.length > 0 && plural(cfg.apps.length, 'app')];
    out.push({ tone: 'ok', title: 'Set up for Sp00ky', detail: bits.filter(Boolean).join(', ') });
  } else if (cfg) {
    out.push({ tone: 'todo', title: 'sp00ky.yml does not parse', detail: cfg.path });
  } else if (ins.empty) {
    out.push({ tone: 'info', title: 'Nothing here yet', detail: 'The commands start the project from scratch' });
  } else {
    out.push({ tone: 'todo', title: 'No sp00ky.yml yet', detail: 'The commands add one next to your code' });
  }
  for (const app of ins.apps) {
    const kind = app.kind === 'backend' ? 'backend' : 'app';
    out.push({ tone: 'ok', title: `${app.framework} ${kind}`, detail: app.path === '.' ? app.name : app.path });
  }
  if (ins.schema_files > 0) {
    out.push({ tone: 'ok', title: plural(ins.schema_files, 'SurrealQL file'), detail: ins.schema_dir });
  }
  const client = ins.apps.find((a) => a.kind !== 'backend');
  if (ins.uses_spooky) {
    out.push({ tone: 'ok', title: 'Sp00ky client installed' });
  } else if (client) {
    out.push({
      tone: 'info',
      title: `Add the client to ${client.path === '.' ? 'your app' : client.path}`,
      detail: 'Once the backend is up',
      href: `https://sp00ky.dev/docs/guide/${GUIDES[client.framework] ?? 'vanilla'}`,
    });
  }
  if (ins.package_manager) {
    out.push({ tone: 'info', title: ins.monorepo ? `${ins.package_manager} workspace` : ins.package_manager });
  }
  if (ins.dockerfiles > 0) {
    out.push({ tone: 'info', title: plural(ins.dockerfiles, 'Dockerfile') });
  }
  if (ins.truncated) {
    out.push({ tone: 'info', title: 'Very large repository', detail: 'GitHub listed only part of it' });
  }
  return out;
}
