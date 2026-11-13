// The site nav's dropdowns: short titles and one-line descriptions, so every
// panel reads as a tidy two-column grid. The feature pages keep their longer copy
// in `features.ts`.
import { basePath } from './nav';

export interface NavMenuItem {
  title: string;
  desc: string;
  href: string;
  icon: string;
  external?: boolean;
  /** Opens in a new tab. */
  newTab?: boolean;
}

export interface NavMenu {
  id: string;
  label: string;
  items: NavMenuItem[];
}

export const CONSOLE_URL = 'https://console.sp00ky.cloud';
export const GITHUB_URL = 'https://github.com/mono424/sp00ky';

export const navMenus: NavMenu[] = [
  {
    id: 'features',
    label: 'Features',
    items: [
      { title: 'Optimistic updates', desc: 'Writes land instantly, sync follows.', href: '/features/optimistic-updates', icon: 'zap' },
      { title: 'Reactive queries', desc: 'Live SurrealQL that updates itself.', href: '/features/reactive-queries', icon: 'activity' },
      { title: 'Job scheduler', desc: 'Durable jobs with retries and queues.', href: '/features/job-scheduler', icon: 'clock' },
      { title: 'File buckets', desc: 'Typed file storage in your schema.', href: '/features/file-sync', icon: 'folder' },
      { title: 'Type safety', desc: 'Types from the database to the UI.', href: '/features/type-safety', icon: 'braces' },
      { title: 'Tooling', desc: 'DevTools, an MCP server and a CLI.', href: '/features/devtools', icon: 'wrench' },
    ],
  },
  {
    id: 'cloud',
    label: 'Cloud',
    items: [
      { title: 'Overview', desc: 'Everything sp00ky Cloud runs for you.', href: '/cloud', icon: 'cloud' },
      { title: 'Managed hosting', desc: 'A production stack in one command.', href: '/cloud/managed-hosting', icon: 'server' },
      { title: 'Git deploys', desc: 'Push to deploy, roll back in a click.', href: `${basePath}/docs/cloud/deploying`, icon: 'rocket' },
      { title: 'Automatic backups', desc: 'Snapshots and one-click restore.', href: '/cloud/automatic-backups', icon: 'backup' },
      { title: 'Shared vault', desc: 'Encrypted stores, shared safely.', href: '/cloud/shared-vault', icon: 'lock' },
      { title: 'Console', desc: 'Sign in to your projects.', href: CONSOLE_URL, icon: 'console', external: true },
    ],
  },
  {
    id: 'resources',
    label: 'Resources',
    items: [
      { title: 'Documentation', desc: 'Guides and API reference.', href: `${basePath}/docs`, icon: 'book' },
      { title: 'Quickstart', desc: 'A synced app in five minutes.', href: `${basePath}/docs/quickstart`, icon: 'play' },
      { title: 'AI coding agents', desc: 'AGENTS.md, MCP and recipes.', href: `${basePath}/docs/reference/ai-agents`, icon: 'bot' },
      { title: 'Customer story', desc: 'How WhitePawn runs live chess.', href: '/customers/whitepawn', icon: 'crown' },
      { title: 'Team', desc: 'The people behind sp00ky.', href: '/team', icon: 'users' },
      { title: 'GitHub', desc: 'Source, issues and releases.', href: GITHUB_URL, icon: 'github', external: true, newTab: true },
    ],
  },
];
