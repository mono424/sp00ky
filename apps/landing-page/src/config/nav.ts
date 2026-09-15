export const basePath = process.env.ASTRO_BASE_PATH || '';

/**
 * Sidebar structure, ordered the way a developer learns the product: what it is,
 * how to run it, what to model, what to build, then how to ship and operate it.
 * The flat order also drives the prev/next footer (`NextSteps.astro`).
 */
export const docsNav = [
  {
    title: 'Start here',
    links: [
      { text: 'Introduction', href: `${basePath}/docs` },
      { text: 'Quickstart', href: `${basePath}/docs/quickstart` },
      { text: 'How it works', href: `${basePath}/docs/how-it-works` },
      { text: 'Installation', href: `${basePath}/docs/install` },
    ],
  },
  {
    title: 'Schema',
    links: [
      { text: 'Define your schema', href: `${basePath}/docs/schema` },
      { text: 'Relationships', href: `${basePath}/docs/schema/relationships` },
      { text: 'Generated types', href: `${basePath}/docs/schema/generated-types` },
      { text: 'Migrations', href: `${basePath}/docs/schema/migrations` },
    ],
  },
  {
    title: 'Client SDK',
    links: [
      { text: 'Reactive queries', href: `${basePath}/docs/client/queries` },
      { text: 'Pagination', href: `${basePath}/docs/client/pagination` },
      { text: 'Mutations', href: `${basePath}/docs/client/mutations` },
      { text: 'Authentication', href: `${basePath}/docs/client/auth` },
      { text: 'Offline & sync health', href: `${basePath}/docs/client/offline-sync` },
      { text: 'Query allowlist', href: `${basePath}/docs/guide/query-allowlist` },
      { text: 'Feature flags', href: `${basePath}/docs/client/feature-flags` },
      { text: 'File buckets', href: `${basePath}/docs/client/buckets`, experimental: true },
      { text: 'CRDT fields', href: `${basePath}/docs/client/crdt`, experimental: true },
    ],
  },
  {
    title: 'Framework Guides',
    links: [
      { text: 'SolidJS', href: `${basePath}/docs/guide/solid` },
      { text: 'Solid 2 (RC)', href: `${basePath}/docs/guide/solid2` },
      { text: 'Flutter', href: `${basePath}/docs/guide/flutter` },
      { text: 'Vanilla JS / TS', href: `${basePath}/docs/guide/vanilla` },
    ],
  },
  {
    title: 'Backends',
    links: [
      { text: 'What is a backend', href: `${basePath}/docs/backend` },
      { text: 'Add a backend', href: `${basePath}/docs/backend/add` },
      { text: 'Backend options', href: `${basePath}/docs/backend/options` },
    ],
  },
  {
    title: 'Background Jobs',
    links: [
      { text: 'Jobs', href: `${basePath}/docs/jobs` },
      { text: 'Schedules', href: `${basePath}/docs/jobs/schedules` },
      { text: 'Workflows', href: `${basePath}/docs/jobs/workflows` },
    ],
  },
  {
    title: 'Local Development',
    links: [
      { text: 'spky dev', href: `${basePath}/docs/dev` },
      { text: 'Dev servers & sidecars', href: `${basePath}/docs/dev/servers` },
      { text: 'Environment variables', href: `${basePath}/docs/dev/env` },
      { text: 'Doctor & troubleshooting', href: `${basePath}/docs/dev/doctor` },
    ],
  },
  {
    title: 'Deploy',
    links: [
      { text: 'Getting started', href: `${basePath}/docs/cloud/getting-started` },
      { text: 'Deploying', href: `${basePath}/docs/cloud/deploying` },
      { text: 'Environment & vault', href: `${basePath}/docs/cloud/env-variables` },
      { text: 'Logs & monitoring', href: `${basePath}/docs/cloud/logs` },
      { text: 'Backups', href: `${basePath}/docs/cloud/backups` },
      { text: 'CI/CD', href: `${basePath}/docs/cloud/ci-cd` },
      { text: 'Teams', href: `${basePath}/docs/cloud/team` },
      { text: 'Self-hosting', href: `${basePath}/docs/self-hosting` },
    ],
  },
  {
    title: 'Operations',
    links: [
      { text: 'Admin dashboard', href: `${basePath}/docs/reference/admin-dashboard` },
      { text: 'MCP server', href: `${basePath}/docs/reference/mcp` },
    ],
  },
  {
    title: 'Reference',
    links: [
      { text: 'CLI', href: `${basePath}/docs/reference/cli` },
      { text: 'sp00ky.yml', href: `${basePath}/docs/reference/config` },
      { text: 'Client config', href: `${basePath}/docs/reference/client-config` },
      { text: 'AI coding agents', href: `${basePath}/docs/reference/ai-agents` },
      { text: 'Architecture', href: `${basePath}/docs/reference/architecture` },
      { text: 'Performance', href: `${basePath}/docs/reference/performance` },
      { text: 'Vault architecture', href: `${basePath}/docs/reference/vault` },
      { text: 'SSP API', href: `${basePath}/docs/reference/ssp-api` },
      { text: 'Scheduler API', href: `${basePath}/docs/reference/scheduler-api` },
    ],
  },
];

const stripSlash = (path: string) => path.replace(/\/+$/, '') || '/';

/**
 * Resolve which sidebar entry a URL belongs to.
 *
 * Several section landing pages are prefixes of their own children
 * (`/docs/schema` vs `/docs/schema/relationships`), so a plain `startsWith`
 * would light up two entries at once. Take the longest matching href instead:
 * exact match always wins, and a child never activates its parent.
 */
export function resolveActive(pathname: string) {
  const current = stripSlash(pathname);

  let link: (typeof docsNav)[number]['links'][number] | undefined;
  let group: (typeof docsNav)[number] | undefined;
  let bestLength = -1;

  for (const candidateGroup of docsNav) {
    for (const candidate of candidateGroup.links) {
      const href = stripSlash(candidate.href);
      if (current !== href && !current.startsWith(`${href}/`)) continue;
      if (href.length <= bestLength) continue;
      bestLength = href.length;
      link = candidate;
      group = candidateGroup;
    }
  }

  return { group, link };
}
