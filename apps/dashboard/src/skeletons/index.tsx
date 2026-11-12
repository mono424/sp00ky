/**
 * Which skeleton a URL paints before its page exists: the cold load (drawn
 * from the entry chunk) and a project's session opening both look the page
 * up here, so the first frame is already the page's shape and title.
 */

import type { JSX } from 'solid-js';
import { BackendDetailSkeleton, BackendsSkeleton } from './Backends';
import { AudienceSkeleton } from './Audience';
import { BackupsSkeleton } from './Backups';
import { BuildRunSkeleton, BuildsSkeleton } from './Builds';
import { DatabaseSkeleton } from './Database';
import { DeploymentsSkeleton } from './Deployments';
import { FlagsSkeleton } from './Flags';
import { IncidentDetailSkeleton, IncidentsSkeleton } from './Incidents';
import { LogsSkeleton } from './Logs';
import { OverviewSkeleton } from './Overview';
import { ResourcesSkeleton } from './Resources';
import { RunsSkeleton, SchedulesSkeleton, WorkflowsSkeleton } from './Runs';
import { SspsSkeleton } from './Ssps';
import { StorageSkeleton } from './Storage';
import { UsersSkeleton } from './Users';
import { ViewDetailSkeleton, ViewsSkeleton } from './Views';
import {
  ActivitySkeleton,
  BillingSkeleton,
  KeysSkeleton,
  MembersSkeleton,
  ProjectsSkeleton,
  SessionsSkeleton,
  VaultSkeleton,
  WorkspaceSettingsSkeleton,
} from './Workspace';

export interface PageSkeletonSpec {
  title: string;
  /** The page's body; absent means the generic one. */
  body?: () => JSX.Element;
}

type Entry = [RegExp, PageSkeletonSpec];

/** A project's pages, by the path under the project. */
const PROJECT: Entry[] = [
  [/^$/, { title: 'Overview', body: OverviewSkeleton }],
  [/^schedules(\/|$)/, { title: 'Schedules', body: SchedulesSkeleton }],
  [/^workflows(\/|$)/, { title: 'Workflows', body: WorkflowsSkeleton }],
  [/^(runs|jobs)(\/|$)/, { title: 'Runs', body: RunsSkeleton }],
  [/^ssps\/?$/, { title: 'SSPs', body: SspsSkeleton }],
  [/^backends\/?$/, { title: 'Backends', body: BackendsSkeleton }],
  [/^backends\/[^/]+/, { title: 'Backends', body: BackendDetailSkeleton }],
  [/^views\/?$/, { title: 'Views', body: ViewsSkeleton }],
  [/^views\/[^/]+/, { title: 'Views', body: ViewDetailSkeleton }],
  [/^incidents\/?$/, { title: 'Incidents', body: IncidentsSkeleton }],
  [/^incidents\/[^/]+/, { title: 'Incidents', body: IncidentDetailSkeleton }],
  [/^resources\/?$/, { title: 'Resources', body: ResourcesSkeleton }],
  [/^backups\/?$/, { title: 'Backups', body: BackupsSkeleton }],
  [/^deployments\/?$/, { title: 'Deployments', body: DeploymentsSkeleton }],
  [/^deployments\/builds\/[^/]+/, { title: 'Build', body: BuildRunSkeleton }],
  [/^deployments\/builds\/?$/, { title: 'Build time', body: BuildsSkeleton }],
  [/^audience\/?$/, { title: 'Audience', body: AudienceSkeleton }],
  [/^logs\/?$/, { title: 'Logs', body: LogsSkeleton }],
  [/^database(\/|$)/, { title: 'Database', body: () => DatabaseSkeleton({}) }],
  [/^users\/?$/, { title: 'Users', body: UsersSkeleton }],
  [/^flags\/?$/, { title: 'Feature flags', body: FlagsSkeleton }],
  [/^storage(\/|$)/, { title: 'Storage', body: () => StorageSkeleton({}) }],
];

/** A workspace's own pages, by the path under `/{workspace}/-`. */
const WORKSPACE: Entry[] = [
  [/^$/, { title: 'Projects', body: ProjectsSkeleton }],
  [/^members\/?$/, { title: 'Members', body: () => MembersSkeleton({ admin: true }) }],
  [/^billing\/?$/, { title: 'Billing & usage', body: BillingSkeleton }],
  [/^activity\/?$/, { title: 'Activity', body: ActivitySkeleton }],
  [/^sessions\/?$/, { title: 'Sessions', body: SessionsSkeleton }],
  [/^vault\/?$/, { title: 'Vault', body: () => VaultSkeleton({}) }],
  [/^keys\/?$/, { title: 'API keys', body: KeysSkeleton }],
  [/^settings\/?$/, { title: 'Settings', body: WorkspaceSettingsSkeleton }],
];

function titleOf(rest: string): string {
  const page = rest.split('/')[0];
  if (!page) return 'Overview';
  return page === 'ssps' ? 'SSPs' : page[0].toUpperCase() + page.slice(1);
}

/** The skeleton for `rest`, the path under a project (or under a workspace's `-`). */
export function pageSkeleton(rest: string, workspace = false): PageSkeletonSpec {
  const path = rest.replace(/^\/+|\/+$/g, '');
  for (const [re, spec] of workspace ? WORKSPACE : PROJECT) if (re.test(path)) return spec;
  return { title: workspace && !path ? 'Projects' : titleOf(path) };
}
