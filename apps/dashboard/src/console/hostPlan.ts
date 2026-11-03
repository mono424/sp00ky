/**
 * What the Machines tab shows about a project's own VMs, as a function of what
 * the control plane answers at `/v1/projects/{id}/host`. Nothing here runs
 * anything; the tab feeds it each poll and renders the answer. The host rows, and
 * who may ask for an upgrade, are the control plane's (internal/projecthost).
 */

import type { ConfirmSpec } from '../components/Actions';

export interface HostView {
  /** `app` is the project host, `db` the database VM. */
  kind: 'app' | 'db' | string;
  status: string;
  server_type: string;
  location: string;
  /** The type an upgrade goes to. Only set while one is pending. */
  target_type?: string;
  target_available: boolean;
  upgrade_pending: boolean;
  upgrade_state?: string;
  upgrade_error?: string;
}

export interface HostStatus {
  /** `active` once the project runs on its own VMs, `preparing` while it moves. */
  placement: 'active' | 'preparing' | '' | string;
  hosts: HostView[];
  quiet_window: string;
  note: string;
}

export const kindLabel = (kind: string) => (kind === 'app' ? 'Project host' : kind === 'db' ? 'Database VM' : kind);

/** What the machine carries, in a line under its heading. */
export const kindRole = (kind: string) =>
  kind === 'app'
    ? 'Scheduler, sync processors, backends, frontends and the proxy'
    : kind === 'db'
      ? 'SurrealDB, with its data on a volume'
      : '';

/** The machine's status as a chip. */
export function statusChip(h: HostView): { label: string; tone: string } {
  if (h.status === 'ready') return { label: 'Ready', tone: 'live' };
  if (h.status === 'failed') return { label: 'Failed', tone: 'fail' };
  return { label: humanize(h.status), tone: 'run' };
}

/** `snake_case` from the control plane as words. */
export const humanize = (s: string) => {
  const words = s.replace(/_/g, ' ').trim();
  return words ? words[0].toUpperCase() + words.slice(1) : '';
};

/** An upgrade in the works: asked for, or running. */
export const upgrading = (h: HostView) => !!h.upgrade_state;

/** The Upgrade button: only for an admin, only while one is waiting and none is running. */
export const canUpgrade = (h: HostView, role: string) => h.kind === 'app' && h.upgrade_pending && !upgrading(h) && role === 'admin';

/** The line under a machine that has an upgrade waiting or running. */
export function upgradeNote(h: HostView, quietWindow: string, role: string): string | undefined {
  if (!h.upgrade_pending) return undefined;
  const to = h.target_type ?? 'a larger type';
  if (upgrading(h)) return `Upgrading to ${to}: ${humanize(h.upgrade_state!).toLowerCase()}.`;
  const when = `It upgrades to ${to} by itself in the quiet window, ${quietWindow}.`;
  const available = h.target_available ? '' : ` ${to} is not available in ${h.location} right now.`;
  const who = role === 'admin' ? '' : ' Only a project admin can upgrade it sooner.';
  return when + available + who;
}

/** The dialog before Upgrade: the downtime, and that the database stays as it is. */
export function upgradeConfirm(h: HostView): ConfirmSpec {
  const to = h.target_type ?? 'the larger type';
  return {
    title: `Upgrade the project host to ${to}`,
    warning: 'An upgrade restarts the project host.',
    consequences: [
      `Moves the project host from ${h.server_type} to ${to}.`,
      'Web, API and live sync are down for about two minutes.',
      'The database VM is not touched.',
    ],
    verb: 'Upgrade',
  };
}

/** What a project with no VMs of its own shows instead of machines. */
export function emptyNote(s: HostStatus): string {
  return s.placement === 'preparing'
    ? "This project's own VMs are being prepared. They appear here when they are up."
    : 'This project has no VMs of its own.';
}
