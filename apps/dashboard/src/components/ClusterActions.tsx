import { A } from '@solidjs/router';
import { currentMode, waitForScheduler } from '../api/client';
import {
  ActionMenu,
  runAction,
  post,
  type ActionEntry,
} from './Actions';
import type {
  CloudRestartRequest,
  OperationResponse,
  SchedulerRestartMode,
  SspEntity,
  SspRestartMode,
} from '../api/types';

/**
 * The restart menus for SSPs and the scheduler.
 *
 * Both the overview and the SSP page show these, so the entries live here and
 * the pages only decide where to put them. The wording is the product: each
 * entry says what will happen at the process level, because "restart" means
 * three different things here and the operator is choosing between them.
 *
 * Cloud-only entries (image upgrade, volume wipe, SurrealDB bounce) need the
 * control plane. They stay in the menu when it is not linked, muted, with the
 * reason, so the difference between "not offered" and "not available here" is
 * visible.
 */

const NOT_LINKED = 'Not linked to Sp00ky Cloud';

function cloudLinked(): boolean {
  return currentMode().config?.cloud_linked === true;
}

function supervised(): boolean {
  // Unknown (an older scheduler) is treated as supervised; the warning is for
  // the case we KNOW it will stay down.
  return currentMode().config?.supervised !== false;
}

/** Version at which an SSP understands the `clean` directive. Older ones do a plain restart. */
function sspUnderstandsClean(ssp: SspEntity): boolean {
  const m = /canary\.(\d+)/.exec(ssp.version ?? '');
  if (!m) return true;
  return Number(m[1]) >= 211;
}

export function sspEntries(ssp: SspEntity, onDone: () => void): ActionEntry[] {
  const restart = (mode: SspRestartMode) =>
    post<OperationResponse>(`/ssps/${encodeURIComponent(ssp.id)}/restart`, { mode });

  return [
    {
      id: 'restart',
      title: 'Restart',
      consequence:
        'Exits on its next heartbeat and comes back from its snapshot. Its clients re-bootstrap.',
      onSelect: () =>
        void runAction({
          label: `Restart ${ssp.id}`,
          confirm: {
            title: `Restart ${ssp.id}?`,
            verb: 'Restart',
            consequences: [
              'The SSP exits on its next heartbeat (within a few seconds) and the supervisor relaunches it.',
              'Every client pinned to it loses its live queries until it is ready again.',
              'Its circuit snapshot is kept, so it only catches up the delta.',
            ],
          },
          request: restart('restart'),
          success: () => 'Flagged. It exits on its next heartbeat.',
          after: onDone,
        }),
    },
    {
      id: 'clean',
      title: 'Clean restart',
      destructive: true,
      consequence: sspUnderstandsClean(ssp)
        ? 'Deletes its circuit snapshot first, then restarts. Full cold rebuild from the database.'
        : `This SSP (${ssp.version}) predates clean restarts; it would restart without wiping.`,
      onSelect: () =>
        void runAction({
          label: `Clean restart ${ssp.id}`,
          confirm: {
            title: `Wipe and restart ${ssp.id}?`,
            verb: 'Wipe and restart',
            typeToConfirm: 'clean',
            consequences: [
              'Its circuit snapshot and row arena on disk are deleted before it exits.',
              'It rebuilds the whole circuit from the database: a full bootstrap, not a delta.',
              'Its clients are offline for the entire rebuild.',
              ...(sspUnderstandsClean(ssp)
                ? []
                : [`This SSP runs ${ssp.version}, which ignores the wipe. It will restart normally.`]),
            ],
          },
          request: restart('clean'),
          success: () => 'Flagged. It wipes and exits on its next heartbeat.',
          after: onDone,
        }),
    },
    {
      id: 'reload',
      title: 'Reload schema',
      consequence:
        'Rebuilds the circuit in-process from the database. No exit; picks up new tables and permissions.',
      disabledReason:
        ssp.status !== 'ready' ? `Only a ready SSP can reload (it is ${ssp.status})` : undefined,
      onSelect: () =>
        void runAction({
          label: `Reload ${ssp.id}`,
          request: restart('reload'),
          success: () => 'Reloading. It serves nothing until the rebuild finishes.',
          after: onDone,
        }),
    },
  ];
}

export function SspActions(props: { ssp: SspEntity; onDone: () => void; size?: 'sm' }) {
  const entries = () => sspEntries(props.ssp, props.onDone);
  return (
    <ActionMenu
      menuId={`ssp:${props.ssp.id}`}
      size={props.size}
      primary={entries()[0]}
      entries={[
        ...entries(),
        {
          id: 'logs',
          title: 'Stream logs',
          consequence: 'Open this processor in the log view.',
          onSelect: () => {
            // A navigation, not an API call; the anchor below is the real link
            // for middle-click and the like.
            (document.getElementById(`ssp-logs-${props.ssp.id}`) as HTMLAnchorElement | null)?.click();
          },
        },
      ]}
    />
  );
}

/** Hidden anchor so the "Stream logs" menu entry navigates through the router. */
export function SspLogsLink(props: { ssp: SspEntity }) {
  return (
    <A
      id={`ssp-logs-${props.ssp.id}`}
      href={`/logs?source=ssp:${encodeURIComponent(props.ssp.id)}`}
      style={{ display: 'none' }}
    >
      logs
    </A>
  );
}

/** The all-SSPs menu on the SSPs page head. */
export function AllSspsActions(props: { count: number; onDone: () => void }) {
  const none = props.count === 0 ? 'No SSPs are registered' : undefined;
  const restartAll = (mode: 'restart' | 'clean', rolling: boolean) =>
    post<OperationResponse>('/ssps/restart-all', { mode, rolling });

  const rolling: ActionEntry = {
    id: 'rolling',
    title: 'Rolling restart',
    consequence: 'One SSP at a time, waiting for each to be ready before the next. The others keep serving.',
    disabledReason: none,
    onSelect: () =>
      void runAction({
        label: 'Rolling restart',
        confirm: {
          title: `Rolling restart of ${props.count} SSP${props.count === 1 ? '' : 's'}?`,
          verb: 'Start rolling restart',
          consequences: [
            'Each SSP is restarted in turn and the next waits until the previous one is ready.',
            'Clients pinned to the SSP being restarted lose live queries for one bootstrap.',
            'Progress shows in the activity strip; a second rolling restart is refused while this runs.',
          ],
        },
        request: restartAll('restart', true),
        success: () => 'Started. Watch the activity strip.',
        after: props.onDone,
      }),
  };

  const entries: ActionEntry[] = [
    rolling,
    {
      id: 'all',
      title: 'Restart all at once',
      destructive: true,
      consequence: 'Every SSP exits on its next heartbeat. All live queries drop until they rebuild.',
      disabledReason: none,
      onSelect: () =>
        void runAction({
          label: 'Restart all SSPs',
          confirm: {
            title: 'Restart every SSP at the same time?',
            verb: 'Restart all',
            typeToConfirm: 'restart',
            consequences: [
              'All SSPs exit within a few seconds of each other.',
              'No client receives live updates until at least one is ready again.',
              'Prefer the rolling restart unless the whole cluster must go at once.',
            ],
          },
          request: restartAll('restart', false),
          success: () => 'Flagged. They exit on their next heartbeat.',
          after: props.onDone,
        }),
    },
    {
      id: 'upgrade',
      title: 'Upgrade images',
      consequence: 'Sp00ky Cloud pulls the latest SSP image and recreates every SSP container.',
      disabledReason: cloudLinked() ? none : NOT_LINKED,
      onSelect: () =>
        void runAction({
          label: 'Upgrade SSP images',
          confirm: {
            title: 'Upgrade the SSP images?',
            verb: 'Upgrade',
            consequences: [
              'The control plane force-pulls the latest SSP image and recreates the SSP containers.',
              'All SSPs go down together while the containers are replaced; clients re-bootstrap.',
              'The scheduler is not touched by this one.',
            ],
          },
          request: post<OperationResponse>('/cloud/restart', {
            roles: ['ssp'],
            upgrade: true,
            clean: false,
            surreal: false,
          } satisfies CloudRestartRequest),
          success: () => 'Queued with Sp00ky Cloud. The worker acts within seconds.',
          after: props.onDone,
        }),
    },
  ];

  return <ActionMenu primary={rolling} entries={entries} />;
}

/** The scheduler's own menu. */
export function SchedulerActions(props: {
  onDone: () => void;
  size?: 'sm';
  /** Show a single menu button with this label instead of a "Restart" split button. */
  label?: string;
}) {
  const native = (mode: SchedulerRestartMode) =>
    post<OperationResponse>('/scheduler/restart', { mode });
  const cloud = (body: CloudRestartRequest) =>
    post<OperationResponse>('/cloud/restart', body);

  const restart: ActionEntry = {
    id: 'restart',
    title: 'Restart',
    consequence: 'The process exits and the supervisor relaunches it. Replica and WAL are kept.',
    onSelect: () =>
      void runAction({
        label: 'Restart scheduler',
        confirm: {
          title: 'Restart the scheduler?',
          verb: 'Restart',
          typeToConfirm: 'restart',
          warning: supervised()
            ? undefined
            : 'This scheduler is not supervised. It will exit and stay down until something starts it again.',
          consequences: [
            'The scheduler exits cleanly; its replica and event log on disk are kept.',
            'Ingest pauses until it is back. SSP heartbeats fail meanwhile and resume after.',
            'This page reconnects on its own once the scheduler answers again.',
          ],
        },
        request: native('restart'),
        success: () => 'Exiting. Reconnecting when it is back.',
        after: async () => {
          props.onDone();
          await waitForScheduler('The scheduler is restarting.');
        },
      }),
  };

  const entries: ActionEntry[] = [
    restart,
    {
      id: 'reclone',
      title: 'Reclone replica',
      consequence: 'Refetch every table from upstream SurrealDB into the replica, then resync all SSPs.',
      onSelect: () =>
        void runAction({
          label: 'Reclone replica',
          confirm: {
            title: 'Reclone the replica from upstream?',
            verb: 'Reclone',
            consequences: [
              'Every table is fetched again from the primary database. Minutes on a large one.',
              'The replica is reset in place under a write lock while the new data loads.',
              'Every SSP is then flagged to re-bootstrap against the fresh hashes.',
            ],
          },
          request: native('reclone'),
          success: () => 'Started. Watch the activity strip.',
          after: props.onDone,
        }),
    },
    {
      id: 'rehash',
      title: 'Rehash snapshot',
      consequence: 'Recompute the per-table hashes from replica content. Cheap; repairs hash drift.',
      onSelect: () =>
        void runAction({
          label: 'Rehash snapshot',
          request: native('rehash'),
          success: () => 'Rehashed. SSPs re-verify on their next heartbeat.',
          after: props.onDone,
        }),
    },
    {
      id: 'upgrade',
      title: 'Upgrade images',
      consequence: 'Sp00ky Cloud pulls the latest scheduler and SSP images and recreates the containers.',
      disabledReason: cloudLinked() ? undefined : NOT_LINKED,
      onSelect: () =>
        void runAction({
          label: 'Upgrade images',
          confirm: {
            title: 'Upgrade scheduler and SSP images?',
            verb: 'Upgrade',
            typeToConfirm: 'upgrade',
            consequences: [
              'The control plane force-pulls the latest images and recreates the scheduler and every SSP.',
              'The sync layer is down for the whole replacement. Clients re-bootstrap after.',
              'The database is not touched.',
            ],
          },
          request: cloud({ upgrade: true, clean: false, surreal: false }),
          success: () => 'Queued with Sp00ky Cloud. Reconnecting when the new scheduler answers.',
          after: async () => {
            props.onDone();
            await waitForScheduler('Sp00ky Cloud is replacing the scheduler.');
          },
        }),
    },
    {
      id: 'clean',
      title: 'Clean volume and restart',
      destructive: true,
      consequence: 'Wipes the scheduler volume (replica + WAL) and recreates it. Only after RocksDB corruption.',
      disabledReason: cloudLinked() ? undefined : NOT_LINKED,
      onSelect: () =>
        void runAction({
          label: 'Clean volume and restart',
          confirm: {
            title: 'Wipe the scheduler volume?',
            verb: 'Wipe and restart',
            typeToConfirm: 'clean',
            consequences: [
              'The scheduler container is destroyed and its volume deleted: the replica AND the event log.',
              'Events not yet applied to the replica are lost. The database itself is untouched.',
              'A fresh clone from upstream follows, then every SSP re-bootstraps.',
            ],
          },
          request: cloud({ upgrade: false, clean: true, surreal: false }),
          success: () => 'Queued with Sp00ky Cloud. Reconnecting when the new scheduler answers.',
          after: async () => {
            props.onDone();
            await waitForScheduler('Sp00ky Cloud is recreating the scheduler.');
          },
        }),
    },
    {
      id: 'surreal',
      title: 'Restart SurrealDB',
      destructive: true,
      consequence: 'Process restart of the database container. Data kept; the whole deployment pauses.',
      disabledReason: cloudLinked() ? undefined : NOT_LINKED,
      onSelect: () =>
        void runAction({
          label: 'Restart SurrealDB',
          confirm: {
            title: 'Restart the SurrealDB container?',
            verb: 'Restart database',
            typeToConfirm: 'surreal',
            consequences: [
              'The database process restarts; its data volume is reused.',
              'Every part of the deployment is briefly unavailable, and the scheduler and SSPs are recreated with it.',
              'Open HTTP sessions to the database are dropped; the scheduler reconnects on its own.',
            ],
          },
          request: cloud({ upgrade: false, clean: false, surreal: true }),
          success: () => 'Queued with Sp00ky Cloud. Reconnecting when the scheduler answers.',
          after: async () => {
            props.onDone();
            await waitForScheduler('Sp00ky Cloud is restarting the database.');
          },
        }),
    },
  ];

  return (
    <ActionMenu
      size={props.size}
      primary={props.label ? undefined : restart}
      label={props.label}
      entries={entries}
    />
  );
}
