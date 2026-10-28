import { A } from '@solidjs/router';
import { For, Show, createEffect, createMemo, createSignal, onCleanup, type JSX } from 'solid-js';
import { api, currentMode } from '../api/client';
import { opLabel } from './Actions';
import { formatBytes, formatCount } from '../lib/format';
import type {
  Operation,
  RestoreJobState,
  RestoreStage,
  RestoreStatus,
  SspEntity,
} from '../api/types';

/**
 * One long-running action, drawn as a card that reads top to bottom:
 *
 *   spinner + what is happening              elapsed clock
 *   [ the thing it is happening to, with its one number ]
 *   where · who                                   Step n of m
 *   ▬▬▬▬ ▬·── ──── ────
 *   ✓ step that finished                              1.6s
 *   ◌ step running now                                0.2s
 *   ○ steps still to come, dimmed
 *   [ safe to close this tab ]                    Open logs ↗
 *
 * Every step is lit from something the scheduler reported (a stage flag, an
 * SSP's registration, a counter), never from elapsed time. Where the scheduler
 * says nothing between two points the card says nothing either: the step stays
 * active, with a clock, until the next fact arrives.
 *
 * Step durations are the scheduler's own marks when it sends them
 * (`detail.marks`), otherwise the moment this tab saw the step finish. A step
 * already finished when the card first rendered has no duration rather than a
 * wrong one.
 */

export type CardStepState = 'done' | 'active' | 'pending' | 'failed';

export interface CardStep {
  label: string;
  state: CardStepState;
  /** A second line: the table being loaded, the rows cleared. */
  note?: string;
  /** 0..1 inside the step; fills its bar segment. Null = unknown. */
  progress?: number | null;
  /** Epoch ms the step finished, when the scheduler stamped it. */
  doneAt?: number | null;
}

export type CardIcon = 'clock' | 'archive' | 'ssp' | 'server' | 'cloud';

export interface OpView {
  /** Stable key for step timing; the operation id. */
  key: string;
  title: string;
  status: 'running' | 'done' | 'failed';
  startedAt: number;
  finishedAt: number | null;
  subject: {
    icon: CardIcon;
    title: string;
    sub?: string;
    figure?: string;
    figureSub?: string;
  };
  meta: string;
  steps: CardStep[];
  /** Shown under the steps: the failure sentence, or a callout. */
  message?: string | null;
  notice?: JSX.Element;
  link?: { href: string; label: string };
  onClose?: () => void;
}

/* ------------------------------------------------------------------ */
/* Clock and step timing                                                */
/* ------------------------------------------------------------------ */

const [now, setNow] = createSignal(Date.now());
let clockUsers = 0;
let clockTimer: ReturnType<typeof setInterval> | undefined;

/** One shared one-second tick for every card on the page. */
function useClock() {
  if (clockUsers++ === 0) clockTimer = setInterval(() => setNow(Date.now()), 1000);
  onCleanup(() => {
    if (--clockUsers === 0) clearInterval(clockTimer);
  });
  return now;
}

/**
 * When this tab saw each step of each operation finish. Module-level because a
 * card inside a polled list is re-created on every poll; the marks must outlive
 * the component. `null` = was already done when first seen, duration unknown.
 */
const seen = new Map<string, (number | null)[]>();

function stamp(key: string, steps: CardStep[]): (number | null)[] {
  const first = !seen.has(key);
  const marks = seen.get(key) ?? [];
  steps.forEach((s, i) => {
    if (s.doneAt != null) marks[i] = s.doneAt;
    else if (s.state === 'done' && marks[i] === undefined) marks[i] = first ? null : Date.now();
  });
  seen.set(key, marks);
  return marks;
}

/** `0:07`, `2:41`, `1:02:09`: the header clock, as the screenshot reads. */
export function clock(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = String(s % 60).padStart(2, '0');
  return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${ss}` : `${m}:${ss}`;
}

/** Short step timings: `0.2s`, `14s`, `3m 05s`. */
function stepTime(ms: number): string {
  if (ms < 10_000) return `${(Math.max(0, ms) / 1000).toFixed(1)}s`;
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, '0')}s`;
}

/* ------------------------------------------------------------------ */
/* Icons                                                                */
/* ------------------------------------------------------------------ */

function Icon(props: { name: string; class?: string }) {
  const paths: Record<string, JSX.Element> = {
    clock: (
      <>
        <circle cx="8" cy="8" r="5.5" />
        <path d="M8 5v3l2 1.3" />
      </>
    ),
    archive: (
      <>
        <rect x="2.5" y="3" width="11" height="3" rx="0.8" />
        <path d="M3.5 6v6.2c0 .4.3.8.8.8h7.4c.5 0 .8-.4.8-.8V6M6.5 8.5h3" />
      </>
    ),
    ssp: (
      <>
        <path d="M3 5.5 8 3l5 2.5L8 8z" />
        <path d="m3 8 5 2.5L13 8M3 10.5 8 13l5-2.5" />
      </>
    ),
    server: (
      <>
        <rect x="2.5" y="3" width="11" height="4" rx="1" />
        <rect x="2.5" y="9" width="11" height="4" rx="1" />
        <path d="M5 5h.01M5 11h.01" />
      </>
    ),
    cloud: <path d="M5 12.5h6.2a2.8 2.8 0 0 0 .4-5.6A3.8 3.8 0 0 0 4.4 7.6 2.5 2.5 0 0 0 5 12.5z" />,
    check: <path d="m3.5 8.3 2.8 2.7 6.2-6" />,
    alert: <path d="M8 4.5v4.2M8 11.3h.01" />,
    pending: <circle cx="8" cy="8" r="2" />,
    external: <path d="M9.5 3H13v3.5M13 3 7.5 8.5M11.5 9.5V12a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V5.5a1 1 0 0 1 1-1h2.5" />,
    close: <path d="m4.5 4.5 7 7m0-7-7 7" />,
  };
  return (
    <svg
      class={props.class ?? 'opc-icon'}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      stroke-width="1.4"
      stroke-linecap="round"
      stroke-linejoin="round"
      aria-hidden="true"
    >
      {paths[props.name]}
    </svg>
  );
}

function Spinner() {
  return (
    <svg class="opc-spin" viewBox="0 0 16 16" aria-hidden="true">
      <circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-opacity="0.22" stroke-width="1.6" />
      <path d="M8 2.5a5.5 5.5 0 0 1 5.5 5.5" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" />
    </svg>
  );
}

/* ------------------------------------------------------------------ */
/* The card                                                             */
/* ------------------------------------------------------------------ */

export function OpCard(props: { view: OpView }) {
  const tick = useClock();
  const v = () => props.view;
  const marks = createMemo(() => stamp(v().key, v().steps));

  const end = () => v().finishedAt ?? tick();
  const activeIndex = () => {
    const steps = v().steps;
    const i = steps.findIndex((s) => s.state === 'active' || s.state === 'failed');
    if (i >= 0) return i;
    const pending = steps.findIndex((s) => s.state === 'pending');
    return pending >= 0 ? pending : steps.length - 1;
  };

  /** Duration of step i: from the previous step's finish (or the start) to its own. */
  const timing = (i: number): string | null => {
    const m = marks();
    const s = v().steps[i];
    const prev = i === 0 ? v().startedAt : m[i - 1];
    if (prev == null) return null;
    if (s.state === 'done') {
      const at = m[i];
      return at == null ? null : stepTime(at - prev);
    }
    if (s.state === 'active' || s.state === 'failed') {
      return stepTime(end() - prev);
    }
    return null;
  };

  return (
    <section
      class="opc"
      classList={{ done: v().status === 'done', failed: v().status === 'failed' }}
      aria-live="polite"
    >
      <header class="opc-head">
        <span class="opc-state">
          <Show when={v().status === 'running'} fallback={<Icon name={v().status === 'done' ? 'check' : 'alert'} />}>
            <Spinner />
          </Show>
        </span>
        <span class="opc-title">{v().title}</span>
        <span class="opc-clock">{clock(end() - v().startedAt)}</span>
        <Show when={v().onClose}>
          <button class="opc-close" aria-label="Dismiss" onClick={() => v().onClose?.()}>
            <Icon name="close" />
          </button>
        </Show>
      </header>

      <div class="opc-subject">
        <span class="opc-subject-icon">
          <Icon name={v().subject.icon} />
        </span>
        <span class="opc-subject-text">
          <span class="opc-subject-title">{v().subject.title}</span>
          <Show when={v().subject.sub}>
            <span class="opc-subject-sub">{v().subject.sub}</span>
          </Show>
        </span>
        <Show when={v().subject.figure}>
          <span class="opc-figure">
            <span class="opc-figure-value">{v().subject.figure}</span>
            <Show when={v().subject.figureSub}>
              <span class="opc-figure-sub">{v().subject.figureSub}</span>
            </Show>
          </span>
        </Show>
      </div>

      <div class="opc-meta">
        <span class="truncate">{v().meta}</span>
        <span class="opc-count">
          {v().status === 'done'
            ? `${v().steps.length} of ${v().steps.length} done`
            : `Step ${activeIndex() + 1} of ${v().steps.length}`}
        </span>
      </div>

      <div class="opc-segs" role="progressbar" aria-valuemin={0} aria-valuemax={v().steps.length} aria-valuenow={v().steps.filter((s) => s.state === 'done').length}>
        <For each={v().steps}>
          {(s) => (
            <span class="opc-seg" classList={{ [s.state]: true }}>
              <span
                class="opc-seg-fill"
                classList={{ indeterminate: s.state === 'active' && s.progress == null }}
                style={
                  s.state === 'active' && s.progress != null
                    ? { width: `${Math.max(4, s.progress * 100)}%` }
                    : undefined
                }
              />
            </span>
          )}
        </For>
      </div>

      <ol class="opc-steps">
        <For each={v().steps}>
          {(s, i) => (
            <li class="opc-step" classList={{ [s.state]: true }}>
              <span class="opc-step-icon">
                <Show
                  when={s.state === 'active'}
                  fallback={<Icon name={s.state === 'done' ? 'check' : s.state === 'failed' ? 'alert' : 'pending'} />}
                >
                  <Spinner />
                </Show>
              </span>
              <span class="opc-step-body">
                <span class="opc-step-label">{s.label}</span>
                <Show when={s.note && s.state !== 'pending'}>
                  <span class="opc-step-note">{s.note}</span>
                </Show>
              </span>
              <span class="opc-step-time">{timing(i()) ?? ''}</span>
            </li>
          )}
        </For>
      </ol>

      <Show when={v().message}>
        <div class="opc-message" classList={{ bad: v().status === 'failed' }}>
          {v().message}
        </div>
      </Show>
      <Show when={v().notice}>{v().notice}</Show>

      <footer class="opc-foot">
        <span class="opc-chip">
          <Icon name={v().status === 'failed' ? 'alert' : 'check'} />
          {v().status === 'running'
            ? 'Safe to close this tab'
            : v().status === 'done'
              ? 'Finished'
              : 'Stopped'}
        </span>
        <Show when={v().link}>
          {(l) => (
            <A class="opc-link" href={l().href}>
              {l().label}
              <Icon name="external" />
            </A>
          )}
        </Show>
      </footer>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/* Operation -> card                                                    */
/* ------------------------------------------------------------------ */

const str = (v: unknown): string | null => (typeof v === 'string' ? v : null);
const num = (v: unknown): number | null => (typeof v === 'number' ? v : null);

function opStatus(op: Operation): OpView['status'] {
  return op.status === 'running' ? 'running' : op.status === 'failed' ? 'failed' : 'done';
}

/**
 * Light a list of step labels from "how many are finished". The step at
 * `done` is active while the operation runs; on a failed operation it is the
 * one that failed; on a finished one everything is done.
 */
function ladder(labels: string[], done: number, status: OpView['status']): CardStep[] {
  return labels.map((label, i) => ({
    label,
    state:
      status === 'done' || i < done
        ? 'done'
        : i === done
          ? status === 'failed'
            ? 'failed'
            : 'active'
          : 'pending',
  }));
}

function project(): string {
  return currentMode().config?.project_slug ?? 'scheduler';
}

function sspSubject(id: string, ssp: SspEntity | undefined, version: string | null): OpView['subject'] {
  return {
    icon: 'ssp',
    title: id,
    sub: [ssp?.ip, ssp?.version ?? version ? `v${ssp?.version ?? version}` : null].filter(Boolean).join(' · ') || 'sync processor',
    figure: ssp ? formatCount(ssp.views) : undefined,
    figureSub: ssp ? (ssp.views === 1 ? 'live view' : 'live views') : undefined,
  };
}

/** The bootstrap step of an SSP coming back: real table counts when it sends them. */
function bootStep(label: string, ssp: SspEntity | undefined): Pick<CardStep, 'progress' | 'note'> {
  const b = ssp?.bootstrap;
  if (b && b.tables_total > 0) {
    return {
      progress: Math.min(1, b.tables_done / b.tables_total),
      note: `${b.tables_done}/${b.tables_total} tables${b.current_table ? ` · ${b.current_table}` : ''} · ${formatCount(b.rows_loaded)} rows`,
    };
  }
  if (ssp?.status === 'replaying') {
    return { progress: null, note: `replaying ${formatCount(ssp.buffered_events)} buffered events` };
  }
  return { progress: null, note: label };
}

/**
 * Where an SSP being restarted is, from the scheduler's phase (newer) or from
 * the pool itself (any version): gone from the pool = exited; registered after
 * the request with a younger uptime = relaunched; status says the rest.
 */
function sspPhase(op: Operation, ssp: SspEntity | undefined): 0 | 1 | 2 | 3 {
  if (op.status !== 'running') return 3;
  const phase = str(op.detail.phase);
  const relaunched =
    phase === 'bootstrapping' ||
    (ssp != null && ssp.uptime_seconds * 1000 < Date.now() - op.started_at);
  if (relaunched) return ssp?.status === 'ready' ? 3 : 2;
  if (phase === 'exited' || ssp == null) return 1;
  return 0;
}

function marks(op: Operation): Record<string, number> {
  const m = op.detail.marks;
  return m && typeof m === 'object' ? (m as Record<string, number>) : {};
}

export function restoreSteps(
  local: RestoreJobState | null,
  stage: RestoreStage,
  queuedInCloud: boolean,
): CardStep[] {
  const failed = stage === 'failed';
  const done = stage === 'done';
  const dbDone = !!local?.main_db_restored || done;
  const repDone = !!local?.replica_restored || done;
  const started = !!local && local.status !== 'queued';
  const st = (isDone: boolean, isActive: boolean): CardStepState =>
    isDone ? 'done' : isActive ? (failed ? 'failed' : 'active') : 'pending';
  return [
    {
      label: queuedInCloud && !local ? 'Queued with Sp00ky Cloud' : 'Queued on the scheduler',
      state: st(started || done, !started),
    },
    {
      label: 'Gate closed, database dropped and re-imported',
      state: st(dbDone, started && !dbDone),
      note: local?.storage_path ?? undefined,
    },
    {
      label: 'Replica rebuilt from the same export',
      state: st(repDone, dbDone && !repDone),
      note: local?.pending_cleared != null ? `${formatCount(local.pending_cleared)} pending events cleared` : undefined,
    },
    {
      label: 'SSPs evicted, traffic reopened',
      state: st(done, repDone && !done),
      note: local?.ssps_evicted != null ? `${local.ssps_evicted} evicted to re-bootstrap` : undefined,
    },
  ];
}

export function restoreSubject(
  backupId: string,
  local: RestoreJobState | null,
  label?: string,
  sub?: string,
): OpView['subject'] {
  return {
    icon: 'clock',
    title: label ?? backupId,
    sub: sub ?? (label ? backupId : local?.restore_id ? `restore ${local.restore_id}` : 'database restore'),
    figure: local?.snapshot_seq != null ? formatCount(local.snapshot_seq) : undefined,
    figureSub: local?.snapshot_seq != null ? 'snapshot seq' : undefined,
  };
}

/** Build the card for any operation the scheduler reports. */
export function viewOf(op: Operation, ssps: SspEntity[] = []): OpView {
  const status = opStatus(op);
  const d = op.detail ?? {};
  const base = {
    key: op.id,
    status,
    startedAt: op.started_at,
    finishedAt: op.finished_at,
    meta: `${project()} · by ${op.requested_by}`,
    message: status === 'failed' ? op.message : null,
  };
  const ssp = op.target ? ssps.find((s) => s.id === op.target) : undefined;
  const m = marks(op);

  switch (op.kind) {
    case 'ssp_restart':
    case 'ssp_clean': {
      const clean = op.kind === 'ssp_clean';
      const id = op.target ?? 'SSP';
      const phase = sspPhase(op, ssp);
      const steps = ladder(
        [
          clean ? 'Flagged to wipe its snapshot and exit' : 'Flagged to exit on its next heartbeat',
          'Exited and relaunched by the supervisor',
          clean ? 'Full bootstrap from the database' : 'Catching up from its snapshot',
          'Ready, clients re-attached',
        ],
        phase,
        status,
      );
      steps[0].doneAt = m.exited_at ?? null;
      steps[1].doneAt = m.registered_at ?? null;
      if (steps[2].state === 'active') Object.assign(steps[2], bootStep('loading circuit', ssp));
      return {
        ...base,
        title: status === 'running' ? `${clean ? 'Clean resyncing' : 'Resyncing'} ${id}` : status === 'done' ? `${id} resynced` : `Resync of ${id} failed`,
        subject: sspSubject(id, ssp, str(d.ssp_version)),
        steps,
        link: { href: `/logs?source=ssp:${encodeURIComponent(id)}`, label: 'Open logs' },
      };
    }

    case 'ssp_reload': {
      const id = op.target ?? 'SSP';
      return {
        ...base,
        title: status === 'running' ? `Reloading ${id}` : status === 'done' ? `${id} reloaded` : `Reload of ${id} failed`,
        subject: sspSubject(id, ssp, str(d.ssp_version)),
        steps: ladder(['Rebuild circuit in-process from the database', 'Serving again'], 0, status),
        link: { href: `/logs?source=ssp:${encodeURIComponent(id)}`, label: 'Open logs' },
      };
    }

    case 'rolling_restart': {
      const total = num(d.total) ?? ssps.length;
      const done = num(d.done) ?? 0;
      const current = str(d.current);
      const rolling = d.rolling !== false;
      const ids = [...ssps].map((s) => s.id).sort();
      let steps: CardStep[];
      if (rolling && ids.length === total && total > 0) {
        steps = ladder(ids.map((id) => `Restart ${id}`), done, status);
        const active = steps.findIndex((s) => s.state === 'active');
        if (active >= 0) {
          Object.assign(steps[active], bootStep('waiting for it to come back', ssps.find((s) => s.id === ids[active])));
        }
      } else {
        steps = ladder(['Every SSP flagged to exit', `SSPs back and ready (${done}/${total})`], done > 0 || !rolling ? 1 : 0, status);
        if (steps[1].state === 'active') steps[1].progress = total > 0 ? done / total : null;
      }
      return {
        ...base,
        title: status === 'running' ? (rolling ? 'Rolling restart' : 'Restarting all SSPs') : status === 'done' ? 'SSPs restarted' : 'Restart failed',
        subject: {
          icon: 'ssp',
          title: rolling ? 'One at a time' : 'All at once',
          sub: current ? `now: ${current}` : `${d.kind === 'clean' ? 'clean' : 'snapshot-preserving'} restart`,
          figure: `${done}/${total}`,
          figureSub: 'SSPs done',
        },
        steps,
        link: { href: '/ssps', label: 'Open SSPs' },
      };
    }

    case 'scheduler_restart':
      return {
        ...base,
        title: status === 'running' ? 'Restarting scheduler' : status === 'done' ? 'Scheduler restarted' : 'Scheduler restart failed',
        subject: { icon: 'server', title: 'Scheduler', sub: d.supervised === false ? 'not supervised' : 'supervised', figure: undefined },
        steps: ladder(['Exit cleanly, replica and WAL kept', 'Relaunched by the supervisor', 'Replica open, SSPs heartbeating'], 0, status),
        link: { href: '/logs?source=scheduler', label: 'Open logs' },
      };

    case 'reclone':
    case 'rehash': {
      const reclone = op.kind === 'reclone';
      const notReady = ssps.filter((s) => s.status !== 'ready').length;
      const labels = reclone
        ? ['Fetch every table from upstream', 'Reset replica, recompute hashes', 'SSPs flagged to re-verify']
        : ['Recompute per-table hashes from the replica', 'SSPs flagged to re-verify'];
      return {
        ...base,
        title: status === 'running'
          ? reclone ? 'Recloning replica' : 'Rehashing snapshot'
          : status === 'done'
            ? reclone ? 'Replica recloned' : 'Snapshot rehashed'
            : reclone ? 'Reclone failed' : 'Rehash failed',
        subject: {
          icon: 'server',
          title: 'Scheduler replica',
          sub: reclone ? 'from upstream SurrealDB' : 'hash drift repair',
          figure: num(d.tables) != null ? formatCount(num(d.tables)) : ssps.length ? `${ssps.length - notReady}/${ssps.length}` : undefined,
          figureSub: num(d.tables) != null ? 'tables' : ssps.length ? 'SSPs ready' : undefined,
        },
        steps: ladder(labels, 0, status),
        message: status === 'done' ? op.message : base.message,
        link: { href: '/logs?source=scheduler', label: 'Open logs' },
      };
    }

    case 'cloud_restart': {
      const roles = Array.isArray(d.roles) && d.roles.length ? (d.roles as string[]).join(', ') : 'scheduler, ssp';
      const what = d.surreal ? 'SurrealDB' : d.clean ? 'clean volume' : d.upgrade ? 'image upgrade' : 'restart';
      return {
        ...base,
        title: status === 'running' ? `Cloud ${what}` : status === 'done' ? `Cloud ${what} done` : `Cloud ${what} failed`,
        subject: { icon: 'cloud', title: 'Sp00ky Cloud', sub: roles, figure: undefined },
        steps: ladder(['Queued with the control plane', 'Worker replacing containers', 'Back online'], d.cloud ? 1 : 0, status),
        link: { href: '/logs', label: 'Open logs' },
      };
    }

    case 'backup_create': {
      const st = str(d.status);
      const done = st === 'completed' ? 2 : st === 'running' ? 1 : 0;
      const size = num(d.size_bytes);
      const id = str(d.backup_id) ?? op.target ?? 'backup';
      return {
        ...base,
        title: status === 'running' ? 'Backing up database' : status === 'done' ? 'Backup written' : 'Backup failed',
        subject: {
          icon: 'archive',
          title: str(d.name) ?? id,
          sub: str(d.name) ? id : 'SurrealDB export, gzipped',
          figure: size != null ? formatBytes(size) : undefined,
          figureSub: size != null ? 'compressed' : undefined,
        },
        steps: ladder(['Queued on the scheduler', 'Export, compress and upload', 'Recorded in the catalog'], done, status),
        link: { href: '/backups', label: 'Open backups' },
      };
    }

    case 'backup_restore': {
      const local = (d.local as RestoreJobState | undefined) ?? null;
      const stage = (str(d.stage) as RestoreStage | null) ?? 'queued';
      const steps = restoreSteps(local, status === 'done' ? 'done' : status === 'failed' ? 'failed' : stage, false);
      return {
        ...base,
        title: status === 'running' ? 'Restoring database' : status === 'done' ? 'Database restored' : 'Restore failed',
        subject: restoreSubject(str(d.backup_id) ?? op.target ?? 'backup', local),
        steps,
        message: status === 'done' ? 'Run your migrations again (spky migrate).' : base.message,
        link: { href: '/backups', label: 'Open backups' },
      };
    }

    default:
      // A kind this build does not know yet: still a card, one honest step.
      return {
        ...base,
        title: opLabel(op.kind),
        subject: { icon: 'server', title: op.target ?? 'Scheduler', sub: op.kind },
        steps: ladder([opLabel(op.kind)], 0, status),
      };
  }
}

/* ------------------------------------------------------------------ */
/* The stack of cards                                                   */
/* ------------------------------------------------------------------ */

/**
 * Operations that just finished, kept on screen for a moment. The overview
 * poll only carries running operations, so without this a card would vanish
 * mid-step the instant its work ended and the operator would never see the
 * check mark. On disappearance the final record is fetched once from
 * `/operations`.
 */
const LINGER_MS = 12_000;
const [lingering, setLingering] = createSignal<Operation[]>([]);
const known = new Set<string>();
let fetching = false;

async function settle(ids: string[]) {
  if (fetching) return;
  fetching = true;
  try {
    const res = await api.get<{ operations: Operation[] }>('/operations');
    const finals = res.operations.filter((o) => ids.includes(o.id) && o.status !== 'running');
    if (finals.length === 0) return;
    setLingering((cur) => [...cur.filter((o) => !ids.includes(o.id)), ...finals]);
    setTimeout(() => {
      setLingering((cur) => cur.filter((o) => !finals.some((f) => f.id === o.id)));
      finals.forEach((f) => seen.delete(f.id));
    }, LINGER_MS);
  } catch {
    // An older scheduler without `/operations`: the card simply goes.
  } finally {
    fetching = false;
  }
}

export function ActivityStrip(props: {
  operations: Operation[] | undefined;
  ssps?: SspEntity[];
  /** Hide operations another panel on the page already draws. */
  exclude?: (op: Operation) => boolean;
}) {
  createEffect(() => {
    const ops = props.operations;
    if (!ops) return;
    const running = new Set(ops.filter((o) => o.status === 'running').map((o) => o.id));
    const gone = [...known].filter((id) => !running.has(id));
    running.forEach((id) => known.add(id));
    if (gone.length) {
      gone.forEach((id) => known.delete(id));
      void settle(gone);
    }
  });

  const shown = () => {
    const running = (props.operations ?? []).filter((o) => o.status === 'running');
    const ids = new Set(running.map((o) => o.id));
    return [...running, ...lingering().filter((o) => !ids.has(o.id))].filter(
      (o) => !props.exclude?.(o),
    );
  };

  const dismiss = (id: string) => setLingering((cur) => cur.filter((o) => o.id !== id));

  return (
    <Show when={shown().length > 0}>
      <div class="opc-grid">
        <For each={shown()}>
          {(op) => (
            <OpCard
              view={{
                ...viewOf(op, props.ssps ?? []),
                onClose: op.status === 'running' ? undefined : () => dismiss(op.id),
              }}
            />
          )}
        </For>
      </div>
    </Show>
  );
}

/** Restore status from `/backups/:id/restore`, as a card. */
export function restoreView(
  backupId: string,
  st: RestoreStatus | undefined,
  label: string | undefined,
  startedFallback: number,
  sub?: string,
): Omit<OpView, 'onClose' | 'notice'> {
  const local = st?.local ?? null;
  const stage = st?.stage ?? 'queued';
  const status: OpView['status'] = stage === 'done' ? 'done' : stage === 'failed' ? 'failed' : 'running';
  const t = (iso: string | null | undefined) => (iso ? Date.parse(iso) : NaN);
  const started = t(local?.started_at ?? local?.enqueued_at ?? st?.cloud?.created_at);
  const finished = t(local?.finished_at ?? st?.cloud?.completed_at);
  return {
    key: `restore:${local?.restore_id ?? backupId}`,
    title: status === 'running' ? 'Restoring database' : status === 'done' ? 'Database restored' : 'Restore failed',
    status,
    startedAt: Number.isNaN(started) ? startedFallback : started,
    finishedAt: status === 'running' || Number.isNaN(finished) ? null : finished,
    subject: restoreSubject(backupId, local, label, sub),
    meta: `${project()} · restore ${local?.restore_id ?? st?.cloud?.id ?? backupId}`,
    steps: restoreSteps(local, stage, !!st?.cloud),
    message:
      status === 'failed'
        ? local?.error ?? st?.cloud?.error ?? 'Restore failed'
        : status === 'done'
          ? 'Run your migrations again (spky migrate), then check the SSPs re-bootstrapped.'
          : null,
    link: { href: '/logs?source=scheduler', label: 'Open logs' },
  };
}
