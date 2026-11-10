/**
 * What the Database tab shows about a SurrealDB upgrade, as a function of what
 * the control plane answers. Nothing here runs anything; the tab feeds it each
 * poll and renders the answer, so a button can never disagree with the state it
 * acts on. The states, and what each one allows, are the runner's in the control
 * plane (internal/dbupgrade).
 */

import type { ConfirmSpec } from '../components/Actions';

export interface UpgradeStats {
  /** The one failure that leaves the project without a database. */
  not_serving?: boolean;
  frozen_ms?: number;
  open_ms?: number;
  hold_ms?: number;
  lag_ms?: number;
  applied_entries?: number;
  applied_rows?: number;
  late_entries?: number;
  verified_tables?: number;
  verified_rows?: number;
  rebuilt_indexes?: number;
  detail?: string;
}

export interface UpgradeRun {
  id: string;
  kind: 'swap' | 'replay' | string;
  state: string;
  active: boolean;
  from_version: string;
  to_version: string;
  /** While an abort replays the new version back onto the old one. */
  reverse: boolean;
  hold_at_ready: boolean;
  abort_requested: boolean;
  backup_id?: string;
  soak_until?: string;
  rollback_until?: string;
  /** When the old database was stopped. */
  flip_at?: string;
  stats?: UpgradeStats | null;
  error?: string;
  warning?: string;
  created_at: string;
  finished_at?: string;
}

export interface UpgradeStatus {
  current: string;
  target?: string;
  kind?: 'swap' | 'replay' | string;
  beta: boolean;
  one_way: boolean;
  up_to_date: boolean;
  blockers: { code: string; message: string }[];
  /** The newest run, active or not. */
  upgrade?: UpgradeRun;
}

/* ---------- the version line ---------- */

/**
 * Current and target. A running upgrade says which way it is going; `current` is
 * no help there, it changes at the cutover and the offer behind it is moot.
 */
export function versionLine(s: UpgradeStatus): { from: string; to?: string; beta: boolean } {
  const r = s.upgrade;
  if (r?.active) {
    if (r.reverse) return { from: r.to_version, to: r.from_version, beta: false };
    return { from: r.from_version, to: r.to_version, beta: r.to_version.includes('-') };
  }
  return { from: s.current, to: s.target, beta: s.beta };
}

/** With no database of its own a project's `current` is only the default image. */
export const hasOwnDatabase = (s: UpgradeStatus) => !s.blockers?.some((b) => b.code === 'no_database');

/* ---------- the steps ---------- */

export interface Phase {
  title: string;
  steps: string[];
  /** A step of this phase, or `done` or `waiting` when the run is past or before it. */
  at: string;
}

const SWAP: Omit<Phase, 'at'>[] = [
  { title: 'Prepare', steps: ['preflight', 'backup', 'rehearse', 'ready'] },
  { title: 'Cut over', steps: ['flip_stop', 'flip_start', 'reconnect', 'smoke'] },
];
const REPLAY: Omit<Phase, 'at'>[] = [
  { title: 'Prepare', steps: ['preflight', 'backup', 'capture', 'clone', 'convert', 'rebuild'] },
  { title: 'Catch up', steps: ['sync', 'verify', 'ready'] },
  { title: 'Cut over', steps: ['fence', 'flip_stop', 'flip_start', 'reconnect', 'smoke'] },
  { title: 'Watch', steps: ['soak', 'finalize'] },
];
/** An abort from the watch window: the same states again, the other way. */
const REVERSE: Omit<Phase, 'at'>[] = [
  { title: 'Catch up', steps: ['convert', 'sync', 'verify', 'ready'] },
  { title: 'Cut back', steps: ['fence', 'flip_stop', 'flip_start', 'reconnect', 'smoke'] },
  { title: 'Clean up', steps: ['finalize'] },
];

const WORD: Record<string, string> = {
  preflight: 'Checks',
  backup: 'Backup',
  rehearse: 'Rehearsal',
  capture: 'Capture',
  clone: 'Copy',
  convert: 'Convert',
  rebuild: 'Indexes',
  sync: 'Replay',
  verify: 'Verify',
  ready: 'Ready',
  fence: 'Fence',
  flip_stop: 'Stop',
  flip_start: 'Start',
  reconnect: 'Connect',
  smoke: 'Test',
  soak: 'Window',
  finalize: 'Finish',
  abort: 'Undo',
  revert: 'Restore',
  rollback: 'Roll back',
};

/** A state as one word, under its segment. */
export const stepWord = (step: string) => WORD[step] ?? step.replace(/_/g, ' ');

/** A run's steps as phases, each knowing how far along it is. */
export function phases(run: UpgradeRun): Phase[] {
  if (run.state === 'rollback') return [{ title: 'Rolling back', steps: ['rollback'], at: 'rollback' }];
  if (run.state === 'abort' || run.state === 'revert') {
    return [{ title: 'Undoing', steps: run.flip_at ? ['revert', 'abort'] : ['abort'], at: run.state }];
  }
  const track = run.reverse ? REVERSE : run.kind === 'swap' ? SWAP : REPLAY;
  const order = track.flatMap((p) => p.steps);
  const now = order.indexOf(run.state);
  // A state this console does not know yet still gets said out loud.
  if (now < 0) return [{ title: 'Upgrade', steps: [run.state], at: run.state }];
  return track.map((p) => ({
    ...p,
    at: p.steps.includes(run.state) ? run.state : order.indexOf(p.steps[0]) < now ? 'done' : 'waiting',
  }));
}

/* ---------- what is going on, in words ---------- */

const CUTTING_OVER = new Set(['fence', 'flip_stop', 'flip_start', 'reconnect', 'smoke']);

/** The run as a chip beside its heading. */
export function runChip(run: UpgradeRun): { label: string; tone: string } {
  if (run.state === 'rollback') return { label: 'Rolling back', tone: 'fail' };
  if (run.state === 'abort' || run.state === 'revert') return { label: 'Undoing', tone: 'fail' };
  if (run.reverse) return { label: 'Going back', tone: 'fail' };
  if (run.abort_requested) return { label: run.state === 'soak' ? 'Going back' : 'Cancelling', tone: 'fail' };
  if (run.state === 'ready' && run.hold_at_ready) return { label: 'Waiting for you', tone: 'live' };
  if (run.state === 'soak') return { label: 'Watching', tone: 'live' };
  if (CUTTING_OVER.has(run.state)) return { label: 'Cutting over', tone: 'run' };
  return { label: 'In progress', tone: 'run' };
}

/** What the run is doing right now, in a line. */
export function doing(run: UpgradeRun): string {
  const from = run.from_version;
  const to = run.to_version;
  const target = run.reverse ? from : to;
  if (run.abort_requested && !run.reverse) {
    return run.state === 'soak' ? `Starting to go back to ${from}` : 'Cancelling the upgrade';
  }
  switch (run.state) {
    case 'preflight':
      return 'Checking that the database can move';
    case 'backup':
      return 'Taking a fresh backup';
    case 'rehearse':
      return `Rehearsing ${to} on a copy of the data`;
    case 'capture':
      return 'Starting to record new writes';
    case 'clone':
      return 'Copying the data';
    case 'convert':
      return `Opening the data on ${target}`;
    case 'rebuild':
      return `Rebuilding indexes on ${to}`;
    case 'sync':
      return run.reverse ? `Replaying writes made since the cutover onto ${from}` : `Replaying new writes onto ${to}`;
    case 'verify':
      return 'Comparing every row on both sides';
    case 'ready':
      if (run.reverse) return `${from} has caught up, about to cut back`;
      return run.hold_at_ready ? `${to} is ready, waiting for you to cut over` : `${to} is ready, about to cut over`;
    case 'fence':
      return 'Refusing writes while the last ones cross over';
    case 'flip_stop':
      return `Stopping ${run.reverse ? to : from}`;
    case 'flip_start':
      return `Starting ${target}`;
    case 'reconnect':
      return 'Reconnecting the sync services';
    case 'smoke':
      return `Checking that ${target} answers`;
    case 'soak':
      return `Serving on ${to}, with the old data waiting in case you go back`;
    case 'finalize':
      return 'Cleaning up';
    case 'abort':
      return 'Putting things back as they were';
    case 'revert':
      return `Starting ${from} again`;
    case 'rollback':
      return `Going back to the old data on ${from}`;
    default:
      return stepWord(run.state);
  }
}

/** What the state means for the app and for the buttons; undefined when `doing` says it all. */
export function stateNote(run: UpgradeRun): string | undefined {
  if (run.state === 'rollback') return 'Writes made since the cutover are dropped.';
  if (run.state === 'abort' || run.state === 'revert') return undefined;
  if (run.reverse) {
    return 'Nothing is lost: every write since the cutover goes onto the old version, then a few seconds of refused writes as it takes over.';
  }
  if (run.abort_requested) return 'The database keeps serving meanwhile.';
  if (run.state === 'ready' && run.hold_at_ready) {
    return 'Nothing moves until you cut over. If nobody does within 24 hours, the upgrade is called off and the database stays as it is.';
  }
  if (run.state === 'soak') {
    return 'Abort and go back replays every write since the cutover onto the old version and loses nothing. Roll back returns to the old data at once and drops those writes.';
  }
  if (CUTTING_OVER.has(run.state)) {
    return run.kind === 'swap'
      ? 'The database is restarting on the new version, a few seconds of downtime. This can no longer be cancelled.'
      : 'Writes are refused for a few seconds. This can no longer be cancelled; the watch window comes next.';
  }
  if (FREE_TO_CANCEL.has(run.state)) return 'Your app is not affected yet. The database keeps serving, and cancelling leaves it as it was.';
  return undefined;
}

/** A finished run: its chip, and what it means for the database. */
export function outcome(run: UpgradeRun): { chip: string; tone: string; text: string; caution?: boolean } {
  const from = run.from_version;
  switch (run.state) {
    case 'done':
      return { chip: 'Done', tone: 'live', text: `Moved from ${from} to ${run.to_version}.` };
    case 'failed':
      // A revert or a rollback that kept failing leaves no database serving, and
      // the run says so. Every other failure left the project as it was.
      if (run.stats?.not_serving) {
        return {
          chip: 'Failed',
          tone: 'fail',
          caution: true,
          text: 'The database is not serving. The message above says what happened. Run `spky restart --surreal`, and contact support if it stays down.',
        };
      }
      return { chip: 'Failed', tone: 'fail', text: `The database is unchanged and serving on ${from}.` };
    case 'aborted':
      return run.reverse || run.flip_at
        ? { chip: 'Aborted', tone: '', text: `The database is back on ${from} and serving. Nothing was lost.` }
        : { chip: 'Aborted', tone: '', text: `Cancelled before the cutover. The database is unchanged and serving on ${from}.` };
    case 'rolled_back':
      return {
        chip: 'Rolled back',
        tone: 'fail',
        text: `The database is back on ${from}, with the data it had at the cutover. Writes made after that were dropped.`,
      };
    default:
      return { chip: stepWord(run.state), tone: '', text: '' };
  }
}

/* ---------- the numbers ---------- */

function span(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms < 3_600_000) return `${(ms / 60_000).toFixed(1)} min`;
  return `${(ms / 3_600_000).toFixed(1)} h`;
}

const count = (n: number) => n.toLocaleString('en-US');

export interface Fact {
  label: string;
  value: string;
  note?: string;
}

/** The numbers worth showing about a run, as far as they are known. */
export function facts(run: UpgradeRun): Fact[] {
  const s = run.stats ?? {};
  const out: Fact[] = [];
  if (s.applied_rows) out.push({ label: 'Rows applied', value: count(s.applied_rows) });
  // Zero is left out of the answer, so past the first catch-up no lag means none.
  if (run.kind === 'replay' && ['sync', 'verify', 'ready'].includes(run.state)) {
    out.push({ label: 'Lag', value: s.lag_ms ? span(s.lag_ms) : run.state === 'sync' ? 'Measuring' : 'None' });
  }
  if (s.verified_rows || s.detail) {
    const tables = s.verified_tables ? `in ${count(s.verified_tables)} tables` : undefined;
    out.push({ label: 'Rows verified', value: count(s.verified_rows ?? 0), note: s.detail ?? tables });
  }
  if (s.rebuilt_indexes) out.push({ label: 'Indexes rebuilt', value: count(s.rebuilt_indexes) });
  if (s.open_ms) out.push({ label: run.kind === 'swap' ? 'Rehearsed open' : 'Converted in', value: span(s.open_ms) });
  if (s.frozen_ms) out.push({ label: 'Frozen for the copy', value: span(s.frozen_ms) });
  if (s.hold_ms) out.push({ label: 'Cutover', value: `Writes refused for ${span(s.hold_ms)}` });
  return out;
}

/* ---------- what can be done ---------- */

/** Where the runner honours an abort by undoing everything; from the fence on it is no longer free. */
const FREE_TO_CANCEL = new Set(['preflight', 'backup', 'rehearse', 'capture', 'clone', 'convert', 'rebuild', 'sync', 'verify', 'ready']);

export interface UpgradeActions {
  cancel: boolean;
  goBack: boolean;
  cutOver: boolean;
  finish: boolean;
  rollBack: boolean;
}

export function actionsFor(run: UpgradeRun, now: number): UpgradeActions {
  // Once an abort is under way only it can move things on.
  const steady = !run.reverse && !run.abort_requested;
  const soak = steady && run.state === 'soak';
  return {
    cancel: steady && FREE_TO_CANCEL.has(run.state),
    goBack: soak,
    cutOver: steady && run.state === 'ready' && run.hold_at_ready,
    finish: soak,
    rollBack: soak || (run.state === 'done' && !!run.rollback_until && Date.parse(run.rollback_until) > now),
  };
}

/* ---------- what each one asks first ---------- */

const HOLD_LIMIT = 'If nobody cuts over within 24 hours, the upgrade is called off.';

export function startConfirm(s: UpgradeStatus, stopBeforeCutover: boolean, slug: string): ConfirmSpec {
  const to = s.target ?? '';
  const when = stopBeforeCutover ? `Stops before the cutover and waits for you. ${HOLD_LIMIT}` : 'Cuts over by itself as soon as the new version is ready.';
  if (s.one_way) {
    return {
      title: `Upgrade to ${to}`,
      warning: [
        s.beta ? 'This moves to a beta release of SurrealDB.' : 'This moves to a new major version of SurrealDB.',
        'The database stays up while the new version catches up, with a few seconds of refused writes at the cutover.',
        'For one hour after the cutover, Abort and go back returns to the old version without losing anything.',
        'After that the old data is kept for 7 days, but going back drops newer writes.',
        'Apps built before JS SDK 2.1.0 or Dart canary.363 must be updated first.',
      ].join('\n'),
      consequences: [`Moves ${s.current} to ${to}, starting with a fresh backup.`, when],
      verb: 'Upgrade',
      typeToConfirm: slug,
    };
  }
  return {
    title: `Upgrade to ${to}`,
    warning: [
      'A fresh backup first, a rehearsal on a copy, then a few seconds of downtime while the database restarts on the new version.',
      'Going back later drops writes made since.',
    ].join('\n'),
    consequences: [`Moves ${s.current} to ${to}.`, when],
    verb: 'Upgrade',
  };
}

export function cancelConfirm(run: UpgradeRun): ConfirmSpec {
  return {
    title: 'Cancel the upgrade',
    consequences: ['Stops the upgrade and removes the copy it made.', `The database stays on ${run.from_version} and keeps serving.`],
    verb: 'Cancel the upgrade',
  };
}

export function goBackConfirm(run: UpgradeRun): ConfirmSpec {
  return {
    title: 'Abort and go back',
    consequences: [
      'Nothing is lost.',
      `Every write since the cutover is replayed onto ${run.from_version}, which then takes over again.`,
      'The database stays up while that happens, with a few seconds of refused writes at the end.',
    ],
    verb: 'Abort and go back',
  };
}

export function cutOverConfirm(run: UpgradeRun): ConfirmSpec {
  return {
    title: `Cut over to ${run.to_version}`,
    consequences:
      run.kind === 'swap'
        ? ['The database restarts on the new version, a few seconds of downtime.', 'Going back later drops writes made since.']
        : ['Writes are refused for a few seconds while the last ones cross over.', 'For one hour after, Abort and go back returns to the old version without losing anything.'],
    verb: 'Cut over',
  };
}

export function finishConfirm(): ConfirmSpec {
  return {
    title: 'End the watch window',
    consequences: ['Going back without losing anything stops being possible.', 'The old data is kept for 7 days, but going back then drops newer writes.'],
    verb: 'Finish now',
  };
}

/** `since` is how long ago the cutover was, in words. */
export function rollbackConfirm(run: UpgradeRun, slug: string, since: string | undefined): ConfirmSpec {
  const lost = since ? `Every write since the cutover is dropped. The cutover was ${since}.` : 'Every write since the cutover is dropped.';
  return {
    title: `Roll back to ${run.from_version}`,
    warning: lost,
    consequences:
      run.state === 'soak'
        ? [`Restarts the database on ${run.from_version} with the data it had at the cutover.`, 'Abort and go back keeps those writes. Use this only if the new version is too broken to read from.']
        : [`Restarts the database on ${run.from_version} with the data it had at the cutover.`],
    verb: 'Roll back',
    typeToConfirm: slug,
  };
}
