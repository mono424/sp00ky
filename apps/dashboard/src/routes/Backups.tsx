import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { api } from '../api/client';
import { PageHead } from '../components/Chrome';
import { FxCell, Sq } from '../components/Frame';
import { runAction, post, toast } from '../components/Actions';
import { ActivityStrip, OpCard, restoreView } from '../components/OpCard';
import { describeCron } from '../lib/cron';
import {
  EMPTY_POLICY,
  TIERS as RETENTION_TIERS,
  cadence,
  pastRuns,
  periodName,
  retain,
  shortDate,
  type Policy,
  type Tier,
} from '../lib/retention';
import {
  elapsed,
  formatBytes,
  formatCount,
  formatStamp,
  relativeStamp,
} from '../lib/format';
import type {
  BackupConfig,
  BackupsData,
  CatalogEntry,
  KeepTiers,
  OperationResponse,
  PointInTime,
  Overview,
  RestoreStatus,
} from '../api/types';
import { BackupsSkeleton } from '../skeletons/Backups';

/**
 * Backups: when the last one was taken, how many are kept, when the next one
 * runs, and the list to restore from.
 *
 * Two sources are shown as one list. The scheduler EXECUTES backups (export,
 * gzip, upload) and remembers recent ones in memory; the catalog, schedule
 * and retention live in Sp00ky Cloud when linked, or in a bucket listing when
 * not. The API joins them by id, so an in-flight backup fills in its size
 * before the control plane has heard of it.
 *
 * A restore is the one action here no later click can undo, so it gets
 * consequences up front, a typed word, then a step card fed by the
 * scheduler's own stage flags.
 */

export function Backups(props: { overview: Overview | undefined; refresh: () => void }) {
  const [result, { refetch }] = createResource(() => api.getResult<BackupsData>('/backups'));
  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  // A backup in flight changes every few seconds; an idle catalog does not.
  const timer = setInterval(() => {
    const d = data();
    const busy =
      d?.local.current_running ||
      (d?.local.queue_len ?? 0) > 0 ||
      d?.catalog.some((c) => c.status === 'pending' || c.status === 'in_progress') ||
      d?.restores.some((r) => r.status === 'queued' || r.status === 'running' || r.status === 'awaiting_replay');
    void (busy ? refetch() : undefined);
  }, 3000);
  const slow = setInterval(refetch, 30_000);
  onCleanup(() => {
    clearInterval(timer);
    clearInterval(slow);
  });

  const refreshAll = () => {
    void refetch();
    props.refresh();
  };

  /** The restore drawn as a card above the catalog: by restore id. */
  const [watching, setWatching] = createSignal<{ id: string; backupId: string; label?: string; sub?: string } | null>(null);

  const completed = () => (data()?.catalog ?? []).filter((c) => c.status === 'completed');
  const latest = () => completed()[0];
  const totalBytes = () => completed().reduce((n, c) => n + (c.size_bytes ?? 0), 0);

  const [name, setName] = createSignal('');
  const [creating, setCreating] = createSignal(false);

  const create = async () => {
    setCreating(true);
    try {
      await runAction<{ operation: OperationResponse['operation']; backup_id: string }>({
        label: 'Backup',
        request: post('/backups', name().trim() ? { name: name().trim() } : {}),
        success: (r) => `Queued as ${r.backup_id}`,
        after: () => {
          setName('');
          refreshAll();
        },
      });
    } finally {
      setCreating(false);
    }
  };

  const restore = (entry: CatalogEntry) =>
    runAction<{ operation: OperationResponse['operation']; restore_id: string }>({
      label: `Restore ${entry.name ?? entry.id}`,
      confirm: {
        title: `Restore from ${entry.name ?? entry.id}?`,
        verb: 'Restore',
        typeToConfirm: 'restore',
        consequences: [
          'The whole database is dropped and re-imported from this backup. Everything written since is lost.',
          `Data as of ${formatStamp(entry.completed_at ?? entry.created_at)}${
            entry.snapshot_seq != null ? `, snapshot seq ${formatCount(entry.snapshot_seq)}` : ''
          }.`,
          'Ingest and queries are refused while it runs; every SSP is evicted and re-bootstraps after.',
          'Migrations must be run again afterwards (spky migrate).',
          'Bucket file contents are not part of a backup; only their definitions are.',
        ],
      },
      request: post(`/backups/${encodeURIComponent(entry.id)}/restore`),
      success: (r) => `Restore ${r.restore_id} queued`,
      after: (r) => {
        setWatching({
          id: r.restore_id,
          backupId: entry.id,
          label: `Data as of ${formatStamp(entry.completed_at ?? entry.created_at)}`,
          sub: entry.name ? `${entry.name} · ${entry.id}` : entry.id,
        });
        refreshAll();
      },
    });

  const remove = (entry: CatalogEntry) =>
    runAction<{ status: string }>({
      label: `Delete ${entry.name ?? entry.id}`,
      confirm: {
        title: `Delete backup ${entry.name ?? entry.id}?`,
        verb: 'Delete',
        consequences: [
          'The object is removed from storage on the next retention pass and the row is marked deleted.',
          'It cannot be restored from afterwards.',
        ],
      },
      request: () => api.delete(`/backups/${encodeURIComponent(entry.id)}`),
      after: refreshAll,
    });

  const configured = () => !!data()?.s3.configured;
  const restoreBlocked = () => {
    const st = data()?.scheduler_status;
    return st && st !== 'ready' ? `Scheduler is ${st}; restore needs ready` : undefined;
  };
  const lastValue = () => {
    const l = latest();
    return l ? relativeStamp(l.completed_at ?? l.created_at) : 'never';
  };
  const running = () => data()?.local.current_running ?? null;
  const failedRecent = () => (data()?.catalog ?? []).filter((c) => c.status === 'failed').length;
  /** Older than a day and a half while a daily schedule is on: a night was missed. */
  const stale = () => {
    const l = latest();
    if (!l || !data()?.config?.enabled) return !l;
    const t = Date.parse(l.completed_at ?? l.created_at);
    return Date.now() - t > 36 * 3_600_000;
  };

  return (
    <>
      <PageHead
        title="Backups"
        actions={
          <Show when={data()}>
            <BackupNow disabled={creating() || !configured()} reason={configured() ? undefined : 'No object storage configured'}
              name={name()} setName={setName} onStart={() => void create()} />
          </Show>
        }
      />

      <Show when={error()}>
        <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{error()}</span></div></FxCell></section>
      </Show>

      <Show when={!error() && data()} fallback={<Show when={!error()}><BackupsSkeleton /></Show>}>
        {(d) => (
          <div class="fx-page">
            <Show when={!d().s3.configured}>
              <section class="fx-row">
                <FxCell>
                  <div class="fx-log bad">
                    <span class="fx-what">No object storage · set S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY and S3_BUCKET on the scheduler</span>
                  </div>
                </FxCell>
              </section>
            </Show>

            <Show when={(props.overview?.operations ?? []).some((o) => o.status === 'running')}>
              <section class="fx-row">
                <FxCell label="Running now">
                  <ActivityStrip
                    operations={props.overview?.operations}
                    ssps={props.overview?.ssps}
                    exclude={(op) => op.kind === 'backup_restore' && op.detail?.restore_id === watching()?.id}
                  />
                </FxCell>
              </section>
            </Show>

            <Show when={watching()} keyed>
              {(w) => (
                <section class="fx-row">
                  <FxCell label="Restore">
                    <RestoreProgress id={w.id} backupId={w.backupId} label={w.label} sub={w.sub}
                      onDone={refreshAll} onClose={() => setWatching(null)} />
                  </FxCell>
                </section>
              )}
            </Show>

            {/* where things stand ============================================ */}
            <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
              <FxCell label="Last backup" meta={<Show when={running()}><span class="warn">backing up now</span></Show>}>
                <div class="fx-big"><span classList={{ warn: stale() }}>{lastValue()}</span></div>
                <div class="fx-sub cap">
                  <Show when={running()} fallback={
                    <Show when={latest()} fallback={<span class="warn">none completed yet</span>}>
                      {(l) => <><span><b>{formatBytes(l().size_bytes)}</b></span><Show when={l().snapshot_seq != null}><span>seq <b>{formatCount(l().snapshot_seq)}</b></span></Show></>}
                    </Show>
                  }>
                    {(r) => <span class="warn">started {elapsed(r().started_at ?? r().enqueued_at, null)} ago</span>}
                  </Show>
                </div>
              </FxCell>
              <FxCell label="Next backup">
                <div class="fx-big">
                  {d().config?.enabled && d().config?.next_run_at ? relativeStamp(d().config!.next_run_at) : 'off'}
                </div>
                <div class="fx-sub cap">
                  <span>
                    {d().config
                      ? d().config!.enabled && d().config!.schedule
                        ? (describeCron(d().config!.schedule!) ?? d().config!.schedule!)
                        : 'no schedule'
                      : 'scheduled by Sp00ky Cloud'}
                  </span>
                </div>
              </FxCell>
              <FxCell label="Kept" meta={<Show when={failedRecent()}><span class="bad">{failedRecent()} failed</span></Show>}>
                <div class="fx-big">{formatCount(completed().length)}<small>{completed().length === 1 ? 'backup' : 'backups'}</small></div>
                <div class="fx-sub cap">
                  <span><b>{formatBytes(totalBytes())}</b></span>
                  <Show when={describeKeep(d().config?.retention, d().config?.keep)}>{(k) => <span>keeps <b>{k()}</b></span>}</Show>
                </div>
              </FxCell>
              <StoredIn endpoint={d().s3.endpoint} bucket={d().s3.bucket} linked={d().linked} />
            </section>

            {/* how they have gone, and when they run ========================== */}
            <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
              <FxCell label="History" meta={<span>size of each backup, oldest left</span>}>
                <History catalog={d().catalog} />
              </FxCell>
              <SchedulePanel linked={d().linked} config={d().config} onSaved={refreshAll} />
            </section>

            {/* restore to a moment ============================================ */}
            <Show when={d().linked && d().config?.point_in_time}>
              {(pit) => (
                <section class="fx-row">
                  <PointInTimePanel
                    pit={pit()}
                    blocked={restoreBlocked()}
                    onSaved={refreshAll}
                    onRestore={(r, at) => {
                      setWatching({ id: r.restore_id, backupId: r.backup_id, label: `Data as of ${formatStamp(at)}`, sub: `point in time · from ${r.backup_id}` });
                      refreshAll();
                    }}
                  />
                </section>
              )}
            </Show>

            {/* every backup =================================================== */}
            <section class="fx-row">
              <FxCell label="Backups" meta={<span><b>{formatCount(d().catalog.length)}</b></span>}>
                <Show when={d().catalog.length > 0} fallback={<div class="fx-empty cap">No backups yet</div>}>
                  <table class="fx-list bu-list">
                    <colgroup>
                      <col style={{ width: '22%' }} /><col style={{ width: '14%' }} /><col style={{ width: '11%' }} />
                      <col style={{ width: '12%' }} /><col /><col style={{ width: '200px' }} />
                    </colgroup>
                    <thead>
                      <tr>
                        <th class="cap">Backup</th><th class="cap">Taken</th><th class="cap r">Size</th>
                        <th class="cap r">Snapshot</th><th class="cap">State</th><th class="cap" />
                      </tr>
                    </thead>
                    <tbody>
                      <For each={d().catalog}>
                        {(c) => {
                          const size = () => c.local?.size_bytes ?? c.size_bytes;
                          const live = () => c.status === 'pending' || c.status === 'in_progress';
                          const tone = () => (c.status === 'failed' ? 'bad' : live() ? 'warn' : c.status === 'deleted' ? 'idle' : '');
                          return (
                            <tr classList={{ 'bu-dead': c.status === 'deleted' }}>
                              <td>
                                <span class="fx-who">
                                  <Sq tone={tone()} />
                                  <span style={{ color: 'var(--ink)' }}>{c.name ?? c.id}</span>
                                </span>
                                <Show when={c.name}><span class="vw-meta cap"><span>{c.id}</span></span></Show>
                              </td>
                              <td title={formatStamp(c.created_at)}>{relativeStamp(c.created_at)}</td>
                              <td class="r">{size() ? formatBytes(size()!) : '—'}</td>
                              <td class="r">{c.snapshot_seq != null ? formatCount(c.snapshot_seq) : '—'}</td>
                              <td class="bu-state" classList={{ bad: c.status === 'failed', warn: live() }} title={c.error ?? undefined}>
                                {c.error ?? (live() ? (c.local?.status ?? c.status.replace('_', ' ')) : c.status === 'completed' ? 'completed' : c.status)}
                              </td>
                              <td class="r">
                                <Show when={c.status === 'completed'}>
                                  <span class="bu-actions">
                                    <button class="btn btn-sm" title={restoreBlocked()} disabled={!!restoreBlocked()} onClick={() => void restore(c)}>Restore</button>
                                    <Show when={d().linked}>
                                      <button class="btn btn-sm" onClick={() => void remove(c)}>Delete</button>
                                    </Show>
                                  </span>
                                </Show>
                              </td>
                            </tr>
                          );
                        }}
                      </For>
                    </tbody>
                  </table>
                </Show>
              </FxCell>
            </section>

            {/* In memory on this scheduler only, so usually empty. */}
            <Show when={d().restores.length > 0}>
              <section class="fx-row">
                <FxCell label="Restores" meta={<span>since the scheduler started</span>}>
                  <table class="fx-list">
                    <thead><tr><th class="cap">From</th><th class="cap">State</th><th class="cap r">Took</th><th class="cap r">Started</th></tr></thead>
                    <tbody>
                      <For each={d().restores}>
                        {(r) => (
                          <tr class="inc-tr" onClick={() => setWatching({ id: r.restore_id, backupId: r.backup_id })}>
                            <td><span class="fx-who"><Sq tone={r.status === 'failed' ? 'bad' : r.status === 'queued' || r.status === 'running' || r.status === 'awaiting_replay' ? 'warn' : ''} /><span style={{ color: 'var(--ink)' }}>{r.backup_id}</span></span></td>
                            <td classList={{ bad: r.status === 'failed', warn: r.status === 'running' || r.status === 'queued' }}>{r.status}</td>
                            <td class="r">{elapsed(r.started_at ?? r.enqueued_at, r.finished_at)}</td>
                            <td class="r">{relativeStamp(r.started_at ?? r.enqueued_at)}</td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </FxCell>
              </section>
            </Show>
          </div>
        )}
      </Show>
    </>
  );
}

/**
 * "Back up now", with an optional name: the button opens a small popover so
 * the name field does not sit in the top bar for the one time in fifty it is
 * wanted. Enter starts it; Escape closes.
 */
function BackupNow(props: { disabled: boolean; reason?: string; name: string; setName: (v: string) => void; onStart: () => void }) {
  const [open, setOpen] = createSignal(false);
  let root: HTMLDivElement | undefined;
  let input: HTMLInputElement | undefined;
  const onDoc = (e: MouseEvent) => {
    if (open() && root && !root.contains(e.target as Node)) setOpen(false);
  };
  document.addEventListener('mousedown', onDoc);
  onCleanup(() => document.removeEventListener('mousedown', onDoc));
  const start = () => {
    setOpen(false);
    props.onStart();
  };
  return (
    <div class="fm" ref={root}>
      <button class="btn btn-sm btn-primary" disabled={props.disabled} title={props.reason}
        onClick={() => { setOpen(!open()); queueMicrotask(() => input?.focus()); }}>
        Back up now
      </button>
      <Show when={open()}>
        <div class="fm-pop bu-pop" style={{ left: 'auto', right: '0' }}
          onKeyDown={(e) => { if (e.key === 'Escape') setOpen(false); if (e.key === 'Enter') start(); }}>
          <div class="fm-search">
            <input ref={input} placeholder="Name (optional)" value={props.name} onInput={(e) => props.setName(e.currentTarget.value)} />
          </div>
          <div class="bu-pop-foot">
            <span class="cap">taken from the live database</span>
            <button class="btn btn-sm btn-primary" onClick={start}>Start</button>
          </div>
        </div>
      </Show>
    </div>
  );
}

/** One bar per backup, oldest left: its size, red where it failed, the newest in the accent. */
function History(props: { catalog: CatalogEntry[] }) {
  const items = createMemo(() =>
    [...props.catalog].filter((c) => c.status !== 'deleted').sort((a, b) => a.created_at.localeCompare(b.created_at)),
  );
  const hi = () => Math.max(1, ...items().map((c) => c.size_bytes || 0));
  const lo = () => Math.min(...items().filter((c) => c.size_bytes).map((c) => c.size_bytes));
  const day = (iso: string) => new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  return (
    <Show when={items().length} fallback={<div class="fx-empty cap">No backups yet</div>}>
      <div class="bu-hist">
        <For each={items()}>
          {(c, i) => {
            const failed = c.status === 'failed';
            // From 40% up, so a few percent of growth is still visible.
            const h = () => (failed || !c.size_bytes ? 100 : 40 + ((c.size_bytes - lo()) / Math.max(1, hi() - lo())) * 60);
            return (
              <span class="bu-col" title={`${c.name ?? c.id} · ${day(c.created_at)} · ${failed ? 'failed' : formatBytes(c.size_bytes)}`}>
                <i classList={{ failed, live: c.status === 'pending' || c.status === 'in_progress', last: !failed && i() === items().length - 1 }}
                  style={{ height: `${h()}%` }} />
              </span>
            );
          }}
        </For>
      </div>
      <div class="fx-axis cap">
        <span>{day(items()[0].created_at)}</span>
        <span>{formatBytes(lo())} – {formatBytes(hi())}</span>
        <span>{day(items()[items().length - 1].created_at)}</span>
      </div>
    </Show>
  );
}

/* ------------------------------------------------------------------ */
/* Schedule                                                             */
/* ------------------------------------------------------------------ */

const KEEP_TIERS = ['hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'] as const satisfies ReadonlyArray<keyof KeepTiers>;

/** backups.RecommendedPolicy in Sp00ky Cloud, for a cloud that does not send it. */
const RECOMMENDED: KeepTiers = { hourly: 0, daily: 3, weekly: 5, monthly: 0, quarterly: 4, yearly: 0 };

/** Sp00ky Cloud's MaxKeep: the most any one tier may hold. */
const MAX_KEEP = 1000;
const DAY_MS = 86_400_000;

/** "newest 7 · 3 daily · 5 weekly", or null when everything is kept. */
function describeKeep(retention: number | null | undefined, keep: KeepTiers | null | undefined): string | null {
  const parts: string[] = [];
  if (retention) parts.push(`newest ${formatCount(retention)}`);
  for (const tier of KEEP_TIERS) {
    const n = keep?.[tier];
    if (n) parts.push(`${formatCount(n)} ${tier}`);
  }
  return parts.length ? parts.join(' · ') : null;
}

/** The form's policy: `last` is the `retention` setting, the rest are tiers. */
const policyOf = (c: BackupConfig | null): Policy => ({
  last: c?.retention ?? 0,
  hourly: c?.keep?.hourly ?? 0,
  daily: c?.keep?.daily ?? 0,
  weekly: c?.keep?.weekly ?? 0,
  monthly: c?.keep?.monthly ?? 0,
  quarterly: c?.keep?.quarterly ?? 0,
  yearly: c?.keep?.yearly ?? 0,
});
const samePolicy = (a: Policy, b: Policy) => RETENTION_TIERS.every((t) => a[t] === b[t]);

/** The ladder's rungs, in the order retention fills them. */
const RUNGS: ReadonlyArray<{ tier: Tier; name: string; help: string; short: string }> = [
  { tier: 'last', name: 'Newest', short: 'the newest, any age', help: 'The newest backups, whatever their age. Filled first, so every rung below starts where this one ends.' },
  { tier: 'hourly', name: 'Hourly', short: 'one per hour', help: 'The newest backup of each hour' },
  { tier: 'daily', name: 'Daily', short: 'one per day', help: 'The newest backup of each day' },
  { tier: 'weekly', name: 'Weekly', short: 'one per week', help: 'The newest backup of each ISO week' },
  { tier: 'monthly', name: 'Monthly', short: 'one per month', help: 'The newest backup of each month' },
  { tier: 'quarterly', name: 'Quarterly', short: 'one per quarter', help: 'The newest backup of each quarter' },
  { tier: 'yearly', name: 'Yearly', short: 'one per year', help: 'The newest backup of each year' },
];

/* The cron shapes the schedule line can edit; anything else is typed. */
type Shape = { freq: 'hourly' | 'daily' | 'weekly' | 'cron'; hour: number; minute: number; dow: number };

function shapeOf(cron: string): Shape {
  const s = cron.trim().split(/\s+/).join(' ');
  const fallback: Shape = { freq: 'cron', hour: 3, minute: 0, dow: 1 };
  let m = /^(\d{1,2}) \* \* \* \*$/.exec(s);
  if (m) return Number(m[1]) < 60 ? { ...fallback, freq: 'hourly', minute: Number(m[1]) } : fallback;
  m = /^(\d{1,2}) (\d{1,2}) \* \* (\*|[0-7])$/.exec(s);
  if (!m || Number(m[1]) > 59 || Number(m[2]) > 23) return fallback;
  const at = { hour: Number(m[2]), minute: Number(m[1]) };
  return m[3] === '*' ? { ...fallback, ...at, freq: 'daily' } : { ...fallback, ...at, freq: 'weekly', dow: Number(m[3]) % 7 };
}

function cronOf(s: Shape): string {
  if (s.freq === 'hourly') return `${s.minute} * * * *`;
  if (s.freq === 'weekly') return `${s.minute} ${s.hour} * * ${s.dow}`;
  return `${s.minute} ${s.hour} * * *`;
}

const WEEK = [1, 2, 3, 4, 5, 6, 0];
const DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const clock = (h: number, m: number) => `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
/** Half hours, plus the stored time when it falls between them. */
const times = (h: number, m: number) => {
  const all = Array.from({ length: 48 }, (_, i) => [i >> 1, (i & 1) * 30] as const);
  return m % 30 ? [[h, m] as const, ...all] : all;
};
const minutes = (m: number) => ([0, 15, 30, 45].includes(m) ? [0, 15, 30, 45] : [m, 0, 15, 30, 45]);
/** A typed cron line in words, always saying it is UTC. */
const cronWords = (cron: string) => {
  const words = describeCron(cron) ?? 'custom expression';
  return words.includes('UTC') ? words : `${words} (UTC)`;
};
/** Width for a select showing `text`: mono, so characters plus the caret's room. */
const selWidth = (text: string) => `calc(${text.length}ch + 16px)`;

/** Axis marks under the lanes, in days ago. */
const TICKS: ReadonlyArray<readonly [number, string]> = [
  [0, 'now'], [1, '1d'], [7, '1w'], [30.4, '1m'], [91, '3m'], [182, '6m'], [365, '1y'], [730, '2y'], [1826, '5y'],
];

/** The count a period comes back with when it is added. */
const ADD_COUNT: Record<Tier, number> = { last: 7, hourly: 24, daily: 7, weekly: 4, monthly: 12, quarterly: 4, yearly: 3 };

/** A bare number; its − and + show while the row is hovered or focused. */
function Count(props: { value: number; label: string; onChange: (n: number) => void }) {
  const set = (n: number) => props.onChange(Math.max(0, Math.min(MAX_KEEP, n)));
  return (
    <span class="bu-count">
      <button type="button" aria-label={`Fewer: ${props.label}`} disabled={props.value <= 0} onClick={() => set(props.value - 1)}>−</button>
      <input
        value={props.value}
        inputmode="numeric"
        aria-label={props.label}
        onInput={(e) => {
          const raw = e.currentTarget.value.trim();
          if (/^\d+$/.test(raw)) set(Number(raw));
        }}
        onKeyDown={(e) => {
          if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;
          e.preventDefault();
          set(props.value + (e.key === 'ArrowUp' ? 1 : -1));
        }}
        onBlur={(e) => {
          if (e.currentTarget.value.trim() === '') set(0);
          else e.currentTarget.value = String(props.value);
        }}
      />
      <button type="button" aria-label={`More: ${props.label}`} disabled={props.value >= MAX_KEEP} onClick={() => set(props.value + 1)}>+</button>
    </span>
  );
}

/**
 * The schedule as one sentence, then retention as a row per period in use,
 * each drawing the backups it keeps on that schedule today on a shared log
 * timeline (newest left). Periods fill in order and each skips what the one
 * above already holds, so the rows step down and to the right: a staircase.
 * Hovering a row puts the dates it covers in the header.
 */
function SchedulePanel(props: {
  linked: boolean;
  config: BackupConfig | null;
  onSaved: () => void;
}) {
  const storedSchedule = () => props.config?.schedule || '0 3 * * *';
  const [enabled, setEnabled] = createSignal(props.config?.enabled ?? false);
  const [schedule, setSchedule] = createSignal(storedSchedule());
  const [typing, setTyping] = createSignal(shapeOf(storedSchedule()).freq === 'cron');
  const [policy, setPolicy] = createSignal<Policy>(policyOf(props.config));
  const [hot, setHot] = createSignal<Tier | null>(null);
  const [saving, setSaving] = createSignal(false);
  /** Tiers need a cloud that knows them; an older one would drop them silently. */
  const tiered = () => props.config?.keep != null;
  const recommended = (): Policy => ({ ...EMPTY_POLICY, ...(props.config?.recommended ?? RECOMMENDED) });

  const shape = createMemo(() => shapeOf(schedule()));
  const freq = () => (typing() ? 'cron' : shape().freq === 'weekly' ? `w${shape().dow}` : shape().freq);
  const freqLabel = () => {
    const f = freq();
    return f === 'cron' ? 'cron expression' : f === 'hourly' ? 'hour' : f === 'daily' ? 'day' : DAY_NAMES[shape().dow]!;
  };

  /** The time of day last chosen, so a detour through hourly does not lose it. */
  let clockAt = { hour: 3, minute: 0 };
  createEffect(() => {
    const s = shape();
    if (s.freq === 'daily' || s.freq === 'weekly') clockAt = { hour: s.hour, minute: s.minute };
  });

  const setFreq = (v: string) => {
    if (v === 'cron') return setTyping(true);
    const base = v === 'hourly' ? shape() : { ...shape(), ...clockAt };
    setTyping(false);
    if (v === 'hourly' || v === 'daily') setSchedule(cronOf({ ...base, freq: v }));
    else setSchedule(cronOf({ ...base, freq: 'weekly', dow: Number(v.slice(1)) }));
  };

  const setTier = (t: Tier, n: number) => setPolicy({ ...policy(), [t]: n });

  /** What the policy keeps on this schedule, as of now. */
  const sim = createMemo(() => {
    const now = Date.now();
    const exact = cadence(schedule(), now);
    const c = exact ?? cadence('0 3 * * *', now)!;
    const p = policy();
    return { now, approx: !exact, step: c.stepMs, r: retain(p, pastRuns(c, p)) };
  });
  const oldest = () => sim().r.kept[sim().r.kept.length - 1]?.at;

  /**
   * Every period has a row whether it is used or not, so adding or removing
   * one never moves the others. Hourly only shows on an hourly schedule.
   */
  const available = () =>
    tiered()
      ? RUNGS.filter((g) => g.tier !== 'hourly' || sim().step < DAY_MS || policy().hourly > 0)
      : RUNGS.filter((g) => g.tier === 'last');

  /** Log scale, so the last few days and the last year both read. */
  const span = () => Math.max(35, ((sim().now - (oldest() ?? sim().now)) / DAY_MS) * 1.1);
  const pctDays = (days: number) => (Math.log1p(Math.max(0, days)) / Math.log1p(span())) * 100;
  const pct = (t: number) => pctDays((sim().now - t) / DAY_MS);
  /** Axis labels under the lanes, skipping any that would run into the one before. */
  const ticks = createMemo(() => {
    let prev = -Infinity;
    return TICKS.filter(([d]) => {
      if (d > span() || pctDays(d) - prev < 17) return false;
      prev = pctDays(d);
      return true;
    });
  });
  const keptBy = (t: Tier) => sim().r.kept.filter((k) => k.tier === t);

  /** The periods a row covers: "10 Nov → 12 Nov", "Q4 2025 → Q3". */
  const reach = (t: Tier) => {
    if (!policy()[t]) return 'off';
    const mine = keptBy(t);
    if (!mine.length) return 'nothing yet';
    const name = (m: { at: number }) => periodName(t, m.at, sim().now);
    return mine.length === 1 ? name(mine[0]!) : `${name(mine[mine.length - 1]!)} → ${name(mine[0]!)}`;
  };

  /** The total, or while a row is hovered, the dates it covers or what adding it keeps. */
  const headline = () => {
    const t = hot();
    if (t) {
      const g = RUNGS.find((r) => r.tier === t)!;
      return policy()[t] ? `${g.name}: ${reach(t)}` : `Add ${g.name.toLowerCase()}: ${g.short}`;
    }
    if (!sim().r.prunes) return 'keeps everything';
    return `${formatCount(sim().r.kept.length)} kept, back to ${shortDate(oldest()!, sim().now)}${sim().approx ? ' (as if nightly)' : ''}`;
  };

  const dirty = () =>
    enabled() !== (props.config?.enabled ?? false) ||
    schedule().trim() !== storedSchedule() ||
    !samePolicy(policy(), policyOf(props.config));

  const useRecommended = () => {
    setPolicy(recommended());
    setEnabled(true);
  };

  const save = async () => {
    setSaving(true);
    try {
      const body: Record<string, unknown> = { enabled: enabled() };
      if (schedule().trim()) body.schedule = schedule().trim();
      // Only what changed: an untouched field must not overwrite a default the
      // cloud applies while nothing is stored (a managed database's policy).
      const p = policy();
      const stored = policyOf(props.config);
      if (p.last !== stored.last) body.retention = p.last;
      if (tiered() && KEEP_TIERS.some((t) => p[t] !== stored[t])) {
        body.keep = Object.fromEntries(KEEP_TIERS.map((t) => [t, p[t]]));
      }
      await runAction<{ status: string }>({
        label: 'Backup schedule',
        request: () => api.put('/backups/config', body),
        success: () => 'Saved',
        after: props.onSaved,
      });
    } finally {
      setSaving(false);
    }
  };

  return (
    <FxCell
      label="Schedule"
      class="bu-sched"
      meta={
        <Show when={props.linked}>
          <label class="switch">
            <input type="checkbox" checked={enabled()} onChange={(e) => setEnabled(e.currentTarget.checked)} />
            <span class="switch-track" />
            <span class="cap">{enabled() ? 'On' : 'Off'}</span>
          </label>
        </Show>
      }
    >
      <Show
        when={props.linked}
        fallback={<div class="fx-empty cap">Scheduled backups need a project linked to Sp00ky Cloud, then spky backup configure</div>}
      >
        {/* when it runs, as a sentence ======================================== */}
        <div class="bu-when" classList={{ off: !enabled() }}>
          <span>Every</span>
          <select class="bu-sel" aria-label="How often" style={{ width: selWidth(freqLabel()) }} onChange={(e) => setFreq(e.currentTarget.value)}>
            <option value="hourly" selected={freq() === 'hourly'}>hour</option>
            <option value="daily" selected={freq() === 'daily'}>day</option>
            <For each={WEEK}>{(d) => <option value={`w${d}`} selected={freq() === `w${d}`}>{DAY_NAMES[d]}</option>}</For>
            <option value="cron" selected={freq() === 'cron'}>cron expression</option>
          </select>
          <Show when={!typing()}>
            <Show
              when={shape().freq === 'hourly'}
              fallback={
                <>
                  <span>at</span>
                  <select
                    class="bu-sel"
                    aria-label="Time, UTC"
                    style={{ width: selWidth('00:00') }}
                    onChange={(e) => {
                      const [h, m] = e.currentTarget.value.split(':').map(Number);
                      setSchedule(cronOf({ ...shape(), hour: h!, minute: m! }));
                    }}
                  >
                    <For each={times(shape().hour, shape().minute)}>
                      {([h, m]) => <option value={`${h}:${m}`} selected={h === shape().hour && m === shape().minute}>{clock(h, m)}</option>}
                    </For>
                  </select>
                  <span>UTC</span>
                </>
              }
            >
              <span>at minute</span>
              <select
                class="bu-sel"
                aria-label="Minute"
                style={{ width: selWidth(':00') }}
                onChange={(e) => setSchedule(cronOf({ ...shape(), minute: Number(e.currentTarget.value) }))}
              >
                <For each={minutes(shape().minute)}>
                  {(m) => <option value={m} selected={m === shape().minute}>:{String(m).padStart(2, '0')}</option>}
                </For>
              </select>
            </Show>
          </Show>
          <Show when={typing()}>
            <input class="mono bu-cron" value={schedule()} placeholder="0 3 * * *" spellcheck={false}
              aria-label="Cron expression, UTC" onInput={(e) => setSchedule(e.currentTarget.value)} />
            <small class="bu-cron-words">{cronWords(schedule())}</small>
          </Show>
        </div>

        {/* what it keeps: a row per period, each on a shared timeline ========= */}
        <div class="fx-head bu-keep-head">
          <span class="cap">Retention</span>
          <span class="fx-meta cap" classList={{ warn: !sim().r.prunes }}>{headline()}</span>
        </div>

        <div class="bu-stairs" onMouseLeave={() => setHot(null)}>
          <For each={available()}>
            {(g) => (
              <Show
                when={policy()[g.tier] > 0}
                fallback={
                  <button type="button" class="bu-stair off" classList={{ hot: hot() === g.tier }} title={g.help}
                    onClick={() => setTier(g.tier, ADD_COUNT[g.tier])}
                    onMouseEnter={() => setHot(g.tier)} onFocus={() => setHot(g.tier)} onBlur={() => setHot(null)}>
                    <span class="bu-stair-name">{g.name}</span>
                    <span />
                    <span class="bu-lane" />
                    <span class="bu-stair-act" aria-hidden="true">+</span>
                  </button>
                }
              >
                <div class="bu-stair" classList={{ hot: hot() === g.tier }} title={`${g.help}: ${reach(g.tier)}`}
                  onMouseEnter={() => setHot(g.tier)}>
                  <span class="bu-stair-name">{g.name}</span>
                  <Count value={policy()[g.tier]} label={g.name} onChange={(n) => setTier(g.tier, n)} />
                  <div class="bu-lane">
                    <For each={ticks().slice(1)}>{([d]) => <span class="bu-lane-grid" style={{ left: `${pctDays(d)}%` }} />}</For>
                    <Show when={keptBy(g.tier).length > 1}>
                      <u style={{
                        left: `${pct(keptBy(g.tier)[0]!.at)}%`,
                        width: `${pct(keptBy(g.tier)[keptBy(g.tier).length - 1]!.at) - pct(keptBy(g.tier)[0]!.at)}%`,
                      }} />
                    </Show>
                    <For each={keptBy(g.tier)}>{(k) => <i style={{ left: `${pct(k.at)}%` }} />}</For>
                  </div>
                  <button type="button" class="bu-stair-act" aria-label={`Remove ${g.name.toLowerCase()}`}
                    title={`Remove ${g.name.toLowerCase()}`} onClick={() => setTier(g.tier, 0)}>×</button>
                </div>
              </Show>
            )}
          </For>
          <div class="bu-stair bu-stair-axis">
            <span class="bu-stair-rec">
              <Show when={tiered()}>
                <Show when={!samePolicy(policy(), recommended())} fallback={<span>Recommended</span>}>
                  <button type="button" class="bu-link" onClick={useRecommended}
                    title="3 days, then one a week for the month before, then four quarter ends">
                    Use recommended
                  </button>
                </Show>
              </Show>
            </span>
            <div class="bu-lane-axis" aria-hidden="true">
              <For each={ticks()}>{([d, label], i) => <span classList={{ first: i() === 0 }} style={{ left: `${pctDays(d)}%` }}>{label}</span>}</For>
            </div>
            <span />
          </div>
        </div>

        <div class="bu-foot">
          <small>Named backups are never pruned</small>
          <button class="btn btn-sm btn-primary" disabled={!dirty() || saving()} onClick={() => void save()}>
            {saving() ? 'Saving…' : 'Save'}
          </button>
        </div>
      </Show>
    </FxCell>
  );
}

/* ------------------------------------------------------------------ */
/* Point in time                                                        */
/* ------------------------------------------------------------------ */

/** An ISO time as a datetime-local input value, in the browser's zone. */
const localInput = (iso: string) => {
  const d = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
};

function PointInTimePanel(props: {
  pit: PointInTime;
  blocked: string | undefined;
  onSaved: () => void;
  onRestore: (r: { restore_id: string; backup_id: string }, at: string) => void;
}) {
  const [moment, setMoment] = createSignal(props.pit.window ? localInput(props.pit.window.latest) : '');

  const toggle = () =>
    runAction<{ status: string }>({
      label: props.pit.enabled ? 'Point-in-time recovery off' : 'Point-in-time recovery on',
      confirm: props.pit.enabled
        ? {
            title: 'Turn point-in-time recovery off?',
            verb: 'Turn off',
            consequences: [
              'The recorded changes are deleted; restores can only reach full backups again.',
              "The database's change feed is taken off.",
            ],
          }
        : {
            title: 'Turn point-in-time recovery on?',
            verb: 'Turn on',
            consequences: [
              'The database keeps a change feed of every write (a day of it), which costs some write throughput and disk.',
              'Changes are read every 15 minutes; a restore can then reach any moment of the last 48 hours.',
              'A full backup starts about ten minutes later to replay from, and at least one a day after that.',
            ],
          },
      request: () => api.put('/backups/config', { point_in_time: !props.pit.enabled }),
      success: () => 'Saved',
      after: props.onSaved,
    });

  const restoreTo = () => {
    const t = Date.parse(moment());
    if (Number.isNaN(t)) {
      toast('bad', 'Pick a moment first');
      return;
    }
    const at = new Date(t).toISOString();
    void runAction<{ operation: OperationResponse['operation']; restore_id: string; backup_id: string }>({
      label: `Restore to ${formatStamp(at)}`,
      confirm: {
        title: `Restore the database to ${formatStamp(at)}?`,
        verb: 'Restore',
        typeToConfirm: 'restore',
        consequences: [
          'The whole database is dropped, re-imported from the newest backup before that moment, and every change recorded up to it is replayed. Everything written after it is lost.',
          'Ingest and queries are refused while it runs; every SSP is evicted and re-bootstraps after. On a large database this takes a while: the replica is rebuilt from the result.',
          'Migrations must be run again afterwards (spky migrate).',
          'Bucket file contents are not part of a backup; only their definitions are.',
        ],
      },
      request: post('/backups/restore-to', { at }),
      success: (r) => `Restore ${r.restore_id} queued`,
      after: (r) => props.onRestore(r, at),
    });
  };

  const outside = () => {
    const w = props.pit.window;
    const t = Date.parse(moment());
    return !w || Number.isNaN(t) || t < Date.parse(w.earliest) - 60_000 || t > Date.parse(w.latest);
  };

  return (
    <FxCell
      label="Point in time"
      meta={
        <span style={{ display: 'inline-flex', 'align-items': 'center', gap: '10px' }}>
          <Show when={props.pit.last_reading}><span>last reading {relativeStamp(props.pit.last_reading)}</span></Show>
          <button type="button" class="chip" onClick={() => void toggle()}>{props.pit.enabled ? 'Turn off' : 'Turn on'}</button>
        </span>
      }
    >
      <Show
        when={props.pit.enabled}
        fallback={<div class="fx-empty cap">Off · turn it on to restore to any moment of the last 48 hours, not just to a backup</div>}
      >
        <div class="bu-pit">
          <div class="fx-sub cap">
            <Show
              when={props.pit.window}
              fallback={<span>Recording changes since {formatStamp(props.pit.since)} · waiting for the first backup to replay from</span>}
            >
              {(w) => (
                <span>
                  any moment from <b>{formatStamp(w().earliest)}</b> to <b>{formatStamp(w().latest)}</b> · read every 15 minutes
                </span>
              )}
            </Show>
          </div>
          <Show when={props.pit.error}>
            <div class="fx-sub cap bad">{props.pit.error}</div>
          </Show>
          <Show when={props.pit.window}>
            {(w) => (
              <div class="bu-pit-form">
                <label class="li-field">
                  <span>Restore to</span>
                  <input type="datetime-local" step="60" value={moment()}
                    min={localInput(w().earliest)} max={localInput(w().latest)}
                    onInput={(e) => setMoment(e.currentTarget.value)} />
                  <small class="cap">your time zone · {Intl.DateTimeFormat().resolvedOptions().timeZone}</small>
                </label>
                <button class="btn btn-sm" title={props.blocked ?? (outside() ? 'Outside the window' : undefined)}
                  disabled={!!props.blocked || outside()} onClick={restoreTo}>
                  Restore to this moment
                </button>
              </div>
            )}
          </Show>
        </div>
      </Show>
    </FxCell>
  );
}

/* ------------------------------------------------------------------ */
/* Restore progress                                                     */
/* ------------------------------------------------------------------ */

function RestoreProgress(props: {
  id: string;
  backupId: string;
  label?: string;
  sub?: string;
  onDone: () => void;
  onClose: () => void;
}) {
  const [status, { refetch }] = createResource(
    () => props.id,
    (id) => api.getResult<RestoreStatus>(`/backups/${encodeURIComponent(id)}/restore`),
  );
  const st = () => {
    const r = status();
    return r?.ok ? r.value : undefined;
  };
  const err = () => {
    const r = status();
    return r && !r.ok ? r.message : undefined;
  };

  let finishedSeen = false;
  const timer = setInterval(() => {
    const s = st();
    const finished = s?.stage === 'done' || s?.stage === 'failed';
    if (finished) {
      if (!finishedSeen) {
        finishedSeen = true;
        props.onDone();
      }
      return;
    }
    void refetch();
  }, 2000);
  onCleanup(() => clearInterval(timer));

  const opened = Date.now();
  const local = () => st()?.local ?? null;

  /**
   * The main database came back but the scheduler's own state did not. It
   * stays in `restoring` on purpose rather than serve a replica that
   * disagrees with the database; say so, with the two ways out.
   */
  const stuck = () =>
    st()?.stage === 'failed' && !!local()?.main_db_restored && !local()?.replica_restored;

  return (
    <Show when={!err()} fallback={<div class="fx-log bad"><span class="fx-what">{err()}</span></div>}>
      <div class="opc-grid">
        <OpCard
          view={{
            ...restoreView(props.backupId, st(), props.label, opened, props.sub),
            onClose: props.onClose,
            notice: (
              <>
                <Show when={st()?.cloud && !local()}>
                  <div class="opc-message">
                    Sp00ky Cloud has queued this restore ({st()!.cloud!.status}). The
                    scheduler's worker picks it up within a few seconds.
                  </div>
                </Show>
                <Show when={stuck()}>
                  <div class="opc-message bad">
                    The database was restored but the scheduler's replica was not, so it
                    stays in <span class="mono">restoring</span> and refuses traffic. Run
                    the restore again, or restart the scheduler with a clean volume so it
                    reclones from the restored database.
                  </div>
                </Show>
              </>
            ),
          }}
        />
      </div>
    </Show>
  );
}

/**
 * Where the backups are, as a person would say it: the provider and its
 * region, read off the S3 endpoint. The bucket follows on one line, shortened
 * in the middle (Sp00ky Cloud's are named after the tenant id) and copyable
 * whole; the full endpoint is the headline's tooltip.
 */
function StoredIn(props: { endpoint: string | null; bucket: string | null; linked: boolean }) {
  const [copied, setCopied] = createSignal(false);
  const where = () => storeOf(props.endpoint);
  return (
    <FxCell label="Stored in" meta={<span>{props.linked ? 'Sp00ky Cloud' : 'bucket listing'}</span>}>
      <div class="fx-big bu-where" title={props.endpoint ?? undefined}>
        <span>{where().provider}</span>
        <Show when={where().region}>
          <small>{where().region}</small>
        </Show>
      </div>
      <Show when={props.bucket} fallback={<div class="fx-sub cap"><span>no bucket configured</span></div>}>
        {(bucket) => (
          <div class="fx-sub cap bu-store">
            <span>bucket</span>
            <b title={bucket()}>{middle(bucket(), 22)}</b>
            <button
              class="bu-copy cap"
              classList={{ done: copied() }}
              title={`Copy ${bucket()}`}
              onClick={() => {
                void navigator.clipboard?.writeText(bucket());
                setCopied(true);
                setTimeout(() => setCopied(false), 1200);
              }}
            >
              {copied() ? 'copied' : 'copy'}
            </button>
          </div>
        )}
      </Show>
    </FxCell>
  );
}

/** The provider and region of an S3 endpoint; the bare host when unknown. */
function storeOf(endpoint: string | null): { provider: string; region?: string } {
  if (!endpoint) return { provider: '—' };
  let host = endpoint;
  try {
    host = new URL(endpoint.includes('://') ? endpoint : `https://${endpoint}`).host;
  } catch {
    // not a URL: show it as given
  }
  const h = host.toLowerCase();
  let m: RegExpExecArray | null;
  if ((m = /(?:^|\.)([a-z]{3}\d)\.your-objectstorage\.com$/.exec(h))) return { provider: 'Hetzner', region: m[1] };
  if (h.endsWith('.r2.cloudflarestorage.com')) return { provider: 'Cloudflare R2' };
  if ((m = /(?:^|\.)s3[.-]([a-z0-9-]+)\.amazonaws\.com$/.exec(h))) return { provider: 'AWS S3', region: m[1] };
  if (h.endsWith('.amazonaws.com')) return { provider: 'AWS S3' };
  if ((m = /(?:^|\.)([a-z]{3}\d)\.digitaloceanspaces\.com$/.exec(h))) return { provider: 'DO Spaces', region: m[1] };
  return { provider: host };
}

/** A long name cut in the middle, keeping both ends. */
function middle(text: string, max: number): string {
  if (text.length <= max) return text;
  const tail = Math.floor((max - 1) / 2);
  return `${text.slice(0, max - 1 - tail)}…${text.slice(-tail)}`;
}
