import { For, Show, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { api } from '../api/client';
import { PageHead } from '../components/Chrome';
import { FxCell, Sq } from '../components/Frame';
import { runAction, post, toast } from '../components/Actions';
import { ActivityStrip, OpCard, restoreView } from '../components/OpCard';
import { describeCron } from '../lib/cron';
import {
  elapsed,
  formatBytes,
  formatCount,
  formatStamp,
  isAbsent,
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

      <Show when={!error() && data()} fallback={<Show when={!error()}><div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div></Show>}>
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

const TIERS: ReadonlyArray<readonly [keyof KeepTiers, string]> = [
  ['hourly', 'Hours'],
  ['daily', 'Days'],
  ['weekly', 'Weeks'],
  ['monthly', 'Months'],
  ['quarterly', 'Quarters'],
  ['yearly', 'Years'],
];

/** backups.RecommendedPolicy in Sp00ky Cloud, for a cloud that does not send it. */
const RECOMMENDED: KeepTiers = { hourly: 0, daily: 3, weekly: 5, monthly: 0, quarterly: 4, yearly: 0 };

/** "newest 7 · 3 daily · 5 weekly", or null when everything is kept. */
function describeKeep(retention: number | null | undefined, keep: KeepTiers | null | undefined): string | null {
  const parts: string[] = [];
  if (retention) parts.push(`newest ${formatCount(retention)}`);
  for (const [tier] of TIERS) {
    const n = keep?.[tier];
    if (n) parts.push(`${formatCount(n)} ${tier}`);
  }
  return parts.length ? parts.join(' · ') : null;
}

const tierText = (keep: KeepTiers | null | undefined) =>
  Object.fromEntries(TIERS.map(([t]) => [t, keep?.[t] ? String(keep[t]) : ''])) as Record<keyof KeepTiers, string>;

function SchedulePanel(props: {
  linked: boolean;
  config: BackupConfig | null;
  onSaved: () => void;
}) {
  const [enabled, setEnabled] = createSignal(props.config?.enabled ?? false);
  const [schedule, setSchedule] = createSignal(props.config?.schedule ?? '');
  const [retention, setRetention] = createSignal(
    props.config?.retention ? String(props.config.retention) : '',
  );
  const [tiers, setTiers] = createSignal(tierText(props.config?.keep));
  const [saving, setSaving] = createSignal(false);
  /** Tiers need a cloud that knows them; an older one would drop them silently. */
  const tiered = () => props.config?.keep != null;
  const recommended = () => props.config?.recommended ?? RECOMMENDED;

  const dirty = () =>
    enabled() !== (props.config?.enabled ?? false) ||
    schedule().trim() !== (props.config?.schedule ?? '') ||
    retention().trim() !== (props.config?.retention ? String(props.config.retention) : '') ||
    TIERS.some(([t]) => tiers()[t].trim() !== tierText(props.config?.keep)[t]);

  const useRecommended = () => {
    setTiers(tierText(recommended()));
    setRetention('');
    if (!schedule().trim()) setSchedule('0 3 * * *');
    setEnabled(true);
  };

  /** What the tiers in the form keep, in words. */
  const keepPreview = () => {
    const keep = Object.fromEntries(TIERS.map(([t]) => [t, Number(tiers()[t]) || 0])) as unknown as KeepTiers;
    return describeKeep(Number(retention()) || 0, keep) ?? 'keeps every backup';
  };

  const preview = () => {
    const s = schedule().trim();
    if (!s) return null;
    return describeCron(s) ?? 'custom expression';
  };

  const save = async () => {
    setSaving(true);
    try {
      const body: Record<string, unknown> = { enabled: enabled() };
      if (schedule().trim()) body.schedule = schedule().trim();
      const whole = (v: string) => {
        const n = Number(v.trim() || '0');
        return Number.isInteger(n) && n >= 0 && n <= 1000 ? n : null;
      };
      // Only what changed: an untouched field must not overwrite a default the
      // cloud applies while nothing is stored (a managed database's policy).
      const r = whole(retention());
      if (r == null) {
        toast('bad', 'Keep newest must be a whole number from 0 to 1000');
        return;
      }
      if (retention().trim() !== (props.config?.retention ? String(props.config.retention) : '')) body.retention = r;
      const stored = tierText(props.config?.keep);
      if (tiered() && TIERS.some(([t]) => tiers()[t].trim() !== stored[t])) {
        const keep: Record<string, number> = {};
        for (const [t, label] of TIERS) {
          const n = whole(tiers()[t]);
          if (n == null) {
            toast('bad', `${label} must be a whole number from 0 to 1000`);
            return;
          }
          keep[t] = n;
        }
        body.keep = keep;
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
    <FxCell label="Schedule" meta={<Show when={props.config?.last_scheduled_at && !isAbsent(props.config?.last_scheduled_at)}><span>last ran {relativeStamp(props.config!.last_scheduled_at)}</span></Show>}>
      <Show
        when={props.linked}
        fallback={<div class="fx-empty cap">Scheduled backups need a project linked to Sp00ky Cloud, then spky backup configure</div>}
      >
        <div class="bu-form">
          <label class="switch">
            <input type="checkbox" checked={enabled()} onChange={(e) => setEnabled(e.currentTarget.checked)} />
            <span class="switch-track" />
            <span class="cap">{enabled() ? 'On' : 'Off'}</span>
          </label>
          <label class="li-field">
            <span>Cron, UTC</span>
            <input class="mono" value={schedule()} placeholder="0 3 * * *" spellcheck={false} onInput={(e) => setSchedule(e.currentTarget.value)} />
            <small class="cap">{preview() ?? 'minute hour day month weekday'}</small>
          </label>
          <label class="li-field">
            <span>Keep newest</span>
            <input value={retention()} placeholder="0" inputmode="numeric" onInput={(e) => setRetention(e.currentTarget.value)} />
            <small class="cap">backups, whatever their age · empty or 0 sets no count</small>
          </label>
          <Show when={tiered()}>
            <div class="li-field">
              <span class="bu-tiers-head">
                Keep one per
                <button type="button" class="chip" onClick={useRecommended}
                  title="3 days, then a backup a week for the month before, then a year of quarter ends">
                  Recommended
                </button>
              </span>
              <div class="bu-tiers">
                <For each={TIERS}>
                  {([t, label]) => (
                    <label>
                      <small class="cap">{label}</small>
                      <input value={tiers()[t]} placeholder="0" inputmode="numeric"
                        onInput={(e) => setTiers({ ...tiers(), [t]: e.currentTarget.value })} />
                    </label>
                  )}
                </For>
              </div>
              <small class="cap">{keepPreview()} · named backups are never pruned</small>
            </div>
          </Show>
          <button class="btn btn-sm btn-primary bu-save" disabled={!dirty() || saving()} onClick={() => void save()}>
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
