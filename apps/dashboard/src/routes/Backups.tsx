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
  OperationResponse,
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
      d?.restores.some((r) => r.status === 'queued' || r.status === 'running');
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
                  <Show when={d().config?.retention}><span>newest <b>{formatCount(d().config!.retention)}</b> kept</span></Show>
                </div>
              </FxCell>
              <FxCell label="Stored in" meta={<span>{d().linked ? 'Sp00ky Cloud' : 'bucket listing'}</span>}>
                <div class="fx-big bk-bucket">{d().s3.bucket ?? '—'}</div>
                <div class="fx-sub cap"><span>{d().s3.endpoint ?? 'no endpoint'}</span></div>
              </FxCell>
            </section>

            {/* how they have gone, and when they run ========================== */}
            <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
              <FxCell label="History" meta={<span>size of each backup, oldest left</span>}>
                <History catalog={d().catalog} />
              </FxCell>
              <SchedulePanel linked={d().linked} config={d().config} onSaved={refreshAll} />
            </section>

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
                            <td><span class="fx-who"><Sq tone={r.status === 'failed' ? 'bad' : r.status === 'queued' || r.status === 'running' ? 'warn' : ''} /><span style={{ color: 'var(--ink)' }}>{r.backup_id}</span></span></td>
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

function SchedulePanel(props: {
  linked: boolean;
  config: BackupConfig | null;
  onSaved: () => void;
}) {
  const [enabled, setEnabled] = createSignal(props.config?.enabled ?? false);
  const [schedule, setSchedule] = createSignal(props.config?.schedule ?? '');
  const [retention, setRetention] = createSignal(
    props.config?.retention != null ? String(props.config.retention) : '',
  );
  const [saving, setSaving] = createSignal(false);

  const dirty = () =>
    enabled() !== (props.config?.enabled ?? false) ||
    schedule().trim() !== (props.config?.schedule ?? '') ||
    (retention().trim() || null) !==
      (props.config?.retention != null ? String(props.config.retention) : null);

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
      const r = retention().trim();
      if (r) {
        const n = Number(r);
        if (!Number.isInteger(n) || n < 0) {
          toast('bad', 'Retention must be a whole number');
          return;
        }
        body.retention = n;
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
            <span>Keep</span>
            <input value={retention()} placeholder="7" inputmode="numeric" onInput={(e) => setRetention(e.currentTarget.value)} />
            <small class="cap">newest backups · empty or 0 keeps all</small>
          </label>
          <button class="btn btn-sm btn-primary bu-save" disabled={!dirty() || saving()} onClick={() => void save()}>
            {saving() ? 'Saving…' : 'Save'}
          </button>
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
