import { For, Show, createResource, createSignal, onCleanup } from 'solid-js';
import { api } from '../api/client';
import { Empty, PageHead, Panel, StatCard } from '../components/Chrome';
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
  RestoreJobState,
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

function catalogTone(status: CatalogEntry['status']) {
  switch (status) {
    case 'completed':
      return 'ok';
    case 'pending':
    case 'in_progress':
      return 'warn';
    case 'failed':
      return 'bad';
    default:
      return 'idle';
  }
}

function restoreTone(status: RestoreJobState['status']) {
  switch (status) {
    case 'completed':
      return 'ok';
    case 'queued':
    case 'running':
      return 'warn';
    case 'failed':
      return 'bad';
    default:
      return 'idle';
  }
}

const ArchiveIcon = () => (
  <svg viewBox="0 0 16 16">
    <path d="M2.5 3.5h11v3h-11zM3.5 6.5v6h9v-6M6.5 9h3" />
  </svg>
);

const RestoreIcon = () => (
  <svg viewBox="0 0 16 16">
    <path d="M3 8a5 5 0 1 0 1.5-3.6M3 2.5v2.2h2.2" />
  </svg>
);

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

  return (
    <>
      <PageHead
        title="Backups"
        subtitle={
          <Show when={data()?.s3.bucket}>
            {(b) => (
              <>
                Stored in <span class="mono">{b()}</span>
              </>
            )}
          </Show>
        }
        actions={
          <Show when={data()}>
            <div class="bk-create">
              <input
                class="bk-name"
                placeholder="Name (optional)"
                value={name()}
                disabled={creating() || !configured()}
                onInput={(e) => setName(e.currentTarget.value)}
                onKeyDown={(e) => e.key === 'Enter' && void create()}
              />
              <button
                class="btn btn-primary"
                disabled={creating() || !configured()}
                title={configured() ? undefined : 'No object storage configured'}
                onClick={() => void create()}
              >
                Back up now
              </button>
            </div>
          </Show>
        }
      />

      <div class="page-body">
        <div class="page-stack">
          <ActivityStrip
            operations={props.overview?.operations}
            ssps={props.overview?.ssps}
            exclude={(op) => op.kind === 'backup_restore' && op.detail?.restore_id === watching()?.id}
          />

          <Show when={error()}>
            <div class="panel">
              <Empty>{error()}</Empty>
            </div>
          </Show>

          <Show when={!error() && data()} fallback={<Show when={!error()}><Empty>Loading…</Empty></Show>}>
            {(d) => (
                <>
                  <Show when={!d().s3.configured}>
                    <div class="banner">
                      <span class="dot bad" />
                      No object storage. Set <span class="mono">S3_ENDPOINT</span>,{' '}
                      <span class="mono">S3_ACCESS_KEY</span>, <span class="mono">S3_SECRET_KEY</span> and{' '}
                      <span class="mono">S3_BUCKET</span> on the scheduler.
                    </div>
                  </Show>

                  <div class="stat-grid">
                    <StatCard
                      label="Last backup"
                      value={lastValue()}
                      tone={latest() ? undefined : 'warn'}
                      note={
                        d().local.current_running
                          ? `backing up now, ${elapsed(
                              d().local.current_running!.started_at ?? d().local.current_running!.enqueued_at,
                              null,
                            )}`
                          : latest()
                            ? formatBytes(latest()!.size_bytes)
                            : 'none completed yet'
                      }
                    />
                    <StatCard
                      label="Kept"
                      value={formatCount(completed().length)}
                      unit={completed().length === 1 ? 'backup' : 'backups'}
                      note={
                        formatBytes(totalBytes()) +
                        (d().config?.retention ? `, newest ${formatCount(d().config!.retention)} kept` : '')
                      }
                    />
                    <StatCard
                      label="Next backup"
                      value={
                        d().config?.enabled && d().config?.next_run_at
                          ? relativeStamp(d().config!.next_run_at)
                          : 'off'
                      }
                      note={
                        d().config
                          ? d().config!.enabled && d().config!.schedule
                            ? (describeCron(d().config!.schedule!) ?? d().config!.schedule!)
                            : 'no schedule'
                          : 'scheduled by Sp00ky Cloud'
                      }
                    />
                  </div>

                  <Show when={watching()} keyed>
                    {(w) => (
                      <RestoreProgress
                        id={w.id}
                        backupId={w.backupId}
                        label={w.label}
                        sub={w.sub}
                        onDone={() => {
                          refreshAll();
                        }}
                        onClose={() => setWatching(null)}
                      />
                    )}
                  </Show>

                  <Panel title="Backups" flush>
                    <Show when={d().catalog.length > 0} fallback={<Empty>No backups yet.</Empty>}>
                      <ul class="list">
                        <For each={d().catalog}>
                          {(c) => {
                            const size = () => c.local?.size_bytes ?? c.size_bytes;
                            const live = () => c.status === 'pending' || c.status === 'in_progress';
                            const state = () =>
                              c.status === 'completed'
                                ? null
                                : c.local && live()
                                  ? c.local.status
                                  : c.status.replace('_', ' ');
                            return (
                              <li class="list-item bk-row" classList={{ [catalogTone(c.status)]: true }}>
                                <span class="list-icon" aria-hidden="true">
                                  <ArchiveIcon />
                                </span>
                                <span class="list-text">
                                  <span class="list-title">
                                    {c.name ?? c.id}
                                    <Show when={state()}>
                                      <span class="bk-state" classList={{ [catalogTone(c.status)]: true }}>
                                        {state()}
                                      </span>
                                    </Show>
                                  </span>
                                  <Show
                                    when={c.error}
                                    fallback={
                                      <span class="list-detail">
                                        <Show when={c.name}>
                                          <span class="mono">{c.id}</span>
                                          {size() ? ' · ' : ''}
                                        </Show>
                                        {size() ? formatBytes(size()!) : ''}
                                      </span>
                                    }
                                  >
                                    <span class="list-detail bk-error" title={c.error!}>
                                      {c.error}
                                    </span>
                                  </Show>
                                </span>
                                <span class="list-meta" title={formatStamp(c.created_at)}>
                                  {relativeStamp(c.created_at)}
                                </span>
                                <span class="bk-actions">
                                  <Show when={c.status === 'completed'}>
                                    <button
                                      class="btn btn-sm"
                                      title={restoreBlocked()}
                                      disabled={!!restoreBlocked()}
                                      onClick={() => void restore(c)}
                                    >
                                      Restore
                                    </button>
                                    <Show when={d().linked}>
                                      <button class="btn btn-sm" onClick={() => void remove(c)}>
                                        Delete
                                      </button>
                                    </Show>
                                  </Show>
                                </span>
                              </li>
                            );
                          }}
                        </For>
                      </ul>
                    </Show>
                  </Panel>

                  {/* In memory on this scheduler only, so usually empty. */}
                  <Show when={d().restores.length > 0}>
                    <Panel title="Restores" flush>
                      <ul class="list">
                        <For each={d().restores}>
                          {(r) => (
                            <li>
                              <button
                                class="list-item bk-restore"
                                classList={{ [restoreTone(r.status)]: true }}
                                onClick={() => setWatching({ id: r.restore_id, backupId: r.backup_id })}
                              >
                                <span class="list-icon" aria-hidden="true">
                                  <RestoreIcon />
                                </span>
                                <span class="list-text">
                                  <span class="list-title">
                                    From <span class="mono">{r.backup_id}</span>
                                  </span>
                                  <span class="list-detail">
                                    {r.status}, took {elapsed(r.started_at ?? r.enqueued_at, r.finished_at)}
                                  </span>
                                </span>
                                <span class="list-meta">{relativeStamp(r.started_at ?? r.enqueued_at)}</span>
                              </button>
                            </li>
                          )}
                        </For>
                      </ul>
                    </Panel>
                  </Show>

                  <SchedulePanel linked={d().linked} config={d().config} onSaved={refreshAll} />
                </>
            )}
          </Show>
        </div>
      </div>
    </>
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
    <Panel title="Schedule">
      <Show
        when={props.linked}
        fallback={
          <div class="bk-unlinked">
            Scheduled backups need a project linked to Sp00ky Cloud, then{' '}
            <span class="mono">spky backup configure</span>.
          </div>
        }
      >
        <div class="bk-form">
          <label class="switch">
            <input
              type="checkbox"
              checked={enabled()}
              onChange={(e) => setEnabled(e.currentTarget.checked)}
            />
            <span class="switch-track" />
            <span>{enabled() ? 'Scheduled backups on' : 'Scheduled backups off'}</span>
          </label>

          <div class="bk-fields">
            <label>
              <span class="tag field-label">Cron, UTC</span>
              <input
                class="mono"
                value={schedule()}
                placeholder="0 3 * * *"
                spellcheck={false}
                onInput={(e) => setSchedule(e.currentTarget.value)}
              />
              <span class="field-hint">
                <Show when={preview()} fallback="minute hour day month weekday">
                  {(p) => <>{p()}</>}
                </Show>
              </span>
            </label>

            <label>
              <span class="tag field-label">Keep</span>
              <input
                value={retention()}
                placeholder="7"
                inputmode="numeric"
                onInput={(e) => setRetention(e.currentTarget.value)}
              />
              <span class="field-hint">newest backups, empty or 0 keeps all</span>
            </label>
          </div>

          <div class="bk-form-foot">
            <span class="field-hint">
              <Show when={props.config?.last_scheduled_at && !isAbsent(props.config?.last_scheduled_at)}>
                Last scheduled run {relativeStamp(props.config!.last_scheduled_at)}
              </Show>
            </span>
            <button
              class="btn btn-primary"
              disabled={!dirty() || saving()}
              onClick={() => void save()}
            >
              Save
            </button>
          </div>
        </div>
      </Show>
    </Panel>
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
    <Show when={!err()} fallback={<div class="panel"><Empty>{err()}</Empty></div>}>
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
