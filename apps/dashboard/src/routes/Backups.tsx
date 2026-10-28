import { For, Show, createResource, createSignal, onCleanup } from 'solid-js';
import { api } from '../api/client';
import {
  Cell,
  Empty,
  PageHead,
  Panel,
  Pill,
  Rail,
  StatusDot,
} from '../components/Chrome';
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
 * Backups.
 *
 * Two sources of truth, deliberately shown as one list. The scheduler EXECUTES
 * backups (export, gzip, upload) and remembers the last fifty in memory. The
 * catalog, the schedule and retention live in Sp00ky Cloud when the scheduler
 * is linked to it, or in a plain bucket listing when it is not. The API joins
 * the two by id, which is what lets an in-flight backup show its size filling
 * in before the control plane has heard about it.
 *
 * A restore is the one destructive action on the whole dashboard that cannot
 * be undone by another click, so it gets the fullest treatment: consequences
 * up front, a typed word, then a step card fed by the scheduler's own stage
 * flags rather than a spinner.
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

  return (
    <>
      <PageHead
        crumb="Dashboard"
        title="Backups"
        subtitle={
          <Show when={data()}>
            {(d) => (
              <>
                {d().linked ? 'catalog from Sp00ky Cloud' : 'catalog from bucket listing'}
                {d().s3.bucket ? ` · ${d().s3.bucket}` : ''}
              </>
            )}
          </Show>
        }
        actions={
          <Show when={data()}>
            {(d) => (
              <Pill tone={d().s3.configured ? 'ok' : 'bad'} dot>
                {d().s3.configured ? 'storage configured' : 'no storage'}
              </Pill>
            )}
          </Show>
        }
      />

      <div class="page-body">
        <ActivityStrip
          operations={props.overview?.operations}
          ssps={props.overview?.ssps}
          exclude={(op) => op.kind === 'backup_restore' && op.detail?.restore_id === watching()?.id}
        />

        <Show
          when={!error()}
          fallback={
            <div class="panel">
              <Empty>{error()}</Empty>
            </div>
          }
        >
          <Show when={data()} fallback={<Empty>Loading…</Empty>}>
            {(d) => (
              <div class="stack">
                <Rail>
                  <Cell
                    label="Last backup"
                    tone={latest() ? 'ok' : 'idle'}
                    value={latest() ? relativeStamp(latest()!.completed_at ?? latest()!.created_at) : 'never'}
                    foot={latest() ? formatBytes(latest()!.size_bytes) : 'no completed backup'}
                  />
                  <Cell
                    label="Kept"
                    value={formatCount(completed().length)}
                    foot={`${formatBytes(totalBytes())} in storage`}
                  />
                  <Cell
                    label="Next scheduled"
                    tone={d().config?.enabled ? 'ok' : 'idle'}
                    value={
                      d().config?.enabled && d().config?.next_run_at
                        ? relativeStamp(d().config!.next_run_at)
                        : 'none'
                    }
                    foot={
                      d().config
                        ? d().config!.enabled
                          ? (d().config!.schedule ? describeCron(d().config!.schedule!) ?? d().config!.schedule : 'no cron set')
                          : 'schedule off'
                        : 'not linked'
                    }
                  />
                  <Cell
                    label="Retention"
                    value={d().config?.retention ? formatCount(d().config!.retention) : '—'}
                    foot={d().config?.retention ? 'most recent kept' : 'not enforced here'}
                  />
                </Rail>

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

                <div class="grid grid-2">
                  <Panel
                    title="Create backup"
                    sub="A SurrealDB export, gzipped, uploaded to the bucket. The scheduler keeps serving."
                  >
                    <Show
                      when={d().s3.configured}
                      fallback={
                        <div class="callout">
                          No object storage configured. The scheduler needs{' '}
                          <code>S3_ENDPOINT</code>, <code>S3_ACCESS_KEY</code>,{' '}
                          <code>S3_SECRET_KEY</code> and <code>S3_BUCKET</code> in its
                          environment.
                        </div>
                      }
                    >
                      <div class="form-row">
                        <input
                          placeholder="Name (optional)"
                          value={name()}
                          disabled={creating()}
                          onInput={(e) => setName(e.currentTarget.value)}
                          onKeyDown={(e) => e.key === 'Enter' && void create()}
                        />
                        <button
                          class="btn btn-primary"
                          disabled={creating()}
                          onClick={() => void create()}
                        >
                          Back up now
                        </button>
                      </div>
                      <Show when={d().local.current_running}>
                        {(job) => (
                          <div style={{ 'margin-top': '12px' }}>
                            <div class="spread faint" style={{ 'font-size': '11.5px', 'margin-bottom': '6px' }}>
                              <span>
                                running · {job().backup_id}
                              </span>
                              <span>{elapsed(job().started_at ?? job().enqueued_at, null)}</span>
                            </div>
                            <div class="bar">
                              <div class="bar-fill indeterminate" />
                            </div>
                          </div>
                        )}
                      </Show>
                      <Show when={d().local.queue_len > 0}>
                        <div class="field-hint">
                          {d().local.queue_len} queued behind it
                        </div>
                      </Show>
                    </Show>
                  </Panel>

                  <SchedulePanel
                    linked={d().linked}
                    config={d().config}
                    onSaved={refreshAll}
                  />
                </div>

                <Panel
                  title="Catalog"
                  sub={
                    d().linked
                      ? 'Newest first. Deleted backups are hidden.'
                      : 'Newest first, from the bucket. Sp00ky Cloud tracks names and retention; a bucket listing has neither.'
                  }
                  flush
                >
                  <Show
                    when={d().catalog.length > 0}
                    fallback={<Empty>No backups yet.</Empty>}
                  >
                    <div class="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>Backup</th>
                            <th>Status</th>
                            <th>Size</th>
                            <th>Snapshot</th>
                            <th>Created</th>
                            <th />
                          </tr>
                        </thead>
                        <tbody>
                          <For each={d().catalog}>
                            {(c) => {
                              const size = () => c.local?.size_bytes ?? c.size_bytes;
                              const seq = () => c.local?.snapshot_seq ?? c.snapshot_seq;
                              const live = () => c.status === 'pending' || c.status === 'in_progress';
                              return (
                                <tr>
                                  <td>
                                    <div class="row">
                                      <StatusDot tone={catalogTone(c.status)} pulse={live()} />
                                      <span>{c.name ?? c.id}</span>
                                    </div>
                                    <Show when={c.name}>
                                      <div class="id" style={{ 'margin-top': '2px' }}>{c.id}</div>
                                    </Show>
                                    <Show when={c.error}>
                                      <div class="id" style={{ 'margin-top': '2px', color: 'var(--bad)' }}>
                                        {c.error}
                                      </div>
                                    </Show>
                                  </td>
                                  <td data-label="Status">
                                    <Pill tone={catalogTone(c.status)}>
                                      {c.local && live() ? c.local.status : c.status.replace('_', ' ')}
                                    </Pill>
                                  </td>
                                  <td class="dim" data-label="Size">
                                    {size() ? formatBytes(size()!) : '—'}
                                  </td>
                                  <td class="ghost" data-label="Snapshot" data-empty={seq() == null}>
                                    {seq() != null ? formatCount(seq()) : '—'}
                                  </td>
                                  <td class="dim" data-label="Created" title={formatStamp(c.created_at)}>
                                    {relativeStamp(c.created_at)}
                                  </td>
                                  <td data-label="Actions" data-empty={c.status !== 'completed'}>
                                    <div class="row" style={{ 'justify-content': 'flex-end' }}>
                                      <Show when={c.status === 'completed'}>
                                        <button
                                          class="btn btn-sm"
                                          title={
                                            d().scheduler_status !== 'ready'
                                              ? `Scheduler is ${d().scheduler_status}; restore needs ready`
                                              : undefined
                                          }
                                          disabled={d().scheduler_status !== 'ready'}
                                          onClick={() => void restore(c)}
                                        >
                                          Restore
                                        </button>
                                        <Show when={d().linked}>
                                          <button
                                            class="btn btn-sm"
                                            onClick={() => void remove(c)}
                                          >
                                            Delete
                                          </button>
                                        </Show>
                                      </Show>
                                    </div>
                                  </td>
                                </tr>
                              );
                            }}
                          </For>
                        </tbody>
                      </table>
                    </div>
                  </Show>
                </Panel>

                <Panel
                  title="Restore history"
                  sub="What this scheduler process has restored. Kept in memory only, so it starts empty after a restart."
                  flush
                >
                  <Show
                    when={d().restores.length > 0}
                    fallback={<Empty>No restores since this scheduler started.</Empty>}
                  >
                    <div class="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>Restore</th>
                            <th>Status</th>
                            <th>From</th>
                            <th>Stages</th>
                            <th>Started</th>
                            <th>Duration</th>
                          </tr>
                        </thead>
                        <tbody>
                          <For each={d().restores}>
                            {(r) => (
                              <tr
                                class="clickable"
                                onClick={() => setWatching({ id: r.restore_id, backupId: r.backup_id })}
                              >
                                <td>
                                  <div class="row">
                                    <StatusDot
                                      tone={restoreTone(r.status)}
                                      pulse={r.status === 'running'}
                                    />
                                    <span class="id">{r.restore_id}</span>
                                  </div>
                                </td>
                                <td data-label="Status">
                                  <Pill tone={restoreTone(r.status)}>{r.status}</Pill>
                                </td>
                                <td class="ghost" data-label="From">{r.backup_id}</td>
                                <td class="ghost" data-label="Stages">
                                  {[
                                    r.main_db_restored ? 'db' : null,
                                    r.replica_restored ? 'replica' : null,
                                    r.ssps_evicted != null ? `${r.ssps_evicted} ssp` : null,
                                  ]
                                    .filter(Boolean)
                                    .join(' · ') || '—'}
                                </td>
                                <td class="dim" data-label="Started">
                                  {relativeStamp(r.started_at ?? r.enqueued_at)}
                                </td>
                                <td class="dim" data-label="Duration">
                                  {elapsed(r.started_at ?? r.enqueued_at, r.finished_at)}
                                </td>
                              </tr>
                            )}
                          </For>
                        </tbody>
                      </table>
                    </div>
                  </Show>
                </Panel>
              </div>
            )}
          </Show>
        </Show>
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
    <Panel
      title="Schedule"
      sub={
        props.linked
          ? 'Run by Sp00ky Cloud on this cron, in UTC. Retention keeps the newest N completed backups.'
          : 'Scheduled backups are run by Sp00ky Cloud.'
      }
    >
      <Show
        when={props.linked}
        fallback={
          <div class="callout">
            This scheduler is not linked to Sp00ky Cloud, so there is no schedule to
            show or edit. From a linked project:{' '}
            <code>spky backup configure --enabled true --schedule "0 3 * * *" --retention 7</code>
          </div>
        }
      >
        <div class="stack" style={{ gap: '12px' }}>
          <label class="switch">
            <input
              type="checkbox"
              checked={enabled()}
              onChange={(e) => setEnabled(e.currentTarget.checked)}
            />
            <span class="switch-track" />
            <span>{enabled() ? 'Scheduled backups on' : 'Scheduled backups off'}</span>
          </label>

          <div>
            <span class="tag field-label">Cron (UTC)</span>
            <input
              value={schedule()}
              placeholder="0 3 * * *"
              spellcheck={false}
              onInput={(e) => setSchedule(e.currentTarget.value)}
            />
            <div class="field-hint">
              <Show when={preview()} fallback="Five fields: minute hour day month weekday">
                {(p) => <>{p()}</>}
              </Show>
              <Show when={props.config?.next_run_at && props.config?.enabled}>
                {' · next '}
                {relativeStamp(props.config!.next_run_at)}
              </Show>
            </div>
          </div>

          <div>
            <span class="tag field-label">Retention (backups kept)</span>
            <input
              value={retention()}
              placeholder="7"
              inputmode="numeric"
              onInput={(e) => setRetention(e.currentTarget.value)}
            />
            <div class="field-hint">
              Older completed backups are deleted from storage. Empty or 0 keeps everything.
            </div>
          </div>

          <div class="spread">
            <span class="faint" style={{ 'font-size': '11px' }}>
              <Show when={props.config?.last_scheduled_at && !isAbsent(props.config?.last_scheduled_at)}>
                last scheduled run {relativeStamp(props.config!.last_scheduled_at)}
              </Show>
            </span>
            <button
              class="btn btn-primary"
              disabled={!dirty() || saving()}
              onClick={() => void save()}
            >
              Save schedule
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
