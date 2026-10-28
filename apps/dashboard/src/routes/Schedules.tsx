import { For, Show, createResource, onCleanup } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Chevron,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Reason,
  ReasonLine,
  StatCard,
  StatusDot,
} from '../components/Chrome';
import { post, runAction } from '../components/Actions';
import { describeCron } from '../lib/cron';
import {
  elapsed,
  formatCount,
  formatDuration,
  formatStamp,
  orNull,
  relativeStamp,
} from '../lib/format';
import { runTone } from '../lib/status';
import { pauseSchedule, triggerSchedule } from '../lib/runActions';
import type { Schedule, ScheduleDetailData, ScheduleRun } from '../api/types';

/**
 * Two ways a schedule can be off, and they mean different things: `paused` is
 * an operator decision, `config_disabled` came from the config file and will
 * come back on the next `spky schedules sync`.
 */
function scheduleState(s: Pick<Schedule, 'paused' | 'config_disabled'>) {
  if (s.config_disabled) return { label: 'disabled', tone: 'idle' };
  if (s.paused) return { label: 'paused', tone: 'warn' };
  return { label: 'active', tone: 'ok' };
}

/** An interval in whole units where it has one: "hour", "15 minutes". */
function every(ms: number) {
  for (const [unit, size] of [
    ['day', 86_400_000],
    ['hour', 3_600_000],
    ['minute', 60_000],
    ['second', 1000],
  ] as const) {
    if (ms % size === 0) {
      const n = ms / size;
      return n === 1 ? unit : `${n} ${unit}s`;
    }
  }
  return formatDuration(ms);
}

/**
 * The cadence in words: "Daily at 06:00 UTC", "Every 15 minutes". A cron the
 * reader cannot phrase is shown as written, since a wrong guess is worse.
 */
function cadence(s: Pick<Schedule, 'cron' | 'every_ms' | 'timezone'>) {
  if (s.every_ms) return `Every ${every(s.every_ms)}`;
  if (!s.cron) return '—';
  const words = describeCron(s.cron);
  if (!words) return s.cron;
  const tz = s.timezone && s.timezone !== 'UTC' ? s.timezone : 'UTC';
  const text = words.replace(/^hourly at minute 0$/, 'every hour').replace(/UTC$/, tz);
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** The row's tone: a failed last run outranks being paused. */
function rowTone(s: Schedule): 'ok' | 'warn' | 'bad' | 'idle' {
  if (s.last_run_status && runTone(s.last_run_status) === 'bad') return 'bad';
  if (s.paused) return 'warn';
  if (s.config_disabled) return 'idle';
  return s.last_run_status ? 'ok' : 'idle';
}

/**
 * Pause/resume and "run now" for one schedule.
 *
 * `config_disabled` is not an operator state: it came from the config file
 * and only `spky schedules sync` can change it, so both controls are off with
 * that reason rather than offering a toggle that would silently do nothing.
 */
function ScheduleControls(props: {
  schedule: Pick<Schedule, 'name' | 'paused' | 'config_disabled'>;
  onChange: () => void;
}) {
  const s = () => props.schedule;
  const disabledReason = () =>
    s().config_disabled
      ? 'Disabled in config; change it there and run spky schedules sync'
      : undefined;
  const triggerReason = () =>
    disabledReason() ?? (s().paused ? 'Paused: pause wins over a queued trigger' : undefined);
  return (
    <div class="row" onClick={(e) => e.stopPropagation()}>
      <button
        class="btn btn-sm"
        disabled={!!disabledReason()}
        title={disabledReason()}
        onClick={() => void pauseSchedule(s().name, !s().paused, props.onChange)}
      >
        {s().paused ? 'Resume' : 'Pause'}
      </button>
      <button
        class="btn btn-sm btn-primary"
        disabled={!!triggerReason()}
        title={triggerReason()}
        onClick={() => void triggerSchedule(s().name, props.onChange)}
      >
        Run now
      </button>
    </div>
  );
}

const ClockIcon = () => (
  <svg viewBox="0 0 16 16">
    <path d="M8 1.8a6.2 6.2 0 1 0 0 12.4A6.2 6.2 0 0 0 8 1.8zM8 4.6V8l2.4 1.5" />
  </svg>
);

/**
 * Every schedule, one line each: what it is, when it runs in words, when it
 * runs next, and how its last run went. The row opens the schedule; its
 * controls sit beside the link rather than inside it.
 */
export function Schedules() {
  const [data, { refetch }] = createResource(() =>
    api.get<{ schedules: Schedule[] }>('/schedules'),
  );

  const timer = setInterval(refetch, 5000);
  onCleanup(() => clearInterval(timer));

  const summary = () => {
    const all = data()?.schedules ?? [];
    if (!all.length) return undefined;
    const paused = all.filter((s) => s.paused).length;
    const failing = all.filter((s) => rowTone(s) === 'bad').length;
    return [
      `${all.length} schedule${all.length === 1 ? '' : 's'}`,
      paused ? `${paused} paused` : null,
      failing ? `${failing} failing` : null,
    ]
      .filter(Boolean)
      .join(', ');
  };

  return (
    <>
      <PageHead title="Schedules" subtitle={summary()} />

      <div class="page-body">
        <Show when={data()} fallback={<Empty>Loading…</Empty>}>
          {(d) => (
            <Show
              when={d().schedules.length > 0}
              fallback={
                <div class="panel">
                  <Empty>
                    No schedules yet. Declare them in your config and run{' '}
                    <span class="mono">spky schedules sync</span>.
                  </Empty>
                </div>
              }
            >
              <ul class="list sch-list">
                <For each={d().schedules}>
                  {(s) => (
                    <li class="sch-row">
                      <A
                        href={`/schedules/${encodeURIComponent(s.name)}`}
                        class="list-item sch-link"
                        classList={{ [rowTone(s)]: true }}
                      >
                        <span class="list-icon" aria-hidden="true">
                          <ClockIcon />
                        </span>
                        <span class="list-text">
                          <span class="list-title">{s.name}</span>
                          <span class="list-detail">
                            {cadence(s)}
                            <Show when={s.kind === 'workflow'}> · workflow</Show>
                          </span>
                        </span>
                        <span class="sch-col">
                          <span class="sch-col-label">Next</span>
                          <Show
                            when={!s.paused && !s.config_disabled}
                            fallback={<span class="sch-col-value dim">{scheduleState(s).label}</span>}
                          >
                            <span class="sch-col-value" title={formatStamp(s.next_fire_at)}>
                              {relativeStamp(s.next_fire_at)}
                            </span>
                          </Show>
                        </span>
                        <span class="sch-col sch-last">
                          <span class="sch-col-label">Last run</span>
                          <Show
                            when={s.last_run_status}
                            fallback={<span class="sch-col-value dim">never</span>}
                          >
                            <span class="sch-col-value" title={formatStamp(s.last_run_at)}>
                              <StatusDot tone={runTone(s.last_run_status!)} />
                              {s.last_run_status} {relativeStamp(s.last_run_at)}
                            </span>
                          </Show>
                        </span>
                        <Chevron />
                      </A>
                      <div class="sch-actions">
                        <ScheduleControls schedule={s} onChange={refetch} />
                      </div>
                    </li>
                  )}
                </For>
              </ul>
            </Show>
          )}
        </Show>
      </div>
    </>
  );
}

/** A forEach key the gate has quarantined after repeated failures. */
interface QuarantinedKey {
  key: string;
  consecutive_failures: number | null;
  quarantined_at: string | null;
}

/**
 * Release one quarantined key: the scheduler forgets its failure streak, so
 * the next fire runs it again and closes its incident.
 */
function releaseKey(name: string, key: string, after?: () => void) {
  return runAction<{ released: boolean }>({
    label: `Release ${key}`,
    confirm: {
      title: `Release ${key} from quarantine?`,
      verb: 'Release',
      consequences: [
        'Its failure streak is forgotten and the next fire runs it again.',
        'If the cause is not fixed, it fails and is quarantined again.',
      ],
    },
    request: post(`/schedules/${encodeURIComponent(name)}/release`, { key }),
    success: () => 'Released. It runs on the next fire.',
    after,
  });
}

/**
 * One schedule: when it fires, how its fires went, and what is holding it.
 *
 * The success and failure tallies come from `_00_run_rollup`, not from the
 * run rows: rows are pruned by retention and would quietly under-report.
 */
export function ScheduleDetail() {
  const params = useParams<{ name: string }>();
  const name = () => decodeURIComponent(params.name);
  const navigate = useNavigate();

  const [result, { refetch }] = createResource(name, (n) =>
    api.getResult<ScheduleDetailData & { quarantined?: QuarantinedKey[] }>(
      `/schedules/${encodeURIComponent(n)}`,
    ),
  );

  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  const timer = setInterval(refetch, 5000);
  onCleanup(() => clearInterval(timer));

  const totals = () => {
    const rollup = data()?.rollup ?? [];
    return rollup.reduce(
      (acc, b) => ({
        success: acc.success + (b.success ?? 0),
        failed: acc.failed + (b.failed ?? 0),
        skipped: acc.skipped + (b.skipped ?? 0),
      }),
      { success: 0, failed: 0, skipped: 0 },
    );
  };

  const s = () => data()?.schedule;
  const quarantined = () => data()?.quarantined ?? [];

  /**
   * Fires this schedule is suppressing, and what holds them.
   *
   * A wall of `skipped` rows reads as "nothing to do" and means "something is
   * still running": with `concurrency: skip` one wedged run suppresses every
   * later fire of its key, so the blocking run is what needs acting on.
   */
  const suppressed = () => {
    const skips = (data()?.runs ?? []).filter(
      (r) => r.status === 'skipped' && r.error?.code === 'concurrency_skip',
    );
    if (!skips.length) return null;
    return { skips, latest: skips[0] };
  };

  const blockingRun = (r: ScheduleRun): string | null =>
    orNull((r.error?.blocked_by_workflow_run as string | undefined) ?? null);

  /** The definition, minus the fields this schedule does not use. */
  const definition = (v: Schedule): [string, string][] =>
    (
      [
        ['Runs', v.cron ? `${cadence(v)} (${v.cron})` : cadence(v)],
        ['Target', v.path ?? v.target_table],
        ['For each', v.for_each],
        ['Concurrency', v.concurrency],
        ['Timeout', v.timeout ? formatDuration(v.timeout * 1000) : null],
        [
          'Retries',
          v.max_retries != null ? `${v.max_retries}, ${v.retry_strategy ?? 'default'}` : null,
        ],
      ] as [string, string | null][]
    ).filter((row): row is [string, string] => !!orNull(row[1]));

  const showKey = () => (data()?.runs ?? []).some((r) => !!r.key);

  return (
    <>
      <PageHead
        title={name()}
        subtitle={<Show when={s()}>{(v) => <>{cadence(v())}</>}</Show>}
        actions={
          <Show when={s()}>
            {(v) => (
              <>
                <Pill tone={scheduleState(v()).tone} dot>
                  {scheduleState(v()).label}
                </Pill>
                <ScheduleControls schedule={v()} onChange={refetch} />
              </>
            )}
          </Show>
        }
      />

      <div class="page-body">
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
              <div class="page-stack">
                <div class="stat-grid">
                  <StatCard
                    label="Next run"
                    value={
                      d().schedule.paused || d().schedule.config_disabled
                        ? scheduleState(d().schedule).label
                        : relativeStamp(d().schedule.next_fire_at)
                    }
                    note={
                      d().schedule.paused || d().schedule.config_disabled
                        ? undefined
                        : formatStamp(d().schedule.next_fire_at)
                    }
                  />
                  <StatCard
                    label="Last run"
                    value={d().schedule.last_run_status ?? 'never'}
                    tone={
                      d().schedule.last_run_status
                        ? runTone(d().schedule.last_run_status!)
                        : undefined
                    }
                    note={relativeStamp(d().schedule.last_run_at)}
                  />
                  <StatCard label="Succeeded" value={formatCount(totals().success)} />
                  <StatCard
                    label="Failed"
                    value={formatCount(totals().failed)}
                    tone={totals().failed > 0 ? 'bad' : undefined}
                    note={totals().skipped > 0 ? `${formatCount(totals().skipped)} skipped` : undefined}
                  />
                </div>

                <Show when={quarantined().length > 0}>
                  <Panel
                    title="Quarantined keys"
                    sub="These keys failed too often in a row and are skipped until released."
                    flush
                  >
                    <ul class="list">
                      <For each={quarantined()}>
                        {(q) => (
                          <li class="list-item bad">
                            <span class="list-text">
                              <span class="list-title mono">{q.key}</span>
                              <span class="list-detail">
                                {q.consecutive_failures != null
                                  ? `${formatCount(q.consecutive_failures)} failures in a row, `
                                  : ''}
                                quarantined {relativeStamp(q.quarantined_at)}
                              </span>
                            </span>
                            <button
                              class="btn btn-sm"
                              onClick={() => void releaseKey(name(), q.key, refetch)}
                            >
                              Release
                            </button>
                          </li>
                        )}
                      </For>
                    </ul>
                  </Panel>
                </Show>

                <Show when={suppressed()}>
                  {(sup) => (
                    <Panel
                      title="Fires are being skipped"
                      sub={`${sup().skips.length} of the last ${d().runs.length} fires, because a run for the same key is still going.`}
                    >
                      <Reason
                        error={sup().latest.error}
                        tone="warn"
                        link={(key, value) =>
                          key === 'blocked_by_workflow_run' ? (
                            <A
                              href={`/workflows/${encodeURIComponent(value)}`}
                              style={{ 'text-decoration': 'underline' }}
                            >
                              {value}
                            </A>
                          ) : null
                        }
                      />
                    </Panel>
                  )}
                </Show>

                {/* `_00_schedule.last_error` is a bare string: why the
                    schedule could not be planned or fanned out at all, before
                    any run row existed to carry a structured cause. */}
                <Show when={d().schedule.last_error}>
                  <Panel title="Could not fire">
                    <Reason error={d().schedule.last_error} />
                  </Panel>
                </Show>

                <Panel title="Recent fires" flush>
                  <Show when={d().runs.length > 0} fallback={<Empty>No fires yet.</Empty>}>
                    <div class="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>Fired</th>
                            <th>Status</th>
                            <th>Duration</th>
                            <th>Why</th>
                            <Show when={showKey()}>
                              <th>Key</th>
                            </Show>
                            <th />
                          </tr>
                        </thead>
                        <tbody>
                          <For each={d().runs}>
                            {(r: ScheduleRun) => {
                              // `type::string(NONE)` is the string "NONE", so a
                              // fire that produced nothing is filtered here or
                              // every row grows a dead link.
                              const runRef = () => orNull(r.workflow_run);
                              const jobRef = () => orNull(r.job_id);
                              return (
                                <tr
                                  class={runRef() || jobRef() ? 'clickable' : undefined}
                                  onClick={() =>
                                    runRef()
                                      ? navigate(`/workflows/${encodeURIComponent(runRef()!)}`)
                                      : jobRef() && navigate(`/jobs/${encodeURIComponent(jobRef()!)}`)
                                  }
                                >
                                  <td title={formatStamp(r.fire_at ?? r.created_at)}>
                                    {relativeStamp(r.fire_at ?? r.created_at)}
                                  </td>
                                  <td data-label="Status">
                                    <Pill tone={runTone(r.status)}>{r.status}</Pill>
                                  </td>
                                  <td class="dim" data-label="Duration">
                                    {elapsed(r.created_at, r.finished_at)}
                                  </td>
                                  {/* Skips and failures say why; a fire by hand
                                      is the only other thing worth noting. */}
                                  <td data-label="Why" data-empty={!r.error && r.trigger !== 'manual'}>
                                    <Show
                                      when={r.error}
                                      fallback={
                                        <Show when={orNull(r.trigger) === 'manual'}>
                                          <span class="mini">run by hand</span>
                                        </Show>
                                      }
                                    >
                                      <ReasonLine
                                        error={r.error}
                                        tone={r.status === 'skipped' ? 'warn' : 'bad'}
                                      />
                                    </Show>
                                  </td>
                                  <Show when={showKey()}>
                                    <td class="ghost truncate mono" data-label="Key" data-empty={!r.key}>
                                      {r.key || '—'}
                                    </td>
                                  </Show>
                                  <td
                                    class="ghost"
                                    data-label="Open"
                                    data-empty={!runRef() && !jobRef() && !blockingRun(r)}
                                  >
                                    <Show
                                      when={runRef()}
                                      fallback={
                                        // A `kind: job` fire has no workflow
                                        // run; what it produced is a job.
                                        <Show
                                          when={jobRef()}
                                          fallback={
                                            <Show when={blockingRun(r)} fallback={''}>
                                              {(blocking) => (
                                                <A
                                                  href={`/workflows/${encodeURIComponent(blocking())}`}
                                                  onClick={(e) => e.stopPropagation()}
                                                >
                                                  blocking run →
                                                </A>
                                              )}
                                            </Show>
                                          }
                                        >
                                          {(job) => (
                                            <A
                                              href={`/jobs/${encodeURIComponent(job())}`}
                                              onClick={(e) => e.stopPropagation()}
                                            >
                                              job →
                                            </A>
                                          )}
                                        </Show>
                                      }
                                    >
                                      run →
                                    </Show>
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

                <Panel title="Definition">
                  <KeyValue rows={definition(d().schedule)} />
                </Panel>
              </div>
            )}
          </Show>
        </Show>
      </div>
    </>
  );
}
