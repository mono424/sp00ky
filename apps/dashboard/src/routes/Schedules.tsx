import { For, Show, createResource, onCleanup } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Cell,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Rail,
  Reason,
  ReasonLine,
  StatusDot,
} from '../components/Chrome';
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

function cadence(s: Pick<Schedule, 'cron' | 'every_ms'>) {
  if (s.cron) return s.cron;
  if (s.every_ms) return `every ${formatDuration(s.every_ms)}`;
  return '—';
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
  size?: 'sm';
}) {
  const s = () => props.schedule;
  const disabledReason = () =>
    s().config_disabled
      ? 'Disabled in config; change it there and run spky schedules sync'
      : undefined;
  const triggerReason = () =>
    disabledReason() ?? (s().paused ? 'Paused: pause wins over a queued trigger' : undefined);
  const cls = () => (props.size === 'sm' ? 'btn btn-sm' : 'btn');
  return (
    <div class="row" onClick={(e) => e.stopPropagation()}>
      <button
        class={cls()}
        disabled={!!disabledReason()}
        title={disabledReason()}
        onClick={() => void pauseSchedule(s().name, !s().paused, props.onChange)}
      >
        {s().paused ? 'Resume' : 'Pause'}
      </button>
      <button
        class={`${cls()} btn-primary`}
        disabled={!!triggerReason()}
        title={triggerReason()}
        onClick={() => void triggerSchedule(s().name, props.onChange)}
      >
        Run now
      </button>
    </div>
  );
}

export function Schedules() {
  const navigate = useNavigate();
  const [data, { refetch }] = createResource(() =>
    api.get<{ schedules: Schedule[] }>('/schedules'),
  );

  const timer = setInterval(refetch, 5000);
  onCleanup(() => clearInterval(timer));

  return (
    <>
      <PageHead crumb="Dashboard" title="Schedules" />

      <div class="page-body">
        <Show when={data()} fallback={<Empty>Loading…</Empty>}>
          {(d) => (
            <Panel flush>
              <Show
                when={d().schedules.length > 0}
                fallback={
                  <Empty>
                    No schedules defined. They are declared in your config and
                    applied with <span class="dim">spky schedules sync</span>.
                  </Empty>
                }
              >
                <div class="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Kind</th>
                        <th>Cadence</th>
                        <th>State</th>
                        <th>Last run</th>
                        <th>Next fire</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      <For each={d().schedules}>
                        {(s) => (
                          <tr
                            class="clickable"
                            onClick={() =>
                              navigate(`/schedules/${encodeURIComponent(s.name)}`)
                            }
                          >
                            <td>
                              <div class="row">
                                <StatusDot tone={scheduleState(s).tone} />
                                {s.name}
                              </div>
                            </td>
                            <td class="ghost" data-label="Kind">{s.kind}</td>
                            <td class="dim" data-label="Cadence">{cadence(s)}</td>
                            <td data-label="State">
                              <Pill tone={scheduleState(s).tone}>
                                {scheduleState(s).label}
                              </Pill>
                            </td>
                            <td data-label="Last run">
                              <Show
                                when={s.last_run_status}
                                fallback={<span class="ghost">never</span>}
                              >
                                <div class="row">
                                  <Pill tone={runTone(s.last_run_status!)}>
                                    {s.last_run_status}
                                  </Pill>
                                  <span class="ghost">
                                    {relativeStamp(s.last_run_at)}
                                  </span>
                                </div>
                              </Show>
                            </td>
                            <td class="dim" data-label="Next fire">{relativeStamp(s.next_fire_at)}</td>
                            <td data-label="Actions" data-empty={true}>
                              <div class="row-actions">
                                <ScheduleControls schedule={s} size="sm" onChange={refetch} />
                              </div>
                            </td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </div>
              </Show>
            </Panel>
          )}
        </Show>
      </div>
    </>
  );
}

/**
 * One schedule: its definition, its recent fires, and the retention-proof
 * tally from `_00_run_rollup`.
 *
 * The rollup matters because run rows are pruned. Counting `_00_schedule_run`
 * would quietly under-report every schedule with a short history window; the
 * engine maintains hourly buckets precisely so a view like this does not have
 * to.
 */
export function ScheduleDetail() {
  const params = useParams<{ name: string }>();
  const name = () => decodeURIComponent(params.name);
  const navigate = useNavigate();

  const [result, { refetch }] = createResource(name, (n) =>
    api.getResult<ScheduleDetailData>(`/schedules/${encodeURIComponent(n)}`),
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
        killed: acc.killed + (b.killed ?? 0),
      }),
      { success: 0, failed: 0, skipped: 0, killed: 0 },
    );
  };

  const s = () => data()?.schedule;

  /**
   * Fires this schedule is currently suppressing, and what is holding them.
   *
   * A wall of `skipped` rows is the single most misread state in the scheduler:
   * it looks like "nothing to do" and it means "something is still running".
   * With the default `concurrency: skip` one run wedged in `running` suppresses
   * every later fire of its key for as long as it stays there, so the blocking
   * run — not the skipped rows — is what needs acting on.
   */
  const suppressed = () => {
    const skips = (data()?.runs ?? []).filter(
      (r) => r.status === 'skipped' && r.error?.code === 'concurrency_skip',
    );
    if (!skips.length) return null;
    const keys = new Set(skips.map((r) => r.key || '—'));
    return { skips, latest: skips[0], keys: keys.size };
  };

  const blockingRun = (r: ScheduleRun): string | null =>
    orNull((r.error?.blocked_by_workflow_run as string | undefined) ?? null);

  return (
    <>
      <PageHead
        crumb="Schedules"
        title={name()}
        subtitle={<Show when={s()}>{(v) => <>{cadence(v())}</>}</Show>}
        actions={
          <Show when={s()}>
            {(v) => (
              <>
                <ScheduleControls schedule={v()} size="sm" onChange={refetch} />
                <Pill tone={scheduleState(v()).tone} dot>
                  {scheduleState(v()).label}
                </Pill>
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
              <div class="stack">
                <Rail>
                  <Cell
                    label="Next fire"
                    value={relativeStamp(d().schedule.next_fire_at)}
                    foot={formatStamp(d().schedule.next_fire_at)}
                    tone={scheduleState(d().schedule).tone}
                  />
                  <Cell
                    label="Last run"
                    value={d().schedule.last_run_status ?? 'never'}
                    foot={relativeStamp(d().schedule.last_run_at)}
                    tone={
                      d().schedule.last_run_status
                        ? runTone(d().schedule.last_run_status!)
                        : undefined
                    }
                  />
                  <Cell
                    label="Succeeded"
                    value={formatCount(totals().success)}
                    foot="rolled up hourly"
                    tone="ok"
                  />
                  <Cell
                    label="Failed"
                    value={formatCount(totals().failed)}
                    foot={
                      totals().skipped > 0
                        ? `${formatCount(totals().skipped)} skipped`
                        : 'rolled up hourly'
                    }
                    tone={totals().failed > 0 ? 'bad' : undefined}
                  />
                </Rail>

                <Show when={suppressed()}>
                  {(sup) => (
                    <Panel
                      title="Fires are being suppressed"
                      sub={`${sup().skips.length} of the last ${data()!.runs.length} fires were skipped across ${sup().keys} key${sup().keys === 1 ? '' : 's'}, because a run for that key was still going`}
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

                {/* `_00_schedule.last_error` is a bare string, not an object:
                    it is why the schedule could not be PLANNED or fanned out at
                    all (a bad cron, a failing forEach), which happens before any
                    run exists to carry a structured cause. */}
                <Show when={d().schedule.last_error}>
                  <Panel
                    title="Last error"
                    sub="From planning or fan-out — this schedule could not fire, so no run row carries it"
                  >
                    <Reason error={d().schedule.last_error} />
                  </Panel>
                </Show>

                <div class="grid grid-2">
                  <Panel title="Definition">
                    <KeyValue
                      rows={[
                        ['Kind', d().schedule.kind],
                        ['Cadence', cadence(d().schedule)],
                        ['Timezone', d().schedule.timezone ?? 'UTC'],
                        ['Concurrency', d().schedule.concurrency],
                        [
                          'Timeout',
                          d().schedule.timeout
                            ? formatDuration(d().schedule.timeout! * 1000)
                            : '—',
                        ],
                        [
                          'Retries',
                          d().schedule.max_retries != null
                            ? `${d().schedule.max_retries} (${d().schedule.retry_strategy ?? 'default'})`
                            : '—',
                        ],
                        ['History', d().schedule.history_mode ?? 'all'],
                      ]}
                    />
                  </Panel>

                  <Panel title="Target">
                    <KeyValue
                      rows={[
                        ['Table', d().schedule.target_table ?? '—'],
                        ['Path', d().schedule.path ?? '—'],
                        ['For each', d().schedule.for_each ?? '—'],
                        ['Key', d().schedule.for_each_key ?? '—'],
                        ['Created', formatStamp(d().schedule.created_at)],
                        ['Updated', formatStamp(d().schedule.updated_at)],
                      ]}
                    />
                  </Panel>
                </div>

                <Panel
                  title="Recent fires"
                  sub="Newest first. Rows are pruned by your retention policy; the tally above is not."
                  flush
                >
                  <Show
                    when={d().runs.length > 0}
                    fallback={<Empty>No fires recorded.</Empty>}
                  >
                    <div class="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>Fired</th>
                            <th>Status</th>
                            <th>Duration</th>
                            <th>Why</th>
                            <th>Key</th>
                            <th>Run</th>
                          </tr>
                        </thead>
                        <tbody>
                          <For each={d().runs}>
                            {(r: ScheduleRun) => {
                              // `type::string(NONE)` is the string "NONE", so a
                              // fire that produced no workflow run must be
                              // filtered here or every row grows a dead link.
                              const runRef = () => orNull(r.workflow_run);
                              return (
                              <tr
                                class={runRef() ? 'clickable' : undefined}
                                onClick={() =>
                                  runRef() &&
                                  navigate(
                                    `/workflows/${encodeURIComponent(runRef()!)}`,
                                  )
                                }
                              >
                                <td>
                                  <div class="row">
                                    <StatusDot tone={runTone(r.status)} />
                                    {relativeStamp(r.fire_at ?? r.created_at)}
                                  </div>
                                </td>
                                <td data-label="Status">
                                  <Pill tone={runTone(r.status)}>{r.status}</Pill>
                                </td>
                                <td class="dim" data-label="Duration">
                                  {elapsed(r.created_at, r.finished_at)}
                                </td>
                                {/* The column that used to hold `trigger`. A fire
                                    is a cron fire 99% of the time, so the trigger
                                    said nothing; `skipped` and `failed` rows, which
                                    is what anyone scrolling this table is looking
                                    for, said nothing either. The trigger is still on
                                    the row, as a tag when it is the unusual one. */}
                                <td data-label="Why" data-empty={!r.error && r.trigger !== 'manual'}>
                                  <Show
                                    when={r.error}
                                    fallback={
                                      <Show when={orNull(r.trigger) === 'manual'}>
                                        <span class="mini">triggered by hand</span>
                                      </Show>
                                    }
                                  >
                                    <ReasonLine
                                      error={r.error}
                                      tone={r.status === 'skipped' ? 'warn' : 'bad'}
                                    />
                                  </Show>
                                </td>
                                <td class="ghost truncate" data-label="Key" data-empty={!r.key}>
                                  {r.key || '—'}
                                </td>
                                <td
                                  class="ghost"
                                  data-label="Run"
                                  data-empty={!runRef() && !blockingRun(r)}
                                >
                                  <Show
                                    when={runRef()}
                                    fallback={
                                      <Show when={blockingRun(r)} fallback={'—'}>
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
                                    open →
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
              </div>
            )}
          </Show>
        </Show>
      </div>
    </>
  );
}
