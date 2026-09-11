import { For, Show, createMemo, type JSX } from 'solid-js';
import { A } from '@solidjs/router';
import {
  Bento,
  Empty,
  KeyValue,
  PageHead,
  Pill,
  Readout,
  Segments,
  SkeletonBento,
  StatusDot,
  Tile,
} from '../components/Chrome';
import { Sparkline, type Point } from '../components/Sparkline';
import { BootstrapProgress } from '../components/BootstrapProgress';
import { ActivityStrip, OpBadge } from '../components/Actions';
import {
  SchedulerActions,
  SspActions,
  SspLogsLink,
} from '../components/ClusterActions';
import {
  formatClock,
  formatCount,
  formatMs,
  formatRelativeTime,
  formatStamp,
  formatUptime,
  relativeStamp,
  splitValue,
} from '../lib/format';
import { backendTone, schedulerTone, sspTone, type Tone } from '../lib/status';
import type {
  JobTotals,
  Overview as OverviewData,
  PresenceSample,
} from '../api/types';

/**
 * One presence count: the number, a line of context, and its history filling
 * the rest of the tile. The three of these on the overview keep separate
 * scales on purpose — views outnumber users by one to two orders of
 * magnitude, and a shared axis would flatten the users line onto the baseline.
 */
function PresenceTile(props: {
  i: number;
  label: string;
  sub: string;
  to?: string;
  ready: boolean;
  value: number | undefined;
  foot?: JSX.Element;
  points: Point[];
}) {
  return (
    <Tile i={props.i} span={3} label={props.label} sub={props.sub} to={props.to}>
      <Show
        when={props.ready}
        fallback={<Empty>Waiting for the first presence sample…</Empty>}
      >
        <Readout value={formatCount(props.value)} />
        <Show when={props.foot}>
          <div class="tile-foot">{props.foot}</div>
        </Show>
        <div class="tile-plot tile-end">
          <Sparkline
            points={props.points}
            fill
            bare
            format={formatCount}
            ariaLabel={props.label}
          />
        </div>
      </Show>
    </Tile>
  );
}

/**
 * Is the cluster healthy, and how fast is a write reaching a client right now.
 *
 * Laid out as a bento: the sync round trip is the hero tile because it is the
 * one number that measures the real product — the scheduler writes
 * `_00_heartbeat:probe` upstream and times the full path through `/ingest`,
 * the WAL, the broadcast and every SSP's circuit. Around it, one tile per
 * question an operator asks during an incident: are the SSPs up, are the
 * backends up, is ingest keeping up, what is the scheduler running. Then who
 * is here, then the two fleets in detail.
 */
export function Overview(props: {
  data: OverviewData | undefined;
  error?: string;
  /** Re-poll the overview now, e.g. right after an action. */
  refresh: () => void;
}) {
  const sched = () => props.data?.scheduler;
  const heartbeat = () => sched()?.heartbeat;

  const points = (): Point[] =>
    (heartbeat()?.samples ?? []).map((s) => ({ ts: s.ts, ms: s.ms, ok: s.ok }));

  // The presence sampler's ring, folded into this same poll — so every readout
  // and chart below costs no request of its own and no database work.
  const presence = () => props.data?.presence;
  const jobs = () => props.data?.jobs;

  /**
   * A stalled job outranks a deep queue: a backlog is the system working, a
   * lease that expired with nobody holding it is the system not working.
   */
  const jobsTone = (j: JobTotals): string => {
    if (!j.ready) return 'idle';
    if (j.stalled > 0 || j.blind) return 'bad';
    if (j.counts.failed > 0 || j.counts.pending > 0) return 'warn';
    return 'ok';
  };
  const totals = () => presence()?.totals;
  const series = (pick: (s: PresenceSample) => number): Point[] =>
    (presence()?.samples ?? []).map((s) => ({
      ts: s.ts,
      ms: pick(s),
      ok: true as const,
    }));

  const hb = createMemo(() => {
    const pts = points();
    const oks = pts
      .filter((p) => p.ok && typeof p.ms === 'number')
      .map((p) => p.ms as number);
    const failed = pts.filter((p) => !p.ok);
    return {
      n: pts.length,
      failed: failed.length,
      lastFailure: failed.length ? failed[failed.length - 1]!.ts : null,
      min: oks.length ? Math.min(...oks) : null,
      max: oks.length ? Math.max(...oks) : null,
    };
  });

  const hbTone = (): Tone => {
    const h = heartbeat();
    if (!h?.enabled) return 'idle';
    if (h.stale || (h.consecutive_failures ?? 0) > 0) return 'warn';
    return 'ok';
  };

  const hbFoot = () => {
    const h = heartbeat();
    if (!h?.enabled) return 'probe disabled';
    if (h.blocked_reason) return h.blocked_reason;
    const fails = h.consecutive_failures ?? 0;
    if (fails > 0) return `${fails} consecutive failures`;
    return h.last_ok_epoch_ms
      ? `last probe ok at ${formatClock(h.last_ok_epoch_ms)}`
      : 'no probe has completed yet';
  };

  const lag = () => sched()?.lag ?? 0;
  const latency = () => splitValue(formatMs(heartbeat()?.last_e2e_ms));

  const sspsTone = (d: OverviewData): Tone =>
    d.totals.ssps === 0
      ? 'bad'
      : d.totals.ssps_ready === d.totals.ssps
        ? 'ok'
        : 'warn';

  const backendsTone = (d: OverviewData): Tone =>
    d.totals.backends === 0
      ? 'idle'
      : d.totals.backends_healthy === d.totals.backends
        ? 'ok'
        : 'bad';

  const slowestBackend = (d: OverviewData) =>
    d.backends
      .filter((b) => typeof b.response_time_ms === 'number')
      .sort((a, b) => (b.response_time_ms ?? 0) - (a.response_time_ms ?? 0))[0];

  const backendName = (b: OverviewData['backends'][number]) => b.name ?? b.id ?? '—';

  const sampledAgo = (ms: number | null) =>
    ms === null ? '—' : formatRelativeTime(ms);

  const viewHealthTone = (): Tone | undefined => {
    const t = totals();
    if (!t) return undefined;
    if (t.errored_views) return 'bad';
    if (t.slow_views || t.large_views) return 'warn';
    return undefined;
  };

  return (
    <>
      <PageHead
        crumb="Dashboard"
        title="Overview"
        actions={
          <>
            <Show when={props.data && !props.error}>
              <Pill tone="live" dot pulse>
                live
              </Pill>
            </Show>
            <Show when={sched()}>
              {(s) => (
                <Pill tone={schedulerTone(s().status)} dot pulse={s().status !== 'ready'}>
                  scheduler {s().status}
                </Pill>
              )}
            </Show>
          </>
        }
      />

      <div class="page-body">
        <Show when={props.error}>
          <div class="banner">
            <span class="dot bad" />
            {props.error}
          </div>
        </Show>

        <ActivityStrip operations={props.data?.operations} />

        <Show when={props.data} fallback={<SkeletonBento />}>
          {(data) => (
            <Bento>
              {/* ---- hero: the one number that measures the product ---- */}
              <Tile
                i={0}
                span={6}
                rows={2}
                hero
                label="Sync round trip"
                sub="A write reaching every SSP's circuit, timed by the scheduler's own probe"
                tone={hbTone()}
                pulse={hbTone() === 'warn'}
                actions={
                  <Show when={hb().failed > 0}>
                    <span
                      class="pill bad"
                      title={
                        hb().lastFailure
                          ? `Last failed cycle at ${formatClock(hb().lastFailure!)}`
                          : undefined
                      }
                    >
                      {hb().failed} failed of {hb().n}
                    </span>
                  </Show>
                }
              >
                <div class="readout">
                  <span class="readout-value">{latency().value}</span>
                  <span class="readout-unit">{latency().unit}</span>
                  <div class="readout-aside">
                    <span class="tag">
                      min<span class="val">{formatMs(hb().min)}</span>
                    </span>
                    <span class="tag">
                      max<span class="val">{formatMs(hb().max)}</span>
                    </span>
                    <Show when={heartbeat()?.interval_secs}>
                      <span class="tag">
                        every<span class="val">{heartbeat()!.interval_secs}s</span>
                      </span>
                    </Show>
                  </div>
                </div>
                <div class="tile-foot">{hbFoot()}</div>
                <div class="tile-plot tile-end">
                  <Sparkline points={points()} fill bare ariaLabel="Sync round trip" />
                </div>
              </Tile>

              {/* ---- the four questions ---- */}
              <Tile
                i={1}
                span={3}
                label="Sync processors"
                sub="Queries are pinned per SSP"
                to="/ssps"
                tone={sspsTone(data())}
                pulse={sspsTone(data()) === 'warn'}
              >
                <Readout
                  value={`${data().totals.ssps_ready}/${data().totals.ssps}`}
                  unit="ready"
                />
                <Segments
                  items={data().ssps.map((s) => ({
                    id: s.id,
                    tone: sspTone(s.status),
                    title: `${s.id} · ${s.status}`,
                  }))}
                />
                <div class="tile-foot tile-end">
                  <Show
                    when={data().totals.ssps > 0}
                    fallback="none registered, nothing is serving live queries"
                  >
                    {formatCount(sched()?.views ?? 0)} live views
                    <Show when={data().totals.ssps - data().totals.ssps_ready > 0}>
                      {' · '}
                      {data().totals.ssps - data().totals.ssps_ready} catching up
                    </Show>
                  </Show>
                </div>
              </Tile>

              <Tile
                i={2}
                span={3}
                label="Backends"
                sub="Health-checked services"
                to="/backends"
                tone={backendsTone(data())}
              >
                <Readout
                  value={`${data().totals.backends_healthy}/${data().totals.backends}`}
                  unit="healthy"
                />
                <Segments
                  items={data().backends.map((b) => ({
                    id: backendName(b),
                    tone: backendTone(b.status),
                    title: `${backendName(b)} · ${b.status}`,
                  }))}
                />
                <div class="tile-foot tile-end">
                  <Show when={data().totals.backends > 0} fallback="none configured">
                    <Show when={slowestBackend(data())} fallback="no response times yet">
                      {(b) => (
                        <>
                          slowest {backendName(b())} at {formatMs(b().response_time_ms)}
                        </>
                      )}
                    </Show>
                  </Show>
                </div>
              </Tile>

              <Tile
                i={3}
                span={3}
                label="Ingest lag"
                sub="Events the replica has yet to apply"
                tone={lag() > 1000 ? 'warn' : 'ok'}
              >
                <Readout value={formatCount(lag())} unit="events" />
                <div class="tile-foot tile-end">
                  seq {formatCount(sched()?.latest_seq)} · snapshot{' '}
                  {formatCount(sched()?.snapshot_seq)} ·{' '}
                  {formatCount(sched()?.pending_events)} pending
                </div>
              </Tile>

              <Tile
                i={4}
                span={3}
                label="Scheduler"
                sub={
                  <span class="id truncate" style={{ display: 'block' }} title={sched()?.id}>
                    {sched()?.id}
                  </span>
                }
                tone={schedulerTone(sched()?.status)}
                pulse={!!sched() && sched()!.status !== 'ready'}
                actions={<SchedulerActions size="sm" onDone={props.refresh} />}
              >
                <KeyValue
                  rows={[
                    [
                      'Status',
                      <Pill tone={schedulerTone(sched()?.status)}>
                        {sched()?.status ?? '—'}
                      </Pill>,
                    ],
                    ['Version', sched()?.version ?? '—'],
                    ['SurrealDB', sched()?.surrealdb_version ?? '—'],
                    ['Uptime', formatUptime(sched()?.uptime_seconds)],
                    ['Address', sched()?.ip ?? '—'],
                  ]}
                />
              </Tile>

              {/* ---- who is here. Every figure counts only rows still inside
                   `lastActiveAt + ttl`, so a closed tab fades out over minutes
                   rather than vanishing. ---- */}
              <Show when={presence()}>
                {(p) => (
                  <>
                    <PresenceTile
                      i={5}
                      label="Users"
                      sub="Signed in, fading out over ~9 min"
                      to="/views"
                      ready={p().ready}
                      value={p().totals.users}
                      foot="with at least one live view"
                      points={series((s) => s.users)}
                    />
                    <PresenceTile
                      i={6}
                      label="Sessions"
                      sub="One per open tab"
                      to="/views"
                      ready={p().ready}
                      value={p().totals.sessions}
                      foot={`${formatCount(p().totals.anon_sessions)} anonymous`}
                      points={series((s) => s.sessions)}
                    />
                    <PresenceTile
                      i={7}
                      label="Registered views"
                      sub="One per session, per query"
                      to="/views"
                      ready={p().ready}
                      value={p().totals.views}
                      foot={`${formatCount(p().totals.shared_views)} shared by more than one tab`}
                      points={series((s) => s.views)}
                    />
                    <Tile
                      i={8}
                      span={3}
                      label="View health"
                      sub="Materialization, current window"
                      to="/views"
                      tone={viewHealthTone()}
                    >
                      <Show
                        when={p().ready}
                        fallback={<Empty>Waiting for the first presence sample…</Empty>}
                      >
                        <div class="stat3 stat4">
                          <div>
                            <div class="k">Shared</div>
                            <div class="v">{formatCount(p().totals.shared_views)}</div>
                          </div>
                          <div>
                            <div class="k">Large</div>
                            <div class="v" classList={{ 'tone-warn': p().totals.large_views > 0 }}>
                              {formatCount(p().totals.large_views)}
                            </div>
                          </div>
                          <div>
                            <div class="k">Slow</div>
                            <div class="v" classList={{ 'tone-warn': p().totals.slow_views > 0 }}>
                              {formatCount(p().totals.slow_views)}
                            </div>
                          </div>
                          <div>
                            <div class="k">Errored</div>
                            <div class="v" classList={{ 'tone-bad': p().totals.errored_views > 0 }}>
                              {formatCount(p().totals.errored_views)}
                            </div>
                          </div>
                        </div>
                        <div class="tile-foot tile-end">
                          <Show
                            when={!p().error}
                            fallback={<span class="tone-bad">{p().error}</span>}
                          >
                            <Show
                              when={!p().truncated}
                              fallback="row cap hit, every figure is a floor"
                            >
                              sampled {sampledAgo(p().taken_at_ms)} · every{' '}
                              {p().sample_interval_secs}s
                            </Show>
                          </Show>
                        </div>
                      </Show>
                    </Tile>
                  </>
                )}
              </Show>

              {/* ---- the outbox. Same bargain as presence: the job sampler
                   holds these, so the tile rides this poll and adds nothing to
                   the database. Span 12 because the presence row above already
                   fills its twelve columns, and a lone span-3 tile here would
                   leave nine of them empty. ---- */}
              <Show when={jobs()}>
                {(j) => (
                  <Tile
                    i={9}
                    span={12}
                    label="Jobs"
                    sub="The outbox queue, across every table"
                    to="/jobs"
                    tone={jobsTone(j())}
                  >
                    <Show
                      when={j().ready}
                      fallback={
                        <Empty>
                          <Show
                            when={j().enabled === false}
                            fallback={<>Waiting for the first job sample…</>}
                          >
                            The job sampler is switched off on this scheduler (
                            <span class="mono">SPKY_ADMIN_JOB_INTERVAL_SECS=0</span>).
                            The Jobs page still lists and filters.
                          </Show>
                        </Empty>
                      }
                    >
                      <div class="stat3 stat5">
                        <div>
                          <div class="k">Pending</div>
                          <div class="v" classList={{ 'tone-warn': j().counts.pending > 0 }}>
                            {formatCount(j().counts.pending)}
                          </div>
                        </div>
                        <div>
                          <div class="k">In flight</div>
                          <div class="v">{formatCount(j().in_flight)}</div>
                        </div>
                        <div>
                          <div class="k">Stalled</div>
                          <div class="v" classList={{ 'tone-bad': j().stalled > 0 }}>
                            {formatCount(j().stalled)}
                          </div>
                        </div>
                        <div>
                          <div class="k">Failed</div>
                          <div class="v" classList={{ 'tone-bad': j().counts.failed > 0 }}>
                            {formatCount(j().counts.failed)}
                          </div>
                        </div>
                        <div>
                          <div class="k">Per min</div>
                          <div class="v">{formatCount(j().throughput_1m)}</div>
                        </div>
                      </div>
                      <div class="tile-foot tile-end">
                        <Show
                          when={!j().blind}
                          fallback={
                            <span class="tone-bad">
                              every outbox table refused to answer, so these are not measurements
                            </span>
                          }
                        >
                          <Show
                            when={j().stalled === 0}
                            fallback={
                              <span class="tone-bad">
                                a stalled job's lease expired with nobody holding it
                              </span>
                            }
                          >
                            <Show
                              when={j().oldest_pending}
                              fallback={<>nothing queued · failures counted over the last hour</>}
                            >
                              oldest pending {relativeStamp(j().oldest_pending)} ·
                              failures over the last hour
                            </Show>
                          </Show>
                        </Show>
                      </div>
                    </Show>
                  </Tile>
                )}
              </Show>

              {/* ---- the fleets in detail ---- */}
              <Tile
                i={10}
                span={12}
                flush
                label="Sync processors"
                sub="One lagging processor is a real outage for the clients pinned to it"
                to="/ssps"
              >
                <Show
                  when={data().ssps.length > 0}
                  fallback={
                    <Empty>No SSPs registered. Nothing is serving live queries.</Empty>
                  }
                >
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>SSP</th>
                          <th>Status</th>
                          <th>Views</th>
                          <th>Uptime</th>
                          <th style={{ width: '30%' }}>Progress</th>
                          <th />
                        </tr>
                      </thead>
                      <tbody>
                        <For each={data().ssps}>
                          {(ssp) => (
                            <tr>
                              <td style={{ 'white-space': 'nowrap' }}>
                                <div class="row">
                                  <StatusDot
                                    tone={sspTone(ssp.status)}
                                    pulse={ssp.status !== 'ready'}
                                  />
                                  <span class="ident mono">{ssp.id}</span>
                                </div>
                                <div class="id truncate" style={{ 'margin-top': '2px' }}>
                                  {ssp.ip ?? '—'} · v{ssp.version}
                                </div>
                              </td>
                              <td data-label="Status">
                                <Pill tone={sspTone(ssp.status)}>{ssp.status}</Pill>
                              </td>
                              <td class="mono" data-label="Views">
                                {formatCount(ssp.views)}
                              </td>
                              <td class="dim mono" data-label="Uptime">
                                {formatUptime(ssp.uptime_seconds)}
                              </td>
                              <td data-label="Progress">
                                <Show
                                  when={ssp.status !== 'ready'}
                                  fallback={
                                    <div class="row">
                                      <span class="ghost">caught up</span>
                                      <OpBadge
                                        operations={data().operations}
                                        target={ssp.id}
                                      />
                                    </div>
                                  }
                                >
                                  <BootstrapProgress
                                    ssp={ssp}
                                    timeoutSecs={data().bootstrap_timeout_secs}
                                  />
                                </Show>
                              </td>
                              <td data-label="Actions" style={{ 'text-align': 'right' }}>
                                <SspActions ssp={ssp} size="sm" onDone={props.refresh} />
                                <SspLogsLink ssp={ssp} />
                              </td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </Show>
              </Tile>

              <Show when={data().backends.length > 0}>
                <Tile
                  i={11}
                  span={12}
                  flush
                  label="Backends"
                  sub="Health-checked application services"
                  to="/backends"
                >
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Status</th>
                          <th>Response</th>
                          <th>Check</th>
                          <th>Last healthy</th>
                          <th>URL</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={data().backends}>
                          {(b) => (
                            <tr>
                              <td style={{ 'white-space': 'nowrap' }}>
                                <A href={`/backends/${encodeURIComponent(backendName(b))}`}>
                                  <div class="row">
                                    <StatusDot tone={backendTone(b.status)} />
                                    <span class="ident">{backendName(b)}</span>
                                  </div>
                                </A>
                              </td>
                              <td data-label="Status">
                                <Pill tone={backendTone(b.status)}>{b.status}</Pill>
                              </td>
                              <td class="dim mono" data-label="Response">
                                {formatMs(b.response_time_ms)}
                              </td>
                              <td class="ghost mono" data-label="Check">
                                {b.healthcheck}
                              </td>
                              <td
                                class="dim"
                                data-label="Last healthy"
                                title={formatStamp(b.last_healthy)}
                              >
                                {relativeStamp(b.last_healthy)}
                              </td>
                              <td class="ghost mono truncate" data-label="URL">
                                {b.url}
                              </td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </Tile>
              </Show>
            </Bento>
          )}
        </Show>
      </div>
    </>
  );
}
