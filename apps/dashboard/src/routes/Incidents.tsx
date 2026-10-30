import { For, Show, createMemo, createResource, createSignal, onCleanup, type Accessor } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import type { PublicationMetrics } from '../api/types';
import { PageHead, Stale } from '../components/Chrome';
import { FilterMenu, FxCell, Sq, type FilterValue } from '../components/Frame';
import { decodeParam, formatBytes, formatCount, formatDuration, formatRelativeTime } from '../lib/format';
import { readStash, writeStash } from '../lib/stash';

interface IncidentEvent {
  at: number;
  component: string;
  kind: string;
  state: string;
  summary: string;
  operation_id: string | null;
  version: string;
}
interface Incident {
  id: string;
  component: string;
  kind: string;
  severity: string;
  state: string;
  started_at: number;
  ended_at: number | null;
  max_buffered_events: number;
  max_publication_operations?: number;
  max_publication_bytes?: number;
  max_publication_age_ms?: number;
  publication?: PublicationMetrics | null;
  event_count: number;
  events: IncidentEvent[];
}
interface IncidentList {
  incidents: Incident[];
  total: number;
  offset: number;
  limit: number;
  storage_error: string | null;
  server_time_ms: number;
}

/**
 * The page fetches one window of history (the newest `WINDOW` episodes in the
 * chosen range) and every filter applies on the client, instantly. Thirty
 * days of a busy tenant is a few hundred rows; a form that round-trips per
 * filter change was the wrong shape for "show me what is open right now".
 */
const WINDOW = 400;
const RANGES: { key: string; label: string; ms: number }[] = [
  { key: '24h', label: '24 h', ms: 24 * 3_600_000 },
  { key: '7d', label: '7 days', ms: 7 * 24 * 3_600_000 },
  { key: '30d', label: '30 days', ms: 30 * 24 * 3_600_000 },
];
const STATES = ['open', 'recovered', 'interrupted', 'failed', 'recorded'];
const label = (value: string) => value.replace(/_/g, ' ');
const stamp = (value: number) => new Date(value).toLocaleString();
const clock = (value: number) =>
  new Date(value).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
const tone = (state: string) => state === 'recovered' ? 'ok'
  : state === 'failed' ? 'bad' : state === 'open' || state === 'interrupted' ? 'warn' : 'idle';
const duration = (incident: Incident, now: number) =>
  formatDuration(Math.max(0, (incident.ended_at ?? now) - incident.started_at));
const dayKey = (ms: number) => {
  const d = new Date(ms);
  const today = new Date();
  const same = (a: Date, b: Date) => a.toDateString() === b.toDateString();
  if (same(d, today)) return 'Today';
  const yesterday = new Date(today.getTime() - 86_400_000);
  if (same(d, yesterday)) return 'Yesterday';
  return d.toLocaleDateString(undefined, { weekday: 'short', day: 'numeric', month: 'short' });
};

/** Match the dashboard's resource polling, with cleanup and no overlapping polls. */
function usePoll<T>(path: Accessor<string>, stash?: string) {
  const [result, { refetch }] = createResource(path, async (url) => {
    const r = await api.getResult<T>(url);
    if (r.ok && stash) writeStash(stash, r.value);
    return r;
  });
  const timer = setInterval(() => {
    if (!result.loading && !document.hidden) void refetch();
  }, 5000);
  onCleanup(() => clearInterval(timer));
  const cached = stash ? readStash<T>(stash)?.value : undefined;
  return {
    data: () => { const r = result(); return r?.ok ? r.value : (r ? undefined : cached); },
    stale: () => !result() && cached != null,
    error: () => { const r = result(); return r && !r.ok ? r.message : undefined; },
    loading: () => result.loading,
    refresh: () => { if (!result.loading) void refetch(); },
  };
}

/** The square's tone for an incident: amber while it is open, red when it failed. */
const sq = (i: Incident) =>
  i.state === 'failed' ? 'bad' : i.state === 'open' || i.state === 'interrupted' ? 'warn' : i.severity === 'info' ? 'idle' : '';

/** The time axis for a range, in whole steps: every 6 h, every day, every 5 days. */
const axisFor = (ms: number) => {
  const h = ms / 3_600_000;
  const step = h <= 24 ? 6 : h <= 7 * 24 ? 24 : 5 * 24;
  const out: string[] = [];
  for (let v = h; v > 0.01; v -= step) out.push(v >= 48 ? `-${Math.round(v / 24)}d` : `-${Math.round(v)}h`);
  out.push('now');
  return out;
};

/**
 * Every incident of the range on one time axis, one lane per component:
 * where things broke, when, and for how long. A bar is clickable; open ones
 * run to the now line.
 */
function Lanes(props: { incidents: Incident[]; from: number; now: number; span: number; onPick: (i: Incident) => void }) {
  const lanes = createMemo(() => {
    const by = new Map<string, Incident[]>();
    for (const i of props.incidents) {
      const list = by.get(i.component) ?? [];
      list.push(i);
      by.set(i.component, list);
    }
    return [...by.entries()].sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]));
  });
  const x = (t: number) => Math.max(0, Math.min(100, ((t - props.from) / props.span) * 100));
  return (
    <div class="inc-lanes">
      <For each={lanes()} fallback={<div class="fx-empty cap">Nothing happened in this range</div>}>
        {([component, items]) => (
          <div class="inc-lane">
            <span class="inc-lane-name mono">{component}</span>
            <div class="inc-lane-track">
              <For each={items}>
                {(i) => {
                  const left = () => x(i.started_at);
                  const width = () => x(i.ended_at ?? props.now) - left();
                  return (
                    <button
                      class={`inc-bar ${sq(i)}`}
                      classList={{ live: i.state === 'open', point: i.ended_at === i.started_at }}
                      style={{ left: `${left()}%`, width: `${width()}%` }}
                      title={`${i.component} ${label(i.kind)} · ${label(i.state)} · ${duration(i, props.now)}\n${stamp(i.started_at)}`}
                      onClick={() => props.onPick(i)}
                    />
                  );
                }}
              </For>
            </div>
          </div>
        )}
      </For>
      <div class="inc-lane inc-lane-axis">
        <span />
        <div class="fx-axis cap">
          <For each={axisFor(props.span)}>{(a) => <span>{a}</span>}</For>
        </div>
      </div>
    </div>
  );
}

export function Incidents() {
  const navigate = useNavigate();
  const [range, setRange] = createSignal('7d');
  const [filters, setFilters] = createSignal<FilterValue>({});
  const states = () => filters().state ?? [];
  const comps = () => filters().component ?? [];
  const [search, setSearch] = createSignal('');
  const [shown, setShown] = createSignal(60);

  const span = () => RANGES.find((x) => x.key === range())?.ms ?? RANGES[1].ms;
  const since = createMemo(() => {
    // Re-evaluated with the range only: the window's edge moving a few
    // seconds per poll is not worth a new resource key.
    return Date.now() - span();
  });
  const path = createMemo(() => `/incidents?limit=${WINDOW}${since() ? `&since=${since()}` : ''}`);
  const poll = usePoll<IncidentList>(path, `incidents:${range()}`);
  const now = () => poll.data()?.server_time_ms ?? Date.now();
  // The scheduler's clock bounds the chart, so a skewed browser clock does not
  // push the history off its own axis.
  const from = () => now() - span();
  const all = () => (poll.data()?.incidents ?? []).filter((i) => (i.ended_at ?? now()) >= from());
  // Open episodes lead the page; the history below is everything that ended.
  const open = createMemo(() => all().filter((i) => i.state === 'open'));
  const past = createMemo(() => all().filter((i) => i.state !== 'open'));

  const components = createMemo(() => {
    const seen = new Map<string, number>();
    for (const i of past()) seen.set(i.component, (seen.get(i.component) ?? 0) + 1);
    return [...seen.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  });
  const stateCounts = createMemo(() => {
    const counts: Record<string, number> = {};
    for (const i of past()) counts[i.state] = (counts[i.state] ?? 0) + 1;
    return counts;
  });
  // Recovery: how long the ones that recovered took, typically and at worst.
  const recovered = createMemo(() =>
    past().filter((i) => i.state === 'recovered' && i.ended_at != null).map((i) => i.ended_at! - i.started_at).sort((a, b) => a - b),
  );
  const median = () => (recovered().length ? recovered()[Math.floor(recovered().length / 2)] : null);
  const longest = () => (recovered().length ? recovered()[recovered().length - 1] : null);
  const filtered = createMemo(() => {
    const q = search().trim().toLowerCase();
    return past().filter((i) =>
      (!states().length || states().includes(i.state)) &&
      (!comps().length || comps().includes(i.component)) &&
      (!q || `${i.component} ${i.kind} ${i.severity} ${i.events.map((e) => e.summary).join(' ')}`.toLowerCase().includes(q)));
  });
  const groups = createMemo(() => {
    const out: { day: string; items: Incident[] }[] = [];
    for (const i of filtered().slice(0, shown())) {
      const day = dayKey(i.started_at);
      const last = out[out.length - 1];
      if (last && last.day === day) last.items.push(i);
      else out.push({ day, items: [i] });
    }
    return out;
  });
  const reset = () => { setFilters({}); setSearch(''); setShown(60); };
  const filtering = () => !!(states().length || comps().length || search().trim());
  const go = (i: Incident) => navigate(`/incidents/${encodeURIComponent(i.id)}`);

  return <>
    <PageHead title="Incidents"
      actions={<>
        <Stale when={poll.stale()} />
        <div class="fx-range" role="group" aria-label="Range">
          <For each={RANGES}>{(r) => (
            <button classList={{ on: range() === r.key }} aria-pressed={range() === r.key}
              onClick={() => { setRange(r.key); setShown(60); }}>{r.key}</button>
          )}</For>
        </div>
        <A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>
      </>} />

    <Show when={poll.error() || poll.data()?.storage_error}>
      {(error) => <section class="fx-row"><div class="fx-cell"><div class="fx-log bad" role="alert"><span class="fx-what">{error()}</span></div></div></section>}
    </Show>

    <Show when={poll.data()} fallback={<Show when={!poll.error()}><div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div></Show>}>
      <div class="fx-page">
        {/* when and where, and how it is going */}
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
          <FxCell label="Timeline" meta={<span><b>{formatCount(all().length)}</b> in {RANGES.find((r) => r.key === range())?.label}</span>}>
            <Lanes incidents={all()} from={from()} now={now()} span={span()} onPick={go} />
          </FxCell>
          <FxCell label="Open now" meta={<Show when={all().length}><span>last {formatRelativeTime(all()[0].started_at - (now() - Date.now()))}</span></Show>}>
            <div class="fx-big" classList={{ 'inc-open-n': open().length > 0 }}>{open().length}</div>
            <div class="fx-foot inc-facts">
              <div><span class="cap">Recovered</span><b>{stateCounts().recovered ?? 0}</b></div>
              <div><span class="cap">Failed</span><b classList={{ bad: (stateCounts().failed ?? 0) > 0 }}>{stateCounts().failed ?? 0}</b></div>
              <div><span class="cap">Typical fix</span><b>{median() != null ? formatDuration(median()!) : '—'}</b></div>
              <div><span class="cap">Longest</span><b>{longest() != null ? formatDuration(longest()!) : '—'}</b></div>
            </div>
          </FxCell>
        </section>

        {/* what is wrong right now */}
        <Show when={open().length}>
          <section class="fx-row">
            <FxCell label="Open" meta={<span class="warn">{open().length} ongoing</span>}>
              <For each={open()}>{(i) => {
                const last = () => i.events[i.events.length - 1];
                return (
                  <div class="fx-log" classList={{ bad: i.severity === 'error' || i.severity === 'critical' }}>
                    <span class="cap" title={stamp(i.started_at)}>{clock(i.started_at)}</span>
                    <span class="fx-what">
                      {i.component} · {label(i.kind)}{last()?.summary ? <em> · {last()!.summary}</em> : null}
                    </span>
                    <span class="cap">{duration(i, now())}</span>
                    <A class="btn btn-sm" href={`/incidents/${encodeURIComponent(i.id)}`}>Open</A>
                  </div>
                );
              }}</For>
            </FxCell>
          </section>
        </Show>

        {/* everything that ended */}
        <section class="fx-row">
          <FxCell label="History" meta={<span><b>{formatCount(filtered().length)}</b>{filtering() ? ` of ${formatCount(past().length)}` : ''}</span>}>
            <div class="inc-filters">
              <FilterMenu
                value={filters()}
                onChange={(v) => { setFilters(v); setShown(60); }}
                fields={[
                  {
                    key: 'state',
                    label: 'State',
                    options: STATES.filter((x) => stateCounts()[x]).map((x) => ({
                      value: x,
                      label: label(x),
                      count: stateCounts()[x],
                      tone: x === 'failed' ? 'bad' : x === 'interrupted' ? 'warn' : x === 'recorded' ? 'idle' : '',
                    })),
                  },
                  {
                    key: 'component',
                    label: 'Component',
                    options: components().map(([c, n]) => ({ value: c, label: c, count: n })),
                  },
                ]}
              />
              <div class="inc-find">
                <input type="search" placeholder="Search" value={search()} onInput={(e) => { setSearch(e.currentTarget.value); setShown(60); }} aria-label="Search incidents" />
                <Show when={filtering()}><button type="button" class="btn btn-sm" onClick={reset}>Clear all</button></Show>
              </div>
            </div>

            <Show when={filtered().length} fallback={<div class="fx-empty cap">{filtering() ? 'Nothing matches these filters' : 'Nothing has ended in this range'}</div>}>
              <table class="fx-list inc-list" aria-busy={poll.loading()}>
                <colgroup>
                  <col style={{ width: '9%' }} /><col style={{ width: '14%' }} /><col style={{ width: '17%' }} />
                  <col /><col style={{ width: '11%' }} /><col style={{ width: '9%' }} />
                </colgroup>
                <For each={groups()}>{(group) => (
                  <tbody>
                    <tr class="fx-gh"><th class="cap" colSpan={6}><span class="fx-gt">{group.day}</span><span>{group.items.length}</span></th></tr>
                    <For each={group.items}>{(i) => {
                      const last = i.events[i.events.length - 1];
                      return (
                        <tr class="inc-tr" role="link" tabIndex={0} onClick={() => go(i)} onKeyDown={(e) => { if (e.key === 'Enter') go(i); }}>
                          <td title={stamp(i.started_at)}>{clock(i.started_at)}</td>
                          <td><span class="fx-who"><Sq tone={sq(i)} /><span style={{ color: 'var(--ink)' }}>{i.component}</span></span></td>
                          <td>{label(i.kind)}</td>
                          <td class="inc-sum">{last?.summary ?? ''}</td>
                          <td><span class="cap" classList={{ warn: sq(i) === 'warn', bad: sq(i) === 'bad' }}>{label(i.state)}</span></td>
                          <td class="r">{i.ended_at === i.started_at ? '·' : duration(i, now())}</td>
                        </tr>
                      );
                    }}</For>
                  </tbody>
                )}</For>
              </table>
              <Show when={filtered().length > shown() || (poll.data()?.total ?? 0) > (poll.data()?.incidents.length ?? 0)}>
                <div class="inc-pager cap">
                  <span aria-live="polite">{Math.min(shown(), filtered().length)} of {formatCount(filtered().length)}
                    <Show when={(poll.data()?.total ?? 0) > (poll.data()?.incidents.length ?? 0)}>, newest {WINDOW} of {formatCount(poll.data()!.total)} loaded</Show></span>
                  <Show when={filtered().length > shown()}><button class="btn btn-sm" onClick={() => setShown(shown() + 60)}>Show more</button></Show>
                </div>
              </Show>
            </Show>
          </FxCell>
        </section>
      </div>
    </Show>
  </>;
}

export function IncidentDetail() {
  const params = useParams();
  const path = createMemo(() => `/incidents/${encodeURIComponent(decodeParam(params.id))}`);
  const poll = usePoll<{ incident: Incident }>(path);
  return <>
    <PageHead crumb="Incidents" title={poll.data() ? `${poll.data()!.incident.component} ${label(poll.data()!.incident.kind)}` : 'Incident'}
      actions={<A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>} />

    <Show when={poll.error()}>
      {(error) => <section class="fx-row"><div class="fx-cell"><div class="fx-log bad" role="alert"><span class="fx-what">{error()}</span></div></div></section>}
    </Show>

    <Show when={poll.data()?.incident} fallback={<Show when={!poll.error()}><div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div></Show>}>
      {(incident) => {
        const ended = () => incident().ended_at != null;
        return <div class="fx-page">
          {/* the four facts */}
          <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
            <FxCell label="State" meta={<span>{incident().severity}</span>}>
              <div class="fx-big inc-state">
                <Sq tone={sq(incident())} />
                <span classList={{ warn: sq(incident()) === 'warn', bad: sq(incident()) === 'bad' }}>{label(incident().state)}</span>
              </div>
            </FxCell>
            <FxCell label="Duration" meta={<span>{ended() ? 'ended' : 'still open'}</span>}>
              <div class="fx-big">{duration(incident(), Date.now())}</div>
              <div class="fx-sub cap"><span>since <b>{stamp(incident().started_at)}</b></span></div>
            </FxCell>
            <FxCell label="Events">
              <div class="fx-big">{formatCount(incident().event_count)}</div>
              <Show when={incident().events.length < incident().event_count}>
                <div class="fx-sub cap"><span><b>{incident().events.length}</b> kept</span></div>
              </Show>
            </FxCell>
            <FxCell label="Peak buffered">
              <div class="fx-big">{incident().max_buffered_events > 0 ? formatCount(incident().max_buffered_events) : '—'}</div>
              <div class="fx-sub cap"><span>events waiting for replay</span></div>
            </FxCell>
          </section>

          <Show when={incident().state === 'interrupted'}>
            <section class="fx-row"><div class="fx-cell"><div class="fx-log"><span class="fx-what">The scheduler restarted before recovery was seen</span></div></div></section>
          </Show>

          {/* what was stuck, for a publication incident */}
          <Show when={incident().publication}>{(publication) => (
            <section class="fx-row">
              <FxCell label="Publication backlog" meta={<span>sampled from SSP heartbeats</span>}>
                <div class="fx-foot inc-facts wide">
                  <div><span class="cap">Peak operations</span><b>{formatCount(incident().max_publication_operations)}</b></div>
                  <div><span class="cap">Peak bytes</span><b>{formatBytes(incident().max_publication_bytes)}</b></div>
                  <div><span class="cap">Longest wait</span><b>{formatDuration(incident().max_publication_age_ms ?? 0)}</b></div>
                  <div><span class="cap">Parked batches</span><b>{formatCount(publication().parked_batches)}</b></div>
                  <div><span class="cap">Overload rejects</span><b>{formatCount(publication().overload_total)}</b></div>
                  <div><span class="cap">Last published</span><b>{publication().last_success_at_ms == null ? '—' : clock(publication().last_success_at_ms!)}</b></div>
                </div>
                <Show when={publication().worst_views.length}>
                  <table class="fx-list" style={{ 'margin-top': '22px' }}>
                    <thead><tr><th class="cap">View</th><th class="cap r">Pending ops</th><th class="cap r">Pending bytes</th><th class="cap r">Oldest wait</th></tr></thead>
                    <tbody><For each={publication().worst_views}>{(view) => <tr>
                      <td><A href={`/views/${encodeURIComponent(view.query_id)}`} style={{ color: 'var(--ink)' }}>{view.query_id}</A></td>
                      <td class="r">{formatCount(view.pending_operations)}</td><td class="r">{formatBytes(view.pending_bytes)}</td><td class="r">{formatDuration(view.oldest_age_ms)}</td>
                    </tr>}</For></tbody>
                  </table>
                </Show>
              </FxCell>
            </section>
          )}</Show>

          {/* what happened, in order */}
          <section class="fx-row">
            <FxCell label="Timeline" meta={<span>oldest first</span>}>
              <Show when={incident().events.length} fallback={<div class="fx-empty cap">No events kept</div>}>
                <table class="fx-list inc-list">
                  <colgroup><col style={{ width: '16%' }} /><col style={{ width: '12%' }} /><col /><col style={{ width: '24%' }} /></colgroup>
                  <thead><tr><th class="cap">Time</th><th class="cap">State</th><th class="cap">What happened</th><th class="cap r">Version</th></tr></thead>
                  <tbody><For each={incident().events}>{(event) => (
                    <tr>
                      <td title={stamp(event.at)}>{new Date(event.at).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })}</td>
                      <td><span class="fx-who"><Sq tone={tone(event.state) === 'ok' ? '' : tone(event.state)} /><span class="cap">{label(event.state)}</span></span></td>
                      <td class="inc-sum" style={{ color: 'var(--ink)' }}>{event.summary}
                        <Show when={event.operation_id}><span class="cap" style={{ 'margin-left': '10px' }}>op {event.operation_id}</span></Show>
                      </td>
                      <td class="r">{event.version.replace(/^0\.0\.1-/, '')}</td>
                    </tr>
                  )}</For></tbody>
                </table>
              </Show>
            </FxCell>
          </section>
        </div>;
      }}
    </Show>
  </>;
}
