import { For, Show, createMemo, createResource, createSignal, onCleanup, type Accessor } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import type { PublicationMetrics } from '../api/types';
import { Cell, Chips, Empty, KeyValue, PageHead, Panel, Pill, Rail, SkeletonList, Stale } from '../components/Chrome';
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

export function Incidents() {
  const navigate = useNavigate();
  const [range, setRange] = createSignal('7d');
  const [state, setState] = createSignal('');
  const [component, setComponent] = createSignal('');
  const [search, setSearch] = createSignal('');
  const [shown, setShown] = createSignal(60);

  const since = createMemo(() => {
    const r = RANGES.find((x) => x.key === range());
    // Re-evaluated with the range only: the window's edge moving a few
    // seconds per poll is not worth a new resource key.
    return r ? Date.now() - r.ms : 0;
  });
  const path = createMemo(() => `/incidents?limit=${WINDOW}${since() ? `&since=${since()}` : ''}`);
  const poll = usePoll<IncidentList>(path, `incidents:${range()}`);
  const now = () => poll.data()?.server_time_ms ?? Date.now();
  const all = () => poll.data()?.incidents ?? [];

  const components = createMemo(() => {
    const seen = new Map<string, number>();
    for (const i of all()) seen.set(i.component, (seen.get(i.component) ?? 0) + 1);
    return [...seen.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  });
  const stateCounts = createMemo(() => {
    const counts: Record<string, number> = {};
    for (const i of all()) counts[i.state] = (counts[i.state] ?? 0) + 1;
    return counts;
  });
  const filtered = createMemo(() => {
    const q = search().trim().toLowerCase();
    return all().filter((i) =>
      (!state() || i.state === state()) &&
      (!component() || i.component === component()) &&
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
  const open = () => stateCounts().open ?? 0;
  const warnings = () => all().filter((i) => i.severity !== 'info').length;
  const longest = () => all().reduce((m, i) => Math.max(m, (i.ended_at ?? now()) - i.started_at), 0);
  const latest = () => all()[0];
  const reset = () => { setState(''); setComponent(''); setSearch(''); setShown(60); };
  const filtering = () => !!(state() || component() || search().trim());

  return <>
    <PageHead crumb="Dashboard" title="Incidents" subtitle="Lag, recoveries, heartbeat failures and operator actions, as the scheduler recorded them."
      actions={<><Stale when={poll.stale()} /><A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>
        <button class="btn btn-sm" onClick={poll.refresh} disabled={poll.loading()}>Refresh</button></>} />
    <div class="page-body stack">
      <Show when={poll.error()}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>
      <Show when={poll.data()?.storage_error}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>

      <Rail>
        <Cell label="Open now" tone={open() > 0 ? 'warn' : 'ok'} value={formatCount(open())}
          foot={open() > 0 ? 'recovery not yet observed' : 'nothing ongoing'} />
        <Cell label={`In the last ${RANGES.find((r) => r.key === range())?.label ?? 'range'}`} value={formatCount(all().length)}
          foot={`${formatCount(warnings())} warning${warnings() === 1 ? '' : 's'}, ${formatCount(all().length - warnings())} informational`} />
        <Cell label="Longest episode" value={all().length ? formatDuration(longest()) : '—'} foot="start to recovery" />
        <Cell label="Latest" value={latest() ? formatRelativeTime(latest()!.started_at) : '—'}
          foot={latest() ? `${latest()!.component} · ${label(latest()!.kind)}` : 'no incidents recorded'} />
      </Rail>

      <Panel flush>
        <div class="toolbar">
          <div class="toolbar-groups">
            <Chips label="Range" value={range()} onChange={(k) => { setRange(k); setShown(60); }}
              options={RANGES.map((r) => ({ key: r.key, label: r.label }))} />
            <Chips label="State" value={state()} onChange={(k) => { setState(k); setShown(60); }}
              options={[{ key: '', label: 'All', count: all().length },
                ...STATES.filter((s) => stateCounts()[s]).map((s) => ({ key: s, label: label(s), count: stateCounts()[s], tone: tone(s) }))]} />
            <Show when={components().length > 1}>
              <Chips label="Component" value={component()} onChange={(k) => { setComponent(k); setShown(60); }}
                options={[{ key: '', label: 'All' }, ...components().map(([c, n]) => ({ key: c, label: c, count: n }))]} />
            </Show>
          </div>
          <div class="row">
            <input type="search" placeholder="Search kind or summary" value={search()} onInput={(e) => { setSearch(e.currentTarget.value); setShown(60); }} aria-label="Search incidents" />
            <Show when={filtering()}><button type="button" class="btn btn-sm" onClick={reset}>Clear</button></Show>
          </div>
        </div>

        <Show when={poll.data()} fallback={<Show when={!poll.error()}><SkeletonList rows={7} /></Show>}>
          <Show when={filtered().length} fallback={<Empty>{filtering() ? 'Nothing matches these filters.' : 'No incidents in this range.'}</Empty>}>
            <div class="feed" aria-busy={poll.loading()}>
              <For each={groups()}>{(group) => <>
                <div class="feed-day">{group.day}</div>
                <For each={group.items}>{(incident) => {
                  const last = incident.events[incident.events.length - 1];
                  return <div class="feed-row" role="link" tabIndex={0} onClick={() => navigate(`/incidents/${encodeURIComponent(incident.id)}`)}
                    onKeyDown={(e) => { if (e.key === 'Enter') navigate(`/incidents/${encodeURIComponent(incident.id)}`); }}>
                    <div class="feed-time" title={stamp(incident.started_at)}>{clock(incident.started_at)}</div>
                    <div class="feed-mark" classList={{ [tone(incident.state)]: true, open: incident.state === 'open' }} />
                    <div>
                      <div class="feed-title">
                        <span class="mono">{incident.component}</span>
                        <span>{label(incident.kind)}</span>
                        <Pill tone={tone(incident.state)} dot pulse={incident.state === 'open'}>{label(incident.state)}</Pill>
                        <Show when={incident.severity !== 'info'}><span class="id">{incident.severity}</span></Show>
                      </div>
                      <Show when={last?.summary}><div class="feed-summary">{last!.summary}</div></Show>
                    </div>
                    <div class="feed-meta">
                      <span title="duration">{duration(incident, now())}{incident.state === 'open' ? ' ongoing' : ''}</span>
                      <span title="recorded events">{formatCount(incident.event_count)} event{incident.event_count === 1 ? '' : 's'}</span>
                      <Show when={incident.max_buffered_events > 0}><span title="peak buffered events">{formatCount(incident.max_buffered_events)} buffered</span></Show>
                    </div>
                  </div>;
                }}</For>
              </>}</For>
            </div>
            <div class="panel-body spread" style={{ 'flex-wrap': 'wrap', gap: '12px' }}>
              <span class="dim" aria-live="polite">{Math.min(shown(), filtered().length)} of {formatCount(filtered().length)} shown
                <Show when={(poll.data()?.total ?? 0) > all().length}> · {formatCount(poll.data()!.total)} in the range, newest {WINDOW} loaded</Show></span>
              <Show when={filtered().length > shown()}><button class="btn btn-sm" onClick={() => setShown(shown() + 60)}>Show more</button></Show>
            </div>
          </Show>
        </Show>
      </Panel>
    </div>
  </>;
}

export function IncidentDetail() {
  const params = useParams();
  const path = createMemo(() => `/incidents/${encodeURIComponent(decodeParam(params.id))}`);
  const poll = usePoll<{ incident: Incident }>(path);
  return <>
    <PageHead crumb="Incidents" title={poll.data()?.incident.component ?? 'Incident detail'}
      subtitle={poll.data() ? label(poll.data()!.incident.kind) : undefined}
      actions={<><A class="btn btn-sm" href="/incidents">All incidents</A><A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>
        <button class="btn btn-sm" onClick={poll.refresh} disabled={poll.loading()}>Refresh</button></>} />
    <div class="page-body stack">
      <Show when={poll.error()}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>
      <Show when={poll.data()?.incident} fallback={<Show when={!poll.error()}><Panel flush><SkeletonList rows={4} /></Panel></Show>}>
        {(incident) => <>
          <Rail>
            <Cell label="State" tone={tone(incident().state)} value={label(incident().state)} foot={incident().severity} />
            <Cell label="Duration" value={duration(incident(), Date.now())} foot={incident().state === 'open' ? 'Recovery not yet observed' : 'Episode closed'} />
            <Cell label="Events" value={formatCount(incident().event_count)} foot="Recorded transitions" />
            <Cell label="Peak buffered" value={formatCount(incident().max_buffered_events)} foot="Events waiting for replay" />
          </Rail>
          <Show when={incident().state === 'interrupted'}><div class="banner"><span class="dot warn" />The scheduler restarted before recovery was observed. This incident is not confirmed recovered.</div></Show>
          <Panel title="Episode"><KeyValue rows={[
            ['Incident ID', <span class="mono">{incident().id}</span>],
            ['Started', stamp(incident().started_at)],
            ['Ended', incident().ended_at == null ? 'Still open' : stamp(incident().ended_at!)],
          ]} /></Panel>
          <Show when={incident().publication}>{(publication) => <Panel title="Publication backlog" sub="Sampled from SSP heartbeats during this incident. Peaks may fall between samples.">
            <KeyValue rows={[
              ['Peak pending operations', formatCount(incident().max_publication_operations)],
              ['Peak pending bytes', formatBytes(incident().max_publication_bytes)],
              ['Longest observed wait', formatDuration(incident().max_publication_age_ms ?? 0)],
              ['Parked batches at last nonempty sample', formatCount(publication().parked_batches)],
              ['Last successful publication', publication().last_success_at_ms == null ? 'Not reported' : stamp(publication().last_success_at_ms!)],
              ['Overload rejections since SSP start', formatCount(publication().overload_total)],
            ]} />
            <Show when={publication().connection}>{(connection) => <KeyValue rows={[
              ['Database session generation', formatCount(connection().generation)],
              ['Last reconnect attempt', connection().last_reconnect_duration_ms == null ? 'No reconnect attempted' : formatDuration(connection().last_reconnect_duration_ms!)],
              ['Failed reconnect attempts', formatCount(connection().reconnect_failures)],
            ]} />}</Show>
            <Show when={publication().worst_views.length}>
              <div class="table-scroll"><table><thead><tr><th>View</th><th>Pending operations</th><th>Pending bytes</th><th>Oldest wait</th></tr></thead>
                <tbody><For each={publication().worst_views}>{(view) => <tr>
                  <td><A class="mono" href={`/views/${encodeURIComponent(view.query_id)}`}>{view.query_id}</A></td>
                  <td>{formatCount(view.pending_operations)}</td><td>{formatBytes(view.pending_bytes)}</td><td>{formatDuration(view.oldest_age_ms)}</td>
                </tr>}</For></tbody></table></div>
            </Show>
          </Panel>}</Show>
          <Panel title="Timeline" sub={`${incident().events.length} retained of ${formatCount(incident().event_count)} recorded events. Oldest first.`}>
            <Show when={incident().events.length} fallback={<Empty>No timeline events retained.</Empty>}>
              <ol style={{ margin: '0', padding: '0', 'list-style': 'none' }}>
                <For each={incident().events}>{(event) => <li style={{ 'border-left': '2px solid var(--rule)', padding: '0 0 24px 20px', 'margin-left': '5px' }}>
                  <div class="row" style={{ 'flex-wrap': 'wrap', 'margin-bottom': '8px' }}><Pill tone={tone(event.state)} dot>{label(event.state)}</Pill>
                    <time class="dim mono" dateTime={new Date(event.at).toISOString()}>{stamp(event.at)}</time></div>
                  <div style={{ 'overflow-wrap': 'anywhere' }}>{event.summary}</div>
                  <div class="id" style={{ 'margin-top': '6px', 'overflow-wrap': 'anywhere' }}>{event.component} · {label(event.kind)} · v{event.version}
                    {event.operation_id ? ` · operation ${event.operation_id}` : ''}</div>
                </li>}</For>
              </ol>
            </Show>
          </Panel>
        </>}
      </Show>
    </div>
  </>;
}
