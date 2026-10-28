import { For, Show, createMemo, createResource, createSignal, onCleanup, type Accessor } from 'solid-js';
import { A, useNavigate, useParams } from '@solidjs/router';
import { api } from '../api/client';
import type { PublicationMetrics } from '../api/types';
import { Chevron, Chips, Empty, KeyValue, PageHead, Panel, SkeletonList, Stale, StatCard } from '../components/Chrome';
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
const sentence = (value: string) => value.charAt(0).toUpperCase() + value.slice(1);
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

/** One open incident: what broke, the latest word on it, how long so far. */
function OpenRow(props: { incident: Incident; now: number }) {
  const last = () => props.incident.events[props.incident.events.length - 1];
  return (
    <li>
      <A href={`/incidents/${encodeURIComponent(props.incident.id)}`} class="list-item warn">
        <span class="list-icon" aria-hidden="true">
          <svg viewBox="0 0 16 16"><path d="M8 2.8 14 13H2zM8 6.6v3M8 11.4h.01" /></svg>
        </span>
        <span class="list-text">
          <span class="list-title">
            <span class="mono">{props.incident.component}</span> {label(props.incident.kind)}
          </span>
          <Show when={last()?.summary}>
            <span class="list-detail">{last()!.summary}</span>
          </Show>
        </span>
        <span class="list-meta" title={`since ${stamp(props.incident.started_at)}`}>
          {duration(props.incident, props.now)} ongoing
        </span>
        <Chevron />
      </A>
    </li>
  );
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
  // Open episodes lead the page; the history below is everything that ended.
  const open = createMemo(() => all().filter((i) => i.state === 'open'));
  const past = createMemo(() => all().filter((i) => i.state !== 'open'));

  const components = createMemo(() => {
    const seen = new Map<string, number>();
    for (const i of past()) seen.set(i.component, (seen.get(i.component) ?? 0) + 1);
    return [...seen.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  });
  const stateCounts = createMemo(() => {
    const counts: Record<string, number> = {};
    for (const i of past()) counts[i.state] = (counts[i.state] ?? 0) + 1;
    return counts;
  });
  const filtered = createMemo(() => {
    const q = search().trim().toLowerCase();
    return past().filter((i) =>
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
  const reset = () => { setState(''); setComponent(''); setSearch(''); setShown(60); };
  const filtering = () => !!(state() || component() || search().trim());
  const go = (i: Incident) => navigate(`/incidents/${encodeURIComponent(i.id)}`);

  return <>
    <PageHead title="Incidents"
      actions={<><Stale when={poll.stale()} /><A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>
        <button class="btn btn-sm" onClick={poll.refresh} disabled={poll.loading()}>Refresh</button></>} />
    <div class="page-body page-stack">
      <Show when={poll.error()}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>
      <Show when={poll.data()?.storage_error}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>

      <Show when={poll.data()}>
        <Show when={open().length} fallback={
          <div class="list"><div class="list-item ok">
            <span class="list-icon" aria-hidden="true"><svg viewBox="0 0 16 16"><path d="m4 8.4 2.6 2.6L12 5.4" /></svg></span>
            <span class="list-text">
              <span class="list-title">Nothing open</span>
              <span class="list-detail">{all().length
                ? `Last incident ${formatRelativeTime(all()[0].started_at)}`
                : 'No incidents in this range.'}</span>
            </span>
          </div></div>
        }>
          <section class="inc-open">
            <h2 class="inc-h">{open().length} open</h2>
            <ul class="list"><For each={open()}>{(i) => <OpenRow incident={i} now={now()} />}</For></ul>
          </section>
        </Show>
      </Show>

      <Panel flush>
        <div class="toolbar inc-toolbar">
          <h2 class="inc-h">History</h2>
          <div class="toolbar-groups">
            <Chips value={range()} label="Range" onChange={(k) => { setRange(k); setShown(60); }}
              options={RANGES.map((r) => ({ key: r.key, label: r.label }))} />
            <Show when={Object.keys(stateCounts()).length > 1}>
              <Chips value={state()} label="State" onChange={(k) => { setState(k); setShown(60); }}
                options={[{ key: '', label: 'All' },
                  ...STATES.filter((s) => stateCounts()[s]).map((s) => ({ key: s, label: label(s), count: stateCounts()[s], tone: tone(s) }))]} />
            </Show>
            <Show when={components().length > 1}>
              <Chips value={component()} label="Component" onChange={(k) => { setComponent(k); setShown(60); }}
                options={[{ key: '', label: 'All' }, ...components().map(([c, n]) => ({ key: c, label: c, count: n }))]} />
            </Show>
          </div>
          <div class="row inc-search">
            <input type="search" placeholder="Search" value={search()} onInput={(e) => { setSearch(e.currentTarget.value); setShown(60); }} aria-label="Search incidents" />
            <Show when={filtering()}><button type="button" class="btn btn-sm" onClick={reset}>Clear</button></Show>
          </div>
        </div>

        <Show when={poll.data()} fallback={<Show when={!poll.error()}><SkeletonList rows={7} /></Show>}>
          <Show when={filtered().length} fallback={<Empty>{filtering() ? 'Nothing matches these filters.' : 'Nothing has ended in this range.'}</Empty>}>
            <div class="inc-feed" aria-busy={poll.loading()}>
              <For each={groups()}>{(group) => <>
                <div class="inc-day">{group.day}</div>
                <For each={group.items}>{(incident) => {
                  const last = incident.events[incident.events.length - 1];
                  return <div class="inc-row" role="link" tabIndex={0} onClick={() => go(incident)}
                    onKeyDown={(e) => { if (e.key === 'Enter') go(incident); }}>
                    <span class="inc-time" title={stamp(incident.started_at)}>{clock(incident.started_at)}</span>
                    <span class={`inc-dot ${tone(incident.state)}`} title={label(incident.state)} />
                    <span class="inc-text">
                      <span class="inc-title"><span class="mono">{incident.component}</span> {label(incident.kind)}</span>
                      <Show when={last?.summary}><span class="inc-summary">{last!.summary}</span></Show>
                    </span>
                    <span class="inc-meta">
                      <span classList={{ [`inc-state-${tone(incident.state)}`]: true }}>{label(incident.state)}</span>
                      <span>{duration(incident, now())}</span>
                    </span>
                  </div>;
                }}</For>
              </>}</For>
            </div>
            <Show when={filtered().length > shown() || (poll.data()?.total ?? 0) > all().length}>
              <div class="inc-more">
                <span aria-live="polite">{Math.min(shown(), filtered().length)} of {formatCount(filtered().length)}
                  <Show when={(poll.data()?.total ?? 0) > all().length}>, newest {WINDOW} of {formatCount(poll.data()!.total)} loaded</Show></span>
                <Show when={filtered().length > shown()}><button class="btn btn-sm" onClick={() => setShown(shown() + 60)}>Show more</button></Show>
              </div>
            </Show>
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
    <PageHead title={poll.data()?.incident.component ?? 'Incident'}
      subtitle={poll.data() ? `${label(poll.data()!.incident.kind)}, started ${stamp(poll.data()!.incident.started_at)}` : undefined}
      actions={<><A class="btn btn-sm" href="/incidents">All incidents</A><A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>
        <button class="btn btn-sm" onClick={poll.refresh} disabled={poll.loading()}>Refresh</button></>} />
    <div class="page-body page-stack">
      <Show when={poll.error()}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>
      <Show when={poll.data()?.incident} fallback={<Show when={!poll.error()}><Panel flush><SkeletonList rows={4} /></Panel></Show>}>
        {(incident) => <>
          <Show when={incident().state === 'interrupted'}><div class="banner"><span class="dot warn" />The scheduler restarted before recovery was seen.</div></Show>
          <div class="stat-grid">
            <StatCard label="State" value={sentence(label(incident().state))} tone={tone(incident().state)} note={incident().severity} />
            <StatCard label="Duration" value={duration(incident(), Date.now())}
              note={incident().ended_at == null ? 'still open' : `ended ${stamp(incident().ended_at!)}`} />
            <StatCard label="Events" value={formatCount(incident().event_count)} />
            <Show when={incident().max_buffered_events > 0}>
              <StatCard label="Peak buffered" value={formatCount(incident().max_buffered_events)} note="waiting for replay" />
            </Show>
          </div>
          <Show when={incident().publication}>{(publication) => <Panel title="Publication backlog" sub="Sampled from SSP heartbeats; peaks may fall between samples.">
            <KeyValue rows={[
              ['Peak pending operations', formatCount(incident().max_publication_operations)],
              ['Peak pending bytes', formatBytes(incident().max_publication_bytes)],
              ['Longest wait', formatDuration(incident().max_publication_age_ms ?? 0)],
              ['Parked batches', formatCount(publication().parked_batches)],
              ['Last publication', publication().last_success_at_ms == null ? 'Not reported' : stamp(publication().last_success_at_ms!)],
              ['Overload rejections', formatCount(publication().overload_total)],
            ]} />
            <Show when={publication().connection}>{(connection) => <KeyValue rows={[
              ['Session generation', formatCount(connection().generation)],
              ['Last reconnect', connection().last_reconnect_duration_ms == null ? 'None' : formatDuration(connection().last_reconnect_duration_ms!)],
              ['Failed reconnects', formatCount(connection().reconnect_failures)],
            ]} />}</Show>
            <Show when={publication().worst_views.length}>
              <div class="table-scroll"><table><thead><tr><th>View</th><th>Pending ops</th><th>Pending bytes</th><th>Oldest wait</th></tr></thead>
                <tbody><For each={publication().worst_views}>{(view) => <tr>
                  <td><A class="mono" href={`/views/${encodeURIComponent(view.query_id)}`}>{view.query_id}</A></td>
                  <td>{formatCount(view.pending_operations)}</td><td>{formatBytes(view.pending_bytes)}</td><td>{formatDuration(view.oldest_age_ms)}</td>
                </tr>}</For></tbody></table></div>
            </Show>
          </Panel>}</Show>
          <Panel title="Timeline" sub={incident().events.length < incident().event_count
            ? `${incident().events.length} of ${formatCount(incident().event_count)} events kept, oldest first`
            : 'Oldest first'}>
            <Show when={incident().events.length} fallback={<Empty>No events kept.</Empty>}>
              <ol class="inc-tl">
                <For each={incident().events}>{(event) => <li class="inc-tl-item">
                  <span class={`inc-dot ${tone(event.state)}`} />
                  <div class="inc-tl-body">
                    <div class="inc-tl-head"><span class="inc-tl-state">{label(event.state)}</span>
                      <time class="inc-tl-time" dateTime={new Date(event.at).toISOString()}>{stamp(event.at)}</time></div>
                    <div class="inc-tl-text">{event.summary}</div>
                    <div class="inc-tl-meta id">{event.component} · {label(event.kind)} · v{event.version}
                      {event.operation_id ? ` · operation ${event.operation_id}` : ''}</div>
                  </div>
                </li>}</For>
              </ol>
            </Show>
          </Panel>
        </>}
      </Show>
    </div>
  </>;
}
