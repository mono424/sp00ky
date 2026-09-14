import { For, Show, batch, createMemo, createResource, createSignal, onCleanup, type Accessor } from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { api } from '../api/client';
import type { PublicationMetrics } from '../api/types';
import { Cell, Empty, KeyValue, PageHead, Panel, Pill, Rail } from '../components/Chrome';
import { decodeParam, formatBytes, formatCount, formatDuration, formatRelativeTime } from '../lib/format';

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

const STATES = ['open', 'recovered', 'interrupted', 'failed', 'recorded'];
const PAGE_SIZE = 25;
const label = (value: string) => value.replace(/_/g, ' ');
const stamp = (value: number) => new Date(value).toLocaleString();
const tone = (state: string) => state === 'recovered' ? 'ok'
  : state === 'failed' ? 'bad' : state === 'open' || state === 'interrupted' ? 'warn' : 'idle';
const duration = (incident: Incident, now: number) =>
  formatDuration(Math.max(0, (incident.ended_at ?? now) - incident.started_at));

/** Match the dashboard's resource polling, with cleanup and no overlapping polls. */
function usePoll<T>(path: Accessor<string>) {
  const [result, { refetch }] = createResource(path, (url) => api.getResult<T>(url));
  const timer = setInterval(() => {
    if (!result.loading && !document.hidden) void refetch();
  }, 5000);
  onCleanup(() => clearInterval(timer));
  return {
    data: () => { const r = result(); return r?.ok ? r.value : undefined; },
    error: () => { const r = result(); return r && !r.ok ? r.message : undefined; },
    loading: () => result.loading,
    refresh: () => { if (!result.loading) void refetch(); },
  };
}

export function Incidents() {
  const [component, setComponent] = createSignal('');
  const [state, setState] = createSignal('');
  const [severity, setSeverity] = createSignal('');
  const [since, setSince] = createSignal('');
  const [before, setBefore] = createSignal('');
  const [filters, setFilters] = createSignal('');
  const [offset, setOffset] = createSignal(0);
  const path = createMemo(() => `/incidents?limit=${PAGE_SIZE}&offset=${offset()}${filters()}`);
  const poll = usePoll<IncidentList>(path);

  const apply = (event: SubmitEvent) => {
    event.preventDefault();
    const query = new URLSearchParams();
    if (component().trim()) query.set('component', component().trim());
    if (state()) query.set('state', state());
    if (severity()) query.set('severity', severity());
    if (since()) query.set('since', String(Date.parse(since())));
    if (before()) query.set('before', String(Date.parse(before())));
    batch(() => {
      setOffset(0);
      setFilters(query.size ? `&${query}` : '');
    });
  };
  const reset = () => batch(() => {
    setComponent(''); setState(''); setSeverity(''); setSince(''); setBefore('');
    setOffset(0); setFilters('');
  });

  return <>
    <PageHead crumb="Dashboard" title="Incidents" subtitle="Recorded failures, recovery and operator actions. Refreshes every 5 seconds."
      actions={<><A class="btn btn-sm" href="/logs?source=scheduler">Scheduler logs</A>
        <button class="btn btn-sm" onClick={poll.refresh} disabled={poll.loading()}>Refresh</button></>} />
    <div class="page-body stack">
      <Panel title="History" sub="Newest first. Filter by component, state, severity or start time.">
        <form class="filters" onSubmit={apply}>
          <label>Component <input type="text" placeholder="e.g. ssp-0" value={component()} onInput={(e) => setComponent(e.currentTarget.value)} /></label>
          <label>State <select value={state()} onChange={(e) => setState(e.currentTarget.value)}>
            <option value="">All states</option><For each={STATES}>{(s) => <option value={s}>{label(s)}</option>}</For>
          </select></label>
          <label>Severity <select value={severity()} onChange={(e) => setSeverity(e.currentTarget.value)}>
            <option value="">All severities</option><option value="info">Info</option><option value="warning">Warning</option><option value="error">Error</option>
          </select></label>
          <label>From <input type="datetime-local" value={since()} max={before() || undefined} onInput={(e) => setSince(e.currentTarget.value)} /></label>
          <label>Through <input type="datetime-local" value={before()} min={since() || undefined} onInput={(e) => setBefore(e.currentTarget.value)} /></label>
          <button type="submit" class="btn btn-primary btn-sm">Apply filters</button>
          <button type="button" class="btn btn-sm" onClick={reset}>Clear</button>
        </form>
      </Panel>
      <Show when={poll.error()}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>
      <Show when={poll.data()} fallback={<Show when={!poll.error()}><Empty>Loading incidents…</Empty></Show>}>
        {(data) => <>
          <Show when={data().storage_error}>{(error) => <div class="banner" role="alert"><span class="dot bad" />{error()}</div>}</Show>
          <Panel title={`${formatCount(data().total)} incident${data().total === 1 ? '' : 's'}`} flush>
            <Show when={data().incidents.length} fallback={<Empty>{filters() ? 'No incidents match these filters.' : 'No incidents recorded yet.'}</Empty>}>
              <div class="table-scroll" aria-busy={poll.loading()}><table>
                <thead><tr><th>Incident</th><th>State</th><th>Severity</th><th>Started</th><th>Duration</th><th>Events</th><th>Peak buffered</th></tr></thead>
                <tbody><For each={data().incidents}>{(incident) => <tr>
                  <td><A href={`/incidents/${encodeURIComponent(incident.id)}`}>{incident.component} · {label(incident.kind)}</A>
                    <div class="id" style={{ 'margin-top': '4px', 'white-space': 'normal' }}>{incident.events[incident.events.length - 1]?.summary}</div></td>
                  <td data-label="State"><Pill tone={tone(incident.state)} dot pulse={incident.state === 'open'}>{label(incident.state)}</Pill></td>
                  <td data-label="Severity" class="dim">{label(incident.severity)}</td>
                  <td data-label="Started" class="dim" title={stamp(incident.started_at)}>{formatRelativeTime(incident.started_at)}</td>
                  <td data-label="Duration" class="mono">{duration(incident, data().server_time_ms)}{incident.state === 'open' ? ' ongoing' : ''}</td>
                  <td data-label="Events" class="mono">{formatCount(incident.event_count)}</td>
                  <td data-label="Peak buffered" class="mono">{formatCount(incident.max_buffered_events)}</td>
                </tr>}</For></tbody>
              </table></div>
            </Show>
            <div class="panel-body spread" style={{ 'flex-wrap': 'wrap', gap: '12px' }}>
              <span class="dim" aria-live="polite">{data().incidents.length ? `${data().offset + 1}-${data().offset + data().incidents.length} of ${formatCount(data().total)}` : '0 shown'}</span>
              <div class="row"><button class="btn btn-sm" disabled={poll.loading() || offset() === 0} onClick={() => setOffset(Math.max(0, offset() - PAGE_SIZE))}>Previous</button>
                <button class="btn btn-sm" disabled={poll.loading() || data().offset + data().incidents.length >= data().total} onClick={() => setOffset(offset() + PAGE_SIZE)}>Next</button></div>
            </div>
          </Panel>
        </>}
      </Show>
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
      <Show when={poll.data()?.incident} fallback={<Show when={!poll.error()}><Empty>Loading incident…</Empty></Show>}>
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
