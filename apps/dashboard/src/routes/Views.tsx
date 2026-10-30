import { For, Show, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { api } from '../api/client';
import { PageHead } from '../components/Chrome';
import { FilterMenu, FxCell, LineChart, SortMenu, Sq, type FilterValue } from '../components/Frame';
import {
  decodeParam,
  formatBytes,
  formatCount,
  formatMs,
  formatRate,
  formatRelativeTime,
  formatStamp,
  formatUptime,
  isAbsent,
  relativeStamp,
} from '../lib/format';
import type { Presence, ViewDetailData, ViewsList } from '../api/types';

/**
 * Every registered live query, and everything known about one of them.
 *
 * The page answers which views cost something: hot (updating fast), slow
 * (p99 over the threshold), large (too many rows to republish) or erroring,
 * and who owns them. The counts across the top are filters themselves; the
 * SSP and top-user tables below them filter on a click; the list at the
 * bottom takes the same filters from its Filter menu, plus sort and search.
 *
 * A closed tab fades out over minutes (clients refresh on a `0.9 * ttl`
 * timer), so a lapsed row is marked rather than treated as a leak.
 */

const POLL_MS = 10_000;

type SortKey = 'slowest' | 'newest' | 'rows' | 'updates' | 'errors' | 'active';
type Condition = 'hot' | 'slow' | 'large' | 'shared';

const SORTS: { key: SortKey; label: string }[] = [
  { key: 'slowest', label: 'Slowest' },
  { key: 'active', label: 'Last active' },
  { key: 'newest', label: 'Newest' },
  { key: 'rows', label: 'Most rows' },
  { key: 'updates', label: 'Most updates' },
  { key: 'errors', label: 'Most errors' },
];

/** Shades of the accent, one per SSP. */
const SHADES = ['#c6cbea', '#9ca5d3', '#7f88bd', '#b7bede', '#6f78a8', '#a8adc4', '#8e96b8', '#5f68ad'];

const plural = (n: number, one: string, many: string) => `${formatCount(n)} ${n === 1 ? one : many}`;

export function Views() {
  const [sort, setSort] = createSignal<SortKey>('slowest');
  const [filters, setFilters] = createSignal<FilterValue>({});
  const [search, setSearch] = createSignal('');
  const conditions = () => (filters().condition ?? []) as Condition[];
  const user = () => filters().user?.[0] ?? '';
  const ssp = () => filters().ssp?.[0] ?? '';
  const has = (c: Condition) => conditions().includes(c);
  // `/presence` is served from the scheduler's sampler memory, so this is a
  // cheap request: it carries the counts, the rankings and the thresholds.
  const [presence, { refetch: refetchPresence }] = createResource(() => api.getResult<Presence>('/presence'));
  const extra = () => {
    const r = presence();
    return r?.ok ? r.value : undefined;
  };

  const query = createMemo(() => {
    const params = new URLSearchParams();
    params.set('sort', sort());
    if (user()) params.set('user', user());
    if (ssp()) params.set('ssp', ssp());
    if (search()) params.set('q', search());
    // The conditions combine: each narrows the list further.
    if (has('shared')) params.set('shared', 'true');
    if (has('slow') && extra()) params.set('slow_ms', String(extra()!.slow_ms));
    if (has('large')) params.set('large', 'true');
    if (has('hot')) params.set('hot', 'true');
    return `/views?${params.toString()}`;
  });

  const [result, { refetch }] = createResource(query, (q) => api.getResult<ViewsList>(q));
  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  const timer = setInterval(() => {
    if (document.hidden) return;
    void refetch();
    void refetchPresence();
  }, POLL_MS);
  onCleanup(() => clearInterval(timer));

  const now = () => data()?.server_time_ms ?? Date.now();
  const totals = () => extra()?.totals;
  const filtering = () => !!(conditions().length || user() || ssp() || search());
  const reset = () => {
    setFilters({});
    setSearch('');
  };

  const bySsp = () => [...(extra()?.by_ssp ?? [])].sort((a, b) => a.ssp_id.localeCompare(b.ssp_id));
  const sspViews = () => bySsp().reduce((n, s) => n + s.views, 0);
  const topUsers = () => (extra()?.top_users ?? []).slice(0, 6);
  const userMax = () => Math.max(1, ...topUsers().map((u) => u.views));

  /** The chosen user stays an option even when it fell out of the ranking. */
  const userOptions = () => {
    const top = extra()?.top_users ?? [];
    const opts = top.map((u) => ({ value: u.auth_id, label: u.auth_id || 'anonymous', count: u.views }));
    if (user() && !top.some((u) => u.auth_id === user())) opts.push({ value: user(), label: user(), count: undefined as unknown as number });
    return opts;
  };

  // Live views over the presence sampler's window, on the scheduler's clock.
  const viewPoints = () => (extra()?.samples ?? []).map((x) => ({ ts: x.ts, v: x.views }));

  return (
    <>
      <PageHead
        title="Views"
        actions={
          <>
            <Show when={extra()?.truncated}>
              <span class="cap warn">sampler capped · counts are a floor</span>
            </Show>
          </>
        }
      />

      <div class="fx-page">
        {/* how many, and what costs something ============================ */}
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
          <FxCell
            label="Live views"
            meta={
              <Show when={totals()}>
                {(t) => (
                  <>
                    <span><b>{formatCount(t().shared_views)}</b> shared</span>
                    <span><b>{formatCount(t().sessions)}</b> tabs</span>
                    <span><b>{formatCount(t().users)}</b> users</span>
                  </>
                )}
              </Show>
            }
          >
            <div class="fx-big">{totals() ? formatCount(totals()!.views) : '—'}</div>
            <LineChart points={viewPoints()} format={(v) => formatCount(Math.round(v))} now={extra()?.taken_at_ms ?? Date.now()} />
          </FxCell>
          <FxCell label="Costing something" meta={<span>now</span>}>
            <div class="vw-conds">
              <Cond label="Hot" n={totals()?.hot_views} tone="warn" note={<>updating ≥ {extra() ? formatRate(extra()!.hot_view_rate) : '—'}</>} />
              <Cond label="Slow" n={totals()?.slow_views} tone="warn" note={<>p99 ≥ {extra() ? formatMs(extra()!.slow_ms) : '—'}</>} />
              <Cond label="Large" n={totals()?.large_views} tone="warn" note={<>≥ {extra() ? formatCount(extra()!.large_view_rows) : '—'} rows</>} />
              <Cond label="Erroring" n={totals()?.errored_views} tone="bad" note="clients see stale data" />
            </div>
          </FxCell>
        </section>

        {/* where they live, and whose they are ============================= */}
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 1fr)' }}>
          <FxCell label="By SSP" meta={<span><b>{bySsp().length}</b> serving</span>}>
            <div class="sp-share">
              <For each={bySsp()}>
                {(x, i) => <i style={{ 'flex-grow': x.views || 0.0001, background: SHADES[i() % SHADES.length] }} title={`${x.ssp_id}: ${formatCount(x.views)}`} />}
              </For>
            </div>
            <table class="fx-list vw-mini">
              <tbody>
                <For each={bySsp()}>
                  {(x, i) => (
                    <tr>
                      <td><span class="fx-who"><i class="vw-key" style={{ background: SHADES[i() % SHADES.length] }} /><span style={{ color: 'var(--ink)' }}>{x.ssp_id}</span></span></td>
                      <td class="r">{formatCount(x.views)}</td>
                      <td class="r" style={{ width: '18%' }}>{sspViews() ? Math.round((x.views / sspViews()) * 100) : 0}%</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
          <FxCell label="Top users" meta={<span>by live views</span>}>
            <table class="fx-list vw-mini">
              <tbody>
                <For each={topUsers()} fallback={<tr><td class="cap" style={{ color: 'var(--ink-ghost)' }}>Nobody is here</td></tr>}>
                  {(u) => (
                    <tr>
                      <td style={{ color: 'var(--ink)' }}>{u.auth_id || 'anonymous'}</td>
                      <td style={{ width: '40%' }}>
                        <span class="vw-bar"><i style={{ width: `${(u.views / userMax()) * 100}%` }} /></span>
                      </td>
                      <td class="r" style={{ width: '12%' }}>{formatCount(u.views)}</td>
                      <td class="r" style={{ width: '16%' }}>{plural(u.sessions, 'tab', 'tabs')}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </FxCell>
        </section>

        {/* churning right now ============================================== */}
        <Show when={(extra()?.hot.length ?? 0) > 0}>
          <section class="fx-row">
            <FxCell label="Hot right now" meta={<span>usually a bulk write into a table these watch</span>}>
              <For each={extra()!.hot.slice(0, 3)}>
                {(h) => (
                  <div class="fx-log">
                    <span class="cap">{formatRate(h.per_sec)}</span>
                    <span class="fx-what">{h.auth_id || 'anonymous'} · {h.ssp_id ?? 'unassigned'} <em>· {h.key}</em></span>
                    <A class="btn btn-sm" href={`/views/${encodeURIComponent(h.key)}`}>Open</A>
                  </div>
                )}
              </For>
            </FxCell>
          </section>
        </Show>

        {/* every view ====================================================== */}
        <section class="fx-row">
          <FxCell label="Live queries" meta={<Show when={data()}>{(d) => <span><b>{formatCount(d().returned)}</b>{d().total > d().returned ? ` of ${formatCount(d().total)}` : ''}</span>}</Show>}>
            <div class="inc-filters">
              <FilterMenu
                value={filters()}
                onChange={setFilters}
                fields={[
                  {
                    key: 'condition',
                    label: 'Condition',
                    options: [
                      { value: 'hot', label: 'hot', count: totals()?.hot_views, tone: 'warn' },
                      { value: 'slow', label: 'slow', count: totals()?.slow_views, tone: 'warn' },
                      { value: 'large', label: 'large', count: totals()?.large_views, tone: 'warn' },
                      { value: 'shared', label: 'shared', count: totals()?.shared_views, tone: '' },
                    ],
                  },
                  { key: 'user', label: 'User', single: true, options: userOptions() },
                  { key: 'ssp', label: 'SSP', single: true, options: (extra()?.by_ssp ?? []).map((s) => ({ value: s.ssp_id, label: s.ssp_id, count: s.views })) },
                ]}
              />
              <SortMenu options={SORTS.map((o) => ({ value: o.key, label: o.label }))} value={sort()} onChange={setSort} />
              <div class="inc-find">
                <input type="search" placeholder="Search the query" value={search()} onInput={(e) => setSearch(e.currentTarget.value)} aria-label="Search the query" />
                <Show when={filtering()}><button type="button" class="btn btn-sm" onClick={reset}>Clear all</button></Show>
              </div>
            </div>

            <Show when={error()}>{(msg) => <div class="fx-log bad"><span class="fx-what">{msg()}</span></div>}</Show>

            <Show when={data()} fallback={<Show when={!error()}><div class="fx-empty cap">Loading…</div></Show>}>
              {(d) => (
                <Show when={d().views.length > 0} fallback={<div class="fx-empty cap">No live view matches</div>}>
                  <table class="fx-list vw-list">
                    <colgroup>
                      <col /><col style={{ width: '15%' }} /><col style={{ width: '9%' }} /><col style={{ width: '9%' }} />
                      <col style={{ width: '13%' }} /><col style={{ width: '7%' }} />
                    </colgroup>
                    <thead>
                      <tr>
                        <th class="cap">Query</th><th class="cap">User</th><th class="cap r">Rows</th>
                        <th class="cap r">p99</th><th class="cap r">Updates</th><th class="cap r">Errors</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={d().views}>
                        {(v) => {
                          const slow = () => v.p99 !== null && v.p99 >= d().slow_ms;
                          const large = () => v.row_count >= d().large_view_rows;
                          const lapsed = () => v.expires_at_ms !== null && v.expires_at_ms <= now();
                          const tone = () => (v.error_count ? 'bad' : v.hot || slow() || large() ? 'warn' : lapsed() ? 'idle' : '');
                          return (
                            <tr classList={{ 'vw-lapsed': lapsed() }}>
                              <td class="vw-q">
                                <A href={`/views/${encodeURIComponent(v.key)}`} title={v.surql ?? v.key}>
                                  <Sq tone={tone()} />
                                  <span class="vw-surql">{v.surql ?? v.key}</span>
                                </A>
                                <span class="vw-meta cap">
                                  <span>{v.ssp_id ?? 'unassigned'}</span>
                                  <Show when={v.shared}><span>shared by {formatCount(v.subscriber_count)}</span></Show>
                                  <Show when={v.hot}><span class="warn">hot</span></Show>
                                  <Show when={lapsed()}><span>lapsed</span></Show>
                                </span>
                              </td>
                              <td>
                                <button type="button" class="vw-user" disabled={!v.auth_id}
                                  title="Only this user's views" onClick={() => setFilters({ ...filters(), user: [v.auth_id] })}>
                                  {v.auth_id || 'anonymous'}
                                </button>
                              </td>
                              <td class="r" classList={{ warn: large() }}>{formatCount(v.row_count)}</td>
                              <td class="r" classList={{ warn: slow() }}>{formatMs(v.p99)}</td>
                              <td class="r">
                                {formatCount(v.update_count)}
                                <Show when={v.hot}><span class="vw-rate">{formatRate(v.update_rate)}</span></Show>
                              </td>
                              <td class="r" classList={{ bad: !!v.error_count }} style={{ color: v.error_count ? undefined : 'var(--ink-ghost)' }}>
                                {formatCount(v.error_count)}
                              </td>
                            </tr>
                          );
                        }}
                      </For>
                    </tbody>
                  </table>
                </Show>
              )}
            </Show>
          </FxCell>
        </section>
      </div>
    </>
  );
}

/** One condition in the grid: its count, amber or red when above zero, and its threshold. */
function Cond(props: { label: string; n: number | undefined; tone: 'warn' | 'bad'; note: import('solid-js').JSX.Element }) {
  return (
    <div>
      <span class="cap">{props.label}</span>
      <span class="vw-n" classList={{ [props.tone]: (props.n ?? 0) > 0, zero: !props.n }}>
        <Sq tone={(props.n ?? 0) > 0 ? props.tone : 'idle'} />
        {props.n == null ? '—' : formatCount(props.n)}
      </span>
      <small class="cap">{props.note}</small>
    </div>
  );
}

export function ViewDetail() {
  const params = useParams<{ key: string }>();
  // Same raw-path-segment caveat as the other detail views: the router does not
  // decode, so a key would otherwise be encoded twice on the way to the API.
  const key = () => decodeParam(params.key);

  const [result, { refetch }] = createResource(key, (k) => api.getResult<ViewDetailData>(`/views/${encodeURIComponent(k)}`));
  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };
  const timer = setInterval(() => !document.hidden && void refetch(), POLL_MS);
  onCleanup(() => clearInterval(timer));

  const view = () => data()?.view;
  const slow = () => {
    const d = data();
    return !!d && view()?.p99 != null && (view()!.p99 as number) >= d.slow_ms;
  };

  return (
    <>
      <PageHead
        crumb="Views"
        title={key()}
        actions={
          <Show when={view()}>
            {(v) => (
              <span class="vw-tags cap">
                <Show when={v().hot}><span class="warn"><Sq tone="warn" />hot</span></Show>
                <Show when={slow()}><span class="warn"><Sq tone="warn" />slow</span></Show>
                <Show when={v().shared}><span><Sq />shared</span></Show>
                <Show when={v().expired}><span><Sq tone="idle" />lapsed</span></Show>
                <Show when={!v().hot && !slow() && !v().shared && !v().expired}><span><Sq />live</span></Show>
              </span>
            )}
          </Show>
        }
      />

      <Show when={error()}>{(msg) => <section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{msg()}</span></div></FxCell></section>}</Show>

      <Show when={data()} fallback={<Show when={!error()}><div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div></Show>}>
        {(d) => {
          const v = () => d().view;
          const heap = () => {
            const sv = d().ssp?.view;
            return sv ? (sv.view_bytes ?? 0) + (sv.operator_bytes ?? 0) : null;
          };
          return (
            <div class="fx-page">
              {/* the four numbers */}
              <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
                <FxCell label="Rows" meta={<Show when={v().row_count >= d().large_view_rows}><span class="warn">large</span></Show>}>
                  <div class="fx-big"><span classList={{ warn: v().row_count >= d().large_view_rows }}>{formatCount(v().row_count)}</span></div>
                  <div class="fx-sub cap"><span>{heap() !== null ? <><b>{formatBytes(heap())}</b> in the SSP</> : 'not measured on the SSP'}</span></div>
                </FxCell>
                <FxCell label="p99" meta={<Show when={slow()}><span class="warn">slow</span></Show>}>
                  <div class="fx-big"><span classList={{ warn: slow() }}>{formatMs(v().p99)}</span></div>
                  <div class="fx-sub cap"><span>p90 <b>{formatMs(v().p90)}</b></span><span>p55 <b>{formatMs(v().p55)}</b></span></div>
                </FxCell>
                <FxCell label="Updates" meta={<Show when={v().hot}><span class="warn">hot</span></Show>}>
                  <div class="fx-big">{formatCount(v().update_count)}</div>
                  <div class="fx-sub cap">
                    <Show when={(v().update_rate ?? 0) > 0}
                      fallback={<Show when={v().update_rate_peak != null && v().update_rate_peak_at_ms != null} fallback={<span>quiet</span>}>
                        <span>peaked <b>{formatRate(v().update_rate_peak)}</b> {formatRelativeTime(v().update_rate_peak_at_ms!)}</span>
                      </Show>}>
                      <span classList={{ warn: v().hot }}><b>{formatRate(v().update_rate)}</b> now</span>
                    </Show>
                  </div>
                </FxCell>
                <FxCell label="Errors">
                  <div class="fx-big"><span classList={{ bad: !!v().error_count }}>{formatCount(v().error_count)}</span></div>
                  <div class="fx-sub cap"><span>{v().error_count ? 'clients see stale data' : 'none'}</span></div>
                </FxCell>
              </section>

              {/* what it asks, and who for */}
              <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
                <FxCell label="Query" meta={<span>{d().siblings.sessions <= 1 ? 'only this session' : `${plural(d().siblings.sessions, 'session', 'sessions')} · ${plural(d().siblings.users, 'user', 'users')}`}</span>}>
                  <pre class="vw-pre">{v().surql ?? '—'}</pre>
                  <div class="fx-head" style={{ 'margin-top': '18px' }}><span class="cap">Params</span></div>
                  <pre class="vw-pre dim">{JSON.stringify(v().params ?? null, null, 2)}</pre>
                </FxCell>
                <FxCell label="Owner">
                  <div class="sp-kv vw-owner">
                    <div><span class="cap">User</span><b>{v().auth_id || 'anonymous'}</b></div>
                    <div><span class="cap">SSP</span><b>{v().ssp_id ?? 'unassigned'}</b></div>
                    <div style={{ 'grid-column': '1 / -1' }}><span class="cap">Session</span><b>{v().client_id || '—'}</b></div>
                    <div><span class="cap">Registered</span><b title={formatStamp(v().created_at)}>{relativeStamp(v().created_at)}</b></div>
                    <div><span class="cap">Last active</span><b>{relativeStamp(v().last_active_at)}</b></div>
                    <div><span class="cap">Expires</span><b classList={{ warn: !!v().expired }}>{isAbsent(v().expires_at) ? '—' : relativeStamp(v().expires_at)}</b></div>
                    <div><span class="cap">Subscribers</span><b>{formatCount(v().subscriber_count)}</b></div>
                  </div>
                </FxCell>
              </section>

              {/* the same query elsewhere, and who watches this one */}
              <Show when={d().siblings.rows.length > 0 || v().subscribers.length > 1}>
                <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1fr))' }}>
                  <FxCell label="Same query elsewhere" meta={<span><b>{d().siblings.rows.length}</b>{d().siblings.truncated ? '+' : ''}</span>}>
                    <Show when={d().siblings.rows.length} fallback={<div class="fx-empty cap">Only this session</div>}>
                      <table class="fx-list">
                        <thead><tr><th class="cap">User</th><th class="cap r">Rows</th><th class="cap r">Active</th></tr></thead>
                        <tbody>
                          <For each={d().siblings.rows}>
                            {(r) => (
                              <tr>
                                <td><A href={`/views/${encodeURIComponent(r.key)}`} style={{ color: 'var(--ink)' }}>{r.auth_id || 'anonymous'}</A></td>
                                <td class="r">{formatCount(r.row_count)}</td>
                                <td class="r">{relativeStamp(r.last_active_at)}</td>
                              </tr>
                            )}
                          </For>
                        </tbody>
                      </table>
                    </Show>
                  </FxCell>
                  <FxCell label="Subscribers" meta={<span><b>{v().subscribers.length}</b></span>}>
                    <Show when={v().subscribers.length} fallback={<div class="fx-empty cap">None</div>}>
                      <table class="fx-list">
                        <thead><tr><th class="cap">Session</th><th class="cap r">Seen</th></tr></thead>
                        <tbody>
                          <For each={v().subscribers}>
                            {(sub) => (
                              <tr>
                                <td><span class="fx-who"><Sq tone={sub.stale ? 'warn' : ''} /><span style={{ color: 'var(--ink)' }}>{sub.id}</span></span></td>
                                <td class="r" classList={{ warn: sub.stale }}>{sub.stale ? 'stale · ' : ''}{formatUptime(sub.age_secs)} ago</td>
                              </tr>
                            )}
                          </For>
                        </tbody>
                      </table>
                    </Show>
                  </FxCell>
                </section>
              </Show>
            </div>
          );
        }}
      </Show>
    </>
  );
}
