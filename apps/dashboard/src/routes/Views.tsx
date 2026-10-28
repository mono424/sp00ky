import {
  For,
  Show,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
} from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { api } from '../api/client';
import {
  Chevron,
  Chips,
  CopyId,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  SkeletonList,
  StatCard,
} from '../components/Chrome';
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
  splitValue,
} from '../lib/format';
import type { Presence, ViewDetailData, ViewsList } from '../api/types';

/**
 * Every registered live query, and everything known about one of them.
 *
 * The page answers which views cost something: hot (updating fast), slow
 * (p99 over the threshold), large (too many rows to republish) or erroring,
 * and who owns them. Conditions come first as rows that apply their filter;
 * the table below is the drill-in. User and SSP filters are chips built from
 * the presence sampler's rankings, so the common case is one click.
 *
 * A closed tab fades out over minutes (clients refresh on a `0.9 * ttl`
 * timer), so a lapsed row is marked rather than treated as a leak.
 */

const POLL_MS = 10_000;

type SortKey = 'slowest' | 'newest' | 'rows' | 'updates' | 'errors' | 'active';
type ShowKey = '' | 'hot' | 'slow' | 'large' | 'shared';

const SORTS: { key: SortKey; label: string }[] = [
  { key: 'slowest', label: 'Slowest' },
  { key: 'active', label: 'Last active' },
  { key: 'newest', label: 'Newest' },
  { key: 'rows', label: 'Rows' },
  { key: 'updates', label: 'Updates' },
  { key: 'errors', label: 'Errors' },
];

const plural = (n: number, one: string, many: string) => `${formatCount(n)} ${n === 1 ? one : many}`;

/** A warning glyph for the condition rows. */
function WarnIcon() {
  return (
    <span class="list-icon" aria-hidden="true">
      <svg viewBox="0 0 16 16">
        <path d="M8 2.8 14 13H2zM8 6.6v3M8 11.4h.01" />
      </svg>
    </span>
  );
}

export function Views() {
  const [sort, setSort] = createSignal<SortKey>('slowest');
  const [show, setShow] = createSignal<ShowKey>('');
  const [user, setUser] = createSignal('');
  const [ssp, setSsp] = createSignal('');
  const [search, setSearch] = createSignal('');

  // `/presence` is served from the scheduler's sampler memory, so this is a
  // cheap request: it carries the counts, the rankings and the slow threshold
  // the filters need.
  const [presence, { refetch: refetchPresence }] = createResource(() =>
    api.getResult<Presence>('/presence'),
  );
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
    if (show() === 'shared') params.set('shared', 'true');
    if (show() === 'slow' && extra()) params.set('slow_ms', String(extra()!.slow_ms));
    if (show() === 'large') params.set('large', 'true');
    if (show() === 'hot') params.set('hot', 'true');
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
    void refetch();
    void refetchPresence();
  }, POLL_MS);
  onCleanup(() => clearInterval(timer));

  const now = () => data()?.server_time_ms ?? Date.now();
  const totals = () => extra()?.totals;
  const filtering = () => !!(show() || user() || ssp() || search());
  const reset = () => {
    setShow('');
    setUser('');
    setSsp('');
    setSearch('');
  };

  /** The chosen value stays a chip even when it fell out of the ranking. */
  const userOptions = () => {
    const top = extra()?.top_users ?? [];
    const opts: { key: string; label: string; count?: number }[] = top.map((u) => ({
      key: u.auth_id,
      label: u.auth_id,
      count: u.views,
    }));
    if (user() && !top.some((u) => u.auth_id === user())) opts.push({ key: user(), label: user() });
    return [{ key: '', label: 'Anyone' }, ...opts];
  };
  const sspOptions = () => [
    { key: '', label: 'All' },
    ...(extra()?.by_ssp ?? []).map((s) => ({ key: s.ssp_id, label: s.ssp_id, count: s.views })),
  ];

  return (
    <>
      <PageHead
        title="Views"
        subtitle={
          <Show when={totals()}>
            {(t) => `${formatCount(t().views)} live, ${formatCount(t().shared_views)} shared`}
          </Show>
        }
        actions={
          <Show when={extra()?.truncated}>
            <Pill tone="warn">sampler capped, counts are a floor</Pill>
          </Show>
        }
      />

      <div class="page-body page-stack">
        {/* What costs something right now, one row each, worst first. A hot
            view is churning its SSP's publication queue and every
            subscriber's socket (a bulk import into an open table, typically);
            a large one is republished row by row on every cold registration
            and has stalled a tenant database before. */}
        <Show when={(totals()?.errored_views ?? 0) + (totals()?.hot_views ?? 0) + (totals()?.large_views ?? 0) > 0}>
          <ul class="list">
            <Show when={totals()!.errored_views > 0}>
              <li>
                <button type="button" class="list-item bad vw-row" onClick={() => setSort('errors')}>
                  <span class="list-icon" aria-hidden="true">
                    <svg viewBox="0 0 16 16">
                      <path d="M8 1.8a6.2 6.2 0 1 0 0 12.4A6.2 6.2 0 0 0 8 1.8zM5.9 5.9l4.2 4.2m0-4.2-4.2 4.2" />
                    </svg>
                  </span>
                  <span class="list-text">
                    <span class="list-title">{plural(totals()!.errored_views, 'view is', 'views are')} erroring</span>
                    <span class="list-detail">Their clients see stale data.</span>
                  </span>
                  <span class="list-meta">Sort by errors</span>
                </button>
              </li>
            </Show>
            <For each={extra()?.hot.slice(0, 3) ?? []}>
              {(h) => (
                <li>
                  <A href={`/views/${encodeURIComponent(h.key)}`} class="list-item warn">
                    <WarnIcon />
                    <span class="list-text">
                      <span class="list-title">
                        Hot view, {formatRate(h.per_sec)} for {h.auth_id || 'anonymous'}
                      </span>
                      <span class="list-detail">
                        Usually a bulk write into a table this view watches. Pace the writer or window the query.
                      </span>
                    </span>
                    <span class="list-meta mono">{h.ssp_id ?? ''}</span>
                    <Chevron />
                  </A>
                </li>
              )}
            </For>
            <Show when={totals()!.large_views > 0}>
              <li>
                <button type="button" class="list-item warn vw-row" onClick={() => setShow('large')}>
                  <WarnIcon />
                  <span class="list-text">
                    <span class="list-title">
                      {plural(totals()!.large_views, 'view holds', 'views hold')}{' '}
                      {formatCount(extra()!.large_view_rows)}+ rows
                    </span>
                    <span class="list-detail">
                      Every cold registration republishes each row. Window it with LIMIT or resolve rows by id.
                    </span>
                  </span>
                  <span class="list-meta">Show them</span>
                </button>
              </li>
            </Show>
          </ul>
        </Show>

        <Panel flush>
          <div class="toolbar">
            <div class="toolbar-groups">
              <Chips
                label="Show"
                value={show()}
                onChange={(k) => setShow(k as ShowKey)}
                options={[
                  { key: '', label: 'All', count: totals()?.views },
                  { key: 'hot', label: 'Hot', count: totals()?.hot_views, tone: 'warn' },
                  { key: 'slow', label: 'Slow', count: totals()?.slow_views, tone: 'warn' },
                  { key: 'large', label: 'Large', count: totals()?.large_views, tone: 'warn' },
                  { key: 'shared', label: 'Shared', count: totals()?.shared_views },
                ]}
              />
              <Chips
                label="Sort"
                value={sort()}
                onChange={(k) => setSort(k as SortKey)}
                options={SORTS}
              />
            </div>
            <div class="row">
              <input
                type="search"
                placeholder="Search the query"
                value={search()}
                onInput={(e) => setSearch(e.currentTarget.value)}
                aria-label="Search the query"
              />
              <Show when={filtering()}>
                <button type="button" class="btn btn-sm" onClick={reset}>
                  Clear
                </button>
              </Show>
            </div>
          </div>
          <Show when={userOptions().length > 2 || sspOptions().length > 2}>
            <div class="toolbar vw-owners">
              <div class="toolbar-groups">
                <Show when={userOptions().length > 1}>
                  <Chips label="User" value={user()} onChange={setUser} options={userOptions()} />
                </Show>
                <Show when={sspOptions().length > 2}>
                  <Chips label="SSP" value={ssp()} onChange={setSsp} options={sspOptions()} />
                </Show>
              </div>
            </div>
          </Show>

          <Show when={error()}>{(msg) => <Empty>{msg()}</Empty>}</Show>

          <Show when={data()} fallback={<Show when={!error()}><SkeletonList rows={6} /></Show>}>
            {(d) => (
              <Show
                when={d().views.length > 0}
                fallback={<Empty>No live views match.</Empty>}
              >
                <div class="table-scroll">
                  <table class="vw-table">
                    <thead>
                      <tr>
                        <th>Query</th>
                        <th>User</th>
                        <th class="num">Rows</th>
                        <th class="num">p99</th>
                        <th class="num">Updates</th>
                        <th class="num">Errors</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={d().views}>
                        {(v) => {
                          const slow = () => v.p99 !== null && v.p99 >= d().slow_ms;
                          const large = () => v.row_count >= d().large_view_rows;
                          const lapsed = () => v.expires_at_ms !== null && v.expires_at_ms <= now();
                          return (
                            <tr classList={{ 'vw-lapsed': lapsed() }}>
                              <td class="vw-query">
                                <A href={`/views/${encodeURIComponent(v.key)}`}>
                                  <span class="vw-surql mono" title={v.surql ?? v.key}>
                                    {v.surql ?? v.key}
                                  </span>
                                </A>
                                <span class="vw-sub">
                                  <span class="mono">{v.ssp_id ?? 'unassigned'}</span>
                                  <Show when={v.shared}>
                                    <span>shared by {formatCount(v.subscriber_count)}</span>
                                  </Show>
                                  <Show when={lapsed()}>
                                    <span>lapsed</span>
                                  </Show>
                                </span>
                              </td>
                              <td class="vw-user" data-label="User">
                                <button
                                  type="button"
                                  class="vw-link"
                                  onClick={() => setUser(v.auth_id)}
                                  title="Only this user's views"
                                  disabled={!v.auth_id}
                                >
                                  {v.auth_id || 'anonymous'}
                                </button>
                              </td>
                              <td class="num" classList={{ 'tone-warn': large() }} data-label="Rows">
                                {formatCount(v.row_count)}
                              </td>
                              <td class="num" classList={{ 'tone-warn': slow() }} data-label="p99">
                                {formatMs(v.p99)}
                              </td>
                              <td class="num" data-label="Updates">
                                {formatCount(v.update_count)}
                                <Show when={v.hot}>
                                  <span class="vw-rate tone-warn">{formatRate(v.update_rate)}</span>
                                </Show>
                              </td>
                              <td class="num" classList={{ 'tone-bad': !!v.error_count, faint: !v.error_count }} data-label="Errors">
                                {formatCount(v.error_count)}
                              </td>
                            </tr>
                          );
                        }}
                      </For>
                    </tbody>
                  </table>
                </div>
                <Show when={d().total > d().returned}>
                  <div class="vw-foot">
                    Showing {formatCount(d().returned)} of {formatCount(d().total)}
                  </div>
                </Show>
              </Show>
            )}
          </Show>
        </Panel>
      </div>
    </>
  );
}

export function ViewDetail() {
  const params = useParams<{ key: string }>();
  // Same raw-path-segment caveat as the other detail views: the router does not
  // decode, so a key would otherwise be encoded twice on the way to the API.
  const key = () => decodeParam(params.key);

  const [result, { refetch }] = createResource(key, (k) =>
    api.getResult<ViewDetailData>(`/views/${encodeURIComponent(k)}`),
  );
  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  const timer = setInterval(refetch, POLL_MS);
  onCleanup(() => clearInterval(timer));

  const view = () => data()?.view;
  const slow = () => {
    const d = data();
    return !!d && view()?.p99 !== null && (view()!.p99 as number) >= d.slow_ms;
  };

  return (
    <>
      <PageHead
        title="View"
        subtitle={<CopyId value={key()} />}
        actions={
          <Show when={view()}>
            {(v) => (
              <>
                <Show when={v().expired}>
                  <Pill tone="idle">lapsed</Pill>
                </Show>
                <Show when={v().shared}>
                  <Pill tone="ok">shared</Pill>
                </Show>
                <Show when={slow()}>
                  <Pill tone="warn">slow</Pill>
                </Show>
                <Show when={v().hot}>
                  <Pill tone="warn" dot>
                    hot
                  </Pill>
                </Show>
              </>
            )}
          </Show>
        }
      />

      <div class="page-body">
        <Show when={error()}>
          {(msg) => (
            <Panel>
              <Empty>{msg()}</Empty>
            </Panel>
          )}
        </Show>

        <Show when={data()} fallback={<Show when={!error()}><SkeletonList rows={5} /></Show>}>
          {(d) => {
            const v = () => d().view;
            const p99 = () => splitValue(formatMs(v().p99));
            const heap = () => {
              const sv = d().ssp?.view;
              return sv ? (sv.view_bytes ?? 0) + (sv.operator_bytes ?? 0) : null;
            };
            return (
              <div class="page-stack">
                <div class="stat-grid">
                  <StatCard
                    label="Rows"
                    value={formatCount(v().row_count)}
                    tone={v().row_count >= d().large_view_rows ? 'warn' : undefined}
                    note={heap() !== null ? `${formatBytes(heap())} in the SSP` : undefined}
                  />
                  <StatCard
                    label="p99"
                    value={p99().value}
                    unit={p99().unit}
                    tone={slow() ? 'warn' : undefined}
                    note={`p90 ${formatMs(v().p90)}`}
                  />
                  <StatCard
                    label="Updates"
                    value={formatCount(v().update_count)}
                    tone={v().hot ? 'warn' : undefined}
                    note={
                      (v().update_rate ?? 0) > 0
                        ? `${formatRate(v().update_rate)} now`
                        : v().update_rate_peak !== null && v().update_rate_peak_at_ms !== null
                          ? `peaked ${formatRate(v().update_rate_peak)} ${formatRelativeTime(v().update_rate_peak_at_ms!)}`
                          : undefined
                    }
                  />
                  <StatCard
                    label="Errors"
                    value={formatCount(v().error_count)}
                    tone={v().error_count ? 'bad' : undefined}
                  />
                </div>

                <Panel title="Query">
                  <pre class="json vw-pre">{v().surql ?? '—'}</pre>
                </Panel>

                <div class="grid grid-2">
                  <Panel title="Owner">
                    <KeyValue
                      rows={[
                        ['User', v().auth_id || 'anonymous'],
                        ['Session', v().client_id ? <CopyId value={v().client_id} /> : '—'],
                        ['SSP', v().ssp_id ?? 'unassigned'],
                        ['Registered', formatStamp(v().created_at)],
                        ['Last active', relativeStamp(v().last_active_at)],
                        ['Expires', isAbsent(v().expires_at) ? '—' : relativeStamp(v().expires_at)],
                        [
                          'Same query',
                          d().siblings.sessions <= 1
                            ? 'only this session'
                            : `${plural(d().siblings.sessions, 'session', 'sessions')}, ${plural(d().siblings.users, 'user', 'users')}`,
                        ],
                      ]}
                    />
                  </Panel>

                  <Panel title="Params">
                    <pre class="json">{JSON.stringify(v().params ?? null, null, 2)}</pre>
                  </Panel>
                </div>

                <Show when={d().siblings.rows.length > 0}>
                  <Panel title="Same query elsewhere" flush>
                    <ul class="list">
                      <For each={d().siblings.rows}>
                        {(r) => (
                          <li>
                            <A href={`/views/${encodeURIComponent(r.key)}`} class="list-item">
                              <span class="list-text">
                                <span class="list-title">{r.auth_id || 'anonymous'}</span>
                                <span class="list-detail mono">{r.key}</span>
                              </span>
                              <span class="list-meta">
                                {formatCount(r.row_count)} rows, active {relativeStamp(r.last_active_at)}
                              </span>
                              <Chevron />
                            </A>
                          </li>
                        )}
                      </For>
                    </ul>
                  </Panel>
                </Show>

                <Show when={v().subscribers.length > 1}>
                  <Panel title="Subscribers" flush>
                    <ul class="list">
                      <For each={v().subscribers}>
                        {(sub) => (
                          <li>
                            <div class="list-item">
                              <span class="list-text">
                                <span class="list-title mono">{sub.id}</span>
                              </span>
                              <span class="list-meta" classList={{ 'tone-warn': sub.stale }}>
                                {sub.stale ? 'stale, ' : ''}seen {formatUptime(sub.age_secs)} ago
                              </span>
                            </div>
                          </li>
                        )}
                      </For>
                    </ul>
                  </Panel>
                </Show>
              </div>
            );
          }}
        </Show>
      </div>
    </>
  );
}
