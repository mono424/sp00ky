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
  Cell,
  CopyId,
  Empty,
  KeyValue,
  PageHead,
  Panel,
  Pill,
  Rail,
} from '../components/Chrome';
import {
  decodeParam,
  formatBytes,
  formatCount,
  formatDuration,
  formatMs,
  formatStamp,
  formatUptime,
  isAbsent,
  relativeStamp,
} from '../lib/format';
import type {
  Presence,
  ViewDetailData,
  ViewSummary,
  ViewsList,
} from '../api/types';

/**
 * Every registered live query, and everything known about one of them.
 *
 * The rollup that used to head this page (users / sessions / views, and their
 * charts) lives on the Overview now: those numbers answer "is anyone here?",
 * which is a landing-page question. What is left here is the question you come
 * to this tab to ask — *which* queries are registered, who owns them, and which
 * ones are costing time.
 *
 * The caveat the page still has to carry: a client refreshes its liveness on a
 * `0.9 * ttl` timer, so a closed tab decays out of this listing over minutes
 * rather than disappearing from it. The per-row "expires" column is what makes
 * that legible instead of looking like a leak.
 */

const POLL_MS = 10_000;

type SortKey = 'slowest' | 'newest' | 'rows' | 'updates' | 'errors' | 'active';

const SORTS: { key: SortKey; label: string }[] = [
  { key: 'slowest', label: 'Slowest' },
  { key: 'active', label: 'Last active' },
  { key: 'newest', label: 'Newest' },
  { key: 'rows', label: 'Rows' },
  { key: 'updates', label: 'Updates' },
  { key: 'errors', label: 'Errors' },
];

/** How long until a view's TTL reclaims it, or how long ago it lapsed. */
function expiry(view: ViewSummary, now: number): { text: string; tone?: string } {
  if (view.expires_at_ms === null) return { text: '—' };
  const delta = view.expires_at_ms - now;
  if (delta <= 0) return { text: 'lapsed', tone: 'idle' };
  return { text: formatDuration(delta) };
}

export function Views() {
  const [sort, setSort] = createSignal<SortKey>('slowest');
  const [user, setUser] = createSignal('');
  const [ssp, setSsp] = createSignal('');
  const [search, setSearch] = createSignal('');
  const [sharedOnly, setSharedOnly] = createSignal(false);
  const [slowOnly, setSlowOnly] = createSignal(false);
  const [largeOnly, setLargeOnly] = createSignal(false);

  // `/presence` is served from the scheduler's sampler memory, so this is a
  // cheap request: it carries the ranking, the per-SSP split and the slow
  // threshold the table's filter needs.
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
    if (sharedOnly()) params.set('shared', 'true');
    if (slowOnly() && extra()) params.set('slow_ms', String(extra()!.slow_ms));
    if (largeOnly()) params.set('large', 'true');
    return `/views?${params.toString()}`;
  });

  const [result, { refetch }] = createResource(query, (q) =>
    api.getResult<ViewsList>(q),
  );
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

  return (
    <>
      <PageHead
        crumb="Dashboard"
        title="Views"
        subtitle="Every live query registered against this stack"
        actions={
          <Show when={extra()?.truncated}>
            <Pill tone="warn">capped — figures are a floor</Pill>
          </Show>
        }
      />

      <div class="page-body">
        <div class="stack">
          {/* Said once, prominently, rather than left to be inferred from a
              listing that keeps rows a closed tab has already abandoned. */}
          <Panel>
            <div class="dim">
              A client proves it is still watching on a{' '}
              <span class="ghost">0.9 &times; ttl</span> timer — about nine
              minutes at the default <span class="ghost">10m</span>. So a closed
              tab fades out of this listing over minutes rather than leaving at
              once, and every row carries its own{' '}
              <span class="ghost">expires</span> so you can see which ones are
              on the way out.
            </div>
          </Panel>

          {/* The one thing on this page that is a warning rather than a
              reading. A view this size is republished row by row as
              `_00_list_ref` edges on every cold registration, in one
              transaction; a few thousand of them at once stalled a tenant's
              SurrealDB (3.0.5) for minutes and took every other registration
              down with it. The fix is on the app side: window the query
              (LIMIT/START) or resolve rows by id. */}
          <Show when={(extra()?.totals.large_views ?? 0) > 0}>
            <Panel>
              <div
                style={{
                  display: 'flex',
                  gap: '12px',
                  'align-items': 'flex-start',
                  'justify-content': 'space-between',
                }}
              >
                <div>
                  <div>
                    <Pill tone="warn" dot>
                      {formatCount(extra()!.totals.large_views)}{' '}
                      {extra()!.totals.large_views === 1 ? 'view holds' : 'views hold'}{' '}
                      {formatCount(extra()!.large_view_rows)}+ rows
                    </Pill>
                  </div>
                  <div class="dim" style={{ 'margin-top': '6px' }}>
                    Every row of an unwindowed live view is an edge the SSP
                    republishes on each cold registration, in a single
                    transaction. At this size that write stalls the database
                    and every other client's registration behind it. Window
                    the query with <span class="ghost">LIMIT</span>/
                    <span class="ghost">START</span>, or resolve rows by id,
                    instead of syncing the whole table.
                  </div>
                </div>
                <button
                  class="btn btn-sm"
                  onClick={() => setLargeOnly(true)}
                  title="Filter the table to the large views"
                >
                  Show them
                </button>
              </div>
            </Panel>
          </Show>

          <Show when={extra()?.top_users?.length}>
            <div class="grid grid-2">
              <Panel title="Heaviest users" flush>
                <div class="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>User</th>
                        <th>Views</th>
                        <th>Sessions</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={extra()!.top_users}>
                        {(u) => (
                          <tr>
                            <td>
                              <button
                                class="btn btn-sm"
                                onClick={() => setUser(u.auth_id)}
                                title="Filter the table to this user"
                              >
                                {u.auth_id}
                              </button>
                            </td>
                            <td class="dim" data-label="Views">
                              {formatCount(u.views)}
                            </td>
                            <td class="dim" data-label="Sessions">
                              {formatCount(u.sessions)}
                            </td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </div>
              </Panel>

              <Panel
                title="Views per SSP"
                sub="Where the scheduler routed each registration"
                flush
              >
                <div class="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>SSP</th>
                        <th>Views</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For
                        each={extra()!.by_ssp}
                        fallback={
                          <tr>
                            <td colSpan={2}>
                              <Empty>No assignments recorded.</Empty>
                            </td>
                          </tr>
                        }
                      >
                        {(s) => (
                          <tr>
                            <td>
                              <button
                                class="btn btn-sm"
                                onClick={() => setSsp(s.ssp_id)}
                                title="Filter the table to this SSP"
                              >
                                {s.ssp_id}
                              </button>
                            </td>
                            <td class="dim" data-label="Views">
                              {formatCount(s.views)}
                            </td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </div>
              </Panel>
            </div>
          </Show>

          <Panel
            title="Registered views"
            sub={
              <Show when={data()} fallback="Loading…">
                {(d) => (
                  <>
                    {formatCount(d().returned)} of {formatCount(d().total)}
                    <Show when={d().ssp_filtered}>
                      {' '}
                      (the total counts every SSP; the SSP filter narrows only
                      this page)
                    </Show>
                  </>
                )}
              </Show>
            }
            actions={
              <Show when={user() || ssp() || search() || sharedOnly() || slowOnly() || largeOnly()}>
                <button
                  class="btn btn-sm"
                  onClick={() => {
                    setUser('');
                    setSsp('');
                    setSearch('');
                    setSharedOnly(false);
                    setSlowOnly(false);
                    setLargeOnly(false);
                  }}
                >
                  Clear filters
                </button>
              </Show>
            }
          >
            <div class="filters">
              <select
                value={sort()}
                onChange={(e) => setSort(e.currentTarget.value as SortKey)}
                aria-label="Sort"
              >
                <For each={SORTS}>
                  {(s) => <option value={s.key}>{s.label}</option>}
                </For>
              </select>
              <input
                class="grow"
                placeholder="Search the SurrealQL"
                value={search()}
                onInput={(e) => setSearch(e.currentTarget.value)}
              />
              <input
                placeholder="auth_id"
                value={user()}
                onInput={(e) => setUser(e.currentTarget.value)}
              />
              <input
                placeholder="SSP id"
                value={ssp()}
                onInput={(e) => setSsp(e.currentTarget.value)}
              />
              <label>
                <input
                  type="checkbox"
                  checked={sharedOnly()}
                  onChange={(e) => setSharedOnly(e.currentTarget.checked)}
                />
                Shared only
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={slowOnly()}
                  onChange={(e) => setSlowOnly(e.currentTarget.checked)}
                />
                Slow only
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={largeOnly()}
                  onChange={(e) => setLargeOnly(e.currentTarget.checked)}
                />
                Large only
              </label>
            </div>
          </Panel>

          <Show when={error()}>
            {(msg) => (
              <Panel>
                <Empty>{msg()}</Empty>
              </Panel>
            )}
          </Show>

          <Show when={data()}>
            {(d) => (
              <Show
                when={d().views.length > 0}
                fallback={
                  <Panel>
                    <Empty>
                      No live views match. A view is reclaimed once{' '}
                      <span class="ghost">lastActiveAt + ttl</span> passes.
                    </Empty>
                  </Panel>
                }
              >
                <Panel flush>
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>View</th>
                          <th>User</th>
                          <th>SSP</th>
                          <th>Rows</th>
                          <th>Subs</th>
                          <th>p90</th>
                          <th>p99</th>
                          <th>Updates</th>
                          <th>Errors</th>
                          <th>Expires</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={d().views}>
                          {(v) => {
                            const exp = () => expiry(v, now());
                            const slow = () =>
                              v.p99 !== null && v.p99 >= d().slow_ms;
                            const large = () =>
                              v.row_count >= d().large_view_rows;
                            return (
                              <tr>
                                <td class="cell-surql">
                                  <A href={`/views/${encodeURIComponent(v.key)}`}>
                                    <span
                                      class="clamp-3"
                                      title={v.surql ?? v.key}
                                    >
                                      {v.surql ?? v.key}
                                    </span>
                                  </A>
                                </td>
                                <td class="dim truncate" data-label="User">
                                  {v.auth_id || '—'}
                                </td>
                                <td class="ghost" data-label="SSP">
                                  {v.ssp_id ?? '—'}
                                </td>
                                <td
                                  classList={{
                                    dim: !large(),
                                    'tone-warn': large(),
                                  }}
                                  data-label="Rows"
                                  title={
                                    large()
                                      ? `At least ${formatCount(d().large_view_rows)} rows: republished as edges on every cold registration. Window this query.`
                                      : undefined
                                  }
                                >
                                  {formatCount(v.row_count)}
                                  <Show when={large()}>
                                    {' '}
                                    <Pill tone="warn">large</Pill>
                                  </Show>
                                </td>
                                <td data-label="Subs">
                                  <Show
                                    when={v.shared}
                                    fallback={
                                      <span class="dim">
                                        {v.subscriber_count}
                                      </span>
                                    }
                                  >
                                    <Pill tone="ok">{v.subscriber_count}</Pill>
                                  </Show>
                                </td>
                                <td class="dim" data-label="p90">
                                  {formatMs(v.p90)}
                                </td>
                                <td
                                  classList={{
                                    dim: !slow(),
                                    'tone-warn': slow(),
                                  }}
                                  data-label="p99"
                                >
                                  {formatMs(v.p99)}
                                </td>
                                <td class="dim" data-label="Updates">
                                  {formatCount(v.update_count)}
                                </td>
                                <td
                                  classList={{
                                    dim: !v.error_count,
                                    'tone-bad': !!v.error_count,
                                  }}
                                  data-label="Errors"
                                >
                                  {formatCount(v.error_count)}
                                </td>
                                <td class="ghost" data-label="Expires">
                                  {exp().text}
                                </td>
                              </tr>
                            );
                          }}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </Panel>
              </Show>
            )}
          </Show>
        </div>
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
        crumb="Views"
        title="Registered view"
        subtitle={<CopyId value={key()} />}
        actions={
          <Show when={view()}>
            {(v) => (
              <>
                <Show when={v().expired}>
                  <Pill tone="idle">lapsed</Pill>
                </Show>
                <Show when={v().shared}>
                  <Pill tone="ok" dot>
                    shared
                  </Pill>
                </Show>
                <Show when={slow()}>
                  <Pill tone="warn">slow</Pill>
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

        <Show when={data()} fallback={<Show when={!error()}><Empty>Loading…</Empty></Show>}>
          {(d) => (
            <div class="stack">
              <Rail>
                <Cell label="Rows" value={formatCount(d().view.row_count)} />
                <Cell
                  label="Updates"
                  value={formatCount(d().view.update_count)}
                />
                <Cell
                  label="Errors"
                  value={formatCount(d().view.error_count)}
                  tone={d().view.error_count ? 'bad' : undefined}
                />
                <Cell
                  label="Subscribers"
                  value={formatCount(d().view.subscribers.length)}
                  foot="live sessions"
                />
                <Cell
                  label="p99"
                  value={formatMs(d().view.p99)}
                  tone={slow() ? 'warn' : undefined}
                  foot="materialization"
                />
              </Rail>

              <Panel
                title="Query"
                sub="Exactly what the SSP compiled, after the permission rewrite"
              >
                <pre class="json">{d().view.surql ?? '—'}</pre>
              </Panel>

              <div class="grid grid-2">
                <Panel
                  title="Params"
                  sub="`auth.id` and `access` are injected server-side so the SSP can evaluate the table's own SELECT predicate"
                >
                  <pre class="json">
                    {JSON.stringify(d().view.params ?? null, null, 2)}
                  </pre>
                </Panel>

                <Panel title="Registration">
                  <KeyValue
                    rows={[
                      ['User', d().view.auth_id || '—'],
                      [
                        'Session',
                        d().view.client_id ? (
                          <CopyId value={d().view.client_id} />
                        ) : (
                          '—'
                        ),
                      ],
                      ['SSP', d().view.ssp_id ?? 'unassigned'],
                      ['Registered', formatStamp(d().view.created_at)],
                      ['Last active', relativeStamp(d().view.last_active_at)],
                      ['TTL', formatUptime(d().view.ttl_secs)],
                      [
                        'Expires',
                        isAbsent(d().view.expires_at)
                          ? '—'
                          : formatStamp(d().view.expires_at),
                      ],
                      ['Build time', formatMs(d().view.registration_ms)],
                    ]}
                  />
                </Panel>
              </div>

              <div class="grid grid-2">
                <Panel
                  title="Materialization"
                  sub="A rolling window the SSP keeps per view and flushes onto its row"
                >
                  <KeyValue
                    rows={[
                      ['p55', formatMs(d().view.p55)],
                      ['p90', formatMs(d().view.p90)],
                      ['p99', formatMs(d().view.p99)],
                      ['Last ingest', formatMs(d().view.last_ingest_ms)],
                    ]}
                  />
                </Panel>

                <Panel
                  title="In the SSP"
                  sub={
                    d().ssp
                      ? 'Heap this view holds, and whether the circuit merged it with another'
                      : undefined
                  }
                >
                  <Show
                    when={d().ssp}
                    fallback={
                      <Empty>
                        {d().view.ssp_id
                          ? 'The SSP serving this view did not answer.'
                          : 'This view has no SSP assignment on this scheduler.'}
                      </Empty>
                    }
                  >
                    {(s) => (
                      <KeyValue
                        rows={[
                          [
                            'Cached records',
                            formatCount(s().view?.cached_records),
                          ],
                          ['View heap', formatBytes(s().view?.view_bytes)],
                          [
                            'Operator heap',
                            formatBytes(s().view?.operator_bytes),
                          ],
                          [
                            'Merging',
                            s().merging ? (
                              // Equal counts with merging on means nothing
                              // actually merged, which is a finding rather
                              // than a formality.
                              <>
                                on — {formatCount(s().graphs)} graphs serving{' '}
                                {formatCount(s().subscribers)} subscribers
                              </>
                            ) : (
                              <span class="dim">
                                off (SPKY_SSP_MERGE_VIEWS)
                              </span>
                            ),
                          ],
                          ['Circuit total', formatBytes(s().total_bytes)],
                        ]}
                      />
                    )}
                  </Show>
                </Panel>
              </div>

              <Panel
                title="Subscribers"
                sub="One entry per WebSocket session, re-stamped on every heartbeat"
                flush
              >
                <Show
                  when={d().view.subscribers.length > 0}
                  fallback={
                    <div class="panel-body">
                      <Empty>
                        No subscriber has stamped this row yet. `lastActiveAt`,
                        not this list, is what keeps the view alive.
                      </Empty>
                    </div>
                  }
                >
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Session</th>
                          <th>Last seen</th>
                          <th>Age</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={d().view.subscribers}>
                          {(s) => (
                            <tr>
                              <td class="id truncate">{s.id}</td>
                              <td class="dim" data-label="Last seen">
                                {s.seen_at_ms
                                  ? new Date(s.seen_at_ms).toLocaleString()
                                  : '—'}
                              </td>
                              <td data-label="Age">
                                <Show
                                  when={s.stale}
                                  fallback={
                                    <span class="dim">
                                      {formatUptime(s.age_secs)}
                                    </span>
                                  }
                                >
                                  <Pill tone="idle">
                                    {formatUptime(s.age_secs)} — stale
                                  </Pill>
                                </Show>
                              </td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </Show>
              </Panel>

              <Panel
                title="Same query elsewhere"
                sub="Other live registrations of identical SurrealQL and identical params. The query id is salted per browser session, so two tabs of one person are two rows here by design."
                flush
              >
                <div class="panel-body">
                  <KeyValue
                    rows={[
                      [
                        'Sessions',
                        <>
                          {formatCount(d().siblings.sessions)}
                          <Show when={d().siblings.sessions <= 1}>
                            {' '}
                            <span class="dim">— only this one</span>
                          </Show>
                        </>,
                      ],
                      ['Users', formatCount(d().siblings.users)],
                    ]}
                  />
                </div>
                <Show when={d().siblings.rows.length > 0}>
                  <div class="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>View</th>
                          <th>User</th>
                          <th>Rows</th>
                          <th>Last active</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={d().siblings.rows}>
                          {(r) => (
                            <tr>
                              <td>
                                <A href={`/views/${encodeURIComponent(r.key)}`}>
                                  <span class="id truncate">{r.key}</span>
                                </A>
                              </td>
                              <td class="dim truncate" data-label="User">
                                {r.auth_id || '—'}
                              </td>
                              <td class="dim" data-label="Rows">
                                {formatCount(r.row_count)}
                              </td>
                              <td class="ghost" data-label="Last active">
                                {relativeStamp(r.last_active_at)}
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
      </div>
    </>
  );
}
