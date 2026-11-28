import { For, Show, batch, createEffect, createMemo, createResource, createSignal, on, onCleanup, untrack } from 'solid-js';
import { A, useNavigate, useParams, useSearchParams } from '@solidjs/router';
import { api, currentMode } from '../api/client';
import { confirm, toast } from '../components/Actions';
import { PageHead } from '../components/Chrome';
import { CopyText, DataError, FieldInput, Pager, ValueCell, enc, useCanWrite } from '../components/DataParts';
import { Sheet, SheetSection, Sq } from '../components/Frame';
import { Sk } from '../components/Skeleton';
import { DatabaseSkeleton, TableDataSkeleton } from '../skeletons/Database';
import {
  FILTER_OPS,
  baseKind,
  columnsOf,
  draftOf,
  editorFor,
  holdsMask,
  isRedacted,
  isTrimmed,
  parseDraft,
  recordTable,
  toCsv,
  writeWords,
  type FieldInfo,
  type Filter,
  type FilterOp,
  type QueryAnswer,
  type QueryResult,
  type Row,
  type RowsPage,
  type TableDetail,
  type TableSummary,
} from '../lib/data';

/**
 * The project's database, in the shape of Supabase's table editor: the tables on
 * the left, one table's rows, filters and sort on the right, a row's fields in a
 * sheet, a SQL editor a tab away.
 *
 * The scheduler holds the root connection and the rules (see
 * `schedule_core::admin_data`): this page never sees a credential outside what a
 * statement it ran returned, and `_00_*` tables are read-only.
 */

function pageTitle(sql: boolean) {
  return (
    <div class="fx-range" role="tablist" aria-label="Database view">
      <A href="/database" class="dx-tab" classList={{ on: !sql }}>Tables</A>
      <A href="/database/sql" class="dx-tab" classList={{ on: sql }}>SQL editor</A>
    </div>
  );
}

export function Database() {
  const params = useParams<{ table?: string }>();
  const navigate = useNavigate();
  const canWrite = useCanWrite();
  const [list, { refetch }] = createResource(() => api.getResult<{ tables: TableSummary[] }>('/data/tables'));
  const tables = () => {
    const r = list();
    return r?.ok ? r.value.tables : [];
  };
  const names = createMemo(() => new Set(tables().map((t) => t.name)));

  const [search, setSearch] = createSignal('');
  const [showInternal, setShowInternal] = createSignal(params.table?.startsWith('_00_') ?? false);
  const matches = (t: TableSummary) => t.name.toLowerCase().includes(search().trim().toLowerCase());
  const appTables = () => tables().filter((t) => !t.internal && matches(t));
  const internal = () => tables().filter((t) => t.internal && matches(t));

  // `/database` opens the app's first table.
  createEffect(() => {
    if (params.table || !list()) return;
    const first = tables().find((t) => !t.internal) ?? tables()[0];
    if (first) navigate(`/database/table/${enc(first.name)}`, { replace: true });
  });

  const TableLink = (props: { table: TableSummary }) => (
    <A class="dx-tlink" activeClass="on" href={`/database/table/${enc(props.table.name)}`} title={props.table.comment ?? props.table.name}>
      <span class="dx-tname">{props.table.name}</span>
      <Show when={props.table.kind === 'relation'}><span class="cap">rel</span></Show>
    </A>
  );

  return (
    <>
      <PageHead title="Database" actions={pageTitle(false)} />
      <Show when={list()} fallback={<DatabaseSkeleton table={params.table} />}>
      <div class="dx-page dx-split">
        <aside class="dx-side">
          <div class="dx-side-find">
            <input type="search" placeholder="Find a table" aria-label="Find a table" value={search()} onInput={(e) => setSearch(e.currentTarget.value)} />
            <button class="dx-iconbtn" title="Reload the table list" aria-label="Reload the table list" onClick={() => void refetch()}>↻</button>
          </div>
          {/* A phone has no room for a column; the same choice in one line. */}
          <select
            class="dx-select dx-side-pick"
            aria-label="Table"
            value={params.table ?? ''}
            onChange={(e) => navigate(`/database/table/${enc(e.currentTarget.value)}`)}
          >
            <For each={tables()}>{(t) => <option value={t.name} selected={t.name === params.table}>{t.name}</option>}</For>
          </select>
          <div class="dx-side-list">
            <div class="dx-side-head cap"><span>Tables</span><span>{appTables().length}</span></div>
            <For each={appTables()} fallback={<Show when={list()}><div class="dx-side-empty cap">No tables</div></Show>}>
              {(t) => <TableLink table={t} />}
            </For>
            <Show when={internal().length}>
              <button class="dx-side-head dx-side-toggle cap" aria-expanded={showInternal()} onClick={() => setShowInternal(!showInternal())}>
                <span>{showInternal() ? '▾' : '▸'} Sync engine</span><span>{internal().length}</span>
              </button>
              <Show when={showInternal()}>
                <For each={internal()}>{(t) => <TableLink table={t} />}</For>
              </Show>
            </Show>
          </div>
        </aside>
        <div class="dx-main">
          <Show when={list() && !list()!.ok}>
            <div class="dx-pad"><DataError message={(list() as { ok: false; message: string }).message} /></div>
          </Show>
          <Show when={list()?.ok && tables().length === 0}>
            <div class="dx-empty"><p class="cap">This database has no tables yet</p></div>
          </Show>
          <Show when={params.table} keyed>
            {(table) => <TableView table={table} tables={names()} canWrite={canWrite()} />}
          </Show>
        </div>
      </div>
      </Show>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* One table                                                            */
/* ------------------------------------------------------------------ */

function TableView(props: { table: string; tables: ReadonlySet<string>; canWrite: boolean }) {
  const navigate = useNavigate();
  const [query, setQuery] = useSearchParams<{ tab?: string; id?: string }>();
  const url = `/data/tables/${enc(props.table)}`;
  const [detailResult] = createResource(() => api.getResult<TableDetail>(url));
  const detail = () => {
    const r = detailResult();
    return r?.ok ? r.value : undefined;
  };
  const kinds = createMemo(() => new Map((detail()?.fields ?? []).map((f) => [f.name, f])));
  const writable = () => props.canWrite && !!detail() && !detail()!.read_only;

  const [sort, setSort] = createSignal<{ field: string; dir: 'asc' | 'desc' } | null>(null);
  const [filters, setFilters] = createSignal<Filter[]>([]);
  const [start, setStart] = createSignal(0);
  const [limit, setLimit] = createSignal(50);
  const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set<string>());
  const [count, setCount] = createSignal<number | 'timed_out' | 'counting' | null>(null);
  const [inserting, setInserting] = createSignal(false);
  const [editing, setEditing] = createSignal<{ id: string; field: string; text: string } | null>(null);

  const filterParam = () => (filters().length ? JSON.stringify(filters()) : '');
  const rowsQuery = createMemo(() => {
    const q = new URLSearchParams({ limit: String(limit()), start: String(start()) });
    const s = sort();
    if (s) {
      q.set('sort', s.field);
      q.set('dir', s.dir);
    }
    if (filterParam()) q.set('filter', filterParam());
    return q.toString();
  });
  const [pageResult, { refetch, mutate }] = createResource(rowsQuery, (q) => api.getResult<RowsPage>(`${url}/rows?${q}`));
  const page = () => {
    const r = pageResult.latest;
    return r?.ok ? r.value : undefined;
  };
  const rows = () => page()?.rows ?? [];
  const columns = createMemo(() => columnsOf(detail()?.fields, rows()));
  /** A column that reads masked cannot be filtered or sorted on: that would read it. */
  const isSecret = (column: string) => !!kinds().get(column)?.credential || rows().some((r) => isRedacted(r[column]));
  const filterable = createMemo(() => columns().filter((c) => !isSecret(c)));

  /** A new filter is a new set of rows, counted again from its start. In one go, so it is one request. */
  const applyFilters = (next: Filter[]) =>
    batch(() => {
      setStart(0);
      setCount(null);
      setFilters(next);
    });
  // What is selected is what is on screen, so a Delete never reaches further.
  createEffect(on(rowsQuery, () => setSelected(new Set<string>()), { defer: true }));

  const toggleSort = (field: string) => {
    if (isSecret(field)) return;
    const s = sort();
    setStart(0);
    if (s?.field !== field) setSort({ field, dir: 'asc' });
    else if (s.dir === 'asc') setSort({ field, dir: 'desc' });
    else setSort(null);
  };

  const replaceRow = (next: Row) => {
    const r = pageResult.latest;
    if (!r?.ok) return;
    mutate({ ok: true, value: { ...r.value, rows: r.value.rows.map((row) => (row.id === next.id ? next : row)) } });
  };

  const openRow = (id: string) => setQuery({ id });
  const closeRow = () => setQuery({ id: undefined });
  const followRecord = (id: string) => {
    const table = recordTable(id, props.tables);
    if (!table) return;
    if (table === props.table) openRow(id);
    else navigate(`/database/table/${enc(table)}?id=${enc(id)}`);
  };

  const countRows = async () => {
    setCount('counting');
    const q = filterParam() ? `?filter=${enc(filterParam())}` : '';
    const r = await api.getResult<{ count: number | null; timed_out?: boolean }>(`${url}/count${q}`);
    if (!r.ok) {
      setCount(null);
      toast('bad', 'Could not count', r.message);
    } else setCount(r.value.timed_out ? 'timed_out' : r.value.count);
  };

  const allOnPage = () => rows().length > 0 && rows().every((r) => selected().has(r.id as string));
  const togglePage = () => setSelected(allOnPage() ? new Set<string>() : new Set(rows().map((r) => r.id as string)));
  const toggleRow = (id: string) => {
    const next = new Set(selected());
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setSelected(next);
  };

  const deleteSelected = async () => {
    const ids = [...selected()];
    const ok = await confirm({
      title: `Delete ${ids.length} ${ids.length === 1 ? 'row' : 'rows'} from ${props.table}?`,
      consequences: ['This cannot be undone, short of restoring a backup.', 'Clients see them disappear right away, and table events run.'],
      verb: 'Delete',
      typeToConfirm: ids.length > 1 ? 'delete' : undefined,
    });
    if (!ok) return;
    try {
      const out = await api.post<{ deleted: number }>(`${url}/rows/delete`, { ids });
      toast('ok', `${out.deleted} deleted`);
      setSelected(new Set<string>());
      void refetch();
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  /** Double-clicking a plain value edits it in place; everything else opens the row. */
  const canEditInline = (column: string, value: unknown) => {
    if (!writable() || column === 'id') return false;
    const f = kinds().get(column);
    if (f?.readonly || f?.credential || holdsMask(value)) return false;
    const editor = editorFor(f?.kind, value);
    return editor !== 'json' && (value === null || value === undefined || typeof value !== 'object');
  };
  const startEdit = (row: Row, column: string) => {
    if (canEditInline(column, row[column])) setEditing({ id: row.id as string, field: column, text: draftOf(row[column]) });
    else openRow(row.id as string);
  };
  const commitEdit = async (row: Row) => {
    const e = editing();
    if (!e) return;
    setEditing(null);
    if (e.text === draftOf(row[e.field])) return;
    const parsed = parseDraft(kinds().get(e.field)?.kind, row[e.field], e.text);
    if (!parsed.ok) return toast('bad', `Not saved: ${e.field}`, parsed.error);
    try {
      const out = await api.put<{ row: Row }>(`${url}/row?id=${enc(e.id)}`, { set: { [e.field]: parsed.value } });
      replaceRow(out.row);
    } catch (err) {
      toast('bad', `Not saved: ${e.field}`, err instanceof Error ? err.message : undefined);
    }
  };

  const tab = () => (query.tab === 'structure' ? 'structure' : 'data');

  return (
    <div class="dx-table">
      <div class="dx-bar">
        <div class="dx-bar-title">
          <b>{props.table}</b>
          <Show when={detail()} fallback={<Show when={!detailResult()}><span class="cap"><Sk w="7em" /></span></Show>}>
            {(d) => (
              <span class="cap">
                {d().kind === 'relation' ? 'relation' : d().schemafull ? 'schemafull' : 'schemaless'}
                <Show when={d().read_only}> · read-only</Show>
              </span>
            )}
          </Show>
        </div>
        <div class="fx-range" role="tablist" aria-label="Table view">
          <button role="tab" aria-selected={tab() === 'data'} classList={{ on: tab() === 'data' }} onClick={() => setQuery({ tab: undefined })}>Data</button>
          <button role="tab" aria-selected={tab() === 'structure'} classList={{ on: tab() === 'structure' }} onClick={() => setQuery({ tab: 'structure' })}>Structure</button>
        </div>
        <Show when={tab() === 'data'}>
          <div class="dx-bar-end">
            <Show when={selected().size}>
              <button class="btn btn-sm dx-danger" onClick={() => void deleteSelected()}>Delete {selected().size}</button>
            </Show>
            <Show when={writable()}>
              <button class="btn btn-sm btn-primary" onClick={() => setInserting(true)}>Insert row</button>
            </Show>
            <button class="dx-iconbtn" title="Reload" aria-label="Reload" onClick={() => void refetch()}>↻</button>
          </div>
        </Show>
      </div>

      <Show when={detailResult() && !detailResult()!.ok}>
        <div class="dx-pad"><DataError message={(detailResult() as { ok: false; message: string }).message} /></div>
      </Show>

      <Show when={tab() === 'structure'} fallback={
        // The first page of rows decides the columns: until then, the grid's shape.
        <Show when={pageResult.latest} fallback={<TableDataSkeleton />}>
          <FilterBar fields={filterable()} kinds={kinds()} filters={filters()} onChange={applyFilters} />
          <Show when={pageResult.latest && !pageResult.latest.ok}>
            <div class="dx-pad"><DataError message={(pageResult.latest as { ok: false; message: string }).message} /></div>
          </Show>
          <div class="dx-grid-wrap">
            <table class="dx-grid">
              <thead>
                <tr>
                  <Show when={writable()}>
                    <th class="dx-check"><input type="checkbox" aria-label="Select this page" checked={allOnPage()} onChange={togglePage} /></th>
                  </Show>
                  <For each={columns()}>
                    {(column) => (
                      <th title={kinds().get(column)?.kind}>
                        <button class="dx-th" disabled={isSecret(column)} onClick={() => toggleSort(column)}>
                          <span>{column}</span>
                          <Show when={kinds().get(column)}>{(f) => <small>{baseKind(f().kind)}</small>}</Show>
                          <Show when={sort()?.field === column}><i>{sort()!.dir === 'asc' ? '↑' : '↓'}</i></Show>
                        </button>
                      </th>
                    )}
                  </For>
                </tr>
              </thead>
              <tbody>
                <For each={rows()}>
                  {(row) => (
                    <tr classList={{ on: selected().has(row.id as string) }}>
                      <Show when={writable()}>
                        <td class="dx-check"><input type="checkbox" aria-label={`Select ${row.id}`} checked={selected().has(row.id as string)} onChange={() => toggleRow(row.id as string)} /></td>
                      </Show>
                      <For each={columns()}>
                        {(column) => (
                          <td onDblClick={() => startEdit(row, column)}>
                            <Show when={column !== 'id'} fallback={<button class="fx-link dx-rid" onClick={() => openRow(row.id as string)}>{row.id as string}</button>}>
                              <Show
                                when={editing()?.id === row.id && editing()?.field === column}
                                fallback={<ValueCell value={row[column]} kind={kinds().get(column)?.kind} tables={props.tables} onRecord={followRecord} />}
                              >
                                <input
                                  class="dx-inline"
                                  ref={(el) => queueMicrotask(() => { el.focus(); el.select(); })}
                                  value={editing()!.text}
                                  onInput={(e) => setEditing({ ...editing()!, text: e.currentTarget.value })}
                                  onBlur={() => void commitEdit(row)}
                                  onKeyDown={(e) => {
                                    if (e.key === 'Enter') e.currentTarget.blur();
                                    if (e.key === 'Escape') setEditing(null);
                                  }}
                                />
                              </Show>
                            </Show>
                          </td>
                        )}
                      </For>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
            <Show when={page() && !rows().length}>
              <div class="dx-empty">
                <p class="cap">{filters().length ? 'No row matches these filters' : start() ? 'Nothing on this page' : 'This table is empty'}</p>
              </div>
            </Show>
          </div>
          <div class="dx-foot">
            <div class="dx-count cap">
              <Show when={count() === null}><button class="fx-link" onClick={() => void countRows()}>Count rows</button></Show>
              <Show when={count() === 'counting'}>Counting…</Show>
              <Show when={count() === 'timed_out'}>
                <span class="warn">Too many to count in 10s</span>
              </Show>
              <Show when={typeof count() === 'number'}><b>{(count() as number).toLocaleString()}</b> rows</Show>
            </div>
            <Pager
              start={page()?.start ?? 0}
              count={rows().length}
              hasMore={!!page()?.has_more}
              limit={limit()}
              loading={pageResult.loading}
              onStart={setStart}
              onLimit={(n) => { setStart(0); setLimit(n); }}
            />
          </div>
        </Show>
      }>
        <Show when={detail()}>{(d) => <Structure table={d()} />}</Show>
      </Show>

      <Show when={query.id} keyed>
        {(id) => (
          <RowSheet
            table={props.table}
            id={id}
            detail={detail()}
            writable={writable()}
            tables={props.tables}
            onRecord={followRecord}
            onClose={closeRow}
            onSaved={(row) => { replaceRow(row); }}
            onDeleted={() => { closeRow(); void refetch(); }}
          />
        )}
      </Show>
      <Show when={inserting()}>
        <RowSheet
          table={props.table}
          detail={detail()}
          writable={writable()}
          tables={props.tables}
          onRecord={followRecord}
          onClose={() => setInserting(false)}
          onSaved={() => { setInserting(false); void refetch(); }}
          onDeleted={() => undefined}
        />
      </Show>
    </div>
  );
}

function FilterBar(props: { fields: string[]; kinds: Map<string, FieldInfo>; filters: Filter[]; onChange: (next: Filter[]) => void }) {
  const [adding, setAdding] = createSignal(false);
  const [field, setField] = createSignal('');
  const [op, setOp] = createSignal<FilterOp>('eq');
  const [value, setValue] = createSignal('');
  const noValue = () => !!FILTER_OPS.find((o) => o.op === op())?.noValue;
  const chosenField = () => field() || props.fields[0] || '';

  const add = (e: Event) => {
    e.preventDefault();
    if (!chosenField()) return;
    props.onChange([...props.filters, { field: chosenField(), op: op(), value: noValue() ? '' : value() }]);
    setValue('');
    setOp('eq');
    setAdding(false);
  };
  const remove = (i: number) => props.onChange(props.filters.filter((_, at) => at !== i));
  const label = (f: Filter) => FILTER_OPS.find((o) => o.op === f.op)?.label ?? f.op;

  return (
    <div class="dx-filters">
      <For each={props.filters}>
        {(f, i) => (
          <span class="dx-chip">
            <b>{f.field}</b> {label(f)} <Show when={f.value !== ''}><em>{f.value}</em></Show>
            <button aria-label={`Remove filter on ${f.field}`} onClick={() => remove(i())}>×</button>
          </span>
        )}
      </For>
      <Show
        when={adding()}
        fallback={<button class="btn btn-sm" onClick={() => setAdding(true)}>Filter</button>}
      >
        <form class="dx-filter-form" onSubmit={add}>
          <select class="dx-select" aria-label="Field" value={chosenField()} onChange={(e) => setField(e.currentTarget.value)}>
            <For each={props.fields}>{(f) => <option value={f} selected={f === chosenField()}>{f}</option>}</For>
          </select>
          <select class="dx-select" aria-label="Comparison" value={op()} onChange={(e) => setOp(e.currentTarget.value as FilterOp)}>
            <For each={FILTER_OPS}>{(o) => <option value={o.op}>{o.label}</option>}</For>
          </select>
          <Show when={!noValue()}>
            <input
              aria-label="Value"
              placeholder={baseKind(props.kinds.get(chosenField())?.kind)}
              ref={(el) => queueMicrotask(() => el.focus())}
              value={value()}
              onInput={(e) => setValue(e.currentTarget.value)}
            />
          </Show>
          <button class="btn btn-sm btn-primary" type="submit">Apply</button>
          <button class="btn btn-sm" type="button" onClick={() => setAdding(false)}>Cancel</button>
        </form>
      </Show>
      <Show when={props.filters.length > 1}>
        <button class="fx-link cap" onClick={() => props.onChange([])}>Clear all</button>
      </Show>
    </div>
  );
}

function Structure(props: { table: TableDetail }) {
  const flags = (f: FieldInfo) =>
    [f.required && 'required', f.optional && 'optional', f.readonly && 'readonly', f.flexible && 'flexible', f.credential && 'credential'].filter(Boolean).join(' · ');
  return (
    <div class="dx-structure">
      <section class="dx-sec">
        <div class="fx-head"><span class="cap">Fields</span><span class="fx-meta cap"><b>{props.table.fields.length}</b></span></div>
        <Show when={props.table.fields.length} fallback={<p class="cap dx-note">No fields defined: this table takes whatever is written to it.</p>}>
          <div class="dx-grid-wrap inline">
            <table class="dx-grid plain">
              <thead><tr><th>Name</th><th>Type</th><th>Flags</th><th>Default / value</th><th>Assert</th></tr></thead>
              <tbody>
                <For each={props.table.fields}>
                  {(f) => (
                    <tr classList={{ 'dx-nested': f.nested }}>
                      <td class="dx-strong">{f.name}</td>
                      <td>{f.kind}</td>
                      <td class="dx-dim">{flags(f)}</td>
                      <td class="dx-dim">{f.default ?? f.value ?? ''}</td>
                      <td class="dx-dim">{f.assert ?? ''}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
        </Show>
      </section>
      <section class="dx-sec">
        <div class="fx-head"><span class="cap">Indexes</span><span class="fx-meta cap"><b>{props.table.indexes.length}</b></span></div>
        <Show when={props.table.indexes.length} fallback={<p class="cap dx-note">No indexes: a filter or a sort on this table reads all of it.</p>}>
          <div class="dx-grid-wrap inline">
            <table class="dx-grid plain">
              <thead><tr><th>Name</th><th>Columns</th><th>Kind</th></tr></thead>
              <tbody>
                <For each={props.table.indexes}>
                  {(ix) => (
                    <tr>
                      <td class="dx-strong">{ix.name}</td>
                      <td>{(ix.cols ?? []).join(', ')}</td>
                      <td class="dx-dim">{typeof ix.index === 'string' ? ix.index : ix.index ? JSON.stringify(ix.index) : 'plain'}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
        </Show>
      </section>
      <Show when={props.table.events.length}>
        <section class="dx-sec">
          <div class="fx-head"><span class="cap">Events</span><span class="fx-meta cap"><b>{props.table.events.length}</b></span></div>
          <div class="dx-grid-wrap inline">
            <table class="dx-grid plain">
              <thead><tr><th>Name</th><th>When</th></tr></thead>
              <tbody>
                <For each={props.table.events}>
                  {(ev) => (
                    <tr>
                      <td class="dx-strong">{ev.name}</td>
                      <td class="dx-dim">{ev.when ?? ''}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
        </section>
      </Show>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* One row                                                              */
/* ------------------------------------------------------------------ */

/**
 * A row's fields, with `id` absent for a new one. Sends only what changed: a
 * value this form never touched is not written back, whatever it looked like.
 */
function RowSheet(props: {
  table: string;
  id?: string;
  detail: TableDetail | undefined;
  writable: boolean;
  tables: ReadonlySet<string>;
  onRecord: (id: string) => void;
  onClose: () => void;
  onSaved: (row: Row) => void;
  onDeleted: () => void;
}) {
  const url = `/data/tables/${enc(props.table)}`;
  const creating = () => !props.id;
  const [loaded, { mutate }] = createResource(
    () => props.id,
    (id) => api.getResult<{ row: Row }>(`${url}/row?id=${enc(id)}`),
  );
  const original = (): Row => {
    const r = loaded();
    return r?.ok ? r.value.row : {};
  };

  const [drafts, setDrafts] = createSignal<Record<string, string>>({});
  const [cleared, setCleared] = createSignal<ReadonlySet<string>>(new Set<string>());
  const [extra, setExtra] = createSignal<string[]>([]);
  const [newKey, setNewKey] = createSignal('');
  const [newId, setNewId] = createSignal('');
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string>();

  const fields = createMemo(() => {
    const defined = (props.detail?.fields ?? []).filter((f) => !f.nested);
    const names = new Set(defined.map((f) => f.name));
    const relation = props.detail?.kind === 'relation';
    const ends = relation && creating() ? ['in', 'out'].filter((n) => !names.has(n)) : [];
    const rest = [...Object.keys(original()), ...extra()].filter((k) => k !== 'id' && !names.has(k) && !ends.includes(k));
    return { defined, others: [...ends, ...new Set(rest)] };
  });
  const allNames = () => [...fields().defined.map((f) => f.name), ...fields().others];
  const field = (name: string) => fields().defined.find((f) => f.name === name);
  const textOf = (name: string) => drafts()[name] ?? draftOf(original()[name]);
  const hasValue = (name: string) => original()[name] !== undefined && original()[name] !== null;

  const locked = (name: string) => {
    const f = field(name);
    return !props.writable || !!f?.credential || holdsMask(original()[name]) || (!!f?.readonly && !creating());
  };
  const changed = () =>
    allNames().filter((n) => !locked(n) && (cleared().has(n) ? hasValue(n) : n in drafts() && drafts()[n] !== draftOf(original()[n])));
  const dirty = () => changed().length > 0 || (creating() && !!newId());

  const clear = (name: string) => {
    const next = new Set(cleared());
    next.add(name);
    setCleared(next);
    const d = { ...drafts() };
    delete d[name];
    setDrafts(d);
  };
  const edit = (name: string, text: string) => {
    if (cleared().has(name)) {
      const next = new Set(cleared());
      next.delete(name);
      setCleared(next);
    }
    setDrafts({ ...drafts(), [name]: text });
  };

  const save = async (e: Event) => {
    e.preventDefault();
    setError(undefined);
    const set: Record<string, unknown> = {};
    const unset: string[] = [];
    for (const name of changed()) {
      if (cleared().has(name)) {
        unset.push(name);
        continue;
      }
      const parsed = parseDraft(field(name)?.kind, original()[name], drafts()[name]);
      if (!parsed.ok) return void setError(`${name}: ${parsed.error}`);
      set[name] = parsed.value;
    }
    setBusy(true);
    try {
      if (creating()) {
        const out = await api.post<{ row: Row }>(`${url}/rows`, { id: newId() || undefined, set });
        toast('ok', 'Row added', out.row?.id as string | undefined);
        props.onSaved(out.row);
      } else {
        const out = await api.put<{ row: Row }>(`${url}/row?id=${enc(props.id!)}`, { set, unset });
        mutate({ ok: true, value: { row: out.row } });
        setDrafts({});
        setCleared(new Set<string>());
        toast('ok', 'Saved');
        props.onSaved(out.row);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not save');
    } finally {
      setBusy(false);
    }
  };

  const remove = async () => {
    const ok = await confirm({
      title: `Delete ${props.id}?`,
      consequences: ['This cannot be undone, short of restoring a backup.', 'Clients see it disappear right away, and table events run.'],
      verb: 'Delete',
    });
    if (!ok) return;
    try {
      await api.delete(`${url}/row?id=${enc(props.id!)}`);
      toast('ok', 'Row deleted');
      props.onDeleted();
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  const addField = (e: Event) => {
    e.preventDefault();
    const name = newKey().trim();
    if (!name) return;
    setExtra([...extra(), name]);
    setNewKey('');
  };

  const editorRow = (name: string) => {
    const f = () => field(name);
    return (
      <div class="dx-field">
        <div class="dx-field-head">
          <b>{name}</b>
          <span class="cap">{f()?.kind ?? ''}</span>
          <Show when={f()?.required}><span class="cap warn">required</span></Show>
          <Show when={f()?.readonly}><span class="cap">readonly</span></Show>
          <Show when={!locked(name) && !creating() && hasValue(name) && !cleared().has(name) && (!f() || f()!.optional)}>
            <button class="fx-link cap dx-clear" type="button" title="Remove this value (NONE)" onClick={() => clear(name)}>clear</button>
          </Show>
          <Show when={cleared().has(name)}><span class="cap warn">will be removed</span></Show>
        </div>
        <Show
          when={!isRedacted(original()[name]) && !f()?.credential}
          fallback={<div class="dx-locked"><span class="dx-secret">••••••</span><span class="cap">Credentials are never shown or edited here</span></div>}
        >
          <Show
            when={!holdsMask(original()[name])}
            fallback={
              <div class="dx-locked">
                <ValueCell value={original()[name]} />
                <span class="cap">
                  {isTrimmed(original()[name]) ? 'Too long to edit here. Read it from the SQL editor.' : 'Holds a credential, so saving it from here would overwrite that. Change it from the SQL editor.'}
                </span>
              </div>
            }
          >
            <FieldInput
              field={f()}
              name={name}
              value={original()[name]}
              draft={cleared().has(name) ? '' : textOf(name)}
              disabled={locked(name)}
              onInput={(text) => edit(name, text)}
            />
          </Show>
        </Show>
      </div>
    );
  };

  return (
    <Sheet
      open
      wide
      title={creating() ? `New row in ${props.table}` : <span class="dx-sheet-id">{props.id}</span>}
      onClose={props.onClose}
    >
      <Show when={loaded() && !loaded()!.ok}>
        <section class="sheet-sec"><DataError message={(loaded() as { ok: false; message: string }).message} /></section>
      </Show>
      <Show when={creating() || loaded()?.ok}>
        <form onSubmit={save}>
          <section class="sheet-sec dx-actions">
            <Show when={props.writable}>
              <button class="btn btn-sm btn-primary" type="submit" disabled={busy() || !(dirty() || creating())}>
                {creating() ? 'Insert' : 'Save changes'}
              </button>
            </Show>
            <Show when={!creating()}>
              <CopyText value={JSON.stringify(original(), null, 2)} label="Copy JSON" />
            </Show>
            <Show when={props.writable && !creating()}>
              <button class="btn btn-sm dx-danger" type="button" onClick={() => void remove()}>Delete</button>
            </Show>
            <Show when={!props.writable}><span class="cap">read-only</span></Show>
          </section>
          <DataError message={error()} />
          <section class="sheet-sec dx-fields">
            <Show when={creating() && props.detail?.kind !== 'relation'}>
              <div class="dx-field">
                <div class="dx-field-head"><b>id</b><span class="cap">optional, generated when empty</span></div>
                <input class="dx-input" value={newId()} onInput={(e) => setNewId(e.currentTarget.value)} />
              </div>
            </Show>
            <For each={fields().defined.filter((f) => !(creating() && (f.credential || f.readonly || f.value)))}>{(f) => editorRow(f.name)}</For>
            <For each={fields().others}>{(name) => editorRow(name)}</For>
          </section>
        </form>
        <Show when={props.writable && !props.detail?.schemafull}>
          <SheetSection label="Add a field">
            <form class="dx-addfield" onSubmit={addField}>
              <input placeholder="field_name" aria-label="Field name" value={newKey()} onInput={(e) => setNewKey(e.currentTarget.value)} />
              <button class="btn btn-sm" type="submit" disabled={!newKey().trim()}>Add</button>
            </form>
          </SheetSection>
        </Show>
      </Show>
    </Sheet>
  );
}

/* ------------------------------------------------------------------ */
/* The SQL editor                                                       */
/* ------------------------------------------------------------------ */

/** Kept for the tab's life only: a statement can hold anything. */
function sqlStore(): Storage | null {
  try {
    return sessionStorage;
  } catch {
    return null;
  }
}
const HISTORY_MAX = 15;

function readHistory(key: string): string[] {
  try {
    const list: unknown = JSON.parse(sqlStore()?.getItem(key) ?? '[]');
    return Array.isArray(list) ? (list as string[]) : [];
  } catch {
    return [];
  }
}

export function SqlEditor() {
  const canWrite = useCanWrite();
  // Per project: a statement written for one must not be waiting in another's editor.
  const project = currentMode().config?.project_slug ?? '';
  const SQL_KEY = `spky.sql.draft:${project}`;
  const HISTORY_KEY = `spky.sql.history:${project}`;
  const [sql, setSql] = createSignal(sqlStore()?.getItem(SQL_KEY) ?? 'SELECT * FROM user LIMIT 20;');
  const [busy, setBusy] = createSignal(false);
  const [answer, setAnswer] = createSignal<QueryAnswer | null>(null);
  const [error, setError] = createSignal<string>();
  const [took, setTook] = createSignal<number>();
  const [history, setHistory] = createSignal(readHistory(HISTORY_KEY));
  const [showHistory, setShowHistory] = createSignal(false);
  createEffect(() => sqlStore()?.setItem(SQL_KEY, sql()));

  const remember = (text: string) => {
    const next = [text, ...untrack(history).filter((h) => h !== text)].slice(0, HISTORY_MAX);
    setHistory(next);
    sqlStore()?.setItem(HISTORY_KEY, JSON.stringify(next));
  };

  const run = async () => {
    const text = sql().trim();
    if (!text || busy()) return;
    const words = writeWords(text);
    if (words.length) {
      const ok = await confirm({
        title: `Run ${words.join(', ')}?`,
        consequences: ['This runs as root on the project’s database, past every table permission.', 'SurrealDB commits statement by statement unless the script says BEGIN.'],
        verb: 'Run',
      });
      if (!ok) return;
    }
    setBusy(true);
    setError(undefined);
    const began = performance.now();
    try {
      const out = await api.post<QueryAnswer>('/data/query', { sql: text });
      setAnswer(out);
      remember(text);
    } catch (err) {
      setAnswer(null);
      setError(err instanceof Error ? err.message : 'The query failed');
    } finally {
      setTook(Math.round(performance.now() - began));
      setBusy(false);
    }
  };

  const onKey = (e: KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      if (canWrite()) void run();
    }
  };
  document.addEventListener('keydown', onKey);
  onCleanup(() => document.removeEventListener('keydown', onKey));

  return (
    <>
      <PageHead title="SQL editor" crumb="Database" actions={pageTitle(true)} />
      <div class="dx-page dx-sql">
        <div class="dx-sql-top">
          <textarea
            class="dx-sql-input"
            aria-label="SurrealQL"
            spellcheck={false}
            rows={Math.min(16, Math.max(5, sql().split('\n').length))}
            value={sql()}
            onInput={(e) => setSql(e.currentTarget.value)}
          />
          <div class="dx-sql-bar">
            <Show
              when={canWrite()}
              fallback={<span class="cap warn">This session is read-only, and a statement can write, so the editor is off.</span>}
            >
              <button class="btn btn-sm btn-primary" disabled={busy() || !sql().trim()} onClick={() => void run()}>
                {busy() ? 'Running…' : 'Run'}
              </button>
              <span class="cap dx-kbd">⌘↵</span>
            </Show>
            <Show when={history().length}>
              <button class="btn btn-sm" aria-expanded={showHistory()} onClick={() => setShowHistory(!showHistory())}>History</button>
            </Show>
            <span class="cap dx-sql-root"><Sq tone="warn" /> runs as root</span>
          </div>
          <Show when={showHistory()}>
            <div class="dx-history">
              <For each={history()}>
                {(h) => (
                  <button onClick={() => { setSql(h); setShowHistory(false); }} title={h}>{h.split('\n')[0]}</button>
                )}
              </For>
            </div>
          </Show>
        </div>
        <div class="dx-sql-out">
          <DataError message={error()} />
          <Show when={error()}>
            <p class="cap dx-note">Statements before the one that failed have already run.</p>
          </Show>
          <Show when={answer()} fallback={<Show when={!error()}><div class="dx-empty"><p class="cap">Results appear here</p></div></Show>}>
            {(a) => (
              <>
                <div class="dx-sql-meta cap">
                  <span>{a().results.length} {a().results.length === 1 ? 'statement' : 'statements'}</span>
                  <Show when={took() !== undefined}><span>{took()} ms</span></Show>
                  <Show when={a().limit_added}><span class="warn">LIMIT {a().limit_added} added, write your own to change it</span></Show>
                </div>
                <For each={a().results}>{(result, i) => <ResultBlock result={result} index={i()} total={a().results.length} limited={!!a().limit_added} />}</For>
              </>
            )}
          </Show>
        </div>
      </div>
    </>
  );
}

function ResultBlock(props: { result: QueryResult; index: number; total: number; limited: boolean }) {
  const rows = () => props.result.rows ?? [];
  const isTable = () => rows().length > 0 && rows().every((r) => r && typeof r === 'object' && !Array.isArray(r));
  const columns = createMemo(() => (isTable() ? columnsOf(undefined, rows() as Row[]).filter((c) => (rows() as Row[]).some((r) => c in r)) : []));
  const isRows = () => props.result.rows !== undefined;

  const download = () => {
    const url = URL.createObjectURL(new Blob([toCsv(rows())], { type: 'text/csv' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = `result-${props.index + 1}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };

  return (
    <section class="dx-result">
      <div class="fx-head">
        <span class="cap">
          <Show when={props.total > 1}>Statement {props.index + 1} · </Show>
          <Show when={isRows()} fallback="Value">
            {/* A limit this page added stops one past what it shows: more exist, how many is unknown. */}
            <Show when={props.limited && props.result.truncated} fallback={<>{props.result.total} {props.result.total === 1 ? 'row' : 'rows'}</>}>
              {rows().length}+ rows
            </Show>
            <Show when={props.result.truncated && !props.limited}> <span class="warn">, showing {rows().length}</span></Show>
          </Show>
        </span>
        <span class="fx-meta cap dx-result-tools">
          <CopyText class="fx-link cap" value={JSON.stringify(isRows() ? rows() : props.result.value, null, 2)} label="Copy JSON" />
          <Show when={rows().length}><button class="fx-link cap" onClick={download}>CSV</button></Show>
        </span>
      </div>
      <Show
        when={isTable()}
        fallback={
          <Show when={!isRows() || rows().length} fallback={<p class="cap dx-note">No rows</p>}>
            <pre class="vw-pre dim dx-pre">{JSON.stringify(isRows() ? rows() : props.result.value ?? null, null, 2)}</pre>
          </Show>
        }
      >
        <div class="dx-grid-wrap inline">
          <table class="dx-grid plain">
            <thead><tr><For each={columns()}>{(c) => <th>{c}</th>}</For></tr></thead>
            <tbody>
              <For each={rows() as Row[]}>
                {(row) => <tr><For each={columns()}>{(c) => <td><ValueCell value={row[c]} /></td>}</For></tr>}
              </For>
            </tbody>
          </table>
        </div>
      </Show>
    </section>
  );
}

