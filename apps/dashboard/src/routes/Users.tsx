import { For, Show, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { useNavigate, useSearchParams } from '@solidjs/router';
import { api } from '../api/client';
import { confirm, toast } from '../components/Actions';
import { PageHead } from '../components/Chrome';
import { CopyText, DataError, FieldInput, Pager, ValueCell, enc, useCanWrite } from '../components/DataParts';
import { FxCell, Sheet, SheetSection, Sq } from '../components/Frame';
import { formatStamp, relativeStamp } from '../lib/format';
import {
  draftOf,
  generatePassword,
  parseDraft,
  type FieldInfo,
  type Row,
  type TableDetail,
  type UserDetail,
  type UserEntry,
  type UserSource,
  type UserStats,
  type UsersPage,
} from '../lib/data';

/**
 * The app's users, read from where its own `SIGNIN` clause looks them up. A user
 * here is a row in the app's table, not an account of the console, so everything
 * on this page is what the app itself would see: passwords are set the way its
 * `SIGNIN` checks them, the roster is what `spky admin` writes.
 */

const PAGE = 50;
const SEARCH_DEBOUNCE_MS = 250;

type Listing = 'all' | 'admins' | 'online';

function identifierOf(source: UserSource | null | undefined, row: Row): string {
  const value = source?.identifier ? row[source.identifier] : undefined;
  return typeof value === 'string' && value ? value : String(row.id ?? '');
}

/** Two letters for a user's square. */
function letters(text: string): string {
  const clean = text.replace(/^[^:]*:/, '').replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
  const words = clean.split(' ').filter(Boolean);
  if (words.length > 1) return (words[0][0] + words[1][0]).toUpperCase();
  return clean.slice(0, 2).toUpperCase() || '?';
}

export function Users() {
  const canWrite = useCanWrite();
  const [query, setQuery] = useSearchParams<{ id?: string }>();
  const [input, setInput] = createSignal('');
  const [q, setQ] = createSignal('');
  const [filter, setFilter] = createSignal<Listing>('all');
  const [start, setStart] = createSignal(0);
  const [source, setSource] = createSignal<string>('');
  const [creating, setCreating] = createSignal(false);
  const [sort, setSort] = createSignal<{ field: string; desc: boolean } | null>(null);

  let timer: ReturnType<typeof setTimeout> | undefined;
  const onSearch = (text: string) => {
    setInput(text);
    clearTimeout(timer);
    timer = setTimeout(() => {
      setStart(0);
      setQ(text);
    }, SEARCH_DEBOUNCE_MS);
  };
  onCleanup(() => clearTimeout(timer));

  const listQuery = createMemo(() => {
    const p = new URLSearchParams({ limit: String(PAGE), start: String(start()) });
    if (q().trim()) p.set('q', q().trim());
    if (filter() !== 'all') p.set('filter', filter());
    if (source()) p.set('source', source());
    const by = sort();
    if (by) {
      p.set('sort', by.field);
      p.set('dir', by.desc ? 'desc' : 'asc');
    }
    return p.toString();
  });
  const [listResult, { refetch }] = createResource(listQuery, (p) => api.getResult<UsersPage>(`/data/users?${p}`));
  const [statsResult, { refetch: refetchStats }] = createResource(source, (s) =>
    api.getResult<UserStats>(`/data/users/stats${s ? `?source=${enc(s)}` : ''}`),
  );
  const list = () => {
    const r = listResult.latest;
    return r?.ok ? r.value : undefined;
  };
  const stats = () => {
    const r = statsResult.latest;
    return r?.ok ? r.value : undefined;
  };
  const src = () => list()?.source ?? null;
  const reload = () => {
    void refetch();
    void refetchStats();
  };

  // What the current source's table holds: columns worth a place in the list,
  // and what a new user must be given.
  const [tableResult] = createResource(
    () => src()?.table,
    (table) => api.getResult<TableDetail>(`/data/tables/${enc(table)}`),
  );
  const fields = () => {
    const r = tableResult();
    return r?.ok ? r.value.fields : [];
  };
  /** One more column besides the identifier, when the table has an obvious one. */
  const secondColumn = () => {
    const identifier = src()?.identifier;
    return ['email', 'name', 'display_name', 'displayName', 'full_name', 'username'].find(
      (f) => f !== identifier && fields().some((x) => x.name === f),
    );
  };

  const openUser = (id: string) => setQuery({ id });
  const closeUser = () => setQuery({ id: undefined });
  const pick = (next: Listing) => {
    setStart(0);
    setFilter(next);
  };
  /** Sorting a table with no index on the field reads all of it, so it waits to be asked. */
  const toggleCreated = () => {
    const field = src()?.created_field;
    if (!field) return;
    setStart(0);
    // Already newest first when an index made that the default: the next is oldest.
    setSort({ field, desc: sort() ? !sort()!.desc : !src()!.created_indexed });
  };
  const newestShown = () => (sort() ? sort()!.desc : src()?.created_indexed);

  return (
    <>
      <PageHead
        title="Users"
        actions={
          <Show when={canWrite() && src()}>
            <button class="btn btn-sm btn-primary" disabled={!src()?.can_set_password} title={src()?.can_set_password ? undefined : 'The SIGNIN clause does not show how passwords are checked'} onClick={() => setCreating(true)}>
              New user
            </button>
          </Show>
        }
      />
      <div class="fx-page">
        <Show when={listResult.latest && !listResult.latest.ok}>
          <section class="fx-row"><FxCell><DataError message={(listResult.latest as { ok: false; message: string }).message} /></FxCell></section>
        </Show>

        <Show
          when={!list() || src()}
          fallback={
            <section class="fx-row">
              <FxCell label="Users">
                <p class="cap dx-note">This project defines no record access method and no `user` table, so it has no users to manage. They appear here once its schema has a `DEFINE ACCESS ... TYPE RECORD`.</p>
              </FxCell>
            </section>
          }
        >
          <section class="fx-row dx-stats">
            <FxCell label="Users">
              <div class="fx-big">{stats()?.total?.toLocaleString() ?? '—'}</div>
              <div class="fx-sub cap"><span>{src() ? `in ${src()!.table}` : ''}</span></div>
            </FxCell>
            <FxCell label="Online now">
              <div class="fx-big">{stats()?.online?.toLocaleString() ?? '—'}</div>
              <div class="fx-sub cap"><span>with a live view</span></div>
            </FxCell>
            <FxCell label="Admins">
              <div class="fx-big">{stats()?.admins?.toLocaleString() ?? '—'}</div>
              <div class="fx-sub cap"><span>on the roster</span></div>
            </FxCell>
            <FxCell label="Sign-in" meta={<Show when={(src()?.sources.length ?? 0) > 1}><span>{src()!.sources.length} methods</span></Show>}>
              <Show
                when={(src()?.sources.length ?? 0) > 1}
                fallback={<div class="dx-source">{src()?.access || 'not readable'}</div>}
              >
                <select class="dx-select" aria-label="Sign-in method" value={src()?.access ?? ''} onChange={(e) => { setStart(0); setSource(e.currentTarget.value); }}>
                  <For each={src()!.sources}>{(s) => <option value={s} selected={s === src()?.access}>{s}</option>}</For>
                </select>
              </Show>
              <div class="fx-sub cap"><span>{src()?.identifier ? `identified by ${src()!.identifier}` : 'no identifying field found'}</span></div>
            </FxCell>
          </section>

          <section class="fx-row">
            <FxCell>
              <div class="inc-filters dx-user-filters">
                <div class="fx-range" role="group" aria-label="Show">
                  <For each={[['all', 'All'], ['online', 'Online'], ['admins', 'Admins']] as [Listing, string][]}>
                    {([value, label]) => <button classList={{ on: filter() === value }} aria-pressed={filter() === value} onClick={() => pick(value)}>{label}</button>}
                  </For>
                </div>
                <div class="inc-find">
                  <input type="search" placeholder={searchHint(src())} aria-label="Search users" value={input()} onInput={(e) => onSearch(e.currentTarget.value)} />
                  <button class="btn btn-sm" onClick={reload}>Refresh</button>
                </div>
              </div>
              <Show when={list()} fallback={<div class="fx-empty cap">Loading…</div>}>
                {(l) => (
                  <Show
                    when={l().users.length}
                    fallback={<div class="fx-empty cap">{q() ? 'No user matches' : filter() === 'online' ? 'Nobody is online' : filter() === 'admins' ? 'Nobody is on the roster' : 'No users yet'}</div>}
                  >
                    <table class="fx-list dx-users">
                      <thead>
                        <tr>
                          <th class="cap">User</th>
                          <Show when={secondColumn()}><th class="cap dx-col-wide">{secondColumn()}</th></Show>
                          <th class="cap dx-col-wide">Id</th>
                          <Show when={src()?.created_field}>
                            <th class="cap dx-col-mid">
                              <button class="dx-sortlink" title="Sort by when they joined" onClick={toggleCreated}>
                                Joined<Show when={sort() || src()?.created_indexed}><i>{newestShown() ? ' ↓' : ' ↑'}</i></Show>
                              </button>
                            </th>
                          </Show>
                          <th class="cap">Now</th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={l().users}>
                          {(u) => <UserRow user={u} source={src()} second={secondColumn()} onOpen={openUser} />}
                        </For>
                      </tbody>
                    </table>
                    <div class="dx-users-foot">
                      <Pager start={l().start ?? 0} count={l().users.length} hasMore={l().has_more} limit={PAGE} loading={listResult.loading} onStart={setStart} />
                    </div>
                  </Show>
                )}
              </Show>
            </FxCell>
          </section>
        </Show>
      </div>

      {/* Keyed on the id: a refreshed list is a new source object, not a new user. */}
      <Show when={src() && query.id} keyed>
        {(id) => <UserSheet id={id} source={src()!} canWrite={canWrite()} onClose={closeUser} onChanged={reload} />}
      </Show>
      <Show when={creating() && src()}>
        {(s) => (
          <NewUserSheet
            source={s()}
            fields={fields()}
            onClose={() => setCreating(false)}
            onCreated={reload}
            onOpen={(id) => {
              setCreating(false);
              openUser(id);
            }}
          />
        )}
      </Show>
    </>
  );
}

function searchHint(source: UserSource | null): string {
  const field = source?.identifier;
  return field ? `Search ${field} or id` : 'Search users';
}

function UserRow(props: { user: UserEntry; source: UserSource | null; second?: string; onOpen: (id: string) => void }) {
  const row = () => props.user.row;
  const name = () => identifierOf(props.source, row());
  const created = () => {
    const f = props.source?.created_field;
    return f ? (row()[f] as string | undefined) : undefined;
  };
  const live = () => (props.user.sessions ?? 0) > 0;
  const open = () => props.onOpen(row().id as string);
  return (
    <tr class="inc-tr" tabIndex={0} onClick={open} onKeyDown={(e) => e.key === 'Enter' && open()}>
      <td class="dx-who">
        <span class="dx-avatar" aria-hidden="true">{letters(name())}</span>
        <span class="dx-who-name">{name()}</span>
        <Show when={props.user.is_admin}><span class="dx-tag">admin</span></Show>
      </td>
      <Show when={props.second}><td class="dx-col-wide"><ValueCell value={row()[props.second!]} /></td></Show>
      <td class="dx-col-wide dx-dim">{row().id as string}</td>
      <Show when={props.source?.created_field}><td class="dx-col-mid" title={formatStamp(created())}>{relativeStamp(created())}</td></Show>
      <td>
        <Show when={props.user.sessions !== null} fallback={<span class="dx-dim">—</span>}>
          <span class="fx-state cap"><Sq tone={live() ? 'ok-live' : 'idle'} />{live() ? `${props.user.sessions} ${props.user.sessions === 1 ? 'tab' : 'tabs'}` : 'away'}</span>
        </Show>
      </td>
    </tr>
  );
}

/* ------------------------------------------------------------------ */
/* One user                                                             */
/* ------------------------------------------------------------------ */

function UserSheet(props: { id: string; source: UserSource; canWrite: boolean; onClose: () => void; onChanged: () => void }) {
  const navigate = useNavigate();
  const sourceParam = () => (props.source.access ? `&source=${enc(props.source.access)}` : '');
  const url = () => `/data/users/user?id=${enc(props.id)}${sourceParam()}`;
  const [result, { refetch }] = createResource(() => api.getResult<UserDetail>(url()));
  const detail = () => {
    const r = result.latest;
    return r?.ok ? r.value : undefined;
  };
  const name = () => (detail() ? identifierOf(detail()!.source, detail()!.row) : props.id);
  const rows = createMemo(() => Object.entries(detail()?.row ?? {}).filter(([k]) => k !== 'id'));

  const [password, setPassword] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);

  const setAdmin = async (admin: boolean) => {
    const ok = await confirm(
      admin
        ? {
            title: `Make ${name()} an admin?`,
            consequences: ['They can sign in to this dashboard and do everything on it, this page included.', 'They can edit feature flags and impersonate users in the DevTools.'],
            verb: 'Make admin',
          }
        : {
            title: `Remove ${name()} from the roster?`,
            consequences: ['They can no longer sign in to this dashboard. A session they already hold lasts until it expires.'],
            verb: 'Remove',
          },
    );
    if (!ok) return;
    try {
      await api.put(`/data/users/user/admin?id=${enc(props.id)}${sourceParam()}`, { admin });
      toast('ok', admin ? 'Added to the roster' : 'Removed from the roster');
      void refetch();
      props.onChanged();
    } catch (err) {
      toast('bad', 'Could not change the roster', err instanceof Error ? err.message : undefined);
    }
  };

  const remove = async () => {
    const ok = await confirm({
      title: `Delete ${name()}?`,
      consequences: [
        `Deletes the row ${props.id}, its roster entry and its feature flags.`,
        'Rows that point at this user stay, unless the app’s own events remove them.',
        'This cannot be undone, short of restoring a backup.',
      ],
      verb: 'Delete user',
      typeToConfirm: name(),
    });
    if (!ok) return;
    try {
      await api.delete(url());
      toast('ok', `${name()} deleted`);
      props.onChanged();
      props.onClose();
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  const resetPassword = async (e: Event) => {
    e.preventDefault();
    const next = password();
    if (!next || busy()) return;
    setBusy(true);
    try {
      await api.put(`/data/users/user/password?id=${enc(props.id)}${sourceParam()}`, { password: next });
      toast('ok', 'Password set', 'Copy it now; it is not shown again.');
    } catch (err) {
      toast('bad', 'Could not set the password', err instanceof Error ? err.message : undefined);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Sheet
      open
      wide
      title={<><span class="dx-avatar" aria-hidden="true">{letters(name())}</span><span class="dx-sheet-id">{name()}</span></>}
      meta={<Show when={detail()?.is_admin}>admin</Show>}
      onClose={props.onClose}
    >
      <Show when={result.latest && !result.latest.ok}>
        <section class="sheet-sec"><DataError message={(result.latest as { ok: false; message: string }).message} /></section>
      </Show>
      <Show when={detail()}>
        {(d) => (
          <>
            <section class="sheet-sec dx-actions">
              <button class="btn btn-sm" onClick={() => navigate(`/database/table/${enc(props.source.table)}?id=${enc(props.id)}`)}>Edit fields</button>
              <Show when={props.canWrite}>
                <Show when={d().is_admin !== null}>
                  <button class="btn btn-sm" onClick={() => void setAdmin(!d().is_admin)}>{d().is_admin ? 'Remove admin' : 'Make admin'}</button>
                </Show>
                <button class="btn btn-sm dx-danger" onClick={() => void remove()}>Delete</button>
              </Show>
            </section>

            <SheetSection label="Profile" meta={<span class="dx-sheet-id">{props.id}</span>}>
              <div class="dx-kv">
                <For each={rows()}>
                  {([field, value]) => (
                    <div>
                      <span class="cap">{field}</span>
                      <div class="dx-kv-v"><ValueCell value={value} /></div>
                    </div>
                  )}
                </For>
              </div>
            </SheetSection>

            <Show when={props.canWrite && props.source.can_set_password}>
              <SheetSection label="Password" meta={<span>sessions already open keep working</span>}>
                <form class="dx-pass" onSubmit={resetPassword}>
                  <input
                    class="dx-input mono"
                    aria-label="New password"
                    autocomplete="new-password"
                    placeholder="New password, 8 characters or more"
                    value={password() ?? ''}
                    onInput={(e) => setPassword(e.currentTarget.value)}
                  />
                  <button class="btn btn-sm" type="button" onClick={() => setPassword(generatePassword())}>Generate</button>
                  <Show when={password()}>{(p) => <CopyText value={p()} />}</Show>
                  <button class="btn btn-sm btn-primary" type="submit" disabled={busy() || (password()?.length ?? 0) < 8}>Set password</button>
                </form>
              </SheetSection>
            </Show>

            <Show when={d().sessions}>
              {(sessions) => (
                <SheetSection label="Live now" meta={<span><b>{sessions().length}</b> {sessions().length === 1 ? 'tab' : 'tabs'}</span>}>
                  <Show when={sessions().length} fallback={<p class="cap dx-note">No live views right now.</p>}>
                    <table class="fx-list">
                      <tbody>
                        <For each={sessions()}>
                          {(s) => <tr><td class="dx-dim">{s.client_id}</td><td class="r">{s.views} {s.views === 1 ? 'view' : 'views'}</td></tr>}
                        </For>
                      </tbody>
                    </table>
                  </Show>
                </SheetSection>
              )}
            </Show>

            <Show when={d().is_admin && d().admin}>
              {(a) => (
                <SheetSection label="Roster">
                  <div class="dx-kv">
                    <div><span class="cap">Added</span><div class="dx-kv-v"><ValueCell value={a().added_at} /></div></div>
                    <Show when={a().note}><div><span class="cap">Note</span><div class="dx-kv-v">{a().note}</div></div></Show>
                  </div>
                </SheetSection>
              )}
            </Show>

            <Show when={d().flags?.length}>
              <SheetSection label="Feature flags" meta={<b>{d().flags!.length}</b>}>
                <table class="fx-list">
                  <tbody>
                    <For each={d().flags!}>{(f) => <tr><td>{f.key}</td><td class="r dx-dim">{f.variant}</td></tr>}</For>
                  </tbody>
                </table>
              </SheetSection>
            </Show>
          </>
        )}
      </Show>
    </Sheet>
  );
}

/** A new row in the app's user table, with what its `SIGNIN` needs. */
function NewUserSheet(props: { source: UserSource; fields: FieldInfo[]; onClose: () => void; onCreated: () => void; onOpen: (id: string) => void }) {
  const [identifier, setIdentifier] = createSignal('');
  const [password, setPassword] = createSignal(generatePassword());
  const [drafts, setDrafts] = createSignal<Record<string, string>>({});
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string>();
  /** Made: the sheet stays, because its password is on no other screen. */
  const [created, setCreated] = createSignal<string | null>(null);

  // What the database will refuse the row without.
  const required = () =>
    props.fields.filter((f) => f.required && !f.credential && !f.readonly && f.name !== props.source.identifier && f.name !== 'id');

  const submit = async (e: Event) => {
    e.preventDefault();
    setError(undefined);
    const fields: Record<string, unknown> = {};
    for (const f of required()) {
      const parsed = parseDraft(f.kind, undefined, drafts()[f.name] ?? '');
      if (!parsed.ok) return void setError(`${f.name}: ${parsed.error}`);
      fields[f.name] = parsed.value;
    }
    setBusy(true);
    try {
      const q = props.source.access ? `?source=${enc(props.source.access)}` : '';
      const out = await api.post<{ row: Row }>(`/data/users${q}`, { identifier: identifier(), password: password(), fields });
      setCreated(out.row.id as string);
      props.onCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not create the user');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Sheet open title="New user" meta={<span>in {props.source.table}</span>} onClose={props.onClose}>
      <Show when={created()}>
        {(id) => (
          <section class="sheet-sec dx-fields">
            <div class="dx-field">
              <div class="dx-field-head"><b>{identifier()}</b><span class="cap">created</span></div>
              <p class="cap dx-note">Hand over the password now. It is not shown again.</p>
            </div>
            <div class="dx-pass">
              <input class="dx-input mono" readOnly aria-label="The new user's password" value={password()} />
              <CopyText value={password()} />
            </div>
            <div class="dx-actions">
              <button class="btn btn-sm btn-primary" onClick={() => props.onOpen(id())}>Open user</button>
              <button class="btn btn-sm" onClick={props.onClose}>Done</button>
            </div>
          </section>
        )}
      </Show>
      <Show when={!created()}>
      <form onSubmit={submit}>
        <section class="sheet-sec dx-fields">
          <div class="dx-field">
            <div class="dx-field-head"><b>{props.source.identifier}</b><span class="cap warn">required</span></div>
            <input class="dx-input" autocomplete="off" required value={identifier()} onInput={(e) => setIdentifier(e.currentTarget.value)} />
          </div>
          <div class="dx-field">
            <div class="dx-field-head"><b>password</b><span class="cap warn">required</span></div>
            <div class="dx-pass">
              <input class="dx-input mono" autocomplete="new-password" minLength={8} required value={password()} onInput={(e) => setPassword(e.currentTarget.value)} />
              <button class="btn btn-sm" type="button" onClick={() => setPassword(generatePassword())}>Generate</button>
              <CopyText value={password()} />
            </div>
          </div>
          <For each={required()}>
            {(f) => (
              <div class="dx-field">
                <div class="dx-field-head"><b>{f.name}</b><span class="cap">{f.kind}</span><span class="cap warn">required</span></div>
                <FieldInput
                  field={f}
                  name={f.name}
                  value={undefined}
                  draft={drafts()[f.name] ?? draftOf(undefined)}
                  onInput={(text) => setDrafts({ ...drafts(), [f.name]: text })}
                />
              </div>
            )}
          </For>
          <p class="cap dx-note">The row is written directly: the app’s SIGNUP block does not run, and its own events do.</p>
        </section>
        <DataError message={error()} />
        <section class="sheet-sec dx-actions">
          <button class="btn btn-sm btn-primary" type="submit" disabled={busy() || !identifier().trim() || password().length < 8}>Create user</button>
          <button class="btn btn-sm" type="button" onClick={props.onClose}>Cancel</button>
        </section>
      </form>
      </Show>
    </Sheet>
  );
}

