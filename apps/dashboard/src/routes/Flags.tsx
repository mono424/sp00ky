import { For, Show, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { useNavigate, useSearchParams } from '@solidjs/router';
import { api } from '../api/client';
import { confirm, toast } from '../components/Actions';
import { PageHead } from '../components/Chrome';
import { DataError, enc, useCanWrite } from '../components/DataParts';
import { FxCell, Sheet, SheetSection, Sq } from '../components/Frame';
import { formatStamp, relativeStamp } from '../lib/format';
import {
  rolloutShares,
  type EvaluatePage,
  type FlagChange,
  type FlagDetail,
  type FlagPin,
  type FlagRollout,
  type FlagSummary,
  type FlagsAnswer,
  type UsersPage,
} from '../lib/data';

/**
 * The project's feature flags, as `spky flag` keeps them: variants, a default,
 * a rollout per variant and users pinned to a variant. What a user gets is
 * written by the project's own `fn::feature::assign`, the evaluator the CLI and
 * the DevTools use too, into the table its clients sync.
 */

type Listing = 'all' | 'on' | 'off';

const message = (err: unknown) => (err instanceof Error ? err.message : 'Request failed');

/* ------------------------------------------------------------------ */
/* Re-evaluation                                                        */
/* ------------------------------------------------------------------ */

/** Flags being re-evaluated now: users walked so far, and of how many. */
const [runs, setRuns] = createSignal<Record<string, { done: number; total: number | null }>>({});

function setRun(key: string, run: { done: number; total: number | null } | null) {
  const next = { ...runs() };
  if (run) next[key] = run;
  else delete next[key];
  setRuns(next);
}

export const evaluating = (key: string) => runs()[key];

function holdTab(e: BeforeUnloadEvent) {
  e.preventDefault();
}

/**
 * Walk every user and write what `key` now gives them, a page per request. A
 * page posts each changed row to the SSP inside its transaction, most of a
 * second a row on Cloudflare, so the page size follows how long the last one
 * took: about two seconds a request. Closing the tab stops it part way, which
 * is why the tab asks first and the flag offers to finish.
 */
export async function reevaluate(key: string): Promise<boolean> {
  if (runs()[key]) return false;
  let after: string | null = null;
  let limit = 100;
  let done = 0;
  let total: number | null = null;
  setRun(key, { done, total });
  window.addEventListener('beforeunload', holdTab);
  try {
    for (;;) {
      const p = new URLSearchParams({ key, limit: String(limit) });
      if (after) p.set('after', after);
      const started = performance.now();
      const page = await api.post<EvaluatePage>(`/data/flags/flag/evaluate?${p}`);
      const took = performance.now() - started;
      if (page.total !== null) total = page.total;
      done += page.evaluated;
      setRun(key, { done, total });
      if (!page.next) break;
      after = page.next;
      limit = took > 4000 ? Math.max(10, Math.floor(limit / 2)) : took < 1000 ? Math.min(500, limit * 2) : limit;
    }
    toast('ok', `${key} reached every user`, `${done.toLocaleString()} ${done === 1 ? 'user' : 'users'} evaluated`);
    return true;
  } catch (err) {
    toast('bad', `${key} stopped part way`, `${message(err)}. ${done.toLocaleString()} users were done; re-evaluate the flag to finish.`);
    return false;
  } finally {
    setRun(key, null);
    if (Object.keys(runs()).length === 0) window.removeEventListener('beforeunload', holdTab);
  }
}

/** A change saved: walk the users when it can have moved them, then reload. */
async function settle(change: FlagChange, reload: () => void) {
  reload();
  if (change.evaluate && (await reevaluate(change.flag.key))) reload();
}

/* ------------------------------------------------------------------ */
/* The list                                                             */
/* ------------------------------------------------------------------ */

export function Flags() {
  const canWrite = useCanWrite();
  const [query, setQuery] = useSearchParams<{ key?: string }>();
  const [filter, setFilter] = createSignal<Listing>('all');
  const [find, setFind] = createSignal('');
  const [creating, setCreating] = createSignal(false);
  const [result, { refetch }] = createResource(() => api.getResult<FlagsAnswer>('/data/flags'));
  const answer = () => {
    const r = result.latest;
    return r?.ok ? r.value : undefined;
  };
  const flags = () => answer()?.flags ?? [];
  const shown = createMemo(() => {
    const q = find().trim().toLowerCase();
    return flags().filter(
      (f) =>
        (filter() === 'all' || (filter() === 'on') === f.enabled) &&
        (!q || f.key.toLowerCase().includes(q) || (f.description ?? '').toLowerCase().includes(q)),
    );
  });
  const reload = () => void refetch();
  const open = (key: string) => setQuery({ key });

  return (
    <>
      <PageHead
        title="Feature flags"
        actions={
          <Show when={canWrite() && answer()?.installed}>
            <button class="btn btn-sm btn-primary" disabled={!answer()?.evaluator} onClick={() => setCreating(true)}>
              New flag
            </button>
          </Show>
        }
      />
      <div class="fx-page">
        <Show when={result.latest && !result.latest.ok}>
          <section class="fx-row"><FxCell><DataError message={(result.latest as { ok: false; message: string }).message} /></FxCell></section>
        </Show>

        <Show when={answer() && !answer()!.installed}>
          <section class="fx-row">
            <FxCell label="Feature flags">
              <p class="cap dx-note">This project's schema has no flag tables yet. Deploy it with a current spky (`spky deploy`) and they appear here.</p>
            </FxCell>
          </section>
        </Show>
        <Show when={answer()?.installed && !answer()!.evaluator}>
          <section class="fx-row">
            <FxCell label="Needs a redeploy">
              <p class="cap dx-note">This project's flag functions predate the console, so flags are shown but cannot be changed here. Redeploy it (`spky deploy`) to update them.</p>
            </FxCell>
          </section>
        </Show>
        <Show when={answer()?.installed && !answer()!.users_table}>
          <section class="fx-row">
            <FxCell label="No user table">
              <p class="cap dx-note">Flags are evaluated for the records of a `user` table, and this project has none, so no client receives an assignment.</p>
            </FxCell>
          </section>
        </Show>

        <Show when={answer()?.installed}>
          <section class="fx-row dx-stats">
            <FxCell label="Flags">
              <div class="fx-big">{flags().length.toLocaleString()}</div>
              <div class="fx-sub cap"><span>defined</span></div>
            </FxCell>
            <FxCell label="On">
              <div class="fx-big">{flags().filter((f) => f.enabled).length.toLocaleString()}</div>
              <div class="fx-sub cap"><span>evaluating their rules</span></div>
            </FxCell>
            <FxCell label="Rolling out">
              <div class="fx-big">{flags().filter((f) => f.enabled && f.rollouts.some((r) => r.percent > 0 && r.percent < 100)).length.toLocaleString()}</div>
              <div class="fx-sub cap"><span>to part of the users</span></div>
            </FxCell>
            <FxCell label="Pinned">
              <div class="fx-big">{flags().reduce((n, f) => n + f.pinned, 0).toLocaleString()}</div>
              <div class="fx-sub cap"><span>user pins across flags</span></div>
            </FxCell>
          </section>

          <section class="fx-row">
            <FxCell>
              <div class="inc-filters dx-user-filters">
                <div class="fx-range" role="group" aria-label="Show">
                  <For each={[['all', 'All'], ['on', 'On'], ['off', 'Off']] as [Listing, string][]}>
                    {([value, label]) => <button classList={{ on: filter() === value }} aria-pressed={filter() === value} onClick={() => setFilter(value)}>{label}</button>}
                  </For>
                </div>
                <div class="inc-find">
                  <input type="search" placeholder="Search key or description" aria-label="Search flags" value={find()} onInput={(e) => setFind(e.currentTarget.value)} />
                  <button class="btn btn-sm" onClick={reload}>Refresh</button>
                </div>
              </div>
              <Show when={answer()} fallback={<div class="fx-empty cap">Loading…</div>}>
                <Show
                  when={shown().length}
                  fallback={<div class="fx-empty cap">{find() ? 'No flag matches' : flags().length ? `No flag is ${filter()}` : 'No flags yet'}</div>}
                >
                  <table class="fx-list dx-users ff-list">
                    <thead>
                      <tr>
                        <th class="cap">Flag</th>
                        <th class="cap">State</th>
                        <th class="cap dx-col-wide">Variants</th>
                        <th class="cap">Targeting</th>
                        <th class="cap dx-col-mid">Changed</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={shown()}>{(f) => <FlagRow flag={f} onOpen={open} />}</For>
                    </tbody>
                  </table>
                </Show>
              </Show>
            </FxCell>
          </section>
        </Show>
      </div>

      <Show when={query.key} keyed>
        {(key) => <FlagSheet flagKey={key} canWrite={canWrite() && !!answer()?.evaluator} onClose={() => setQuery({ key: undefined })} onChanged={reload} />}
      </Show>
      <Show when={creating()}>
        <NewFlagSheet
          onClose={() => setCreating(false)}
          onCreated={(change) => {
            setCreating(false);
            open(change.flag.key);
            void settle(change, reload);
          }}
        />
      </Show>
    </>
  );
}

/** What decides a flag beyond its default, in a few words. */
function targeting(f: FlagSummary): string {
  if (!f.enabled) return `everyone ${f.default_variant}`;
  const shares = rolloutShares(f.rollouts);
  const parts = f.rollouts.map((r, i) => `${r.variant} ${shares[i]}%`).filter((_, i) => shares[i] > 0);
  if (f.pinned) parts.push(`${f.pinned} pinned`);
  return parts.length ? parts.join(' · ') : `everyone ${f.default_variant}`;
}

function FlagRow(props: { flag: FlagSummary; onOpen: (key: string) => void }) {
  const f = () => props.flag;
  const open = () => props.onOpen(f().key);
  const run = () => evaluating(f().key);
  return (
    <tr class="inc-tr" tabIndex={0} onClick={open} onKeyDown={(e) => e.key === 'Enter' && open()}>
      <td class="ff-key">
        <span class="dx-who-name">{f().key}</span>
        <Show when={f().description}><span class="ff-desc">{f().description}</span></Show>
      </td>
      <td>
        <span class="fx-state cap"><Sq tone={f().enabled ? 'ok-live' : 'idle'} />{f().enabled ? 'on' : 'off'}</span>
      </td>
      <td class="dx-col-wide">
        <span class="ff-variants">
          <For each={f().variants}>{(v) => <span class="dx-tag" classList={{ 'ff-default': v === f().default_variant }} title={v === f().default_variant ? 'The default' : undefined}>{v}</span>}</For>
        </span>
      </td>
      <td class="dx-dim">
        <Show when={run()} fallback={targeting(f())}>
          {(r) => <span class="ff-running">evaluating {r().done.toLocaleString()}{r().total !== null ? ` of ${r().total!.toLocaleString()}` : ''}</span>}
        </Show>
      </td>
      <td class="dx-col-mid" title={formatStamp(f().updated_at)}>{relativeStamp(f().updated_at)}</td>
    </tr>
  );
}

/* ------------------------------------------------------------------ */
/* One flag                                                             */
/* ------------------------------------------------------------------ */

function FlagSheet(props: { flagKey: string; canWrite: boolean; onClose: () => void; onChanged: () => void }) {
  const navigate = useNavigate();
  const url = () => `/data/flags/flag?key=${enc(props.flagKey)}`;
  const [result, { refetch }] = createResource(() => api.getResult<FlagDetail>(url()));
  const detail = () => {
    const r = result.latest;
    return r?.ok ? r.value : undefined;
  };
  const flag = () => detail()?.flag;
  const run = () => evaluating(props.flagKey);
  const [busy, setBusy] = createSignal(false);
  const reload = () => {
    void refetch();
    props.onChanged();
  };

  const change = async (body: Record<string, unknown>, done?: string) => {
    if (busy()) return false;
    setBusy(true);
    try {
      const out = await api.put<FlagChange>(url(), body);
      if (done) toast('ok', done);
      void settle(out, reload);
      return true;
    } catch (err) {
      toast('bad', 'Could not save the flag', message(err));
      return false;
    } finally {
      setBusy(false);
    }
  };

  const toggle = async (on: boolean) => {
    const f = flag();
    if (!f) return;
    const ok = await confirm(
      on
        ? { title: `Turn ${f.key} on?`, consequences: ['Its rollouts and pinned users decide again what each user gets.', 'Every user is re-evaluated; the page shows how far it got.'], verb: 'Turn on' }
        : { title: `Turn ${f.key} off?`, consequences: [`Every user gets ${f.default_variant}, pinned users too. The rules stay for when it is turned on again.`, 'Every user is re-evaluated; the page shows how far it got.'], verb: 'Turn off' },
    );
    if (ok) await change({ enabled: on });
  };

  const remove = async () => {
    const f = flag();
    if (!f) return;
    const ok = await confirm({
      title: `Delete ${f.key}?`,
      consequences: [
        'Deletes the flag, its rules and every user’s assignment of it.',
        'Clients still asking for it get the fallback their code passes.',
        'This cannot be undone, short of restoring a backup.',
      ],
      verb: 'Delete flag',
      typeToConfirm: f.key,
    });
    if (!ok) return;
    try {
      await api.delete(url());
      toast('ok', `${f.key} deleted`);
      props.onChanged();
      props.onClose();
    } catch (err) {
      toast('bad', 'Could not delete', message(err));
    }
  };

  return (
    <Sheet open wide title={<span class="dx-sheet-id">{props.flagKey}</span>} meta={<Show when={flag()}>{(f) => <>{f().enabled ? 'on' : 'off'}</>}</Show>} onClose={props.onClose}>
      <Show when={result.latest && !result.latest.ok}>
        <section class="sheet-sec"><DataError message={(result.latest as { ok: false; message: string }).message} /></section>
      </Show>
      <Show when={flag()}>
        {(f) => (
          <>
            <section class="sheet-sec dx-actions">
              <Show when={f().description}><p class="dx-note ff-sheet-desc">{f().description}</p></Show>
              <Show when={props.canWrite}>
                <label class="switch" title={f().enabled ? 'Rules decide' : `Everyone gets ${f().default_variant}`}>
                  <input type="checkbox" checked={f().enabled} disabled={busy() || !!run()} onChange={(e) => { const on = e.currentTarget.checked; e.currentTarget.checked = f().enabled; void toggle(on); }} />
                  <span class="switch-track" />
                  <span class="cap">{f().enabled ? 'On' : 'Off'}</span>
                </label>
                <button class="btn btn-sm" disabled={!!run()} title="Write what the flag gives each user now, for users a change did not reach" onClick={() => void reevaluate(f().key).then((ok) => ok && reload())}>
                  Re-evaluate all users
                </button>
                <button class="btn btn-sm dx-danger" disabled={!!run()} onClick={() => void remove()}>Delete</button>
              </Show>
            </section>

            <Show when={run()}>
              {(r) => (
                <section class="sheet-sec ff-progress">
                  <div class="ff-track" role="progressbar" aria-valuemin={0} aria-valuemax={r().total ?? undefined} aria-valuenow={r().done}>
                    <i style={{ width: r().total ? `${Math.min(100, (r().done / r().total!) * 100)}%` : '8%' }} />
                  </div>
                  <span class="cap">Evaluating {r().done.toLocaleString()}{r().total !== null ? ` of ${r().total!.toLocaleString()}` : ''} users. Keep this tab open.</span>
                </section>
              )}
            </Show>

            <SheetSection label="Users per variant" meta={<Show when={detail()?.counts === null}><span>count timed out</span></Show>}>
              <Distribution flag={f()} counts={detail()?.counts ?? null} />
              <Show when={!f().enabled}>
                <p class="cap dx-note ff-gap">Off: everyone gets {f().default_variant}, whatever the rules below say.</p>
              </Show>
            </SheetSection>

            <SheetSection label="Rollout" meta={<span>a stable share of users, by a hash of the flag and the user</span>}>
              <RolloutEditor flag={f()} canWrite={props.canWrite} busy={busy() || !!run()} onSave={(rollouts) => change({ rollouts }, 'Rollout saved')} />
            </SheetSection>

            <SheetSection label="Pinned users" meta={<span><b>{detail()?.pins_total ?? 0}</b> pinned · a pin beats every rollout</span>}>
              <Pins
                flag={f()}
                pins={detail()?.pins ?? []}
                total={detail()?.pins_total ?? 0}
                canWrite={props.canWrite}
                onOpenUser={(id) => navigate(`/users?id=${enc(id)}`)}
                onChanged={reload}
              />
            </SheetSection>

            <SheetSection label="Definition">
              <DefinitionEditor flag={f()} canWrite={props.canWrite} busy={busy() || !!run()} onSave={(body) => change(body, 'Flag saved')} />
            </SheetSection>

            <Show when={f().payloads && Object.keys(f().payloads!).length}>
              <SheetSection label="Payloads" meta={<span>sent to clients with each variant</span>}>
                <pre class="fx-pre dx-pre">{JSON.stringify(f().payloads, null, 2)}</pre>
              </SheetSection>
            </Show>

            <Show when={f().other_rules}>
              <section class="sheet-sec"><p class="cap dx-note">{f().other_rules} {f().other_rules === 1 ? 'rule' : 'rules'} of another kind, written outside the console, are kept as they are.</p></section>
            </Show>
          </>
        )}
      </Show>
    </Sheet>
  );
}

function Distribution(props: { flag: FlagSummary; counts: Record<string, number> | null }) {
  const total = () => Object.values(props.counts ?? {}).reduce((a, b) => a + b, 0);
  return (
    <div class="ff-dist">
      <For each={props.flag.variants}>
        {(v) => {
          const n = () => props.counts?.[v] ?? 0;
          const pct = () => (total() ? (n() / total()) * 100 : 0);
          return (
            <div class="ff-dist-row">
              <span class="ff-dist-name">
                {v}
                <Show when={v === props.flag.default_variant}><span class="dx-tag">default</span></Show>
              </span>
              <span class="ff-track"><i style={{ width: `${pct()}%` }} /></span>
              <span class="ff-dist-n">
                <Show when={props.counts} fallback="—">
                  <b>{n().toLocaleString()}</b> <span class="dx-dim">{total() ? `${pct() < 1 && n() ? '<1' : Math.round(pct())}%` : ''}</span>
                </Show>
              </span>
            </div>
          );
        }}
      </For>
    </div>
  );
}

/** A percent per variant other than the default, in the order they are tried. */
function RolloutEditor(props: { flag: FlagSummary; canWrite: boolean; busy: boolean; onSave: (rollouts: FlagRollout[]) => Promise<boolean> }) {
  const targets = () => {
    // Saved rollouts first, in their order, then the variants without one.
    const saved = props.flag.rollouts.map((r) => r.variant);
    const rest = props.flag.variants.filter((v) => v !== props.flag.default_variant && !saved.includes(v));
    return [...saved, ...rest];
  };
  const saved = () => Object.fromEntries(props.flag.rollouts.map((r) => [r.variant, r.percent]));
  const [drafts, setDrafts] = createSignal<Record<string, string>>({});
  const percentOf = (v: string) => {
    const text = drafts()[v];
    if (text === undefined) return saved()[v] ?? 0;
    const n = Number(text);
    return Number.isInteger(n) && n >= 0 && n <= 100 ? n : NaN;
  };
  const list = (): FlagRollout[] => targets().map((variant) => ({ variant, percent: percentOf(variant) }));
  const invalid = () => list().some((r) => Number.isNaN(r.percent));
  const dirty = () => list().some((r) => r.percent !== (saved()[r.variant] ?? 0));
  const shares = () => rolloutShares(list().map((r) => ({ ...r, percent: Number.isNaN(r.percent) ? 0 : r.percent })));
  const rest = () => Math.max(0, 100 - shares().reduce((a, b) => a + b, 0));

  return (
    <div class="ff-rollout">
      <For each={targets()}>
        {(v, i) => (
          <div class="ff-roll-row">
            <span class="ff-dist-name">{v}</span>
            <Show
              when={props.canWrite}
              fallback={<span class="dx-num">{saved()[v] ?? 0}%</span>}
            >
              <span class="ff-pct">
                <input
                  class="dx-input mono"
                  inputmode="numeric"
                  aria-label={`Percent of users for ${v}`}
                  value={drafts()[v] ?? String(saved()[v] ?? 0)}
                  disabled={props.busy}
                  onInput={(e) => setDrafts({ ...drafts(), [v]: e.currentTarget.value.trim() })}
                />
                <span class="cap">%</span>
              </span>
            </Show>
            <span class="cap dx-dim">
              <Show when={shares()[i()] !== percentOf(v) && !Number.isNaN(percentOf(v))} fallback={shares()[i()] ? `${shares()[i()]}% of users` : 'nobody'}>
                {shares()[i()]}% of users, the rest went to a variant above
              </Show>
            </span>
          </div>
        )}
      </For>
      <p class="cap dx-note ff-gap">
        {rest()}% get {props.flag.default_variant}, the default, unless pinned. Raising a percent keeps the users who had the variant and adds to them.
      </p>
      <Show when={props.canWrite}>
        <div class="dx-actions ff-save">
          <button class="btn btn-sm btn-primary" disabled={props.busy || invalid() || !dirty()} onClick={() => void props.onSave(list()).then((ok) => ok && setDrafts({}))}>
            Save rollout
          </button>
          <Show when={dirty()}><button class="btn btn-sm" onClick={() => setDrafts({})}>Reset</button></Show>
          <Show when={invalid()}><span class="cap warn">A percent is a whole number from 0 to 100</span></Show>
        </div>
      </Show>
    </div>
  );
}

function Pins(props: {
  flag: FlagSummary;
  pins: FlagPin[];
  total: number;
  canWrite: boolean;
  onOpenUser: (id: string) => void;
  onChanged: () => void;
}) {
  const [working, setWorking] = createSignal<string | null>(null);
  const pin = async (id: string, variant: string | null, label: string) => {
    setWorking(id);
    try {
      await api.put(`/data/flags/flag/user?key=${enc(props.flag.key)}&id=${enc(id)}`, { variant });
      toast('ok', variant ? `${label} pinned to ${variant}` : `${label} unpinned`, variant ? undefined : 'The rollouts and the default decide for them again.');
      props.onChanged();
      return true;
    } catch (err) {
      toast('bad', 'Could not change the pin', message(err));
      return false;
    } finally {
      setWorking(null);
    }
  };

  return (
    <>
      <Show when={props.pins.length} fallback={<p class="cap dx-note">Nobody is pinned. A pinned user gets their variant whatever the rollout says.</p>}>
        <table class="fx-list ff-pins">
          <tbody>
            <For each={props.pins}>
              {(p) => (
                <tr>
                  <td class="ff-key">
                    <button class="fx-link dx-rec" onClick={() => props.onOpenUser(p.id)}>{p.name ?? p.id}</button>
                    <Show when={p.name}><span class="ff-desc">{p.id}</span></Show>
                    <Show when={p.exists === false}><span class="dx-tag ff-gone" title="The user no longer exists; the pin is left over">deleted</span></Show>
                  </td>
                  <td class="r">
                    <Show when={props.canWrite} fallback={<span class="dx-tag">{p.variant}</span>}>
                      <select
                        class="dx-select ff-variant"
                        aria-label={`Variant for ${p.name ?? p.id}`}
                        value={p.variant}
                        disabled={working() === p.id}
                        onChange={(e) => void pin(p.id, e.currentTarget.value, p.name ?? p.id)}
                      >
                        <For each={props.flag.variants}>{(v) => <option value={v} selected={v === p.variant}>{v}</option>}</For>
                      </select>
                      <button class="btn btn-sm" disabled={working() === p.id} onClick={() => void pin(p.id, null, p.name ?? p.id)}>Unpin</button>
                    </Show>
                  </td>
                </tr>
              )}
            </For>
          </tbody>
        </table>
        <Show when={props.total > props.pins.length}>
          <p class="cap dx-note ff-gap">Showing {props.pins.length} of {props.total.toLocaleString()} pinned users.</p>
        </Show>
      </Show>
      <Show when={props.canWrite}>
        <PinFinder flag={props.flag} pinned={props.pins} onPin={(id, variant, label) => pin(id, variant, label)} />
      </Show>
    </>
  );
}

const FIND_DEBOUNCE_MS = 250;

/** Find a user by what the Users page searches, and pin them. */
function PinFinder(props: { flag: FlagSummary; pinned: FlagPin[]; onPin: (id: string, variant: string, label: string) => Promise<boolean> }) {
  const firstOther = () => props.flag.variants.find((v) => v !== props.flag.default_variant) ?? props.flag.variants[0];
  const [variant, setVariant] = createSignal(firstOther());
  const [input, setInput] = createSignal('');
  const [q, setQ] = createSignal('');
  let timer: ReturnType<typeof setTimeout> | undefined;
  onCleanup(() => clearTimeout(timer));
  const onFind = (text: string) => {
    setInput(text);
    clearTimeout(timer);
    timer = setTimeout(() => setQ(text.trim()), FIND_DEBOUNCE_MS);
  };
  const [found] = createResource(q, (text) => api.getResult<UsersPage>(`/data/users?limit=6&q=${enc(text)}`));
  const users = () => {
    const r = found.latest;
    return r?.ok ? r.value.users : [];
  };
  const nameOf = (row: Record<string, unknown>, identifier?: string | null) => {
    const v = identifier ? row[identifier] : undefined;
    return typeof v === 'string' && v ? v : String(row.id ?? '');
  };
  const pinnedTo = (id: string) => props.pinned.find((p) => p.id === id)?.variant;

  return (
    <div class="ff-find">
      <div class="dx-pass">
        <input class="dx-input" type="search" placeholder="Find a user to pin" aria-label="Find a user to pin" value={input()} onInput={(e) => onFind(e.currentTarget.value)} />
        <select class="dx-select ff-variant" aria-label="Variant to pin to" value={variant()} onChange={(e) => setVariant(e.currentTarget.value)}>
          <For each={props.flag.variants}>{(v) => <option value={v} selected={v === variant()}>{v}</option>}</For>
        </select>
      </div>
      <Show when={q()}>
        <Show when={found.latest && !found.latest.ok}>
          <DataError message={(found.latest as { ok: false; message: string }).message} />
        </Show>
        <Show when={users().length} fallback={<p class="cap dx-note ff-gap">{found.loading ? 'Searching…' : 'No user matches'}</p>}>
          <table class="fx-list ff-pins">
            <tbody>
              <For each={users()}>
                {(u) => {
                  const id = () => String(u.row.id ?? '');
                  const label = () => nameOf(u.row, found.latest?.ok ? found.latest.value.source?.identifier : null);
                  return (
                    <tr>
                      <td class="ff-key">
                        <span class="dx-who-name">{label()}</span>
                        <span class="ff-desc">{id()}</span>
                        <Show when={pinnedTo(id())}>{(v) => <span class="dx-tag">pinned {v()}</span>}</Show>
                      </td>
                      <td class="r">
                        <button
                          class="btn btn-sm"
                          disabled={pinnedTo(id()) === variant()}
                          onClick={() => void props.onPin(id(), variant(), label()).then((ok) => ok && onFind(''))}
                        >
                          Pin to {variant()}
                        </button>
                      </td>
                    </tr>
                  );
                }}
              </For>
            </tbody>
          </table>
        </Show>
      </Show>
    </div>
  );
}

const splitVariants = (text: string) => text.split(',').map((v) => v.trim()).filter(Boolean);

function DefinitionEditor(props: { flag: FlagSummary; canWrite: boolean; busy: boolean; onSave: (body: Record<string, unknown>) => Promise<boolean> }) {
  const [description, setDescription] = createSignal<string>();
  const [variants, setVariants] = createSignal<string>();
  const [fallback, setFallback] = createSignal<string>();
  const descriptionNow = () => description() ?? props.flag.description ?? '';
  const variantsNow = () => splitVariants(variants() ?? props.flag.variants.join(', '));
  const defaultNow = () => fallback() ?? props.flag.default_variant;
  const body = () => {
    const out: Record<string, unknown> = {};
    if (descriptionNow().trim() !== (props.flag.description ?? '')) out.description = descriptionNow().trim() || null;
    if (variantsNow().join('\n') !== props.flag.variants.join('\n')) out.variants = variantsNow();
    if (defaultNow() !== props.flag.default_variant) out.default_variant = defaultNow();
    return out;
  };
  const problem = () => {
    const list = variantsNow();
    if (list.length < 2) return 'A flag has at least two variants';
    if (new Set(list).size !== list.length) return 'A variant is listed twice';
    if (!list.includes(defaultNow())) return 'The default must be one of the variants';
    return undefined;
  };
  const reset = () => {
    setDescription(undefined);
    setVariants(undefined);
    setFallback(undefined);
  };

  return (
    <Show
      when={props.canWrite}
      fallback={
        <div class="dx-kv">
          <div><span class="cap">Default</span><div class="dx-kv-v">{props.flag.default_variant}</div></div>
          <div><span class="cap">Variants</span><div class="dx-kv-v">{props.flag.variants.join(', ')}</div></div>
          <div><span class="cap">Created</span><div class="dx-kv-v" title={formatStamp(props.flag.created_at)}>{relativeStamp(props.flag.created_at)}</div></div>
          <div><span class="cap">Changed</span><div class="dx-kv-v" title={formatStamp(props.flag.updated_at)}>{relativeStamp(props.flag.updated_at)}</div></div>
        </div>
      }
    >
      <div class="dx-fields">
        <div class="dx-field">
          <div class="dx-field-head"><b>description</b><span class="cap">what it is for, for the next person</span></div>
          <input class="dx-input" value={descriptionNow()} disabled={props.busy} onInput={(e) => setDescription(e.currentTarget.value)} />
        </div>
        <div class="dx-field">
          <div class="dx-field-head"><b>variants</b><span class="cap">comma separated · one a rule or the default uses cannot go</span></div>
          <input class="dx-input mono" value={variants() ?? props.flag.variants.join(', ')} disabled={props.busy} onInput={(e) => setVariants(e.currentTarget.value)} />
        </div>
        <div class="dx-field">
          <div class="dx-field-head"><b>default</b><span class="cap">for everyone no rollout or pin reaches, and for all when off</span></div>
          <select class="dx-input" value={defaultNow()} disabled={props.busy} onChange={(e) => setFallback(e.currentTarget.value)}>
            <For each={variantsNow()}>{(v) => <option value={v} selected={v === defaultNow()}>{v}</option>}</For>
          </select>
        </div>
        <div class="dx-actions ff-save">
          <button class="btn btn-sm btn-primary" disabled={props.busy || !!problem() || !Object.keys(body()).length} onClick={() => void props.onSave(body()).then((ok) => ok && reset())}>
            Save
          </button>
          <Show when={Object.keys(body()).length}><button class="btn btn-sm" onClick={reset}>Reset</button></Show>
          <Show when={problem()}>{(p) => <span class="cap warn">{p()}</span>}</Show>
          <span class="cap dx-dim ff-stamp" title={formatStamp(props.flag.updated_at)}>changed {relativeStamp(props.flag.updated_at)}</span>
        </div>
      </div>
    </Show>
  );
}

/* ------------------------------------------------------------------ */
/* A new flag                                                           */
/* ------------------------------------------------------------------ */

function NewFlagSheet(props: { onClose: () => void; onCreated: (change: FlagChange) => void }) {
  const [key, setKey] = createSignal('');
  const [description, setDescription] = createSignal('');
  const [variants, setVariants] = createSignal('off, on');
  const [fallback, setFallback] = createSignal<string>();
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string>();
  const list = () => splitVariants(variants());
  const defaultNow = () => (fallback() && list().includes(fallback()!) ? fallback()! : list()[0] ?? '');
  const problem = () => {
    if (!/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,99}$/.test(key().trim())) return key().trim() ? 'Letters, digits and _ - . : /, starting with a letter or digit' : undefined;
    if (list().length < 2) return 'A flag has at least two variants';
    if (new Set(list()).size !== list().length) return 'A variant is listed twice';
    return undefined;
  };

  const submit = async (e: Event) => {
    e.preventDefault();
    if (busy() || problem()) return;
    setError(undefined);
    setBusy(true);
    try {
      const out = await api.post<FlagChange>('/data/flags', {
        key: key().trim(),
        description: description().trim() || undefined,
        variants: list(),
        default_variant: defaultNow(),
      });
      toast('ok', `${out.flag.key} created`, 'Every user gets the default while it is written for them.');
      props.onCreated(out);
    } catch (err) {
      setError(message(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Sheet open title="New flag" onClose={props.onClose}>
      <form onSubmit={submit}>
        <section class="sheet-sec dx-fields">
          <div class="dx-field">
            <div class="dx-field-head"><b>key</b><span class="cap">what the app asks for: client.feature('…')</span><span class="cap warn">required</span></div>
            <input class="dx-input mono" autocomplete="off" spellcheck={false} required placeholder="new-checkout" value={key()} onInput={(e) => setKey(e.currentTarget.value)} />
          </div>
          <div class="dx-field">
            <div class="dx-field-head"><b>description</b></div>
            <input class="dx-input" value={description()} onInput={(e) => setDescription(e.currentTarget.value)} />
          </div>
          <div class="dx-field">
            <div class="dx-field-head"><b>variants</b><span class="cap">comma separated</span></div>
            <input class="dx-input mono" spellcheck={false} value={variants()} onInput={(e) => setVariants(e.currentTarget.value)} />
          </div>
          <div class="dx-field">
            <div class="dx-field-head"><b>default</b><span class="cap">what everyone gets until a rollout or a pin says otherwise</span></div>
            <select class="dx-input" value={defaultNow()} onChange={(e) => setFallback(e.currentTarget.value)}>
              <For each={list()}>{(v) => <option value={v} selected={v === defaultNow()}>{v}</option>}</For>
            </select>
          </div>
          <p class="cap dx-note">The flag starts on, with no rules: every user gets the default. Roll it out or pin users from its page.</p>
        </section>
        <DataError message={error() ?? problem()} />
        <section class="sheet-sec dx-actions">
          <button class="btn btn-sm btn-primary" type="submit" disabled={busy() || !key().trim() || !!problem()}>Create flag</button>
          <button class="btn btn-sm" type="button" onClick={props.onClose}>Cancel</button>
        </section>
      </form>
    </Sheet>
  );
}
