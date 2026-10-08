import { For, Show, createEffect, createMemo, createSignal, on, type JSX } from 'solid-js';
import { useDevTools } from '../../context/DevToolsContext';
import { formatMs, formatRelativeTime, formatTime } from '../../utils/formatters';
import { JsonView } from '../ui/JsonView';
import type {
  FailedMutation,
  MutationEntry,
  MutationStatus,
  MutationsState,
  PendingMutationRow,
} from '../../types/devtools';

type View = 'all' | 'queued' | 'synced' | 'rolled-back' | 'tray' | 'debounced';

type Selection = { kind: 'entry' | 'tray'; id: string } | null;

const STATUS_PILL: Record<MutationStatus, string> = {
  pending: 'status-updating',
  retrying: 'status-initializing',
  synced: 'status-active',
  'rolled-back': 'status-destroyed',
  dropped: '',
};

const STATUS_HINT: Record<MutationStatus, string> = {
  pending: 'In the outbox, waiting for the server',
  retrying: 'In the outbox; at least one push failed and it is backing off',
  synced: 'The server accepted it',
  'rolled-back': 'The server rejected it: the local change was undone and the write moved to the failed tray',
  dropped: 'Left the outbox without an answer this tab saw (bucket switch, sign-out, or its row was gone)',
};

// 24x24 stroke glyphs, drawn at 10px in `currentColor` so they take the pill's
// colour: waiting, trying again, done, undone, gone. Factories, not elements:
// Solid JSX is a real DOM node, and one node cannot sit in every row at once.
const STATUS_ICON: Record<MutationStatus, () => JSX.Element> = {
  pending: () => (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </>
  ),
  retrying: () => (
    <>
      <path d="M20 12a8 8 0 1 1-2.34-5.66" />
      <path d="M20 4v5h-5" />
    </>
  ),
  synced: () => <path d="M5 12.5l4.5 4.5L19 7.5" />,
  'rolled-back': () => (
    <>
      <path d="M9 14L4 9l5-5" />
      <path d="M4 9h10.5a5.5 5.5 0 0 1 0 11H11" />
    </>
  ),
  dropped: () => <path d="M6 12h12" />,
};

function StatusPill(props: { status: MutationStatus }) {
  return (
    <span class={`status-pill ${STATUS_PILL[props.status]}`} title={STATUS_HINT[props.status]}>
      <svg class="status-icon" viewBox="0 0 24 24" aria-hidden="true">
        {STATUS_ICON[props.status]()}
      </svg>
      {props.status}
    </span>
  );
}

/** The id minus its table prefix, which is the same on every row. */
const shortId = (id: string) => id.slice(id.indexOf(':') + 1);

function OpBadge(props: { op: string }) {
  return <span class={`op-badge op-${props.op}`}>{props.op}</span>;
}

function Section(props: { title: string; children: JSX.Element }) {
  return (
    <details class="detail-section" open>
      <summary class="detail-section-title">{props.title}</summary>
      <div class="detail-section-body">{props.children}</div>
    </details>
  );
}

function Kv(props: { k: string; children: JSX.Element }) {
  return (
    <div class="kv-row">
      <span class="kv-k">{props.k}</span>
      <span class="kv-v mono kv-wrap">{props.children}</span>
    </div>
  );
}

/** Why nothing is draining, if that is the case: the first thing to rule out. */
function syncLine(m: MutationsState): { text: string; warn: boolean } {
  if (m.role === 'follower') return { text: 'follower tab, the leader tab pushes these writes', warn: false };
  if (m.connection !== 'connected') return { text: `${m.connection}, writes wait until the socket is back`, warn: true };
  if (m.health === 'degraded') {
    return { text: `degraded after ${m.consecutiveFailures} failed rounds${m.lastError ? `: ${m.lastError}` : ''}`, warn: true };
  }
  return { text: `connected${m.role === 'leader' ? ', this tab drains for all tabs' : ''}`, warn: false };
}

function TrayActions(props: { id: string }) {
  const { retryFailedMutation, discardFailedMutation, mutationBusy } = useDevTools();
  return (
    <span class="mt-actions">
      <button
        class="btn mt-btn"
        disabled={mutationBusy() !== null}
        title="Apply it again as a new optimistic write"
        onClick={(e) => {
          e.stopPropagation();
          void retryFailedMutation(props.id);
        }}
      >
        Retry
      </button>
      <button
        class="btn mt-btn flags-btn-danger"
        disabled={mutationBusy() !== null}
        title="Drop it from the tray; the local change stays undone"
        onClick={(e) => {
          e.stopPropagation();
          void discardFailedMutation(props.id);
        }}
      >
        Discard
      </button>
    </span>
  );
}

function MutationDetail(props: { selection: NonNullable<Selection>; onClose: () => void }) {
  const { state, failedMutations, fetchPendingMutation } = useDevTools();
  const entry = createMemo(() =>
    props.selection.kind === 'entry'
      ? state.mutations?.entries.find((e) => e.id === props.selection.id)
      : undefined
  );
  const tray = createMemo(() => failedMutations()?.find((f) => f.id === props.selection.id));
  const [stored, setStored] = createSignal<{ id: string; row: PendingMutationRow | null; error?: string } | null>(null);

  // A queued write's payload stays in the page until somebody looks at it.
  createEffect(
    on(
      () => [entry()?.id, entry()?.status] as const,
      ([id, status]) => {
        if (!id || (status !== 'pending' && status !== 'retrying')) return;
        if (stored()?.id === id) return;
        setStored({ id, row: null });
        fetchPendingMutation(id).then(
          (row) => setStored((cur) => (cur?.id === id ? { id, row } : cur)),
          (err) => setStored((cur) => (cur?.id === id ? { id, row: null, error: String(err) } : cur))
        );
      }
    )
  );

  const payload = (): { data?: unknown; before?: unknown; note?: string } => {
    const t = tray();
    if (t) return t.data === undefined ? { before: t.beforeRecord, note: 'None: a delete carries no payload.' } : { data: t.data, before: t.beforeRecord };
    const e = entry();
    if (!e) return {};
    if (e.status === 'pending' || e.status === 'retrying') {
      const s = stored();
      if (s?.id !== e.id) return { note: 'Loading…' };
      if (s.error) return { note: `Could not read the outbox row: ${s.error}` };
      if (!s.row) return { note: 'It left the outbox while loading.' };
      if (s.row.data === undefined) return { before: s.row.beforeRecord, note: 'None: a delete carries no payload.' };
      return { data: s.row.data, before: s.row.beforeRecord };
    }
    if (e.status === 'rolled-back') return { note: 'Discarded or retried from the tray: the payload is gone.' };
    return { note: 'The payload is not kept once the server has it; open the record in the Database tab.' };
  };

  return (
    <div class="query-detail">
      <div class="detail-tabs" role="tablist">
        <button class="detail-close" title="Close detail panel" onClick={props.onClose}>
          ✕
        </button>
        <button class="detail-tab active" role="tab">
          {entry()?.recordId ?? tray()?.recordId ?? 'Mutation'}
        </button>
      </div>
      <div class="detail-body">
        <Show
          when={entry() || tray()}
          fallback={<div class="detail-empty">This mutation is no longer listed</div>}
        >
          <Section title="General">
            <div class="kv">
              <Kv k="Mutation">{props.selection.id}</Kv>
              <Kv k="Record">{entry()?.recordId ?? tray()?.recordId}</Kv>
              <Kv k="Operation">
                <OpBadge op={entry()?.op ?? tray()?.mutationType ?? ''} />
              </Kv>
              <Show when={entry()}>
                {(e) => (
                  <Kv k="Status">
                    <StatusPill status={e().status} />
                  </Kv>
                )}
              </Show>
              <Show when={entry()?.fields?.length ? entry()?.fields : undefined}>
                {(fields) => <Kv k="Fields">{fields().join(', ')}</Kv>}
              </Show>
            </div>
          </Section>
          <Section title="Timing">
            <div class="kv">
              <Show when={(entry()?.queuedAt ?? tray()?.createdAt) || undefined}>
                {(at) => (
                  <Kv k="Queued">
                    {formatTime(at())} <span class="kv-rel">{formatRelativeTime(at())}</span>
                  </Kv>
                )}
              </Show>
              <Show when={entry()?.settledAt ? entry() : undefined}>
                {(e) => (
                  <Kv k="Answered">
                    {formatTime(e().settledAt ?? 0)}{' '}
                    <span class="kv-rel">after {formatMs((e().settledAt ?? 0) - e().queuedAt)}</span>
                  </Kv>
                )}
              </Show>
              <Show when={tray()?.failedAt}>
                {(at) => (
                  <Kv k="Rejected">
                    {formatTime(at())} <span class="kv-rel">{formatRelativeTime(at())}</span>
                  </Kv>
                )}
              </Show>
              <Kv k="Push attempts">{String(tray()?.attempts ?? entry()?.attempts ?? 0)}</Kv>
            </div>
          </Section>
          <Show when={tray() ?? (entry()?.error ? entry() : undefined)}>
            <Section title="Rejection">
              <div class="kv">
                <Kv k="Error">
                  <span class="storage-error-text">{tray()?.error.message ?? entry()?.error}</span>
                </Kv>
                <Show when={tray()}>
                  {(t) => (
                    <>
                      <Kv k="Kind">{t().error.kind}</Kv>
                      <Kv k="Local revert">{t().revert}</Kv>
                      <div class="kv-row">
                        <span class="kv-k">Tray</span>
                        <span class="kv-v">
                          <TrayActions id={t().id} />
                        </span>
                      </div>
                    </>
                  )}
                </Show>
              </div>
            </Section>
          </Show>
          <Section title="Payload">
            <Show when={payload().note}>
              <div class="muted">{payload().note}</div>
            </Show>
            <Show when={payload().data !== undefined}>
              <JsonView class="code-block" value={payload().data} />
            </Show>
          </Section>
          <Show when={payload().before}>
            <Section title="Before (the row it reverts to)">
              <JsonView class="code-block" value={payload().before} />
            </Section>
          </Show>
        </Show>
      </div>
    </div>
  );
}

export function MutationsTab() {
  const { state, isSp00kyAvailable, failedMutations, failedMutationsError, fetchFailedMutations } = useDevTools();
  const [view, setView] = createSignal<View>('all');
  const [filter, setFilter] = createSignal('');
  const [selection, setSelection] = createSignal<Selection>(null);
  const m = () => state.mutations;

  // The tray lives in the local store; re-read it whenever its size moves.
  createEffect(
    on(
      () => [isSp00kyAvailable(), m()?.counts.failed] as const,
      ([available, failed]) => {
        if (available && failed !== undefined) void fetchFailedMutations();
      }
    )
  );

  const term = () => filter().trim().toLowerCase();
  const matches = (...parts: Array<string | undefined>) =>
    !term() || parts.some((p) => p?.toLowerCase().includes(term()));

  const entries = createMemo((): MutationEntry[] => {
    const all = m()?.entries ?? [];
    const v = view();
    return all.filter((e) => {
      if (v === 'queued' && e.status !== 'pending' && e.status !== 'retrying') return false;
      if (v === 'synced' && e.status !== 'synced') return false;
      if (v === 'rolled-back' && e.status !== 'rolled-back') return false;
      return matches(e.recordId, e.id, e.op, e.fields?.join(' '), e.error);
    });
  });
  const tray = createMemo((): FailedMutation[] =>
    (failedMutations() ?? []).filter((f) => matches(f.recordId, f.id, f.mutationType, f.error.message))
  );
  const debounced = createMemo(() => (m()?.debounced ?? []).filter((d) => matches(d.recordId, d.fields.join(' '))));

  const chips = (): Array<{ id: View; label: string; n?: number; danger?: boolean }> => {
    const c = m()?.counts;
    return [
      { id: 'all', label: 'All', n: m()?.total },
      { id: 'queued', label: 'Queued', n: c ? c.pending + c.retrying : undefined },
      { id: 'synced', label: 'Synced', n: c?.synced },
      { id: 'rolled-back', label: 'Rolled back', n: c?.rolledBack, danger: !!c?.rolledBack },
      { id: 'tray', label: 'Failed tray', n: c?.failed, danger: !!c?.failed },
      { id: 'debounced', label: 'Debounced', n: c?.debounced },
    ];
  };

  const select = (next: Selection) =>
    setSelection((cur) => (cur && next && cur.kind === next.kind && cur.id === next.id ? null : next));

  const collapsed = () => selection() !== null;

  return (
    <div class="queries-container">
      <Show
        when={m()}
        fallback={
          <div class="empty-state">
            {isSp00kyAvailable()
              ? 'This client does not report its mutations. Update @spooky-sync/core to see the outbox here.'
              : 'Waiting for Sp00ky…'}
          </div>
        }
      >
        {(mut) => (
          <>
            <div class="mt-toolbar">
              <input
                class="dt-filter-input mt-filter"
                type="text"
                placeholder="Filter record, field, error"
                value={filter()}
                onInput={(e) => setFilter(e.currentTarget.value)}
              />
              <For each={chips()}>
                {(chip) => (
                  <button
                    class="filter-chip"
                    classList={{ active: view() === chip.id, danger: chip.danger }}
                    onClick={() => {
                      setView(chip.id);
                      setSelection(null);
                    }}
                  >
                    {chip.label}
                    <Show when={chip.n !== undefined}>
                      <span class="chip-count">{chip.n}</span>
                    </Show>
                  </button>
                )}
              </For>
            </div>

            <div class="queries-body">
              <div class="qt-table-wrap" classList={{ collapsed: collapsed() }}>
                <Show when={view() === 'tray'}>
                  <Show when={failedMutationsError()}>
                    <div class="empty-state">Could not read the tray: {failedMutationsError()}</div>
                  </Show>
                  <Show
                    when={tray().length > 0}
                    fallback={
                      <div class="empty-state">
                        {failedMutations() === null ? 'Loading the tray…' : 'No rejected writes. The tray is empty.'}
                      </div>
                    }
                  >
                    <table class="qt-table">
                      <thead>
                        <tr>
                          <th class="mt-col-record">Record</th>
                          <Show when={!collapsed()}>
                            <th class="mt-col-op">Op</th>
                            <th>Error</th>
                            <th class="mt-col-time">Rejected</th>
                            <th class="mt-col-actions" />
                          </Show>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={tray()}>
                          {(f) => (
                            <tr
                              classList={{ selected: selection()?.id === f.id }}
                              onClick={() => select({ kind: 'tray', id: f.id })}
                              title={f.error.message}
                            >
                              <td class="mono">{f.recordId}</td>
                              <Show when={!collapsed()}>
                                <td>
                                  <OpBadge op={f.mutationType} />
                                </td>
                                <td class="storage-error-text">{f.error.message}</td>
                                <td class="qt-time">{formatRelativeTime(f.failedAt)}</td>
                                <td>
                                  <TrayActions id={f.id} />
                                </td>
                              </Show>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </Show>
                </Show>

                <Show when={view() === 'debounced'}>
                  <Show
                    when={debounced().length > 0}
                    fallback={<div class="empty-state">No debounced update is waiting for its flush.</div>}
                  >
                    <table class="qt-table">
                      <thead>
                        <tr>
                          <th class="mt-col-record">Record</th>
                          <th>Fields</th>
                          <th class="mt-col-time">Since</th>
                          <th class="mt-col-time" title="Mirrored to _00_pending_writes: survives a reload">
                            Survives reload
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={debounced()}>
                          {(d) => (
                            <tr>
                              <td class="mono">{d.recordId}</td>
                              <td class="mono">{d.fields.join(', ')}</td>
                              <td class="qt-time">{formatRelativeTime(d.since)}</td>
                              <td>{d.durable ? 'yes' : 'no (flushOnHide: false)'}</td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </Show>
                </Show>

                <Show when={view() !== 'tray' && view() !== 'debounced'}>
                  <Show
                    when={entries().length > 0}
                    fallback={
                      <div class="empty-state">
                        {term()
                          ? 'No mutations match the filter'
                          : 'No writes since DevTools attached. Queued ones show up here the moment they are made.'}
                      </div>
                    }
                  >
                    <table class="qt-table">
                      <thead>
                        <tr>
                          <th class="mt-col-record">Record</th>
                          <Show when={!collapsed()}>
                            <th class="mt-col-op">Op</th>
                            <th>Fields</th>
                            <th class="mt-col-status">Status</th>
                            <th class="qt-num mt-col-num">Tries</th>
                            <th class="qt-num mt-col-num">Took</th>
                            <th class="mt-col-time">Queued</th>
                          </Show>
                        </tr>
                      </thead>
                      <tbody>
                        <For each={entries()}>
                          {(e) => (
                            <tr
                              classList={{ selected: selection()?.id === e.id }}
                              onClick={() => select({ kind: 'entry', id: e.id })}
                              title={e.error ?? shortId(e.id)}
                            >
                              <td class="mono">{e.recordId}</td>
                              <Show when={!collapsed()}>
                                <td>
                                  <OpBadge op={e.op} />
                                </td>
                                <td class="mono muted">{e.fields?.join(', ') ?? ''}</td>
                                <td>
                                  <StatusPill status={e.status} />
                                </td>
                                <td class="qt-num">{e.attempts || ''}</td>
                                <td class="qt-num">{e.settledAt ? formatMs(e.settledAt - e.queuedAt) : ''}</td>
                                <td class="qt-time">{e.queuedAt ? formatTime(e.queuedAt) : '—'}</td>
                              </Show>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </Show>
                </Show>
              </div>

              <Show when={selection()}>
                {(sel) => <MutationDetail selection={sel()} onClose={() => setSelection(null)} />}
              </Show>
            </div>

            <div class="queries-statusbar">
              <span classList={{ 'mt-sync-warn': syncLine(mut()).warn }}>{syncLine(mut()).text}</span>
              <span class="statusbar-sep" />
              <span>{mut().counts.pending + mut().counts.retrying} queued</span>
              <Show when={mut().counts.retrying}>
                <span class="statusbar-sep" />
                <span>{mut().counts.retrying} retrying</span>
              </Show>
              <span class="statusbar-sep" />
              <span classList={{ 'mt-sync-warn': mut().counts.failed > 0 }}>{mut().counts.failed} in failed tray</span>
              <Show when={mut().total > mut().entries.length}>
                <span class="statusbar-sep" />
                <span>
                  showing newest {mut().entries.length} of {mut().total}
                </span>
              </Show>
            </div>
          </>
        )}
      </Show>
    </div>
  );
}
