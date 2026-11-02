import { For, Show, createResource, createSignal, type JSX } from 'solid-js';
import { api } from '../api/client';
import type { MeResponse } from '../api/types';
import { formatBytes, formatStamp } from '../lib/format';
import { baseKind, editorFor, isRedacted, isTrimmed, looksLikeStamp, recordTable, type FieldInfo } from '../lib/data';

/** Pieces the Database, Users and Storage pages share. */

export const enc = encodeURIComponent;

/**
 * Whether this session may change anything: read-only tokens browse only. No until
 * proven yes, so a slow or failed `/me` shows no write controls that would be refused.
 */
export function useCanWrite(): () => boolean {
  const [me] = createResource(() => api.getResult<MeResponse>('/me'));
  return () => {
    const m = me();
    return !!m && m.ok && (m.value.scope ?? 'full') === 'full';
  };
}

/** A value in a grid: its type at a glance, a record as a way to it. */
export function ValueCell(props: {
  value: unknown;
  kind?: string;
  tables?: ReadonlySet<string>;
  onRecord?: (id: string) => void;
}) {
  const v = () => props.value;
  const link = () => {
    if (!props.tables || !props.onRecord) return null;
    const base = baseKind(props.kind);
    return base === 'any' || base.startsWith('record') ? recordTable(v(), props.tables) : null;
  };
  return (
    <Show when={v() !== null && v() !== undefined} fallback={<span class="dx-none">NONE</span>}>
      <Show when={!isRedacted(v())} fallback={<span class="dx-secret" title="Credentials are never shown here">••••••</span>}>
        <Show when={!isTrimmed(v())} fallback={<span class="dx-json" title={`${formatBytes((v() as { bytes: number }).bytes)}, cut for this list`}>{(v() as { preview: string }).preview}…</span>}>
          <Show when={!link()} fallback={<button class="fx-link dx-rec" onClick={(e) => { e.stopPropagation(); props.onRecord!(v() as string); }}>{v() as string}</button>}>
            <Show when={typeof v() === 'object'} fallback={<Scalar value={v()} kind={props.kind} />}>
              <span class="dx-json">{JSON.stringify(v())}</span>
            </Show>
          </Show>
        </Show>
      </Show>
    </Show>
  );
}

function Scalar(props: { value: unknown; kind?: string }) {
  return (
    <Show when={!looksLikeStamp(props.value)} fallback={<span title={props.value as string}>{formatStamp(props.value as string)}</span>}>
      <span classList={{ 'dx-num': typeof props.value === 'number', 'dx-bool': typeof props.value === 'boolean' }}>{String(props.value)}</span>
    </Show>
  );
}

/** Where a page of rows is, and the way to the next. */
export function Pager(props: {
  start: number;
  count: number;
  hasMore: boolean;
  limit: number;
  loading?: boolean;
  onStart: (start: number) => void;
  onLimit?: (limit: number) => void;
}) {
  return (
    <div class="dx-pager">
      <span class="cap" classList={{ 'dx-busy': props.loading }}>
        {props.count ? `${props.start + 1}–${props.start + props.count}` : '0 rows'}
      </span>
      <div class="fx-range">
        <button disabled={props.start === 0} aria-label="Previous page" onClick={() => props.onStart(Math.max(0, props.start - props.limit))}>‹</button>
        <button disabled={!props.hasMore} aria-label="Next page" onClick={() => props.onStart(props.start + props.limit)}>›</button>
      </div>
      <Show when={props.onLimit}>
        <select class="dx-select" aria-label="Rows per page" value={props.limit} onChange={(e) => props.onLimit!(Number(e.currentTarget.value))}>
          <For each={[25, 50, 100, 500]}>{(n) => <option value={n}>{n} rows</option>}</For>
        </select>
      </Show>
    </div>
  );
}

/** One field of a form, typed by the table's definition. */
export function FieldInput(props: {
  field?: FieldInfo;
  name: string;
  value: unknown;
  draft: string;
  disabled?: boolean;
  onInput: (text: string) => void;
}) {
  const editor = () => editorFor(props.field?.kind, props.value);
  // By lines, and by how far a long line wraps at this sheet's width.
  const rows = () =>
    Math.min(14, Math.max(editor() === 'json' ? 3 : 1, props.draft.split('\n').reduce((n, line) => n + Math.max(1, Math.ceil(line.length / 88)), 0)));
  return (
    <Show
      when={editor() !== 'bool'}
      fallback={
        <select class="dx-input" disabled={props.disabled} value={props.draft} onChange={(e) => props.onInput(e.currentTarget.value)}>
          <Show when={props.draft === ''}><option value="">Choose</option></Show>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      }
    >
      <textarea
        class="dx-input"
        classList={{ mono: editor() === 'json' }}
        rows={rows()}
        spellcheck={false}
        disabled={props.disabled}
        value={props.draft}
        placeholder={placeholderFor(props.field?.kind)}
        onInput={(e) => props.onInput(e.currentTarget.value)}
      />
    </Show>
  );
}

function placeholderFor(kind: string | undefined): string {
  const base = baseKind(kind);
  if (base === 'datetime') return '2026-01-31T12:00:00Z';
  if (base === 'duration') return '1h30m';
  if (base.startsWith('record')) return `${/record<(\w+)/.exec(base)?.[1] ?? 'table'}:id`;
  return '';
}

/** A button that copies, and says so. */
export function CopyText(props: { value: string; label?: string; class?: string }) {
  const [done, setDone] = createSignal(false);
  return (
    <button
      class={props.class ?? 'btn btn-sm'}
      onClick={() => {
        void navigator.clipboard?.writeText(props.value);
        setDone(true);
        setTimeout(() => setDone(false), 1400);
      }}
    >
      {done() ? 'Copied' : (props.label ?? 'Copy')}
    </button>
  );
}

/** A load that failed, in the server's own words. */
export function DataError(props: { message: string | undefined; children?: JSX.Element }) {
  return (
    <Show when={props.message}>
      <div class="fx-log bad dx-err"><span class="fx-what">{props.message}</span>{props.children}</div>
    </Show>
  );
}

