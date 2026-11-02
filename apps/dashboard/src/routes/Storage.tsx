import { For, Show, createEffect, createMemo, createResource, createSignal, on, onCleanup } from 'solid-js';
import { A, useNavigate, useParams, useSearchParams } from '@solidjs/router';
import { api, ApiError, currentMode } from '../api/client';
import { confirm, toast } from '../components/Actions';
import { PageHead } from '../components/Chrome';
import { CopyText, DataError, enc, useCanWrite } from '../components/DataParts';
import { Sheet, SheetSection } from '../components/Frame';
import { formatBytes, formatStamp, relativeStamp } from '../lib/format';
import {
  base64ToBytes,
  crumbsOf,
  fileKindOf,
  normalizePrefix,
  previewMime,
  type BucketInfo,
  type BucketsAnswer,
  type FileContent,
  type FileEntry,
  type FilesPage,
} from '../lib/data';

/**
 * The files of the project's SurrealDB buckets. A bucket has keys, not folders;
 * the server reads a folder out of them (see `list_files`).
 *
 * Bytes come back as base64. Nothing is ever opened as a page: a blob URL keeps
 * this page's origin, so an uploaded HTML file rendered from one could read the
 * session. Images go in an `<img>` as a data URL (the console's CSP allows `data:`
 * for images and no `blob:`), text in a `<pre>`, and a download is always
 * `application/octet-stream`.
 */

/** A preview loads by itself up to this size; past it, on request. */
const AUTO_PREVIEW_BYTES = 4 * 1024 * 1024;
const TEXT_PREVIEW_BYTES = 256 * 1024;

/** Folders opened by hand, for a backend that cannot list them. */
function rememberedKey(bucket: string): string {
  return `spky.storage.paths:${currentMode().config?.project_slug ?? ''}:${bucket}`;
}
function readRemembered(bucket: string): string[] {
  try {
    return JSON.parse(localStorage.getItem(rememberedKey(bucket)) ?? '[]') as string[];
  } catch {
    return [];
  }
}
function remember(bucket: string, prefix: string) {
  try {
    const next = [prefix, ...readRemembered(bucket).filter((p) => p !== prefix)].slice(0, 12);
    localStorage.setItem(rememberedKey(bucket), JSON.stringify(next));
  } catch {
    /* nothing lost but a shortcut */
  }
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error ?? new Error('Could not read the file'));
    reader.onload = () => {
      const text = reader.result as string;
      resolve(text.slice(text.indexOf(',') + 1));
    };
    reader.readAsDataURL(file);
  });
}

export function Storage() {
  const params = useParams<{ bucket?: string }>();
  const navigate = useNavigate();
  const canWrite = useCanWrite();
  const [list, { refetch }] = createResource(() => api.getResult<BucketsAnswer>('/data/buckets'));
  const answer = () => {
    const r = list();
    return r?.ok ? r.value : undefined;
  };
  const buckets = () => answer()?.buckets ?? [];
  const current = () => buckets().find((b) => b.name === params.bucket);

  createEffect(() => {
    if (params.bucket || !answer()) return;
    const first = buckets().find((b) => b.browsable) ?? buckets()[0];
    if (first) navigate(`/storage/${enc(first.name)}`, { replace: true });
  });

  return (
    <>
      <PageHead title="Storage" />
      <div class="dx-page dx-split">
        <aside class="dx-side">
          <div class="dx-side-find dx-side-title">
            <span class="cap">Buckets</span>
            <button class="dx-iconbtn" title="Reload the bucket list" aria-label="Reload the bucket list" onClick={() => void refetch()}>↻</button>
          </div>
          <select class="dx-select dx-side-pick" aria-label="Bucket" value={params.bucket ?? ''} onChange={(e) => navigate(`/storage/${enc(e.currentTarget.value)}`)}>
            <For each={buckets()}>{(b) => <option value={b.name} selected={b.name === params.bucket}>{b.name}</option>}</For>
          </select>
          <div class="dx-side-list">
            <For each={buckets()} fallback={<Show when={list()}><div class="dx-side-empty cap">No buckets</div></Show>}>
              {(b) => (
                <A class="dx-tlink" activeClass="on" href={`/storage/${enc(b.name)}`}>
                  <span class="dx-tname">{b.name}</span>
                  <span class="cap">{b.backend}</span>
                </A>
              )}
            </For>
          </div>
        </aside>
        <div class="dx-main">
          <Show when={list() && !list()!.ok}>
            <div class="dx-pad"><DataError message={(list() as { ok: false; message: string }).message} /></div>
          </Show>
          <Show when={answer() && !buckets().length}>
            <div class="dx-empty">
              <p class="cap">This project has no buckets</p>
              <p class="dx-note cap">Add one with `spky bucket create`.</p>
            </div>
          </Show>
          <Show when={current()} keyed>
            {(b) => <BucketView bucket={b} limits={answer()!} canWrite={canWrite()} />}
          </Show>
        </div>
      </div>
    </>
  );
}

interface Upload {
  name: string;
  size: number;
  state: 'waiting' | 'uploading' | 'done' | 'failed';
  error?: string;
}

function BucketView(props: { bucket: BucketInfo; limits: BucketsAnswer; canWrite: boolean }) {
  const [query, setQuery] = useSearchParams<{ path?: string; file?: string }>();
  const url = `/data/buckets/${enc(props.bucket.name)}`;
  const prefix = () => normalizePrefix(query.path ?? '');
  const writable = () => props.canWrite && props.bucket.browsable && !props.bucket.readonly;

  const [cursor, setCursor] = createSignal<string | null>(null);
  const [folders, setFolders] = createSignal<string[]>([]);
  const [files, setFiles] = createSignal<FileEntry[]>([]);
  const [next, setNext] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string>();
  const [loading, setLoading] = createSignal(false);
  const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set<string>());
  const [filter, setFilter] = createSignal('');
  const [goTo, setGoTo] = createSignal('');
  const [uploads, setUploads] = createSignal<Upload[]>([]);
  const [dragging, setDragging] = createSignal(false);
  const [remembered, setRemembered] = createSignal(readRemembered(props.bucket.name));
  let fileInput: HTMLInputElement | undefined;

  // Only the newest request answers: a slow folder must not land over the one
  // opened after it.
  let latest = 0;
  const load = async (more: boolean) => {
    if (!props.bucket.browsable) return;
    const mine = ++latest;
    setLoading(true);
    const p = new URLSearchParams();
    if (prefix()) p.set('prefix', prefix());
    if (more && cursor()) p.set('start', cursor()!);
    const r = await api.getResult<FilesPage>(`${url}/files?${p.toString()}`);
    if (mine !== latest) return;
    setLoading(false);
    if (!r.ok) {
      setError(r.message);
      return;
    }
    setError(undefined);
    setFolders(more ? [...new Set([...folders(), ...r.value.folders])] : r.value.folders);
    setFiles(more ? [...files(), ...r.value.files] : r.value.files);
    setNext(r.value.next);
    setCursor(r.value.next);
    if (!more) setSelected(new Set<string>());
    // A folder found by its path is worth a shortcut on a backend that hides it.
    if (!props.bucket.lists_folders && prefix() && (r.value.files.length || r.value.folders.length)) {
      remember(props.bucket.name, prefix());
      setRemembered(readRemembered(props.bucket.name));
    }
  };
  createEffect(() => {
    prefix();
    void load(false);
  });

  const open = (path: string) => setQuery({ path: path || undefined, file: undefined });
  const openTyped = (e: Event) => {
    e.preventDefault();
    open(normalizePrefix(goTo()));
    setGoTo('');
  };

  const needle = () => filter().trim().toLowerCase();
  const shownFolders = createMemo(() => folders().filter((f) => f.toLowerCase().includes(needle())));
  const shownFiles = createMemo(() => files().filter((f) => f.name.toLowerCase().includes(needle())));
  const empty = () => !loading() && !error() && !folders().length && !files().length;

  const allShown = () => shownFiles().length > 0 && shownFiles().every((f) => selected().has(f.key));
  const toggleAll = () => setSelected(allShown() ? new Set<string>() : new Set(shownFiles().map((f) => f.key)));
  const toggle = (key: string) => {
    const set = new Set(selected());
    if (set.has(key)) set.delete(key);
    else set.add(key);
    setSelected(set);
  };

  const removeSelected = async () => {
    const keys = [...selected()];
    const ok = await confirm({
      title: `Delete ${keys.length} ${keys.length === 1 ? 'file' : 'files'} from ${props.bucket.name}?`,
      consequences: ['This cannot be undone, short of restoring a backup that holds them.', 'Rows that point at these files keep pointing at nothing.'],
      verb: 'Delete',
      typeToConfirm: keys.length > 1 ? 'delete' : undefined,
    });
    if (!ok) return;
    try {
      await api.post(`${url}/delete`, { keys });
      toast('ok', `${keys.length} deleted`);
      void load(false);
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  const patchUpload = (name: string, patch: Partial<Upload>) =>
    setUploads((list) => list.map((u) => (u.name === name ? { ...u, ...patch } : u)));

  /** One at a time: each holds the whole file in memory on the way through. */
  const upload = async (list: File[]) => {
    if (!list.length) return;
    setUploads(list.map((f) => ({ name: f.name, size: f.size, state: 'waiting' })));
    for (const file of list) {
      patchUpload(file.name, { state: 'uploading' });
      const key = `${prefix()}${file.name}`;
      try {
        const data = await fileToBase64(file);
        const put = (overwrite: boolean) =>
          api.put(`${url}/file?key=${enc(key)}${overwrite ? '&overwrite=1' : ''}`, { data });
        try {
          await put(false);
        } catch (err) {
          if (!(err instanceof ApiError) || err.status !== 409) throw err;
          const replace = await confirm({
            title: `Replace ${file.name}?`,
            consequences: [`${key} already exists in ${props.bucket.name}. The file there is lost.`],
            verb: 'Replace',
          });
          if (!replace) {
            patchUpload(file.name, { state: 'failed', error: 'Kept the existing file' });
            continue;
          }
          await put(true);
        }
        patchUpload(file.name, { state: 'done' });
      } catch (err) {
        patchUpload(file.name, { state: 'failed', error: err instanceof Error ? err.message : 'Upload failed' });
      }
    }
    void load(false);
  };
  const onDrop = (e: DragEvent) => {
    e.preventDefault();
    setDragging(false);
    if (writable() && e.dataTransfer?.files.length) void upload([...e.dataTransfer.files]);
  };
  const uploadsDone = () => uploads().length > 0 && uploads().every((u) => u.state === 'done' || u.state === 'failed');

  const opened = () => query.file;

  return (
    <div
      class="dx-table dx-bucket"
      classList={{ dropping: dragging() }}
      onDragOver={(e) => {
        if (!writable()) return;
        e.preventDefault();
        setDragging(true);
      }}
      onDragLeave={(e) => {
        if (e.currentTarget === e.target) setDragging(false);
      }}
      onDrop={onDrop}
    >
      <div class="dx-bar">
        <div class="dx-bar-title">
          <b>{props.bucket.name}</b>
          <span class="cap">
            {props.bucket.backend}
            <Show when={props.bucket.max_bytes}> · app limit {formatBytes(props.bucket.max_bytes)}</Show>
            <Show when={props.bucket.readonly}> · read-only</Show>
          </span>
        </div>
        <Show when={props.bucket.browsable}>
          <div class="dx-bar-end">
            <Show when={selected().size}>
              <button class="btn btn-sm dx-danger" onClick={() => void removeSelected()}>Delete {selected().size}</button>
            </Show>
            <input class="dx-find" type="search" placeholder="Filter this folder" aria-label="Filter this folder" value={filter()} onInput={(e) => setFilter(e.currentTarget.value)} />
            <Show when={writable()}>
              <input
                ref={fileInput}
                type="file"
                multiple
                hidden
                onChange={(e) => {
                  void upload([...(e.currentTarget.files ?? [])]);
                  e.currentTarget.value = '';
                }}
              />
              <button class="btn btn-sm btn-primary" title={`Up to about ${formatBytes(props.limits.default_upload_bytes)} a file, unless the database's request limit was raised`} onClick={() => fileInput?.click()}>Upload</button>
            </Show>
            <button class="dx-iconbtn" title="Reload" aria-label="Reload" onClick={() => void load(false)}>↻</button>
          </div>
        </Show>
      </div>

      <Show when={!props.bucket.browsable}>
        <div class="dx-pad">
          <div class="dx-hint">
            <b>This bucket's files are not in the database.</b>
            <span>It is declared `BACKEND "r2"`: the SSP keeps its files in R2 (on Cloudflare) or a directory, and the database only checks permissions. Browsing that store is not available here yet.</span>
          </div>
        </div>
      </Show>

      <Show when={props.bucket.browsable}>
        <div class="dx-crumbs">
          <button class="fx-link" classList={{ 'dx-here': !prefix() }} onClick={() => open('')}>{props.bucket.name}</button>
          <For each={crumbsOf(prefix())}>
            {(c, i) => (
              <>
                <span class="dx-sep">/</span>
                <button class="fx-link" classList={{ 'dx-here': i() === crumbsOf(prefix()).length - 1 }} onClick={() => open(c.prefix)}>{c.name}</button>
              </>
            )}
          </For>
          <form class="dx-goto" onSubmit={openTyped}>
            <input placeholder="Open a folder by path" aria-label="Open a folder by path" value={goTo()} onInput={(e) => setGoTo(e.currentTarget.value)} />
          </form>
        </div>

        <Show when={!props.bucket.lists_folders}>
          <div class="dx-hint inline">
            <span><b>Subfolders don't show up here.</b> This bucket lives on disk, and SurrealDB lists one folder at a time without naming the folders inside it. Open one by its path.</span>
            <Show when={remembered().length}>
              <span class="dx-remembered">
                <For each={remembered()}>{(p) => <button class="dx-chip plain" onClick={() => open(p)}>{p}</button>}</For>
              </span>
            </Show>
          </div>
        </Show>

        <Show when={uploads().length}>
          <div class="dx-uploads">
            <div class="fx-head">
              <span class="cap">Uploads</span>
              <Show when={uploadsDone()}><button class="fx-link cap fx-meta" onClick={() => setUploads([])}>Dismiss</button></Show>
            </div>
            <For each={uploads()}>
              {(u) => (
                <div class="dx-upload" classList={{ bad: u.state === 'failed' }}>
                  <span class="dx-tname">{u.name}</span>
                  <span class="cap">{formatBytes(u.size)}</span>
                  <span class="cap dx-upload-state">{u.state === 'failed' ? u.error : u.state}</span>
                </div>
              )}
            </For>
          </div>
        </Show>

        <DataError message={error()} />

        <div class="dx-grid-wrap">
          <table class="dx-grid dx-files">
            <thead>
              <tr>
                <Show when={writable()}>
                  <th class="dx-check"><input type="checkbox" aria-label="Select all files" checked={allShown()} onChange={toggleAll} /></th>
                </Show>
                <th>Name</th>
                <th class="dx-r">Size</th>
                <th class="dx-col-mid">Modified</th>
              </tr>
            </thead>
            <tbody>
              <For each={shownFolders()}>
                {(f) => (
                  <tr class="dx-rowlink" onClick={() => open(`${prefix()}${f}/`)}>
                    <Show when={writable()}><td class="dx-check" /></Show>
                    <td class="dx-fname"><FileGlyph folder /><button class="dx-open">{f}/</button></td>
                    <td class="dx-r dx-dim">—</td>
                    <td class="dx-col-mid dx-dim">—</td>
                  </tr>
                )}
              </For>
              <For each={shownFiles()}>
                {(f) => (
                  <tr class="dx-rowlink" classList={{ on: selected().has(f.key) }} onClick={() => setQuery({ file: f.key })}>
                    <Show when={writable()}>
                      <td class="dx-check" onClick={(e) => e.stopPropagation()}>
                        <input type="checkbox" aria-label={`Select ${f.name}`} checked={selected().has(f.key)} onChange={() => toggle(f.key)} />
                      </td>
                    </Show>
                    <td class="dx-fname"><FileGlyph name={f.name} /><button class="dx-open">{f.name}</button></td>
                    <td class="dx-r">{formatBytes(f.size)}</td>
                    <td class="dx-col-mid" title={formatStamp(f.updated)}>{relativeStamp(f.updated)}</td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
          <Show when={empty()}>
            <div class="dx-empty">
              <p class="cap">{prefix() ? 'Nothing in this folder' : 'This bucket is empty'}</p>
              <Show when={writable()}><p class="dx-note cap">Drop files here, or use Upload.</p></Show>
            </div>
          </Show>
          <Show when={next()}>
            <div class="dx-more"><button class="btn btn-sm" disabled={loading()} onClick={() => void load(true)}>{loading() ? 'Loading…' : 'Load more'}</button></div>
          </Show>
        </div>
      </Show>

      <Show when={opened()} keyed>
        {(key) => (
          <FileSheet
            bucket={props.bucket}
            fileKey={key}
            maxRead={props.limits.max_read_bytes}
            writable={writable()}
            onClose={() => setQuery({ file: undefined })}
            onChanged={() => void load(false)}
          />
        )}
      </Show>
    </div>
  );
}

function FileGlyph(props: { folder?: boolean; name?: string }) {
  const kind = () => (props.folder ? 'folder' : fileKindOf(props.name ?? ''));
  const paths: Record<string, string> = {
    folder: 'M3 6h6l2 2h10v11H3z',
    image: 'M4 4h16v16H4z M4 16l5-5 4 4 3-3 4 4 M15 9h.01',
    video: 'M4 5h16v14H4z M10 9l5 3-5 3z',
    audio: 'M9 18V6l10-2v12 M9 18a3 3 0 1 1-6 0 3 3 0 0 1 6 0z M19 16a3 3 0 1 1-6 0 3 3 0 0 1 6 0z',
    text: 'M6 3h9l4 4v14H6z M9 11h7 M9 15h7',
    other: 'M6 3h9l4 4v14H6z',
  };
  return (
    <svg class="dx-glyph" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d={paths[kind()]} />
    </svg>
  );
}

/* ------------------------------------------------------------------ */
/* One file                                                             */
/* ------------------------------------------------------------------ */

function FileSheet(props: { bucket: BucketInfo; fileKey: string; maxRead: number; writable: boolean; onClose: () => void; onChanged: () => void }) {
  const url = `/data/buckets/${enc(props.bucket.name)}`;
  const name = () => props.fileKey.slice(props.fileKey.lastIndexOf('/') + 1);
  const kind = () => fileKindOf(props.fileKey);
  const reference = () => `f"${props.bucket.name}:/${props.fileKey}"`;

  const [meta] = createResource(() => api.getResult<FileContent>(`${url}/file?key=${enc(props.fileKey)}&meta=1`));
  const size = () => {
    const m = meta();
    return m?.ok ? m.value.size : undefined;
  };

  const [content, setContent] = createSignal<FileContent | null>(null);
  const [mediaUrl, setMediaUrl] = createSignal<string | null>(null);
  const [mediaBlocked, setMediaBlocked] = createSignal(false);
  const [text, setText] = createSignal<string | null>(null);
  const [cut, setCut] = createSignal(false);
  const [loading, setLoading] = createSignal(false);
  const [error, setError] = createSignal<string>();
  onCleanup(() => {
    const u = mediaUrl();
    if (u?.startsWith('blob:')) URL.revokeObjectURL(u);
  });

  const fetchContent = async (): Promise<FileContent | null> => {
    if (content()) return content();
    setLoading(true);
    setError(undefined);
    const r = await api.getResult<FileContent>(`${url}/file?key=${enc(props.fileKey)}`);
    setLoading(false);
    if (!r.ok) {
      setError(r.message);
      return null;
    }
    setContent(r.value);
    return r.value;
  };

  const preview = async () => {
    const c = await fetchContent();
    if (!c?.data) return;
    const bytes = base64ToBytes(c.data);
    if (kind() === 'text') {
      const head = bytes.subarray(0, TEXT_PREVIEW_BYTES);
      setCut(bytes.length > head.length);
      const decoded = new TextDecoder().decode(head);
      if (props.fileKey.endsWith('.json') && bytes.length <= TEXT_PREVIEW_BYTES) {
        try {
          return void setText(JSON.stringify(JSON.parse(decoded), null, 2));
        } catch {
          /* not JSON after all: show it as it is */
        }
      }
      setText(decoded);
    } else if (kind() === 'image') {
      setMediaUrl(`data:${previewMime(props.fileKey)};base64,${c.data}`);
    } else {
      setMediaUrl(URL.createObjectURL(new Blob([bytes], { type: previewMime(props.fileKey) })));
    }
  };

  const previewable = () => kind() !== 'other';
  const tooBig = () => (size() ?? 0) > props.maxRead;
  // Start by itself for what is quick to bring across.
  createEffect(on(size, (s) => {
    if (s !== undefined && previewable() && s <= AUTO_PREVIEW_BYTES) void preview();
  }));

  const download = async () => {
    const c = await fetchContent();
    if (!c?.data) return;
    const blobLink = URL.createObjectURL(new Blob([base64ToBytes(c.data)], { type: 'application/octet-stream' }));
    const a = document.createElement('a');
    a.href = blobLink;
    a.download = name();
    a.click();
    setTimeout(() => URL.revokeObjectURL(blobLink), 2000);
  };

  const [renaming, setRenaming] = createSignal(false);
  const [target, setTarget] = createSignal(props.fileKey);
  const [, setQuery] = useSearchParams<{ file?: string }>();

  const rename = async (e: Event) => {
    e.preventDefault();
    const to = target().trim().replace(/^\/+/, '');
    if (!to || to === props.fileKey) return void setRenaming(false);
    try {
      await api.post(`${url}/rename`, { key: props.fileKey, target: to });
      toast('ok', 'Renamed', to);
      props.onChanged();
      setQuery({ file: to });
    } catch (err) {
      toast('bad', 'Could not rename', err instanceof Error ? err.message : undefined);
    }
  };

  const remove = async () => {
    const ok = await confirm({
      title: `Delete ${name()}?`,
      consequences: ['This cannot be undone, short of restoring a backup that holds it.', 'Rows that point at this file keep pointing at nothing.'],
      verb: 'Delete',
    });
    if (!ok) return;
    try {
      await api.delete(`${url}/file?key=${enc(props.fileKey)}`);
      toast('ok', `${name()} deleted`);
      props.onChanged();
      props.onClose();
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  return (
    <Sheet open wide title={<span class="dx-sheet-id">{name()}</span>} meta={<Show when={size() !== undefined}>{formatBytes(size())}</Show>} onClose={props.onClose}>
      <Show when={meta() && !meta()!.ok}>
        <section class="sheet-sec"><DataError message={(meta() as { ok: false; message: string }).message} /></section>
      </Show>
      <Show when={meta()?.ok}>
        <section class="sheet-sec dx-actions">
          <button class="btn btn-sm btn-primary" disabled={loading() || tooBig()} title={tooBig() ? `Larger than the ${formatBytes(props.maxRead)} this API reads` : undefined} onClick={() => void download()}>
            {loading() ? 'Reading…' : 'Download'}
          </button>
          <CopyText value={reference()} label="Copy reference" />
          <Show when={props.writable}>
            <button class="btn btn-sm" onClick={() => setRenaming(!renaming())}>Rename</button>
            <button class="btn btn-sm dx-danger" onClick={() => void remove()}>Delete</button>
          </Show>
        </section>
        <Show when={renaming()}>
          <section class="sheet-sec">
            <form class="dx-pass" onSubmit={rename}>
              <input class="dx-input mono" aria-label="New key" value={target()} onInput={(e) => setTarget(e.currentTarget.value)} />
              <button class="btn btn-sm btn-primary" type="submit">Rename</button>
            </form>
          </section>
        </Show>
        <DataError message={error()} />
        <SheetSection label="Preview">
          <Show when={mediaUrl() || text() !== null} fallback={
            <div class="dx-empty small">
              <Show when={previewable()} fallback={<p class="cap">No preview for this type of file</p>}>
                <Show when={tooBig()} fallback={
                  <Show when={(size() ?? 0) > AUTO_PREVIEW_BYTES} fallback={<p class="cap">{loading() ? 'Reading…' : ''}</p>}>
                    <button class="btn btn-sm" disabled={loading()} onClick={() => void preview()}>{loading() ? 'Reading…' : `Load preview (${formatBytes(size())})`}</button>
                  </Show>
                }>
                  <p class="cap">Too large to read through this API</p>
                </Show>
              </Show>
            </div>
          }>
            <Show when={kind() === 'image'}><img class="dx-preview" src={mediaUrl() ?? undefined} alt={name()} onError={() => setMediaBlocked(true)} /></Show>
            <Show when={kind() === 'video' && !mediaBlocked()}><video class="dx-preview" src={mediaUrl() ?? undefined} controls onError={() => setMediaBlocked(true)} /></Show>
            <Show when={kind() === 'audio' && !mediaBlocked()}><audio class="dx-preview audio" src={mediaUrl() ?? undefined} controls onError={() => setMediaBlocked(true)} /></Show>
            <Show when={mediaBlocked()}><p class="cap dx-note">This browser cannot show it here. Download it to open it.</p></Show>
            <Show when={text() !== null}>
              <pre class="vw-pre dim dx-pre">{text()}</pre>
              <Show when={cut()}><p class="cap dx-note">Showing the first {formatBytes(TEXT_PREVIEW_BYTES)}.</p></Show>
            </Show>
          </Show>
        </SheetSection>
        <SheetSection label="Details">
          <div class="dx-kv">
            <div><span class="cap">Key</span><div class="dx-kv-v">/{props.fileKey}</div></div>
            <div><span class="cap">Size</span><div class="dx-kv-v">{formatBytes(size())}</div></div>
            <div><span class="cap">Modified</span><div class="dx-kv-v">{formatStamp(meta()?.ok ? (meta() as { ok: true; value: FileContent }).value.updated : undefined)}</div></div>
            <div><span class="cap">Reference</span><div class="dx-kv-v">{reference()}</div></div>
          </div>
        </SheetSection>
      </Show>
    </Sheet>
  );
}
