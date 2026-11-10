import { For, Show, createEffect, createMemo, createResource, createSignal, on, onCleanup, onMount, untrack } from 'solid-js';
import { A, useNavigate, useParams, useSearchParams } from '@solidjs/router';
import { api, ApiError, currentMode } from '../api/client';
import { confirm, toast } from '../components/Actions';
import { PageHead } from '../components/Chrome';
import { CopyText, DataError, enc, useCanWrite } from '../components/DataParts';
import { formatBytes, formatStamp, relativeStamp } from '../lib/format';
import {
  base64ToBytes,
  childOn,
  columnPrefixes,
  countLabel,
  crumbsOf,
  extensionOf,
  fileKindOf,
  folderSummary,
  normalizePrefix,
  parentOf,
  previewMime,
  type BucketInfo,
  type BucketsAnswer,
  type FileContent,
  type FileEntry,
  type FilesPage,
} from '../lib/data';

/**
 * The files of the project's SurrealDB buckets, as columns: one per folder on the
 * way to where you are, with what you opened in the last. A bucket has keys, not
 * folders; the server reads a folder out of them (see `list_files`), or off the
 * disk on a `file:` bucket whose volume it can read.
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

/** The widths in styles/frame.css, which the columns are fitted to. */
const COLUMN_WIDTH = 248;
const PANE_MIN_WIDTH = 340;

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
      <div class="dx-page dx-split st-page">
        <aside class="dx-side">
          <div class="dx-side-find dx-side-title">
            <span class="cap">Buckets</span>
            <button class="dx-iconbtn" title="Reload the bucket list" aria-label="Reload the bucket list" onClick={() => void refetch()}>↻</button>
          </div>
          <select class="dx-select dx-side-pick" aria-label="Bucket" value={params.bucket ?? ''} onChange={(e) => navigate(`/storage/${enc(e.currentTarget.value)}`)}>
            <For each={buckets()}>{(b) => <option value={b.name} selected={b.name === params.bucket}>{b.name}</option>}</For>
          </select>
          <div class="dx-side-list">
            <For each={buckets()} fallback={<Show when={list()}><div class="dx-side-empty">No buckets</div></Show>}>
              {(b) => (
                <A class="dx-tlink st-blink" activeClass="on" href={`/storage/${enc(b.name)}`}>
                  <span class="dx-tname">{b.name}</span>
                  <span class="st-backend">{b.backend}</span>
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
            <div class="st-note">
              <h2>No buckets yet</h2>
              <p>Add one with <code>spky bucket create</code>, then deploy.</p>
            </div>
          </Show>
          {/* By name: reloading the list must not drop running uploads or the selection. */}
          <Show when={current()?.name} keyed>
            {(_name) => <BucketView bucket={current()!} limits={answer()!} canWrite={canWrite()} />}
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

interface Summary {
  folders: number;
  files: number;
  bytes: number;
  more: boolean;
}

function BucketView(props: { bucket: BucketInfo; limits: BucketsAnswer; canWrite: boolean }) {
  const [query, setQuery] = useSearchParams<{ path?: string; file?: string }>();
  const url = `/data/buckets/${enc(props.bucket.name)}`;
  const fileKey = () => query.file;
  // An open file decides the folder, whatever else the address says.
  const path = () => (fileKey() ? parentOf(fileKey()!) : normalizePrefix(query.path ?? ''));
  const prefixes = createMemo(() => columnPrefixes(path()));
  const writable = () => props.canWrite && props.bucket.browsable && !props.bucket.readonly;

  /** Bumped by anything that changes files, so every open column reads again. */
  const [version, setVersion] = createSignal(0);
  const refresh = () => setVersion((v) => v + 1);
  const [summaries, setSummaries] = createSignal<Record<string, Summary>>({});
  const [selected, setSelected] = createSignal<ReadonlySet<string>>(new Set<string>());
  const [filter, setFilter] = createSignal('');
  const [goTo, setGoTo] = createSignal('');
  const [uploads, setUploads] = createSignal<Upload[]>([]);
  const [dragging, setDragging] = createSignal(false);
  const [remembered, setRemembered] = createSignal(readRemembered(props.bucket.name));
  const [room, setRoom] = createSignal(0);
  let fileInput: HTMLInputElement | undefined;
  let scroller: HTMLDivElement | undefined;

  onMount(() => {
    if (!scroller) return;
    // Measured now as well: the first scroll below has to see the pane's real width.
    setRoom(scroller.clientWidth);
    const watch = new ResizeObserver(() => setRoom(scroller?.clientWidth ?? 0));
    watch.observe(scroller);
    onCleanup(() => watch.disconnect());
  });
  /**
   * How wide the last column is, so that scrolling to the end stops on a column's
   * edge: as many whole columns as fit beside it, and it takes the rest. Anything
   * else leaves the first one on screen cut off through its names.
   */
  const paneWidth = () => {
    const fits = Math.max(1, Math.floor((room() - PANE_MIN_WIDTH) / COLUMN_WIDTH));
    return `${Math.max(PANE_MIN_WIDTH, room() - fits * COLUMN_WIDTH)}px`;
  };

  const openFolder = (prefix: string) => {
    setFilter('');
    setQuery({ path: prefix || undefined, file: undefined });
  };
  const openFile = (key: string) => setQuery({ path: undefined, file: key });
  const openTyped = (e: Event) => {
    e.preventDefault();
    openFolder(normalizePrefix(goTo().trim()));
    setGoTo('');
  };

  // The newest column is the one being looked at. A phone shows one pane at a
  // time, so there it stops on the folder's contents, with its details a swipe away.
  const showNewest = (behavior: ScrollBehavior) => {
    if (!scroller) return;
    const columns = scroller.querySelectorAll<HTMLElement>('.st-col');
    const contents = !fileKey() && window.matchMedia?.('(max-width: 860px)').matches ? columns[columns.length - 1] : undefined;
    scroller.scrollTo({ left: contents ? contents.offsetLeft : scroller.scrollWidth, behavior });
  };
  createEffect(
    on(
      () => [prefixes().length, fileKey()],
      (_, before) => {
        // Opening a page lands on its place at once; only walking there slides.
        const slide = before && !window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
        queueMicrotask(() => showNewest(slide ? 'smooth' : 'auto'));
      },
    ),
  );
  createEffect(on(room, () => showNewest('auto'), { defer: true }));

  // A selection belongs to the folder it was made in.
  createEffect(on(path, () => setSelected(new Set<string>()), { defer: true }));

  const toggle = (key: string) => {
    const set = new Set(selected());
    if (set.has(key)) set.delete(key);
    else set.add(key);
    setSelected(set);
  };

  /** A folder found by its path is worth a shortcut where nothing else can list it. */
  const noteFolder = (prefix: string, found: boolean) => {
    if (props.bucket.lists_folders || !prefix || !found) return;
    remember(props.bucket.name, prefix);
    setRemembered(readRemembered(props.bucket.name));
  };

  const summarize = (prefix: string, s: Summary) => setSummaries((all) => ({ ...all, [prefix]: s }));

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
      if (fileKey() && keys.includes(fileKey()!)) setQuery({ path: path() || undefined, file: undefined });
      setSelected(new Set<string>());
      refresh();
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  const patchUpload = (name: string, patch: Partial<Upload>) =>
    setUploads((list) => list.map((u) => (u.name === name ? { ...u, ...patch } : u)));

  /** One at a time: each holds the whole file in memory on the way through. */
  const upload = async (list: File[]) => {
    if (!list.length) return;
    const target = path();
    setUploads(list.map((f) => ({ name: f.name, size: f.size, state: 'waiting' })));
    for (const file of list) {
      patchUpload(file.name, { state: 'uploading' });
      const key = `${target}${file.name}`;
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
    refresh();
  };
  const onDrop = (e: DragEvent) => {
    e.preventDefault();
    setDragging(false);
    if (writable() && e.dataTransfer?.files.length) void upload([...e.dataTransfer.files]);
  };
  const uploadsDone = () => uploads().length > 0 && uploads().every((u) => u.state === 'done' || u.state === 'failed');

  /** Arrows walk the columns: up and down within one, right into a folder, left back out. */
  const onKey = (e: KeyboardEvent) => {
    const row = (e.target as HTMLElement).closest<HTMLElement>('.st-row');
    if (!row) return;
    const column = row.closest<HTMLElement>('.st-col');
    if (!column) return;
    const rows = [...column.querySelectorAll<HTMLElement>('.st-row')];
    const at = rows.indexOf(row);
    const step = (to: HTMLElement | undefined) => {
      e.preventDefault();
      to?.focus();
    };
    if (e.key === 'ArrowDown') return step(rows[at + 1]);
    if (e.key === 'ArrowUp') return step(rows[at - 1]);
    if (e.key === 'ArrowRight' && row.dataset.folder) {
      e.preventDefault();
      row.click();
      // The next column is there on the next tick, its rows once it has read.
      setTimeout(() => (column.nextElementSibling?.querySelector('.st-row') as HTMLElement | null)?.focus(), 250);
    }
    if (e.key === 'ArrowLeft') {
      const before = column.previousElementSibling as HTMLElement | null;
      if (!before) return;
      e.preventDefault();
      (before.querySelector('.st-row.trail, .st-row.now') as HTMLElement | null)?.focus();
    }
  };

  // Escape closes what is open, back to the folder it is in.
  const onEscape = (e: KeyboardEvent) => {
    if (e.key === 'Escape' && fileKey() && !document.querySelector('dialog[open]')) setQuery({ path: path() || undefined, file: undefined });
  };
  document.addEventListener('keydown', onEscape);
  onCleanup(() => document.removeEventListener('keydown', onEscape));

  const reference = (prefix: string) => `/${prefix}`;

  return (
    <div
      class="st-bucket"
      classList={{ dropping: dragging() }}
      data-target={`Drop to upload to ${reference(path())}`}
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
      <div class="st-bar">
        <nav class="st-crumbs" aria-label="Path">
          <button class="st-crumb" classList={{ here: !path() && !fileKey() }} onClick={() => openFolder('')}>{props.bucket.name}</button>
          <For each={crumbsOf(path())}>
            {(c) => (
              <>
                <span class="st-sep" aria-hidden="true">/</span>
                <button class="st-crumb" classList={{ here: c.prefix === path() && !fileKey() }} onClick={() => openFolder(c.prefix)}>{c.name}</button>
              </>
            )}
          </For>
          <Show when={fileKey()}>
            {(k) => (
              <>
                <span class="st-sep" aria-hidden="true">/</span>
                <span class="st-crumb here">{k().slice(k().lastIndexOf('/') + 1)}</span>
              </>
            )}
          </Show>
        </nav>
        <Show when={props.bucket.browsable}>
          <Show
            when={selected().size}
            fallback={
              <div class="st-tools">
                <form class="st-goto" onSubmit={openTyped}>
                  <input placeholder="Go to folder" aria-label="Go to folder" value={goTo()} onInput={(e) => setGoTo(e.currentTarget.value)} />
                </form>
                <input class="st-filter" type="search" placeholder="Filter" aria-label="Filter this folder" value={filter()} onInput={(e) => setFilter(e.currentTarget.value)} />
                <button class="dx-iconbtn" title="Reload" aria-label="Reload" onClick={refresh}>↻</button>
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
                  <button
                    class="btn btn-sm btn-primary"
                    title={`Up to about ${formatBytes(props.limits.default_upload_bytes)} a file, unless the database's request limit was raised`}
                    onClick={() => fileInput?.click()}
                  >
                    Upload
                  </button>
                </Show>
              </div>
            }
          >
            <div class="st-tools st-picked">
              <span>{selected().size} selected</span>
              <button class="btn btn-sm" onClick={() => setSelected(new Set<string>())}>Clear</button>
              <button class="btn btn-sm dx-danger" onClick={() => void removeSelected()}>
                Delete {selected().size} {selected().size === 1 ? 'file' : 'files'}
              </button>
            </div>
          </Show>
        </Show>
      </div>

      <Show
        when={props.bucket.browsable}
        fallback={
          <div class="st-note">
            <h2>These files are in R2, not in the database</h2>
            <p>This bucket is declared <code>BACKEND "r2"</code>: the SSP keeps its files next to itself, and the database only checks permissions. Browsing that store is not available here yet.</p>
          </div>
        }
      >
        <Show when={!props.bucket.lists_folders}>
          <p class="st-limit-short">Subfolders are hidden here. Use Go to folder to open one by its path.</p>
        </Show>
        {/* Columns scroll sideways; arrow keys are handled for the rows inside them. */}
        <div class="st-columns" ref={scroller} style={{ '--st-pane': paneWidth() }} onKeyDown={onKey}>
          <For each={prefixes()}>
            {(prefix) => (
              <Column
                url={url}
                prefix={prefix}
                path={path()}
                fileKey={fileKey()}
                version={version()}
                filter={prefix === path() ? filter() : ''}
                selected={selected()}
                selectable={writable()}
                onFolder={openFolder}
                onFile={openFile}
                onToggle={toggle}
                onLoaded={(found) => noteFolder(prefix, found)}
                onSummary={(s) => summarize(prefix, s)}
              />
            )}
          </For>
          <Show
            when={fileKey()}
            keyed
            fallback={
              <FolderPane
                bucket={props.bucket}
                prefix={path()}
                summary={summaries()[path()]}
                writable={writable()}
                remembered={remembered()}
                onUpload={() => fileInput?.click()}
                onOpen={openFolder}
              />
            }
          >
            {(key) => (
              <FilePane
                bucket={props.bucket}
                fileKey={key}
                maxRead={props.limits.max_read_bytes}
                writable={writable()}
                onClose={() => setQuery({ path: parentOf(key) || undefined, file: undefined })}
                onMoved={(to) => {
                  refresh();
                  openFile(to);
                }}
                onGone={() => {
                  setQuery({ path: parentOf(key) || undefined, file: undefined });
                  refresh();
                }}
              />
            )}
          </Show>
        </div>
      </Show>

      <Show when={uploads().length}>
        <div class="st-tray" role="status">
          <div class="st-tray-head">
            <span>{uploadsDone() ? 'Uploads finished' : 'Uploading'}</span>
            <Show when={uploadsDone()}><button class="fx-link" onClick={() => setUploads([])}>Dismiss</button></Show>
          </div>
          <For each={uploads()}>
            {(u) => (
              <div class="st-up" classList={{ bad: u.state === 'failed', done: u.state === 'done', busy: u.state === 'uploading' }}>
                <span class="st-up-name">{u.name}</span>
                <span class="st-up-state">{u.state === 'failed' ? u.error : u.state === 'done' ? formatBytes(u.size) : u.state === 'waiting' ? 'Waiting' : 'Uploading'}</span>
              </div>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* One folder                                                           */
/* ------------------------------------------------------------------ */

function Column(props: {
  url: string;
  prefix: string;
  path: string;
  fileKey: string | undefined;
  version: number;
  filter: string;
  selected: ReadonlySet<string>;
  selectable: boolean;
  onFolder: (prefix: string) => void;
  onFile: (key: string) => void;
  onToggle: (key: string) => void;
  onLoaded: (found: boolean) => void;
  onSummary: (s: Summary) => void;
}) {
  const [folders, setFolders] = createSignal<string[]>([]);
  const [files, setFiles] = createSignal<FileEntry[]>([]);
  const [counts, setCounts] = createSignal<Record<string, number>>({});
  const [next, setNext] = createSignal<string | null>(null);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string>();

  // Only the newest request answers: a slow page must not land over a fresh one.
  let latest = 0;
  const load = async (more: boolean) => {
    const mine = ++latest;
    setLoading(true);
    const q = new URLSearchParams();
    if (props.prefix) q.set('prefix', props.prefix);
    const cursor = untrack(next);
    if (more && cursor) q.set('start', cursor);
    const r = await api.getResult<FilesPage>(`${props.url}/files?${q.toString()}`);
    if (mine !== latest) return;
    setLoading(false);
    if (!r.ok) return void setError(r.message);
    setError(undefined);
    const nextFolders = more ? [...new Set([...untrack(folders), ...r.value.folders])] : r.value.folders;
    const nextFiles = more ? [...untrack(files), ...r.value.files] : r.value.files;
    setFolders(nextFolders);
    setFiles(nextFiles);
    setCounts(more ? { ...untrack(counts), ...r.value.counts } : (r.value.counts ?? {}));
    setNext(r.value.next);
    props.onLoaded(nextFolders.length + nextFiles.length > 0);
    props.onSummary({
      folders: nextFolders.length,
      files: nextFiles.length,
      bytes: nextFiles.reduce((n, f) => n + f.size, 0),
      more: !!r.value.next,
    });
  };
  createEffect(on(() => props.version, () => void load(false)));

  const needle = () => props.filter.trim().toLowerCase();
  const activeChild = () => childOn(props.prefix, props.path);
  // The way forward stays visible even where the listing cannot name it: a folder
  // typed in on a backend that hides folders, one that has no file in it yet.
  const shownFolders = createMemo(() => {
    const child = activeChild();
    const list = child && !folders().includes(child) ? [...folders(), child].sort() : folders();
    return list.filter((f) => f.toLowerCase().includes(needle()));
  });
  const shownFiles = createMemo(() => files().filter((f) => f.name.toLowerCase().includes(needle())));
  const empty = () => !loading() && !error() && !shownFolders().length && !shownFiles().length;
  const fileOpenHere = () => !!props.fileKey && parentOf(props.fileKey) === props.prefix;

  return (
    <section class="st-col" aria-label={props.prefix ? `Folder ${props.prefix}` : 'Bucket root'}>
      <Show when={error()}>
        <div class="st-col-note bad">{error()}</div>
      </Show>
      <Show when={loading() && !folders().length && !files().length && !activeChild()}>
        <div class="st-skel" aria-hidden="true"><i /><i /><i /><i /></div>
      </Show>
      <For each={shownFolders()}>
        {(name) => {
          const on = () => activeChild() === name;
          const count = () => counts()[name];
          return (
            <div class="st-item">
              <button
                class="st-row"
                classList={{ trail: on() && (fileOpenHere() || childOn(`${props.prefix}${name}/`, props.path) !== null || !!props.fileKey), now: on() && props.path === `${props.prefix}${name}/` && !props.fileKey }}
                data-folder="1"
                aria-current={on() ? 'true' : undefined}
                onClick={() => props.onFolder(`${props.prefix}${name}/`)}
              >
                <Glyph kind="folder" />
                <span class="st-name">{name}</span>
                <Show when={count() !== undefined}><span class="st-aside">{countLabel(count())}</span></Show>
                <svg class="st-chev" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" aria-hidden="true"><path d="M9 6l6 6-6 6" /></svg>
              </button>
            </div>
          );
        }}
      </For>
      <For each={shownFiles()}>
        {(f) => (
          <div class="st-item" classList={{ picked: props.selected.has(f.key), picking: props.selected.size > 0 }}>
            <Show when={props.selectable}>
              <input class="st-check" type="checkbox" aria-label={`Select ${f.name}`} checked={props.selected.has(f.key)} onChange={() => props.onToggle(f.key)} />
            </Show>
            <button
              class="st-row"
              classList={{ now: props.fileKey === f.key }}
              aria-current={props.fileKey === f.key ? 'true' : undefined}
              title={`${f.name}, modified ${formatStamp(f.updated)}`}
              onClick={() => props.onFile(f.key)}
            >
              <Glyph kind={fileKindOf(f.name)} />
              <span class="st-name">{f.name}</span>
              <span class="st-aside">{formatBytes(f.size)}</span>
            </button>
          </div>
        )}
      </For>
      <Show when={empty()}>
        <div class="st-col-note">{props.filter ? 'Nothing matches' : 'Empty'}</div>
      </Show>
      <Show when={next()}>
        <button class="st-more" disabled={loading()} onClick={() => void load(true)}>{loading() ? 'Loading' : 'Load more'}</button>
      </Show>
    </section>
  );
}

const GLYPHS: Record<string, string> = {
  image: 'M4 4h16v16H4z M4 16l5-5 4 4 3-3 4 4 M15 9h.01',
  video: 'M4 5h16v14H4z M10 9l5 3-5 3z',
  audio: 'M9 18V6l10-2v12 M9 18a3 3 0 1 1-6 0 3 3 0 0 1 6 0z M19 16a3 3 0 1 1-6 0 3 3 0 0 1 6 0z',
  text: 'M6 3h9l4 4v14H6z M9 11h7 M9 15h7',
  other: 'M6 3h9l4 4v14H6z',
};

function Glyph(props: { kind: string }) {
  return (
    <Show
      when={props.kind !== 'folder'}
      fallback={
        <svg class="st-glyph folder" width="15" height="15" viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M3 5h6.5l2 2.5H21V19H3z" /></svg>
      }
    >
      <svg class={`st-glyph ${props.kind}`} width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d={GLYPHS[props.kind]} />
      </svg>
    </Show>
  );
}

/* ------------------------------------------------------------------ */
/* The last column                                                      */
/* ------------------------------------------------------------------ */

/** Where a bucket's files live, and what that means for them. */
function whereKept(bucket: BucketInfo): string {
  if (bucket.backend === 'file') return `On disk at ${bucket.location}`;
  if (bucket.backend === 'memory') return 'In memory: every file is lost when the database restarts';
  if (bucket.backend === 'r2') return 'In R2, next to the SSP';
  return bucket.backend === 'global' ? "In the database server's global store" : bucket.backend;
}

/** Nothing open: where you are, what is here, and the way to add to it. */
function FolderPane(props: {
  bucket: BucketInfo;
  prefix: string;
  summary: Summary | undefined;
  writable: boolean;
  remembered: string[];
  onUpload: () => void;
  onOpen: (prefix: string) => void;
}) {
  const title = () => {
    const parts = props.prefix.split('/').filter(Boolean);
    return parts[parts.length - 1] ?? props.bucket.name;
  };
  return (
    <aside class="st-pane st-folder">
      <div class="st-folder-mark"><Glyph kind="folder" /></div>
      <h2 class="st-title">{title()}</h2>
      <Show when={props.summary}>
        {(s) => (
          <p class="st-holds">
            {folderSummary(s().folders, s().files, s().more)}
            <Show when={s().bytes > 0}>, {formatBytes(s().bytes)}</Show>
          </p>
        )}
      </Show>
      <Show when={props.writable}>
        <div class="st-pane-actions">
          <button class="btn btn-sm" onClick={props.onUpload}>Upload here</button>
          <span class="st-quiet">or drop files anywhere on this page</span>
        </div>
      </Show>
      <dl class="st-facts">
        <div><dt>Kept</dt><dd>{whereKept(props.bucket)}</dd></div>
        <Show when={props.bucket.max_bytes}><div><dt>App upload limit</dt><dd>{formatBytes(props.bucket.max_bytes)} a file</dd></div></Show>
        <Show when={props.bucket.readonly}><div><dt>Access</dt><dd>Read-only</dd></div></Show>
      </dl>
      <Show when={!props.bucket.lists_folders}>
        <div class="st-limit">
          <p><b>Subfolders are hidden here.</b> This project's scheduler cannot read the bucket's disk, and SurrealDB does not name the folders inside a folder. Use Go to folder to open one by its path.</p>
          <Show when={props.remembered.length}>
            <div class="st-remembered">
              <For each={props.remembered}>{(p) => <button class="dx-chip plain" onClick={() => props.onOpen(p)}>{p}</button>}</For>
            </div>
          </Show>
        </div>
      </Show>
    </aside>
  );
}

function FilePane(props: {
  bucket: BucketInfo;
  fileKey: string;
  maxRead: number;
  writable: boolean;
  onClose: () => void;
  onMoved: (to: string) => void;
  onGone: () => void;
}) {
  const url = `/data/buckets/${enc(props.bucket.name)}`;
  const name = () => props.fileKey.slice(props.fileKey.lastIndexOf('/') + 1);
  const kind = () => fileKindOf(props.fileKey);
  const reference = () => `f"${props.bucket.name}:/${props.fileKey}"`;

  const [meta] = createResource(() => api.getResult<FileContent>(`${url}/file?key=${enc(props.fileKey)}&meta=1`));
  const size = () => {
    const m = meta();
    return m?.ok ? m.value.size : undefined;
  };
  const updated = () => {
    const m = meta();
    return m?.ok ? m.value.updated : undefined;
  };

  const [content, setContent] = createSignal<FileContent | null>(null);
  const [mediaUrl, setMediaUrl] = createSignal<string | null>(null);
  const [mediaBlocked, setMediaBlocked] = createSignal(false);
  const [text, setText] = createSignal<string | null>(null);
  const [cut, setCut] = createSignal(false);
  const [loading, setLoading] = createSignal(false);
  const [error, setError] = createSignal<string>();
  let closed = false;
  onCleanup(() => {
    closed = true;
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
    if (closed || !c?.data) return;
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

  const rename = async (e: Event) => {
    e.preventDefault();
    if (target() === props.fileKey) return void setRenaming(false);
    const to = target().trim().replace(/^\/+/, '');
    if (!to || to === props.fileKey) return void setRenaming(false);
    try {
      await api.post(`${url}/rename`, { key: props.fileKey, target: to });
      toast('ok', 'Renamed', to);
      props.onMoved(to);
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
      props.onGone();
    } catch (err) {
      toast('bad', 'Could not delete', err instanceof Error ? err.message : undefined);
    }
  };

  return (
    <aside class="st-pane st-file" aria-label={`File ${name()}`}>
      <Show when={meta() && !meta()!.ok}>
        <DataError message={(meta() as { ok: false; message: string }).message} />
      </Show>
      <div class="st-stage" classList={{ media: kind() === 'image', text: text() !== null }}>
        <Show when={kind() === 'image' && mediaUrl()}>
          <img class="st-media" src={mediaUrl() ?? undefined} alt={name()} onError={() => setMediaBlocked(true)} />
        </Show>
        <Show when={kind() === 'video' && mediaUrl() && !mediaBlocked()}>
          <video class="st-media" src={mediaUrl() ?? undefined} controls onError={() => setMediaBlocked(true)} />
        </Show>
        <Show when={kind() === 'audio' && mediaUrl() && !mediaBlocked()}>
          <audio class="st-audio" src={mediaUrl() ?? undefined} controls onError={() => setMediaBlocked(true)} />
        </Show>
        <Show when={text() !== null}>
          <pre class="st-text">{text()}</pre>
        </Show>
        <Show when={!mediaUrl() && text() === null}>
          <div class="st-placeholder">
            <span class="st-ext">{extensionOf(props.fileKey) || 'file'}</span>
            <Show
              when={previewable()}
              fallback={<span>Download to open it.</span>}
            >
              <Show
                when={!tooBig()}
                fallback={<span>Larger than the {formatBytes(props.maxRead)} this page can read.</span>}
              >
                <Show
                  when={(size() ?? 0) > AUTO_PREVIEW_BYTES}
                  fallback={<span aria-busy="true">{loading() ? 'Reading' : ''}</span>}
                >
                  <button class="btn btn-sm" disabled={loading()} onClick={() => void preview()}>{loading() ? 'Reading' : `Load preview, ${formatBytes(size())}`}</button>
                </Show>
              </Show>
            </Show>
          </div>
        </Show>
        <Show when={mediaBlocked()}>
          <div class="st-placeholder"><span>This browser cannot play it here. Download it to open it.</span></div>
        </Show>
      </div>
      <Show when={cut()}><p class="st-quiet st-cut">Showing the first {formatBytes(TEXT_PREVIEW_BYTES)}.</p></Show>
      <DataError message={error()} />

      <div class="st-file-body">
        <h2 class="st-title">{name()}</h2>
        <p class="st-holds">
          <Show when={size() !== undefined}>{formatBytes(size())}</Show>
          <Show when={updated()}>{(u) => <span title={formatStamp(u())}>, modified {relativeStamp(u())}</span>}</Show>
        </p>

        <div class="st-pane-actions">
          <button
            class="btn btn-sm btn-primary"
            disabled={loading() || tooBig()}
            title={tooBig() ? `Larger than the ${formatBytes(props.maxRead)} this page can read` : undefined}
            onClick={() => void download()}
          >
            {loading() ? 'Reading' : 'Download'}
          </button>
          <CopyText value={reference()} label="Copy reference" />
          <Show when={props.writable}>
            <button class="btn btn-sm" aria-expanded={renaming()} onClick={() => setRenaming(!renaming())}>Rename</button>
            <button class="btn btn-sm dx-danger" onClick={() => void remove()}>Delete</button>
          </Show>
        </div>

        <Show when={renaming()}>
          <form class="st-rename" onSubmit={rename}>
            <input aria-label="New path" ref={(el) => queueMicrotask(() => el.focus())} value={target()} onInput={(e) => setTarget(e.currentTarget.value)} />
            <button class="btn btn-sm btn-primary" type="submit">Save</button>
          </form>
        </Show>

        <dl class="st-facts">
          <div><dt>Path</dt><dd>/{props.fileKey}</dd></div>
          <div><dt>In queries</dt><dd>{reference()}</dd></div>
        </dl>
      </div>
      <button class="st-close" aria-label="Close preview" title="Close preview (Esc)" onClick={props.onClose}>
        <svg width="12" height="12" viewBox="0 0 10 10" stroke="currentColor" stroke-width="1.3" aria-hidden="true"><path d="M2 2l6 6M8 2l-6 6" /></svg>
      </button>
    </aside>
  );
}
