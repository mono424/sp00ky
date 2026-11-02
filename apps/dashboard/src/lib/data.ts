/**
 * The wire shapes of `/admin/api/data` and the pure rules the Database, Users
 * and Storage pages share: what a value looks like in a grid, how text in a form
 * becomes the value it means, how a key breaks into folders.
 */

export interface TableSummary {
  name: string;
  kind: 'normal' | 'relation' | 'any' | string;
  in?: string[] | null;
  out?: string[] | null;
  schemafull: boolean;
  view: boolean;
  comment?: string | null;
  internal: boolean;
}

export interface FieldInfo {
  name: string;
  kind: string;
  optional: boolean;
  nested: boolean;
  readonly: boolean;
  flexible: boolean;
  default?: string | null;
  value?: string | null;
  assert?: string | null;
  comment?: string | null;
  credential: boolean;
  required: boolean;
}

export interface TableDetail extends TableSummary {
  fields: FieldInfo[];
  indexes: { name: string; cols?: string[]; index?: unknown; table?: string }[];
  events: { name: string; when?: string }[];
  read_only: boolean;
}

export type Row = Record<string, unknown>;

export interface RowsPage {
  rows: Row[];
  has_more: boolean;
  limit: number;
  start: number;
}

export type FilterOp = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'starts_with' | 'is_none' | 'not_none';

export interface Filter {
  field: string;
  op: FilterOp;
  value: string;
}

export const FILTER_OPS: { op: FilterOp; label: string; noValue?: boolean }[] = [
  { op: 'eq', label: '=' },
  { op: 'ne', label: '≠' },
  { op: 'gt', label: '>' },
  { op: 'gte', label: '≥' },
  { op: 'lt', label: '<' },
  { op: 'lte', label: '≤' },
  { op: 'contains', label: 'contains' },
  { op: 'starts_with', label: 'starts with' },
  { op: 'is_none', label: 'is NONE', noValue: true },
  { op: 'not_none', label: 'is set', noValue: true },
];

export interface QueryResult {
  rows?: unknown[];
  total?: number;
  truncated?: boolean;
  value?: unknown;
}

export interface QueryAnswer {
  results: QueryResult[];
  limit_added: number | null;
}

export interface UserSource {
  access: string;
  table: string;
  identifier: string | null;
  created_field: string | null;
  /** The list is newest first by itself only when an index makes that free. */
  created_indexed: boolean;
  search_fields: string[];
  can_set_password: boolean;
  sources: string[];
}

export interface UserEntry {
  row: Row;
  is_admin: boolean | null;
  views: number | null;
  sessions: number | null;
}

export interface UsersPage {
  source: UserSource | null;
  users: UserEntry[];
  has_more: boolean;
  limit?: number;
  start?: number;
}

export interface UserStats {
  total: number | null;
  admins: number | null;
  online: number | null;
}

export interface UserDetail {
  source: UserSource;
  row: Row;
  is_admin: boolean | null;
  admin: { note?: string | null; added_at?: string } | null;
  sessions: { client_id: string; views: number }[] | null;
  flags: { key: string; variant: string }[] | null;
}

export interface BucketInfo {
  name: string;
  backend: string;
  location?: string | null;
  readonly: boolean;
  max_bytes?: number | null;
  browsable: boolean;
  lists_folders: boolean;
}

export interface BucketsAnswer {
  buckets: BucketInfo[];
  max_read_bytes: number;
  default_upload_bytes: number;
}

export interface FileEntry {
  key: string;
  name: string;
  size: number;
  updated: string;
}

export interface FilesPage {
  prefix: string;
  folders: string[];
  /** Entries in each folder, where the host can read the bucket's disk. */
  counts?: Record<string, number>;
  files: FileEntry[];
  next: string | null;
}

export interface FileContent {
  key: string;
  size: number;
  updated: string;
  data?: string;
}

/* ------------------------------------------------------------------ */
/* Values                                                               */
/* ------------------------------------------------------------------ */

export interface Trimmed {
  $trimmed: true;
  bytes: number;
  preview: string;
}

export function isRedacted(value: unknown): boolean {
  return !!value && typeof value === 'object' && (value as { $redacted?: unknown }).$redacted === true;
}

export function isTrimmed(value: unknown): value is Trimmed {
  return !!value && typeof value === 'object' && (value as { $trimmed?: unknown }).$trimmed === true;
}

/**
 * Whether a value holds a mask at any depth. A form showing one must not be saved
 * as text: the mask would land on top of what it stands for.
 */
export function holdsMask(value: unknown): boolean {
  if (isRedacted(value) || isTrimmed(value)) return true;
  if (Array.isArray(value)) return value.some(holdsMask);
  if (value && typeof value === 'object') return Object.values(value).some(holdsMask);
  return false;
}

/** A defined type without its optional wrapper: `none | datetime` is `datetime`. */
export function baseKind(kind: string | undefined): string {
  if (!kind) return 'any';
  const wrapped = /^option<(.*)>$/.exec(kind.trim());
  const inner = wrapped ? wrapped[1] : kind;
  const parts: string[] = [];
  let depth = 0;
  let part = '';
  for (const c of inner) {
    if ('<([{'.includes(c)) depth++;
    if ('>)]}'.includes(c)) depth--;
    if (c === '|' && depth === 0) {
      parts.push(part);
      part = '';
    } else part += c;
  }
  parts.push(part);
  const real = parts.map((p) => p.trim()).filter((p) => p !== 'none' && p !== 'null');
  return real.length === 1 ? real[0] : 'any';
}

const NUMBER_KINDS = new Set(['int', 'float', 'number']);

export type Editor = 'bool' | 'number' | 'decimal' | 'text' | 'json' | 'loose';

/** How a field is edited, by its defined type, or by the value it holds now. */
export function editorFor(kind: string | undefined, value: unknown): Editor {
  const base = baseKind(kind);
  if (base === 'bool') return 'bool';
  if (NUMBER_KINDS.has(base)) return 'number';
  if (base === 'decimal') return 'decimal';
  if (/^(string|datetime|duration|uuid|record|file|references)\b/.test(base)) return 'text';
  if (/^(array|set|object|geometry|bytes)\b/.test(base)) return 'json';
  // No type to go by: keep whatever it is.
  if (typeof value === 'boolean') return 'bool';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'string') return 'text';
  if (value && typeof value === 'object') return 'json';
  return 'loose';
}

/** The text a form shows for a value. */
export function draftOf(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'object') return JSON.stringify(value, null, 2);
  return String(value);
}

export type Parsed = { ok: true; value: unknown } | { ok: false; error: string };

/** The value text in a form means. */
export function parseDraft(kind: string | undefined, original: unknown, text: string): Parsed {
  switch (editorFor(kind, original)) {
    case 'bool':
      if (text === 'true') return { ok: true, value: true };
      if (text === 'false') return { ok: true, value: false };
      return { ok: false, error: 'true or false' };
    case 'number': {
      const n = Number(text);
      if (text.trim() === '' || !Number.isFinite(n)) return { ok: false, error: 'A number is required' };
      return { ok: true, value: n };
    }
    // As text, so a long decimal keeps its digits; the server casts it.
    case 'decimal':
    case 'text':
      return { ok: true, value: text };
    case 'json':
      try {
        return { ok: true, value: JSON.parse(text) };
      } catch {
        return { ok: false, error: 'Not valid JSON' };
      }
    case 'loose':
      try {
        return { ok: true, value: JSON.parse(text) };
      } catch {
        return { ok: true, value: text };
      }
  }
}

/** `id`, the table's own fields in order, then whatever rows carry beyond them. */
export function columnsOf(fields: FieldInfo[] | undefined, rows: Row[]): string[] {
  const out = ['id'];
  const seen = new Set(out);
  const add = (name: string) => {
    if (!seen.has(name)) {
      seen.add(name);
      out.push(name);
    }
  };
  for (const f of fields ?? []) if (!f.nested) add(f.name);
  for (const row of rows) for (const key of Object.keys(row)) add(key);
  return out;
}

/** The table a value links to, if it reads as a record of one that exists. */
export function recordTable(value: unknown, tables: ReadonlySet<string>): string | null {
  if (typeof value !== 'string') return null;
  const at = value.indexOf(':');
  if (at <= 0 || at === value.length - 1) return null;
  const table = value.slice(0, at);
  return tables.has(table) ? table : null;
}

const ISO_STAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

export function looksLikeStamp(value: unknown): value is string {
  return typeof value === 'string' && ISO_STAMP.test(value);
}

/** One line of a value, for places that are only text: a copied cell, a CSV. */
export function plainText(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

export function toCsv(rows: unknown[]): string {
  const records = rows.map((r) => (r && typeof r === 'object' && !Array.isArray(r) ? (r as Row) : { value: r }));
  const headers: string[] = [];
  for (const record of records) for (const key of Object.keys(record)) if (!headers.includes(key)) headers.push(key);
  const quote = (text: string) => (/[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text);
  return [headers.map(quote).join(','), ...records.map((r) => headers.map((h) => quote(plainText(r[h]))).join(','))].join('\n');
}

const WRITE_WORDS = /\b(CREATE|INSERT|RELATE|UPDATE|UPSERT|DELETE|REMOVE|DEFINE|ALTER|REBUILD|KILL|DROP|PUT|RENAME|COPY)\b/gi;

/**
 * The words of a script that write something, strings and comments aside. One pass
 * over the text, because each hides the other: the `//` in `'https://a'` is not a
 * comment, and the `'` in `-- don't` does not open a string.
 */
export function writeWords(sql: string): string[] {
  let code = '';
  for (let i = 0; i < sql.length; i++) {
    const c = sql[i];
    if (c === "'" || c === '"' || c === '`') {
      for (i++; i < sql.length && sql[i] !== c; i++) if (sql[i] === '\\') i++;
      code += ' ';
    } else if ((c === '-' && sql[i + 1] === '-') || (c === '/' && sql[i + 1] === '/') || c === '#') {
      while (i < sql.length && sql[i] !== '\n') i++;
      code += ' ';
    } else if (c === '/' && sql[i + 1] === '*') {
      const end = sql.indexOf('*/', i + 2);
      i = end < 0 ? sql.length : end + 1;
      code += ' ';
    } else {
      code += c;
    }
  }
  return [...new Set((code.match(WRITE_WORDS) ?? []).map((w) => w.toUpperCase()))];
}

/* ------------------------------------------------------------------ */
/* Files                                                                */
/* ------------------------------------------------------------------ */

/** Each folder on the way to `prefix`: `a/b/` is `a` at `a/` and `b` at `a/b/`. */
export function crumbsOf(prefix: string): { name: string; prefix: string }[] {
  const out: { name: string; prefix: string }[] = [];
  let at = '';
  for (const name of prefix.split('/').filter(Boolean)) {
    at += `${name}/`;
    out.push({ name, prefix: at });
  }
  return out;
}

/** A typed folder path as a listing prefix. */
export function normalizePrefix(path: string): string {
  const inner = path.split('/').filter(Boolean).join('/');
  return inner ? `${inner}/` : '';
}

/** The folder a key is in: `a/b/x.png` is in `a/b/`, `x.png` in the root (`''`). */
export function parentOf(key: string): string {
  return key.slice(0, key.lastIndexOf('/') + 1);
}

/** One column per level on the way to a folder: `a/b/` is `''`, `a/`, `a/b/`. */
export function columnPrefixes(path: string): string[] {
  return ['', ...crumbsOf(path).map((c) => c.prefix)];
}

/** The next folder on the way from `prefix` to `path`, if `path` is under it. */
export function childOn(prefix: string, path: string): string | null {
  if (path === prefix || !path.startsWith(prefix)) return null;
  return path.slice(prefix.length).split('/')[0] || null;
}

/** A folder's entry count, which a host stops at 1000 so a huge one stays cheap. */
export function countLabel(n: number): string {
  return n >= 1000 ? '999+' : String(n);
}

/** What a folder holds, in words. */
export function folderSummary(folders: number, files: number, more: boolean): string {
  const part = (n: number, one: string, many: string) => (n === 1 ? `1 ${one}` : `${n} ${many}`);
  if (!folders && !files) return 'Empty';
  const said = [folders ? part(folders, 'folder', 'folders') : '', files ? part(files, 'file', 'files') : ''].filter(Boolean).join(', ');
  return more ? `${said} so far` : said;
}

export type FileKind = 'image' | 'video' | 'audio' | 'text' | 'other';

const MIME: Record<string, [string, FileKind]> = {
  png: ['image/png', 'image'],
  jpg: ['image/jpeg', 'image'],
  jpeg: ['image/jpeg', 'image'],
  gif: ['image/gif', 'image'],
  webp: ['image/webp', 'image'],
  avif: ['image/avif', 'image'],
  // Only ever an `<img>`, where its scripts do not run.
  svg: ['image/svg+xml', 'image'],
  ico: ['image/x-icon', 'image'],
  mp4: ['video/mp4', 'video'],
  webm: ['video/webm', 'video'],
  mov: ['video/quicktime', 'video'],
  m4a: ['audio/mp4', 'audio'],
  mp3: ['audio/mpeg', 'audio'],
  ogg: ['audio/ogg', 'audio'],
  wav: ['audio/wav', 'audio'],
};

const TEXT_EXTENSIONS = new Set([
  'txt', 'md', 'json', 'jsonl', 'ndjson', 'csv', 'tsv', 'log', 'xml', 'yml', 'yaml', 'toml', 'ini', 'pgn', 'fen', 'surql', 'sql',
  'js', 'ts', 'tsx', 'jsx', 'css', 'rs', 'go', 'py', 'sh', 'env',
  // Read as text, never rendered: a blob keeps this page's origin.
  'html', 'htm',
]);

export function extensionOf(name: string): string {
  const base = name.slice(name.lastIndexOf('/') + 1);
  const dot = base.lastIndexOf('.');
  return dot > 0 ? base.slice(dot + 1).toLowerCase() : '';
}

/** Own keys only: a file named `.constructor` has no type. */
const mimeOf = (ext: string) => (Object.prototype.hasOwnProperty.call(MIME, ext) ? MIME[ext] : undefined);

export function fileKindOf(name: string): FileKind {
  const ext = extensionOf(name);
  return mimeOf(ext)?.[1] ?? (TEXT_EXTENSIONS.has(ext) ? 'text' : 'other');
}

/** The type a preview blob gets. Never one a browser would run as a page. */
export function previewMime(name: string): string {
  return mimeOf(extensionOf(name))?.[0] ?? 'application/octet-stream';
}

export function base64ToBytes(base64: string): Uint8Array<ArrayBuffer> {
  const binary = atob(base64.replace(/=+$/, ''));
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/** A password to hand over once: 20 characters a person can still read aloud. */
export function generatePassword(random: (bytes: Uint8Array<ArrayBuffer>) => void = (b) => void crypto.getRandomValues(b)): string {
  const alphabet = 'abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const bytes = new Uint8Array(20);
  random(bytes);
  // Above the last whole multiple would favour the first few letters.
  const limit = 256 - (256 % alphabet.length);
  let out = '';
  for (const byte of bytes) {
    let b = byte;
    while (b >= limit) {
      const one = new Uint8Array(1);
      random(one);
      b = one[0];
    }
    out += alphabet[b % alphabet.length];
  }
  return out;
}
