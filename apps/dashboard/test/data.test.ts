// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  base64ToBytes,
  baseKind,
  childOn,
  columnPrefixes,
  columnsOf,
  countLabel,
  crumbsOf,
  draftOf,
  editorFor,
  extensionOf,
  fileKindOf,
  folderSummary,
  generatePassword,
  holdsMask,
  isRedacted,
  isTrimmed,
  looksLikeStamp,
  normalizePrefix,
  parentOf,
  parseDraft,
  previewMime,
  recordTable,
  toCsv,
  writeWords,
  type FieldInfo,
} from '../src/lib/data.ts';

test('a type loses its optional wrapper, and only that', () => {
  assert.equal(baseKind('none | datetime'), 'datetime');
  assert.equal(baseKind('option<int>'), 'int');
  assert.equal(baseKind('none | array<int | string>'), 'array<int | string>');
  assert.equal(baseKind('string | int'), 'any');
  assert.equal(baseKind(undefined), 'any');
});

test('a form edits by the defined type, then by the value', () => {
  assert.equal(editorFor('none | bool', undefined), 'bool');
  assert.equal(editorFor('int', 1), 'number');
  assert.equal(editorFor('decimal', '1.50'), 'decimal');
  assert.equal(editorFor('datetime', undefined), 'text');
  assert.equal(editorFor('record<user>', undefined), 'text');
  assert.equal(editorFor('array<string>', []), 'json');
  assert.equal(editorFor('any', 'x'), 'text');
  assert.equal(editorFor(undefined, true), 'bool');
  assert.equal(editorFor(undefined, 3), 'number');
  assert.equal(editorFor(undefined, { a: 1 }), 'json');
  assert.equal(editorFor(undefined, undefined), 'loose');
});

test('text in a form becomes the value it means', () => {
  const value = (kind: string | undefined, original: unknown, text: string) => {
    const parsed = parseDraft(kind, original, text);
    assert.ok(parsed.ok, `${text} should parse`);
    return (parsed as { value: unknown }).value;
  };
  assert.equal(value('int', 1, '42'), 42);
  assert.equal(value('bool', true, 'false'), false);
  assert.equal(value('string', 'x', '123'), '123', 'a string stays a string');
  assert.equal(value(undefined, 'x', '123'), '123', 'a string with no schema stays a string');
  assert.equal(value(undefined, 5, '123'), 123);
  assert.deepEqual(value('array<int>', [], '[1, 2]'), [1, 2]);
  assert.equal(value(undefined, undefined, 'plain'), 'plain');
  assert.equal(value(undefined, undefined, '12'), 12);
  assert.equal(value('decimal', '1', '1.123456789012345678'), '1.123456789012345678');
  assert.equal(parseDraft('int', 1, 'abc').ok, false);
  assert.equal(parseDraft('int', 1, '').ok, false);
  assert.equal(parseDraft('object', {}, '{ nope').ok, false);
  assert.equal(parseDraft('bool', true, 'maybe').ok, false);
  assert.equal(draftOf(null), '');
  assert.equal(draftOf(0), '0');
  assert.equal(draftOf({ a: 1 }), '{\n  "a": 1\n}');
});

test('columns are the id, the schema, then whatever the rows carry', () => {
  const fields = [
    { name: 'name', nested: false },
    { name: 'tags', nested: false },
    { name: 'tags.*', nested: true },
  ] as FieldInfo[];
  assert.deepEqual(columnsOf(fields, [{ id: 't:1', extra: 1, name: 'a' }]), ['id', 'name', 'tags', 'extra']);
  assert.deepEqual(columnsOf(undefined, []), ['id']);
});

test('only a real table makes a record link', () => {
  const tables = new Set(['user']);
  assert.equal(recordTable('user:abc', tables), 'user');
  assert.equal(recordTable('user:`a-b`', tables), 'user');
  assert.equal(recordTable('https://x', tables), null);
  assert.equal(recordTable('user:', tables), null);
  assert.equal(recordTable(':x', tables), null);
  assert.equal(recordTable(5, tables), null);
});

test('a mask anywhere in a value is found', () => {
  assert.ok(holdsMask({ $redacted: true }));
  assert.ok(holdsMask({ a: { b: [{ $redacted: true }] } }));
  assert.ok(holdsMask([{ $trimmed: true, bytes: 1, preview: '' }]));
  assert.ok(!holdsMask({ a: { b: 1 } }));
  assert.ok(!holdsMask('text'));
  assert.ok(!holdsMask(null));
});

test('markers from the server read as markers', () => {
  assert.ok(isRedacted({ $redacted: true }));
  assert.ok(!isRedacted('••••'));
  assert.ok(!isRedacted(null));
  assert.ok(isTrimmed({ $trimmed: true, bytes: 9, preview: 'x' }));
  assert.ok(!isTrimmed({ bytes: 9 }));
  assert.ok(looksLikeStamp('2026-01-02T03:04:05.123Z'));
  assert.ok(looksLikeStamp('2026-01-02T03:04:05+01:00'));
  assert.ok(!looksLikeStamp('2026-01-02'));
});

test('CSV quotes what needs it and flattens the rest', () => {
  assert.equal(toCsv([{ a: 1, b: 'x,y' }, { b: 'q"r', c: { d: 1 } }]), 'a,b,c\n1,"x,y",\n,"q""r","{""d"":1}"');
  assert.equal(toCsv([1, 2]), 'value\n1\n2');
});

test('words that write are found in code, not in strings or comments', () => {
  assert.deepEqual(writeWords('SELECT * FROM user'), []);
  assert.deepEqual(writeWords("SELECT * FROM log WHERE action = 'DELETE'"), []);
  assert.deepEqual(writeWords('-- DELETE everything\nSELECT 1'), []);
  assert.deepEqual(writeWords('/* UPDATE */ SELECT 1'), []);
  assert.deepEqual(writeWords('delete user; UPDATE note SET a = 1; delete x'), ['DELETE', 'UPDATE']);
  assert.deepEqual(writeWords('REMOVE TABLE user'), ['REMOVE']);
  assert.deepEqual(writeWords('DEFINE TABLE x'), ['DEFINE']);
  assert.deepEqual(writeWords('CREATE note:a'), ['CREATE']);
  assert.deepEqual(writeWords('SELECT updated, created FROM t'), []);
  // What hides what: a URL's `//` is no comment, a comment's `'` opens no string.
  assert.deepEqual(writeWords("CREATE x SET u = 'https://a'; DELETE user;"), ['CREATE', 'DELETE']);
  assert.deepEqual(writeWords("-- don't\nDELETE user; SELECT 'x'"), ['DELETE']);
  assert.deepEqual(writeWords("RETURN 'it\\'s'; DELETE user"), ['DELETE']);
  assert.deepEqual(writeWords('RETURN f"a:/b".put("x")'), ['PUT']);
});

test('a prefix breaks into the folders on the way to it', () => {
  assert.deepEqual(crumbsOf(''), []);
  assert.deepEqual(crumbsOf('a/b/'), [{ name: 'a', prefix: 'a/' }, { name: 'b', prefix: 'a/b/' }]);
  assert.equal(normalizePrefix('/a//b/'), 'a/b/');
  assert.equal(normalizePrefix(' a/'), ' a/');
  assert.equal(normalizePrefix('/'), '');
  assert.equal(normalizePrefix('a'), 'a/');
});

test('a path opens as one column per level', () => {
  assert.deepEqual(columnPrefixes(''), ['']);
  assert.deepEqual(columnPrefixes('a/b/'), ['', 'a/', 'a/b/']);
  assert.equal(parentOf('a/b/x.png'), 'a/b/');
  assert.equal(parentOf('x.png'), '');
  assert.equal(childOn('', 'a/b/'), 'a');
  assert.equal(childOn('a/', 'a/b/'), 'b');
  assert.equal(childOn('a/b/', 'a/b/'), null, 'the last column has no next folder');
  assert.equal(childOn('c/', 'a/b/'), null, 'not on the way');
  // A folder that only shares a start with another is not on the way to it.
  assert.equal(childOn('ab/', 'a/b/'), null);
  assert.equal(extensionOf('a/photo.PNG'), 'png');
  assert.equal(extensionOf('.env'), '');
});

test('a folder says what it holds', () => {
  assert.equal(countLabel(3), '3');
  assert.equal(countLabel(1000), '999+');
  assert.equal(folderSummary(0, 0, false), 'Empty');
  assert.equal(folderSummary(1, 0, false), '1 folder');
  assert.equal(folderSummary(2, 1, false), '2 folders, 1 file');
  assert.equal(folderSummary(0, 100, true), '100 files so far');
});

test('nothing previewable can run as a page', () => {
  assert.equal(fileKindOf('a/photo.PNG'), 'image');
  assert.equal(fileKindOf('x.constructor'), 'other');
  assert.equal(previewMime('x.__proto__'), 'application/octet-stream');
  assert.equal(fileKindOf('clip.webm'), 'video');
  assert.equal(fileKindOf('notes.md'), 'text');
  assert.equal(fileKindOf('page.html'), 'text', 'read as text, never rendered');
  assert.equal(fileKindOf('blob'), 'other');
  assert.equal(fileKindOf('.env'), 'other');
  assert.equal(previewMime('x.svg'), 'image/svg+xml');
  for (const name of ['page.html', 'page.htm', 'x.xml', 'x.js', 'noext']) {
    assert.equal(previewMime(name), 'application/octet-stream', name);
  }
});

test('base64 round trips bytes, padded or not', () => {
  assert.deepEqual([...base64ToBytes('aGVsbG8=')], [...new TextEncoder().encode('hello')]);
  assert.deepEqual([...base64ToBytes('aGVsbG8')], [...new TextEncoder().encode('hello')]);
  assert.deepEqual([...base64ToBytes('')], []);
});

test('a generated password is long, readable and unbiased at the edge', () => {
  const password = generatePassword();
  assert.equal(password.length, 20);
  assert.match(password, /^[a-km-zA-HJ-NP-Z2-9]+$/);
  // Bytes above the last whole multiple of the alphabet are drawn again.
  let calls = 0;
  const out = generatePassword((bytes) => {
    calls++;
    bytes.fill(calls === 1 ? 255 : 0);
  });
  assert.equal(out, 'a'.repeat(20));
});
