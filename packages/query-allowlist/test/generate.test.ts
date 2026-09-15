import { describe, expect, it } from 'vitest';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { generateAllowlist, isArgProxy, makeArgProxy } from '../src/index.js';

const fixtures = resolve(__dirname, 'fixtures');
const schema = join(fixtures, 'schema.ts');
const queries = join(fixtures, 'query.ts');

describe('arg proxy', () => {
  it('behaves as a number, a string, an array and an object at once', () => {
    const p = makeArgProxy('x');
    expect(isArgProxy(p)).toBe(true);
    expect(isArgProxy({})).toBe(false);
    expect(String(p)).toBe('proxy:x');
    expect(`${p}`).toBe('proxy:x');
    expect(p * 2).toBe(2);
    expect(p.length).toBe(1);
    expect([...p]).toHaveLength(1);
    expect(isArgProxy(p.nested.deeper)).toBe(true);
    expect(isArgProxy(p.getTime())).toBe(true);
    expect(p.map(String)).toHaveLength(1);
    expect(p.includes('a')).toBe(true);
    expect('_op' in p).toBe(false);
    expect(Object.keys(p)).toEqual([]);
    expect({ ...p }).toEqual({});
    expect(p.then).toBeUndefined();
    expect(JSON.stringify({ v: p })).toBe('{"v":"proxy:x"}');
  });
});

describe('generateAllowlist', () => {
  it('records every q* export from the fixture module', async () => {
    const { allowlist, errors, warnings } = await generateAllowlist({ queries, schema, app: 'web' });
    expect(errors).toEqual([]);
    expect(allowlist.generator).toBe(1);
    expect(allowlist.app).toBe('web');
    expect(allowlist.sourceHash).toBe(createHash('sha256').update(readFileSync(queries)).digest('hex'));

    const byName = Object.fromEntries(allowlist.entries.map((e) => [e.name, e]));
    const names = allowlist.entries.map((e) => e.name);

    // sorted by name, no r* / constants
    expect(names).toEqual([...names].sort());
    expect(names.some((n) => n.startsWith('r'))).toBe(false);
    expect(names).not.toContain('NOT_A_QUERY');

    // static builder
    expect(byName.qStatic).toEqual({
      name: 'qStatic',
      surql: 'SELECT id, white FROM game ORDER BY sort_index asc LIMIT 20;',
      whereMode: 'static',
    });
    // same text: deduped, first name wins
    expect(byName.qStaticTwin).toBeUndefined();

    // dynamic where: mode any, and nothing from the proxy reaches the WHERE clause
    expect(byName.qDynamicWhere).toEqual({
      name: 'qDynamicWhere',
      surql: 'SELECT * FROM game LIMIT 10;',
      whereMode: 'any',
    });

    // .one() carries LIMIT 1; toRecordId(String(proxy)) binds as a plain param
    expect(byName.qOne).toEqual({
      name: 'qOne',
      surql: 'SELECT * FROM user WHERE id = $id LIMIT 1;',
      whereMode: 'static',
    });

    // _or branches and an operator object
    expect(byName.qOrRange.surql).toBe(
      'SELECT * FROM game WHERE (white = $white__or0 OR black = $black__or1) AND created_ms >= $created_ms ORDER BY created_ms desc LIMIT 50;'
    );
    expect(byName.qOrRange.whereMode).toBe('static');

    // related subquery with a projection
    expect(byName.qRelated.surql).toBe(
      'SELECT *, (SELECT id, name FROM player_name WHERE id=$parent.white LIMIT 1)[0] AS white FROM game WHERE database = $database;'
    );

    // numeric args render as 1
    expect(byName.qWindow.surql).toBe('SELECT * FROM game ORDER BY sort_index asc LIMIT 1 START 1;');

    // proxied table name fans out over every schema table
    expect(byName['qRowById[game]'].surql).toBe('SELECT * FROM game WHERE id = $id LIMIT 1;');
    expect(byName['qRowById[player_name]'].surql).toBe('SELECT * FROM player_name WHERE id = $id LIMIT 1;');
    // user's by-id text is the same as qOne's: deduped under the earlier name
    expect(byName['qRowById[user]']).toBeUndefined();
    expect(warnings.some((w) => w.startsWith('qRowById:'))).toBe(true);

    // allowlistSamples add static entries with real predicates
    expect(byName['qDynamicWhere#0']).toEqual({
      name: 'qDynamicWhere#0',
      surql: 'SELECT * FROM game WHERE database = $database LIMIT 10;',
      whereMode: 'static',
    });
    expect(byName['qDynamicWhere#1'].surql).toBe(
      'SELECT * FROM game WHERE white = $white AND black = $black LIMIT 10;'
    );

    // every surql ends with the builder's terminator
    for (const e of allowlist.entries) expect(e.surql.endsWith(';')).toBe(true);
  });

  it('reports throwing, misfiled and async q* exports as errors', async () => {
    const { allowlist, errors } = await generateAllowlist({
      queries: join(fixtures, 'query-broken.ts'),
      schema,
    });
    expect(allowlist.entries.map((e) => e.name)).toEqual(['qFine']);
    const byName = Object.fromEntries(errors.map((e) => [e.name, e.error]));
    expect(byName.qThrows).toBe('boom');
    expect(byName.qMisfiled).toMatch(/r\*-only API called from a q\* builder: db\.useRemote\(\)/);
    expect(byName.qAsync).toMatch(/must be pure\/sync/);
  });

  it('honours allowlistSkip and allowlistWhereAny', async () => {
    const { allowlist, errors, warnings } = await generateAllowlist({
      queries: join(fixtures, 'query-skip.ts'),
      schema,
    });
    expect(errors).toEqual([]);
    expect(allowlist.entries).toEqual([{ name: 'qFine', surql: 'SELECT * FROM user;', whereMode: 'any' }]);
    expect(warnings).toContain('qThrows: skipped (allowlistSkip)');
  });
});

describe('cli', () => {
  const cli = resolve(__dirname, '../src/cli.ts');
  const run = (args: string[]) => {
    try {
      const stdout = execFileSync(process.execPath, ['--import', 'jiti/register', cli, ...args], {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      return { status: 0, stdout, stderr: '' };
    } catch (e: any) {
      return { status: e.status as number, stdout: String(e.stdout), stderr: String(e.stderr) };
    }
  };

  it('writes the JSON and exits 0', () => {
    const out = join(mkdtempSync(join(tmpdir(), 'allowlist-')), 'nested', 'allowlist.json');
    const r = run(['--queries', queries, '--schema', schema, '--out', out, '--app', 'web']);
    expect(r.status).toBe(0);
    const json = JSON.parse(readFileSync(out, 'utf8'));
    expect(json.app).toBe('web');
    expect(json.entries.length).toBeGreaterThan(5);
  });

  it('exits 1 and lists the errors for a broken module', () => {
    const out = join(mkdtempSync(join(tmpdir(), 'allowlist-')), 'allowlist.json');
    const r = run(['--queries', join(fixtures, 'query-broken.ts'), '--schema', schema, '--out', out]);
    expect(r.status).toBe(1);
    expect(r.stderr).toContain('qThrows: boom');
    expect(r.stderr).toContain('qAsync');
  });

  it('exits 1 on missing flags', () => {
    expect(run([]).status).toBe(1);
  });
});
