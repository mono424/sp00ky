import { McpServer, ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import type { Bridge } from './bridge.js';
import type { SurrealClient } from './surreal.js';
import { BRIDGE_METHODS } from './protocol.js';

function json(data: unknown) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }] };
}

const LOG_LEVELS = { trace: 10, debug: 20, info: 30, warn: 40, error: 50, fatal: 60 } as const;
type LogLevel = keyof typeof LOG_LEVELS;
const LEVEL_NAMES = Object.keys(LOG_LEVELS) as [LogLevel, ...LogLevel[]];

const levelName = (level: number): string =>
  (Object.entries(LOG_LEVELS) as Array<[string, number]>).reduce(
    (name, [n, v]) => (level >= v ? n : name),
    'trace'
  );

/** A captured line as the agent reads it: the JSON parsed, minus what is already a field. */
function readableLog(e: { seq: number; time: number; level: number; msg: string; line: string }) {
  let data: unknown;
  try {
    const { time: _time, level: _level, msg: _msg, ...rest } = JSON.parse(e.line);
    data = Object.keys(rest).length ? rest : undefined;
  } catch {
    data = { truncated: e.line };
  }
  return { seq: e.seq, time: new Date(e.time).toISOString(), level: levelName(e.level), msg: e.msg, data };
}

export function createServer(bridge: Bridge, surreal?: SurrealClient | null): McpServer {
  const server = new McpServer({
    name: 'sp00ky-devtools',
    version: '0.0.1',
  });

  // --- Tools ---

  server.tool(
    'list_connections',
    'List browser tabs connected to Sp00ky DevTools',
    {},
    async () => {
      return json({ connected: bridge.isConnected, tabs: bridge.getConnectedTabs() });
    }
  );

  server.tool(
    'get_state',
    'Get the full Sp00ky DevTools state (events, queries, auth, database)',
    { tabId: z.number().optional().describe('Browser tab ID (uses first connected tab if omitted)') },
    async ({ tabId }) => {
      if (!bridge.isConnected) {
        throw new Error('No extension connected. get_state requires the Sp00ky DevTools browser extension.');
      }
      const result = await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId);
      return json(result);
    }
  );

  server.tool(
    'run_query',
    'Execute a SurrealQL query against the database',
    {
      query: z.string().describe('SurrealQL query to execute'),
      target: z.enum(['local', 'remote']).optional().default('remote').describe('Query target: local or remote database'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ query, target, tabId }) => {
      if (bridge.isConnected) {
        const result = await bridge.request(BRIDGE_METHODS.RUN_QUERY, { query, target }, tabId);
        return json(result);
      }
      if (surreal) {
        const result = await surreal.query(query);
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured. Set SURREAL_URL or connect the browser extension.');
    }
  );

  server.tool(
    'list_tables',
    'List all database tables',
    { tabId: z.number().optional().describe('Browser tab ID') },
    async ({ tabId }) => {
      if (bridge.isConnected) {
        const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
        const tables = state?.database?.tables ?? [];
        return json(tables);
      }
      if (surreal) {
        const result = await surreal.query('INFO FOR DB;');
        const info = result as any[];
        const tables = info?.[0]?.result?.tables ?? info?.[0]?.tables ?? {};
        return json(Object.keys(tables));
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'get_query_rows',
    'Fetch the rows (data, localArray, remoteArray) of ONE active query by its queryHash. ' +
      'The state from get_active_queries carries counts and capped ids only.',
    {
      queryHash: z.number().describe('queryHash from get_active_queries'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ queryHash, tabId }) => {
      if (!bridge.isConnected) throw new Error('No extension connected.');
      const result = await bridge.request(BRIDGE_METHODS.GET_QUERY_ROWS, { queryHash }, tabId);
      return json(result);
    }
  );

  server.tool(
    'get_table_data',
    'Fetch records from a database table',
    {
      tableName: z.string().describe('Name of the table'),
      limit: z.number().optional().default(100).describe('Max number of records to return'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ tableName, limit, tabId }) => {
      if (bridge.isConnected) {
        const result = await bridge.request(BRIDGE_METHODS.GET_TABLE_DATA, { tableName }, tabId);
        return json(result);
      }
      if (surreal) {
        const result = await surreal.query(`SELECT * FROM \`${tableName}\` LIMIT ${limit};`);
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'update_table_row',
    'Update a record in a database table',
    {
      tableName: z.string().optional().describe('Name of the table (used when browser extension is connected)'),
      recordId: z.string().describe('Record ID to update (e.g. "users:abc123")'),
      updates: z.record(z.unknown()).describe('Fields to update'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ tableName, recordId, updates, tabId }) => {
      if (bridge.isConnected) {
        const result = await bridge.request(
          BRIDGE_METHODS.UPDATE_TABLE_ROW,
          { tableName, recordId, updates },
          tabId
        );
        return json(result);
      }
      if (surreal) {
        const result = await surreal.query(
          `UPDATE ${recordId} MERGE ${JSON.stringify(updates)};`
        );
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'delete_table_row',
    'Delete a record from a database table',
    {
      tableName: z.string().optional().describe('Name of the table (used when browser extension is connected)'),
      recordId: z.string().describe('Record ID to delete (e.g. "users:abc123")'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ tableName, recordId, tabId }) => {
      if (bridge.isConnected) {
        const result = await bridge.request(
          BRIDGE_METHODS.DELETE_TABLE_ROW,
          { tableName, recordId },
          tabId
        );
        return json(result);
      }
      if (surreal) {
        const result = await surreal.query(`DELETE ${recordId};`);
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'get_active_queries',
    'Get all active live queries: surql, params, counts, capped membership ids and timings ' +
      '(rows are not included; use get_query_rows for one query)',
    { tabId: z.number().optional().describe('Browser tab ID') },
    async ({ tabId }) => {
      if (bridge.isConnected) {
        const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
        return json(state?.activeQueries ?? []);
      }
      if (surreal) {
        const result = await surreal.query('SELECT * FROM _00_query;');
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'get_query_timings',
    'Per-query processing-time breakdown for debugging: SSP phases (parse/plan/snapshot at registration; store-apply/circuit-step/transform per ingest, plus whole-ingest `ssp` wall time), local & remote record-fetch, and frontend reconcile — each as { lastMs, p50, p90, p99, count } — sorted slowest-first. Requires the Sp00ky DevTools browser extension. The SurrealDB fallback only exposes persisted materialization percentiles (the per-phase samples are in-memory).',
    { tabId: z.number().optional().describe('Browser tab ID') },
    async ({ tabId }) => {
      if (bridge.isConnected) {
        const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
        const queries: any[] = state?.activeQueries
          ? Object.values(state.activeQueries)
          : [];
        // Rank by the dominant phases' p90 (fall back to last) so the slowest
        // query/phase surfaces first.
        const scoreOf = (t: any) =>
          ['ssp', 'localFetch', 'remoteFetch', 'frontend'].reduce(
            (s, k) => s + (t?.[k]?.p90 ?? t?.[k]?.lastMs ?? 0),
            0
          );
        const rows = (queries as any[])
          .map((q) => ({
            queryHash: q.queryHash,
            query: q.query,
            updateCount: q.timings?.updateCount ?? q.updateCount,
            errorCount: q.timings?.errorCount,
            timings: q.timings ?? null,
            _score: scoreOf(q.timings),
          }))
          .sort((a, b) => b._score - a._score)
          // oxlint-disable-next-line no-unused-vars -- strip the internal sort key
          .map(({ _score, ...rest }) => rest);
        return json(rows);
      }
      if (surreal) {
        const result = await surreal.query('SELECT * FROM _00_query;');
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'get_events',
    'Get event history, optionally filtered by type',
    {
      eventType: z.string().optional().describe('Filter by event type'),
      limit: z.number().optional().default(50).describe('Max number of events to return'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ eventType, limit, tabId }) => {
      if (bridge.isConnected) {
        const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
        let events = state?.eventsHistory ?? [];
        if (eventType) {
          events = events.filter((e: any) => e.eventType === eventType);
        }
        if (limit) {
          events = events.slice(-limit);
        }
        return json(events);
      }
      if (surreal) {
        const result = await surreal.query(
          `SELECT * FROM _00_events ORDER BY timestamp DESC LIMIT ${limit};`
        );
        return json(result);
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'get_mutations',
    "The client's writes: the outbox (pending / retrying with attempt counts), what left it since DevTools " +
      'attached (synced with the server latency, or rolled back with the server error), debounced updates, ' +
      'the tab role and connection (a follower tab never drains), and the rows of the persistent failed-writes ' +
      'tray with their payload and before image. Answers "did my write reach the server?" and "why was it rolled back?".',
    {
      status: z
        .enum(['pending', 'retrying', 'synced', 'rolled-back', 'dropped'])
        .optional()
        .describe('Only entries with this status'),
      includeTray: z.boolean().optional().default(true).describe('Also read the failed-writes tray rows'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ status, includeTray, tabId }) => {
      if (!bridge.isConnected) {
        throw new Error('No extension connected. get_mutations requires the Sp00ky DevTools browser extension.');
      }
      const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
      const mutations = state?.mutations;
      if (!mutations) {
        throw new Error('This client does not report its mutations; update @spooky-sync/core.');
      }
      const entries = status
        ? (mutations.entries ?? []).filter((e: any) => e.status === status)
        : mutations.entries;
      const failed = includeTray
        ? (
            (await bridge.request(
              BRIDGE_METHODS.PAGE_OP,
              { method: 'mutationOp', op: 'listFailed', args: {} },
              tabId
            )) as any
          )?.failed
        : undefined;
      return json({ ...mutations, entries, failed });
    }
  );

  server.tool(
    'get_logs',
    "Recent lines from the client's own logger (the page keeps its last 500), oldest first, each with its " +
      "structured fields. Pass `after` = a previous call's `head` to read only what is new. `captureLevel` " +
      "changes what the page records (e.g. 'debug') without printing more to the console; it holds for the tab's " +
      'session, so a reload records its boot at that level.',
    {
      minLevel: z.enum(LEVEL_NAMES).optional().describe('Only lines at or above this level'),
      contains: z.string().optional().describe('Only lines whose JSON contains this text (case-insensitive)'),
      after: z.number().optional().describe('Only lines after this seq (a previous `head`)'),
      limit: z.number().optional().default(100).describe('Max lines, newest kept'),
      captureLevel: z.enum(LEVEL_NAMES).optional().describe('Set what the page records from now on'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ minLevel, contains, after, limit, captureLevel, tabId }) => {
      if (!bridge.isConnected) {
        throw new Error('No extension connected. get_logs requires the Sp00ky DevTools browser extension.');
      }
      if (captureLevel) {
        await bridge.request(
          BRIDGE_METHODS.PAGE_OP,
          { method: 'logOp', op: 'setCaptureLevel', args: { level: captureLevel } },
          tabId
        );
      }
      const read = (await bridge.request(
        BRIDGE_METHODS.PAGE_OP,
        { method: 'logOp', op: 'read', args: { after: after ?? 0 } },
        tabId
      )) as any;
      const floor = minLevel ? LOG_LEVELS[minLevel] : 0;
      const term = contains?.toLowerCase();
      const entries = ((read?.entries ?? []) as any[]).filter(
        (e) => e.level >= floor && (!term || String(e.line).toLowerCase().includes(term))
      );
      return json({
        head: read?.head,
        dropped: read?.dropped,
        consoleLevel: read?.consoleLevel,
        captureLevel: read?.captureLevel,
        entries: entries.slice(Math.max(0, entries.length - (limit ?? 100))).map(readableLog),
      });
    }
  );

  server.tool(
    'get_auth_state',
    'Get the current authentication state',
    { tabId: z.number().optional().describe('Browser tab ID') },
    async ({ tabId }) => {
      if (!bridge.isConnected) {
        throw new Error('No extension connected. get_auth_state requires the Sp00ky DevTools browser extension.');
      }
      const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
      return json(state?.auth ?? null);
    }
  );

  server.tool(
    'clear_history',
    'Clear the event history',
    { tabId: z.number().optional().describe('Browser tab ID') },
    async ({ tabId }) => {
      if (!bridge.isConnected) {
        throw new Error('No extension connected. clear_history requires the Sp00ky DevTools browser extension.');
      }
      await bridge.request(BRIDGE_METHODS.CLEAR_HISTORY, {}, tabId);
      return { content: [{ type: 'text' as const, text: 'History cleared.' }] };
    }
  );

  server.tool(
    'describe_schema',
    'Describe all tables with columns, types, and sp00ky annotations (@crdt, @parent). Stitches INFO FOR DB with parsed schema metadata. With the browser extension this returns @crdt/@parent semantics; direct-DB mode returns raw column info only.',
    { tabId: z.number().optional().describe('Browser tab ID') },
    async ({ tabId }) => {
      if (bridge.isConnected) {
        const state = (await bridge.request(BRIDGE_METHODS.GET_STATE, {}, tabId)) as any;
        const dbState = state?.database ?? {};
        return json({
          source: 'extension',
          tables: dbState.tables ?? [],
          relationships: dbState.relationships ?? [],
        });
      }
      if (surreal) {
        const dbInfo = (await surreal.query('INFO FOR DB;')) as any[];
        const tablesObj =
          dbInfo?.[0]?.result?.tables ?? dbInfo?.[0]?.tables ?? {};
        const tableNames = Object.keys(tablesObj);
        const tables = await Promise.all(
          tableNames.map(async (name) => {
            try {
              const info = (await surreal.query(`INFO FOR TABLE \`${name}\`;`)) as any[];
              const fieldsObj = info?.[0]?.result?.fields ?? info?.[0]?.fields ?? {};
              const columns = Object.entries(fieldsObj).map(([fname, def]) => ({
                name: fname,
                definition: typeof def === 'string' ? def : JSON.stringify(def),
              }));
              return { name, columns };
            } catch (e) {
              return { name, columns: [], error: e instanceof Error ? e.message : String(e) };
            }
          })
        );
        return json({
          source: 'direct-db',
          note: '@crdt / @parent annotations are not visible in direct-DB mode; connect the browser extension to see them.',
          tables,
        });
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.tool(
    'lint_query',
    'Validate a SurrealQL query without running it. Sends EXPLAIN <query> through the connected channel; returns parse / plan errors with location when SurrealDB provides them.',
    {
      query: z.string().describe('SurrealQL query to validate'),
      target: z
        .enum(['local', 'remote'])
        .optional()
        .default('remote')
        .describe('When using the extension: lint against local (cache) or remote DB'),
      tabId: z.number().optional().describe('Browser tab ID'),
    },
    async ({ query, target, tabId }) => {
      const trimmed = query.trim().replace(/;\s*$/, '');
      const explainQuery = /^\s*EXPLAIN\b/i.test(trimmed) ? trimmed : `EXPLAIN ${trimmed};`;

      const parseError = (msg: string) => {
        const m = msg.match(/line\s+(\d+)(?:[,\s]+col(?:umn)?\s+(\d+))?/i);
        return {
          ok: false,
          errors: [
            {
              message: msg,
              line: m ? Number(m[1]) : undefined,
              column: m && m[2] ? Number(m[2]) : undefined,
            },
          ],
        };
      };

      const inspectResult = (raw: unknown) => {
        const arr = Array.isArray(raw) ? raw : [raw];
        const errors = arr
          .map((r: any) => (r && r.status === 'ERR' ? r.result ?? r.message : null))
          .filter(Boolean) as string[];
        if (errors.length > 0) {
          return { ok: false, errors: errors.map((m) => parseError(m).errors[0]) };
        }
        return { ok: true, plan: arr };
      };

      try {
        if (bridge.isConnected) {
          const result = await bridge.request(
            BRIDGE_METHODS.RUN_QUERY,
            { query: explainQuery, target },
            tabId
          );
          return json(inspectResult(result));
        }
        if (surreal) {
          const result = await surreal.query(explainQuery);
          return json(inspectResult(result));
        }
        throw new Error('No extension connected and no direct database configured.');
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return json(parseError(msg));
      }
    }
  );

  // --- Resources ---

  server.resource('state', 'sp00ky://state', { description: 'Full Sp00ky DevTools state' }, async (uri) => {
    if (!bridge.isConnected) {
      throw new Error('No extension connected. State resource requires the browser extension.');
    }
    const state = await bridge.request(BRIDGE_METHODS.GET_STATE);
    return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(state, null, 2) }] };
  });

  server.resource('tables', 'sp00ky://tables', { description: 'List of database tables' }, async (uri) => {
    if (bridge.isConnected) {
      const state = (await bridge.request(BRIDGE_METHODS.GET_STATE)) as any;
      const tables = state?.database?.tables ?? [];
      return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(tables, null, 2) }] };
    }
    if (surreal) {
      const result = await surreal.query('INFO FOR DB;');
      const info = result as any[];
      const tables = info?.[0]?.result?.tables ?? info?.[0]?.tables ?? {};
      return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(Object.keys(tables), null, 2) }] };
    }
    throw new Error('No extension connected and no direct database configured.');
  });

  server.resource(
    'table-data',
    new ResourceTemplate('sp00ky://tables/{tableName}', { list: undefined }),
    { description: 'Contents of a specific database table' },
    async (uri, variables) => {
      const tableName = variables.tableName as string;
      if (bridge.isConnected) {
        const result = await bridge.request(BRIDGE_METHODS.GET_TABLE_DATA, { tableName });
        return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(result, null, 2) }] };
      }
      if (surreal) {
        const result = await surreal.query(`SELECT * FROM \`${tableName}\` LIMIT 100;`);
        return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(result, null, 2) }] };
      }
      throw new Error('No extension connected and no direct database configured.');
    }
  );

  server.resource('queries', 'sp00ky://queries', { description: 'Active live queries' }, async (uri) => {
    if (bridge.isConnected) {
      const state = (await bridge.request(BRIDGE_METHODS.GET_STATE)) as any;
      const queries = state?.activeQueries ?? [];
      return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(queries, null, 2) }] };
    }
    if (surreal) {
      const result = await surreal.query('SELECT * FROM _00_query;');
      return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(result, null, 2) }] };
    }
    throw new Error('No extension connected and no direct database configured.');
  });

  server.resource('events', 'sp00ky://events', { description: 'Event history' }, async (uri) => {
    if (bridge.isConnected) {
      const state = (await bridge.request(BRIDGE_METHODS.GET_STATE)) as any;
      const events = state?.eventsHistory ?? [];
      return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(events, null, 2) }] };
    }
    if (surreal) {
      const result = await surreal.query('SELECT * FROM _00_events ORDER BY timestamp DESC LIMIT 50;');
      return { contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(result, null, 2) }] };
    }
    throw new Error('No extension connected and no direct database configured.');
  });

  return server;
}
