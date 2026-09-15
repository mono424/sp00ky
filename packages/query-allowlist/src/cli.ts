#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { generateAllowlist } from './index.js';

const USAGE =
  'usage: spooky-query-allowlist --queries <query.ts> --schema <schema.gen.ts> --out <allowlist.json> [--app <name>]';

async function main(): Promise<number> {
  let values: Record<string, string | boolean | undefined>;
  try {
    ({ values } = parseArgs({
      options: {
        queries: { type: 'string' },
        schema: { type: 'string' },
        out: { type: 'string' },
        app: { type: 'string' },
        help: { type: 'boolean', short: 'h' },
      },
      strict: true,
    }));
  } catch (e) {
    console.error(e instanceof Error ? e.message : String(e));
    console.error(USAGE);
    return 1;
  }
  if (values.help) {
    console.log(USAGE);
    return 0;
  }
  const queries = values.queries as string | undefined;
  const schema = values.schema as string | undefined;
  const out = values.out as string | undefined;
  if (!queries || !schema || !out) {
    console.error(USAGE);
    return 1;
  }

  const result = await generateAllowlist({
    queries: resolve(queries),
    schema: resolve(schema),
    app: values.app as string | undefined,
  });

  for (const w of result.warnings) console.error(`warning: ${w}`);
  if (result.errors.length > 0) {
    console.error(`${result.errors.length} query export(s) could not be recorded:`);
    for (const { name, error } of result.errors) console.error(`  ${name}: ${error}`);
    console.error('Fix them, or list the names in `export const allowlistSkip = [...]` in the query module.');
    return 1;
  }

  const outPath = resolve(out);
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(result.allowlist, null, 2) + '\n');
  console.error(`wrote ${result.allowlist.entries.length} entries to ${outPath}`);
  return 0;
}

main().then(
  (code) => process.exit(code),
  (e) => {
    console.error(e instanceof Error ? e.stack ?? e.message : String(e));
    process.exit(1);
  }
);
