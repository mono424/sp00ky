// Build gate for the wasm-free subpath entries: dist/pure.js, dist/live.js and
// dist/sw.js (`@spooky-sync/core/pure`, `/live`, `/sw`). They are imported by
// service workers, dedicated workers and Node, so across each entry and every
// chunk it reaches:
//   - no import of a wasm engine, the circuit, CRDTs or the logger;
//   - no reference to `window`, `document` or `localStorage` (a service worker
//     has none of them; an access at module top level would throw on import);
//   - `/sw` does not even pull in the `surrealdb` SDK.
// Each entry is then really imported in this Node process (no DOM globals)
// to catch top-level side effects the static scan cannot see. Prints the
// gzipped size of each entry (own code, and fully bundled with its npm
// dependencies when rolldown is reachable). Run after `tsdown` in the build.
import { readFileSync, existsSync } from 'node:fs';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';
import { createRequire } from 'node:module';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const dist = join(root, 'dist');

const ENTRIES = ['pure.js', 'live.js', 'sw.js'];
const FORBIDDEN_IMPORTS = [
  '@spooky-sync/ssp-wasm',
  '@surrealdb/wasm',
  '@sqlite.org/sqlite-wasm',
  'loro-crdt',
  'pino',
  'blurhash',
];
const FORBIDDEN_PER_ENTRY = { 'sw.js': ['surrealdb'] };
const FORBIDDEN_GLOBALS = ['window', 'document', 'localStorage'];

const importRe =
  /(?:import|export)\s*(?:[^'"`;]*?\sfrom\s*)?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)/g;

/** Blank out comments and string/template literals so identifier scans see code only. */
function codeOnly(src) {
  let out = '';
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    const n = src[i + 1];
    if (c === '/' && n === '/') {
      while (i < src.length && src[i] !== '\n') i++;
      continue;
    }
    if (c === '/' && n === '*') {
      i += 2;
      while (i < src.length && !(src[i] === '*' && src[i + 1] === '/')) i++;
      i += 2;
      continue;
    }
    if (c === '"' || c === "'" || c === '`') {
      const q = c;
      i++;
      while (i < src.length && src[i] !== q) i += src[i] === '\\' ? 2 : 1;
      i++;
      out += '""';
      continue;
    }
    out += c;
    i++;
  }
  return out;
}

function walk(entry) {
  const seen = new Set();
  const bare = new Set();
  const stack = [join(dist, entry)];
  while (stack.length > 0) {
    const file = stack.pop();
    if (seen.has(file)) continue;
    seen.add(file);
    if (!existsSync(file)) throw new Error(`${entry}: missing ${file}`);
    const src = readFileSync(file, 'utf8');
    for (const m of src.matchAll(importRe)) {
      const spec = m[1] ?? m[2];
      if (spec.startsWith('.')) stack.push(resolve(dirname(file), spec));
      else bare.add(spec);
    }
  }
  return { files: [...seen], bare: [...bare] };
}

const problems = [];
const sizes = [];

for (const entry of ENTRIES) {
  let graph;
  try {
    graph = walk(entry);
  } catch (error) {
    problems.push(String(error.message ?? error));
    continue;
  }
  const forbidden = [...FORBIDDEN_IMPORTS, ...(FORBIDDEN_PER_ENTRY[entry] ?? [])];
  for (const spec of graph.bare) {
    if (forbidden.some((f) => spec === f || spec.startsWith(`${f}/`)))
      problems.push(`${entry} imports ${spec}`);
  }
  let own = '';
  for (const file of graph.files) {
    const src = readFileSync(file, 'utf8');
    own += src;
    if (/\.wasm\b/.test(src))
      problems.push(`${entry}: ${file.slice(dist.length + 1)} references a .wasm file`);
    const code = codeOnly(src);
    for (const name of FORBIDDEN_GLOBALS) {
      // `x.window` (a property) is fine; a bare identifier is not.
      const re = new RegExp(`(?<![.\\w$])${name}(?![\\w$])`);
      if (re.test(code))
        problems.push(`${entry}: ${file.slice(dist.length + 1)} references \`${name}\``);
    }
  }
  sizes.push({
    entry,
    files: graph.files.length,
    raw: Buffer.byteLength(own),
    gzip: gzipSync(own).length,
    deps: graph.bare,
  });
}

// Import each entry for real: top-level code must run without DOM globals.
for (const entry of ENTRIES) {
  try {
    await import(pathToFileURL(join(dist, entry)).href);
  } catch (error) {
    problems.push(`${entry} fails to import in Node: ${error?.message ?? error}`);
  }
}

// Fully bundled size (entry + npm deps, minified), when rolldown is reachable
// through tsdown. Informational only.
async function bundledGzip(entry) {
  try {
    const req = createRequire(createRequire(join(root, 'package.json')).resolve('tsdown'));
    const { rolldown } = await import(pathToFileURL(req.resolve('rolldown')).href);
    const bundle = await rolldown({
      input: join(dist, entry),
      platform: 'browser',
      logLevel: 'silent',
    });
    const { output } = await bundle.generate({ format: 'esm', minify: true });
    await bundle.close();
    const code = output.map((o) => o.code ?? '').join('');
    return gzipSync(code).length;
  } catch {
    return null;
  }
}

for (const s of sizes) s.bundled = await bundledGzip(s.entry);

const kb = (n) => `${(n / 1024).toFixed(2)} kB`;
for (const s of sizes) {
  const bundled = s.bundled === null ? '' : `, bundled with deps + minified ${kb(s.bundled)} gzip`;
  console.log(
    `check-worker-entries: ${s.entry.padEnd(8)} ${kb(s.raw)} (${kb(s.gzip)} gzip, ${s.files} file${s.files === 1 ? '' : 's'}; deps: ${s.deps.join(', ') || 'none'})${bundled}`
  );
}

if (problems.length > 0) {
  for (const p of problems) console.error(`check-worker-entries: ${p}`);
  console.error(
    'The /pure, /live and /sw entries must stay free of wasm, the logger and DOM globals.'
  );
  process.exit(1);
}
console.log('check-worker-entries: ok');
