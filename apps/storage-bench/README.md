# Storage benchmark

Browser benchmark of the local cache backends, on the reads and writes the client makes:

- **indexeddb**: IndexedDB used directly on the page, written for this benchmark the way a careful
  app would: one object store per table, an index on each foreign key and on the list's filter, a
  side store of versions. It is the only backend here with secondary indexes.
- **sqlite**: the client's SQLite engine (`localEngine: 'sqlite'`), SQLite-WASM in a worker on OPFS.
- **rows**: the client's row store (`localEngine: 'rows'`), in a worker on OPFS.

The two engines are driven through the client's own code: a sync-down batch is the
`BEGIN … UPSERT ONLY $id MERGE $data … COMMIT` transaction the sync layer sends, a screen is
`select(plan)` with relations. Every number is what the page waits: worker round trip and parse
included.

## Run

```sh
pnpm --filter @spooky-sync/storage-bench build
pnpm --filter @spooky-sync/storage-bench preview   # http://localhost:4410
```

Press **Run**. The page reloads itself through the steps (about 120 page loads, six minutes) and
ends on charts: every scenario is a row of bars, one per backend, with the median beside it and a
tick at the p95. Hover a bar for the detail; the same numbers are below as a table and as JSON. Use
the production build: against the dev server the fresh-page numbers measure Vite, not the stores.

**Comparing runs.** Every finished run is kept (the last eight, in localStorage). Give a run a name
before pressing Run, change the code, rebuild, run again, and pick the earlier run under *compare
with*: each bar then shows how far it moved (a green arrow down is faster).

**Shared-tabs test.** A second button, because it needs a second tab: it opens one that leads (it
holds the store and, on request, keeps writing sync-down batches) and follows it from this tab
twice, once reading through the leader and once with `followerReads: 'direct'`. Allow the popup,
or open `/?leader=1` yourself in another tab first.

## What is measured

Each backend runs alone, in page loads of its own:

1. **warm**: wipe every backend, load the dataset (20,000 games of about 1 kB, 60,000 comments,
   500 users by default), then time each read and write in one session: median and p95 over the
   runs, after a few unmeasured ones. The first call of the relation and scan queries is reported
   on its own line, since that is where a backend builds whatever it builds lazily.
2. **cold**: a fresh page opens the store and reads a window of 50 rows, the moment its script
   runs.
3. **boot**: the same as an app gets to it: after 30 ms in which the main thread is idle (the
   script is on its way) and 50 ms in which it is busy (the script runs). Both are inputs on the
   page. This is the phase to read for a real app; `cold` is the same read with neither, which no
   app with a bundle to load ever sees.
4. **reload**: the same after 200 single-row writes, one more read of the window (the client
   re-materializes a query whose rows changed) and a reload that closed nothing.

The three fresh-page phases report two times: until the page has **rows to paint from**, and until
**the read itself** answered. They are the same thing for IndexedDB and SQLite. The row store keeps
a copy of an app's startup reads where a page can read it synchronously (its paint cache), so it
has rows to paint from before its worker exists; the line below says how often that copy turned
out to differ from the read. Its two times come from alternate page loads, one that peeks and one
that does not, since a peek is main-thread work in the read's way.

The page head starts what can be started ahead of the app: the row store's worker (the client's
`ROWS_PREWARM_SNIPPET`, added by `vite.config.ts`), which opens the store and reads ahead what the
last load read first, and the IndexedDB connection.

The fresh-page read is the one a query's first render makes in the client: the rows of its render
set, by id, as one `select` the client names after the query. The row store seeds a new query with
what that read answered in the last page load (its paint cache), which is the "first rows on
screen" line.

Two more sections follow the three-way comparison: the row store's worker started with and without
the V8 compile hint (`vite.config.ts` writes a copy of the worker without it), and JSON text
against CBOR (the SurrealDB SDK's codec) as the stored form of a row.

## Reading the numbers

- The machine and the browser matter more than anything: compare columns of one run, not runs.
- `indexeddb` runs on the page, with no worker to start, which is why its read in the **cold**
  phase is the shortest: a browser starts a worker only once the page's main thread is free, and a
  store behind a worker cannot answer before that. In the **boot** phase the row store's worker
  has had the script download to start in, has opened the store and has read ahead, so the app's
  first read is answered from what is already in the page. The two engines pay for the worker; it
  is also why they never block the page.
- The row store has indexes of its own: on every field that holds record links, and on a field a
  query filters on with `=` (built the first time, which is the "first time after loading" line).
- "A user write" is each backend's durable path: a `strict` transaction on IndexedDB, the SQLite
  engine as the client configures it, a flushed batch on the row store.
