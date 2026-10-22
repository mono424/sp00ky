# `@spooky-sync/backend`: agent guide

## What this package is

The sp00ky backend SDK. Today it holds **durable workflows**: Cloudflare Workflows' API
(`WorkflowEntrypoint`, `step.do`, `step.sleep`, `step.waitForEvent`) running on sp00ky's own job
engine, so the same backend code runs on a VM, under `spky dev` and as a Cloudflare Worker backend,
with no Cloudflare Workflows. Design: `docs/durable-steps.md` in the repo; user docs:
https://sp00ky.cloud/docs/backend/durable-workflows.

## Usage

```ts
import { WorkflowEntrypoint, withWorkflows, NonRetryableError,
  type Workflow, type WorkflowEvent, type WorkflowStep } from '@spooky-sync/backend';

type Env = { INGEST: Workflow<{ id: string }> };

export class Ingest extends WorkflowEntrypoint<Env, { id: string }> {
  async run(event: WorkflowEvent<{ id: string }>, step: WorkflowStep) {
    const n = await step.do('count', { retries: { limit: 3, delay: '10 seconds' } }, async () => count(event.payload.id));
    await step.sleep('wait', '1 minute');
    return { n };
  }
}

const app = new Hono<{ Bindings: Env }>();
app.post('/start', async (c) => c.json({ id: (await c.env.INGEST.create({ params: { id: 'x' } })).id }));
export default withWorkflows(app, { INGEST: Ingest });   // on Node: serve({ fetch: handler.fetch })
```

sp00ky.yml: `apps.<backend>.workflows: [{ binding: INGEST, class: Ingest }]`, and the backend needs
`method: { type: outbox, table: ... }` (runs are jobs in that table).

## Key exports (`src/index.ts`)

- `WorkflowEntrypoint<Env, Params>`: `this.env`, `this.ctx`, `run(event, step)`.
- `withWorkflows(handler, { BINDING: Class }, options?)`: puts `env.BINDING` on every request and
  answers `POST /__spky/workflow/<BINDING>` (the job route). Options: `outbox`, `store`, `query`,
  `token`, `run` (test seams). `.env(env)` gives the bindings outside a request.
- `Workflow<Params>` binding: `create({ id?, params?, owner? })`, `createBatch`, `get(id)`;
  instance: `id`, `status()`, `terminate()`, `restart()`, `sendEvent({ type, payload })`.
  `pause()` / `resume()` throw (not supported yet).
- `NonRetryableError`, `toMs`, `retryDelayMs`, `budgetMs`.
- Stores: `SurrealStore` + `httpQuery(dbConfigFromEnv(...))` (default, from `SPKY_DB_*`), and
  `MemoryStore` for unit tests of your own workflows (it also records the jobs it would create).

## Rules agents must respect

- Code between steps must be deterministic: `Date.now()`, randomness and mutable reads go inside a step.
- Step names (with their occurrence) are the cache key; keep step order stable across replays.
- Steps run at least once: make them idempotent.
- Step results are JSON, at most 1 MiB; put big data in a bucket and return its key.
- A failed step throws a reconstructed `Error` (name + message), never the original object.

## Env it reads

`SPKY_DB_URL`, `SPKY_DB_NS`, `SPKY_DB_NAME`, `SPKY_DB_USER`, `SPKY_DB_PASS`, `SPKY_DB_LEVEL`
(injected by sp00ky), `SPKY_OUTBOX_TABLE` (injected for outbox backends), `SPKY_JOB_TOKEN`
(optional bearer the workflow route requires).

## Tests

`pnpm --filter @spooky-sync/backend test`. With `SPKY_TEST_SURREAL_URL=http://127.0.0.1:<port>`
(root/root) the SurrealDB store tests run too.
