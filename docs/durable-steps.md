# Durable steps: Cloudflare Workflows' API on sp00ky's own job engine

Backend code that uses `WorkflowEntrypoint`, `step.do`, `step.sleep` and
`step.waitForEvent` runs unchanged on a VM, under `spky dev` and on Cloudflare
(as a Worker backend), without Cloudflare Workflows. The engine is a small
replay loop inside the backend SDK; the queue is the backend's own outbox
table, so retries, kill, recovery and concurrency are the job runner's.

Status: **first cut built 2026-10-22** (branch `claude/dazzling-allen-c8e32c`,
not released): SDK, tables, job headers, admin, CLI config, docs, example.
Not built yet: synced views over the tables (§5.3), `pause()`/`resume()`,
non-TypeScript SDKs, a dashboard page for instances. Related:
[`cloudflare-native.md`](cloudflare-native.md) §6 (jobs, schedules and
workflows on the Cloudflare SSP) and §7 (Worker backends).

---

## 1. Why

- Cloudflare projects move to Workers for Platforms, and Cloudflare Workflows
  may not be available inside a WfP namespace. A Workflow class bound in the
  project script (today's `worker.workflows`, run through
  `@cloudflare/dynamic-workflows`) is also Cloudflare-only: the same backend on
  a VM or under `spky dev` has no `step.do`.
- sp00ky already has a durable queue with retries, leases, a recovery sweep,
  kill and per-table concurrency on every host (the outbox job runner in
  `packages/ssp-node/src/jobs`, in the SSP and in the SSP Durable Object).
  Durable steps need nothing more from the platform than "call me again,
  possibly later".
- Progress lives in ordinary SurrealDB tables, readable under the project's
  permissions and listed and cancelled by the admin API.

## 2. Developer API

TypeScript first, in `@spooky-sync/backend` (`packages/backend`). The names and
shapes are Cloudflare's (`cloudflare:workers` / `cloudflare:workflows`):

```ts
import { WorkflowEntrypoint, NonRetryableError, withWorkflows,
  type WorkflowEvent, type WorkflowStep, type Workflow } from '@spooky-sync/backend';

type Env = { INGEST: Workflow<IngestParams> };

export class IngestMaterial extends WorkflowEntrypoint<Env, IngestParams> {
  async run(event: WorkflowEvent<IngestParams>, step: WorkflowStep) {
    const pages = await step.do('count pages', { retries: { limit: 3, delay: '10 seconds', backoff: 'exponential' }, timeout: '2 minutes' },
      async () => countPages(event.payload.materialId));
    for (let from = 1; from <= pages; from += 10) {
      await step.do(`convert ${from}`, async () => convert(from));
    }
    await step.sleep('cool down', '1 minute');
    const approval = await step.waitForEvent('approval', { type: 'approved', timeout: '1 day' });
    return { pages, by: approval.payload.by };
  }
}

const app = new Hono<{ Bindings: Env }>();
app.post('/materials/:id/index', async (c) => {
  const instance = await c.env.INGEST.create({ params: { materialId: c.req.param('id') } });
  return c.json({ id: instance.id, status: await instance.status() });
});

export default withWorkflows(app, { INGEST: IngestMaterial });
```

| Surface | Behaviour |
|---|---|
| `WorkflowEntrypoint<Env, Params>` | `this.env`, `this.ctx`, `run(event, step)` |
| `event` | `{ payload, timestamp: Date, instanceId }` |
| `step.do(name, [config], fn)` | memoized; `config = { retries: { limit, delay, backoff: 'constant' \| 'linear' \| 'exponential' }, timeout }`, Cloudflare's defaults (5 retries, 10 s, exponential, 10 minutes); `fn` gets `{ attempt }` |
| `step.sleep(name, duration)` / `step.sleepUntil(name, date)` | durable timers |
| `step.waitForEvent(name, { type, timeout })` | resolves with `{ type, payload, timestamp }`; throws on timeout (default 24 h) |
| `NonRetryableError` | fails the step at once; the error reaches `run()` |
| `env.X.create({ id?, params, owner? })`, `createBatch`, `get(id)` | `owner` is a sp00ky extension (§5.3) |
| `instance.id`, `status()`, `terminate()`, `restart()`, `sendEvent({ type, payload })` | `status()` answers `{ status, error?, output? }` with Cloudflare's status names |
| `pause()` / `resume()` | not in the first cut (throw) |

Durations take Cloudflare's forms: a number of milliseconds or
`'<n> <unit>'` with second, minute, hour, day, week (singular or plural).

**Migrating code written for Cloudflare Workflows** is two code changes and one
config change:
1. the import: `cloudflare:workers` becomes `@spooky-sync/backend` for
   `WorkflowEntrypoint`, `WorkflowEvent`, `WorkflowStep`, and
   `cloudflare:workflows` for `NonRetryableError`;
2. the default export: `export default withWorkflows(app, { INGEST: IngestMaterial })`;
3. sp00ky.yml: the binding moves from `worker.workflows` to the backend's
   `workflows` (§7).

The wrapper is the one change beyond the import because nothing else can
build `env.INGEST` on Node, where `env` is the app's own object, and nothing
else can mount the route the job runner calls (§4). It works the same on all
three hosts, so the code has one shape everywhere.

### 2.1 Rules (the same as Cloudflare's)

- **Code between steps must be deterministic.** `run()` is replayed from the
  top on every job; only `step.*` results are stored. `Date.now()`,
  `Math.random()`, `crypto.randomUUID()` and reads of mutable state belong
  inside a step. Branching on a step's result is fine.
- **Step names are the cache key.** Name and occurrence: the second
  `step.do('poll')` in one run is a different step from the first, so a loop
  may reuse a name, but the order of calls must not change between replays.
- **Steps run at least once.** A crash between a step's side effect and its
  stored result runs the step again; make steps idempotent (upserts, keys
  derived from the instance id and step name).
- **Step results are JSON.** A step returns what `JSON.stringify` keeps: a
  `Date` comes back as its ISO string, `undefined` fields disappear, `Map` and
  class instances do not survive. The first run and every replay see the same
  JSON round trip, so behaviour cannot differ between them.
- **Large outputs go to a bucket, not the row.** A step result, the params and
  the output are capped at 1 MiB of JSON (Cloudflare's step limit). Store files
  and big blobs in a bucket (`type::file(...)`, R2) and return the key.

## 3. Storage

Three root-written tables, defined by the CLI with the other `_00_*` tables
(`apps/cli/src/schedule_tables.surql`, applied by `migrate::apply_internal_schema`
on VMs and `schema_builder::build_server_schema` on Cloudflare):

### `_00_wf_instance` (one row per instance, id `_00_wf_instance:<instance id>`)

| Field | Meaning |
|---|---|
| `binding`, `class` | the binding it was created through, and its class (for the admin) |
| `outbox`, `path` | where its jobs go: the backend's outbox table and `/__spky/workflow/<binding>` |
| `params`, `output`, `error` | JSON; `error` is `{ name, message }` |
| `status` | `queued`, `running`, `waiting`, `complete`, `errored`, `terminated` |
| `owner` | optional record; the select permission (§5.3) |
| `wake` | the token of the one job allowed to run the instance next (§4.2) |
| `wake_at` | when that job is due (a sleep, a retry backoff, an event timeout) |
| `lease_epoch`, `lease_until` | the run lease and its fencing token (§4.3) |
| `steps_done`, `current_step` | progress for a UI |
| `created_at`, `started_at`, `updated_at`, `finished_at` | |

### `_00_wf_step` (one row per step, id `_00_wf_step:[<instance key>, '<name>#<n>']`)

`instance`, `name`, `n` (occurrence), `kind` (`do`, `sleep`, `event`),
`status` (`success`, `failed`, `retrying`, `sleeping`, `waiting`), `attempts`,
`output`, `error`, `next_at` (retry, wake or timeout time), `event_type`,
`started_at`, `finished_at`, `updated_at`. Indexed on `instance`.

### `_00_wf_event` (events sent to an instance)

`instance`, `type`, `payload`, `step` (the step that consumed it, NONE until
then), `created_at`. An event sent before the instance waits for it is kept
and consumed by the first matching `waitForEvent`, oldest first.

## 4. Engine

### 4.1 The job envelope

`create()` writes the instance row and one outbox job in one transaction:

```json
{ "path": "/__spky/workflow/INGEST",
  "payload": { "instance": "_00_wf_instance:3f2a...", "wake": "9c1e44d0a1b2c3d4" },
  "delay": 0, "max_retries": 10, "retry_strategy": "exponential" }
```

The job id is `<outbox>:dwf_<wake>`. The SSP posts the payload to the backend
like any job, with the backend's bearer token and three headers the runner now
sends with every job: `x-spky-job-id`, `x-spky-job-attempt` and
`x-spky-job-timeout-ms` (the request deadline the runner enforces). The
wrapper answers the route; the app never sees it.

### 4.2 One valid job per instance: the wake token

Every job that may run an instance carries the instance's current `wake`
token. Whoever schedules the next run (the end of a run, `sendEvent`,
`restart`) replaces the token and creates the job in the same transaction, so
an older job (a retried job whose run already moved the instance on, a sleep
timer superseded by an event) finds a different token and answers 200 with
nothing to do. Duplicate continuation chains cannot form, and the token is
128 random bits, so a forged request to a public VM backend cannot wake an
instance early.

### 4.3 The run lease

A job **claims** the instance: `lease_epoch + 1`, `lease_until = now + 30 s`,
`WHERE wake = $wake AND status IN [queued, running, waiting] AND lease is free`.

| Claim outcome | Answer |
|---|---|
| claimed | run (below) |
| instance gone or finished | 200 `done` |
| token differs | 200 `stale` |
| lease held by a live run | **503 `busy`**: the outbox retries the job (exponential, 10 attempts, about 7 minutes in all) |

While it runs, the wrapper renews the lease every 10 s. Every write a run makes
(a step result, the end of the run) is a transaction that first checks
`lease_epoch` and throws `spky:fenced` if it moved, so a run whose lease was
taken over (it hung past 30 s without renewing, or `terminate()` bumped the
epoch) stops at its next write and writes nothing.

The 503 is what recovers a crashed run: the process died, its job request
failed, the outbox retries the same job (same token) and the claim succeeds as
soon as the dead run's lease expires (30 s).

### 4.4 Replay

```
claim (one round trip, also reads the DB clock)
load every _00_wf_step row of the instance (one round trip)
new Class(ctx, env).run(event, step), raced against a "suspend" signal
  step.do(name):  key = name#n
    stored success  -> return the stored JSON
    stored failed   -> throw the stored error
    retrying, due later, or budget spent -> suspend (never resolves)
    otherwise       -> execute with the step timeout
      ok            -> persist success (fenced) -> return the JSON round trip
      error, retries left -> persist retrying {attempts, error, next_at}
                             wait inline if the delay is under 2 s, else suspend at next_at
      error, none left or NonRetryableError -> persist failed, throw to run()
  step.sleep: first visit persists sleeping {next_at}; a visit at or after next_at persists success
  step.waitForEvent: first visit persists waiting {type, next_at = timeout};
    a stored unconsumed event of that type is claimed and becomes the step's result;
    past next_at the step fails with a timeout error
run() returned  -> instance complete, output (fenced)
run() threw     -> instance errored, error (fenced)
suspended at T  -> new wake token + job with delay T - now; status waiting (or running when T is now)
```

**Suspension** is a promise that never resolves: user `try/catch` around a step
never sees it (ingest's `catch` that writes "mark failed" keeps working), and
the abandoned `run()` is garbage collected after the request ends.

**Time budget.** A run keeps executing steps until a budget is spent, then
suspends with "due now" so the next job continues. The budget is
`x-spky-job-timeout-ms` minus 20% (at least 1 s of margin), capped at 60 s,
and 8 s when the header is missing (the runner's 10 s default). Replayed steps
are free; at least one step executes per run. A step that started inside the
budget still has to finish inside the job timeout (on Cloudflare the
request is cancelled at the deadline), so split long work into steps and raise
the backend's `deploy.timeout` for long single steps.

**Clock.** Every due time is in the database's clock: the claim reads
`time::now()` and the run corrects its own clock by the difference, because
the job runner decides "due" with `created_at + delay <= time::now()` on the
database. A sleep never wakes early by the backend's clock skew.

**Events while running.** `sendEvent` stores the event and replaces the wake
token with a job due now. If a run holds the lease, that job answers 503 and
retries; the run, when it ends, sees the token changed under it and schedules
itself "due now" instead of at its timer, so the event is seen at once.

**Parallel steps.** `Promise.all([step.do(a), step.do(b)])` executes both; the
run waits for every executing step to settle before it suspends.

### 4.5 Limits

| Limit | Value |
|---|---|
| Step result, params, output | 1 MiB of JSON each |
| Steps per instance | 10,000 (every run loads every step row) |
| Step name | 256 characters |
| Instance id | `^[a-zA-Z0-9_][a-zA-Z0-9-_]*$`, at most 100 characters (Cloudflare's rule) |
| Sleep / event timeout | any duration; the job `delay` is an int of milliseconds |
| Concurrency | the outbox table's (`method.concurrency`), shared with the backend's other jobs |

### 4.6 Failure modes

| What happens | Effect |
|---|---|
| Process dies while a step runs | the job request fails; the outbox retries it; the claim waits out the 30 s lease (503s) and the replay runs the step again |
| Process dies between a step and its persist | the step runs again (at least once) |
| Process dies after the persist, before the next job is written | the retried job replays to the same place and goes on |
| Two jobs for one instance at once | one claims, the other gets 503 or `stale`; never two runs at once |
| A run hangs without renewing its lease | after 30 s another job may claim; the hung run's writes are fenced |
| `terminate()` mid-run | epoch bumped: the run stops at its next write; the instance stays `terminated` |
| The database is unreachable | the handler answers 5xx; the outbox retries with its backoff |
| The outbox gives up (10 failed attempts, e.g. the backend was down for minutes) | the instance stays `running` or `waiting` with no job; `restart()` or the admin `retry` creates a new token and job |
| A user-owned outbox column the workflow job does not set is required | the CREATE fails (same constraint as `schedules:`); make it `option<...>` |
| A step longer than the job timeout | VM: the SSP retries a request the backend is still serving; the replay is fenced or stale, the original run persists and continues. Cloudflare: the request is cancelled, the step retries until its retries run out |
| A long sleep across an SSP restart or a Durable Object eviction | Cloudflare: a sleep over 30 s never waits in memory; the job wake fires when the delayed row is due (about a second late) and the recovery sweep runs it. VM: the in-memory timer is lost on restart; the recovery sweep picks the due row up (up to its 60 s interval late) |
| `sendEvent` committing while a run hands over | the store request loses the write race and is repeated (4 times, jittered); the run then sees the new token and schedules itself now |

## 5. Visibility and control

### 5.1 Admin

`schedule_core::admin` gains `list_instances` (filters: binding, status),
`instance_detail` (the row and its steps) and `cancel_instance` (= terminate:
status `terminated`, epoch bumped, token replaced), served by both admin hosts
(`packages/ssp-node/src/admin.rs` and the scheduler) at
`GET /admin/api/durable/instances`, `GET /admin/api/durable/instances/{id}`,
`POST /admin/api/durable/instances/{id}/cancel`, with MCP tools
`durable_instances_list`, `durable_instance_get`, `durable_instance_cancel` in
`ssp_protocol::admin_mcp`. A continuation job's key starts with `dwf_`, so the
jobs list classifies it as workflow work.

### 5.2 From the backend

`env.X.get(id).status()`, `terminate()`, `restart()`, `sendEvent()`.

### 5.3 From clients

The instance and step rows are readable by their `owner`
(`PERMISSIONS FOR select WHERE owner != NONE AND owner = $auth.id`; steps
check `instance.owner`). `create({ params, owner: 'user:abc' })` sets it; an
instance without an owner is root-only. Writes are root-only (the backend's
own login). A client can `LIVE SELECT` its instance on SurrealDB directly.

Synced views over these tables (`useQuery` through the SSP) are the next
step and not in the first cut: they need the tables in
`ssp_protocol::SYNCED_META_TABLES`, ingest-notify events like
`_00_user_feature`'s (`apps/cli/src/sp00ky.rs`), client meta tables and a
client hook. Until then the recommended way to show progress in an app is the
one talktobolo uses: write it to the app's own synced rows from inside the
steps.

## 6. Hosts

| Host | How the job reaches the class | DB access |
|---|---|---|
| VM | the SSP's job runner POSTs `baseUrl + /__spky/workflow/<binding>` | `SPKY_DB_*` from the orchestrator |
| `spky dev` | same, against the dev process | `SPKY_DB_*` injected by `spky dev` |
| Cloudflare (Worker backend) | `worker://<name>/__spky/workflow/<binding>` through `host.mjs` into the backend's default export | `SPKY_DB_*` in the backend's env (control plane d22240a) |

No host needs platform support beyond the outbox; `host.mjs` is unchanged.
The SDK talks to SurrealDB over HTTP `/rpc` with a session token (one sign-in
per isolate, binds sent as `LET` statements, as `HttpSqlDb` does); a backend
can pass its own `query` function instead.

## 7. Config

```yaml
apps:
  api:
    type: backend
    method: { type: outbox, table: job }
    workflows:                    # durable workflows on sp00ky's engine (every host)
      - { binding: INGEST, class: IngestMaterial }
    worker:
      workflows:                  # Cloudflare Workflows (Cloudflare only, kept for now)
        - { binding: LEGACY, class: OldFlow }
```

`apps.<name>.workflows` is a new key on the backend, valid on every hosting
mode. The CLI:
- requires `method: outbox` with a `table` (the queue);
- validates binding names (`^[A-Za-z_][A-Za-z0-9_]*$`), class names, and
  uniqueness across `workflows`, `worker.workflows` and `worker.ai`;
- injects `SPKY_OUTBOX_TABLE=<table>` into the backend's env on every host,
  which the SDK reads to know where `create()` puts jobs.

`worker.workflows` keeps meaning Cloudflare Workflows through
`@cloudflare/dynamic-workflows`, so talktobolo keeps working; a separate
session decides whether Workers for Platforms can host them. Whether
`worker.workflows` later becomes an alias for `workflows` is that decision.

## 8. Relation to the `workflows:` DAG

| | `workflows:` (top level) | durable steps (`apps.<name>.workflows`) |
|---|---|---|
| Defined in | sp00ky.yml, a static DAG | backend code, any control flow |
| A step is | one outbox job, any backend, any route | a function call inside one backend |
| Data between steps | `payload.steps.<name>` (the job results) | ordinary variables, replayed |
| Engine | `schedule_core` in the SSP / scheduler | the SDK in the backend |
| Started by | a schedule, `spky workflows run`, the admin | `env.X.create()` |
| Tables | `_00_workflow_run`, `_00_step_run` | `_00_wf_instance`, `_00_wf_step`, `_00_wf_event` |

Both use the outbox and its retries. The DAG fits fixed, cross-backend
pipelines on a schedule; durable steps fit imperative logic inside one backend
(loops over pages, polling with sleeps, waiting for a human). A DAG step can
start a durable workflow by calling a route that calls `create()`.

## 9. Tests

Results 2026-10-22: SDK 32 tests (engine over the memory store; SQL store
against SurrealDB 3.1.5 and 3.3.4 in Docker), `schedule-core` 203 (admin list,
cancel, retry, and the retention prune over the real DDL), CLI 627, ssp-server
36, ssp-node 161 plus its integration tests, ssp-protocol 72.

`spky dev` with `example/api` (`ThreadDigest`: load a thread, count words with
a flaky first attempt, sleep 3 s, write a title suggestion), on a dev stack
built from this branch with shifted ports so it ran beside another project's:
a job to `/digest` created the instance; run 1 executed two steps (the flaky
one retried inline after 1 s) and handed the 3 s sleep to a job with
`delay: 2997`; run 2 finished; the suggestion was on the thread 5.4 s after
the first job was created. An instance cancelled through the SSP's admin API
mid-sleep stayed `terminated`, its delayed job answered `done`, and no later
step ran. The run surfaced four dev bugs, fixed on the same branch: the
example outbox's `assigned_to` was still required in its migrations; a
host-binary dev SSP was handed `host.docker.internal` URLs it cannot resolve;
a `devOnly` backend's outbox got no ingest events (jobs waited for the
recovery sweep); and the VM SSP dropped the admin API's query string, so every
admin list filter was ignored.

**Manual check on a plain `spky dev`** (default ports, with no other dev stack
running): in `example/sp00ky.yml` swap `surrealdb:` to
`{ namespace: main, database: example }` and drop the `web`, `mailpit` and
`worker` apps and the dart `clientTypes` entry (restore the file afterwards),
build `ssp-server` (`cargo build -p ssp-server`), run `spky dev -y
--apply-migrations` in `example/`, then create a thread and
`CREATE job SET path = '/digest', payload = { id: '<thread id>', flaky: true }`
as root. Within about 5 s `_00_wf_instance` shows the instance `complete` with
four steps, the thread has `title_suggestion = '<title> (<n> words)'`, and
`GET /admin/api/durable/instances` on the SSP (bearer `mysecret`) lists it.

The plan the tests follow:

- SDK unit tests over an in-memory store with the same CAS semantics: replay,
  retries with backoff, budget hand-off, sleeps, events, the wake token, the
  lease (busy, stale, takeover and fencing), a crash between a step and its
  persist, parallel steps, size limits.
- SDK store tests against a real SurrealDB (3.1.5 and 3.3.4 in Docker) for the
  SQL: claim, fenced persist, finish, events, terminate.
- `spky dev` with `example/api`: a small multi-step workflow (steps, a retry, a
  sleep) started from a route, watched to `complete` in `_00_wf_instance`.
