# Machine pools: jobs that run on their own, autoscaled machines

Status: Phases 1 and 2 BUILT on branch `feat/machine-pools`, verified locally
end to end (Phase 2 offline only, never against real cloud). Phases 3 and 4 are
still plan. See "As built" right below;
where it differs from the original design further down, "As built" wins.

## Related: dedicated machines (2026-09-22)

`machines:` + `runOn: { machine }` is a separate feature that shares only the
deploy body's placement keys and the CLI's `PoolProvider`: one always-on backend
on one fixed Hetzner VM, owned by the control plane end to end (no scheduler, no
pool engine, no agent), reached through a forwarder container on the core host
that carries the backend's name. CLI side: `apps/cli/src/pool_config.rs`
(`MachineConfig`, `RunOnConfig` as `pool` XOR `machine`, `cloud_machine_manifests`).
Control plane side: `spooky-cloud` `docs/17-dedicated-machines.md`. User docs:
`docs/cloud/dedicated-machines`.

## As built (Phase 1)

What exists, all with tests:

| Piece | Where |
|---|---|
| Wire types agent <-> scheduler | `packages/pool-protocol` |
| Engine: sizing, machine lifecycle, assignment, reclaim, agent entry points | `packages/pool-core` (44 tests, incl. a chaos simulation that checks the invariants after every step, run against an embedded SurrealDB with the shipped DDL) |
| DDL `_00_pool`, `_00_machine` | `apps/cli/src/pool_tables.surql`, applied with the other internal tables |
| `pools:` + `runOn:` config, validation, `_00_pool` sync | `apps/cli/src/pool_config.rs`, `pool_sync.rs` |
| `spky dev`: builds pool backend images, gives the scheduler the docker socket + pool env, removes pool machines on exit | `apps/cli/src/dev.rs` |
| Scheduler host: sweep, token-authenticated pool listener, docker provider, admin API + MCP tools, job kill routing | `apps/scheduler/src/pool_engine.rs`, `pool_docker.rs`, `admin/pools.rs` |
| Agent (5.7 MB static binary) | `apps/agent` |

Decisions that changed while building:

- **Plain HTTP long-poll, not a WebSocket.** `POST /pool/v1/hello|ready|poll|result`.
  Every poll reply is DERIVED from database rows at that moment (a job bound to
  the machine that the agent does not report running becomes `Assign`; a job it
  reports that is no longer bound becomes `Cancel`). There is no per-agent
  command queue, so a scheduler restart loses nothing and a lost reply is simply
  re-derived. Polls are held up to 10s and woken by the sweep.
- **No binding table, and no new job fields.** A pool job is bound to a machine
  by the outbox row itself: `status = 'processing'` and `assignee = <machine id>`,
  under the row's existing lease and `lease_epoch`. Occupancy is a query. The
  `machine` / `executor` fields in section 5 were not needed.
- **The agent wraps the backend's own command** (`spky-agent -- <cmd>`), mounted
  into the unchanged backend image from a volume. No docker-in-docker, no
  sidecar. `recycle: job` restarts the backend PROCESS between jobs.
- **Machine tokens are HMACs of the machine id** under `SPKY_POOL_SECRET` (falls
  back to `SPKY_AUTH_SECRET`): stateless, survive a scheduler restart.
- **A failed machine still passes through `terminating`** (with `failure = true`)
  so its provider destroy is retried until it sticks, and only then is filed
  `failed`. Creates stop at 2x ceiling + 1 live machines, so failing destroys
  cannot become a runaway bill.
- Pools are addressed by `name` in SQL: hyphenated record keys do not survive a
  round trip through `type::record()`.

Scheduler environment: `SPKY_POOL_ENABLED` (default on), `SPKY_POOL_PORT` (9669),
`SPKY_POOL_PUBLIC_URL` (the listener as a MACHINE sees it), `SPKY_POOL_SECRET`,
`SPKY_POOL_DOCKER` (enables the docker provider), `SPKY_POOL_DOCKER_NETWORK`,
`SPKY_POOL_AGENT_IMAGE`. Build the agent image with
`docker build -f apps/agent/Dockerfile -t mono424/spooky-agent:dev .`

Verified on a real stack (SurrealDB 3.0.5 server, scheduler from source, docker
provider, agent in a real container): job pending to running on a freshly
spawned machine in ~2s; idle machine drained and removed after its idle timeout;
machine killed mid-job, detected after one lease, job requeued under a new epoch
and finished on a new machine; scheduler killed for 8s mid-job, job finished
under the SAME epoch with zero retries (adopted, not restarted); operator kill
cancels on the machine and the backend is restarted; two jobs packed on a
2-slot machine while the buffer refills.

Worth knowing when using it:

- A schedule or workflow whose job runs longer than an hour on a pool needs its
  own `deadline:`; the default run deadline is 3600s and would close the run
  while the job is still going. The engine's job kill does reach pool jobs.
- Pools need `mode: cluster` (the scheduler runs them).
- v1 is one backend per pool.

## As built (Phase 2, 2026-09-21)

The `hetzner` provider exists on both sides, verified offline end to end. It has
NOT been deployed and has never touched the real Hetzner API.

- **Scheduler** (`apps/scheduler/src/pool_cloud.rs`): registered whenever the
  scheduler is linked to a control plane. It asks over the existing internal,
  per-project-secret route family, sends the machine token, the pool listener's
  public URL and the agent image, and never the backend's environment.
- **CLI**: every deploy sends `pools` (provider, machine shape, ceiling) and marks
  pool backends with `run_on`; cloud pool rows get their image, command, working
  directory and resolved environment from the deploy's own backend manifests. A
  pool whose backend is not part of this deploy (`--only` something else) is left
  untouched rather than written half-empty.
- **Control plane** (`spooky-cloud`, branch `feat/machine-pools`,
  `docs/16-machine-pools.md`): machines API, caps enforced a second time, Hetzner
  client, cloud-init bootstrap, reaper, project-destroy hook, machine-hours usage,
  pool backends kept off the core host, pool listener published at
  `<slug>-pool.<domain>`. Off until `HetznerCloudToken` is set.

Offline end to end (real scheduler, real Go handler behind the real
project-secret middleware, the generated bootstrap script actually executed the
way cloud-init would): job pending to success in ~10s on a machine that did not
exist before; idle machine destroyed through the control plane with its
machine-hours recorded; the backend's secret never appeared in the user-data.

Go-live audit (2026-09-20), everything the offline run could not see. Found and
fixed: the pool hostname had no DNS record (the zone has no wildcard, so machines
could never have resolved their scheduler); the default machine type was from a
line Hetzner discontinued in January 2026; a type the provider does not know came
back as a 500 the scheduler retries forever, now a `409 rejected` carrying the
provider's message, and a location that does not offer a type is walked past like
a full one. Agent image: `docker-publish.yml` now builds `mono424/spooky-agent`
(amd64 + arm64, its own cache scope) under the same tag as the scheduler, and
stamps that tag into the scheduler image as `SPKY_RELEASE_VERSION`; the scheduler
names its agent by it (the crate version is only the fallback, since a manually
dispatched build carries a tag of its own). Dispatching that workflow by hand
pushes an immutable tag and moves neither `canary` nor `latest`, which is the way
to put an unreleased scheduler on one test project without rolling every cluster.

First live run (staging, 2026-09-21, scheduler and agent images `pools-rc1`): a
throwaway project with one `hetzner` pool (`cx33`, min 0, max 1, idle timeout 2m)
and one hand-triggered 20 second job. VM requested 07:45:55 UTC, agent ready
07:47:00 (about 65s), job assigned 07:47:12 under lease epoch 1, success 07:47:23
with the backend's result, machine gone after the idle timeout, machine-hours
billed once. Then a project destroyed under a second machine mid-job: the control
plane took the VM down before it deleted the project. Before the first success
the safety net got a real workout: four creates rejected by Hetzner over a missing
SSH key came back as `409 rejected`, the machine rows failed with the reason, and
the breaker opened after three, with nothing created.

What the live run found on this side, all fixed:
- The sync event generator emitted sub-path field definitions as object keys
  (`errors[*]: $after.errors[*]`), a parse error that fails the WHOLE internal
  schema, so `_00_pool` never installed and the pool sync had nowhere to write.
  The stock outbox template of `spky api add` defines `errors[*]`, so every
  freshly scaffolded backend hit it on its first deploy.
- The same template's plain `DEFINE FIELD errors[*]` fails on SurrealDB 3.1 with
  "The field 'errors.*' already exists" (3.1 defines the element of an
  `array<object>` by itself), which killed `spky migrate create`. OVERWRITE now.
- A scheduler whose database has no pool tables (every existing project right
  after an image upgrade, and any git-linked project, since that path never
  installs new internal tables) logged a sweep error every two seconds. The sweep
  goes dormant instead: one info line, a look every 60s, back the moment the
  tables appear. `pools-rc1` does NOT have this; build an rc2 before promoting.
- A fresh project needs `spky migrate create init` before its first deploy, or its
  own tables never exist SCHEMAFULL. `spky init` and `spky api add` both need a
  TTY even with `--yes`, so an agent has to write the project by hand.

Second finding (2026-09-22): the control plane pushes a pool backend to the
scheduler's `/backends` list like any other (`http://{name}:{port}`), and at zero
machines nothing listens there, so the prober said `unreachable` and opened a
`backend_down` incident for a backend that was resting exactly as designed.
Fixed scheduler-side, so no control plane change is needed: every pool sweep pass
reports what it saw of each pool (`TickReport.pools`), the sweep writes that into
the backend health cache (`maintenance::set_pool_backing`), the prober skips a
backend a pool has claimed, and the incident recorder never opens `backend_down`
for one. Statuses: `idle` (zero machines, nothing queued), `starting`, `healthy`,
`unhealthy` (breaker open or pass failed; the pool's own incident says why).

Still open for Phase 2: git-linked deploys (they do not send `pools` / `run_on`
or write `_00_pool` rows yet), and a pool backend's env lacks the runtime-injected
database credentials a core-host backend gets.

Not built yet: `spky pools` / `spky machines` CLI commands (the admin API and MCP
tools exist), machine stats beyond a version string, and everything in Phases 3
to 4.
First consumer: the WhitePawn server-side broadcast renderer (one long-running
render job per live output, about 1 core at 720p, 2 to 3 cores at 1080p).

## 1. Goal

A job can declare that it must run on a dedicated machine from a named pool.
sp00ky owns the whole lifecycle:

- no free machine: spawn one, run the job there, including jobs that run for hours;
- job done: the machine is released (reused as warm capacity or destroyed);
- `sp00ky.yml` declares the pool: fixed size or autoscaled, a baseline (`min`),
  a ceiling (`max`), and a warm `buffer` of ready machines above what is in use;
- it works unchanged with schedules and workflows, because those only create
  outbox rows and the pool layer picks the rows up;
- it must be hard to break: no lost jobs, no double runs, no leaked machines,
  no runaway spend.

Non-goals for v1: priorities, GPU machines, multi-region placement policy,
moving the ordinary always-on backends off the core host (the Nomad split
covers that separately).

## 2. What exists today (verified in code)

sp00ky job runtime:

- A job is one row in an app-owned outbox table. States: `pending`,
  `processing`, `success`, `failed`. The machine is a set of guarded updates in
  `packages/ssp-node/src/jobs/runner.rs`.
- Claim = CAS on `status='pending'` plus a lease (`lease_until`) and a fencing
  token (`lease_epoch`). Every later write is fenced on the epoch.
  There is no heartbeat and no lease renewal: lease = job timeout + 30s, capped
  at 24h, so a dead runner on a 6h job is only noticed after 6h.
- Dispatch = one HTTP `POST {base_url}{path}` held open until the backend
  answers. Default timeout 10s. A long job means a connection held open for
  hours across hosts. A 202 is recorded as `success` immediately.
- Routing is `outbox table -> one base_url` via `SPKY_JOB_CONFIG`. No notion of
  placement, capacity, runner registration, or more than one target per table.
- Per-table concurrency lives in `_00_job_policy`; the outbox table is the
  queue, drained oldest first (`packages/ssp-node/src/jobs/dispatcher.rs`).
- The cluster scheduler (`apps/scheduler`) is a singleton. Double dispatch is
  prevented by database CAS, not coordination. It already has vestigial
  `/job/dispatch` and `/job/result` routes (`job_scheduler.rs`), and an admin
  API + MCP tool table (`admin/mcp.rs`).
- Schedules and workflow steps only create ordinary outbox rows
  (`packages/schedule-core`). Runs are reaped at `run_deadline_secs`
  (default 3600), which matters for hour-long jobs.

spooky-cloud control plane:

- Hetzner Cloud only. One control-plane box; on staging every tenant container
  (SurrealDB, scheduler, SSPs, backends) runs on that one box under the Docker
  runtime, sharing one NIC. No egress accounting or shaping.
- The Hetzner API is used only through Pulumi/SST at infra-deploy time. There
  is no runtime VM creation: no `hcloud-go`, no autoscaler, no replicas above 1
  for backends, no idle shutdown, no warm pool.
- `internal/runtime/runtime.go` (`Runtime`, `AppSpec`) is the seam for running
  things. Nomad adapter exists but is opt-in and not live.
- Images are tarballs on the control-plane disk, `docker import`ed. Hetzner
  Object Storage (S3 API, `minio-go`) is already used for backups.
- `usage_events` exists but nothing writes to it. Billing is flat per plan.
- The scheduler already calls the control plane (`SPKY_CLOUD_API_URL` +
  `SPKY_AUTH_SECRET`) for `cloud_restart`. That is the precedent for a
  tenant-to-control-plane API.

## 3. Design in one picture

```
 tenant SurrealDB                tenant scheduler (singleton)            spooky-cloud control plane        Hetzner
 ----------------                ----------------------------            --------------------------        -------
 outbox rows (jobs)  <--claim/--  PoolController (pool-core)  --HTTPS-->  MachineManager                --> create /
 _00_pool                lease    - computes desired size                 - owns the provider token        destroy VM
 _00_machine         <--state--   - assigns job <-> machine               - quotas, plan caps, budget
                                  - renews leases on heartbeat            - machines table (billing truth)
                                  - reaps                                 - independent safety reaper
                                        ^
                                        | wss, OUTBOUND from the machine, per-machine token
                                  spky-agent on the machine
                                        | localhost POST, held open (today's backend contract)
                                  user container (e.g. renderer)
```

Three decisions carry the design:

1. **Machines dial out, nothing dials in.** The agent on each machine opens an
   authenticated WebSocket to the tenant scheduler and receives its job over
   it. Machines need no inbound ports, no Tailscale, no service discovery. The
   Hetzner firewall for pool machines is deny-all inbound.
2. **The scheduler executes pool jobs, not the SSPs.** It is already the
   singleton and the only recovery authority in cluster mode, so job-to-machine
   assignment has exactly one writer. SSPs skip pool tables entirely.
3. **The tenant never holds cloud credentials.** The scheduler asks the control
   plane for machines; the control plane enforces caps and runs its own reaper,
   so a buggy or dead tenant scheduler cannot leak machines or overspend.

## 4. Configuration

```yaml
pools:
  render:
    provider: hetzner          # hetzner | docker (container on the core host; default in dev)
    machine:
      type: cpx32              # or resources: { vcpus: 4, memory: 8GB }
      locations: [fsn1, nbg1]  # ordered fallbacks
    slots: 1                   # concurrent jobs per machine; 1 = one job owns the machine
    min: 1                     # baseline that always runs
    autoscale: true            # false = fixed size (= min); jobs wait for a free machine
    max: 8                     # required when autoscale is true
    buffer: 2                  # ready, idle machines kept ABOVE what is in use
    idleTimeout: 10m           # how long surplus idle capacity may linger
    recycle: job               # job = fresh container per job | never
    maxJobDuration: 8h         # hard stop for one attempt
    maxLifetime: 24h           # hard stop for one machine
    lease: 90s                 # failover speed vs tolerance of scheduler restarts

apps:
  renderer:
    type: backend
    runOn: { pool: render }    # the only new key on an app
    deploy: { dockerfile: ./Dockerfile, context: ., port: 8080, healthcheck: /health }
    method: { type: outbox, table: render_job, schema: ./src/outbox/render.surql }
```

The three modes the feature must support fall out of three keys:

| Mode | Keys | Behaviour |
|---|---|---|
| Fixed size | `autoscale: false`, `min: N` | N machines always. Extra jobs stay `pending` until one frees up. `buffer`/`max` rejected by validation. |
| Baseline + scale on demand | `autoscale: true`, `min: N`, `buffer: 0`, `max: M` | A job with no free machine waits only as long as a machine takes to boot. |
| Warm buffer | `autoscale: true`, `buffer: B`, `max: M` | Always B ready machines above usage, so jobs start instantly; the buffer refills in the background. |

Sizing rule, evaluated every tick:

```
capacity = busy_slots + queued_jobs + buffer * slots
needed   = ceil(capacity / slots)
desired  = autoscale ? clamp(max(min, needed), min, max) : min
supply   = machines in {requested, provisioning, booting, ready, busy}
```

`queued_jobs` counts only due, non-quarantined `pending` rows of the pool's
tables. Machines still booting count as supply, so a burst never over-spawns.

Validation in the CLI (`apps/cli/src/backend.rs`): `max` required with
autoscale, `min <= max`, `runOn.pool` must exist, pools require
`mode: cluster`, a pool backend must have an outbox `method`.

## 5. Data model (tenant DB, platform-owned)

`_00_pool:<name>`: normalized spec + `spec_hash`, operator fields (`paused`,
`max_override`), breaker state. Written at deploy by `schedule_sync.rs` the same
way `_00_schedule` is: spec fields only, operator fields preserved.

`_00_machine:<pool>_<ulid>`:

| Field | Meaning |
|---|---|
| `pool`, `provider_id`, `image_digest`, `location`, `type` | identity |
| `state` | `requested -> provisioning -> booting -> ready -> busy -> draining -> terminating -> gone`, or `failed` |
| `slots_total`, `slots_busy`, `jobs[]` | occupancy |
| `last_seen` | agent heartbeat, DB clock |
| `created_at`, `ready_at`, `paid_until` | boot metrics and billing-aware scale-down |
| `fail_reason`, `boot_attempt` | diagnostics |

Job rows gain two platform-injected fields (via `schema_builder.rs`, like
`lease_until` today): `machine` (record link, set for the attempt) and
`executor` (`'pool'`). The four job states stay as they are: a job waiting
for a machine is simply `pending`, which keeps every existing tool working.

Control-plane Postgres gets a `machines` table (project, pool, provider id,
created/destroyed, type). This is the billing and leak-detection source of
truth, independent of the tenant DB.

## 6. The protocols

### 6.1 Scheduler to control plane (pull capacity)

`POST /v1/projects/{id}/pools/{pool}/machines` with an `Idempotency-Key`
(= the `_00_machine` id), body `{type, locations, image, labels}`. Returns
`{machine_id, provider_id, state}`. Also `GET` (list by project/pool) and
`DELETE`. Auth is the existing scheduler-to-cloud secret. The control plane:

- enforces `max` again, plus the plan cap and an account-wide cap;
- creates the VM from a base snapshot (Debian + Docker + `spky-agent`) with
  labels `spky-project`, `spky-pool`, `spky-machine`, deny-all-inbound firewall;
- puts only a **single-use bootstrap token** (10 minute expiry) and the
  scheduler URL into cloud-init user-data. No secrets in user-data;
- uploads the pool backend's image tarball to object storage at deploy time and
  hands the machine a short-lived presigned URL, so image pulls never touch the
  core host's NIC.

### 6.2 Agent to scheduler (outbound WebSocket)

New authenticated listener on the scheduler ("pool port"), routed by Traefik as
`<slug>-pool.<domain>`. Machine tokens are JWTs signed by a per-project key the
control plane holds; the scheduler verifies them offline with the public key.

1. `hello {bootstrap_token}` -> scheduler exchanges it (via control plane) for a
   machine token, sends the container spec: image URL, cmd, env including
   resolved vault secrets. Secrets travel only over this TLS channel.
2. Agent loads the image, starts the user container, probes `healthcheck`,
   sends `ready`. Machine goes `booting -> ready`.
3. `assign {job, epoch, path, payload, deadline}` -> agent does
   `POST http://127.0.0.1:{port}{path}` and **holds it open locally**. The user
   backend keeps today's contract: handle a POST, return when the work is done.
   The fragile cross-host held-open connection is gone.
4. Agent heartbeats every 10s with `{machine, jobs:[{job, epoch}], cpu, mem,
   net_tx, net_rx}`. Each ack renews the job lease (`lease_until = now + lease`)
   fenced on the epoch. This is the lease renewal the job system lacks today.
5. Local POST returns -> `result {job, epoch, status, body}` -> scheduler writes
   the terminal state with the existing fenced helpers. With `recycle: job`
   the agent recreates the container before reporting `ready` again.
6. `cancel {job}` (from `job kill`, a deadline, or a drain) -> agent closes the
   local request, SIGTERMs the container, reports `{code: "cancelled"}`.

Rules that make it safe:

- **Lost contact.** No ack for `lease - 30s`: the agent stops the job itself.
  It therefore always stops before the scheduler may reclaim the row, so a
  network partition cannot produce two live runs.
- **Reconnect.** The agent presents `(job, epoch)`. Row still `processing` with
  that epoch: adopted, lease extended. Epoch moved on: agent is told to abort.
- **Scheduler restart.** On boot the controller waits one `lease` before
  reclaiming pool jobs, so agents reconnect and are adopted instead of killed.
  `lease` is the knob: 90s fails over fast, 5m rides out longer scheduler
  outages at the cost of slower failover.

### 6.3 Assignment (one writer, one transaction)

For each free slot on a `ready` machine running the current image digest, take
the oldest due `pending` row and run one SurrealDB transaction: claim the job
(the existing CAS, `status='processing'`, `lease_epoch + 1`, `machine = $m`) and
occupy the slot (`WHERE state IN ['ready','busy'] AND slots_busy < slots_total`).
If either CAS loses, THROW and abort both. Then send `assign`.

Retries reuse the existing policy (`max_retries`, backoff). The binding is per
attempt: a retry goes back to `pending` and gets whatever machine is free.

SSP side: pool backends are left out of `SPKY_JOB_CONFIG` (built in
spooky-cloud `internal/vms/specs.go`) and the dispatcher plus both recovery
sweeps skip `executor = 'pool'` tables. Two lines of filter, no new behaviour.

## 7. The controller: a pure, testable core

New crate `packages/pool-core`, modelled on `schedule-core`: no I/O, ports for
`Db`, `MachineProvider`, `AgentLink`, `Clock`. Hosted by
`apps/scheduler/src/pool_engine.rs` next to `schedule_engine.rs`. It is
level-triggered: every pass recomputes from rows, so a crash at any point
resumes correctly.

`tick_pass` (every 2s, plus a wake on changefeed events for pool tables):

1. **observe**: load pool specs, machines, queue depth, agent liveness.
2. **land**: terminal jobs free their slot; failed heartbeats mark machines.
3. **assign**: section 6.3.
4. **scale up**: `desired - supply` creates, capped per tick (default 5),
   deterministic ids as idempotency keys, fallback across `locations`.
5. **scale down**: surplus idle machines older than `idleTimeout`, preferring
   the one closest to `paid_until` (Hetzner bills by the started hour, so an
   idle machine is free warm capacity until its hour is up; verify the billing
   granularity before relying on this). Busy machines are never selected.
6. **roll**: after a deploy changes `image_digest`, idle old-digest machines
   drain and get replaced first; busy ones finish their job, bounded by
   `maxJobDuration`. New jobs only land on the current digest.
7. **reap**: `bootTimeout` exceeded, heartbeat lost past `lease`,
   `maxLifetime` or `maxJobDuration` exceeded -> cancel/terminate.
8. **reconcile provider**: list provider machines by label; anything not in a
   live `_00_machine` row is destroyed; any row whose VM vanished is failed.

## 8. Failure matrix

| Failure | What happens |
|---|---|
| Machine dies mid-job | Heartbeats stop, lease expires (<= `lease`), row reclaimed with `epoch + 1`, retried on another machine, dead VM destroyed. Late writes from the zombie are fenced out. |
| Partition, machine alive | Agent stops the job at `lease - 30s`, before reclaim. At most one live run. |
| Scheduler crash or redeploy | State is all in rows. Agents reconnect with backoff and are adopted during the boot grace. No job is restarted. |
| Control plane down | Running jobs unaffected (that path never touches it). Scale up/down pauses with backoff and an incident. The buffer absorbs the gap. |
| Hetzner API errors, capacity, rate limit | Exponential backoff with jitter, next location, per-pool circuit breaker. Jobs stay `pending`, never lost. |
| Machine never becomes ready | `bootTimeout` -> destroy -> respawn. K of the last N boots failing pauses scale-up and raises an incident (same idea as `quarantineAfter`), so a broken image cannot burn money in a loop. |
| Poison job that kills machines | Bounded by `max_retries`, then `failed`. |
| Leaked VM | Three independent layers: controller reconcile (step 8), control-plane reaper (no approved heartbeat for 10 min, or past `maxLifetime`), agent dead-man switch (no scheduler for 15 min -> power off, reaper deletes). |
| Runaway scaling | `max` in yaml, re-checked by the control plane, under a plan cap, under an account cap. Per-tick create cap. Only due, non-quarantined rows count as demand. |
| Double spawn | Deterministic machine id + idempotency key. |
| Clock skew | Every lease and timeout uses the DB clock, as today. |

Invariants, asserted by a simulation test that injects random crashes, drops
and provider errors into `pool-core` (the repo already gates `schedule-core` at
100% coverage; hold this crate to the same bar):

1. A job attempt is bound to at most one live machine per epoch.
2. Live machines never exceed `max`.
3. No machine stays non-terminal without a heartbeat longer than `lease` + reap interval.
4. With a healthy provider, every due `pending` job eventually runs.
5. Surplus idle capacity is eventually destroyed.
6. Provider machines are a subset of live rows plus in-flight creates.

## 9. Worker architecture and bandwidth

Default for cloud pools is `provider: hetzner`: every pool machine is its own
VM with its own NIC and its own included traffic, so heavy jobs cannot starve
SurrealDB, the scheduler or the SSPs on the core host, and worker egress (RTMP
to Twitch/YouTube, HLS upload) leaves directly from the worker.
`provider: docker` stays available for light jobs and is the dev default.

Two things the pool feature does NOT fix and that need their own decision:

- **Viewer bandwidth.** A render worker sends one encoded stream out (roughly
  0.1 to 4 Mbps). The expensive direction is HLS delivery to viewers, which
  today is served by the relay on the core host: viewers x bitrate on one NIC.
  HLS segments are immutable, so put them behind the CDN (or write them to
  object storage) before worrying about worker count.
- **Provider account limits.** Hetzner projects have a default server limit.
  Raise it before launch; the account-wide cap in the control plane should sit
  just under it.

Later, the same `MachineManager` can grow Nomad clients for the ordinary
backends. That is out of scope here but is why the provider is an interface.

## 10. Operations and billing

- Admin API + MCP tools + CLI: `pools list/get`, `pool pause/resume`,
  `pool scale --max`, `machines list`, `machine drain`, `machine kill`.
  Job tools gain the `machine` link and a derived "waiting for a machine" hint.
- Metrics and incidents: queue wait p50/p95, boot time p50/p95, utilization,
  machines by state, spawn failures, breaker open, cost per hour.
- Metering: the control plane's `machines` table yields machine-seconds per
  project and type; agents report egress bytes. This is the first real writer
  of `usage_events`, and the basis for usage-based pricing of pools.

## 11. Delivery plan

**Phase 0: prerequisites (whitepawn, days)**
- Measure the Linux software-render path in a container at 1, 2 and 4 CPUs to
  pick the machine type.
- Build a slim, self-contained renderer runtime image (AOT bundle, embedder,
  ffmpeg, fonts). Today's container compiles at start and needs repo bind
  mounts, so it cannot run anywhere else.
- Exit: `docker run` with env only streams a live game; cold start under 10s.

**Phase 1: sp00ky core, local only (no cloud)**
- Config structs + validation, `_00_pool` / `_00_machine` DDL, deploy sync.
- `pool-core` with the `docker` provider, `spky-agent`, the pool port, the
  assignment transaction, lease renewal, SSP skip filter, run-deadline override
  for schedules/workflows that target a pool.
- Simulation + invariant tests; `spky dev` runs pools with local containers.
- Exit: fixed, baseline and buffer modes pass end to end locally; killing the
  agent, the container or the scheduler mid-job never loses or doubles a job.

**Phase 2: spooky-cloud**
- `MachineManager` with `hcloud-go`, `machines` table, machines API, bootstrap
  and machine tokens, base snapshot, object-storage image delivery, firewall,
  control-plane reaper, caps, Traefik route for the pool port.
- Exit: staging tenant spawns and destroys real VMs; leak test (kill the
  scheduler for 20 minutes) ends with zero orphan VMs.

**Phase 3: hardening**
- Rolling image updates, billing-aware scale-down, breaker and incidents,
  admin UI/MCP/CLI, metering into `usage_events`, location fallback.
- Chaos run on staging: random VM deletion, scheduler restarts, control-plane
  outage, provider 5xx injection. All six invariants hold.
- Optional: `slots > 1`, per-tenant golden snapshots for ~25s boots.

**Phase 4: WhitePawn hosted renderer**
- `renderer` backend with `runOn: { pool: render }`; `POST /render` blocks
  until the stream ends (broadcast row `enabled = false` / `ended_at`, which the
  relay's stale-broadcast sweep already sets) or a cancel arrives.
- New `render_job` outbox table (the old `broadcast_job` is in an inconsistent
  migrated state; do not revive it). A DB event on `broadcast` enqueues one job
  per requested format when a hosted broadcast is enabled, following the
  existing `statistics_enqueue` pattern. Deterministic job ids per session and
  format make the enqueue idempotent.
- Worker reports `state` and `hls_url` on the broadcast row. Plan gating and a
  per-user concurrent limit sit in the enqueue event.
- Start with `min: 0, buffer: 1, max: 5`, single 720p output.

## 12. Open decisions

1. Agent pull model (recommended) versus SSPs pushing to machines over
   Tailscale. Pull needs no inbound networking and one fewer moving part.
2. After a job: recreate the container and keep the VM until its paid hour ends
   (recommended, cheaper and warmer) versus destroying the VM immediately.
3. `lease` default: 90s (fast failover) or 5m (tolerates longer scheduler
   outages). Live streams argue for the longer value.
4. Naming: `pools` + `runOn`, or something closer to existing vocabulary.
