#!/usr/bin/env node
import path from "node:path";
import { Command } from "commander";
import { runVelocity } from "./suites/velocity.js";
import { runMutationMix } from "./suites/mutation-mix.js";
import { runSubqueryDepth } from "./suites/subquery-depth.js";
import { runJoinFanout } from "./suites/join-fanout.js";
import { runJoinSaturation } from "./suites/join-saturation.js";
import { runViewDensity } from "./suites/view-density.js";
import { runBacklog } from "./suites/backlog.js";
import { runIndex } from "./suites/index-rollup.js";
import { runWhitepawn } from "./suites/whitepawn.js";
import { log } from "./util/log.js";

function parseIntList(value: string): number[] {
  const out = value
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map((s) => {
      const n = Number.parseInt(s, 10);
      if (!Number.isFinite(n) || n < 0) throw new Error(`invalid integer in list: "${s}"`);
      return n;
    });
  if (out.length === 0) throw new Error("list is empty");
  return out;
}

function defaultOutDir(suite: string): string {
  return path.join(process.cwd(), "results", suite);
}

const STACK_OPTS = (cmd: Command) =>
  cmd
    .option("--scheduler-bin <path>", "prebuilt scheduler binary")
    .option("--ssp-bin <path>", "prebuilt ssp-server binary")
    .option("--surreal-image <ref>", "SurrealDB container image")
    .option("--modules-dir <path>", "path to tests/.spooky module/schema dir")
    .option("--schema <path>", "path to user schema (defaults to tests/schema.surql)")
    .option("--verbose", "stream scheduler/SSP/SurrealDB stdout");

const program = new Command();
program.name("spooky-bench").description("sp00ky performance domains").version("0.0.2");

// Domain 1.1
STACK_OPTS(
  program
    .command("velocity")
    .description("Domain 1.1, ramp concurrent ingest until p95 exceeds threshold")
    .option("--rates <list>", "comma-separated events/sec to step through", "100,200,500,1000,2000")
    .option("--rows <n>", "rows to seed", (v) => parseInt(v, 10), 5000)
    .option("--registered-queries <n>", "background views", (v) => parseInt(v, 10), 100)
    .option("--hold-secs <n>", "seconds per ramp step", (v) => parseInt(v, 10), 5)
    .option("--concurrency <n>", "max concurrent in-flight POSTs", (v) => parseInt(v, 10), 32)
    .option("--threshold-ms <n>", "p95 threshold to mark a step as over", (v) => parseInt(v, 10), 100)
    .option("--smoke", "tiny config for end-to-end verification (~30s)")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  const rates = smoke ? [50, 100] : parseIntList(opts.rates);
  await runVelocity({
    rates,
    rows: smoke ? 200 : opts.rows,
    registeredQueries: smoke ? 5 : opts.registeredQueries,
    holdSecs: smoke ? 2 : opts.holdSecs,
    concurrency: smoke ? 4 : opts.concurrency,
    thresholdMs: opts.thresholdMs,
    outDir: opts.out ?? defaultOutDir("velocity"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

// Domain 1.2
STACK_OPTS(
  program
    .command("mutation-mix")
    .description("Domain 1.2, INSERT vs UPDATE vs DELETE latency")
    .option("--rows <n>", "rows to seed", (v) => parseInt(v, 10), 1000)
    .option("--registered-queries <n>", "background views", (v) => parseInt(v, 10), 50)
    .option("--events-per-phase <n>", "mutations per phase", (v) => parseInt(v, 10), 200)
    .option("--smoke", "tiny config (~30s)")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  await runMutationMix({
    rows: smoke ? 200 : opts.rows,
    registeredQueries: smoke ? 5 : opts.registeredQueries,
    eventsPerPhase: smoke ? 20 : opts.eventsPerPhase,
    outDir: opts.out ?? defaultOutDir("mutation-mix"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

// Domain 1.3
STACK_OPTS(
  program
    .command("subquery-depth")
    .description("Domain 1.3, propagation latency vs SurQL subquery nesting")
    .option("--rows <n>", "rows to seed", (v) => parseInt(v, 10), 1000)
    .option("--ingest-events <n>", "ingests per depth", (v) => parseInt(v, 10), 200)
    .option("--row-bytes <n>", "padding bytes per comment row (grows total DB size)", (v) => parseInt(v, 10), 0)
    .option("--smoke", "tiny config")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  await runSubqueryDepth({
    rows: smoke ? 200 : opts.rows,
    ingestEvents: smoke ? 20 : opts.ingestEvents,
    rowBytes: opts.rowBytes,
    outDir: opts.out ?? defaultOutDir("subquery-depth"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

// Domain 2.1
STACK_OPTS(
  program
    .command("join-fanout")
    .description("Domain 2.1, write amplification from a 1-to-K equi-join update")
    .option("--fanouts <list>", "comma-separated fan-out factors", "1,10,100")
    .option("--iterations <n>", "update iterations per fan-out level", (v) => parseInt(v, 10), 20)
    .option("--smoke", "tiny config")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  const fanouts = smoke ? [1, 10] : parseIntList(opts.fanouts);
  await runJoinFanout({
    fanouts,
    iterationsPerStep: smoke ? 3 : opts.iterations,
    outDir: opts.out ?? defaultOutDir("join-fanout"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

// Domain 2.2
STACK_OPTS(
  program
    .command("join-saturation")
    .description("Domain 2.2, small active table joined against larger inactive (≤10k)")
    .option("--inactive-sizes <list>", "comma-separated inactive table sizes", "1000,5000,10000")
    .option("--active-size <n>", "active table size", (v) => parseInt(v, 10), 100)
    .option("--ingest-events <n>", "ingests per inactive size", (v) => parseInt(v, 10), 200)
    .option("--smoke", "tiny config")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  const inactiveSizes = smoke ? [200, 1000] : parseIntList(opts.inactiveSizes);
  await runJoinSaturation({
    inactiveSizes,
    activeSize: smoke ? 20 : opts.activeSize,
    ingestEvents: smoke ? 10 : opts.ingestEvents,
    outDir: opts.out ?? defaultOutDir("join-saturation"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

// Domain 3.2
STACK_OPTS(
  program
    .command("view-density")
    .description("Domain 3.2, sweep registered query count, observe per-view RSS")
    .option("--views <list>", "comma-separated registered view counts", "50,100,500,1000,2000")
    .option("--rows <n>", "rows to seed (held fixed)", (v) => parseInt(v, 10), 5000)
    .option("--row-bytes <n>", "padding bytes per comment row", (v) => parseInt(v, 10), 0)
    .option("--ingest-events <n>", "ingests per step", (v) => parseInt(v, 10), 200)
    .option("--smoke", "tiny config")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  const views = smoke ? [10, 50] : parseIntList(opts.views);
  await runViewDensity({
    views,
    rows: smoke ? 200 : opts.rows,
    rowBytes: opts.rowBytes,
    ingestEvents: smoke ? 10 : opts.ingestEvents,
    outDir: opts.out ?? defaultOutDir("view-density"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

// Domain 4.1
STACK_OPTS(
  program
    .command("backlog")
    .description("Domain 4.1, SSP outage and catch-up rate after restart")
    .option("--rows <n>", "rows to seed", (v) => parseInt(v, 10), 1000)
    .option("--registered-queries <n>", "background views", (v) => parseInt(v, 10), 50)
    .option("--baseline-events <n>", "events for steady-state phase", (v) => parseInt(v, 10), 100)
    .option("--baseline-rate <n>", "events/sec for steady-state", (v) => parseInt(v, 10), 50)
    .option("--outage-secs <n>", "minimum outage duration", (v) => parseInt(v, 10), 30)
    .option("--buffered-events <n>", "events to push while SSP is down", (v) => parseInt(v, 10), 5000)
    .option("--buffered-rate <n>", "events/sec during outage", (v) => parseInt(v, 10), 200)
    .option("--catch-up-timeout <n>", "max seconds to wait for lag=0", (v) => parseInt(v, 10), 60)
    .option("--smoke", "tiny config (~60s)")
    .option("--out <dir>", "output directory"),
).action(async (opts) => {
  const smoke = !!opts.smoke;
  await runBacklog({
    rows: smoke ? 200 : opts.rows,
    registeredQueries: smoke ? 5 : opts.registeredQueries,
    baselineEvents: smoke ? 20 : opts.baselineEvents,
    baselineRatePerSec: opts.baselineRate,
    outageSecs: smoke ? 2 : opts.outageSecs,
    bufferedEvents: smoke ? 100 : opts.bufferedEvents,
    bufferedRatePerSec: opts.bufferedRate,
    catchUpTimeoutSec: smoke ? 30 : opts.catchUpTimeout,
    outDir: opts.out ?? defaultOutDir("backlog"),
    smoke,
    schedulerBin: opts.schedulerBin,
    sspBin: opts.sspBin,
    surrealImage: opts.surrealImage,
    modulesDir: opts.modulesDir,
    schemaPath: opts.schema,
    verbose: opts.verbose,
  });
});

program
  .command("whitepawn")
  .description("WhitePawn-style large game libraries, live windows, and changefeed sync")
  .option("--profile <name>", "standard or large (includes 10M compact games)", "large")
  .option("--smoke", "10K compact and PGN games with short live-write phases")
  .option("--spky-bin <path>", "prebuilt spky schema-generator binary")
  .option("--scheduler-bin <path>", "prebuilt scheduler binary")
  .option("--ssp-bin <path>", "prebuilt ssp-server binary")
  .option("--surreal-image <ref>", "SurrealDB image", "surrealdb/surrealdb:v3.1.5")
  .option("--schema <path>", "custom source schema compatible with the chess fixture")
  .option("--startup-timeout-secs <n>", "startup and publication-readiness deadline", Number, 3600)
  .option("--verbose", "stream child process output")
  .option("--out <dir>", "output directory (default: timestamped results directory)")
  .action(async (opts) => {
    if (!["standard", "large"].includes(opts.profile))
      throw new Error(
        "WhitePawn profile must be standard or large; use --smoke for smoke validation"
      );
    await runWhitepawn({
      profile: opts.smoke ? "smoke" : opts.profile,
      outDir:
        opts.out ??
        path.join(
          process.cwd(),
          "results",
          `whitepawn-${new Date().toISOString().replace(/[:.]/g, "-")}`
        ),
      startupTimeoutSecs: opts.startupTimeoutSecs,
      spkyBin: opts.spkyBin,
      schedulerBin: opts.schedulerBin,
      sspBin: opts.sspBin,
      surrealImage: opts.surrealImage,
      schemaPath: opts.schema,
      verbose: opts.verbose,
    });
  });

// Cross-suite rollup
program
  .command("index")
  .description("Roll up the latest summary per suite into results/index.md")
  .option("--results-root <dir>", "directory holding suite output", "./results")
  .action(async (opts) => {
    await runIndex({ resultsRoot: opts.resultsRoot });
  });

program.parseAsync(process.argv).catch((e) => {
  log.error(e instanceof Error ? e.stack ?? e.message : String(e));
  process.exit(1);
});
