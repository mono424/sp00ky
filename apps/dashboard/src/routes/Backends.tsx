import { For, Index, Show, createMemo, createResource, createSignal, onCleanup, type JSX } from 'solid-js';
import { A, useParams } from '@solidjs/router';
import { api } from '../api/client';
import { PageHead } from '../components/Chrome';
import {
  DayTicks,
  FxCell,
  Gauge,
  LineChart,
  Sheet,
  SheetSection,
  Sq,
  memText,
  useCloudServices,
} from '../components/Frame';
import { decodeParam, formatCount, formatMs, formatRelativeTime, formatStamp, formatUptime, isAbsent } from '../lib/format';
import { backendTone } from '../lib/status';
import type { BackendDetail, BackendSummary, Overview } from '../api/types';

/**
 * The backends the scheduler routes work to, for any number of them.
 *
 * First the fleet: how many answer their health check, how fast, how often
 * they are checked, and the machines behind any pool. Then one full-width row
 * per backend, trouble first: who it is and how it has been, its response time
 * over the recent checks (or, for a pool backend, its machines), and its CPU
 * and memory. Target and environment open in a sheet beside the list.
 */

const ICON = 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M4 12c0 4 16 4 16 0';
const POLL_MS = 10_000;

type Use = ReturnType<ReturnType<typeof useCloudServices>['byName']>;

/** A backend that is not answering its health check. */
const failing = (b: BackendSummary) => backendTone(b.status) === 'bad' || b.status === 'unhealthy';
const nameOf = (b: BackendSummary) => b.name ?? b.id ?? 'backend';
const toneOf = (b: BackendSummary) => (failing(b) ? 'bad' : backendTone(b.status) === 'warn' ? 'warn' : b.status === 'idle' ? 'idle' : '');

/** How long ago, by the scheduler's clock rather than the browser's. */
function agoBy(now: number) {
  return (iso: string | null | undefined) => {
    if (isAbsent(iso)) return '—';
    const t = Date.parse(iso!);
    // Never in the future: a stamp a little ahead of the clock is "just now".
    return Number.isNaN(t) ? '—' : formatRelativeTime(Date.now() - Math.max(0, now - t));
  };
}

/** Every backend's recent checks, one request each, refreshed on the probe cadence. */
function useDetails(names: () => string[]) {
  const [res, { refetch }] = createResource(
    () => names().join('\n'),
    async (key) => {
      const list = key ? key.split('\n') : [];
      const all = await Promise.all(list.map((n) => api.getResult<BackendDetail>(`/backends/${encodeURIComponent(n)}`)));
      const out: Record<string, BackendDetail> = {};
      all.forEach((r, i) => {
        if (r.ok) out[list[i]] = r.value;
      });
      return out;
    },
  );
  const timer = setInterval(() => !document.hidden && void refetch(), POLL_MS);
  onCleanup(() => clearInterval(timer));
  return (name: string) => res()?.[name];
}

export function Backends(props: { overview: Overview | undefined }) {
  const [data, { refetch }] = createResource(() =>
    api.getResult<{ backends: BackendSummary[]; check_interval_secs: number }>('/backends'),
  );
  // The scheduler's own probe cadence: polling faster shows nothing new.
  const timer = setInterval(() => !document.hidden && void refetch(), POLL_MS);
  onCleanup(() => clearInterval(timer));

  const cloud = useCloudServices(() => props.overview?.cloud_linked);
  const now = () => props.overview?.server_time_ms ?? Date.now();
  const ago = (iso: string | null | undefined) => agoBy(now())(iso);

  const list = () => {
    const r = data();
    return r?.ok ? r.value.backends : props.overview?.backends ?? [];
  };
  const interval = () => {
    const r = data();
    return r?.ok ? r.value.check_interval_secs : null;
  };
  const backends = createMemo(() => {
    const rank = (b: BackendSummary) => (failing(b) ? 0 : backendTone(b.status) === 'warn' ? 1 : 2);
    return [...list()].sort((a, b) => rank(a) - rank(b) || nameOf(a).localeCompare(nameOf(b)));
  });
  const details = useDetails(() => backends().map(nameOf).sort());

  const answering = () => backends().filter((b) => !failing(b)).length;
  const probed = () => backends().filter((b) => !b.pool && b.response_time_ms != null);
  const typical = () => {
    const v = probed().map((b) => b.response_time_ms!).sort((a, b) => a - b);
    return v.length ? v[Math.floor(v.length / 2)] : null;
  };
  const slowest = () => [...probed()].sort((a, b) => b.response_time_ms! - a.response_time_ms!)[0];
  const lastCheck = () => {
    const t = backends().map((b) => (b.last_checked ? Date.parse(b.last_checked) : 0)).filter(Boolean);
    return t.length ? new Date(Math.max(...t)).toISOString() : null;
  };
  const pools = () => backends().filter((b) => b.pool);
  const machines = () => pools().reduce((n, b) => n + b.pool!.ready + b.pool!.starting, 0);
  const queued = () => pools().reduce((n, b) => n + b.pool!.queued, 0);

  return (
    <>
      <PageHead
        title="Backends"
        actions={
          <Show when={backends().length}>
            <span class="cap">
              <b style={{ color: answering() === backends().length ? 'var(--ink-dim)' : 'var(--bad)' }}>{answering()}</b> of {backends().length} answering
            </span>
          </Show>
        }
      />

      <Show when={data()?.ok === false}>
        <section class="fx-row">
          <FxCell>
            <div class="fx-log bad">
              <span class="fx-what">{(data() as { message: string }).message}</span>
            </div>
          </FxCell>
        </section>
      </Show>

      <Show when={data() || props.overview} fallback={<div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div>}>
        <div class="fx-page">
          <Show
            when={backends().length > 0}
            fallback={
              <section class="fx-row">
                <FxCell>
                  <div class="fx-empty cap">No backends are configured</div>
                </FxCell>
              </section>
            }
          >
            {/* the fleet ====================================================== */}
            <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
              <FxCell label="Answering">
                <div class="fx-big">
                  <span classList={{ bad: answering() < backends().length }}>{answering()}</span>
                  <small>/ {backends().length}</small>
                </div>
                <div class="sp-squares">
                  <For each={[...backends()].sort((a, b) => nameOf(a).localeCompare(nameOf(b)))}>
                    {(b) => <span class={`sp-sq ${toneOf(b)}`} title={`${nameOf(b)} · ${b.status}`} />}
                  </For>
                </div>
              </FxCell>
              <FxCell label="Response">
                <div class="fx-big">
                  {typical() != null ? Math.round(typical()!) : '—'}
                  <small>ms typical</small>
                </div>
                <Show when={slowest()}>
                  {(s) => (
                    <div class="fx-sub cap">
                      <span>
                        slowest <b>{nameOf(s())}</b> <b>{formatMs(s().response_time_ms)}</b>
                      </span>
                    </div>
                  )}
                </Show>
              </FxCell>
              <FxCell label="Checks">
                <div class="fx-big">
                  {interval() ?? '—'}
                  <small>s apart</small>
                </div>
                <div class="fx-sub cap">
                  <span>
                    last <b>{ago(lastCheck())}</b>
                  </span>
                </div>
              </FxCell>
              <FxCell label="Machines">
                <div class="fx-big">
                  {pools().length ? formatCount(machines()) : '—'}
                  <Show when={pools().length}>
                    <small>in {pools().length} {pools().length === 1 ? 'pool' : 'pools'}</small>
                  </Show>
                </div>
                <div class="fx-sub cap">
                  <Show when={pools().length} fallback={<span>no backend runs on a pool</span>}>
                    <span classList={{ warn: queued() > 0 }}>
                      <b>{formatCount(queued())}</b> jobs queued
                    </span>
                  </Show>
                </div>
              </FxCell>
            </section>

            {/* one row per backend, keyed by name so open details survive a poll */}
            <For each={backends().map(nameOf)}>
              {(name) => (
                <BackendRow
                  backend={backends().find((b) => nameOf(b) === name)!}
                  detail={details(name)}
                  use={cloud.byName(name)}
                  overview={props.overview}
                  now={now()}
                  link
                />
              )}
            </For>
          </Show>
        </div>
      </Show>
    </>
  );
}

/** One backend: who it is and how it has been, its responses or machines, its CPU and memory. */
function BackendRow(props: {
  backend: BackendSummary;
  detail: BackendDetail | undefined;
  use: Use;
  overview: Overview | undefined;
  now: number;
  /** On the list: link to the backend's own page. */
  link?: boolean;
}) {
  const [open, setOpen] = createSignal(false);
  const b = () => props.backend;
  const ago = (iso: string | null | undefined) => agoBy(props.now)(iso);
  const tone = () => (toneOf(b()) || ((props.use?.memPct ?? 0) >= 80 ? 'warn' : ''));
  const mem = () => (props.use?.memBytes != null ? memText(props.use.memBytes) : null);
  const limit = () => (props.use ? memText(props.use.memLimit) : null);
  const points = () => (props.detail?.history ?? []).map((h) => ({ ts: h.at, v: h.ok ? h.ms : null }));
  const failed = () => points().filter((p) => p.v == null).length;

  return (
    <>
      <section class="fx-row bk-row" classList={{ warn: tone() === 'warn', bad: tone() === 'bad' }}>
        <div class="fx-cell sp-id">
          <div class="fx-head fx-title" style={{ 'margin-bottom': '10px' }}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
              <path d={ICON} />
            </svg>
            <Show when={props.link} fallback={<span class="fx-name">{nameOf(b())}</span>}>
              <A class="fx-name bk-link" href={`/backends/${encodeURIComponent(nameOf(b()))}`}>{nameOf(b())}</A>
            </Show>
          </div>
          <div class="fx-state cap" classList={{ warn: tone() === 'warn', bad: tone() === 'bad' }}>
            <Sq tone={tone()} />
            {toneOf(b()) === '' && (props.use?.memPct ?? 0) >= 80 ? 'memory high' : b().status}
          </div>
          <dl class="sp-facts">
            <div>
              <dt class="cap">Address</dt>
              <dd>{b().ip ? `${b().ip}${b().port ? `:${b().port}` : ''}` : '—'}</dd>
            </div>
            <div>
              <dt class="cap">{failing(b()) ? 'Healthy' : 'Checked'}</dt>
              <dd classList={{ bad: failing(b()) }}>{failing(b()) ? ago(b().last_healthy) : ago(b().last_checked)}</dd>
            </div>
            <div>
              <dt class="cap">Up</dt>
              <dd>{props.use?.since ? formatUptime((props.now - Date.parse(props.use.since)) / 1000) : '—'}</dd>
            </div>
          </dl>
          <DayTicks component={nameOf(b())} incidents={props.overview?.incidents?.recent} now={props.now} />
          <div class="sp-actions">
            <Show when={props.link}>
              <A class="btn btn-sm" href={`/backends/${encodeURIComponent(nameOf(b()))}`}>Open</A>
            </Show>
            {/* On the backend's own page the same facts are already below. */}
            <Show when={props.link}>
              <button class="btn btn-sm" aria-haspopup="dialog" onClick={() => setOpen(true)}>
                Details
              </button>
            </Show>
          </div>
        </div>

        {/* what it answers: response time over the recent checks, or its machines */}
        <Show
          when={b().pool}
          fallback={
            <div class="fx-cell bk-resp">
              <div class="fx-head">
                <span class="cap">Response</span>
                <span class="fx-meta cap">
                  <Show when={failed()}>
                    <span class="bad">{failed()} failed</span>
                  </Show>
                  <span>{b().healthcheck}</span>
                </span>
              </div>
              <div class="fx-big">
                <span classList={{ bad: failing(b()) }}>{b().response_time_ms != null ? Math.round(b().response_time_ms!) : '—'}</span>
                <small>ms</small>
              </div>
              <Show when={points().length > 1} fallback={<div class="fx-canvas fx-empty cap">No checks recorded yet</div>}>
                <LineChart points={points()} format={(v) => `${Math.round(v)} ms`} now={props.now} />
              </Show>
            </div>
          }
        >
          {(p) => (
            <div class="fx-cell bk-resp">
              <div class="fx-head">
                <span class="cap">Machines</span>
                <span class="fx-meta cap">
                  <span>pool {p().pool}</span>
                  <Show when={p().paused}><span class="warn">paused</span></Show>
                  <Show when={p().breaker_open}><span class="bad">breaker open</span></Show>
                </span>
              </div>
              <div class="fx-big">
                {formatCount(p().ready + p().starting)}
                <small>running</small>
              </div>
              <div class="bk-pool">
                <Pool label="Ready" value={p().ready} />
                <Pool label="Booting" value={p().starting} />
                <Pool label="Busy slots" value={p().busy_slots} />
                <Pool label="Queued" value={p().queued} warn={p().queued > 0} />
              </div>
              <Show when={p().error}>
                {(e) => (
                  <div class="fx-log bad" style={{ 'margin-top': '14px' }}>
                    <span class="fx-what">{e()}</span>
                  </div>
                )}
              </Show>
            </div>
          )}
        </Show>

        <div class="fx-cell sp-gauge">
          <Gauge
            label="CPU"
            of={props.use ? `${props.use.vcpus} vCPU` : undefined}
            value={props.use?.cpuPct != null ? String(Math.round(props.use.cpuPct)) : null}
            unit="%"
            pct={props.use?.cpuPct ?? null}
            series={props.use?.cpuSeries}
          />
        </div>
        <div class="fx-cell sp-gauge">
          <Gauge
            label="Memory"
            of={limit() ? `${limit()!.value} ${limit()!.unit}` : undefined}
            value={mem()?.value ?? null}
            unit={mem()?.unit}
            pct={props.use?.memPct ?? null}
            series={props.use?.memSeries}
          />
        </div>
      </section>

      <Sheet
        open={open()}
        onClose={() => setOpen(false)}
        title={
          <>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
              <path d={ICON} />
            </svg>
            {nameOf(b())}
          </>
        }
        meta={b().status}
      >
        <BackendFacts backend={b()} detail={props.detail} ago={ago} />
      </Sheet>
    </>
  );
}

function Pool(props: { label: string; value: number; warn?: boolean }) {
  return (
    <div>
      <span class="cap">{props.label}</span>
      <b classList={{ warn: props.warn }}>{formatCount(props.value)}</b>
    </div>
  );
}

/** Target, pool and environment: the sheet on the list, the lower rows on a backend's page. */
function BackendFacts(props: {
  backend: BackendSummary;
  detail: BackendDetail | undefined;
  ago: (iso: string | null | undefined) => string;
}) {
  const b = () => props.backend;
  const env = () => Object.entries(props.detail?.env ?? {}).sort((x, y) => x[0].localeCompare(y[0]));
  return (
    <>
      <SheetSection label="Target">
        <div class="sp-kv">
          <div style={{ 'grid-column': '1 / -1' }}><span class="cap">URL</span><b>{b().url}</b></div>
          <div style={{ 'grid-column': '1 / -1' }}><span class="cap">Health check</span><b>{b().healthcheck_url}</b></div>
          <div><span class="cap">Address</span><b>{b().ip ? `${b().ip}${b().port ? `:${b().port}` : ''}` : '—'}</b></div>
          <div><span class="cap">Checked every</span><b>{props.detail ? `${props.detail.check_interval_secs}s` : '—'}</b></div>
          <div><span class="cap">Last checked</span><b>{props.ago(b().last_checked)}</b></div>
          <div><span class="cap">Last healthy</span><b classList={{ bad: failing(b()) }} title={formatStamp(b().last_healthy)}>{props.ago(b().last_healthy)}</b></div>
        </div>
      </SheetSection>
      <Show when={b().pool}>
        {(p) => (
          <SheetSection label="Machine pool" meta={<span>{p().pool}</span>}>
            <div class="sp-kv">
              <div><span class="cap">Ready</span><b>{formatCount(p().ready)}</b></div>
              <div><span class="cap">Booting</span><b>{formatCount(p().starting)}</b></div>
              <div><span class="cap">Busy slots</span><b>{formatCount(p().busy_slots)}</b></div>
              <div><span class="cap">Queued jobs</span><b classList={{ warn: p().queued > 0 }}>{formatCount(p().queued)}</b></div>
              <div><span class="cap">Paused</span><b classList={{ warn: p().paused }}>{p().paused ? 'yes' : 'no'}</b></div>
              <div><span class="cap">Breaker</span><b classList={{ bad: p().breaker_open }}>{p().breaker_open ? 'open' : 'closed'}</b></div>
            </div>
          </SheetSection>
        )}
      </Show>
      <Show when={env().length}>
        <SheetSection label="Environment" meta={<span>{env().length}</span>}>
          <table class="fx-list">
            <tbody>
              <Index each={env()}>
                {(kv) => (
                  <tr>
                    <td style={{ width: '42%' }}>{kv()[0]}</td>
                    <td style={{ color: 'var(--ink)' }}>{kv()[1]}</td>
                  </tr>
                )}
              </Index>
            </tbody>
          </table>
        </SheetSection>
      </Show>
    </>
  );
}

export function BackendDetailView(props: { overview: Overview | undefined }) {
  const params = useParams<{ name: string }>();
  // The router does not decode path segments; a name with anything
  // URL-special would otherwise be encoded twice on the way to the API.
  const name = () => decodeParam(params.name);
  const [result, { refetch }] = createResource(name, (n) => api.getResult<BackendDetail>(`/backends/${encodeURIComponent(n)}`));
  const timer = setInterval(() => !document.hidden && void refetch(), POLL_MS);
  onCleanup(() => clearInterval(timer));

  const cloud = useCloudServices(() => props.overview?.cloud_linked);
  const now = () => props.overview?.server_time_ms ?? Date.now();
  const data = () => {
    const r = result();
    return r?.ok ? r.value : undefined;
  };
  const error = () => {
    const r = result();
    return r && !r.ok ? r.message : undefined;
  };

  return (
    <>
      <PageHead
        crumb="Backends"
        title={name()}
        actions={
          <Show when={data()}>
            {(b) => (
              <span class="cap fx-state" classList={{ warn: toneOf(b()) === 'warn', bad: toneOf(b()) === 'bad' }}>
                <Sq tone={toneOf(b())} />
                {b().status}
              </span>
            )}
          </Show>
        }
      />
      <Show when={!error()} fallback={<section class="fx-row"><FxCell><div class="fx-log bad"><span class="fx-what">{error()}</span></div></FxCell></section>}>
        <Show when={data()} fallback={<div class="fx-empty cap" style={{ padding: '24px' }}>Loading…</div>}>
          {(b) => (
            <div class="fx-page">
              <BackendRow backend={b()} detail={b()} use={cloud.byName(name())} overview={props.overview} now={now()} />
              <section class="fx-row bk-facts-row">
                <BackendFactsRow>
                  <BackendFacts backend={b()} detail={b()} ago={agoBy(now())} />
                </BackendFactsRow>
              </section>
            </div>
          )}
        </Show>
      </Show>
    </>
  );
}

/** The facts as cells of the page rather than sections of a sheet. */
function BackendFactsRow(props: { children: JSX.Element }) {
  return <div class="bk-facts">{props.children}</div>;
}
