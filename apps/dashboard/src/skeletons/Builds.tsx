import { For } from 'solid-js';
import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { ChartHead } from './Audience';

const PHASES = [
  ['queue', 'Queue'],
  ['source', 'Source'],
  ['build', 'Build'],
  ['infra', 'Infra'],
  ['migrate', 'Migrate'],
  ['rollout', 'Rollout'],
] as const;

/** Bars on the line's top: in a line-height-1 figure a centred bar makes the line taller. */
const TOP = { 'vertical-align': 'top' } as const;

/** A row of five figure tiles, as both build-time pages open with. */
function TilesSkeleton(props: { tiles: string[] }) {
  return (
    <section class="fx-row bt-tiles">
      <For each={props.tiles}>
        {(label, i) => (
          <div class="fx-cell">
            <div class="fx-head"><span class="cap">{label}</span></div>
            <span class="bt-figure" classList={{ hero: i() === 0 }}><Sk w="2.6em" i={i()} style={TOP} /></span>
            <span class="bt-delta none"><Sk w={i() === 0 ? '14em' : '9em'} i={i()} style={TOP} /></span>
          </div>
        )}
      </For>
    </section>
  );
}

/** Build time while the builds load: tiles, the chart with its phase buttons, steps and the list. */
export function BuildsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <TilesSkeleton tiles={['Push to live, median', 'Slowest 1 in 10', 'Build, median', 'Rollout, median', 'Images reused']} />

      <section class="fx-row">
        <div class="fx-cell">
          <ChartHead
            title="Push to live, build by build"
            sub={<>Each dot is one successful build, from the push until it was live. The line is the median of each build and the 3 on each side.</>}
          >
            <span><i class="dot" style={{ background: 'var(--accent-strong)' }} />One build</span>
            <span><i class="stroke" style={{ background: 'var(--accent-strong)' }} />7-build median</span>
            <span><span class="cross" aria-hidden="true">×</span>Failed</span>
            <span><span class="tri" aria-hidden="true">▲</span>Off the scale</span>
          </ChartHead>
          <div class="bt-metrics" role="group" aria-label="What to chart">
            <button aria-pressed="true" tabIndex={-1}>
              <span class="n">Push to live</span>
              <b><Sk w="3em" style={TOP} /></b>
            </button>
            <For each={PHASES}>
              {([key, label], i) => (
                <button aria-pressed="false" tabIndex={-1}>
                  <span class="n"><i style={{ background: `var(--ph-${key})` }} />{label}</span>
                  <b><Sk w="2.5em" i={i() + 1} style={TOP} /></b>
                </button>
              )}
            </For>
          </div>
          <Sk block h={342} class="bt-plot" />
          <Sk block h={97} class="bt-plot bt-stack" i={1} />
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Where the time goes, by step</span><span class="fx-meta cap"><span>Slowest first · median of the runs that did work</span></span></div>
          <div class="bt-table-wrap">
            <table class="bt-table keep-table">
              <thead>
                <tr>
                  <th>Step</th>
                  <th class="num">Median</th>
                  <th class="num opt">P90</th>
                  <th class="num">vs prior</th>
                  <th class="num opt">Skipped</th>
                  <th class="sp opt">Last 30</th>
                </tr>
              </thead>
              <tbody>
                <SkRepeat n={6}>
                  {(i) => (
                    <tr class="bt-step">
                      <td>
                        <span class="bt-step-name">
                          <i class="bt-tw none" />
                          <i class="bt-swatch" style={{ background: `var(--ph-${PHASES[i % PHASES.length][0]})` }} />
                          <span class="t"><Sk w={vary(i, 90, 70)} i={i} style={TOP} /></span>
                        </span>
                      </td>
                      <td class="num">
                        <span class="bt-median">
                          <span class="bt-med" />
                          <b><Sk w="3em" i={i} style={TOP} /></b>
                        </span>
                      </td>
                      <td class="num dim opt"><Sk w="3em" i={i} style={TOP} /></td>
                      <td class="num"><Sk w="3em" i={i} style={TOP} /></td>
                      <td class="num dim opt"><Sk w="2em" i={i} style={TOP} /></td>
                      <td class="sp opt"><svg width={92} height={22} class="bt-spark" aria-hidden="true" /></td>
                    </tr>
                  )}
                </SkRepeat>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Builds</span><span class="fx-meta cap"><Sk w="7em" /></span></div>
          <div class="bt-runs">
            <SkRepeat n={8}>
              {(i) => (
                <div class="bt-run">
                  <span class="cx-st"><Sq tone="idle" /></span>
                  <span class="bt-run-v"><b><Sk w="2.2em" i={i} style={TOP} /></b></span>
                  <span class="bt-run-what">
                    <span class="t"><Sk w={vary(i, 160, 120)} i={i} style={TOP} /></span>
                    <span class="m"><Sk w={vary(i, 90, 40)} i={i} style={TOP} /></span>
                  </span>
                  <span class="bt-run-bar"><Sk block w={vary(i, 120, 80)} h={6} i={i} /></span>
                  <span class="bt-run-total"><b><Sk w="3em" i={i} style={TOP} /></b></span>
                  <span class="bt-run-delta"><Sk w="2.5em" i={i} style={TOP} /></span>
                  <span class="bt-run-when"><Sk w="3em" i={i} style={TOP} /></span>
                </div>
              )}
            </SkRepeat>
          </div>
        </div>
      </section>
    </div>
  );
}

/** One build while it loads: its tiles and the waterfall. */
export function BuildRunSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row bt-tiles">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap"><Sk w="7em" style={TOP} /></span><span class="fx-meta cap"><Sk w="4em" style={TOP} /></span></div>
          <span class="bt-figure hero"><Sk w="2.6em" style={TOP} /></span>
          <span class="bt-delta none"><Sk w="10em" style={TOP} /></span>
        </div>
        <For each={[['Queue', 'waiting for a builder'], ['Build', null], ['Deploy', 'infra, migrations, rollout'], ['Layers cached', null]] as const}>
          {([label, sub], i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span></div>
              <span class="bt-figure"><Sk w="2.6em" i={i() + 1} style={TOP} /></span>
              <span class="bt-delta none">{sub ?? <Sk w="9em" i={i() + 1} style={TOP} />}</span>
            </div>
          )}
        </For>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head">
            <span class="cap">Waterfall</span>
            <span class="fx-meta cap">
              <span class="bt-wf-key">
                <span><i class="tick" />typical</span>
                <span><i class="hollow" />cached</span>
              </span>
            </span>
          </div>
          <div class="bt-wf" aria-hidden="true">
            <div class="bt-wf-axis">
              <span class="cap">Step</span>
              <div class="bt-track" />
              <span class="cap num">Took</span>
            </div>
            <SkRepeat n={8}>
              {(i) => (
                <div class="bt-wf-row phase">
                  <span class="bt-wf-name">
                    <i class="bt-tw none" />
                    <span class="t"><Sk w={vary(i, 60, 80)} i={i} style={TOP} /></span>
                  </span>
                  <div class="bt-track">
                    <Sk w={vary(i, 40, 160)} i={i} style={{ position: 'absolute', left: `${(i * 11) % 70}%`, top: 0, bottom: 0, margin: 'auto 0', height: 'fit-content' }} />
                  </div>
                  <span class="bt-wf-took num"><b><Sk w="3em" i={i} style={TOP} /></b></span>
                </div>
              )}
            </SkRepeat>
          </div>
        </div>
      </section>
    </div>
  );
}
