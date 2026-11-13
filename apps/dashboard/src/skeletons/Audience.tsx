import { For, type JSX } from 'solid-js';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const HOURS = Array.from({ length: 24 }, (_, h) => h);
/** Bars on the line's top: in a line-height-1 figure a centred bar makes the line taller. */
const TOP = { 'vertical-align': 'top' } as const;

/** One chart's head, as the Audience and build-time pages draw it. */
export function ChartHead(props: { title: JSX.Element; sub: JSX.Element; children: JSX.Element }) {
  return (
    <div class="bt-chart-head">
      <div>
        <h2 class="bt-title">{props.title}</h2>
        <p class="bt-sub">{props.sub}</p>
      </div>
      <div class="bt-legend" aria-hidden="true">{props.children}</div>
    </div>
  );
}

/** Audience while the report loads: the tiles, the three charts, the week and the quiet hours. */
export function AudienceSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row bt-tiles">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap"><Sk w="7em" style={TOP} /></span><span class="fx-meta cap"><Sk w="3em" style={TOP} /></span></div>
          <span class="bt-figure hero"><Sk w="1.4em" style={TOP} /></span>
          <span class="bt-delta none"><Sk w="14em" style={TOP} /></span>
        </div>
        <For each={['Online on average', 'Busiest moment', 'Views registered', 'Views per person']}>
          {(label, i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span></div>
              <span class="bt-figure"><Sk w="1.6em" i={i() + 1} style={TOP} /></span>
              <span class="bt-delta none"><Sk w="8em" i={i() + 1} style={TOP} /></span>
            </div>
          )}
        </For>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <ChartHead
            title="People online"
            sub="Signed-in users and signed-out sessions with a live view. A tab that closes leaves within a minute; one that dies takes up to 10."
          >
            <span><i class="stroke" style={{ background: 'var(--accent-strong)' }} />7-day average</span>
            <span><i class="stroke au-faint" style={{ background: 'var(--accent-strong)' }} />Measured</span>
            <span><i class="au-band" />Peak</span>
          </ChartHead>
          <Sk block h={230} class="bt-plot" />
          <div class="au-sub-chart">
            <h2 class="bt-title">Views registered</h2>
            <Sk block h={150} class="bt-plot" i={1} />
          </div>
          <div class="au-sub-chart">
            <h2 class="bt-title">Views per person</h2>
            <Sk block h={110} class="bt-plot" i={2} />
          </div>
        </div>
      </section>

      <section class="fx-row au-week">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">When people are online</span><span class="fx-meta cap"><Sk w="12em" /></span></div>
          {/* The grid is the same for every project, so it is drawn for real, every hour unmeasured. */}
          <div class="au-heat">
            <div class="au-heat-grid" aria-hidden="true">
              <span />
              <For each={HOURS}>{(h) => <span class="au-hour">{h % 3 === 0 ? String(h).padStart(2, '0') : ''}</span>}</For>
              <For each={DAYS}>
                {(name) => (
                  <>
                    <span class="au-day">{name}</span>
                    <For each={HOURS}>{() => <span class="au-cell none" />}</For>
                  </>
                )}
              </For>
            </div>
            <div class="au-heat-foot">
              <span class="au-read">Hover an hour to read it</span>
              <span class="au-scale" aria-hidden="true">
                <span>Empty</span>
                <i />
                <span><Sk w="5em" /></span>
              </span>
            </div>
          </div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Quietest hours</span></div>
          <ol class="au-quiet">
            <SkRepeat n={3}>
              {(i) => (
                <li>
                  <span class="bt-flag">{i + 1}</span>
                  <b><Sk w={vary(i, 110, 40)} i={i} style={TOP} /></b>
                  <span class="dim"><Sk w="10em" i={i} style={TOP} /></span>
                </li>
              )}
            </SkRepeat>
          </ol>
          <p class="au-note">The hours to aim backups and upgrades at. Nothing is scheduled from this yet.</p>
        </div>
      </section>
    </div>
  );
}
