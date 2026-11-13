import { For } from 'solid-js';
import { FilterMenu, Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { BASE } from './Ssps';

/** The 7-day axis the page opens on. */
const AXIS = ['-7d', '-6d', '-5d', '-4d', '-3d', '-2d', '-24h', 'now'];

/** Incidents while they load: the timeline and counts, then the history. */
export function IncidentsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Timeline</span><span class="fx-meta cap"><span><Sk w="1.5em" /> in 7 days</span></span></div>
          <div class="inc-lanes">
            <SkRepeat n={6}>
              {(i) => (
                <div class="inc-lane">
                  <span class="inc-lane-name mono"><Sk w={vary(i, 30, 40)} i={i} /></span>
                  <div class="inc-lane-track" />
                </div>
              )}
            </SkRepeat>
            <div class="inc-lane inc-lane-axis">
              <span />
              <div class="fx-axis cap"><For each={AXIS}>{(a) => <span>{a}</span>}</For></div>
            </div>
          </div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Open now</span><span class="fx-meta cap"><span><Sk w="6em" /></span></span></div>
          <div class="fx-big"><Sk w="0.8em" /></div>
          <div class="fx-foot inc-facts">
            <For each={['Recovered', 'Failed', 'Typical fix', 'Longest']}>
              {(label, i) => <div><span class="cap">{label}</span><b><Sk w="3em" i={i()} style={BASE} /></b></div>}
            </For>
          </div>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">History</span><span class="fx-meta cap"><span><Sk w="1.5em" /></span></span></div>
          <div class="inc-filters">
            <FilterMenu value={{}} onChange={() => undefined} fields={[]} />
            <div class="inc-find">
              <input type="search" placeholder="Search" disabled aria-label="Search incidents" />
            </div>
          </div>
          <table class="fx-list inc-list">
            <colgroup>
              <col style={{ width: '9%' }} /><col style={{ width: '14%' }} /><col style={{ width: '17%' }} />
              <col /><col style={{ width: '11%' }} /><col style={{ width: '9%' }} />
            </colgroup>
            <tbody>
              <tr class="fx-gh"><th class="cap" colSpan={6}><span class="fx-gt"><Sk w="7em" /></span><Sk w="1em" /></th></tr>
              <SkRepeat n={6}>
                {(i) => (
                  <tr>
                    <td><Sk w="2.5em" i={i} /></td>
                    <td><span class="fx-who"><Sq tone="idle" /><Sk w={vary(i, 40, 50)} i={i} /></span></td>
                    <td><Sk w={vary(i, 60, 60)} i={i} /></td>
                    <td class="inc-sum"><Sk w={vary(i, 140, 160)} i={i} /></td>
                    <td><span class="cap"><Sk w="5em" i={i} /></span></td>
                    <td class="r"><Sk w="2.5em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

/** One incident while it loads: its four facts and its event timeline. */
export function IncidentDetailSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">State</span><span class="fx-meta cap"><span><Sk w="3em" /></span></span></div>
          <div class="fx-big inc-state"><Sq tone="idle" /><span><Sk w="5em" style={BASE} /></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Duration</span><span class="fx-meta cap"><span><Sk w="4em" /></span></span></div>
          <div class="fx-big"><Sk w="2em" /></div>
          <div class="fx-sub cap"><span>since <b><Sk w="10em" /></b></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Events</span></div>
          <div class="fx-big"><Sk w="1em" /></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Peak buffered</span></div>
          <div class="fx-big"><Sk w="1.5em" /></div>
          <div class="fx-sub cap"><span>events waiting for replay</span></div>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Timeline</span><span class="fx-meta cap"><span>oldest first</span></span></div>
          <table class="fx-list inc-list inc-events">
            <colgroup><col style={{ width: '16%' }} /><col style={{ width: '12%' }} /><col /><col style={{ width: '24%' }} /></colgroup>
            <thead><tr><th class="cap">Time</th><th class="cap">State</th><th class="cap">What happened</th><th class="cap r">Version</th></tr></thead>
            <tbody>
              <SkRepeat n={4}>
                {(i) => (
                  <tr>
                    <td><Sk w="9em" i={i} /></td>
                    <td><span class="fx-who"><Sq tone="idle" /><span class="cap"><Sk w="5em" i={i} /></span></span></td>
                    <td class="inc-sum"><Sk w={vary(i, 160, 160)} i={i} /></td>
                    <td class="r"><Sk w="6em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
