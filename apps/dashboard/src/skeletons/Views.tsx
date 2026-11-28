import { For, type JSX } from 'solid-js';
import { FilterMenu, SortMenu, Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { BASE } from './Ssps';

/** Views while they load: the counts, where they live and whose, then the list. */
export function ViewsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <div class="fx-head">
            <span class="cap">Live views</span>
            <span class="fx-meta cap"><span><Sk w="2em" /> shared</span><span><Sk w="2em" /> tabs</span><span><Sk w="2em" /> users</span></span>
          </div>
          <div class="fx-big"><Sk w="1.4em" /></div>
          <Sk block class="fx-canvas" />
          <div class="fx-axis cap"><Sk w="100%" /></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Costing something</span><span class="fx-meta cap"><span>now</span></span></div>
          <div class="vw-conds">
            <CondSkeleton label="Hot" note={<>updating ≥ <Sk w="3em" /></>} />
            <CondSkeleton label="Slow" note={<>p99 ≥ <Sk w="3em" /></>} />
            <CondSkeleton label="Large" note={<>≥ <Sk w="2.5em" /> rows</>} />
            <CondSkeleton label="Erroring" note="clients see stale data" />
          </div>
        </div>
      </section>

      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">By SSP</span><span class="fx-meta cap"><span><Sk w="1em" /> serving</span></span></div>
          <Sk block h={10} class="sp-share" />
          <table class="fx-list vw-mini">
            <tbody>
              <SkRepeat n={2}>
                {(i) => (
                  <tr>
                    <td><span class="fx-who"><i class="vw-key" style={{ background: 'var(--line)' }} /><Sk w="3.5em" i={i} /></span></td>
                    <td class="r"><Sk w="1.5em" i={i} /></td>
                    <td class="r" style={{ width: '18%' }}><Sk w="2em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Top users</span><span class="fx-meta cap"><span>by live views</span></span></div>
          <table class="fx-list vw-mini">
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td><Sk w={vary(i, 60, 40)} i={i} /></td>
                    <td style={{ width: '40%' }}><span class="vw-bar" /></td>
                    <td class="r" style={{ width: '12%' }}><Sk w="1em" i={i} /></td>
                    <td class="r" style={{ width: '16%' }}><Sk w="3em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Live queries</span><span class="fx-meta cap"><span><Sk w="4em" /></span></span></div>
          <div class="inc-filters">
            <FilterMenu value={{}} onChange={() => undefined} fields={[]} />
            <SortMenu options={[{ value: 'slowest', label: 'Slowest' }]} value="slowest" onChange={() => undefined} />
            <div class="inc-find">
              <input type="search" placeholder="Search the query" disabled aria-label="Search the query" />
            </div>
          </div>
          <table class="fx-list vw-list">
            <colgroup>
              <col /><col style={{ width: '15%' }} /><col style={{ width: '9%' }} /><col style={{ width: '9%' }} />
              <col style={{ width: '13%' }} /><col style={{ width: '7%' }} />
            </colgroup>
            <thead>
              <tr>
                <th class="cap">Query</th><th class="cap">User</th><th class="cap r">Rows</th>
                <th class="cap r">p99</th><th class="cap r">Updates</th><th class="cap r">Errors</th>
              </tr>
            </thead>
            <tbody>
              <SkRepeat n={6}>
                {(i) => (
                  <tr>
                    <td class="vw-q">
                      <a><Sq tone="idle" /><span class="vw-surql"><Sk w={vary(i, 240, 300)} i={i} /></span></a>
                      <span class="vw-meta cap"><Sk w={vary(i, 50, 60)} i={i} /></span>
                    </td>
                    <td><button type="button" class="vw-user" disabled><Sk w="5em" i={i} /></button></td>
                    <td class="r"><Sk w="2em" i={i} /></td>
                    <td class="r"><Sk w="3em" i={i} /></td>
                    <td class="r"><Sk w="3em" i={i} /></td>
                    <td class="r"><Sk w="1em" i={i} /></td>
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

/** One condition of the grid: its label, a count and its threshold. */
function CondSkeleton(props: { label: string; note: JSX.Element }) {
  return (
    <div>
      <span class="cap">{props.label}</span>
      <span class="vw-n zero"><Sq tone="idle" /><Sk w="0.7em" style={BASE} /></span>
      <small class="cap">{props.note}</small>
    </div>
  );
}

/** One view while it loads: the four numbers, then its query and its owner. */
export function ViewDetailSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
        <For each={['Rows', 'p99', 'Updates', 'Errors']}>
          {(label, i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span></div>
              <div class="fx-big"><Sk w="2.4em" i={i()} /></div>
              <div class="fx-sub cap"><span><Sk w="9em" i={i()} /></span></div>
            </div>
          )}
        </For>
      </section>

      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Query</span><span class="fx-meta cap"><span><Sk w="10em" /></span></span></div>
          <pre class="vw-pre"><Sk w="70%" style={BASE} /></pre>
          <div class="fx-head" style={{ 'margin-top': '18px' }}><span class="cap">Params</span></div>
          {/* One line each: a JSON object of two params, as most views carry. */}
          <pre class="vw-pre dim"><For each={['1em', '14em', '12em', '1em']}>{(w) => <div><Sk w={w} style={BASE} /></div>}</For></pre>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Owner</span></div>
          <div class="sp-kv vw-owner">
            <For each={['User', 'SSP', 'Session', 'Registered', 'Last active', 'Expires', 'Subscribers']}>
              {(label, i) => (
                <div style={label === 'Session' ? { 'grid-column': '1 / -1' } : undefined}>
                  <span class="cap">{label}</span>
                  <b><Sk w={vary(i(), 40, 50)} style={BASE} /></b>
                </div>
              )}
            </For>
          </div>
        </div>
      </section>
    </div>
  );
}
