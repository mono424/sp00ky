import { For, type JSX } from 'solid-js';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { BASE, GaugeCellSkeleton, IdentitySkeleton, SquaresSkeleton } from './Ssps';

const ICON = 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6 M4 12c0 4 16 4 16 0';

/** Backends while they load: the fleet row and two backend rows. */
export function BackendsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Answering</span></div>
          <div class="fx-big"><Sk w="0.8em" /><small>/ <Sk w="0.8em" /></small></div>
          <SquaresSkeleton n={2} />
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Response</span></div>
          <div class="fx-big"><Sk w="1.2em" /><small>ms typical</small></div>
          <div class="fx-sub cap"><span>slowest <b><Sk w="9em" /></b></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Checks</span></div>
          <div class="fx-big"><Sk w="1.2em" /><small>s apart</small></div>
          <div class="fx-sub cap"><span>last <b><Sk w="4em" /></b></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Machines</span></div>
          <div class="fx-big"><Sk w="0.8em" /></div>
          <div class="fx-sub cap"><span><Sk w="14em" /></span></div>
        </div>
      </section>

      <SkRepeat n={2}>
        {(i) => (
          <BackendRowSkeleton
            i={i}
            actions={
              <>
                <button class="btn btn-sm" disabled>Open</button>
                <button class="btn btn-sm" disabled>Details</button>
              </>
            }
          />
        )}
      </SkRepeat>
    </div>
  );
}

/** One backend's own page while it loads: its row, then target and environment. */
export function BackendDetailSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <BackendRowSkeleton i={0} />
      <section class="fx-row bk-facts-row">
        <div class="bk-facts">
          <section class="sheet-sec">
            <div class="fx-head"><span class="cap">Target</span></div>
            <div class="sp-kv">
              <div style={{ 'grid-column': '1 / -1' }}><span class="cap">URL</span><b><Sk w="10em" style={BASE} /></b></div>
              <div style={{ 'grid-column': '1 / -1' }}><span class="cap">Health check</span><b><Sk w="14em" style={BASE} /></b></div>
              <For each={['Address', 'Checked every', 'Last checked', 'Last healthy']}>
                {(label, n) => <div><span class="cap">{label}</span><b><Sk w={vary(n(), 40, 80)} style={BASE} /></b></div>}
              </For>
            </div>
          </section>
          <section class="sheet-sec">
            <div class="fx-head"><span class="cap">Environment</span><span class="fx-meta cap"><span><Sk w="1em" /></span></span></div>
            <table class="fx-list">
              <tbody>
                <SkRepeat n={3}>
                  {(i) => (
                    <tr>
                      <td style={{ width: '42%' }}><Sk w={vary(i, 50, 70)} i={i} /></td>
                      <td><Sk w={vary(i, 70, 120)} i={i} /></td>
                    </tr>
                  )}
                </SkRepeat>
              </tbody>
            </table>
          </section>
        </div>
      </section>
    </div>
  );
}

/** A backend row: identity, response chart, CPU and memory. */
function BackendRowSkeleton(props: { i: number; actions?: JSX.Element }) {
  return (
    <section class="fx-row bk-row">
      <IdentitySkeleton icon={ICON} i={props.i} facts={['Address', 'Checked', 'Up']} actions={props.actions} />
      <div class="fx-cell bk-resp">
        <div class="fx-head"><span class="cap">Response</span><span class="fx-meta cap"><span><Sk w="4em" i={props.i} /></span></span></div>
        <div class="fx-big"><Sk w="1.2em" i={props.i} style={BASE} /><small>ms</small></div>
        {/* No height: the chart grows to the row, as the real one does. */}
        <Sk block class="fx-canvas" i={props.i} />
        <div class="fx-axis cap"><Sk w="100%" i={props.i} /></div>
      </div>
      <GaugeCellSkeleton label="CPU" i={props.i} />
      <GaugeCellSkeleton label="Memory" i={props.i} />
    </section>
  );
}
