import { For, Index } from 'solid-js';
import { FilterMenu, Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { QueuesSkeleton } from './Overview';

/** Schedules while they load: the table, its two-line rows and the fire squares. */
export function SchedulesSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Schedules</span><span class="fx-meta cap"><Sk w="14em" /></span></div>
          <table class="fx-list rn-sched">
            <colgroup>
              <col style={{ width: '27%' }} /><col style={{ width: '17%' }} /><col style={{ width: '20%' }} />
              <col style={{ width: '11%' }} /><col style={{ width: '10%' }} /><col style={{ width: '15%' }} />
            </colgroup>
            <thead>
              <tr>
                <th class="cap">Schedule</th><th class="cap">Fires</th><th class="cap">Last fires</th>
                <th class="cap r">Last</th><th class="cap r">Next</th><th class="cap" />
              </tr>
            </thead>
            <tbody>
              <SkRepeat n={5}>
                {(i) => (
                  <tr>
                    <td>
                      <span class="fx-who"><Sq tone="idle" /><Sk w={vary(i, 90, 60)} i={i} /></span>
                      <span class="vw-meta cap"><Sk w={vary(i, 70, 50)} i={i} /></span>
                    </td>
                    <td class="rn-cadence"><Sk w={vary(i, 80, 40)} i={i} /></td>
                    <td><span class="rn-fires"><Index each={Array.from({ length: 12 })}>{() => <i class="none" />}</Index></span></td>
                    <td class="r"><Sk w="3.5em" i={i} /></td>
                    <td class="r"><Sk w="3em" i={i} /></td>
                    <td class="r" />
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

/** Workflow runs while they load. */
export function WorkflowsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <RunListSkeleton label="Workflow runs" rows={8} />
      </section>
    </div>
  );
}

/** Jobs while they load: the four figures, the list and the queues. */
export function RunsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
        <For each={[['Running', ''], ['Waiting', ''], ['Done', 'per minute'], ['Failed', 'last hour']]}>
          {([label, meta], i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span>{meta ? <span class="fx-meta cap"><span>{meta}</span></span> : null}</div>
              <div class="fx-big"><Sk w="1.4em" i={i()} /></div>
              <div class="fx-sub cap"><Sk w="7em" i={i()} /></div>
              <Sk block h={72} class="fx-barcode" i={i()} />
            </div>
          )}
        </For>
      </section>
      <section class="fx-row">
        <RunListSkeleton label="Jobs" rows={6} />
      </section>
      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Queues</span><span class="fx-meta cap"><span>clear finished</span></span></div>
          <QueuesSkeleton class="rn-queues" actions />
        </div>
      </section>
    </div>
  );
}

/** The run list: its filter bar and two-line rows. */
function RunListSkeleton(props: { label: string; rows: number }) {
  return (
    <div class="fx-cell">
      <div class="fx-head"><span class="cap">{props.label}</span><span class="fx-meta cap"><Sk w="1.5em" /></span></div>
      <div class="inc-filters">
        <FilterMenu value={{}} onChange={() => undefined} fields={[]} />
        <div class="inc-find">
          <input type="search" placeholder="Search name, id, error" disabled aria-label="Search runs" />
        </div>
      </div>
      <table class="fx-list rn-list">
        <colgroup>
          <col /><col style={{ width: '14%' }} /><col style={{ width: '10%' }} /><col style={{ width: '9%' }} />
          <col style={{ width: '22%' }} /><col style={{ width: '170px' }} />
        </colgroup>
        <tbody>
          <tr class="fx-gh"><th class="cap" colSpan={6}><span class="fx-gt">Recent</span><Sk w="1.5em" /></th></tr>
          <SkRepeat n={props.rows}>
            {(i) => (
              <tr>
                <td>
                  <span class="fx-who"><Sq tone="idle" /><Sk w={vary(i, 90, 70)} i={i} /></span>
                  <span class="vw-meta cap"><Sk w={vary(i, 60, 40)} i={i} /></span>
                </td>
                <td><Sk w="80%" i={i} /></td>
                <td class="r"><Sk w="3.5em" i={i} /></td>
                <td class="r"><Sk w="2.5em" i={i} /></td>
                <td class="bu-state"><Sk w={vary(i, 50, 60)} i={i} /></td>
                <td class="r" />
              </tr>
            )}
          </SkRepeat>
        </tbody>
      </table>
    </div>
  );
}
