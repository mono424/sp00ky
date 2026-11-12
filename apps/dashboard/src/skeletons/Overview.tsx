import { For } from 'solid-js';
import { Seg } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/** The Overview while it loads: its four rows, at their loaded heights. */
export function OverviewSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Sync round trip</span><span class="fx-meta cap"><Sk w="11em" /></span></div>
          <div class="fx-big"><Sk w="1.6em" /></div>
          <Sk block h={152} class="fx-canvas" />
          <div class="fx-axis cap"><Sk w="100%" /></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Live users</span></div>
          <div class="fx-big"><Sk w="1.8em" /></div>
          <div class="fx-sub cap"><Sk w="4em" /><Sk w="4em" /><Sk w="4em" /></div>
          <Sk block h={97.7} class="fx-matrix" />
        </div>
      </section>

      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1fr))' }}>
        <For each={['SurrealDB', 'Scheduler']}>
          {(name, n) => (
            <div class="fx-cell">
              <div class="fx-head fx-title">
                <Sk w={16} h={16} />
                <span class="fx-name">{name}</span>
                <span class="fx-meta cap"><Sk w="5em" /></span>
              </div>
              <div class="fx-gauges">
                <For each={['CPU', 'Memory', 'Disk']}>
                  {(label, i) => <GaugeSkeleton label={label} i={n() * 3 + i()} />}
                </For>
              </div>
              <div class="fx-foot">
                <For each={n() === 0 ? ['Write to live', 'Disk writes', 'Up'] : ['Ingest lag', 'Pending events', 'Up']}>
                  {(label) => (
                    <div>
                      <span class="cap">{label}</span>
                      <b><Sk w="3em" /></b>
                    </div>
                  )}
                </For>
              </div>
            </div>
          )}
        </For>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Services</span><span class="fx-meta cap"><Sk w="5em" /></span></div>
          <table class="fx-list ov-procs">
            <colgroup>
              <col style={{ width: '24%' }} /><col style={{ width: '8%' }} /><col style={{ width: '8%' }} /><col style={{ width: '8%' }} />
              <col style={{ width: '17%' }} /><col style={{ width: '17%' }} /><col style={{ width: '6%' }} /><col style={{ width: '12%' }} />
            </colgroup>
            <For each={[['Sync processors', 'Views', 'Heartbeat', 'Backlog'], ['Backends', 'Response', 'Checked', 'Machines']]}>
              {(heads, g) => (
                <tbody>
                  <tr class="fx-gh">
                    <th class="cap"><span class="fx-gt">{heads[0]}</span><Sk w="5em" /></th>
                    <th class="cap r">{heads[1]}</th>
                    <th class="cap r">{heads[2]}</th>
                    <th class="cap r">{heads[3]}</th>
                    <th class="cap">CPU</th>
                    <th class="cap">Memory</th>
                    <th class="cap r">Up</th>
                    <th class="cap r">Last 24h</th>
                  </tr>
                  <SkRepeat n={2}>
                    {(i) => (
                      <tr>
                        <td><Sk w={vary(i + g() * 2, 70, 40)} i={i} /></td>
                        <td class="r"><Sk w="2em" i={i} /></td>
                        <td class="r"><Sk w="2em" i={i} /></td>
                        <td class="r"><Sk w="2em" i={i} /></td>
                        <td><Sk w="70%" i={i} /></td>
                        <td><Sk w="70%" i={i} /></td>
                        <td class="r"><Sk w="2.5em" i={i} /></td>
                        <td class="r"><Sk w="90%" i={i} /></td>
                      </tr>
                    )}
                  </SkRepeat>
                </tbody>
              )}
            </For>
          </table>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Jobs</span><span class="fx-meta cap"><Sk w="6em" /></span></div>
          <div class="fx-figure">
            <div class="fx-big"><Sk w="1.6em" /></div>
            <div class="fx-inline">
              <For each={['Pending', 'Running', 'Stalled', 'Oldest wait']}>
                {(label) => (
                  <div>
                    <span class="cap">{label}</span>
                    <b><Sk w="2em" /></b>
                  </div>
                )}
              </For>
            </div>
          </div>
          <Sk block h={72} class="fx-barcode" />
          <QueuesSkeleton class="ov-queues" style={{ 'margin-top': '18px' }} />
        </div>
      </section>
    </div>
  );
}

/** One gauge: its label, a value, the empty segment strip, the trend's room. */
export function GaugeSkeleton(props: { label: string; i?: number }) {
  return (
    <div class="fx-gauge">
      <div class="fx-g-top"><span class="cap">{props.label}</span><span class="cap"><Sk w="4em" i={props.i} /></span></div>
      <div class="fx-gv"><Sk w="2em" i={props.i} /></div>
      <Seg pct={null} wide />
      <Sk block h={40} i={props.i} class="fx-spark" />
    </div>
  );
}

/** The queue table Overview and Runs both draw. */
export function QueuesSkeleton(props: { class: string; style?: Record<string, string>; actions?: boolean }) {
  return (
    <table class={`fx-list ${props.class}`} style={props.style}>
      <colgroup>
        <col style={{ width: props.actions ? '26%' : '30%' }} /><col style={{ width: props.actions ? '18%' : '22%' }} />
        <col style={{ width: props.actions ? '11%' : '12%' }} /><col style={{ width: props.actions ? '11%' : '12%' }} />
        <col style={{ width: props.actions ? '11%' : '12%' }} /><col style={{ width: props.actions ? '11%' : '12%' }} />
        {props.actions ? <col /> : null}
      </colgroup>
      <thead>
        <tr>
          <th class="cap">Queue</th><th class="cap">Running / limit</th><th class="cap r">Rate</th>
          <th class="cap r">Waiting</th><th class="cap r">Failed 1h</th><th class="cap r">Oldest</th>
          {props.actions ? <th class="cap" /> : null}
        </tr>
      </thead>
      <tbody>
        <SkRepeat n={4}>
          {(i) => (
            <tr>
              <td><Sk w={vary(i, 70, 50)} i={i} /></td>
              <td><Sk w="60%" i={i} /></td>
              <td class="r"><Sk w="2.5em" i={i} /></td>
              <td class="r"><Sk w="1.5em" i={i} /></td>
              <td class="r"><Sk w="1.5em" i={i} /></td>
              <td class="r"><Sk w="3em" i={i} /></td>
              {props.actions ? <td /> : null}
            </tr>
          )}
        </SkRepeat>
      </tbody>
    </table>
  );
}
