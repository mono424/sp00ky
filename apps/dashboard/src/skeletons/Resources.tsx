import { For, type JSX } from 'solid-js';
import { Seg, Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { BASE } from './Ssps';

/**
 * Resources while they load, drawn as a project on its own VMs (the database's
 * first): each VM's header, its CPU and memory charts beside its disks, then
 * what runs on it.
 */
export function ResourcesSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <HostSkeleton
        stack={
          <>
            <DiskSkeleton label="Data volume" totalLabel="Volume" />
            <DiskSkeleton label="Bucket volume" totalLabel="Volume" />
          </>
        }
      />
      <HostSkeleton
        stack={
          <>
            <DiskSkeleton label="System disk" totalLabel="Disk" />
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">Container limits</span><span class="fx-meta cap"><span>what they may use</span></span></div>
              <div class="fx-big"><Sk w="2.5em" /><small>memory</small></div>
              <div class="fx-foot rs-disk">
                <For each={['vCPU', 'Machine', 'Running']}>{(label) => <div><span class="cap">{label}</span><b><Sk w="2em" style={BASE} /></b></div>}</For>
              </div>
            </div>
          </>
        }
      />
    </div>
  );
}

/** One VM: its header, CPU and memory charts beside its disks, its containers. */
function HostSkeleton(props: { stack: JSX.Element }) {
  return (
    <>
      <section class="fx-row">
        <div class="fx-cell rs-vm">
          <div class="fx-head">
            <span class="cap"><span class="rs-vm-name"><Sk w="7em" /></span></span>
            <span class="fx-meta cap"><span class="rs-vm-spec"><Sk w="16em" /></span></span>
          </div>
        </div>
      </section>
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(2, minmax(0, 1.4fr)) minmax(0, 1fr)' }}>
        <For each={['CPU', 'Memory']}>
          {(label, i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span><span class="fx-meta cap"><span>containers <b><Sk w="3em" i={i()} /></b> · last 15 min</span></span></div>
              <div class="fx-big"><Sk w="2em" i={i()} /><small><Sk w="9em" i={i()} /></small></div>
              {/* No height: the chart grows to the disks beside it, as the real one does. */}
              <Sk block class="fx-canvas" i={i()} />
              <div class="fx-axis cap"><Sk w="100%" i={i()} /></div>
            </div>
          )}
        </For>
        <div class="rs-stack">{props.stack}</div>
      </section>
      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">On this VM</span><span class="fx-meta cap"><span><Sk w="1em" /> of <Sk w="1em" /> running · closest to a limit first</span></span></div>
          <table class="fx-list rs-containers">
            <colgroup>
              <col style={{ width: '13%' }} /><col style={{ width: '18%' }} /><col style={{ width: '18%' }} />
              <col style={{ width: '19%' }} /><col style={{ width: '8%' }} /><col style={{ width: '17%' }} /><col style={{ width: '7%' }} />
            </colgroup>
            <thead>
              <tr>
                <th class="cap">Container</th><th class="cap">CPU</th><th class="cap">Memory</th><th class="cap">Disk</th>
                <th class="cap r">Disk writes</th><th class="cap r">Network in / out</th><th class="cap r">Up</th>
              </tr>
            </thead>
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td>
                      <span class="fx-who"><Sq tone="idle" /><Sk w={vary(i, 50, 40)} i={i} /></span>
                      <span class="vw-meta cap"><Sk w={vary(i, 40, 30)} i={i} /></span>
                    </td>
                    <For each={[0, 1, 2]}>{() => <td><span class="fx-meter"><Seg pct={null} /><b><Sk w="6em" i={i} /></b></span></td>}</For>
                    <td class="r"><Sk w="4em" i={i} /></td>
                    <td class="r"><Sk w="9em" i={i} /></td>
                    <td class="r"><Sk w="2.5em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

/** A disk: how full, the strip, then used, free and its size. */
function DiskSkeleton(props: { label: string; totalLabel: string }) {
  return (
    <div class="fx-cell">
      <div class="fx-head"><span class="cap">{props.label}</span><span class="fx-meta cap"><span><Sk w="8em" /></span></span></div>
      <div class="fx-big"><Sk w="2em" /><small>full</small></div>
      <div style={{ 'margin-top': '16px' }}><Seg pct={null} wide /></div>
      <div class="fx-foot rs-disk">
        <For each={['Used', 'Free', props.totalLabel]}>{(label) => <div><span class="cap">{label}</span><b><Sk w="3em" style={BASE} /></b></div>}</For>
      </div>
    </div>
  );
}
