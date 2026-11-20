import { For, Index, type JSX } from 'solid-js';
import { Seg, Sq } from '../components/Frame';
import { Sk, SkRepeat } from '../components/Skeleton';

/** On the text's baseline: centred, a bar in a large font grows the line a little. */
export const BASE = { 'vertical-align': 'baseline' } as const;

const ICON = 'M4 3h16v7H4z M4 14h16v7H4z M7 6.5h1 M7 17.5h1';

/** Sync processors while they load: the fleet row and two SSP rows. */
export function SspsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Ready</span></div>
          <div class="fx-big"><Sk w="0.8em" /><small>/ <Sk w="0.8em" /></small></div>
          <SquaresSkeleton n={2} />
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Live views</span></div>
          <div class="fx-big"><Sk w="1.4em" /></div>
          <Sk block h={10} class="sp-share" />
          <div class="sp-legend cap"><Sk w="6em" /><Sk w="6em" /></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Backlog</span></div>
          <div class="fx-big"><Sk w="1.4em" /><small>ops</small></div>
          <div class="fx-sub cap"><span>oldest <b><Sk w="4em" /></b></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Heartbeats</span></div>
          <div class="fx-big"><Sk w="1.2em" /><small>stalest</small></div>
          <div class="fx-sub cap"><span>all within <b><Sk w="2em" /></b></span></div>
        </div>
      </section>

      <SkRepeat n={2}>
        {(i) => (
          <section class="fx-row sp-row">
            <IdentitySkeleton
              icon={ICON}
              accent
              i={i}
              facts={['Address', 'Up', 'Heartbeat']}
              actions={
                <>
                  <SplitSkeleton label="Restart" />
                  <button class="btn btn-sm" disabled>Logs</button>
                  <button class="btn btn-sm" disabled>Details</button>
                </>
              }
            />
            <GaugeCellSkeleton label="CPU" i={i} />
            <GaugeCellSkeleton label="Memory" i={i} />
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">Live views</span><span class="fx-meta cap"><span><Sk w="1.4em" i={i} />% of fleet</span></span></div>
              <div class="fx-big"><Sk w="1.2em" i={i} /></div>
              <Sk block h={12} class="sp-sharebar" i={i} />
            </div>
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">Backlog</span></div>
              <div class="fx-big"><Sk w="1.2em" i={i} /><small>ops</small></div>
              <div class="fx-sub cap"><span><b><Sk w="3em" i={i} /></b></span><span>oldest <b><Sk w="3em" i={i} /></b></span></div>
            </div>
          </section>
        )}
      </SkRepeat>
    </div>
  );
}

/** The fleet's status squares, one per member. */
export function SquaresSkeleton(props: { n: number }) {
  return (
    <div class="sp-squares">
      <SkRepeat n={props.n}>{(i) => <Sk block w={14} h={14} i={i} />}</SkRepeat>
    </div>
  );
}

/** The identity cell of an SSP or backend row: name, state, facts, day ticks, actions. */
export function IdentitySkeleton(props: {
  icon: string;
  /** An SSP: the accent icon and its version after the name. */
  accent?: boolean;
  i: number;
  facts: string[];
  actions: JSX.Element;
}) {
  return (
    <div class="fx-cell sp-id">
      <div class="fx-head fx-title" style={{ 'margin-bottom': '10px' }}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true" style={props.accent ? { color: 'var(--accent)' } : undefined}>
          <path d={props.icon} />
        </svg>
        <span class="fx-name"><Sk w="3.5em" i={props.i} style={BASE} /></span>
        {props.accent ? <span class="cap"><Sk w="6em" i={props.i} /></span> : null}
      </div>
      <div class="fx-state cap"><Sq tone="idle" /><Sk w="5em" i={props.i} /></div>
      <dl class="sp-facts">
        <For each={props.facts}>
          {(f) => (
            <div>
              <dt class="cap">{f}</dt>
              <dd><Sk w={f === 'Address' ? '6.5em' : '3.5em'} i={props.i} /></dd>
            </div>
          )}
        </For>
      </dl>
      <div class="fx-ticks"><Index each={Array.from({ length: 24 })}>{() => <i />}</Index></div>
      <div class="sp-actions">{props.actions}</div>
    </div>
  );
}

/** A split action button, drawn inert. */
export function SplitSkeleton(props: { label: string }) {
  return (
    <div class="split split-sm">
      <button class="btn split-primary btn-sm" disabled>{props.label}</button>
      <button class="btn split-toggle btn-sm" disabled aria-hidden="true"><span class="split-label">Actions</span><span class="chev" aria-hidden="true" /></button>
    </div>
  );
}

/** A gauge alone in its cell, as SSP and backend rows draw CPU and memory. */
export function GaugeCellSkeleton(props: { label: string; i?: number }) {
  return (
    <div class="fx-cell sp-gauge">
      <div class="fx-gauge">
        <div class="fx-g-top"><span class="cap">{props.label}</span><span class="cap"><Sk w="5em" i={props.i} /></span></div>
        <div class="fx-gv"><Sk w="1.6em" i={props.i} style={BASE} /></div>
        <Seg pct={null} wide />
        {/* No height: `.sp-gauge .fx-spark` sets the trend's own. */}
        <Sk block class="fx-spark" i={props.i} />
      </div>
    </div>
  );
}
