import { For } from 'solid-js';
import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/**
 * Bars sit at the top of their line: in small text a middle-aligned bar
 * stretches the line box by a px or two, and the row with it.
 */
const top = { 'vertical-align': 'top' } as const;

/** Deployments while they load: production, the health strip, addresses and history. */
export function DeploymentsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <DeploymentsTopSkeleton />
      <DeploymentsListSkeleton />
    </div>
  );
}

/**
 * The preview, production and health rows. Its own export so the page can
 * keep the build-time box between this and the list mounted while it loads.
 */
export function DeploymentsTopSkeleton() {
  return (
    <>
      <section class="fx-row cx-prod" aria-busy="true" aria-label="Loading">
        <div class="fx-cell cx-prod-shot">
          <div class="fx-head"><span class="cap">Preview</span></div>
          <div class="cx-shot">
            <div class="cx-shot-bar">
              <i /><i /><i />
              <span><Sk w="9em" style={top} /></span>
            </div>
            <div class="cx-shot-screen">
              <span class="sk cx-shot-sk" />
            </div>
          </div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Production</span><span class="fx-meta cap"><Sk w="5em" style={top} /></span></div>
          <div class="cx-main">
            <span class="cap"><Sk w="4em" style={top} /></span>
            <span class="cx-main-url"><Sk w="9em" style={top} /></span>
            <div class="cx-alts"><a><Sk w="16em" style={top} /></a><a><Sk w="16em" style={top} /></a><span class="cx-more"><Sk w="4em" style={top} /></span></div>
          </div>
          <div class="cx-facts">
            <For each={['Status', 'Created', 'Rolled out in']}>
              {(label, i) => (
                <div>
                  <span class="cap">{label}</span>
                  <b><Sk w={i() === 1 ? '10em' : '4em'} i={i()} style={top} /></b>
                </div>
              )}
            </For>
          </div>
          {/* The branch icon's room: the 12px icon sets the line's height. */}
          <div class="cx-commit"><span><span class="cx-branch"><svg width="12" height="12" aria-hidden="true" /><Sk w="3em" style={top} /></span> <Sk w="4em" style={top} /> <Sk w="22em" style={top} /></span></div>
          <div class="cx-kv">
            <SkRepeat n={3}>
              {(i) => (
                <div><span class="cap"><Sk w="5em" i={i} style={top} /></span><b><Sk w="6em" i={i} style={top} /></b></div>
              )}
            </SkRepeat>
          </div>
        </div>
      </section>

      <section class="fx-row cx-strip" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }} aria-busy="true" aria-label="Loading">
        <For each={['SSPs', 'Backends', 'Round trip', 'Incidents']}>
          {(label, i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span><span class="fx-meta cap"><Sq tone="idle" /></span></div>
              <div class="cx-stat"><Sk w="1.6em" i={i()} style={top} /><small><Sk w="4em" i={i()} style={top} /></small></div>
            </div>
          )}
        </For>
      </section>
    </>
  );
}

/** The addresses and the history: rows at their loaded height. */
export function DeploymentsListSkeleton() {
  return (
    <>
      <section class="fx-row" aria-busy="true" aria-label="Loading">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Domains</span><span class="fx-meta cap"><Sk w="6em" style={top} /><span>Add a domain</span></span></div>
          <div class="cx-urls">
            <SkRepeat n={3}>
              {(i) => (
                <div class="cx-url">
                  <span class="cx-url-host"><span><Sk w={vary(i, 150, 90)} i={i} style={top} /></span></span>
                  <span class="cx-url-serves"><Sk w={vary(i, 40, 50)} i={i} style={top} /></span>
                  <span class="cap cx-url-kind"><Sk w="7em" i={i} style={top} /></span>
                  <span class="cx-st idle"><Sq tone="idle" /><Sk w="3.5em" i={i} style={top} /></span>
                  <span class="cx-url-acts">
                    <button class="btn btn-sm" disabled>Copy</button>
                    <button class="btn btn-sm" disabled>Open ↗</button>
                  </span>
                </div>
              )}
            </SkRepeat>
          </div>
        </div>
      </section>

      <section class="fx-row" aria-busy="true" aria-label="Loading">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">History</span><span class="fx-meta cap"><Sk w="5em" style={top} /></span></div>
          <div class="cx-deps">
            <SkRepeat n={4}>
              {(i) => (
                <div class="cx-dep">
                  <div class="cx-dep-v"><b><Sk w="2.2em" i={i} style={top} /></b><span class="cap"><Sk w="5em" i={i} style={top} /></span></div>
                  <div class="cx-dep-st"><span class="cx-st idle"><Sq tone="idle" /><Sk w="5em" i={i} style={top} /></span><span class="cap"><Sk w="3em" i={i} style={top} /></span></div>
                  <div class="cx-dep-what"><span class="t"><Sk w={vary(i, 200, 140)} i={i} style={top} /></span><span class="m"><Sk w={vary(i, 90, 60)} i={i} style={top} /></span></div>
                  <div class="cx-dep-when"><span><Sk w="5em" i={i} style={top} /></span><span class="cx-who-by"><Sk w="6em" i={i} style={top} /></span></div>
                  <div class="cx-dep-act" />
                </div>
              )}
            </SkRepeat>
          </div>
        </div>
      </section>
    </>
  );
}
