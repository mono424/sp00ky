import { Show } from 'solid-js';
import { formatCount, formatUptime } from '../lib/format';
import type { SspEntity } from '../api/types';

/**
 * How far along a bootstrapping or replaying SSP is.
 *
 * Two fidelities, because progress reporting is advisory and an older SSP
 * simply does not send it:
 *
 *  - With `bootstrap`, a real fraction of tables loaded.
 *  - Without it, the phase and how long it has been in it, against the
 *    scheduler's bootstrap budget. The bar is drawn indeterminate rather than
 *    inventing a percentage from elapsed time, which would read as progress
 *    when it is only patience.
 */
export function BootstrapProgress(props: { ssp: SspEntity; timeoutSecs: number }) {
  const boot = () => props.ssp.bootstrap;

  const fraction = () => {
    const b = boot();
    if (!b || b.tables_total === 0) return null;
    return Math.min(1, b.tables_done / b.tables_total);
  };

  // The budget the scheduler reaps a hung bootstrap on. Worth showing, because
  // an SSP past it is about to be evicted and restarted.
  const overdue = () =>
    props.ssp.state_seconds !== null && props.ssp.state_seconds > props.timeoutSecs;

  return (
    <div class="ssp-boot">
      <div class="ssp-boot-line">
        <span class="ssp-boot-what">
          <Show
            when={boot()}
            fallback={
              props.ssp.status === 'replaying'
                ? `Replaying ${formatCount(props.ssp.buffered_events)} events`
                : 'Loading circuit'
            }
          >
            {(b) => (
              <>
                {b().tables_done} of {b().tables_total} tables
                <Show when={b().current_table}>
                  {', '}
                  <span class="mono">{b().current_table}</span>
                </Show>
              </>
            )}
          </Show>
        </span>
        <span class="ssp-boot-time" classList={{ bad: overdue() }}>
          {formatUptime(props.ssp.state_seconds)} of {formatUptime(props.timeoutSecs)}
        </span>
      </div>
      <div class="bar">
        <div
          class="bar-fill"
          classList={{ indeterminate: fraction() === null }}
          style={{ width: fraction() !== null ? `${fraction()! * 100}%` : undefined }}
        />
      </div>
    </div>
  );
}
