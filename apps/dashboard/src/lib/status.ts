/**
 * Status → colour mapping, in one place.
 *
 * Green and red are reserved for status across the whole app (see
 * `styles/theme.css`), so every caller must come through here rather than
 * picking a class inline. That is what keeps "a red dot means something is
 * wrong" true.
 */

export type Tone = 'ok' | 'warn' | 'bad' | 'idle';

export function schedulerTone(status: string | undefined): Tone {
  switch (status) {
    case 'ready':
      return 'ok';
    // Frozen and updating are normal, transient snapshot states, not faults.
    case 'frozen':
    case 'updating':
    case 'cloning':
    case 'restoring':
      return 'warn';
    default:
      return 'idle';
  }
}

export function sspTone(status: string | undefined): Tone {
  switch (status) {
    case 'ready':
      return 'ok';
    case 'bootstrapping':
    case 'replaying':
    case 'lagging':
      return 'warn';
    default:
      return 'idle';
  }
}

export function backendTone(status: string | undefined): Tone {
  switch (status) {
    case 'healthy':
      return 'ok';
    case 'unhealthy':
      return 'warn';
    case 'unreachable':
      return 'bad';
    default:
      return 'idle';
  }
}

export function runTone(status: string | undefined): Tone {
  switch (status) {
    case 'success':
      return 'ok';
    case 'running':
      return 'warn';
    case 'failed':
    case 'killed':
      return 'bad';
    default:
      return 'idle';
  }
}

/**
 * Outbox job statuses, asserted by the generated table DDL:
 * `pending | processing | success | failed` (`apps/cli/src/add_api.rs`).
 *
 * `pending` is idle rather than warn on purpose: a job waits in the outbox both
 * because nothing has picked it up yet AND between its own retries, and neither
 * is a fault to announce in a colour reserved for one.
 */
export function jobTone(status: string | undefined): Tone {
  switch (status) {
    case 'success':
      return 'ok';
    case 'processing':
      return 'warn';
    case 'failed':
      return 'bad';
    default:
      return 'idle';
  }
}

/**
 * Step statuses are a closed set, asserted by the schema:
 * `blocked | ready | dispatched | success | failed | skipped`
 * (`apps/cli/src/schedule_tables.surql`). Note there is no `running` — a step
 * in flight is `dispatched`.
 */
export function stepTone(status: string | undefined): Tone {
  switch (status) {
    case 'success':
      return 'ok';
    case 'dispatched':
      return 'warn';
    case 'failed':
      return 'bad';
    // 'blocked', 'ready' and 'skipped' are resting states, not problems.
    default:
      return 'idle';
  }
}
