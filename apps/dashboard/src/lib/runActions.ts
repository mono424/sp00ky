import { api } from '../api/client';
import { runAction, post } from '../components/Actions';
import type {
  CancelResponse,
  JobsClearResponse,
  JobKillResponse,
  JobRetryResponse,
  RerunResponse,
  RetryResponse,
} from '../api/types';

/**
 * The run-level actions, shared by the list and the detail page.
 *
 * Wording follows what the engine actually does (see the scheduler's
 * `schedule-core`), not Temporal's vocabulary: a rerun is a NEW ad-hoc run
 * with the same frozen dag and input; a retry reopens THIS run and
 * re-dispatches only its failed and skipped steps under fresh job ids, keeping
 * every successful step's output.
 */

const runPath = (id: string, verb: string) =>
  `/workflows/runs/${encodeURIComponent(id)}/${verb}`;

export function cancelRun(id: string, name: string, after?: () => void) {
  return runAction<CancelResponse>({
    label: `Cancel ${name}`,
    confirm: {
      title: `Cancel this run of ${name}?`,
      verb: 'Cancel run',
      consequences: [
        'Every in-flight step job is killed on its SSP; blocked steps are skipped.',
        'The run ends as killed. It can be retried from its failed steps afterwards.',
      ],
    },
    request: post(runPath(id, 'cancel')),
    success: (r) =>
      r.status === 'killed'
        ? 'Killed. Steps were stopped immediately.'
        : 'Kill requested. The engine acts on its next sweep.',
    after,
  });
}

export function rerunRun(
  id: string,
  navigate: (to: string) => void,
) {
  return runAction<RerunResponse>({
    label: 'Rerun',
    confirm: {
      title: 'Rerun this workflow?',
      verb: 'Rerun',
      consequences: [
        'A new ad-hoc run is created with the same dag, input and target table.',
        'It is not attached to the schedule: it does not count for its concurrency policy or last-run status.',
        'The original run is left untouched.',
      ],
    },
    request: post(runPath(id, 'rerun')),
    success: (r) => `Started ${r.run}`,
    after: (r) => navigate(`/workflows/${encodeURIComponent(r.run)}`),
  });
}

export function retryRun(id: string, name: string, after?: () => void) {
  return runAction<RetryResponse>({
    label: `Retry ${name}`,
    confirm: {
      title: 'Retry from the failed steps?',
      verb: 'Retry',
      consequences: [
        'Failed and skipped steps go back to blocked and are dispatched again under new job ids.',
        'Successful steps keep their output and are not re-run.',
        'The run reopens as running; if it came from a schedule, that fire reopens too.',
      ],
    },
    request: post(runPath(id, 'retry')),
    success: (r) =>
      `Attempt ${r.retry_count}: ${r.reset.length} step${r.reset.length === 1 ? '' : 's'} reset` +
      (r.kept.length ? `, ${r.kept.length} kept` : ''),
    after,
  });
}

export function killJob(jobId: string, after?: () => void) {
  return runAction<JobKillResponse>({
    label: 'Kill job',
    confirm: {
      title: `Kill job ${jobId}?`,
      verb: 'Kill',
      consequences: [
        'The request in flight on its SSP is cancelled; the job ends failed with "killed by operator".',
        'The job is not retried by its own backoff. Its step fails and the workflow reacts per its policy.',
      ],
    },
    request: post(`/jobs/${encodeURIComponent(jobId)}/kill`),
    success: (r) => `Sent to ${r.dispatched} of ${r.ssps} SSPs`,
    after,
  });
}

export function retryJob(jobId: string, after?: () => void) {
  return runAction<JobRetryResponse>({
    label: 'Retry job',
    request: post(`/jobs/${encodeURIComponent(jobId)}/retry`),
    success: (r) => `${r.status} on ${r.assigned_to}`,
    after,
  });
}

/**
 * Delete terminal outbox rows, the dashboard's `spky jobs clear`.
 *
 * `processing` is spared at either width, which is why the consequences say so:
 * the fear this dialog has to answer is "will this kill the job running right
 * now", and the answer is no.
 */
export function clearJobs(
  opts: { table?: string; all?: boolean },
  after?: () => void,
) {
  const scope = opts.table ?? 'every outbox table';
  return runAction<JobsClearResponse>({
    label: `Clear jobs in ${scope}`,
    confirm: {
      title: `Delete terminal jobs in ${scope}?`,
      verb: 'Delete',
      consequences: [
        opts.all
          ? 'Every success, failed AND pending job is deleted. Pending jobs are queued work that will now never run.'
          : 'Every success and failed job is deleted. Pending work is left queued.',
        'Jobs in processing are never touched, so nothing running right now is interrupted.',
        'This is a delete, not an archive. The rows are gone.',
      ],
      typeToConfirm: opts.all ? 'delete' : undefined,
    },
    request: post('/jobs/clear', { table: opts.table, all: !!opts.all }),
    success: (r) =>
      r.total === 0
        ? 'Nothing to clear.'
        : `Removed ${r.total} job${r.total === 1 ? '' : 's'}` +
          (r.more ? '. More is left; run it again to continue.' : '.'),
    after,
  });
}

export function pauseSchedule(name: string, paused: boolean, after?: () => void) {
  return runAction<{ name: string; paused: boolean }>({
    label: paused ? `Pause ${name}` : `Resume ${name}`,
    confirm: paused
      ? {
          title: `Pause ${name}?`,
          verb: 'Pause',
          consequences: [
            'No further fires until resumed. A queued manual trigger waits too: pause wins.',
            'Runs already in flight are not touched.',
          ],
        }
      : undefined,
    request: () =>
      api.post(`/schedules/${encodeURIComponent(name)}/${paused ? 'pause' : 'resume'}`),
    success: (r) => (r.paused ? 'Paused' : 'Resumed. The next fire is planned on the next sweep.'),
    after,
  });
}

export function triggerSchedule(name: string, after?: () => void) {
  return runAction<{ name: string; triggered_at: string }>({
    label: `Run ${name} now`,
    request: () => api.post(`/schedules/${encodeURIComponent(name)}/trigger`),
    success: () => 'Queued. The engine fires it within a few seconds.',
    after,
  });
}
