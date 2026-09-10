import type { RuntimeEvent } from '../kernel/events';
import type { Lane, Saga } from '../kernel/saga';
import type { SagaEnv } from '../query/env';
import { ensureRegistered, registerRemote } from '../query/register.saga';
import { readDirtyMembership, readMembership } from '../query/membership.saga';
import { fetchRows } from '../query/fetch.saga';
import { materialize, streamUpdate } from '../query/materialize.saga';
import { ackPrune, gcTick, lifecycleTick } from '../query/lifecycle.saga';
import { drain } from '../mutation/push.saga';
import { flushWrite } from '../mutation/write.saga';
import { pollTick } from '../sync/poll.saga';
import { liveChange, liveStart } from '../sync/live.saga';
import { connectionChanged, selfHealTick, syncOutcome } from '../sync/connection.saga';
import { setRole, tabMessage } from '../sync/tabs.saga';
import { pageHide, primeCircuit, startRemote, versionsPrimed, warmBlobs } from '../boot/boot.saga';
import { authFlip } from '../boot/auth-flip.saga';
import { bucketSwitch } from '../boot/bucket-switch.saga';
import { fx } from '../kernel/effects';

export interface RouteTarget {
  saga: Saga<unknown>;
  lane?: Lane;
}

const serial = (key: string): Lane => ({ kind: 'serial', key });
const dedupe = (key: string): Lane => ({ kind: 'dedupe', key });

function* materializeDirty(): Saga<void> {
  const dirty = (yield fx.state.read((s) => [...s.dirty])) as string[];
  for (const hash of dirty) yield fx.dispatch({ type: 'Materialize', hash });
}

/** One table: which saga handles an event, and on which lane. */
export function route(env: SagaEnv, event: RuntimeEvent): RouteTarget {
  switch (event.type) {
    case 'EnsureRegistered':
      return { saga: ensureRegistered(env, { requireAuth: event.requireAuth, attempt: event.attempt }), lane: serial('ensure') };
    case 'RegisterRemote':
      return { saga: registerRemote(env, event.hash, event.retry), lane: dedupe(`register:${event.hash}`) };
    case 'ReadDirtyMembership':
      return { saga: readDirtyMembership(env), lane: serial('membership') };
    case 'ReadMembership':
      return { saga: readMembership(env, event.hashes, { force: true }), lane: serial('membership') };
    case 'FetchRows':
      return { saga: fetchRows(env), lane: serial('fetch') };
    case 'Materialize':
      return { saga: materialize(event.hash), lane: serial(`mat:${event.hash}`) };
    case 'MaterializeDirty':
      return { saga: materializeDirty() };
    case 'StreamUpdate':
      return { saga: streamUpdate(event.update), lane: serial(`stream:${event.update.queryHash}`) };
    case 'LifecycleTick':
    case 'HeartbeatNow':
      return { saga: lifecycleTick(env), lane: dedupe('lifecycle') };
    case 'AckPrune':
      return { saga: ackPrune(), lane: dedupe('ack-prune') };
    case 'GcTick':
      return { saga: gcTick(), lane: dedupe('gc') };
    case 'Drain':
      return { saga: drain(env), lane: serial('outbox') };
    case 'FlushWrite':
      return { saga: flushWrite(env, event.key), lane: serial('outbox-write') };
    case 'PollTick':
      return { saga: pollTick(env), lane: dedupe('poll') };
    case 'SelfHealTick':
      return { saga: selfHealTick(env), lane: dedupe('heal') };
    case 'SyncOutcome':
      return { saga: syncOutcome(env, event.ok, event.error), lane: serial('health') };
    case 'ConnectionChanged':
      return { saga: connectionChanged(env, event.state), lane: serial('connection') };
    case 'LiveStart':
      return { saga: liveStart(env), lane: serial('live') };
    case 'LiveChange':
      return { saga: liveChange(event.hashes), lane: serial('live-change') };
    case 'TabRole':
      return { saga: setRole(env, event.role), lane: serial('tabs') };
    case 'TabMessage':
      return { saga: tabMessage(env, event.message), lane: serial('tabs') };
    case 'StartRemote':
      return { saga: startRemote(), lane: dedupe('start-remote') };
    case 'PrimeCircuit':
      return { saga: primeCircuit(), lane: dedupe('prime') };
    case 'VersionsPrimed':
      return { saga: versionsPrimed(event.entries) };
    case 'WarmBlobs':
      return { saga: warmBlobs(), lane: dedupe('blobs') };
    case 'AuthFlip':
      return { saga: authFlip(env, event.userId), lane: serial('bucket') };
    case 'BucketSwitch':
      return { saga: bucketSwitch(env, event.target), lane: serial('bucket') };
    case 'PageHide':
      return { saga: pageHide() };
  }
}
