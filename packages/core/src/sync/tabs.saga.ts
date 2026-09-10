import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import type { IngestRecord } from '../services/stream-processor/index';
import type { ClientState, TabRole } from '../state/client-state';
import * as R from '../state/reducers';
import type { SagaEnv } from '../query/env';
import { loadOutbox } from '../mutation/push.saga';
import { liveChange } from './live.saga';

/**
 * Shared-tabs role changes. The leader owns the outbox drain, the poll and
 * LIVE; a follower keeps its own circuit and materialization but gets
 * membership dirt, acks and rollbacks relayed.
 */
export function* setRole(env: SagaEnv, role: TabRole): Saga<void> {
  const prev = (yield fx.state.read((s) => s.tabRole)) as TabRole;
  if (prev === role) return;
  yield fx.state.update(R.setTabRole(role));
  if (role === 'follower') {
    yield fx.timer.clear('poll');
    yield fx.timer.clear('outbox');
    yield fx.state.update(R.patchSync({ liveUuid: null, liveTable: null }));
    return;
  }
  yield* loadOutbox(env);
  yield fx.dispatch({ type: 'LiveStart' });
  yield fx.dispatch({ type: 'PollTick' });
  yield fx.dispatch({ type: 'EnsureRegistered' });
}

type TabMessage =
  | { type: 'ingest'; records: IngestRecord[] }
  | { type: 'membership-dirty'; hashes: string[] }
  | { type: 'outbox-changed'; mutationId: string }
  | { type: 'mutation-settled'; mutationId: string; recordId: string; eventType: string }
  | { type: 'mutation-rolled-back'; mutationId: string; recordId: string; eventType: string; error: string }
  | { type: 'failed-mutations-changed'; count: number };

/** A message from another tab of the same origin. */
export function* tabMessage(env: SagaEnv, raw: unknown): Saga<void> {
  const msg = raw as TabMessage | null;
  if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;
  const role = (yield fx.state.read((s) => s.tabRole)) as ClientState['tabRole'];
  switch (msg.type) {
    case 'ingest': {
      if (!Array.isArray(msg.records) || msg.records.length === 0) return;
      try {
        yield fx.ssp.ingest(msg.records);
      } catch (error) {
        yield fx.emit({ type: 'log', level: 'warn', message: 'relayed ingest failed', data: { error } });
      }
      yield fx.state.update(
        R.compose(
          R.setVersions(
            msg.records
              .filter((r) => r.op !== 'DELETE')
              .map((r) => [r.id, typeof r.record?._00_rv === 'number' ? (r.record._00_rv as number) : 0] as const)
          ),
          R.deleteVersions(msg.records.filter((r) => r.op === 'DELETE').map((r) => r.id)),
          ...[...new Set(msg.records.map((r) => r.table))].map((t) => R.markTableDirty(t))
        )
      );
      return;
    }
    case 'membership-dirty':
      if (Array.isArray(msg.hashes)) yield* liveChange(msg.hashes);
      return;
    case 'outbox-changed':
      if (role !== 'leader') return;
      yield* loadOutbox(env);
      return;
    case 'mutation-settled': {
      const now = (yield fx.now()) as number;
      yield fx.state.update(R.outboxAck(msg.mutationId, now));
      yield fx.emit({ type: 'mutation:settled', mutationId: msg.mutationId, recordId: msg.recordId, eventType: msg.eventType });
      return;
    }
    case 'mutation-rolled-back': {
      const table = msg.recordId.slice(0, msg.recordId.indexOf(':'));
      yield fx.state.update(R.compose(R.outboxRemove(msg.mutationId), R.markTableDirty(table)));
      yield fx.emit({ type: 'mutation:rolled-back', mutationId: msg.mutationId, recordId: msg.recordId, eventType: msg.eventType, error: msg.error });
      return;
    }
    case 'failed-mutations-changed':
      if (typeof msg.count !== 'number') return;
      yield fx.state.update(R.setFailedCount(msg.count));
      yield fx.emit({ type: 'tray:changed', count: msg.count });
      return;
    default:
      return;
  }
}
