import { RecordId } from 'surrealdb';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import * as R from '../state/reducers';
import { bucketIdForUser } from '../modules/ref-tables';
import type { SagaEnv } from '../query/env';
import { columnsFor } from '../query/env';
import * as sql from '../query/sql';
import { encodeRecordId } from '../utils/index';
import { cleanRecord } from '../utils/parser';
import { bucketSwitch, rebindQueries } from './bucket-switch.saga';

/**
 * The signed-in principal changed (sign-in, sign-out, boot verification).
 * Identity first (routing and `$auth` for the local SSP), then the salt if
 * the principal really changed, then the bucket, then the verified user row.
 *
 * The salt goes before the bucket because the switch re-keys every active
 * query (`rebindQueries`) from the salt it finds. A query's remote id hashes
 * the salt but not the principal, so re-keyed under the old salt it named the
 * view the previous principal still held, and the SSP refused it as another
 * identity's (409 `auth_mismatch`) until that view's TTL ran out. On whitepawn
 * every impersonation start, stop and token lapse refused a tab's queries for
 * the 10 minutes of that TTL, each refusal a failed sync round, so the rail
 * read "Can't reach the server" the whole time.
 */
export function* authFlip(env: SagaEnv, userId: string | null): Saga<void> {
  const authId = (yield fx.service('auth.sessionAuthId')) as string | null;
  const access = (yield fx.service('auth.access')) as string | null;
  yield fx.state.update(R.setIdentity({ userId }));
  yield fx.service('ssp.setSessionAuth', authId, access);
  const saltUserId = (yield fx.state.read((s) => s.saltUserId)) as string | null;
  const resalted = authId !== saltUserId;
  if (resalted) {
    const salt = (yield fx.id('salt')) as string;
    yield fx.state.update(R.setIdentity({ sessionId: salt, saltUserId: authId }));
    yield fx.service('crdt.setSessionId', salt);
  }
  const target = bucketIdForUser(userId);
  yield fx.service('hint.write', target);
  yield fx.state.update(R.setIdentity({ pendingBucket: target }));
  const current = (yield fx.service('local.currentBucketId')) as string;
  const release = current === target ? null : ((yield fx.service('local.beginSwitch')) as () => void);
  yield* bucketSwitch(env, target, release);
  // No switch means nothing re-keyed the queries under the new salt.
  if (resalted && current === target) {
    yield* rebindQueries();
    yield fx.dispatch({ type: 'EnsureRegistered' });
  }
  yield* persistVerifiedUser(env);
}

/**
 * Write the user's own row, as the server just returned it, into the local
 * store so `user` queries see `email_verified` and friends without waiting
 * for membership. A field refresh, not a version claim.
 */
export function* persistVerifiedUser(env: SagaEnv): Saga<void> {
  const row = (yield fx.service('auth.currentUser')) as Record<string, unknown> | null;
  if (!row || !(row.id instanceof RecordId) || Object.keys(row).length <= 1) return;
  const rid = row.id as RecordId<string>;
  const table = String(rid.table);
  const columns = columnsFor(env, table);
  if (!columns) return;
  const id = encodeRecordId(rid);
  const version = ((yield fx.state.read((s) => s.versions.get(id))) as number | undefined) || 1;
  const cleaned = cleanRecord(columns as never, row);
  const { id: _id, ...content } = cleaned;
  try {
    const tx = sql.upsertBodiesTx([{ id: rid, content: { ...content, _00_rv: version } }]);
    yield fx.local.execute(tx.query, tx.vars);
    yield fx.ssp.ingest([{ table, op: 'UPDATE', id, record: { ...cleaned, _00_rv: version } }]);
    yield fx.state.update(R.compose(R.setVersions([[id, version]]), R.markTableDirty(table)));
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'could not persist the verified user row', data: { error } });
  }
}
