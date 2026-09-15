// Fixture query module: the shapes the generator must handle.
import { toRecordId } from './utils';

type Db = any;

export const qStatic = (db: Db) =>
  db.query('game').select('id', 'white').orderBy('sort_index', 'asc').limit(20).build();

export const qDynamicWhere = (db: Db, where: any) => db.query('game').where(where).limit(10).build();

export const qOne = (db: Db, id: any) => db.query('user').where({ id: toRecordId(String(id)) }).one().build();

export const qOrRange = (db: Db, a: any, b: any, since: number) =>
  db
    .query('game')
    .where({ _or: [{ white: a }, { black: b }], created_ms: { _op: '>=', _val: since } })
    .orderBy('created_ms', 'desc')
    .limit(50)
    .build();

export const qRelated = (db: Db, database: any) =>
  db
    .query('game')
    .where({ database })
    .related('white', (r: any) => r.select('id', 'name'))
    .build();

export const qWindow = (db: Db, window: number, page: number) =>
  db.query('game').orderBy('sort_index', 'asc').limit(page).offset(window * page).build();

export const qRowById = (db: Db, table: string, id: any) =>
  db.query(table).where({ id: toRecordId(String(id)) }).one().build();

// Same text as qStatic: must be deduped away (qStatic sorts first).
export const qStaticTwin = (db: Db) =>
  db.query('game').select('id', 'white').orderBy('sort_index', 'asc').limit(20).build();

export async function rRemote(db: Db, id: string): Promise<any[]> {
  return db.useRemote('SELECT * FROM game WHERE id = $id', { id });
}

export const NOT_A_QUERY = 42;

export const allowlistSamples = {
  qDynamicWhere: [[{ database: 'game_database:abc' }], [{ white: 'player_name:x', black: 'player_name:y' }]],
};
