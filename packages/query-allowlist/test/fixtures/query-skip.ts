type Db = any;

export const qFine = (db: Db) => db.query('user').build();

export const qThrows = (_db: Db) => {
  throw new Error('boom');
};

export const allowlistSkip = ['qThrows'];
export const allowlistWhereAny = ['qFine'];
