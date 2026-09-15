type Db = any;

export const qFine = (db: Db) => db.query('user').build();

export const qThrows = (_db: Db) => {
  throw new Error('boom');
};

export const qMisfiled = (db: Db, id: string) => db.useRemote('SELECT * FROM user WHERE id = $id', { id });

export const qAsync = async (db: Db) => db.query('user').build();
