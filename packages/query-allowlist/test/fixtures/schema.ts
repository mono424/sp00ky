const id = { type: 'string' as const, recordId: true, optional: false };

export const schema = {
  tables: [
    {
      name: 'game' as const,
      columns: {
        id,
        white: { type: 'string' as const, optional: true },
        black: { type: 'string' as const, optional: true },
        database: { type: 'string' as const, optional: true },
        created_ms: { type: 'number' as const, optional: true },
        sort_index: { type: 'number' as const, optional: true },
      },
      primaryKey: ['id'] as const,
    },
    {
      name: 'player_name' as const,
      columns: {
        id,
        name: { type: 'string' as const, optional: false },
      },
      primaryKey: ['id'] as const,
    },
    {
      name: 'user' as const,
      columns: {
        id,
        username: { type: 'string' as const, optional: true },
      },
      primaryKey: ['id'] as const,
    },
  ],
  relationships: [
    { from: 'game' as const, field: 'white' as const, to: 'player_name' as const, cardinality: 'one' as const },
  ],
  backends: {},
};
