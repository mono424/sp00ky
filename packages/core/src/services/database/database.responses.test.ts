import { describe, expect, it } from 'vitest';
import { AbstractDatabaseService } from './database';
import { createDatabaseEventSystem, DatabaseEventTypes } from './events/index';

class Fake extends AbstractDatabaseService {
  protected eventType = DatabaseEventTypes.RemoteQuery;
  constructor(client: any) {
    const noop = () => {};
    const logger: any = { debug: noop, trace: noop, error: noop, info: noop, warn: noop, child: () => logger };
    super(client, logger, createDatabaseEventSystem());
  }
  async connect() {}
}

describe('queryResponses', () => {
  it('maps per-statement successes and failures without rejecting', async () => {
    const client = {
      query: () => ({
        responses: async () => [
          { success: true, result: [1] },
          { success: false, error: new Error('denied') },
          { success: false, error: 'plain' },
        ],
      }),
    };
    const out = await new Fake(client).queryResponses('A; B; C');
    expect(out).toEqual([
      { status: 'OK', result: [1] },
      { status: 'ERR', error: 'denied' },
      { status: 'ERR', error: 'plain' },
    ]);
  });
  it('still rejects on a transport failure', async () => {
    const client = {
      query: () => ({
        responses: async () => {
          throw new Error('socket closed');
        },
      }),
    };
    await expect(new Fake(client).queryResponses('A')).rejects.toThrow('socket closed');
  });
});
