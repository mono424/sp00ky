import { OpenAPIHono, createRoute, z as zOpenApi } from '@hono/zod-openapi';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { bearerAuth } from 'hono/bearer-auth';
import { z } from 'zod';
import { generateText, Output } from 'ai';
import { anthropic } from '@ai-sdk/anthropic';
import { DateTime, RecordId, Surreal } from 'surrealdb';
import {
  WorkflowEntrypoint,
  withWorkflows,
  type Workflow,
  type WorkflowEvent,
  type WorkflowStep,
} from '@spooky-sync/backend';

const model = anthropic('claude-haiku-4-5');

type DigestParams = { id: string; flaky?: boolean };
type Env = { DIGEST: Workflow<DigestParams> };

export const app = new OpenAPIHono<{ Bindings: Env }>();

app.use(logger());
// CORS for the dev port.
app.use('/*', cors({ origin: '*', allowHeaders: ['Authorization', 'Content-Type'], allowMethods: ['POST', 'OPTIONS'] }));

// Liveness, outside the auth gate: the deploy healthcheck, and on a Cloudflare
// project an `expose`d route when the api runs as a Worker backend.
app.get('/health', (c) => c.text('ok'));

// Static-secret bearer auth gates every route. (Invite links are accepted
// in the database itself: see `share_accept` in the schema.)
app.use(
  '/*',
  bearerAuth({
    token: process.env.API_AUTH_TOKEN || 'THIS_IS_TOP_SECRET',
  })
);

export const db = new Surreal({
  codecOptions: {
    valueDecodeVisitor(value) {
      if (value instanceof RecordId) {
        return value.table.name + ':' + value.id.toString();
      }

      if (value instanceof DateTime) {
        return value.toDate();
      }

      return value;
    },
  },
});

/** Connect and sign in to SurrealDB (the `SPKY_DB_*` sp00ky injects). */
export async function connect() {
  const surrealUrl = process.env.SPKY_DB_URL || 'http://127.0.0.1:8000/rpc';
  await db.connect(surrealUrl);

  await db.use({
    namespace: process.env.SPKY_DB_NS || 'main',
    database: process.env.SPKY_DB_NAME || 'example',
  });

  // A VM backend gets a root login; a Worker backend on the shared free DB
  // gets its project's namespace owner (SPKY_DB_LEVEL=namespace).
  const credentials = {
    username: process.env.SPKY_DB_USER || 'root',
    password: process.env.SPKY_DB_PASS || 'root',
  };
  await db.signin(
    process.env.SPKY_DB_LEVEL === 'namespace'
      ? { ...credentials, namespace: process.env.SPKY_DB_NS || 'main' }
      : credentials
  );
}

const ErrorSchema = zOpenApi.object({
  error: zOpenApi.string(),
});

const spookifyRoute = createRoute({
  method: 'post',
  path: '/spookify',
  request: {
    body: {
      content: {
        'application/json': {
          schema: zOpenApi.object({
            id: zOpenApi.string().openapi({
              example: 'thread:kv9b3b...',
            }),
          }),
        },
      },
    },
  },
  responses: {
    200: {
      description: 'ok',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorSchema,
        },
      },
      description: 'Thread not found',
    },
    500: {
      content: {
        'application/json': {
          schema: ErrorSchema,
        },
      },
      description: 'Internal server error',
    },
  },
});

export const parseId = (id: string) => {
  if (id.startsWith('thread:')) {
    return new RecordId(id.substring(0, 6), id.substring(7));
  }
  const [table, ...rest] = id.split(':');
  if (table !== 'thread') {
    throw new Error('Invalid table');
  }
  if (!rest.length) {
    throw new Error('Invalid id');
  }
  return new RecordId(table, rest.join(':'));
};

app.openapi(spookifyRoute, async (c) => {
  const { id } = c.req.valid('json');
  const recordId = parseId(id);

  try {
    await connect();

    // 1. Query the record
    // db.query returns Promise<T>. For one statement, T should be [ResultType]
    // ResultType is an array of records.
    type ThreadRecord = { id: string; title: string; content: string };
    const [result] = await db.query<[ThreadRecord[]]>('SELECT id, title, content FROM $id', {
      id: recordId,
    });

    // result is ThreadRecord[]
    const record = result && result.length > 0 ? result[0] : null;

    if (!record) {
      return c.json({ error: 'Thread not found' }, 404);
    }

    // 2. Generate sp00ky content with AI
    // Warning: Requires ANTHROPIC_API_KEY environment variable
    // Use z from 'zod' here
    // Define the output structure
    type Sp00kySuggestion = {
      title_suggestion: string;
      content_suggestion: string;
    };

    const resultSchema = z.object({
      title_suggestion: z.string().describe('A sp00ky version of the original title'),
      content_suggestion: z.string().describe('A sp00ky, eerie version of the original content'),
    }) as z.Schema<Sp00kySuggestion>;

    const resultAI = await generateText({
      model,
      output: Output.object({
        schema: resultSchema as any,
      }),
      prompt: `Sp00kify the following thread content. Make it sound haunted, eerie, and fit for a ghost story.
      
      Original Title: ${record.title}
      Original Content: ${record.content}
      `,
    });

    if (!resultAI.output) {
      throw new Error('Failed to generate output');
    }

    const { title_suggestion, content_suggestion } = resultAI.output as z.infer<
      typeof resultSchema
    >;

    // 3. Update the record
    await db.update(recordId).merge({
      title_suggestion,
      content_suggestion,
    });

    c.status(200);
    return c.text('ok');
  } catch (e: any) {
    console.error(e);
    return c.json({ error: e.message || 'Internal Server Error' }, 500);
  }
});

// ── A durable workflow ─────────────────────────────────────────────────────
// Cloudflare Workflows' API on sp00ky's own job engine (`@spooky-sync/backend`):
// each step's result is stored in `_00_wf_step`, so a crash, a retry or the
// sleep below resumes where it left off instead of starting over. Declared in
// sp00ky.yml as `apps.api.workflows`.

/** Writes "<title> (<n> words)" to a thread's title suggestion, a few seconds later. */
export class ThreadDigest extends WorkflowEntrypoint<Env, DigestParams> {
  async run(event: WorkflowEvent<DigestParams>, step: WorkflowStep) {
    const thread = await step.do('load thread', async () => {
      await connect();
      const [rows] = await db.query<[{ title: string; content: string }[]]>(
        'SELECT title, content FROM $id',
        { id: parseId(event.payload.id) }
      );
      if (!rows?.[0]) throw new Error(`thread ${event.payload.id} not found`);
      return rows[0];
    });

    const words = await step.do(
      'count words',
      { retries: { limit: 2, delay: '1 second', backoff: 'constant' } },
      async ({ attempt }) => {
        // `flaky` fails the first attempt, to show a retry.
        if (event.payload.flaky && attempt === 1) throw new Error('flaky first attempt');
        return `${thread.title} ${thread.content}`.split(/\s+/).filter(Boolean).length;
      }
    );

    // Longer than a run waits inline, so this hands over to a delayed job.
    await step.sleep('settle', '3 seconds');

    await step.do('write suggestion', async () => {
      await connect();
      await db.query('UPDATE $id SET title_suggestion = $suggestion', {
        id: parseId(event.payload.id),
        suggestion: `${thread.title} (${words} words)`,
      });
    });
    return { words };
  }
}

// Start a digest, and read where one is. Called directly (a job or a client
// with the API token), they live behind the same bearer gate as /spookify.
app.post('/digest', async (c) => {
  const { id, flaky } = await c.req.json<DigestParams>();
  const instance = await c.env.DIGEST.create({ params: { id, flaky } });
  return c.json({ instance: instance.id });
});

app.get('/digest/:instance', async (c) => {
  const instance = await c.env.DIGEST.get(c.req.param('instance'));
  return c.json(await instance.status());
});

export default withWorkflows(app, { DIGEST: ThreadDigest });
