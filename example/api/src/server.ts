import { OpenAPIHono, createRoute, z as zOpenApi } from '@hono/zod-openapi';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { bearerAuth } from 'hono/bearer-auth';
import { z } from 'zod';
import { generateText, Output } from 'ai';
import { anthropic } from '@ai-sdk/anthropic';
import { DateTime, RecordId, Surreal } from 'surrealdb';

const model = anthropic('claude-haiku-4-5');

const app = new OpenAPIHono();

app.use(logger());
// CORS for the dev port.
app.use('/*', cors({ origin: '*', allowHeaders: ['Authorization', 'Content-Type'], allowMethods: ['POST', 'OPTIONS'] }));

// Static-secret bearer auth gates every route. (Invite links are accepted
// in the database itself: see `share_accept` in the schema.)
app.use(
  '/*',
  bearerAuth({
    token: process.env.API_AUTH_TOKEN || 'THIS_IS_TOP_SECRET',
  })
);

const db = new Surreal({
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

const parseId = (id: string) => {
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
    // Connect to SurrealDB
    const surrealUrl = process.env.SPKY_DB_URL || 'http://127.0.0.1:8000/rpc';
    await db.connect(surrealUrl);

    await db.use({
      namespace: process.env.SPKY_DB_NS || 'main',
      database: process.env.SPKY_DB_NAME || 'example',
    });

    await db.signin({
      username: process.env.SPKY_DB_USER || 'root',
      password: process.env.SPKY_DB_PASS || 'root',
    });

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

export default app;
