# @spooky-sync/backend

The sp00ky backend SDK: durable workflows with Cloudflare Workflows' API
(`WorkflowEntrypoint`, `step.do`, `step.sleep`, `step.waitForEvent`) on sp00ky's own job engine.
The same backend code runs on a VM, under `spky dev` and as a Cloudflare Worker backend.

```bash
pnpm add @spooky-sync/backend
```

```ts
import { WorkflowEntrypoint, withWorkflows, type WorkflowEvent, type WorkflowStep } from '@spooky-sync/backend';

export class Ingest extends WorkflowEntrypoint<Env, { id: string }> {
  async run(event: WorkflowEvent<{ id: string }>, step: WorkflowStep) {
    const pages = await step.do('count pages', async () => countPages(event.payload.id));
    await step.sleep('settle', '1 minute');
    return { pages };
  }
}

export default withWorkflows(app, { INGEST: Ingest });
```

```yaml
# sp00ky.yml
apps:
  api:
    type: backend
    method: { type: outbox, table: job, schema: ./schema/src/outbox/api.surql }
    workflows:
      - { binding: INGEST, class: Ingest }
```

Docs: https://sp00ky.cloud/docs/backend/durable-workflows
