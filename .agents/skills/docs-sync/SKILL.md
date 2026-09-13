---
name: docs-sync
description: >-
  Keep the sp00ky docs in sync with the code. Use this skill BEFORE finishing any
  change that adds, renames, removes or alters public behaviour: a new package,
  an exported hook/function/type, a CLI flag or subcommand, a sp00ky.yml or client
  config key, an SSP/scheduler HTTP endpoint, an env var, a schema annotation, or a
  changed default. It maps each kind of change to the doc files that must be updated
  (Astro docs site, nav config, package AGENTS.md / SKILL.md / README, root README)
  and how to verify the docs still build.
user_invocable: true
---

# Docs sync

Docs in this repo are code-adjacent and hand-written, nothing is generated from source.
A change that lands without its docs edit silently rots the site. Treat the docs edit as
part of the change, not as follow-up work.

## Where docs live

| Surface | Path | What belongs there |
|---|---|---|
| Docs site (published) | `apps/landing-page/src/pages/docs/**/*.mdx` | Everything user-facing. Astro + MDX. |
| Sidebar / prev-next order | `apps/landing-page/src/config/nav.ts` | A new page is invisible until it is listed here. |
| Marketing pages | `apps/landing-page/src/pages/*.astro`, `src/config/features.ts` | Feature claims, hero code snippets. |
| Per-package agent guide | `packages/<pkg>/AGENTS.md` | Public API surface for coding agents. Every client-facing package ships one. |
| Per-package skill | `packages/<pkg>/skills/<name>/SKILL.md` | Same content, skill-shaped, for agents that load skills. |
| Package readme / quick start | `packages/<pkg>/README.md`, `QUICK_START.md` | Install + minimal usage. |
| Root readme | `README.md` | Package list, badges, quick start snippet. |
| Deep internals | `docs/*.md`, `docs/internals/`, `docs/surrealdb-bugs/` | Architecture and upstream-bug notes. Not published. |

## What to update, by kind of change

- **New package** → docs-site page (usually a guide under `docs/guide/` or a `docs/client/` page)
  + `nav.ts` entry + `packages/<pkg>/README.md` + `QUICK_START.md` + `AGENTS.md`
  + `skills/<name>/SKILL.md` + a mention in `docs/reference/ai-agents.mdx`'s per-package list
  + root `README.md` if it is user-installable.
- **New / changed export in a client package** (hook, provider prop, option, exported type)
  → the relevant `docs/client/*.mdx` page **and** that package's `AGENTS.md` + `SKILL.md`.
  Check whether the sibling binding (`client-solid` vs `client-solid2` vs `client-dart`)
  documents the same thing and needs the same edit.
- **CLI flag or subcommand** → `docs/reference/cli.mdx`; if it changes the local workflow,
  also `docs/dev/*.mdx`.
- **`sp00ky.yml` key** → `docs/reference/config.mdx`. **Client config key** → `docs/reference/client-config.mdx`.
- **Env var** → `docs/dev/env.mdx` and, if it is set on deployed infra, `docs/cloud/env-variables.mdx`.
- **SSP or scheduler HTTP endpoint** → `docs/reference/ssp-api.mdx` / `docs/reference/scheduler-api.mdx`.
- **Schema annotation or codegen output** → `docs/schema/*.mdx`.
- **Jobs, schedules, workflows behaviour** → `docs/jobs/*.mdx`.
- **Changed default, renamed API, removed feature** → grep the whole docs tree for the old
  name and fix every hit, including code samples:
  `grep -rn "<old name>" apps/landing-page/src README.md packages/*/AGENTS.md packages/*/README.md docs/`
- **Deprecated but still exported** → say so in the docs (deprecated alias, what replaces it),
  do not just delete the section.
- **Experimental feature** → mark it: `experimental: true` on the `nav.ts` link and the
  `<Experimental>` callout on the page.

## MDX conventions on the docs site

- Frontmatter: `layout: ../../…/layouts/DocsLayout.astro`, `title`, `description` (one sentence,
  shows in search and cards).
- Components live in `apps/landing-page/src/components/ui/`: `CodeBlock` (`code`, `lang`, `fileName`),
  `Steps` + `Step`, `Note`, `Warning`, `Experimental`, `Tabs`, `PropTable`, `CardGrid` + `LinkCard`.
  Import only the ones you use.
- Code samples are `export const someCode = <template literal>`, usually at the bottom of the
  file, rendered with `<CodeBlock code={someCode} />`.
  **Do not nest a template literal inside one.** Escaping it is error-prone and has been written
  wrong before; use string concatenation in the sample instead
  (`'post:' + crypto.randomUUID()`, not a backtick-interpolated id).
- Every new page ends with a `<CardGrid>` of next steps, and links to sibling pages with root-relative
  hrefs (`/docs/client/queries`).
- `nav.ts` order is also the prev/next footer order, so insert the entry where a reader would
  reach it, not at the end of the group.

## Finish the loop

1. Build the site, it type-checks the MDX and catches broken component imports:
   `cd apps/landing-page && pnpm build`  (expect `[build] Complete!` and a page count).
2. Confirm the new page rendered: `ls apps/landing-page/dist/docs/<path>/` and grep the built
   HTML for a distinctive line of a code sample, to prove escaping survived the build.
3. Re-grep for the old name if this was a rename, the samples inside `export const` strings are
   easy to miss.
4. Mention in the final summary which docs pages changed, so the reader knows the docs moved with
   the code.

Do not skip these because a change "looks internal". The test is whether a user or a coding agent
reading the docs would now be told something false.
