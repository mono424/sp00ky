<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="apps/landing-page/public/logo.svg" />
  <source media="(prefers-color-scheme: light)" srcset="apps/landing-page/public/logo_dark.svg" />
  <img src="apps/landing-page/public/logo_dark.svg" alt="Sp00ky Logo" width="200" />
</picture>

**The Reactive, Local-First Framework for SurrealDB**

> **⚠️ Under active development — not production-ready. APIs may change without notice.**

[![npm version](https://img.shields.io/npm/v/@spooky-sync/core?label=%40spooky-sync%2Fcore&color=cb3837)](https://www.npmjs.com/package/@spooky-sync/core)
[![npm downloads](https://img.shields.io/npm/dm/@spooky-sync/core)](https://www.npmjs.com/package/@spooky-sync/core)
[![bundle size](https://img.shields.io/bundlejs/size/@spooky-sync/core@canary)](https://bundlejs.com/?q=%40spooky-sync%2Fcore%40canary)
[![npm version](https://img.shields.io/npm/v/@spooky-sync/cli?label=%40spooky-sync%2Fcli&color=cb3837)](https://www.npmjs.com/package/@spooky-sync/cli)
[![license](https://img.shields.io/github/license/mono424/sp00ky)](https://github.com/mono424/sp00ky)
[![stars](https://img.shields.io/github/stars/mono424/sp00ky)](https://github.com/mono424/sp00ky/stargazers)
[![last commit](https://img.shields.io/github/last-commit/mono424/sp00ky)](https://github.com/mono424/sp00ky/commits/main)
[![Docker SSP](https://img.shields.io/docker/v/mono424/spooky-ssp?label=spooky-ssp&logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/mono424/spooky-ssp)
[![Docker Scheduler](https://img.shields.io/docker/v/mono424/spooky-scheduler?label=spooky-scheduler&logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/mono424/spooky-scheduler)
[![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-FF00A0?logo=surrealdb&logoColor=white)](https://surrealdb.com/)

[Documentation](https://mono424.github.io/sp00ky/) · [Example App](example/app-solid) · [CLI](https://www.npmjs.com/package/@spooky-sync/cli) · [Contributing](#contributing)

</div>

## Features

- **Live Queries** — Your UI updates instantly when data changes
- **Local-First** — Works offline using IndexedDB, syncs when back online
- **End-to-End Type Safety** — Generated TypeScript definitions from your SQL schema
- **Optimistic UI** — Immediate feedback for user actions while syncing in the background

## Quick Start

### Install

```bash
pnpm add @spooky-sync/client-solid
```

### Generate Types with CLI

```bash
npx @spooky-sync/cli generate
```

### Usage (SolidJS)

```tsx
import { useQuery } from '@spooky-sync/client-solid';
import { db } from './db';

const ThreadList = () => {
  const threads = useQuery(() => db.query('thread').select('*').all());

  return (
    <ul>
      <For each={threads.data}>{(thread) => <li>{thread.title}</li>}</For>
    </ul>
  );
};
```

## Packages

| Package                                                                                  | Description                                        |
| ---------------------------------------------------------------------------------------- | -------------------------------------------------- |
| [`@spooky-sync/core`](https://www.npmjs.com/package/@spooky-sync/core)                   | Core client SDK — sync engine, caching, reactivity |
| [`@spooky-sync/client-solid`](https://www.npmjs.com/package/@spooky-sync/client-solid)   | SolidJS bindings (`useQuery`, etc.)                |
| [`@spooky-sync/query-builder`](https://www.npmjs.com/package/@spooky-sync/query-builder) | Type-safe query builder                            |
| [`@spooky-sync/cli`](https://www.npmjs.com/package/@spooky-sync/cli)                     | CLI for schema generation                          |
| [`@spooky-sync/query-allowlist`](https://www.npmjs.com/package/@spooky-sync/query-allowlist) | Records the query allowlist from an app's `q*` module (`spky generate`) |
| [`mono424/spooky-ssp`](https://hub.docker.com/r/mono424/spooky-ssp)                     | SSP sidecar (Docker)                               |
| [`mono424/spooky-scheduler`](https://hub.docker.com/r/mono424/spooky-scheduler)          | Scheduler service, and the operator dashboard and MCP server it serves on port 9668 (Docker) |

## Example App

Check out the full-featured reference app built with SolidJS:

```bash
cd example/app-solid && pnpm install && pnpm dev
```

## Documentation

- [Grok Bot template recipe](https://mono424.github.io/sp00ky/docs/reference/grok-bot) — source material for a public Bot template under `apps/grok-bot`

Full documentation is available at **[mono424.github.io/sp00ky](https://mono424.github.io/sp00ky/)**.

- [Secrets in migrations](docs/migration-secrets.md) — inject vault secrets into
  schema via `{{KEY}}` placeholders, and `spky env set --file`.

## Contributing

Contributions are welcome! This is a monorepo — see the individual package directories under `packages/` for details.

## License

[MIT](LICENSE)

---

## Star History

<a href="https://www.star-history.com/?repos=mono424%2Fsp00ky&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=mono424/sp00ky&type=Date&theme=dark" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=mono424/sp00ky&type=Date" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=mono424/sp00ky&type=Date" />
 </picture>
</a>

---

<div align="center">
If you find Sp00ky useful, consider giving it a ⭐ on <a href="https://github.com/mono424/sp00ky">GitHub</a>!
</div>
