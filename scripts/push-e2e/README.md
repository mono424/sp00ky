# Web Push browser checks

Manual end-to-end checks for Web Push against a real push service: a real
Chrome subscribes through FCM, and the pushes come from a `spky dev` stack
built from this checkout. Not part of CI (needs Chrome, Docker and the
network).

```bash
cargo build -p sp00ky-cli -p ssp-server -p scheduler
pnpm --filter @spooky-sync/core build
cd scripts/push-e2e/project && ../../../target/debug/spky dev -y --apply-migrations --clean-db
```

Then, in another shell from `scripts/push-e2e/`:

```bash
node e2e.mjs
```

```bash
node e2e-live.mjs
```

`e2e.mjs` covers a content rule (`with`, `except`, templated topic and url),
`fn::push::test`, `spky push send` and throttling. `e2e-live.mjs` covers a
nudge rule rendered in the service worker from live data, including the
notification closing once its row is seen.

`project/sp00ky.yml` runs `mode: cluster` with `sync.transport: changefeed`
(the scheduler hosts the engine). Switch to `mode: singlenode` and drop the
`sync:` block to exercise the standalone SSP host and the http transport.

The Chrome profile lives in the OS temp dir and is kept between runs: a brand
new profile's first FCM subscription answers 410 for a few seconds, which the
engine retries (see `fresh_subscription_grace_ms`), so the very first run is a
few seconds slower. `HEADFUL=1` shows the browser.
