# Security advisory: `migrate create` wrote `KEY '[REDACTED]'` into DEFINE ACCESS

## Summary

`spky migrate create` built the generated migration by dumping the schema from
an ephemeral SurrealDB via `INFO FOR DB`. SurrealDB scrubs JWT keys in that
output to the literal `[REDACTED]`. When a `DEFINE ACCESS` had **no explicit JWT
key** in its schema source (so SurrealDB had auto-generated a random key), the
generated migration kept the redacted placeholder verbatim:

```surql
DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP {...} SIGNIN (...)
  WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]'
  DURATION FOR TOKEN 15m, FOR SESSION 4w2d;
```

**Applying that migration sets the JWT signing key to the 10-byte ASCII string
`[REDACTED]`.** Anyone who knows this (the migration is committed to the repo)
can mint a valid HS512 session token for **any record user**: sign a JWT with
the key `[REDACTED]` carrying `NS`, `DB`, `AC` (the access name) and `ID` (the
target `user:*`) claims, and SurrealDB authenticates it as that user. All
record-level permissions then apply as if the attacker were that user.

Verified on SurrealDB `v3.1.0-beta.3` and `v3.3.4`.

## Am I affected?

You are affected if **all** of these hold:

1. You ran `spky migrate create` (or `spky dev`'s auto-generate) with a CLI
   built before this fix, and
2. the `DEFINE ACCESS` in your schema source did **not** declare an explicit
   `WITH JWT ... KEY '...'` (literal or `{{VAULT}}` placeholder) — i.e. you
   relied on SurrealDB's auto-generated key, and
3. you ran `spky migrate apply` / `spky dev` / a cloud deploy against a database
   that applied that migration.

Check a live database directly:

```sh
# inspect the stored access definition
spky query "INFO FOR DB" --url <your-db-url> --cloud   # or --url/--user/--pass
```

If the `accesses` entry shows `KEY '[REDACTED]'`, the signing key **is** the
literal string `[REDACTED]` and tokens are forgeable. (SurrealDB `v3.3`+ refuses
to *apply* such a definition, but a key already stored as `[REDACTED]` by an
older server stays forgeable after you upgrade.)

Projects that injected a real key via a `{{VAULT}}` placeholder are **not**
affected — the placeholder was always preserved.

## How to rotate (invalidate forged tokens)

Re-`DEFINE` the access so SurrealDB mints a fresh random key (or supply your own
via the vault). Rotating the signing key invalidates every token signed with the
old key, so all **future** forgery attempts fail.

Pick one:

- **Random key (simplest).** Re-define the access with the JWT clause omitted;
  SurrealDB generates a new random key:

  ```surql
  DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
    SIGNUP {...} SIGNIN (...)
    DURATION FOR TOKEN 15m, FOR SESSION 4w2d;
  ```

- **Explicit key from the vault (if you want a known/rotatable key).** Set the
  secret (`spky env set JWT_SECRET ...`) and define:

  ```surql
  DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
    SIGNUP {...} SIGNIN (...)
    WITH JWT ALGORITHM HS512 KEY '{{JWT_SECRET}}'
    DURATION FOR TOKEN 15m, FOR SESSION 4w2d;
  ```

  See [migration-secrets](../migration-secrets.md) for the vault placeholder
  flow.

Apply the re-definition to every affected database.

### Important: rotation does not evict live sessions

On SurrealDB 3.1 an **already-open** WebSocket session that authenticated with a
forged token before you rotated the key **keeps working** after rotation — the
server re-checks the signature on authenticate, not on every query. So if you
suspect an active intruder:

1. Rotate the key (above), then
2. **restart the SSP / bounce all client WebSocket connections** so every client
   is forced to re-authenticate against the new key.

### Then clean up the repo

Regenerate the migration with the fixed CLI so a fresh deploy no longer ships a
redacted key:

```sh
spky migrate create fix_access_key   # generated clause is now omitted
spky lint                            # refuses any migration with KEY '[REDACTED]'
spky doctor                          # `access-keys` check fails on offenders
```

## What the CLI now does

- `spky migrate create` never writes `KEY '[REDACTED]'`: for each
  `DEFINE ACCESS` it restores the key from the schema source when one is
  declared (literal or `{{VAULT}}`), otherwise it omits the generated JWT clause
  so SurrealDB mints a random key on apply.
- `spky migrate apply` (and cloud deploy) **refuse** a committed migration that
  contains `KEY '[REDACTED]'`.
- `spky lint` and `spky doctor` flag any such migration.
