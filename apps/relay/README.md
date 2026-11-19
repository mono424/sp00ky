# sp00ky-relay

A small WebSocket relay for the devices of one user.

The browsers and devices of ONE user join that user's room and send each other
small JSON messages (optimistic updates). The relay forwards them without
reading them. It keeps everything in memory: no disk, no database of its own,
no durability. A lost message is harmless, the normal sync path still delivers
the change, the relay only makes it show up sooner on the user's other devices.

- one room per user, named by the user id the database gives for the session
  token, so a client can only ever join its own room;
- forwarding never waits for a slow receiver;
- every limit is bounded and configurable (frame size, rate, room size, queue
  length, session length).

## Run it

```sh
SPKY_DB_URL=ws://127.0.0.1:8000 SPKY_DB_NS=main SPKY_DB_NAME=main \
    cargo run -p sp00ky-relay
```

It listens on `0.0.0.0:8668`: `GET /room` is the WebSocket, `GET /health`
answers `200 ok`. It stops on SIGTERM or ctrl-c. Rooms are in memory only, so
the open sockets simply close with the process and the clients reconnect.

### Configuration

All of it comes from the environment. A missing required variable, or a limit
that does not parse, stops the start with a message that names the variable.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPKY_DB_URL` | required | The SurrealDB endpoint, as any sp00ky service gets it: `ws://`, `wss://`, `http://` or `https://`, with or without a trailing `/` or `/rpc`. |
| `SPKY_DB_NS` | required | Namespace. A token of another namespace is refused. |
| `SPKY_DB_NAME` | required | Database. A token of another database is refused. |
| `SPKY_RELAY_LISTEN_ADDR` | `0.0.0.0:8668` | Address to listen on. |
| `SPKY_RELAY_MAX_FRAME_BYTES` | `65536` | Largest frame a client may send. |
| `SPKY_RELAY_MAX_ROOM_PEERS` | `16` | Members per room. |
| `SPKY_RELAY_RATE_BURST` | `400` | Frames one connection may send in a burst. |
| `SPKY_RELAY_RATE_PER_SEC` | `200` | Frames per second one connection may keep sending. |
| `SPKY_RELAY_QUEUE_FRAMES` | `256` | Frames that may wait to be written to one connection. |
| `SPKY_RELAY_SESSION_SECS` | `3600` | Longest session before the client is told to `reauth`. |
| `SPKY_RELAY_ALLOWED_ORIGINS` | empty (any) | Comma separated origins that may upgrade, for example `https://app.example.com,http://localhost:5173`. |
| `RUST_LOG` | `sp00ky_relay=info,warn` | Log filter. `sp00ky_relay=debug` logs every join and leave. |
| `SPKY_LOG_FORMAT` | unset | `compact` drops the timestamp, as in the other services. |

The timers are fixed in the binary: hello within 10 s, a server ping every
25 s, idle close after 60 s, and 1000 frames dropped in a row by the rate limit
before the connection is closed. They are fields of `Config` for whoever embeds
the library, which is how the tests shorten them.

### Docker

```sh
docker build -f apps/relay/Dockerfile -t mono424/spooky-relay:dev .   # from the repo root
docker run --rm -p 8668:8668 \
    -e SPKY_DB_URL=ws://surrealdb:8000 -e SPKY_DB_NS=main -e SPKY_DB_NAME=main \
    mono424/spooky-relay:dev
```

The image is one static binary in an empty image, built from this crate alone
with the workspace's lock file. There is no shell in it, so a health check has
to come from outside (`GET /health`).

## Protocol v1

JSON text frames over one WebSocket at `GET /room`. A binary frame is a
protocol error. Fields this version does not know are ignored, so a later
client may send more.

### 1. Hello

The client's first frame, within 10 s of the upgrade:

```json
{"t":"hello","v":1,"token":"<session token>","peer":"<id>"}
```

- `v` is the number `1`.
- `token` is the client's SurrealDB session token. The relay asks the database
  who it belongs to (see [Who is the user](#who-is-the-user)).
- `peer` names this connection inside the room: 1 to 64 characters of
  `[A-Za-z0-9_-]`. Use an id that is unique per tab or app instance, see
  `replaced` below.

The answer is a welcome, and every other member of the room gets a join:

```json
{"t":"welcome","user":"user:abc","peer":"<id>","peers":["<the other peer ids in the room>"]}
{"t":"join","peer":"<id>"}
```

The welcome is always the first frame a client gets. `peers` is sorted and
never contains the client itself. From then on `peers`, plus every `join`,
minus every `leave`, is exactly who is in the room.

A hello that is refused gets an error frame and the connection is closed:

```json
{"t":"error","code":"<code>","message":"<for a human, do not parse>"}
```

Every error frame, at any time, has this shape and is the last frame on its
connection.

| `code` | When | What a client should do |
| --- | --- | --- |
| `bad_request` | The hello is malformed (not JSON, not a hello, no `token`, a bad `peer`, an unsupported `v`), did not come within 10 s, or was a binary frame. After the welcome: any frame outside the protocol. | Nothing to retry, it is a client bug. |
| `unauthorized` | The database says the token is invalid, expired, belongs to no user, or belongs to another database. | Get a fresh session token first. |
| `unavailable` | The relay could not reach the database to check the token. | Retry later with the same token, with backoff. |
| `room_full` | The room already has the maximum number of members (16). | Retry later, with backoff. |
| `replaced` | Another connection said hello with the same `peer` and took the seat. | Do not reconnect with the same peer id by itself: two connections that both do would replace each other forever. |
| `rate_limited` | 1000 frames in a row were over the rate limit. | Reconnect with backoff and send less. |
| `reauth` | The session is older than the maximum (1 hour). | Reconnect at once with a fresh token. |

**Replacement.** A hello whose `peer` is already in the room replaces the old
connection: the old one gets `{"t":"error","code":"replaced",...}` and is
closed, the new one gets its welcome, and the others get a `leave` and then a
`join` for that peer. They have to: a client that reconnects before the relay
noticed its old socket was dead (a phone changing networks) starts out knowing
nothing, and the others only tell a member what they have when it joins.
Replacing a member of a full room is not an extra member and is not refused.

### 2. After the welcome

**msg** is the only frame that is forwarded:

```json
{"t":"msg","to":["p2","p3"],"d":<any JSON value>}
```

Each named member gets

```json
{"t":"msg","from":"<the sender's peer id>","d":<the same value>}
```

- `d` is required and may be any JSON value, `null` included. It is forwarded
  byte for byte: spacing, key order, escapes and number spelling inside the
  value arrive exactly as sent. The relay never parses it.
- `to` absent or `null` means every other member. An empty list means nobody.
- Unknown peers in `to` are skipped silently, a peer named twice gets the frame
  once, and the sender never gets its own frame back, also not when it names
  itself.
- Frames from one sender reach one receiver in the order they were sent.
- There is no acknowledgement and no replay: a member that joins later, or is
  not connected, never sees the frame.

**ping** is for clients that cannot see WebSocket pings (browsers):

```json
{"t":"ping"}
```

answers, to the sender only,

```json
{"t":"pong"}
```

**leave**: when a member's connection ends, for whatever reason, the others get

```json
{"t":"leave","peer":"<id>"}
```

A room with no members is removed from memory.

Any other frame, a `msg` without `d`, a `to` that is not a list of strings, or
a `hello` after the welcome gets `{"t":"error","code":"bad_request",...}` and
the connection is closed.

### 3. Liveness and limits

Defaults in brackets.

- **Ping.** The relay sends a WebSocket Ping every [25 s] and closes a
  connection it has heard nothing from, any frame and the Pong included, for
  [60 s]. Browsers answer the Ping by themselves. An idle close has no error
  frame, only the WebSocket Close.
- **Frame size** [65536 bytes], for a frame and for a whole message. A larger
  one closes the connection without an error frame.
- **Rate limit** per connection, a token bucket [burst 400, refill 200 frames
  a second]. A frame over the limit is dropped silently. [1000] dropped frames
  in a row close the connection with `rate_limited`. Every frame after the
  hello counts, a `ping` too.
- **Room size** [16 members]: a hello into a full room is refused with
  `room_full`.
- **Outbound queue** [256 frames] per connection. A member whose queue is full
  is disconnected at once, with no error frame and no Close (a browser sees
  close code 1006): a stuck device must not grow memory or hold up the others.
  They get its `leave`, and it may reconnect.
- **Session length** [3600 s] from the welcome, however busy the connection
  is: then `reauth` and close, so a session that was revoked does not stay in
  a room for ever.
- **Origin allowlist**, optional. When it is set, an upgrade whose `Origin`
  header is not in the list gets `403`. An upgrade with no `Origin` header at
  all is let through: browsers always send one, so that is a native client,
  which the list is not about and which could forge the header anyway. The
  token is what protects a room, the list only keeps other websites from using
  the relay.

### Closing

After an error frame the relay sends a WebSocket Close frame without a status
code (a browser reports code 1005) and closes the connection once the client
has answered it, or after 2 s. A client should therefore read the error frame
in `onmessage`, the close event carries no reason.

## Who is the user

The relay holds no credential. For every hello it asks SurrealDB, as the
caller:

```
POST {SPKY_DB_URL as http(s)}/rpc
Authorization: Bearer <token>
surreal-ns: <SPKY_DB_NS>
surreal-db: <SPKY_DB_NAME>
Accept: application/json
Content-Type: application/json

{"id":1,"method":"query","params":["RETURN [$auth.id, session::ns(), session::db()];",{}]}
```

The room is `$auth.id`, for example `user:abc`.

- HTTP 401 or 403, a JSON-RPC `error`, a statement that is not `OK`, or an
  `$auth.id` that is not a non-empty string (a system user and an anonymous
  session have none) is `unauthorized`.
- The namespace and database in the answer have to be the configured ones,
  otherwise `unauthorized`. This matters: SurrealDB takes the namespace and
  database of a session from the token, not from the two headers, so
  `RETURN $auth.id;` alone accepts a token of ANY database on the same
  instance. The owner of another database there can define an access method
  that signs a token for any record id, and would be let into the room of the
  user with that id. Checked against SurrealDB 3.3.5 and 4.0.1.
- A transport error, a timeout (10 s), HTTP 5xx, any other unexpected status
  or an answer that is not what SurrealDB sends is `unavailable`. The relay
  only says `unauthorized` when the database itself did, because a client
  drops its session on it.
- On SurrealDB 3.x `$auth.id` is read with the user's own permissions: the
  user table has to let a user select its own record.
- A token is never logged.

## What it deliberately does not do

- **No storage.** Nothing is written anywhere, nothing survives a restart, and
  nothing is replayed to a member that was away.
- **No reading of payloads.** `d` is copied from one socket to the others. The
  relay does not validate it, does not apply permissions to it, and knows
  nothing of tables or records. Whatever a device sends, the user's other
  devices have to treat as a hint from themselves, never as the truth: the
  database stays the authority.
- **No cross-user rooms yet.** A room is one user's own devices. Two users
  cannot share a room, and there is no way to name a room in the hello.
- **No clustering.** A room lives in the memory of one process, so all devices
  of a user have to reach the same instance. Running several instances needs
  routing by user in front of them, which is not built.
- **No protection against a flood of connections.** Each hello costs one query
  to the database before anything is known about the caller. Limit connections
  per address in the reverse proxy.

## Trust

Payloads cross the relay in plain form, in memory, for as long as it takes to
copy them to the other sockets. The relay does not speak TLS: it has to sit
behind a reverse proxy that terminates TLS (`wss://`), so on the wire the
payloads and the session token in the hello are only protected by that proxy.
Whoever runs the relay or the proxy could read both. The proxy has to pass the
WebSocket upgrade and the `Origin` header through and must not close a
connection that is idle for less than the 25 s between two pings.

## Tests

```sh
cargo test -p sp00ky-relay
cargo clippy -p sp00ky-relay --all-targets -- -D warnings
```

`tests/relay.rs` runs the protocol over real sockets with a fake verifier,
`tests/auth.rs` runs the SurrealDB verifier against a stand-in for `/rpc`. One
test there is ignored by default because it needs a real SurrealDB. It also
proves that a token of another database is refused:

```sh
docker run --rm -p 8000:8000 surrealdb/surrealdb:v4.0.1 start --user root --pass root memory
SPKY_RELAY_TEST_SURREAL=http://127.0.0.1:8000 cargo test -p sp00ky-relay --test auth -- --ignored
```
