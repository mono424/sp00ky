-- ==================================================
-- COMMON REMOTE FUNCTIONS
-- ==================================================

-- Prove this session is still watching a SHARED incantation.
--
-- Any subscriber may call it. The gate is `_00_query`'s auth-scoped update
-- rule, not the registrant's `session::id()` — which is the whole point:
-- under the old session-scoped rule only the most recent registrant matched,
-- so every other tab's heartbeat silently updated zero rows and returned
-- success, and the shared row expired out from under tabs that were still
-- watching.
--
-- Refreshes the row-level `lastActiveAt` (the TTL sweep's authority) AND
-- re-stamps this session in `subscribers`, dropping its own previous entry and
-- everyone else's expired ones in the same atomic statement — so there is no
-- read-modify-write round trip and the array cannot grow across reconnects.
--
-- Idempotent and safe to lose: the client heartbeats at 0.9 * ttl, so an entry
-- is re-stamped well before `seenAt + ttl` would drop it.
--
-- No PERMISSIONS clause: a function body runs as the CALLER, so the table rule
-- is the real gate. (Contrast the fn::feature::* block, where the clause is
-- load-bearing.)
DEFINE FUNCTION OVERWRITE fn::query::heartbeat($id: record) {
    LET $sid = <string>session::id();
    LET $now = time::now();
    UPDATE $id SET
        lastActiveAt = $now,
        subscribers  = array::append(
            array::filter(subscribers ?? [], |$s|
                <string>$s.id != $sid AND <datetime>$s.seenAt + ttl > $now
            ),
            { id: $sid, seenAt: $now }
        );
};

-- Release THIS session's claim on a shared incantation.
--
-- Deletes the row — and therefore, via the `_00_dbsp_cleanup` event, the SSP
-- view and every `_00_list_ref` edge — ONLY when no other session is still
-- watching. Before this existed the client issued a bare `DELETE $id`, which
-- would have torn the view down for every other tab; in practice `_00_query`
-- granted no delete permission, so it silently did nothing at all and the TTL
-- sweep was the only reclamation that ever ran.
--
-- Stale entries are pruned on the way past, so a tab that crashed without
-- unsubscribing cannot keep a view alive forever. If this leaves the set
-- empty the row goes now; otherwise `lastActiveAt` and the sweep remain the
-- backstop.
--
-- Best-effort, not a guarantee: two tabs closing at the same instant can each
-- observe the other and neither delete, deferring release to TTL. That is
-- benign — the set is advisory — so do not build anything that assumes the
-- eager path is deterministic.
DEFINE FUNCTION OVERWRITE fn::query::unsubscribe($id: record) -> object {
    LET $sid = <string>session::id();
    LET $now = time::now();
    LET $row = (SELECT subscribers, ttl FROM ONLY $id);
    RETURN IF $row = NONE {
        { released: false, remaining: 0 }
    } ELSE {
        LET $rest = array::filter($row.subscribers ?? [], |$s|
            <string>$s.id != $sid AND <datetime>$s.seenAt + $row.ttl > $now
        );
        IF array::len($rest) = 0 {
            DELETE $id;
            { released: true, remaining: 0 }
        } ELSE {
            UPDATE $id SET subscribers = $rest;
            { released: false, remaining: array::len($rest) }
        }
    };
};

-- Hand back a page's claim on a view when the page goes away, without tearing
-- the view down.
--
-- Sent by the browser client from `pagehide` as a keepalive HTTP request,
-- because a WebSocket frame sent during unload is not guaranteed to flush. An
-- HTTP request runs in a fresh session whose `session::id()` matches no
-- subscriber entry, which is why `fn::query::unsubscribe` released nothing on
-- that path: the page passes the WebSocket session ids it held as `$sids`
-- instead. Naming another session only reaches rows the caller could already
-- update or delete, so the argument grants nothing the table rule does not.
--
-- It never deletes. When nobody else is watching it pulls `lastActiveAt` back
-- so the row expires one minute from now, and the TTL sweep reclaims it on a
-- pass after that. A reload of the same tab keeps its query salt and
-- re-registers onto this very row; registration refreshes `lastActiveAt`, which
-- cancels the release and keeps the SSP view warm. Deleting here would hand
-- every reload a cold view. A beacon that lands after the reload's
-- registration finds the new session's entry and changes nothing but its own.
--
-- Never extends: a row already due sooner keeps its expiry.
DEFINE FUNCTION OVERWRITE fn::query::leave($id: record, $sids: array<string>) -> object {
    LET $now = time::now();
    LET $row = (SELECT subscribers, ttl, lastActiveAt FROM ONLY $id);
    RETURN IF $row = NONE {
        { leaving: false, remaining: 0 }
    } ELSE {
        LET $rest = array::filter($row.subscribers ?? [], |$s|
            <string>$s.id NOTINSIDE $sids AND <datetime>$s.seenAt + $row.ttl > $now
        );
        LET $expiry = $now + 1m - $row.ttl;
        IF array::len($rest) = 0 AND $expiry < $row.lastActiveAt {
            UPDATE $id SET subscribers = $rest, lastActiveAt = $expiry;
        } ELSE {
            UPDATE $id SET subscribers = $rest;
        };
        { leaving: array::len($rest) = 0, remaining: array::len($rest) }
    };
};
