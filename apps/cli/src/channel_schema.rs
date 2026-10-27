//! Server schema generated for `-- @channel` tables.
//!
//! Channels are peer to peer (WebRTC data channels between clients). The only
//! thing the database does for them is carry the signaling that sets a
//! connection up: offers, answers and ICE candidates, as rows of one
//! server-only table that each client follows with a raw `LIVE SELECT`.
//!
//! The rows are what binds a connection to a user. `sender` is stamped by the
//! database (`VALUE $before OR $auth.id`), and an offer carries the DTLS
//! fingerprint of the connection it opens, so the peer that answers knows
//! which user is on the other end. What that user may then send is the app's
//! decision (the client's `channels.accept` rules).

use crate::annotations;

/// The signaling table. `_00_` keeps it out of sync, codegen and the SSP: it
/// is never cached locally, only read through the raw LIVE.
pub const SIGNAL_TABLE: &str = "_00_channel_signal";
/// Access method of an impersonation session. It may not open channels: the
/// peer would see the impersonated user, not the operator.
const IMPERSONATE_ACCESS: &str = "_00_impersonate";
/// Largest signaling payload (an SDP with its candidates is a few KB).
const MAX_PAYLOAD_CHARS: usize = 16 * 1024;

/// True when any table in `source` is marked `-- @channel`.
pub fn has_channel_tables(source: &str) -> bool {
    annotations::extract_table_annotations(source)
        .values()
        .any(|anns| annotations::has_annotation(anns, "channel"))
}

/// The signaling table, or `None` when the schema has no channel table.
pub fn server_ddl(source: &str) -> Option<String> {
    if !has_channel_tables(source) {
        return None;
    }
    Some(format!(
        "\n-- WebRTC signaling for `@channel` tables (hello, offer, answer, ICE)\n\
         DEFINE TABLE OVERWRITE {SIGNAL_TABLE} SCHEMAFULL\n\
         \x20 PERMISSIONS\n\
         \x20   FOR select WHERE recipient = $auth.id OR sender = $auth.id\n\
         \x20   FOR create WHERE $auth.id != NONE AND $access != \"{IMPERSONATE_ACCESS}\"\n\
         \x20   FOR update NONE\n\
         \x20   FOR delete WHERE sender = $auth.id;\n\
         DEFINE FIELD OVERWRITE sender ON TABLE {SIGNAL_TABLE} TYPE record VALUE $before OR $auth.id READONLY;\n\
         DEFINE FIELD OVERWRITE recipient ON TABLE {SIGNAL_TABLE} TYPE record READONLY;\n\
         DEFINE FIELD OVERWRITE sender_peer ON TABLE {SIGNAL_TABLE} TYPE string ASSERT string::len($value) <= 64 READONLY;\n\
         DEFINE FIELD OVERWRITE recipient_peer ON TABLE {SIGNAL_TABLE} TYPE option<string> ASSERT $value = NONE OR string::len($value) <= 64 READONLY;\n\
         DEFINE FIELD OVERWRITE kind ON TABLE {SIGNAL_TABLE} TYPE string ASSERT $value IN ['hello', 'offer', 'answer', 'ice'] READONLY;\n\
         DEFINE FIELD OVERWRITE payload ON TABLE {SIGNAL_TABLE} TYPE option<string> ASSERT $value = NONE OR string::len($value) <= {MAX_PAYLOAD_CHARS} READONLY;\n\
         DEFINE FIELD OVERWRITE at ON TABLE {SIGNAL_TABLE} TYPE datetime VALUE $before OR time::now() READONLY;\n\
         DEFINE INDEX OVERWRITE {SIGNAL_TABLE}_recipient ON TABLE {SIGNAL_TABLE} FIELDS recipient;\n\
         DEFINE INDEX OVERWRITE {SIGNAL_TABLE}_sender ON TABLE {SIGNAL_TABLE} FIELDS sender, at;\n\
         -- A signal is only useful for the seconds a connection takes to set up.\n\
         -- Each new one sweeps its sender's stale rows, so the table stays tiny.\n\
         DEFINE EVENT OVERWRITE {SIGNAL_TABLE}_sweep ON TABLE {SIGNAL_TABLE} WHEN $event = \"CREATE\" THEN {{\n\
         \x20 DELETE {SIGNAL_TABLE} WHERE sender = $after.sender AND at < time::now() - 2m;\n\
         }};\n"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_channel_table_generates_nothing() {
        let src = "DEFINE TABLE call SCHEMAFULL;\n";
        assert!(server_ddl(src).is_none());
    }

    #[test]
    fn a_channel_table_generates_the_signal_table() {
        let src = "-- @channel\nDEFINE TABLE call SCHEMAFULL;\n";
        let ddl = server_ddl(src).expect("signal table");
        assert!(ddl.contains("DEFINE TABLE OVERWRITE _00_channel_signal"));
        assert!(ddl.contains("VALUE $before OR $auth.id"), "sender must be stamped by the database");
        assert!(ddl.contains("FOR update NONE"));
    }

    #[test]
    fn the_signal_ddl_parses() {
        let src = "-- @channel\nDEFINE TABLE call SCHEMAFULL;\n";
        let ddl = server_ddl(src).unwrap();
        surrealdb_core::syn::parse_with_capabilities(&ddl, &surrealdb_core::dbs::Capabilities::all())
            .expect("generated DDL must parse");
    }
}
