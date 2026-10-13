//! SurrealQL of the CRDT fold. Bytes cross the JSON `Db` port as base64:
//! encoded in the SELECTs, decoded in the writes. Every id is built in
//! SurrealQL from string binds, so no record-id escaping is done in Rust.

/// The canonical doc row of `($doc, $field)`. An array id, so one row per
/// field with no lookup index, and a lease taken by `CLAIM` is per field too.
pub const DOC_ID: &str = "type::record('_00_crdt_doc', [type::record($doc), $field])";

/// Ensure the doc row exists, then take (or renew) its lease. Result 2 is the
/// row when this node holds the lease, `null` when another node does.
pub fn claim() -> String {
    format!(
        "LET $id = {DOC_ID};\n\
         INSERT IGNORE INTO _00_crdt_doc {{ id: $id, doc: type::record($doc), field: $field, rev: 0 }} RETURN NONE;\n\
         UPDATE ONLY $id SET lease_by = $me, lease_until = time::now() + <duration>$lease\n\
         \x20 WHERE lease_until = NONE OR lease_until < time::now() OR lease_by = $me\n\
         \x20 RETURN rev, (IF snapshot = NONE {{ NONE }} ELSE {{ encoding::base64::encode(snapshot) }}) AS snapshot;"
    )
}

/// Statement index of the claim's result.
pub const CLAIM_RESULT: usize = 2;

/// The log rows of one document field.
pub const ROWS: &str = "SELECT <string>id AS id, peer, encoding::base64::encode(data) AS data, created_at \
     FROM type::table($log) WHERE doc = type::record($doc) AND field = $field LIMIT $limit;";

/// Publish a fold: compare-and-swap on `rev` (and our lease), then the parent's
/// text mirror and fold stamp. A lost race throws, which rolls back both.
/// `UPDATE ONLY` on the parent never recreates a record deleted meanwhile.
/// `{field}` is a plain identifier from the log table's marker.
pub fn commit(field: &str) -> String {
    format!(
        "BEGIN TRANSACTION;\n\
         LET $id = {DOC_ID};\n\
         LET $won = UPDATE ONLY $id SET snapshot = encoding::base64::decode($snapshot), rev = $rev + 1,\n\
         \x20 upto = $upto, state_hash = $hash OR NONE, folded_at = time::now(), lease_by = NONE, lease_until = NONE\n\
         \x20 WHERE rev = $rev AND lease_by = $me RETURN AFTER;\n\
         IF $won = NONE {{ THROW \"{CAS_LOST}\" }};\n\
         UPDATE ONLY type::record($doc) SET {field} = $text, {field}__crdt = $meta RETURN NONE;\n\
         COMMIT TRANSACTION;"
    )
}

/// The error a lost `commit` race throws.
pub const CAS_LOST: &str = "crdt_cas_lost";

/// Give the lease back without publishing (nothing new to fold).
pub fn release() -> String {
    format!("UPDATE {DOC_ID} SET lease_by = NONE, lease_until = NONE WHERE lease_by = $me RETURN NONE;")
}

/// Delete folded or refused rows, one statement per id so each still fires the
/// table's delete event (membership leaves every client's view).
pub const DELETE_ROWS: &str = "FOR $i IN $ids { DELETE type::record($i) RETURN NONE; };";

/// Pending rows per document field, to rebuild the tracker after a restart.
pub const PENDING: &str = "SELECT <string>doc AS doc, field, count() AS rows, math::sum(size) AS bytes, \
     time::max(created_at) AS last FROM type::table($log) GROUP BY doc, field;";

/// Rows whose parent record is gone (its delete event already dropped the
/// canonical doc).
pub const ORPHANS: &str = "DELETE type::table($log) WHERE !record::exists(doc) RETURN NONE;";

/// Presence rows nobody refreshed within the TTL.
pub const STALE_PRESENCE: &str =
    "DELETE type::table($presence) WHERE at < time::now() - <duration>$ttl RETURN NONE;";
