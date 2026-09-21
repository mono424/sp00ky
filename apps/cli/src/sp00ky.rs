use crate::annotations::has_annotation;
use crate::backend::{DeployMode, SyncTransport};
use crate::parser::{FieldDefinition, FieldType, TableSchema};
use std::collections::BTreeMap;

/// Fields whose value must never enter the sync machinery: `@crdt`, `@nosync`
/// and `@opaque` on a `DEFINE FIELD`. Excluded from the ingest event payload
/// here and from the replica/bootstrap row scans via the `sp00ky:opaque` marker
/// baked on by `schema_builder::add_opaque_field_markers`. The two must agree —
/// see the call site for what happens when they don't.
fn is_excluded_field(field_def: &FieldDefinition) -> bool {
    ["crdt", "nosync", "opaque"]
        .iter()
        .any(|name| has_annotation(&field_def.annotations, name))
}

/// A `DEFINE FIELD` on a sub-path (`errors[*]`, `settings.theme`) types part of
/// a value its parent field already carries whole. It is not a key of the
/// record, so it has no place in the `$plain_after` / `$plain_before` objects:
/// `errors[*]: $after.errors[*]` is not even valid SurrealQL, and one such line
/// fails the whole internal schema. The stock outbox template of `spky api add`
/// defines `errors[*]`, so every project scaffolded from it hit this on deploy.
fn is_sub_path(field_name: &str) -> bool {
    field_name.contains('[') || field_name.contains('.')
}

/// Generate Sp00ky events for data hashing and graph synchronization
// ... imports ...

/// The `/ingest` call every generated DB event makes, wrapped so it cannot
/// hang the user's transaction.
///
/// SurrealDB runs `DEFINE EVENT` bodies **inside the parent transaction**, and
/// `http::post` only honours a reqwest timeout when the surrounding query
/// context carries one (`fnc::util::http`). A bare call therefore has no bound
/// at all: while the scheduler was slow, a single user write held a SurrealDB
/// write transaction open indefinitely, which starved the database, which made
/// the scheduler slower — the write → scheduler → SSP → database → write cycle
/// behind "the SSP lags and goes down".
///
/// `TIMEOUT` is a statement clause, so the call has to be wrapped in a
/// statement that accepts one — a bare `http::post(...) TIMEOUT 10s` is a parse
/// error. `SELECT * FROM <call>` is the cheapest wrapper that does; verified on
/// SurrealDB 3.1.5, including that a genuine connection error still surfaces as
/// a statement error, so a refused ingest keeps aborting the write as before.
///
/// A timeout reads as `"...exceeded the timeout: 10s"`, which the client's
/// `classifySyncError` already matches as `network` — so a queued write is
/// retried rather than rolled back and discarded.
fn ingest_post() -> String {
    format!(
        "    SELECT * FROM http::post($sp00ky_endpoint + '/ingest', $payload, {{ \"Authorization\": \"Bearer \" + $sp00ky_secret }}) TIMEOUT {}s;\n",
        EVENT_HTTP_TIMEOUT_SECS
    )
}

/// Bound for the HTTP calls a DB event makes inside the user's transaction
/// (`/ingest` here, `/view/unregister` in `migrate.rs`). The scheduler
/// acknowledges as soon as the event is durable (WAL flush + buffer) and no
/// longer waits for the SSP fan-out, so a healthy ack is milliseconds; this
/// only has to be clear of a slow flush, not of a slow SSP.
pub(crate) const EVENT_HTTP_TIMEOUT_SECS: u64 = 10;


/// Generate Sp00ky events for data hashing and graph synchronization
/// The `in` / `out` lines of a relation table's ingest payload.
///
/// An edge's endpoints are implicit: `DEFINE TABLE likes TYPE RELATION IN user
/// OUT post` declares no `in` or `out` field, so they are not in `table.fields`
/// and the payload loop below would build an edge with no endpoints. The SSP
/// bootstrap reads the row whole, endpoints included, so without these the
/// circuit and the ingest stream would disagree about the row's key set - the
/// same disagreement the excluded-field skip below is careful to avoid.
///
/// Stringified like every other record link. Skipped when the schema declares
/// the field itself, which the field loop then emits with its own typing.
fn relation_endpoints(table: &TableSchema, row: &str) -> String {
    if !table.is_relation {
        return String::new();
    }
    ["in", "out"]
        .iter()
        .filter(|end| !table.fields.contains_key(**end))
        .map(|end| format!("        {end}: <string>({row}.{end} OR \"\"),\n"))
        .collect()
}

pub fn generate_sp00ky_events(
    tables: &BTreeMap<String, TableSchema>,
    _raw_content: &str,
    is_client: bool,
    mode: &DeployMode,
    _endpoint: Option<&str>,
    _secret: Option<&str>,
    transport: SyncTransport,
) -> String {
    // 2. Generate Events
    let mut events = String::from("\n-- ==================================================\n-- AUTO-GENERATED SP00KY EVENTS\n-- ==================================================\n\n");

    // Client Logic: Minimal logic, only Intrinsic Hash, Dirty Flags
    if is_client {
        // ... (existing client logic omitted for brevity, assuming it's unchanged.
        // Logic: if I'm replacing the whole function I need to include it.
        // The instruction said "Update signature and implement sidecar logic".
        // I will replace likely the whole function or large chunks to ensure consistency.)

        // Use the existing client logic from the file view
        // Sort table names for deterministic output
        let mut sorted_table_names: Vec<_> = tables.keys().collect();
        sorted_table_names.sort();

        for table_name in &sorted_table_names {
            // Skip system/internal tables and the sp00ky hash tables themselves
            if table_name.starts_with("_00_") {
                continue;
            }

            let table = tables.get(*table_name).unwrap();

            if table.is_relation {
                continue;
            }

            // @nosync tables never sync: emit no events for them.
            if table.no_sync {
                continue;
            }

            // --------------------------------------------------
            // A. Client Mutation Event
            // --------------------------------------------------
            events.push_str(&format!("-- Table: {} Client Mutation\n", table_name));
            events.push_str(&format!(
                "DEFINE EVENT OVERWRITE _00_{}_client_mutation ON TABLE {}\n",
                table_name, table_name
            ));
            events.push_str("WHEN $before != $after AND $event != \"DELETE\"\nTHEN {\n");
            // Placeholder: Could add dirty flag logic here if needed for client-side sync tracking
            events.push_str("    -- No-op for now. Client mutation sync logic moved to DBSP.\n");
            events.push_str("};\n\n");

            // --------------------------------------------------
            // B. Client Deletion Event
            // --------------------------------------------------
            events.push_str(&format!("-- Table: {} Client Deletion\n", table_name));
            events.push_str(&format!(
                "DEFINE EVENT OVERWRITE _00_{}_client_delete ON TABLE {}\n",
                table_name, table_name
            ));
            events.push_str("WHEN $event = \"DELETE\"\nTHEN {\n");
            events.push_str("    -- No-op for now.\n");
            events.push_str("};\n\n");
        }

        return events;
    }

    // Remote Logic: DBSP Ingest (Surrealism) OR Sidecar HTTP Call

    let is_http = *mode == DeployMode::Singlenode || *mode == DeployMode::Cluster;
    // With the changefeed transport the events keep the version bookkeeping
    // (`_00_version` is what the tail reads `_00_rv` from) and post nothing:
    // the scheduler reads the committed change from `SHOW CHANGES`.
    let post_ingest = is_http && transport == SyncTransport::Http;

    // Sort table names for deterministic output
    let mut sorted_table_names: Vec<_> = tables.keys().collect();
    sorted_table_names.sort();

    for table_name in &sorted_table_names {
        // Skip system/internal tables and the sp00ky hash tables themselves
        if table_name.starts_with("_00_") {
            continue;
        }

        let table = tables.get(*table_name).unwrap();

        // `TYPE RELATION` tables get events like any other table. They were
        // skipped here for years with no stated reason, while the replica clone,
        // the drift check and the SSP bootstrap all kept carrying them - see
        // `schema_builder::table_takes_changefeed` for what that did to a live
        // query over an edge table.

        // @nosync tables never sync: emit no events for them, so SurrealDB
        // posts no ingest to the scheduler/SSP.
        if table.no_sync {
            continue;
        }

        // ===================================
        // 1. MUTATION EVENT (CREATE / UPDATE)
        // ===================================
        // Merges version tracking and data ingestion
        events.push_str(&format!(
            "DEFINE EVENT OVERWRITE _00_{}_mutation ON TABLE {}\n",
            table_name, table_name
        ));
        events.push_str("WHEN $before != $after AND $event != \"DELETE\"\nTHEN {\n");

        // --- Versioning Logic ---
        events.push_str("    LET $sp00ky_ver_rec = IF $event = \"CREATE\" {\n");
        events.push_str(
            "        (CREATE _00_version SET record_id = $after.id, version = 1 RETURN AFTER)\n",
        );
        events.push_str("    } ELSE IF $event = \"UPDATE\" {\n");
        events.push_str("        IF $sp00ky_target_version != NONE AND $sp00ky_target_version.id == $after.id {\n");
        events.push_str("            LET $u = (UPDATE _00_version SET version = <int>$sp00ky_target_version.version WHERE record_id = $after.id RETURN AFTER);\n");
        events.push_str("            LET $sp00ky_target_version = NONE;\n");
        events.push_str("            $u\n");
        events.push_str("        } ELSE {\n");
        events.push_str("            (UPDATE _00_version SET version += 1 WHERE record_id = $after.id RETURN AFTER)\n");
        events.push_str("        }\n");
        events.push_str("    };\n");
        events.push_str("    LET $sp00ky_ver = $sp00ky_ver_rec[0].version;\n\n");

        // --- Ingestion Logic ---
        events.push_str("    LET $plain_after = {\n");
        events.push_str("        id: <string>($after.id OR \"\"),\n");

        events.push_str(&relation_endpoints(table, "$after"));

        let mut all_fields: Vec<_> = table.fields.keys().collect();
        all_fields.sort();

        for field_name in all_fields {
            let field_def = table.fields.get(field_name).unwrap();
            // Skip fields whose value must never enter the sync machinery.
            //
            // `@crdt`: the value is a raw LoroDoc snapshot (TYPE bytes) and JSON
            // has no native bytes encoding, so SurrealDB's http::post would
            // either drop the field or shape-shift it depending on transport.
            // `@nosync`: server-only, also absent from the client schema.
            // `@opaque`: synced to the client (which reads it straight from
            // SurrealDB) but deliberately not held server-side.
            //
            // In all three cases the SSP's job is membership tracking, not
            // content storage, so omitting them keeps the payload clean and
            // saves bandwidth on every keystroke debounce push.
            //
            // The same three classes get `COMMENT 'sp00ky:opaque'` baked onto
            // their DEFINE FIELD (see `schema_builder::add_opaque_field_markers`),
            // which is what makes the scheduler replica and the SSP bootstrap
            // scan omit them too. Both halves are required: skipping here while
            // the bootstrap still loads the column leaves the circuit and the
            // replica permanently disagreeing about the row's key set.
            if is_excluded_field(field_def) || is_sub_path(field_name) {
                continue;
            }
            match field_def.field_type {
                FieldType::Record(_) | FieldType::Datetime => {
                    events.push_str(&format!(
                        "        {}: <string>($after.{} OR \"\"),\n",
                        field_name, field_name
                    ));
                }
                _ => {
                    events.push_str(&format!("        {}: $after.{},\n", field_name, field_name));
                }
            }
        }
        events.push_str("        _00_rv: (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $after.id)\n");
        events.push_str("    };\n");

        if post_ingest {
            events.push_str("    LET $payload = {\n");
            events.push_str(&format!("        table: '{}',\n", table_name));
            events.push_str("        op: $event,\n");
            events.push_str("        id: <string>($after.id OR \"\"),\n");
            events.push_str("        record: $plain_after,\n");
            events.push_str("        hash: \"\"\n");
            events.push_str("    };\n");

            events.push_str(&ingest_post());
        } else if !is_http {
            // Surrealism / WASM Mode
            events.push_str(&format!(
                "    mod::dbsp::ingest('{}', $event, <string>($after.id OR \"\"), $plain_after);\n",
                table_name
            ));
            events.push_str("    mod::dbsp::save_state(NONE);\n");
        }
        events.push_str("};\n\n");

        // ===================================
        // 2. DELETE EVENT
        // ===================================
        // Merges version cleanup and data ingestion
        events.push_str(&format!(
            "DEFINE EVENT OVERWRITE _00_{}_delete ON TABLE {}\n",
            table_name, table_name
        ));
        events.push_str("WHEN $event = \"DELETE\"\nTHEN {\n");

        // --- Versioning Logic ---
        events.push_str("    DELETE _00_version WHERE record_id = $before.id;\n\n");
        // CRDT and cursor state live inline on the parent row itself, so
        // there is no sidecar table to clean up here — the row deletion
        // takes the snapshot with it.

        // --- Ingestion Logic ---
        events.push_str("    LET $plain_before = {\n");
        events.push_str("        id: <string>($before.id OR \"\"),\n");

        events.push_str(&relation_endpoints(table, "$before"));

        let mut all_fields_del: Vec<_> = table.fields.keys().collect();
        all_fields_del.sort();

        for field_name in all_fields_del {
            let field_def = table.fields.get(field_name).unwrap();
            // See the matching skip in the mutation event above.
            if is_excluded_field(field_def) || is_sub_path(field_name) {
                continue;
            }
            match field_def.field_type {
                FieldType::Record(_) | FieldType::Datetime => {
                    events.push_str(&format!(
                        "        {}: <string>($before.{} OR \"\"),\n",
                        field_name, field_name
                    ));
                }
                _ => {
                    events.push_str(&format!(
                        "        {}: $before.{},\n",
                        field_name, field_name
                    ));
                }
            }
        }
        events.push_str("    };\n");

        if post_ingest {
            events.push_str("    LET $payload = {\n");
            events.push_str(&format!("        table: '{}',\n", table_name));
            events.push_str("        op: \"DELETE\",\n");
            events.push_str("        id: <string>($before.id OR \"\"),\n");
            events.push_str("        record: $plain_before,\n");
            events.push_str("        hash: \"\"\n");
            events.push_str("    };\n");

            events.push_str(&ingest_post());
        } else if !is_http {
            events.push_str(&format!("    mod::dbsp::ingest('{}', \"DELETE\", <string>($before.id OR \"\"), $plain_before);\n", table_name));
            events.push_str("    mod::dbsp::save_state(NONE);\n");
        }
        events.push_str("};\n\n");
    }

    // ===================================================================
    // _00_user_feature (feature-flag assignments)
    // ===================================================================
    // Feature-flag assignments are written by the scheduler sweep and the
    // `spky flag` CLI under the project root token, never through the client
    // up-queue. Without an ingest-notify event the SSP never learns of the
    // change, so a client already subscribed to its flag would not see a new
    // variant until it re-registered. Emit the same mutation/delete events
    // every app table gets (skipped above for `_00_` tables) so a root UPSERT
    // reaches `/ingest`, the SSP recomputes the registered query, and the
    // subscriber's `_00_list_ref_user_<id>` updates in real time.
    //
    // Server-written only, so there is no client-mutation version targeting
    // (`$sp00ky_target_version`); the version simply increments.
    events.push_str("-- Table: _00_user_feature Mutation (server-written; ingest-notify)\n");
    events.push_str("DEFINE EVENT OVERWRITE _00_user_feature_mutation ON TABLE _00_user_feature\n");
    events.push_str("WHEN $before != $after AND $event != \"DELETE\"\nTHEN {\n");
    events.push_str("    LET $sp00ky_ver_rec = IF $event = \"CREATE\" {\n");
    events.push_str(
        "        (CREATE _00_version SET record_id = $after.id, version = 1 RETURN AFTER)\n",
    );
    events.push_str("    } ELSE {\n");
    events.push_str(
        "        (UPDATE _00_version SET version += 1 WHERE record_id = $after.id RETURN AFTER)\n",
    );
    events.push_str("    };\n");
    events.push_str("    LET $plain_after = {\n");
    events.push_str("        id: <string>($after.id OR \"\"),\n");
    events.push_str("        user: <string>($after.user OR \"\"),\n");
    events.push_str("        key: $after.key,\n");
    events.push_str("        variant: $after.variant,\n");
    events.push_str("        payload: $after.payload,\n");
    events.push_str("        evaluated_at: <string>($after.evaluated_at OR \"\"),\n");
    events.push_str("        _00_rv: (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $after.id)\n");
    events.push_str("    };\n");
    if post_ingest {
        events.push_str("    LET $payload = {\n");
        events.push_str("        table: '_00_user_feature',\n");
        events.push_str("        op: $event,\n");
        events.push_str("        id: <string>($after.id OR \"\"),\n");
        events.push_str("        record: $plain_after,\n");
        events.push_str("        hash: \"\"\n");
        events.push_str("    };\n");
        events.push_str(&ingest_post());
    } else if !is_http {
        events.push_str("    mod::dbsp::ingest('_00_user_feature', $event, <string>($after.id OR \"\"), $plain_after);\n");
        events.push_str("    mod::dbsp::save_state(NONE);\n");
    }
    events.push_str("};\n\n");

    events.push_str("-- Table: _00_user_feature Deletion (ingest-notify)\n");
    events.push_str("DEFINE EVENT OVERWRITE _00_user_feature_delete ON TABLE _00_user_feature\n");
    events.push_str("WHEN $event = \"DELETE\"\nTHEN {\n");
    events.push_str("    DELETE _00_version WHERE record_id = $before.id;\n");
    events.push_str("    LET $plain_before = {\n");
    events.push_str("        id: <string>($before.id OR \"\"),\n");
    events.push_str("        user: <string>($before.user OR \"\"),\n");
    events.push_str("        key: $before.key,\n");
    events.push_str("        variant: $before.variant,\n");
    events.push_str("        payload: $before.payload,\n");
    events.push_str("        evaluated_at: <string>($before.evaluated_at OR \"\")\n");
    events.push_str("    };\n");
    if post_ingest {
        events.push_str("    LET $payload = {\n");
        events.push_str("        table: '_00_user_feature',\n");
        events.push_str("        op: \"DELETE\",\n");
        events.push_str("        id: <string>($before.id OR \"\"),\n");
        events.push_str("        record: $plain_before,\n");
        events.push_str("        hash: \"\"\n");
        events.push_str("    };\n");
        events.push_str(&ingest_post());
    } else if !is_http {
        events.push_str("    mod::dbsp::ingest('_00_user_feature', \"DELETE\", <string>($before.id OR \"\"), $plain_before);\n");
        events.push_str("    mod::dbsp::save_state(NONE);\n");
    }
    events.push_str("};\n\n");

    // ===================================================================
    // _00_app_release (per-frontend current-version announcements)
    // ===================================================================
    // Written root-only by `spky deploy` / `spky release` / the git-linked
    // builder, never through the client up-queue - same situation as
    // _00_user_feature above, so it needs the same explicit ingest-notify
    // events for a row change to reach already-subscribed clients live.
    events.push_str("-- Table: _00_app_release Mutation (server-written; ingest-notify)\n");
    events.push_str("DEFINE EVENT OVERWRITE _00_app_release_mutation ON TABLE _00_app_release\n");
    events.push_str("WHEN $before != $after AND $event != \"DELETE\"\nTHEN {\n");
    events.push_str("    LET $sp00ky_ver_rec = IF $event = \"CREATE\" {\n");
    events.push_str(
        "        (CREATE _00_version SET record_id = $after.id, version = 1 RETURN AFTER)\n",
    );
    events.push_str("    } ELSE {\n");
    events.push_str(
        "        (UPDATE _00_version SET version += 1 WHERE record_id = $after.id RETURN AFTER)\n",
    );
    events.push_str("    };\n");
    events.push_str("    LET $plain_after = {\n");
    events.push_str("        id: <string>($after.id OR \"\"),\n");
    events.push_str("        app: $after.app,\n");
    events.push_str("        version: $after.version,\n");
    events.push_str("        cache_bust: $after.cache_bust,\n");
    events.push_str("        mandatory: $after.mandatory,\n");
    events.push_str("        released_at: <string>($after.released_at OR \"\"),\n");
    events.push_str("        _00_rv: (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $after.id)\n");
    events.push_str("    };\n");
    if post_ingest {
        events.push_str("    LET $payload = {\n");
        events.push_str("        table: '_00_app_release',\n");
        events.push_str("        op: $event,\n");
        events.push_str("        id: <string>($after.id OR \"\"),\n");
        events.push_str("        record: $plain_after,\n");
        events.push_str("        hash: \"\"\n");
        events.push_str("    };\n");
        events.push_str(&ingest_post());
    } else if !is_http {
        events.push_str("    mod::dbsp::ingest('_00_app_release', $event, <string>($after.id OR \"\"), $plain_after);\n");
        events.push_str("    mod::dbsp::save_state(NONE);\n");
    }
    events.push_str("};\n\n");

    // ===================================================================
    // _00_query_allowlist (per-release query shapes; SSP reload trigger)
    // ===================================================================
    // Root-only rows written by `spky deploy` / `spky release` / `spky dev`.
    // Never synced to clients and never loaded into the circuit: the SSP's
    // `/ingest` handler intercepts this table and re-reads the allowlist from
    // the DB, so a new release's shapes are admitted without an SSP restart.
    // The payload therefore carries only the identity, not `entries`.
    if post_ingest {
        for (suffix, when, side) in [
            ("mutation", "$before != $after AND $event != \"DELETE\"", "$after"),
            ("delete", "$event = \"DELETE\"", "$before"),
        ] {
            events.push_str(&format!(
                "-- Table: _00_query_allowlist {} (server-written; SSP reload trigger)\n",
                if suffix == "mutation" { "Mutation" } else { "Delete" }
            ));
            events.push_str(&format!(
                "DEFINE EVENT OVERWRITE _00_query_allowlist_{suffix} ON TABLE _00_query_allowlist\nWHEN {when}\nTHEN {{\n"
            ));
            events.push_str("    LET $payload = {\n");
            events.push_str("        table: '_00_query_allowlist',\n");
            events.push_str("        op: $event,\n");
            events.push_str(&format!("        id: <string>({side}.id OR \"\"),\n"));
            events.push_str(&format!(
                "        record: {{ id: <string>({side}.id OR \"\"), app: {side}.app, version: {side}.version, released_at: <string>({side}.released_at OR \"\") }},\n"
            ));
            events.push_str("        hash: \"\"\n");
            events.push_str("    };\n");
            events.push_str(&ingest_post());
            events.push_str("};\n\n");
        }
    }

    events.push_str("-- Table: _00_app_release Deletion (ingest-notify)\n");
    events.push_str("DEFINE EVENT OVERWRITE _00_app_release_delete ON TABLE _00_app_release\n");
    events.push_str("WHEN $event = \"DELETE\"\nTHEN {\n");
    events.push_str("    DELETE _00_version WHERE record_id = $before.id;\n");
    events.push_str("    LET $plain_before = {\n");
    events.push_str("        id: <string>($before.id OR \"\"),\n");
    events.push_str("        app: $before.app,\n");
    events.push_str("        version: $before.version,\n");
    events.push_str("        cache_bust: $before.cache_bust,\n");
    events.push_str("        mandatory: $before.mandatory,\n");
    events.push_str("        released_at: <string>($before.released_at OR \"\")\n");
    events.push_str("    };\n");
    if post_ingest {
        events.push_str("    LET $payload = {\n");
        events.push_str("        table: '_00_app_release',\n");
        events.push_str("        op: \"DELETE\",\n");
        events.push_str("        id: <string>($before.id OR \"\"),\n");
        events.push_str("        record: $plain_before,\n");
        events.push_str("        hash: \"\"\n");
        events.push_str("    };\n");
        events.push_str(&ingest_post());
    } else if !is_http {
        events.push_str("    mod::dbsp::ingest('_00_app_release', \"DELETE\", <string>($before.id OR \"\"), $plain_before);\n");
        events.push_str("    mod::dbsp::save_state(NONE);\n");
    }
    events.push_str("};\n\n");

    // ===================================================================
    // _00_heartbeat (e2e sync-pipeline probe)
    // ===================================================================
    // The scheduler's heartbeat loop UPSERTs `_00_heartbeat:probe` and then
    // polls each SSP for the last hb_seq it saw. This event is the first hop:
    // without it a probe write never leaves the database and the loop
    // measures nothing. `_00_` tables are skipped by the generator above, so
    // it is hand-written like _00_user_feature — minus the `_00_version`
    // machinery, because nothing subscribes to this row (the SSP just
    // records the seq in memory; the row is never client-synced).
    events.push_str("-- Table: _00_heartbeat Mutation (probe-written; ingest-notify)\n");
    events.push_str("DEFINE EVENT OVERWRITE _00_heartbeat_mutation ON TABLE _00_heartbeat\n");
    events.push_str("WHEN $before != $after AND $event != \"DELETE\"\nTHEN {\n");
    events.push_str("    LET $plain_after = {\n");
    events.push_str("        id: <string>($after.id OR \"\"),\n");
    events.push_str("        hb_seq: $after.hb_seq,\n");
    events.push_str("        sent_at: <string>($after.sent_at OR \"\")\n");
    events.push_str("    };\n");
    if post_ingest {
        events.push_str("    LET $payload = {\n");
        events.push_str("        table: '_00_heartbeat',\n");
        events.push_str("        op: $event,\n");
        events.push_str("        id: <string>($after.id OR \"\"),\n");
        events.push_str("        record: $plain_after,\n");
        events.push_str("        hash: \"\"\n");
        events.push_str("    };\n");
        events.push_str(&ingest_post());
    } else if !is_http {
        events.push_str("    mod::dbsp::ingest('_00_heartbeat', $event, <string>($after.id OR \"\"), $plain_after);\n");
        events.push_str("    mod::dbsp::save_state(NONE);\n");
    }
    events.push_str("};\n\n");

    events
}

#[cfg(test)]
mod tests {
    use super::*;

    // An empty table map isolates the always-emitted system-table events
    // (the user-table loop produces nothing), so these assertions target the
    // `_00_user_feature` ingest-notify events specifically.
    fn gen(is_client: bool, mode: DeployMode) -> String {
        generate_sp00ky_events(&BTreeMap::new(), "", is_client, &mode, None, None, SyncTransport::Http)
    }

    /// The changefeed transport keeps every event (the `_00_version`
    /// bookkeeping is what stamps `_00_rv` in the feed) but posts nothing.
    #[test]
    fn changefeed_transport_keeps_versioning_and_drops_the_post() {
        use crate::parser::SchemaParser;
        let schema = r#"
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD body ON TABLE doc TYPE string;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();
        let out = generate_sp00ky_events(
            &parser.tables,
            schema,
            false,
            &DeployMode::Cluster,
            None,
            None,
            SyncTransport::Changefeed,
        );
        assert!(out.contains("DEFINE EVENT OVERWRITE _00_doc_mutation"));
        assert!(out.contains("DEFINE EVENT OVERWRITE _00_doc_delete"));
        assert!(out.contains("CREATE _00_version SET record_id = $after.id"), "version rows still written");
        assert!(out.contains("DELETE _00_version WHERE record_id = $before.id"));
        assert!(!out.contains("http::post"), "no network call inside the transaction");
        assert!(!out.contains("mod::dbsp::ingest"), "not the surrealism path either");
        // The http transport on the same schema still posts.
        let http = generate_sp00ky_events(&parser.tables, schema, false, &DeployMode::Cluster, None, None, SyncTransport::Http);
        assert!(http.contains("http::post($sp00ky_endpoint + '/ingest'"));
    }

    #[test]
    fn user_feature_events_post_to_ingest_in_http_modes() {
        for mode in [DeployMode::Singlenode, DeployMode::Cluster] {
            let out = gen(false, mode);
            assert!(
                out.contains(
                    "DEFINE EVENT OVERWRITE _00_user_feature_mutation ON TABLE _00_user_feature"
                ),
                "missing mutation event"
            );
            assert!(
                out.contains(
                    "DEFINE EVENT OVERWRITE _00_user_feature_delete ON TABLE _00_user_feature"
                ),
                "missing delete event"
            );
            assert!(
                out.contains("table: '_00_user_feature'"),
                "missing ingest payload table"
            );
            assert!(
                out.contains("http::post($sp00ky_endpoint + '/ingest'"),
                "feature-flag changes must notify the SSP ingest endpoint"
            );
        }
    }

    #[test]
    fn heartbeat_event_posts_to_ingest_in_http_modes() {
        for mode in [DeployMode::Singlenode, DeployMode::Cluster] {
            let out = gen(false, mode);
            assert!(
                out.contains(
                    "DEFINE EVENT OVERWRITE _00_heartbeat_mutation ON TABLE _00_heartbeat"
                ),
                "missing heartbeat mutation event"
            );
            assert!(
                out.contains("table: '_00_heartbeat'"),
                "missing heartbeat ingest payload table"
            );
        }
        // Surrealism mode routes through the module instead.
        let out = gen(false, DeployMode::Surrealism);
        assert!(
            out.contains("mod::dbsp::ingest('_00_heartbeat'"),
            "surrealism mode must route heartbeat through mod::dbsp"
        );
    }

    #[test]
    fn user_feature_events_use_dbsp_in_surrealism_mode() {
        let out = gen(false, DeployMode::Surrealism);
        assert!(
            out.contains("mod::dbsp::ingest('_00_user_feature'"),
            "missing dbsp ingest"
        );
        assert!(
            !out.contains("http::post"),
            "surrealism mode must not emit http::post"
        );
    }

    #[test]
    fn user_feature_events_are_remote_only_not_client() {
        // The client schema must not carry server-side ingest events for the
        // root-written assignments table.
        let out = gen(true, DeployMode::Singlenode);
        assert!(
            !out.contains("_00_user_feature_mutation"),
            "client schema must not emit _00_user_feature ingest events"
        );
    }

    /// A relation table is cloned, drift-checked, bootstrapped into circuits and
    /// exposed to clients as queryable. It has to be in the ingest stream too, or
    /// a live query over it returns the bootstrap-time edges and never updates.
    /// The stock outbox template defines `errors[*]` (FLEXIBLE elements). A
    /// sub-path is not a record key: emitting it produced
    /// `errors[*]: $after.errors[*]`, a parse error that took the whole internal
    /// schema down with it on the first deploy of a freshly scaffolded project.
    #[test]
    fn a_sub_path_field_definition_never_becomes_an_event_object_key() {
        use crate::parser::SchemaParser;
        let schema = r#"
DEFINE TABLE job SCHEMAFULL;
DEFINE FIELD path ON TABLE job TYPE string;
DEFINE FIELD errors ON TABLE job TYPE array<object> DEFAULT ALWAYS [];
DEFINE FIELD errors[*] ON TABLE job TYPE object FLEXIBLE;
DEFINE FIELD settings ON TABLE job TYPE object;
DEFINE FIELD settings.theme ON TABLE job TYPE string;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();

        let out = generate_sp00ky_events(
            &parser.tables,
            schema,
            false,
            &DeployMode::Cluster,
            None,
            None,
            SyncTransport::Http,
        );
        assert!(out.contains("errors: $after.errors,"), "the parent field carries the value: {out}");
        assert!(out.contains("settings: $after.settings,"), "{out}");
        assert!(out.contains("path: $after.path,"), "{out}");
        for bad in ["errors[*]:", "$after.errors[*]", "$before.errors[*]", "settings.theme:"] {
            assert!(!out.contains(bad), "`{bad}` must not be generated: {out}");
        }
    }

    #[test]
    fn a_relation_table_gets_events_with_its_endpoints() {
        use crate::parser::SchemaParser;
        let schema = r#"
DEFINE TABLE user SCHEMALESS;
DEFINE TABLE post SCHEMALESS;
DEFINE TABLE likes TYPE RELATION IN user OUT post SCHEMAFULL;
DEFINE FIELD weight ON TABLE likes TYPE int;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();
        assert!(parser.tables["likes"].is_relation);

        let out = generate_sp00ky_events(
            &parser.tables,
            schema,
            false,
            &DeployMode::Singlenode,
            None,
            None,
            SyncTransport::Http,
        );
        assert!(out.contains("DEFINE EVENT OVERWRITE _00_likes_mutation ON TABLE likes"), "{out}");
        assert!(out.contains("DEFINE EVENT OVERWRITE _00_likes_delete ON TABLE likes"), "{out}");

        // `in`/`out` are implicit on an edge, so they are not in `table.fields`;
        // without them the payload is an edge with no endpoints, and the circuit
        // (which bootstraps the row whole) disagrees with the stream about it.
        let mutation = out.split("_00_likes_mutation").nth(1).unwrap();
        let mutation = mutation.split("_00_likes_delete").next().unwrap();
        assert!(mutation.contains(r#"in: <string>($after.in OR ""),"#), "{mutation}");
        assert!(mutation.contains(r#"out: <string>($after.out OR ""),"#), "{mutation}");
        assert!(mutation.contains("weight: $after.weight,"), "declared fields still ride along");

        let delete = out.split("_00_likes_delete").nth(1).unwrap();
        assert!(delete.contains(r#"in: <string>($before.in OR ""),"#), "{delete}");

        // A plain table gets no invented endpoints.
        let user = out.split("_00_user_mutation").nth(1).unwrap();
        let user = user.split("_00_user_delete").next().unwrap();
        assert!(!user.contains("$after.in"), "{user}");

        // And what is generated has to be SurrealQL the server will accept: one
        // bad statement fails the whole internal-schema batch.
        if let Err(e) = surrealdb_core::syn::parse_with_capabilities(
            &out,
            &surrealdb_core::dbs::Capabilities::all(),
        ) {
            panic!("generated relation events do not parse: {e}");
        }
    }

    #[test]
    fn a_nosync_relation_table_still_emits_nothing() {
        use crate::parser::SchemaParser;
        let schema = "DEFINE TABLE a SCHEMALESS;\nDEFINE TABLE b SCHEMALESS;\n-- @nosync\nDEFINE TABLE audit_edge TYPE RELATION IN a OUT b;\n";
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();
        let out = generate_sp00ky_events(
            &parser.tables, schema, false, &DeployMode::Singlenode, None, None, SyncTransport::Http,
        );
        assert!(!out.contains("ON TABLE audit_edge"), "@nosync wins over being a relation: {out}");
        assert!(!crate::schema_builder::table_takes_changefeed("audit_edge", true, true));
        assert!(crate::schema_builder::table_takes_changefeed("likes", true, false));
    }

    #[test]
    fn nosync_table_emits_no_events() {
        use crate::parser::SchemaParser;
        let schema = r#"
DEFINE TABLE public SCHEMALESS;
DEFINE FIELD name ON TABLE public TYPE string;

-- @nosync
DEFINE TABLE secrets SCHEMALESS;
DEFINE FIELD token ON TABLE secrets TYPE string;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();
        assert!(
            parser.tables["secrets"].no_sync,
            "secrets must be marked no_sync"
        );

        for is_client in [false, true] {
            let out = generate_sp00ky_events(
                &parser.tables,
                schema,
                is_client,
                &DeployMode::Singlenode,
                None,
                None,
                SyncTransport::Http,
            );
            assert!(
                out.contains("ON TABLE public"),
                "public table must get events"
            );
            assert!(
                !out.contains("ON TABLE secrets"),
                "@nosync table must not get events (is_client={is_client})"
            );
            assert!(
                !out.contains("table: 'secrets'"),
                "@nosync table must not appear in any ingest payload"
            );
        }
    }

    /// Every field-level exclusion must be absent from BOTH ingest payloads,
    /// while `_00_rv` survives. `_00_rv` is what makes the SSP bump the record
    /// version so subscribed clients refetch the row and pick the new value up
    /// from SurrealDB — drop it and an `@opaque`-only write becomes invisible.
    #[test]
    fn excluded_fields_are_absent_from_ingest_payloads_but_rv_survives() {
        use crate::parser::SchemaParser;
        let schema = r#"
DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD title ON TABLE doc TYPE string;

-- @nosync
DEFINE FIELD import_batch ON TABLE doc TYPE string;

-- @opaque
DEFINE FIELD thumbnail ON TABLE doc TYPE bytes;

-- @crdt text
DEFINE FIELD body ON TABLE doc TYPE string;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();

        let out = generate_sp00ky_events(
            &parser.tables,
            schema,
            false,
            &DeployMode::Singlenode,
            None,
            None,
            SyncTransport::Http,
        );

        for excluded in ["import_batch", "thumbnail", "body"] {
            assert!(
                !out.contains(&format!("{excluded}: $after.")),
                "{excluded} must not be in $plain_after:\n{out}"
            );
            assert!(
                !out.contains(&format!("{excluded}: $before.")),
                "{excluded} must not be in $plain_before:\n{out}"
            );
        }
        assert!(out.contains("title: $after.title"), "got:\n{out}");
        assert!(
            out.contains("_00_rv: (SELECT VALUE version FROM ONLY _00_version"),
            "the record version must still be stamped:\n{out}"
        );
    }
}
