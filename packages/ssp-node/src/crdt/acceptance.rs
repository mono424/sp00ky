//! Which edit-log rows may enter a document.
//!
//! Every client and the SSP fold apply this exact rule before importing a row,
//! so they import the same rows and converge on the same document: a row one
//! side accepted and another refused would leave them different forever (a
//! CRDT never forgets an op). The rule reads only the row itself:
//!
//! - the blob decodes as a Loro **update** (a snapshot could smuggle in
//!   arbitrary history from any peer);
//! - it is no larger than the column allows;
//! - every op in it was made by the row's `peer`, which the database already
//!   checked belongs to the row's author (`_00_crdt_peer`).
//!
//! The TypeScript client mirrors this in `packages/core/src/modules/crdt/acceptance.ts`.

use loro::{EncodedBlobMode, LoroDoc, PeerID, VersionVector};

/// Why a row was refused. Stable strings: they are logged and counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Empty,
    TooLarge,
    BadPeer,
    Undecodable,
    NotAnUpdate,
    NoOps,
    ForeignPeer,
}

impl Rejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Rejection::Empty => "empty",
            Rejection::TooLarge => "too_large",
            Rejection::BadPeer => "bad_peer",
            Rejection::Undecodable => "undecodable",
            Rejection::NotAnUpdate => "not_an_update",
            Rejection::NoOps => "no_ops",
            Rejection::ForeignPeer => "foreign_peer",
        }
    }
}

/// An accepted row: what it contains, so the fold can tell when the document
/// holds all of it.
#[derive(Debug, Clone)]
pub struct Accepted {
    pub peer: PeerID,
    /// Exclusive end counter of the row's ops for `peer`.
    pub end: i32,
}

pub fn check(blob: &[u8], peer: &str, max_bytes: usize) -> Result<Accepted, Rejection> {
    if blob.is_empty() {
        return Err(Rejection::Empty);
    }
    if blob.len() > max_bytes {
        return Err(Rejection::TooLarge);
    }
    let peer: PeerID = peer.parse().map_err(|_| Rejection::BadPeer)?;
    let meta = LoroDoc::decode_import_blob_meta(blob, true).map_err(|_| Rejection::Undecodable)?;
    if !matches!(meta.mode, EncodedBlobMode::Updates) {
        return Err(Rejection::NotAnUpdate);
    }
    let mut authors = changed_peers(&meta.partial_start_vv, &meta.partial_end_vv);
    match (authors.next(), authors.next()) {
        (None, _) => Err(Rejection::NoOps),
        (Some((p, end)), None) if p == peer => Ok(Accepted { peer, end }),
        _ => Err(Rejection::ForeignPeer),
    }
}

/// Peers whose counter advances across the blob, with their end counter.
fn changed_peers<'a>(
    start: &'a VersionVector,
    end: &'a VersionVector,
) -> impl Iterator<Item = (PeerID, i32)> + 'a {
    end.iter()
        .filter(move |(peer, end)| **end > start.get(peer).copied().unwrap_or(0))
        .map(|(peer, end)| (*peer, *end))
}

/// True once `doc` holds every op of an accepted row.
pub fn covered(doc: &LoroDoc, row: &Accepted) -> bool {
    doc.oplog_vv().get(&row.peer).copied().unwrap_or(0) >= row.end
}

#[cfg(test)]
mod tests {
    use super::*;
    use loro::{ExportMode, LoroDoc};

    fn doc_with_peer(peer: u64) -> LoroDoc {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer).unwrap();
        doc
    }

    #[test]
    fn own_update_is_accepted_and_coverage_follows_import() {
        let a = doc_with_peer(7);
        a.get_text("body").insert(0, "hello").unwrap();
        a.commit();
        let blob = a.export(ExportMode::all_updates()).unwrap();
        let ok = check(&blob, "7", 1 << 20).expect("own ops");
        assert_eq!((ok.peer, ok.end), (7, 5));

        let server = LoroDoc::new();
        assert!(!covered(&server, &ok));
        server.import(&blob).unwrap();
        assert!(covered(&server, &ok));
    }

    #[test]
    fn foreign_ops_snapshots_and_garbage_are_rejected() {
        let a = doc_with_peer(7);
        a.get_text("body").insert(0, "a").unwrap();
        a.commit();
        let b = doc_with_peer(9);
        b.import(&a.export(ExportMode::all_updates()).unwrap()).unwrap();
        b.get_text("body").insert(1, "b").unwrap();
        b.commit();

        // B's full update carries A's op too.
        let mixed = b.export(ExportMode::all_updates()).unwrap();
        assert_eq!(check(&mixed, "9", 1 << 20).unwrap_err(), Rejection::ForeignPeer);
        // Claiming another peer's ops.
        let a_only = a.export(ExportMode::all_updates()).unwrap();
        assert_eq!(check(&a_only, "9", 1 << 20).unwrap_err(), Rejection::ForeignPeer);
        // B's own ops alone pass.
        let own = b.export(ExportMode::updates(&a.oplog_vv())).unwrap();
        assert!(check(&own, "9", 1 << 20).is_ok());

        let snapshot = a.export(ExportMode::Snapshot).unwrap();
        assert_eq!(check(&snapshot, "7", 1 << 20).unwrap_err(), Rejection::NotAnUpdate);
        assert_eq!(check(&[1, 2, 3], "7", 1 << 20).unwrap_err(), Rejection::Undecodable);
        assert_eq!(check(&[], "7", 1 << 20).unwrap_err(), Rejection::Empty);
        assert_eq!(check(&a_only, "7", 4).unwrap_err(), Rejection::TooLarge);
        assert_eq!(check(&a_only, "not-a-peer", 1 << 20).unwrap_err(), Rejection::BadPeer);
    }

    #[test]
    fn an_update_with_no_new_ops_is_rejected() {
        let a = doc_with_peer(7);
        a.get_text("body").insert(0, "a").unwrap();
        a.commit();
        let empty = a.export(ExportMode::updates(&a.oplog_vv())).unwrap();
        assert_eq!(check(&empty, "7", 1 << 20).unwrap_err(), Rejection::NoOps);
    }
}
