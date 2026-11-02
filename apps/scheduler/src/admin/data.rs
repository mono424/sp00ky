//! `/admin/api/data/*`: tables, users and bucket files, for the dashboard's
//! Database, Users and Storage pages. The routes and everything they do are
//! `schedule_core::admin_data`, shared with the SSP node; this is the scheduler's
//! half: the checks only a host can make, the frame around the answer, and reading
//! a bucket volume's folders when one is mounted here.

use std::collections::BinaryHeap;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{Method, StatusCode};
use axum::{Extension, Json};
use schedule_core::admin_data::{self as core, BucketDirs, DirFile, DirPage, Request, DIR_COUNT_CAP};
use serde_json::Value;
use tokio::sync::Semaphore;

use super::session::SessionMode;
use super::{api_error, core_db, core_error, AdminState, ApiError, CurrentSession};

/// The database stops each browse statement sooner (see `READ_TIMEOUT` there);
/// this frees the request if the database itself has stopped answering.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Directories being read at once. A read that hangs on a stalled volume cannot be
/// cancelled, so this keeps those from using up the blocking threads the event log
/// needs: the next requests wait and time out instead.
static DISK_READS: Semaphore = Semaphore::const_new(4);

/// Any `/admin/api/data/*` request. The bearer gate has already refused a
/// read-only session everything but GET.
pub async fn handle(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    method: Method,
    UrlPath(rest): UrlPath<String>,
    Query(query): Query<Vec<(String, String)>>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    if !state.config.data {
        return Err(api_error(StatusCode::FORBIDDEN, "Data access is turned off on this project (SPKY_ADMIN_DATA)"));
    }
    // An agent token was minted to operate the cluster, and existed before this
    // surface did; it does not grow into reading the app's data.
    if session.mode == SessionMode::Mcp {
        return Err(api_error(StatusCode::FORBIDDEN, "MCP tokens cannot reach project data; sign in to the dashboard"));
    }
    let db = core_db(&state)?;
    let path: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();
    let mounted = state.config.bucket_dir.clone().map(|root| MountedBuckets { root });
    let request = Request {
        method: method.as_str(),
        path: &path,
        query: &query,
        body: &body,
        actor: &session.subject,
        dirs: mounted.as_ref().map(|m| m as &dyn BucketDirs),
    };
    match tokio::time::timeout(REQUEST_TIMEOUT, core::dispatch(&db, request)).await {
        Ok(answer) => answer.map(Json).map_err(core_error),
        Err(_) => Err(api_error(StatusCode::GATEWAY_TIMEOUT, "The database did not answer within 60s")),
    }
}

/// `file:` bucket volumes mounted on this host. Reads only, and the mount should
/// be read-only too: every write stays with SurrealDB, which owns these files.
struct MountedBuckets {
    root: PathBuf,
}

#[async_trait::async_trait]
impl BucketDirs for MountedBuckets {
    fn covers(&self, location: &str) -> bool {
        let location = Path::new(location);
        location.starts_with(&self.root) && !location.components().any(|c| c == Component::ParentDir)
    }

    async fn list(&self, location: &str, prefix: &str, start: Option<&str>, limit: usize) -> Result<DirPage, String> {
        let (mount, location, prefix, start) = (self.root.clone(), location.to_string(), prefix.to_string(), start.map(str::to_string));
        let reading = DISK_READS.acquire().await.map_err(|e| e.to_string())?;
        tokio::task::spawn_blocking(move || {
            let _reading = reading;
            let location = Path::new(&location);
            check_inside(&mount, location)?;
            read_dir_page(location, &prefix, start.as_deref(), limit)
        })
        .await
        .map_err(|e| e.to_string())?
    }
}

/// A bucket's real path has to be under the mount as well: `covers` only compares
/// names, and a link can stand anywhere on the way there.
fn check_inside(mount: &Path, location: &Path) -> Result<(), String> {
    let mount = std::fs::canonicalize(mount).map_err(|e| e.to_string())?;
    match std::fs::canonicalize(location) {
        Ok(real) if !real.starts_with(&mount) => Err("this bucket is outside the mounted volume".to_string()),
        _ => Ok(()),
    }
}

/// One page of a directory under a bucket's root, in name order. A directory that
/// is not there is empty: SurrealDB makes one when the first file lands in it.
fn read_dir_page(location: &Path, prefix: &str, start: Option<&str>, limit: usize) -> Result<DirPage, String> {
    let dir = location.join(prefix);
    let real = match std::fs::canonicalize(&dir) {
        Ok(real) => real,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(DirPage::default()),
        Err(e) => return Err(e.to_string()),
    };
    // `prefix` was validated against `..`; this is for a link inside the volume.
    let root = std::fs::canonicalize(location).map_err(|e| e.to_string())?;
    if !real.starts_with(&root) {
        return Err("that folder is outside the bucket".to_string());
    }

    // Only the lowest names past `start` are kept, so memory follows the page and
    // not the folder. One more than a page says whether another one follows.
    let mut lowest: BinaryHeap<(String, bool)> = BinaryHeap::with_capacity(limit + 2);
    for entry in std::fs::read_dir(&real).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        // Not a name SurrealDB could give a key, or a link to anywhere: skipped.
        let Ok(name) = entry.file_name().into_string() else { continue };
        let Ok(kind) = entry.file_type() else { continue };
        if (kind.is_dir() || kind.is_file()) && start.is_none_or(|s| name.as_str() > s) {
            lowest.push((name, kind.is_dir()));
            if lowest.len() > limit + 1 {
                lowest.pop();
            }
        }
    }
    let names = lowest.into_sorted_vec();

    let mut page = DirPage::default();
    if names.len() > limit {
        page.next = names.iter().take(limit).last().map(|(name, _)| name.clone());
    }
    for (name, is_dir) in names.iter().take(limit) {
        let path = real.join(name);
        if *is_dir {
            let count = std::fs::read_dir(&path).map_or(0, |d| d.take(DIR_COUNT_CAP as usize).count() as u64);
            page.folders.push((name.clone(), count));
        } else if let Ok(meta) = std::fs::metadata(&path) {
            page.files.push(DirFile {
                name: name.clone(),
                size: meta.len(),
                updated: meta.modified().ok().map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()),
            });
        }
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn volume() -> (tempfile::TempDir, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let bucket = root.path().join("covers");
        for dir in ["a/b", "a/c", "empty", "z"] {
            std::fs::create_dir_all(bucket.join(dir)).unwrap();
        }
        for (file, bytes) in [("top.png", 3), ("a/one.txt", 5), ("a/b/deep.txt", 7), ("z/last", 1)] {
            std::fs::write(bucket.join(file), vec![b'x'; bytes]).unwrap();
        }
        (root, bucket)
    }

    fn names(page: &DirPage) -> Vec<String> {
        let mut all: Vec<String> = page.folders.iter().map(|f| format!("{}/", f.0)).collect();
        all.extend(page.files.iter().map(|f| f.name.clone()));
        all
    }

    #[test]
    fn lists_folders_files_and_counts() {
        let (_root, bucket) = volume();
        let page = read_dir_page(&bucket, "", None, 100).unwrap();
        assert_eq!(names(&page), ["a/", "empty/", "z/", "top.png"]);
        assert_eq!(page.folders, [("a".to_string(), 3), ("empty".to_string(), 0), ("z".to_string(), 1)]);
        assert_eq!(page.files[0].size, 3);
        assert!(page.files[0].updated.as_deref().unwrap().contains('T'));
        assert_eq!(page.next, None);

        let page = read_dir_page(&bucket, "a/", None, 100).unwrap();
        assert_eq!(names(&page), ["b/", "c/", "one.txt"]);
        let page = read_dir_page(&bucket, "a/b/", None, 100).unwrap();
        assert_eq!(names(&page), ["deep.txt"]);
    }

    #[test]
    fn pages_through_everything_exactly_once() {
        let (_root, bucket) = volume();
        let mut seen = Vec::new();
        let mut start: Option<String> = None;
        for _ in 0..10 {
            let page = read_dir_page(&bucket, "", start.as_deref(), 1).unwrap();
            seen.extend(names(&page));
            start = page.next;
            if start.is_none() {
                break;
            }
        }
        seen.sort();
        assert_eq!(seen, ["a/", "empty/", "top.png", "z/"]);
    }

    #[test]
    fn a_folder_that_is_not_there_is_empty() {
        let (_root, bucket) = volume();
        assert_eq!(read_dir_page(&bucket, "nope/", None, 100).unwrap(), DirPage::default());
    }

    #[cfg(unix)]
    #[test]
    fn a_link_cannot_walk_out_of_the_bucket() {
        let (root, bucket) = volume();
        let outside = root.path().join("secrets");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("key"), b"x").unwrap();
        std::os::unix::fs::symlink(&outside, bucket.join("link")).unwrap();
        // Not listed as a folder, and refused when asked for by name.
        let page = read_dir_page(&bucket, "", None, 100).unwrap();
        assert!(!names(&page).iter().any(|n| n.starts_with("link")));
        assert!(read_dir_page(&bucket, "link/", None, 100).is_err());
    }

    #[test]
    fn covers_only_what_is_under_the_mount() {
        let m = MountedBuckets { root: "/buckets".into() };
        assert!(m.covers("/buckets/covers"));
        assert!(!m.covers("/bucketsy/covers"));
        assert!(!m.covers("/data/covers"));
        assert!(!m.covers("/buckets/../data"));
    }

    #[test]
    fn a_bucket_declared_through_a_link_is_refused() {
        let (root, bucket) = volume();
        let mount = root.path().join("mount");
        std::fs::create_dir(&mount).unwrap();
        std::os::unix::fs::symlink(&bucket, mount.join("elsewhere")).unwrap();
        assert!(check_inside(&mount, &mount.join("elsewhere")).is_err());
        // Inside, or not made yet: fine.
        assert!(check_inside(root.path(), &bucket).is_ok());
        assert!(check_inside(&mount, &mount.join("new")).is_ok());
    }

    #[test]
    fn a_folder_bigger_than_a_page_still_starts_with_its_first_names() {
        let root = tempfile::tempdir().unwrap();
        for n in (0..50).rev() {
            std::fs::write(root.path().join(format!("{n:03}")), b"x").unwrap();
        }
        let page = read_dir_page(root.path(), "", None, 3).unwrap();
        assert_eq!(names(&page), ["000", "001", "002"]);
        assert_eq!(page.next.as_deref(), Some("002"));
        let page = read_dir_page(root.path(), "", Some("047"), 3).unwrap();
        assert_eq!(names(&page), ["048", "049"]);
        assert_eq!(page.next, None);
    }
}
