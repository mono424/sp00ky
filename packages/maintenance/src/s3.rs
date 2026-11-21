use anyhow::{Context, Result};
use s3::bucket::{Bucket, CHUNK_SIZE};
use s3::command::{Command, Multipart};
use s3::creds::Credentials;
use s3::request::tokio_backend::HyperRequest;
use s3::request::{Request, ResponseData};
use s3::serde_types::Part;
use s3::Region;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tracing::warn;

const GZIP_CONTENT_TYPE: &str = "application/gzip";

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_bucket: String,
    pub s3_region: String,
}

impl BackupConfig {
    pub fn from_env() -> Self {
        Self {
            s3_endpoint: std::env::var("S3_ENDPOINT")
                .unwrap_or_else(|_| "http://10.100.1.5:9000".to_string()),
            s3_access_key: std::env::var("S3_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            s3_secret_key: std::env::var("S3_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            s3_bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "backups".to_string()),
            s3_region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        }
    }

    /// True when any S3_* variable is present in the environment — used by
    /// hosts that only enable the backup plane when storage is configured.
    pub fn env_configured() -> bool {
        ["S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET"]
            .iter()
            .any(|k| std::env::var(k).is_ok())
    }

    fn region(&self) -> Region {
        Region::Custom {
            region: self.s3_region.clone(),
            endpoint: self.s3_endpoint.clone(),
        }
    }

    fn credentials(&self) -> Result<Credentials> {
        Credentials::new(
            Some(&self.s3_access_key),
            Some(&self.s3_secret_key),
            None,
            None,
            None,
        )
        .context("Failed to build S3 credentials")
    }

    pub fn get_bucket(&self) -> Result<Box<Bucket>> {
        let bucket = Bucket::new(&self.s3_bucket, self.region(), self.credentials()?)?
            .with_path_style();
        Ok(bucket)
    }
}

/// Ensure the configured bucket exists. Idempotent; ignores errors for existing buckets.
pub async fn ensure_bucket(config: &BackupConfig) {
    let creds = match config.credentials() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to build S3 credentials for bucket ensure");
            return;
        }
    };
    let _ = Bucket::create_with_path_style(
        &config.s3_bucket,
        config.region(),
        creds,
        s3::BucketConfiguration::default(),
    )
    .await;
}

/// How often one request of an upload is tried before the backup fails. A
/// multi-GB backup is hundreds of requests, and object stores answer some of
/// them with 503 `SlowDown`, 504 or a reset connection when they are busy:
/// with one try each, a single blip threw the whole export away.
const MAX_ATTEMPTS: u32 = 8;

/// Longest one attempt may take. rust-s3's own timeout only bounds the
/// connect, so a request the store stops answering would hang the backup for
/// good. An 8 MiB part needs about 30 s even at 300 KB/s.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(300);

/// Wait before the second attempt, doubled for each one after, capped at a
/// minute: about two minutes of waiting in all.
const FIRST_BACKOFF: Duration = if cfg!(test) {
    Duration::from_millis(1)
} else {
    Duration::from_secs(1)
};

/// How one attempt went wrong: worth another try, or not.
enum Failure {
    /// 429, 5xx, a timeout or a transport error.
    Transient(anyhow::Error),
    /// Anything else (403, 404 `NoSuchUpload`, ...): retrying cannot help.
    Fatal(anyhow::Error),
}

impl From<s3::error::S3Error> for Failure {
    /// Errors before a status arrives are the connection's, so transient.
    fn from(e: s3::error::S3Error) -> Self {
        Failure::Transient(e.into())
    }
}

/// `Ok` for 2xx, else the status and body as a [`Failure`].
fn check_status(resp: &ResponseData) -> Result<(), Failure> {
    let code = resp.status_code();
    if (200..300).contains(&code) {
        return Ok(());
    }
    let err = anyhow::anyhow!(
        "Got HTTP {code} with content '{}'",
        resp.as_str().unwrap_or("")
    );
    if code == 429 || code >= 500 {
        Err(Failure::Transient(err))
    } else {
        Err(Failure::Fatal(err))
    }
}

/// Run `attempt` until it succeeds, fails for good, or [`MAX_ATTEMPTS`] are
/// spent, waiting longer after each transient failure.
async fn with_retry<T, F, Fut>(what: &str, mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    let mut backoff = FIRST_BACKOFF;
    for n in 1..=MAX_ATTEMPTS {
        let outcome = match tokio::time::timeout(ATTEMPT_TIMEOUT, attempt()).await {
            Ok(outcome) => outcome,
            Err(_) => Err(Failure::Transient(anyhow::anyhow!(
                "no answer within {}s",
                ATTEMPT_TIMEOUT.as_secs()
            ))),
        };
        match outcome {
            Ok(value) => return Ok(value),
            Err(Failure::Fatal(e)) => return Err(e.context(format!("Failed to {what}"))),
            Err(Failure::Transient(e)) if n == MAX_ATTEMPTS => {
                return Err(e.context(format!("Failed to {what} after {MAX_ATTEMPTS} attempts")));
            }
            Err(Failure::Transient(e)) => {
                warn!(attempt = n, error = %e, "Failed to {what}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
    unreachable!("the last attempt returns")
}

/// Upload a file to `key` one [`CHUNK_SIZE`] part at a time, so memory stays
/// at one part however big the file is. rust-s3's own `put_object_stream`
/// reads every part before sending any, which holds the whole file in RAM:
/// a multi-GB backup got the scheduler OOM-killed that way.
pub async fn upload_file(bucket: &Bucket, path: &Path, key: &str) -> Result<()> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open {}", path.display()))?;
    let first = read_part(&mut file).await?;
    if first.len() < CHUNK_SIZE {
        return with_retry("upload backup", || async {
            let resp = bucket
                .put_object_with_content_type(key, &first, GZIP_CONTENT_TYPE)
                .await?;
            check_status(&resp)
        })
        .await;
    }

    let upload = with_retry("start multipart upload", || async {
        Ok(bucket
            .initiate_multipart_upload(key, GZIP_CONTENT_TYPE)
            .await?)
    })
    .await?;
    // Address the parts by the key we asked for, never `upload.key`: on a
    // virtual-hosted endpoint (`https://<bucket>.<host>`) with path-style
    // requests the store keys the object as `<bucket>/<key>` and echoes that
    // back, so re-sending it prefixes the bucket twice and every part fails
    // with `NoSuchUpload`. Single-request PUTs and GETs never read the echo,
    // which is why small backups and restores kept working.
    let result = upload_parts(bucket, &mut file, first, key, &upload.upload_id).await;
    if result.is_err() {
        // Best effort: an unfinished multipart upload keeps its parts stored.
        let _ = bucket.abort_upload(key, &upload.upload_id).await;
    }
    result
}

async fn upload_parts(
    bucket: &Bucket,
    file: &mut tokio::fs::File,
    first: Vec<u8>,
    key: &str,
    upload_id: &str,
) -> Result<()> {
    let mut parts: Vec<Part> = Vec::new();
    let mut chunk = first;
    while !chunk.is_empty() {
        let part_number = parts.len() as u32 + 1;
        let etag = with_retry(&format!("upload part {part_number}"), || {
            put_part(bucket, key, upload_id, part_number, &chunk)
        })
        .await?;
        parts.push(Part { etag, part_number });
        chunk = read_part(file).await?;
    }
    with_retry("complete multipart upload", || async {
        let resp = bucket
            .complete_multipart_upload(key, upload_id, parts.clone())
            .await?;
        check_status(&resp)
    })
    .await
}

/// PUT one part, answering its ETag. Not rust-s3's `put_multipart_chunk`: on
/// any non-2xx answer that aborts the whole multipart upload before returning,
/// so the part could never be tried again.
async fn put_part(
    bucket: &Bucket,
    key: &str,
    upload_id: &str,
    part_number: u32,
    chunk: &[u8],
) -> Result<String, Failure> {
    let command = Command::PutObject {
        content: chunk,
        content_type: GZIP_CONTENT_TYPE,
        multipart: Some(Multipart::new(part_number, upload_id)),
    };
    let resp = HyperRequest::new(bucket, key, command)
        .await?
        .response_data(true)
        .await?;
    check_status(&resp)?;
    match resp.as_str() {
        Ok(etag) if !etag.is_empty() => Ok(etag.to_string()),
        _ => Err(Failure::Transient(anyhow::anyhow!(
            "part {part_number} answered without an ETag"
        ))),
    }
}

/// Next part of the file: exactly [`CHUNK_SIZE`] bytes, or the rest at EOF.
async fn read_part(file: &mut tokio::fs::File) -> Result<Vec<u8>> {
    let mut part = Vec::with_capacity(CHUNK_SIZE);
    file.take(CHUNK_SIZE as u64)
        .read_to_end(&mut part)
        .await
        .context("Failed to read upload part")?;
    Ok(part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Hetzner Object Storage behind a virtual-hosted endpoint echoes the key
    /// with the bucket prefixed. Parts and completion must still go to the key
    /// the upload was started with, or the store answers `NoSuchUpload`.
    #[tokio::test]
    async fn multipart_upload_ignores_echoed_key() {
        let server = MockServer::start().await;
        let object = "/backups/whitepawn/b1.surql.gz";
        let upload_id = "2~abc";

        Mock::given(method("POST"))
            .and(path(object))
            .and(query_param("uploads", ""))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "<InitiateMultipartUploadResult><Bucket>backups</Bucket>\
                 <Key>backups/whitepawn/b1.surql.gz</Key><UploadId>{upload_id}</UploadId>\
                 </InitiateMultipartUploadResult>"
            )))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(object))
            .and(query_param("uploadId", upload_id))
            .respond_with(ResponseTemplate::new(200).insert_header("ETag", "\"e\""))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(object))
            .and(query_param("uploadId", upload_id))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        // Anything addressed elsewhere is what Ceph rejects in production.
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(404).set_body_string(
                "<Error><Code>NoSuchUpload</Code></Error>",
            ))
            .mount(&server)
            .await;

        let config = BackupConfig {
            s3_endpoint: server.uri(),
            s3_access_key: "k".into(),
            s3_secret_key: "s".into(),
            s3_bucket: "backups".into(),
            s3_region: "fsn1".into(),
        };
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&vec![7u8; CHUNK_SIZE + 10]).unwrap();

        let bucket = config.get_bucket().unwrap();
        upload_file(&bucket, file.path(), "whitepawn/b1.surql.gz")
            .await
            .unwrap();
    }

    fn test_bucket(server: &MockServer) -> Box<Bucket> {
        BackupConfig {
            s3_endpoint: server.uri(),
            s3_access_key: "k".into(),
            s3_secret_key: "s".into(),
            s3_bucket: "backups".into(),
            s3_region: "auto".into(),
        }
        .get_bucket()
        .unwrap()
    }

    fn two_part_file() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&vec![7u8; CHUNK_SIZE + 10]).unwrap();
        file
    }

    async fn mount_initiate(server: &MockServer, object: &str, upload_id: &str) {
        Mock::given(method("POST"))
            .and(path(object))
            .and(query_param("uploads", ""))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "<InitiateMultipartUploadResult><Bucket>backups</Bucket>\
                 <Key>whitepawn/b.surql.gz</Key><UploadId>{upload_id}</UploadId>\
                 </InitiateMultipartUploadResult>"
            )))
            .expect(1)
            .mount(server)
            .await;
    }

    /// A busy store answers a part with 503 `SlowDown`: the part is sent again
    /// and the upload goes on, instead of the whole backup failing.
    #[tokio::test]
    async fn part_is_retried_after_slowdown() {
        let server = MockServer::start().await;
        let object = "/backups/whitepawn/b.surql.gz";
        let upload_id = "2~retry";
        mount_initiate(&server, object, upload_id).await;
        Mock::given(method("PUT"))
            .and(path(object))
            .and(query_param("partNumber", "1"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_body_string("<Error><Code>SlowDown</Code></Error>"),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(object))
            .and(query_param("uploadId", upload_id))
            .respond_with(ResponseTemplate::new(200).insert_header("ETag", "\"e\""))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(object))
            .and(query_param("uploadId", upload_id))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;

        let file = two_part_file();
        upload_file(&test_bucket(&server), file.path(), "whitepawn/b.surql.gz")
            .await
            .unwrap();
    }

    /// A 403 cannot get better by asking again: the upload fails on the first
    /// answer and is aborted.
    #[tokio::test]
    async fn forbidden_part_fails_without_retry() {
        let server = MockServer::start().await;
        let object = "/backups/whitepawn/b.surql.gz";
        let upload_id = "2~denied";
        mount_initiate(&server, object, upload_id).await;
        Mock::given(method("PUT"))
            .and(path(object))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string("<Error><Code>AccessDenied</Code></Error>"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(object))
            .and(query_param("uploadId", upload_id))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        let file = two_part_file();
        let err = upload_file(&test_bucket(&server), file.path(), "whitepawn/b.surql.gz")
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("Failed to upload part 1"), "{err:#}");
    }
}
