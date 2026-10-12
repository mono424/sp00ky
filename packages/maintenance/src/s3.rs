use anyhow::{Context, Result};
use s3::bucket::{Bucket, CHUNK_SIZE};
use s3::creds::Credentials;
use s3::serde_types::Part;
use s3::Region;
use std::path::Path;
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
        let resp = bucket
            .put_object_with_content_type(key, &first, GZIP_CONTENT_TYPE)
            .await?;
        if resp.status_code() != 200 {
            anyhow::bail!("S3 upload returned status {}", resp.status_code());
        }
        return Ok(());
    }

    let upload = bucket
        .initiate_multipart_upload(key, GZIP_CONTENT_TYPE)
        .await
        .context("Failed to start multipart upload")?;
    let result = upload_parts(bucket, &mut file, first, &upload.key, &upload.upload_id).await;
    if result.is_err() {
        // Best effort: an unfinished multipart upload keeps its parts stored.
        let _ = bucket.abort_upload(&upload.key, &upload.upload_id).await;
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
        let part = bucket
            .put_multipart_chunk(chunk, key, part_number, upload_id, GZIP_CONTENT_TYPE)
            .await
            .with_context(|| format!("Failed to upload part {part_number}"))?;
        parts.push(part);
        chunk = read_part(file).await?;
    }
    let resp = bucket
        .complete_multipart_upload(key, upload_id, parts)
        .await
        .context("Failed to complete multipart upload")?;
    if resp.status_code() != 200 {
        anyhow::bail!("S3 multipart completion returned status {}", resp.status_code());
    }
    Ok(())
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
