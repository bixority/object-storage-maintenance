use crate::object_storage::{compress, delete_keys};
use crate::s3::{get_client, get_s3_params};
use aws_sdk_s3::primitives::DateTime;
use chrono::{Duration, Utc};
use std::error::Error;
use std::sync::Arc;

pub async fn archive(
    src_bucket: String,
    src_prefix: String,
    dst_bucket: String,
    dst_prefix: String
) -> Result<(), Box<dyn Error>> {
    let s3_params = get_s3_params();
    let src_client = get_client(&s3_params);

    let dst_object_key = dst_prefix + "archive.zip";
    let dst_client = get_client(&s3_params);

    let now = Utc::now();
    let cutoff_dt = now - Duration::seconds(24 * 60 * 60);
    let cutoff_aws_dt = DateTime::from_secs(cutoff_dt.timestamp());

    let mut archived_keys: Vec<String> = Vec::new();

    if let Err(e) = compress(
        Arc::new(src_client.clone()),
        src_bucket.clone(),
        src_prefix,
        Arc::new(dst_client),
        dst_bucket,
        dst_object_key,
        cutoff_aws_dt,
        &mut archived_keys,
    )
    .await
    {
        eprintln!("Error deleting archived keys: {}", e);
    }

    let src_bucket_str = src_bucket.as_str();

    if let Err(e) = delete_keys(Arc::new(src_client), src_bucket_str, archived_keys).await {
        eprintln!("Error deleting archived keys: {}", e);
    }

    Ok(())
}
