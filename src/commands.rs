use crate::compressor::compress;
use crate::helpers::parse_url;
use crate::object_storage::delete_keys;
use crate::s3::{get_client, get_s3_params};
use aws_sdk_s3::primitives::DateTime;
use chrono::{DateTime as ChronoDateTime, Duration, Utc};
use std::error::Error;
use std::sync::Arc;

pub async fn archive(
    src: String,
    dst: String,
    cutoff: Option<ChronoDateTime<Utc>>,
    buffer_size: usize,
) -> Result<(), Box<dyn Error>> {
    let (src_bucket, src_prefix) = match parse_url(&src) {
        Some(values) => values,
        None => {
            panic!("Invalid source URL");
        }
    };

    let (dst_bucket, dst_prefix) = match parse_url(&dst) {
        Some(values) => values,
        None => {
            panic!("Invalid destination URL");
        }
    };

    let dst_object_key = match &dst_prefix {
        Some(prefix) => {
            if prefix.ends_with('/') {
                format!("{}archive.tar.bz2", prefix)
            } else {
                format!("{}/archive.tar.bz2", prefix)
            }
        }
        None => "archive.tar.bz2".to_string(),
    };

    let s3_params = get_s3_params();
    let src_client = get_client(&s3_params);
    let dst_client = get_client(&s3_params);

    let cutoff_dt = cutoff.unwrap_or_else(|| {
        let now = Utc::now();
        now - Duration::seconds(1)
    });
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
        buffer_size,
        &mut archived_keys,
    )
    .await
    {
        eprintln!("Error compressing objects: {e}");
    }

    let src_bucket_str = src_bucket.as_str();

    if let Err(e) = delete_keys(Arc::new(src_client), src_bucket_str, archived_keys).await {
        eprintln!("Error deleting archived keys: {e}");
    }

    Ok(())
}
