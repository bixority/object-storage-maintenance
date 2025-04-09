use crate::uploader::MultipartUploadSink;
use async_compression::tokio::write::XzEncoder;
use async_compression::Level;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::Client;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_tar::{Builder, Header};

async fn compress_object(
    resp: GetObjectOutput,
    size: i64,
    last_modified: DateTime,
    key: String,
    tar_builder: &mut Builder<XzEncoder<MultipartUploadSink>>,
    processed_keys: &mut Vec<String>,
) {
    let stream = resp.body.into_async_read();

    let mut header = Header::new_gnu();
    header.set_size(size.try_into().expect("object size must be non-negative"));
    header.set_mode(0o644);
    header.set_mtime(
        last_modified
            .secs()
            .try_into()
            .expect("mtime must be non-negative"),
    );
    header.set_cksum();
    tar_builder
        .append_data(&mut header, &key, stream)
        .await
        .unwrap();

    processed_keys.push(key);
}

async fn process_object(
    obj: Object,
    cutoff_aws_dt: DateTime,
    src_client: Arc<Client>,
    src_bucket_str: &str,
    tar_builder: &mut Builder<XzEncoder<MultipartUploadSink>>,
    processed_keys: &mut Vec<String>,
) {
    if obj.last_modified < Some(cutoff_aws_dt) {
        if let Some(key) = obj.key {
            let Some(last_modified) = obj.last_modified else {
                todo!()
            };
            let Some(size) = obj.size else { todo!() };

            let object = src_client
                .get_object()
                .bucket(src_bucket_str)
                .key(&key)
                .send()
                .await;

            match object {
                Ok(resp) => {
                    compress_object(resp, size, last_modified, key, tar_builder, processed_keys)
                        .await;
                }
                Err(e) => {
                    eprintln!("Failed to fetch object '{key}': {e}");
                }
            }
        }
    }
}

async fn process_objects(
    src_client: Arc<Client>,
    src_bucket_str: &str,
    src_prefix: Option<String>,
    cutoff_aws_dt: DateTime,
    tar_builder: &mut Builder<XzEncoder<MultipartUploadSink>>,
    processed_keys: &mut Vec<String>,
) {
    let mut continuation_token = None;

    loop {
        let mut request = src_client.list_objects_v2().bucket(src_bucket_str);

        if let Some(ref prefix) = src_prefix {
            request = request.prefix(prefix);
        }

        if let Some(token) = continuation_token.clone() {
            request = request.continuation_token(token);
        }

        match request.send().await {
            Ok(response) => {
                if let Some(contents) = response.contents {
                    for obj in contents {
                        process_object(
                            obj,
                            cutoff_aws_dt,
                            src_client.clone(),
                            src_bucket_str,
                            tar_builder,
                            processed_keys,
                        )
                        .await;
                    }
                }

                if response.next_continuation_token.is_none() {
                    break;
                }

                continuation_token = response.next_continuation_token;
            }
            Err(e) => {
                eprintln!("Failed to list objects: {e:?}");

                if let Some(source) = e.source() {
                    eprintln!("Caused by: {source:?}");
                }

                panic!("Detailed error: {e:#?}");
            }
        }
    }
}

pub async fn compress(
    src_client: Arc<Client>,
    src_bucket: String,
    src_prefix: Option<String>,
    dst_client: Arc<Client>,
    dst_bucket: String,
    dst_object_key: String,
    cutoff_aws_dt: DateTime,
    buffer_size: usize,
    processed_keys: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let src_bucket_str = src_bucket.as_str();
    let sink = MultipartUploadSink::new(dst_client, dst_bucket, dst_object_key, buffer_size);
    let encoder = XzEncoder::with_quality(sink, Level::Best);
    let mut tar_builder = Builder::new(encoder);

    process_objects(
        src_client,
        src_bucket_str,
        src_prefix,
        cutoff_aws_dt,
        &mut tar_builder,
        processed_keys,
    )
    .await;

    tar_builder.finish().await.unwrap();
    let mut encoder = tar_builder.into_inner().await.unwrap();

    if let Err(e) = encoder.flush().await {
        eprintln!("Encoder flush failed: {e:?}");

        return Err(e.into());
    }

    if let Err(e) = encoder.shutdown().await {
        eprintln!("Encoder shutdown failed: {e:?}");

        return Err(e.into());
    }

    Ok(())
}

#[cfg(test)]
mod tests;
