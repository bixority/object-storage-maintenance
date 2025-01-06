use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bzip2::bufread::BzEncoder;
use bzip2::Compression;
use chrono::{Duration, Utc};
use std::error::Error;
use tar::{Builder, Header};
use tokio_util::io::ReaderStream;

const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MB
const COMPRESSED_CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5 MB

pub async fn delete_keys(
    client: Client,
    bucket_name: &str,
    keys: Vec<String>,
) -> Result<(), aws_sdk_s3::Error> {
    for chunk in keys.chunks(1000) {
        let objects_to_delete: Vec<ObjectIdentifier> = chunk
            .iter()
            .filter_map(|key| match ObjectIdentifier::builder().key(key).build() {
                Ok(obj) => Some(obj),
                Err(err) => {
                    eprintln!(
                        "Failed to build ObjectIdentifier for key '{}': {}",
                        key, err
                    );
                    None
                }
            })
            .collect();

        if objects_to_delete.is_empty() {
            eprintln!("No valid objects to delete in this chunk.");
            continue;
        }

        let delete = Delete::builder()
            .set_objects(Some(objects_to_delete))
            .build()?;

        match client
            .delete_objects()
            .bucket(bucket_name)
            .delete(delete)
            .send()
            .await
        {
            Ok(response) => {
                let deleted_objects = response.deleted();
                if !deleted_objects.is_empty() {
                    println!("Successfully deleted objects: {:?}", deleted_objects);
                }

                let errors = response.errors();
                if !errors.is_empty() {
                    eprintln!("Failed to delete some objects: {:?}", errors);
                }
            }
            Err(err) => {
                eprintln!("Error occurred while deleting objects: {}", err);
                return Err(err.into());
            }
        }
    }

    Ok(())
}

pub async fn compress(
    src_client: &Client,
    src_bucket: String,
    src_prefix: String,
    dst_client: &Client,
    dst_bucket: String,
    dst_prefix: String,
    processed_keys: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let src_bucket_str = src_bucket.as_str();

    let dst_object_key = dst_prefix + "archive.tar.bz2";

    let now = Utc::now();
    let cutoff_dt = now - Duration::seconds(24 * 60 * 60);
    let cutoff_aws_dt = DateTime::from_secs(cutoff_dt.timestamp());

    let bz2_encoder = BzEncoder::new(sink, Compression::best());
    let mut tar_builder = Builder::new(bz2_encoder);

    let mut continuation_token = None;

    loop {
        let mut request = src_client
            .list_objects_v2()
            .bucket(src_bucket_str)
            .prefix(&src_prefix);

        if let Some(token) = continuation_token.clone() {
            request = request.continuation_token(token);
        }

        match request.send().await {
            Ok(response) => {
                if let Some(contents) = response.contents {
                    for obj in contents.into_iter() {
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
                                        let stream = resp.body.into_async_read();
                                        let mut reader =
                                            ReaderStream::with_capacity(stream, CHUNK_SIZE);

                                        let mut header = Header::new_gnu();
                                        header.set_size(size as u64);
                                        header.set_mode(0o644);
                                        header.set_cksum();
                                        tar_builder.append_data(&mut header, &key, &mut reader)?;

                                        processed_keys.push(key);
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to fetch object '{}': {}", key, e);
                                    }
                                }
                            }
                        }
                    }
                }

                if response.next_continuation_token.is_none() {
                    break;
                }

                continuation_token = response.next_continuation_token;
            }
            Err(e) => {
                eprintln!("Failed to list objects: {}", e)
            }
        }
    }

    tar_builder.finish()?;

    Ok(())
}
