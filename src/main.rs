use std::env;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use aws_types::region::Region;
use bzip2::write::BzEncoder; // Import the synchronous BzEncoder
use bzip2::Compression;
use tar::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Source MinIO configuration
    let src_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let src_endpoint = env::var("OBJECT_STORAGE_ENDPOINT").expect("OBJECT_STORAGE_ENDPOINT must be set");
    let src_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY must be set");
    let src_secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_KEY must be set");
    let src_bucket = env::var("OBJECT_STORAGE_BUCKET").expect("OBJECT_STORAGE_BUCKET must be set");
    let src_bucket_str = src_bucket.as_str();

    // Destination MinIO configuration
    let dst_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let dst_endpoint = env::var("OBJECT_STORAGE_ENDPOINT").expect("OBJECT_STORAGE_ENDPOINT must be set");
    let dst_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY must be set");
    let dst_secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_KEY must be set");
    let dst_bucket = env::var("OBJECT_STORAGE_BUCKET").expect("OBJECT_STORAGE_BUCKET must be set");
    let dst_bucket_str = dst_bucket.as_str();
    let dst_object = "output.tar.bz2";

    // Initialize source and destination S3 clients
    let src_config = Config::builder()
        .region(Region::new(src_region.to_string()))
        .endpoint_url(src_endpoint)
        .credentials_provider(Credentials::new(
            src_access_key,
            src_secret_key,
            None,
            None,
            "static",
        ))
        .build();
    let src_client = Client::from_conf(src_config);

    let dst_config = Config::builder()
        .region(Region::new(dst_region.to_string()))
        .endpoint_url(dst_endpoint)
        .credentials_provider(Credentials::new(
            dst_access_key,
            dst_secret_key,
            None,
            None,
            "static",
        ))
        .build();
    let dst_client = Client::from_conf(dst_config);

    // Create an in-memory buffer for the tar.bz2 archive
    let mut archive_buffer: Vec<u8> = Vec::new();
    let bz2_encoder = BzEncoder::new(&mut archive_buffer, Compression::best()); // Synchronous BzEncoder
    let mut tar_builder = Builder::new(bz2_encoder);

    // List objects in the source bucket
    match src_client
        .list_objects_v2()
        .bucket(src_bucket_str)
        .send()
        .await
    {
        Ok(objects) => {
            if let Some(contents) = objects.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        // Get the object from the source bucket
                        match src_client
                            .get_object()
                            .bucket(src_bucket_str)
                            .key(&key)
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                let data = resp.body.collect().await.unwrap().into_bytes();

                                // Add the object to the tar archive
                                let mut header = tar::Header::new_gnu();
                                header.set_path(&key)?;
                                header.set_size(data.len() as u64);
                                header.set_mode(0o644);
                                header.set_cksum();
                                tar_builder.append(&header, data.as_ref())?;
                            }
                            Err(e) => {
                                eprintln!("Failed to fetch object '{}': {}", key, e);
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to list objects in bucket '{}': {}", src_bucket_str, e);
        }
    }

    // Finalize the tar archive and Bzip2 compression
    tar_builder.finish()?;
    let bz2_encoder = tar_builder.into_inner()?; // Retrieve the BzEncoder
    bz2_encoder.finish()?; // Finalize Bzip2 compression

    // Upload the tar.bz2 archive to the destination bucket
    let body = ByteStream::from(archive_buffer);
    match dst_client
        .put_object()
        .bucket(dst_bucket_str)
        .key(dst_object)
        .body(body)
        .send()
        .await
    {
        Ok(_) => println!(
            "Successfully uploaded tar.bz2 archive to {}/{}",
            dst_bucket_str, dst_object
        ),
        Err(e) => eprintln!("Failed to upload archive: {}", e),
    }

    Ok(())
}
