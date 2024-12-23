use aws_sdk_s3::{types::ByteStream, Client, Config, Region};
use aws_types::credentials::Credentials;
use flate2::write::BzEncoder;
use flate2::Compression;
use std::io::Write;
use aws_sdk_s3::config::Credentials;
use aws_types::region::Region;
use tar::Builder;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Source MinIO configuration
    let source_region = "us-east-1";
    let source_endpoint = "http://YOUR_SOURCE_MINIO_ENDPOINT";
    let source_access_key = "YOUR_SOURCE_ACCESS_KEY";
    let source_secret_key = "YOUR_SOURCE_SECRET_KEY";
    let source_bucket = "YOUR_SOURCE_BUCKET";

    // Destination MinIO configuration
    let dest_region = "us-east-1";
    let dest_endpoint = "http://YOUR_DEST_MINIO_ENDPOINT";
    let dest_access_key = "YOUR_DEST_ACCESS_KEY";
    let dest_secret_key = "YOUR_DEST_SECRET_KEY";
    let dest_bucket = "YOUR_DEST_BUCKET";
    let dest_object = "output.tar.bz2";

    // Initialize source and destination S3 clients
    let source_config = Config::builder()
        .region(Region::new(source_region.to_string()))
        .endpoint_url(source_endpoint)
        .credentials_provider(Credentials::new(
            source_access_key,
            source_secret_key,
            None,
            None,
            "static",
        ))
        .build();
    let source_client = Client::from_conf(source_config);

    let dest_config = Config::builder()
        .region(Region::new(dest_region.to_string()))
        .endpoint_url(dest_endpoint)
        .credentials_provider(Credentials::new(
            dest_access_key,
            dest_secret_key,
            None,
            None,
            "static",
        ))
        .build();
    let dest_client = Client::from_conf(dest_config);

    // Create a pipe for streaming the tar.bz2 archive
    let (reader, writer) = tokio::io::duplex(8192); // Increased buffer size to 8KB

    // Spawn a task to create the tar.bz2 archive and write to the pipe
    let archive_task = tokio::spawn(async move {
        let bz2_encoder = BzEncoder::new(writer, Compression::best());
        let mut tar_builder = Builder::new(bz2_encoder);

        // List objects in the source bucket
        match source_client
            .list_objects_v2()
            .bucket(source_bucket)
            .send()
            .await
        {
            Ok(objects) => {
                if let Some(contents) = objects.contents {
                    for object in contents {
                        if let Some(key) = object.key {
                            // Get the object from the source bucket
                            match source_client
                                .get_object()
                                .bucket(source_bucket)
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
                eprintln!("Failed to list objects in bucket '{}': {}", source_bucket, e);
            }
        }

        if let Err(e) = tar_builder.finish() {
            eprintln!("Error finishing tar archive: {}", e);
        }
    });

    // Stream the tar.bz2 archive to the destination bucket
    let body = ByteStream::from_reader(reader);
    match dest_client
        .put_object()
        .bucket(dest_bucket)
        .key(dest_object)
        .body(body)
        .send()
        .await
    {
        Ok(_) => println!(
            "Successfully uploaded tar.bz2 archive to {}/{}",
            dest_bucket, dest_object
        ),
        Err(e) => eprintln!("Failed to upload archive: {}", e),
    }

    // Wait for the archive task to complete
    if let Err(e) = archive_task.await {
        eprintln!("Archive task failed: {}", e);
    }

    Ok(())
}
