use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::{Client, Config, Error};
use aws_types::region::Region;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use chrono::{Duration, Utc};
use clap::Parser;
use std::env;
use tar::Builder;

struct S3Params {
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    src_prefix: String,

    #[arg(short, long)]
    dst_object: String,
}

fn get_client(params: &S3Params) -> Client {
    let config = Config::builder()
        .region(Region::new(params.region.clone()))
        .endpoint_url(&params.endpoint)
        .credentials_provider(Credentials::new(
            &params.access_key,
            &params.secret_key,
            None,
            None,
            "static",
        ))
        .build();

    let client = Client::from_conf(config);

    client
}

async fn delete_archived_keys(
    client: Client,
    bucket_name: &str,
    keys: Vec<String>,
) -> Result<(), Error> {
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

fn get_s3_params() -> S3Params {
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let endpoint =
        env::var("OBJECT_STORAGE_ENDPOINT").expect("OBJECT_STORAGE_ENDPOINT must be set");
    let access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY must be set");
    let secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_KEY must be set");

    let params = S3Params {
        region,
        access_key,
        secret_key,
        endpoint,
    };

    params
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let src_prefix = args.src_prefix;
    let dst_object = args.dst_object;

    let src_bucket = env::var("OBJECT_STORAGE_BUCKET").expect("OBJECT_STORAGE_BUCKET must be set");
    let src_bucket_str = src_bucket.as_str();
    let s3_params = get_s3_params();
    let src_client = get_client(&s3_params);

    let dst_bucket = src_bucket.clone();
    let dst_bucket_str = dst_bucket.as_str();
    let dst_client = get_client(&s3_params);

    let mut archive_buffer: Vec<u8> = Vec::new();
    let bz2_encoder = BzEncoder::new(&mut archive_buffer, Compression::best());
    let mut tar_builder = Builder::new(bz2_encoder);

    let now = Utc::now();
    let cutoff_dt = now - Duration::seconds(24 * 60 * 60);
    let cutoff_aws_dt = DateTime::from_secs(cutoff_dt.timestamp());

    let mut continuation_token = None;
    let mut archived_keys: Vec<String> = vec![];

    loop {
        let mut request = src_client
            .list_objects_v2()
            .bucket(src_bucket_str)
            .prefix(&src_prefix);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await?;

        if let Some(contents) = response.contents {
            for obj in contents.into_iter() {
                if obj.last_modified < Some(cutoff_aws_dt) {
                    if let Some(key) = obj.key {
                        match src_client
                            .get_object()
                            .bucket(src_bucket_str)
                            .key(&key)
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                let data = resp.body.collect().await.unwrap().into_bytes();
                                let mut header = tar::Header::new_gnu();

                                header.set_path(&key)?;
                                header.set_size(data.len() as u64);
                                header.set_mode(0o644);
                                header.set_cksum();
                                tar_builder.append(&header, data.as_ref())?;
                                archived_keys.push(key);
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

    tar_builder.finish()?;
    let bz2_encoder = tar_builder.into_inner()?;
    bz2_encoder.finish()?;

    let body = ByteStream::from(archive_buffer);
    match dst_client
        .put_object()
        .bucket(dst_bucket_str)
        .key(&dst_object)
        .body(body)
        .send()
        .await
    {
        Ok(_) => println!(
            "Successfully uploaded tar.bz2 archive to {}/{}",
            dst_bucket_str, &dst_object
        ),
        Err(e) => eprintln!("Failed to upload archive: {}", e),
    }

    if let Err(e) = delete_archived_keys(src_client, src_bucket_str, archived_keys).await {
        eprintln!("Error deleting archived keys: {}", e);
    }

    Ok(())
}
