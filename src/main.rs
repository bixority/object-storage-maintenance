use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::primitives::{ByteStreamError, DateTime};
use aws_sdk_s3::types::{Delete, Object, ObjectIdentifier};
use aws_sdk_s3::{Client, Config};
use aws_types::region::Region;
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use std::error::Error;
use std::sync::Arc;
use std::{env, fmt};
use tokio::io::AsyncWriteExt;
use tokio::{task, try_join};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use zip::{write::SimpleFileOptions, ZipWriter};

const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MB
const MAX_QUEUE_SIZE: usize = 16 * 1024 * 1024;
const COMPRESSED_CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5 MB

#[derive(Debug)]
pub enum S3Error {
    ListObjectsError(SdkError<ListObjectsV2Error>),
    GetObjectError(SdkError<GetObjectError>),
    ByteStreamError(ByteStreamError),
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::ListObjectsError(e) => write!(f, "ListObjectsV2 error: {}", e),
            S3Error::GetObjectError(e) => write!(f, "GetObject error: {}", e),
            S3Error::ByteStreamError(e) => write!(f, "ByteStream error: {}", e),
        }
    }
}

impl Error for S3Error {}

struct S3Params {
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Archive {
        #[arg(long)]
        src_bucket: String,

        #[arg(long)]
        src_prefix: String,

        #[arg(long)]
        dst_bucket: String,

        #[arg(long)]
        dst_prefix: String,
    },
}

#[derive(Parser, Debug)]
#[command(version, about = "Object storage maintenance tool", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Clone)]
struct ObjectChunk {
    object_key: String,
    object_size: i64,
    last_modified: DateTime,
    data: Arc<[u8]>,
    is_last_chunk: bool,
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

async fn process_object(
    s3_client: &Arc<Client>,
    bucket: &String,
    obj: Object,
    cutoff_aws_dt: DateTime,
    data_tx: &Sender<ObjectChunk>,
) {
    if obj.last_modified < Some(cutoff_aws_dt) {
        if let Some(key) = obj.key {
            let Some(last_modified) = obj.last_modified else {
                todo!()
            };
            let Some(size) = obj.size else { todo!() };
            let object_key = key.clone();

            let object = s3_client.get_object().bucket(bucket).key(&key).send().await;

            match object {
                Ok(resp) => {
                    let stream = resp.body.into_async_read();
                    let mut reader = ReaderStream::with_capacity(stream, CHUNK_SIZE);

                    while let Some(chunk) = reader.next().await {
                        match chunk {
                            Ok(data) => {
                                let object_chunk = ObjectChunk {
                                    object_key: object_key.clone(),
                                    last_modified,
                                    object_size: size,
                                    data: data.into_iter().collect(),
                                    is_last_chunk: false,
                                };
                                data_tx
                                    .send(object_chunk)
                                    .await
                                    .expect("Failed to send chunk");
                            }
                            Err(e) => {
                                eprintln!("Error while reading S3 object {}: {}", key, e);
                                break;
                            }
                        }
                    }
                    // archived_keys.push(key);
                }
                Err(e) => {
                    eprintln!("Failed to fetch object '{}': {}", key, e);
                }
            }
        }
    }
}

async fn iter_objects(
    s3_client: Arc<Client>,
    bucket: String,
    prefix: String,
    cutoff_aws_dt: DateTime,
    data_tx: Sender<ObjectChunk>,
) {
    let mut continuation_token = None;

    loop {
        let mut request = s3_client.list_objects_v2().bucket(&bucket).prefix(&prefix);

        if let Some(token) = continuation_token.clone() {
            request = request.continuation_token(token);
        }

        match request.send().await {
            Ok(response) => {
                if let Some(contents) = response.contents {
                    for obj in contents.into_iter() {
                        process_object(&s3_client, &bucket, obj, cutoff_aws_dt, &data_tx).await;
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
}

async fn archive_objects(mut data_rx: Receiver<ObjectChunk>, archive_tx: Sender<Arc<[u8]>>) {
    let mut zip_writer = ZipWriter::new(archive_tx);
    let mut is_new_file: bool = true;

    while let Some(chunk) = data_rx.recv().await {
        if is_new_file {
            zip_writer
                .start_file(&chunk.object_key, SimpleFileOptions::default())
                .unwrap();
        }

        if let Err(e) = zip_writer.write(&chunk.data) {
            eprintln!("Failed to write chunk for {}: {}", chunk.object_key, e);
            continue;
        }

        is_new_file = chunk.is_last_chunk;
    }

    if let Err(e) = zip_writer.finish() {
        eprintln!("Failed to finish ZIP archive: {}", e);
    }
}

async fn store_archive(
    s3_params: &S3Params,
    bucket: String,
    archive_rx: Receiver<Arc<[u8]>>,
    target_key: String,
) {
    let s3_client = get_client(&s3_params);

    s3_client
        .put_object()
        .bucket(bucket)
        .key(target_key)
        .body(archive_rx)
        .send()
        .await
        .expect("Failed to upload archive to S3");
}

async fn archive(
    src_bucket: String,
    src_prefix: String,
    dst_bucket: String,
    dst_prefix: String,
) -> Result<(), Box<dyn Error>> {
    let src_bucket_str = src_bucket.as_str();
    let s3_params = get_s3_params();
    let src_client = get_client(&s3_params);

    // let dst_bucket_str = dst_bucket.as_str();
    let dst_object_key = dst_prefix + "archive.zip";

    let now = Utc::now();
    let cutoff_dt = now - Duration::seconds(24 * 60 * 60);
    let cutoff_aws_dt = DateTime::from_secs(cutoff_dt.timestamp());

    let archived_keys: Vec<String> = Vec::new();

    let (data_tx, mut data_rx) = channel(MAX_QUEUE_SIZE);
    let (archive_tx, mut archive_rx) =  channel(MAX_QUEUE_SIZE);

    let client_clone = src_client.clone();
    let iter_handle = task::spawn(iter_objects(
        Arc::new(client_clone),
        src_bucket.clone(),
        src_prefix.clone(),
        cutoff_aws_dt,
        data_tx,
    ));

    let archive_handle = task::spawn(archive_objects(data_rx, archive_tx));

    let store_handle = task::spawn(store_archive(
        &s3_params,
        dst_bucket,
        archive_rx,
        dst_object_key,
    ));

    try_join!(iter_handle, archive_handle, store_handle)?;

    if let Err(e) = delete_archived_keys(src_client, src_bucket_str, archived_keys).await {
        eprintln!("Error deleting archived keys: {}", e);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.command {
        Some(Commands::Archive {
            src_bucket,
            src_prefix,
            dst_bucket,
            dst_prefix,
        }) => {
            if let Err(e) = archive(src_bucket, src_prefix, dst_bucket, dst_prefix).await {
                eprintln!("Error running 'archive' command: {}", e);
            }
        }
        None => {
            println!("No subcommand selected. Add a subcommand like 'archive'.");
        }
    }

    Ok(())
}
