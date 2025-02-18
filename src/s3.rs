use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::primitives::ByteStreamError;
use aws_sdk_s3::{Client, Config};
use aws_types::region::Region;
use std::error::Error;
use std::{env, fmt};

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

pub struct S3Params {
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
}

pub fn get_s3_params() -> S3Params {
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

pub fn get_client(params: &S3Params) -> Client {
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
