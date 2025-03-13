use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config};
use aws_types::region::Region;
use std::env;

pub struct S3Params {
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: Option<String>,
}

pub fn get_s3_params() -> S3Params {
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let endpoint = env::var("OBJECT_STORAGE_ENDPOINT").ok();
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
    let mut builder = Config::builder()
        .region(Region::new(params.region.clone()))
        .credentials_provider(Credentials::new(
            &params.access_key,
            &params.secret_key,
            None,
            None,
            "static",
        ));

    if let Some(ref endpoint) = params.endpoint {
        builder = builder.endpoint_url(endpoint);
    }

    let config = builder.build();
    let client = Client::from_conf(config);

    client
}
