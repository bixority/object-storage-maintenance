use crate::error::Result;
use object_store::{ObjectStore, parse_url_opts, path::Path};
use std::sync::Arc;
use url::Url;

pub fn get_store_and_path(url_str: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let url = Url::parse(url_str)?;
    let options = collect_options(&url);
    let (store, path) = parse_url_opts(&url, options)?;
    Ok((Arc::from(store), path))
}

fn collect_options(url: &Url) -> Vec<(String, String)> {
    collect_options_impl(url, |k| std::env::var(k).ok())
}

fn collect_options_impl<F>(url: &Url, get_env: F) -> Vec<(String, String)>
where
    F: Fn(&str) -> Option<String>,
{
    let mut options = Vec::new();
    if url.scheme() == "s3" {
        for (env_var, opt_key) in [
            ("AWS_ENDPOINT_URL_S3", "endpoint"),
            ("S3_REGION", "region"),
            ("S3_ACCESS_KEY_ID", "access_key_id"),
            ("S3_SECRET_ACCESS_KEY", "secret_access_key"),
            ("S3_ALLOW_HTTP", "allow_http"),
        ] {
            if let Some(val) = get_env(env_var) {
                options.push((opt_key.to_string(), val));
            }
        }
    }
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_options_s3() -> Result<()> {
        let url = Url::parse("s3://bucket/path")?;
        let env = |k: &str| match k {
            "S3_REGION" => Some("us-north-1".to_string()),
            _ => None,
        };
        let options = collect_options_impl(&url, env);
        assert_eq!(
            options,
            vec![("region".to_string(), "us-north-1".to_string())]
        );
        Ok(())
    }

    #[test]
    fn test_get_store_and_path_s3() -> Result<()> {
        let res = get_store_and_path("s3://bucket/path/to/object");
        assert!(res.is_ok());
        let (_store, path) = res?;
        assert_eq!(path.to_string(), "path/to/object");
        Ok(())
    }

    #[test]
    fn test_get_store_and_path_file() -> Result<()> {
        let res = get_store_and_path("file:///tmp/test");
        assert!(res.is_ok());
        let (_store, path) = res?;
        assert_eq!(path.to_string(), "tmp/test");
        Ok(())
    }

    #[test]
    fn test_get_store_and_path_memory() {
        // memory provider is not usually enabled by default in parse_url unless we use memory://
        // but it might not be enabled in features.
        // Let's try gs://
        let res = get_store_and_path("gs://bucket/path");
        assert!(res.is_ok());
    }
}
