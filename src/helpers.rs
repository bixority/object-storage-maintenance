pub fn parse_url(url: &str) -> Option<(String, Option<String>)> {
    let mut parts = url.splitn(2, "://");
    let protocol = parts.next()?.to_string();
    let rest = parts.next()?;
    let mut rest_parts = rest.splitn(2, '/');
    let bucket = rest_parts.next()?.to_string();
    let prefix = rest_parts.next().map(|s| s.to_string());

    if protocol != "s3" {
        panic!("Unsupported protocol: {protocol}")
    }

    Some((bucket, prefix))
}
