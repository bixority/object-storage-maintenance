use crate::error::Result;
use async_compression::Level;
use async_compression::tokio::write::XzEncoder;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_tar::{Builder, Header};

async fn compress_object(
    stream: futures::stream::BoxStream<'static, object_store::Result<Bytes>>,
    size: u64,
    last_modified: DateTime<Utc>,
    location: Path,
    tar_builder: &mut Builder<XzEncoder<BufWriter>>,
) -> Result<()> {
    let mut header = Header::new_gnu();
    header.set_size(size);
    header.set_mode(0o644);
    header.set_mtime(last_modified.timestamp().cast_unsigned());
    header.set_cksum();

    // Adapt the stream to AsyncRead
    let async_read = tokio_util::io::StreamReader::new(stream);

    println!("Archiving {}", location.as_ref());

    tar_builder
        .append_data(&mut header, location.as_ref(), async_read)
        .await
        .map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("Failed to append data for object '{}': {e}", location.as_ref()),
            )
        })?;

    Ok(())
}

async fn process_objects(
    store: &dyn ObjectStore,
    prefix: Path,
    cutoff_dt: DateTime<Utc>,
    tar_builder: &mut Builder<XzEncoder<BufWriter>>,
    processed_keys: &mut Vec<Path>,
) -> Result<()> {
    let mut list_stream = store.list(Some(&prefix));

    while let Some(meta_res) = list_stream.next().await {
        match meta_res {
            Ok(meta) if meta.last_modified < cutoff_dt => {
                let result = store.get(&meta.location).await?;
                compress_object(
                    result.into_stream(),
                    meta.size,
                    meta.last_modified,
                    meta.location.clone(),
                    tar_builder,
                )
                .await?;

                processed_keys.push(meta.location);
            }
            Ok(_) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn compress(
    src_store: &dyn ObjectStore,
    src_path: Path,
    dst_store: Arc<dyn ObjectStore>,
    dst_path: Path,
    cutoff_dt: DateTime<Utc>,
    buffer_size: usize,
    level: Level,
    processed_keys: &mut Vec<Path>,
) -> Result<()> {
    let sink = BufWriter::with_capacity(dst_store, dst_path, buffer_size);
    let encoder = XzEncoder::with_quality(sink, level);
    let mut tar_builder = Builder::new(encoder);

    process_objects(
        src_store,
        src_path,
        cutoff_dt,
        &mut tar_builder,
        processed_keys,
    )
    .await?;

    tar_builder.finish().await?;
    let mut encoder = tar_builder.into_inner().await?;

    encoder.shutdown().await?;

    Ok(())
}

#[cfg(test)]
mod tests;
