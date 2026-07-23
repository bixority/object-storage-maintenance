use crate::compressor::compress;
use crate::error::{AppError, Result};
use crate::object_storage::delete_keys;
use crate::storage::get_store_and_path;
use async_compression::Level;
use chrono::{DateTime as ChronoDateTime, Duration, Utc};
use object_store::path::Path;

pub async fn archive(
    src: String,
    dst: String,
    cutoff: Option<ChronoDateTime<Utc>>,
    buffer_size: usize,
    level: Level,
) -> Result<()> {
    let (src_store, src_path) = get_store_and_path(&src)?;
    let (dst_store, dst_path) = get_store_and_path(&dst)?;

    println!("Archiving from {src} to {dst}");

    let cutoff_dt = cutoff.unwrap_or_else(|| {
        let now = Utc::now();
        now - Duration::seconds(1)
    });
    let cutoff_str = format!("{}", cutoff_dt.format("%Y%m%d_%H%M%S"));

    let dst_file_path = dst_path.join(format!("archive_{cutoff_str}.tar.xz"));

    let mut archived_keys: Vec<Path> = Vec::new();
    compress(
        src_store.as_ref(),
        src_path,
        dst_store,
        dst_file_path,
        cutoff_dt,
        buffer_size,
        level,
        &mut archived_keys,
    )
    .await
    .map_err(|e| AppError::Compression(Box::new(e)))?;

    delete_keys(src_store.as_ref(), archived_keys)
        .await
        .map_err(|e| AppError::Deletion(Box::new(e)))?;

    Ok(())
}
