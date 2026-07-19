use super::*;
use chrono::Utc;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;

#[tokio::test]
async fn test_compress_basic() -> crate::error::Result<()> {
    let src_store = Arc::new(InMemory::new());
    let dst_store = Arc::new(InMemory::new());

    let path1 = Path::from("file1.txt");
    let path2 = Path::from("file2.txt");
    src_store.put(&path1, "content1".into()).await?;
    src_store.put(&path2, "content2".into()).await?;

    let cutoff = Utc::now();
    let mut processed_keys = Vec::new();

    compress(
        src_store.clone(),
        Path::from(""),
        dst_store.clone(),
        Path::from("archive.tar.xz"),
        cutoff,
        1024 * 1024,
        Level::Fastest,
        &mut processed_keys,
    )
    .await?;

    assert_eq!(processed_keys.len(), 2);
    assert!(processed_keys.contains(&"file1.txt".to_string()));
    assert!(processed_keys.contains(&"file2.txt".to_string()));

    let dst_list = dst_store.list(None).collect::<Vec<_>>().await;
    assert_eq!(dst_list.len(), 1);

    Ok(())
}
