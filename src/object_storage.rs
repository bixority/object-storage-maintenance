use crate::error::Result;
use futures::StreamExt;
use object_store::{ObjectStore, path::Path};

pub async fn delete_keys(store: &dyn ObjectStore, keys: Vec<Path>) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }

    let locations = futures::stream::iter(keys.into_iter().map(Ok));
    let mut results = store.delete_stream(locations.boxed());

    let mut success_count = 0;
    let mut error_count = 0;

    while let Some(res) = results.next().await {
        match res {
            Ok(_) => success_count += 1,
            Err(e) => {
                eprintln!("Failed to delete object: {e}");
                error_count += 1;
            }
        }
    }

    if success_count > 0 {
        println!("Successfully deleted {success_count} objects.");
    }
    if error_count > 0 {
        eprintln!("Failed to delete {error_count} objects.");
    }

    Ok(())
}
