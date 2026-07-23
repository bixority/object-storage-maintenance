use crate::error::{AppError, Result};
use futures::StreamExt;
use object_store::{ObjectStore, path::Path};

pub async fn delete_keys(store: &dyn ObjectStore, keys: Vec<Path>) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }

    let locations = futures::stream::iter(keys.into_iter().map(Ok));
    let mut results = store.delete_stream(locations.boxed());

    let mut success_count = 0;

    while let Some(res) = results.next().await {
        res.map_err(AppError::from)?;
        success_count += 1;
    }

    if success_count > 0 {
        println!("Successfully deleted {success_count} objects.");
    }

    Ok(())
}
