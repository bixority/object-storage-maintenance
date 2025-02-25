use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use std::sync::Arc;

pub async fn delete_keys(
    client: Arc<Client>,
    bucket_name: &str,
    keys: Vec<String>,
) -> Result<(), aws_sdk_s3::Error> {
    for chunk in keys.chunks(1000) {
        let objects_to_delete: Vec<ObjectIdentifier> = chunk
            .iter()
            .filter_map(|key| match ObjectIdentifier::builder().key(key).build() {
                Ok(obj) => Some(obj),
                Err(e) => {
                    eprintln!("Failed to build ObjectIdentifier for key '{key}': {e}");

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
            Err(e) => {
                eprintln!("Error occurred while deleting objects: {e}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}
