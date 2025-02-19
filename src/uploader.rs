use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::Client;
use futures::executor::block_on;
use futures::AsyncWrite;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

const BUFFER_SIZE: usize = 5 * 1024 * 1024; // 5MB

pub struct MultipartUploadSink {
    buffer: Vec<u8>,
    client: Arc<Client>,
    bucket: String,
    key: String,
    upload_id: Option<String>,
    part_number: i32,
    completed_parts: Vec<CompletedPart>,
}

impl MultipartUploadSink {
    pub async fn new(
        client: Arc<Client>,
        bucket: String,
        key: String,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            buffer: Vec::with_capacity(BUFFER_SIZE),
            client,
            bucket,
            key,
            upload_id: None,
            part_number: 0,
            completed_parts: Vec::new(),
        })
    }

    async fn complete_multipart_upload(&mut self, upload_id: String) -> Result<(), io::Error> {
        if !self.buffer.is_empty() {
            // self.upload_part().await?;
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(self.completed_parts.clone()))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    async fn put_object(&mut self) -> Result<(), io::Error> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(self.buffer.clone().into())
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    async fn complete_upload(&mut self) -> Result<(), io::Error> {
        // If we have an upload_id, we're in multipart mode
        if let Some(upload_id) = self.upload_id.take() {
            // Upload any remaining data as the final part
            return self.complete_multipart_upload(upload_id).await;
        } else if !self.buffer.is_empty() {
            // No multipart upload was initiated, do a regular upload
            return self.put_object().await;
        }

        Ok(())
    }
}

impl AsyncWrite for MultipartUploadSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), io::Error>> {
        let result = block_on(self.complete_upload());
        Poll::Ready(result)
    }
}
