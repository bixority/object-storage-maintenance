use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::Client;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use aws_sdk_s3::primitives::ByteStream;
use tokio::io::AsyncWrite;
use tokio::runtime::Handle;

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

    async fn init_multipart_upload(&mut self) -> Result<(), io::Error> {
        let handle = Handle::current();
        let create_multipart_upload_response = handle.block_on(
            self.client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
        ).unwrap();

        self.upload_id = create_multipart_upload_response
            .upload_id()
            .map(ToString::to_string);

        if self.upload_id.is_none() {
            panic!("Something went wrong when creating multipart upload.");
        }

        Ok(())
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
                    .set_parts(Some(self.completed_parts.to_owned()))
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
            .body(self.buffer.to_owned().into())
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

        if self.buffer.len() >= BUFFER_SIZE {
            let handle = Handle::current();

            if self.part_number == 0 {
                handle
                    .block_on(self.init_multipart_upload())
                    .expect("Failed to init multipart upload");
            }

            let chunk: Vec<u8> = self.buffer.drain(..BUFFER_SIZE).collect();
            let body = ByteStream::from(chunk);
            self.part_number += 1;

            if let Some(upload_id) = &self.upload_id {
                let _upload_part_response = handle.block_on(
                    self.client
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(&self.key)
                        .upload_id(upload_id)
                        .part_number(self.part_number)
                        .body(body)
                        .send()
                );
            }
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), io::Error>> {
        let result = Handle::current().block_on(self.complete_upload());

        Poll::Ready(result)
    }
}
