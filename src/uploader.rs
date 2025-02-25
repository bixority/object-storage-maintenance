use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedPart;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::io;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

const BUFFER_SIZE: usize = 5 * 1024 * 1024; // 5MB

enum UploadState {
    Idle,
    InitiatingUpload(BoxFuture<'static, Result<String, io::Error>>),
    UploadingPart(BoxFuture<'static, Result<CompletedPart, io::Error>>),
    CompletingUpload(BoxFuture<'static, Result<(), io::Error>>),
    Failed(io::Error),
}

pub struct MultipartUploadSink {
    buffer: Vec<u8>,
    client: Arc<Client>,
    bucket: String,
    key: String,
    upload_id: Option<String>,
    part_number: i32,
    completed_parts: Vec<CompletedPart>,
    state: UploadState,
}

impl MultipartUploadSink {
    pub fn new(client: Arc<Client>, bucket: String, key: String) -> Self {
        Self {
            buffer: Vec::with_capacity(BUFFER_SIZE),
            client,
            bucket,
            key,
            upload_id: None,
            part_number: 0,
            completed_parts: Vec::new(),
            state: UploadState::Idle,
        }
    }

    fn start_multipart_upload(&mut self) {
        let client = Arc::clone(&self.client);
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        let future = async move {
            let create_response = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(
                    |e| io::Error::new(io::ErrorKind::Other, e.to_string())
                )?;

            create_response
                .upload_id()
                .map(ToString::to_string)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No upload ID received"))
        }
        .boxed();

        self.state = UploadState::InitiatingUpload(future);
    }

    fn start_part_upload(&mut self) {
        if self.buffer.len() < BUFFER_SIZE && self.upload_id.is_some() {
            // Not enough data to upload a part yet
            return;
        }

        // If we don't have an upload_id yet, we need to start a multipart upload
        if self.upload_id.is_none() {
            self.start_multipart_upload();
            return;
        }

        let upload_size = self.buffer.len().min(BUFFER_SIZE);
        let chunk: Vec<u8> = self.buffer.drain(..upload_size).collect();
        let body = ByteStream::from(chunk);

        let part_number = self.part_number + 1;
        let client = Arc::clone(&self.client);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone().unwrap();

        let future = async move {
            let upload_response = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            let e_tag = upload_response
                .e_tag()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, "No ETag in upload part response")
                })?
                .to_string();

            Ok(CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part_number)
                .build())
        }
        .boxed();

        self.part_number = part_number;
        self.state = UploadState::UploadingPart(future);
    }

    fn start_complete_upload(&mut self) -> Poll<Result<(), io::Error>> {
        // Handle any remaining data
        if !self.buffer.is_empty() && self.upload_id.is_some() {
            self.start_part_upload();
            return Poll::Pending;
        }

        // If we have an upload_id, complete the multipart upload
        if let Some(upload_id) = self.upload_id.take() {
            let client = Arc::clone(&self.client);
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let completed_parts = mem::take(&mut self.completed_parts);

            let future = async move {
                client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .multipart_upload(
                        aws_sdk_s3::types::CompletedMultipartUpload::builder()
                            .set_parts(Some(completed_parts))
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                Ok(())
            }
            .boxed();

            self.state = UploadState::CompletingUpload(future);
            Poll::Pending
        } else if !self.buffer.is_empty() {
            // Direct upload for small files
            let client = Arc::clone(&self.client);
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let data = mem::take(&mut self.buffer);

            let future = async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(data.into())
                    .send()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                Ok(())
            }
            .boxed();

            self.state = UploadState::CompletingUpload(future);
            Poll::Pending
        } else {
            // Nothing to upload
            Poll::Ready(Ok(()))
        }
    }

    fn poll_state(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match &mut self.state {
            UploadState::Idle => Poll::Ready(Ok(())),

            UploadState::InitiatingUpload(future) => {
                match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(upload_id)) => {
                        self.upload_id = Some(upload_id);
                        self.state = UploadState::Idle;

                        // Now that we have an upload ID, try to upload a part
                        if self.buffer.len() >= BUFFER_SIZE {
                            self.start_part_upload();
                            self.poll_state(cx)
                        } else {
                            Poll::Ready(Ok(()))
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = UploadState::Failed(e);
                        Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to init multipart upload",
                        )))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }

            UploadState::UploadingPart(future) => {
                match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(completed_part)) => {
                        self.completed_parts.push(completed_part);
                        self.state = UploadState::Idle;

                        // Check if we need to upload more parts
                        if self.buffer.len() >= BUFFER_SIZE {
                            self.start_part_upload();
                            self.poll_state(cx)
                        } else {
                            Poll::Ready(Ok(()))
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = UploadState::Failed(e);
                        Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to upload part",
                        )))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }

            UploadState::CompletingUpload(future) => match future.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    self.state = UploadState::Idle;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    self.state = UploadState::Failed(e);
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to complete upload",
                    )))
                }
                Poll::Pending => Poll::Pending,
            },

            UploadState::Failed(e) => Poll::Ready(Err(io::Error::new(e.kind(), e.to_string()))),
        }
    }
}

impl AsyncWrite for MultipartUploadSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // Processing any ongoing operations
        match self.poll_state(cx) {
            Poll::Ready(Ok(())) => {
                // Add the new data to the buffer
                self.buffer.extend_from_slice(buf);

                // If there is enough accumulated data, start uploading a part
                if self.buffer.len() >= BUFFER_SIZE {
                    self.start_part_upload();
                    // Then poll the new state
                    match self.poll_state(cx) {
                        Poll::Ready(Ok(())) => Poll::Ready(Ok(buf.len())),
                        Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                        Poll::Pending => Poll::Ready(Ok(buf.len())), // Still accept the write even if part upload is pending
                    }
                } else {
                    Poll::Ready(Ok(buf.len()))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                // We're in the middle of an operation, but we can still accept the write
                // This is a bit of a compromise - ideally we'd apply backpressure, but for simplicity we'll
                // accept the write and buffer it
                self.buffer.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // If we have data and an upload ID, start a part upload
        if !self.buffer.is_empty() && self.upload_id.is_some() {
            match self.state {
                UploadState::Idle => {
                    self.start_part_upload();
                },
                _ => {}
            }
        }

        // Then poll the state machine until it's done with current operations
        self.poll_state(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // First handle any ongoing operations
        match self.poll_state(cx) {
            Poll::Ready(Ok(())) => {
                // Now start the complete upload process
                self.start_complete_upload()
            }
            other => other,
        }
    }
}
