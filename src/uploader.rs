use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::Client;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::io;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

enum UploadState {
    Idle,
    InitiatingUpload(BoxFuture<'static, Result<String, io::Error>>),
    UploadingPart(BoxFuture<'static, Result<CompletedPart, io::Error>>),
    CompletingUpload(BoxFuture<'static, Result<(), io::Error>>),
    Completed,
    Failed(io::Error),
}

pub struct MultipartUploadSink {
    buffer: Vec<u8>,
    buffer_size: usize,
    client: Arc<Client>,
    bucket: String,
    key: String,
    multipart_upload_id: Option<String>,
    current_part_number: i32,
    completed_parts: Vec<CompletedPart>,
    state: UploadState,
}

impl MultipartUploadSink {
    pub fn new(client: Arc<Client>, bucket: String, key: String, buffer_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(buffer_size),
            buffer_size,
            client,
            bucket,
            key,
            multipart_upload_id: None,
            current_part_number: 0,
            completed_parts: Vec::new(),
            state: UploadState::Idle,
        }
    }

    fn start_multipart_upload(&mut self) {
        if matches!(self.state, UploadState::Idle) {
            println!("Starting multipart upload.");

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
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                let result = create_response
                    .upload_id()
                    .map(ToString::to_string)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No upload ID received"));

                println!("Multipart upload initiation future ended");

                result
            }
            .boxed();

            self.state = UploadState::InitiatingUpload(future);
        }
    }

    fn start_part_upload(&mut self, force: bool) {
        // If we don't have an upload_id yet, we need to start a multipart upload
        if self.multipart_upload_id.is_none() {
            println!("No multipart upload started yet.");
            self.start_multipart_upload();

            return;
        }

        if self.buffer.len() < self.buffer_size && !force {
            println!("Not enough data to upload a part yet.");

            return;
        }

        println!("Starting part upload");

        let upload_size = self.buffer.len().min(self.buffer_size);
        let chunk = self.buffer.drain(..upload_size).collect::<Vec<u8>>();
        let body = ByteStream::from(chunk);

        let part_number = self.current_part_number + 1;
        let client = Arc::clone(&self.client);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.multipart_upload_id.clone().unwrap();

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

            println!("Part {part_number} upload future ended");

            Ok(CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part_number)
                .build())
        }
        .boxed();

        println!("Started part {part_number} upload.");

        self.current_part_number = part_number;
        self.state = UploadState::UploadingPart(future);
    }

    fn start_complete_upload(&mut self) {
        println!("Completing multipart upload.");

        if self.multipart_upload_id.is_none() {
            println!("There is no multipart_upload_id, returning ok");

            return;
        }

        if !self.buffer.is_empty() {
            println!("There is still something in a buffer, initiating part upload.");

            self.start_part_upload(true);

            return;
        }

        if let Some(upload_id) = self.multipart_upload_id.take() {
            let client = Arc::clone(&self.client);
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let completed_parts = mem::take(&mut self.completed_parts);

            let future = async move {
                println!("Starting multipart upload completion future.");

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

                println!("Multipart upload completion future ended");

                Ok(())
            }
            .boxed();

            self.state = UploadState::CompletingUpload(future);

            println!("Multipart upload completion initiated.");
        } else {
            self.state = UploadState::Completed;

            println!("Completed multipart upload.");
        }
    }

    fn poll_state(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // println!("poll_state: current state = {:?}", self.state);

        loop {
            match &mut self.state {
                UploadState::Idle => {
                    if self.buffer.len() >= self.buffer_size {
                        self.start_part_upload(false);
                    }
                    return Poll::Ready(Ok(()));
                }
                UploadState::InitiatingUpload(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(upload_id)) => {
                        self.multipart_upload_id = Some(upload_id);
                        self.state = UploadState::Idle;
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = UploadState::Failed(e);
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to init multipart upload",
                        )));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                UploadState::UploadingPart(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(completed_part)) => {
                        self.completed_parts.push(completed_part);
                        self.state = UploadState::Idle;
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = UploadState::Failed(e);
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to upload part",
                        )));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                UploadState::CompletingUpload(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        println!("Multipart upload completed successfully!");

                        self.state = UploadState::Completed;

                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = UploadState::Failed(e);

                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to complete upload",
                        )));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                UploadState::Completed => return Poll::Ready(Ok(())),
                UploadState::Failed(e) => {
                    eprintln!("poll_state() is in failed state: {e}");

                    return Poll::Ready(Err(io::Error::new(e.kind(), e.to_string())));
                }
            }
        }
    }
}

impl AsyncWrite for MultipartUploadSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // println!("poll_write()");

        match self.poll_state(cx) {
            Poll::Ready(Ok(())) => {
                self.buffer.extend_from_slice(buf);

                if self.buffer.len() >= self.buffer_size {
                    self.start_part_upload(false);
                }

                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(e)) => {
                eprintln!("poll_write() received an error from poll_state(): {e}");

                Poll::Ready(Err(e))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // println!("poll_flush()");

        if !self.buffer.is_empty() && self.multipart_upload_id.is_some() {
            if let UploadState::Idle = self.state {
                println!("Upload state is idle, initiating part upload");

                self.start_part_upload(false);
            } else {
                println!("Upload state is not idle, skipping part upload");
            }
        } else {
            println!("Buffer is empty or multipart upload is not started, skipping part upload");
        }

        self.poll_state(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // println!("poll_shutdown()");

        match self.poll_state(cx) {
            Poll::Ready(Ok(())) => {
                println!("Upload state is ready, initiating multipart upload completion.");

                self.start_complete_upload();

                self.poll_state(cx)
            }
            Poll::Ready(Err(e)) => {
                eprintln!("Upload state is failed ({e}), skipping multipart upload completion.");

                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                println!("Upload state is pending, skipping multipart upload completion.");

                Poll::Pending
            }
        }
    }
}
