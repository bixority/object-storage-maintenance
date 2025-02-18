use aws_sdk_s3::Client;
use futures::io::AsyncRead as FuturesAsyncRead;
use futures::AsyncWrite;
use std::error::Error;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncBufRead, ReadBuf};

const BUFFER_SIZE: usize = 5 * 1024 * 1024; // 5MB

pub struct TokioBufReadAsAsyncRead<R> {
    inner: R,
}

impl<R> TokioBufReadAsAsyncRead<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(inner: R) -> Self {
        TokioBufReadAsAsyncRead { inner }
    }
}

impl<R> FuturesAsyncRead for TokioBufReadAsAsyncRead<R>
where
    R: AsyncBufRead + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        // Create a ReadBuf from the byte slice
        let mut read_buf = ReadBuf::new(buf);

        // Use poll_read to fill the ReadBuf from the inner AsyncBufRead
        let poll_result = std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut read_buf);

        // Return the result, mapping it to the number of bytes filled in the buffer
        poll_result.map(|result| result.map(|_| read_buf.filled().len()))
    }
}

pub struct MultipartUploadSink {
    buffer: Vec<u8>,
    client: Arc<Client>,
    bucket: String,
    key: String,
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
        })
    }
}

impl AsyncWrite for MultipartUploadSink {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        self.buffer.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context,
    ) -> std::task::Poll<Result<(), io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context,
    ) -> std::task::Poll<Result<(), io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
