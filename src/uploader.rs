use aws_sdk_s3::Client;
use futures::io::AsyncRead as FuturesAsyncRead;
use futures::AsyncWrite;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        // Create a ReadBuf from the byte slice
        let mut read_buf = ReadBuf::new(buf);

        // Use poll_read to fill the ReadBuf from the inner AsyncBufRead
        let poll_result = Pin::new(&mut self.inner).poll_read(cx, &mut read_buf);

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
        mut self: Pin<&mut Self>,
        _: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _: &mut Context,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}
