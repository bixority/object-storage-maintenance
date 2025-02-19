use futures::io::AsyncRead as FuturesAsyncRead;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufRead, ReadBuf};

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
