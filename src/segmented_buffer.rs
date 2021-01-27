use std::future::Future;
use std::io::Write;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_buf_pool::{Pool, Reusable};
use bytes::buf::Buf;
use bytes::buf::BufMutExt;
use bytes::{Bytes, BytesMut};

use futures::AsyncWrite;
use pin_project::pin_project;

use smallvec::SmallVec;
use thiserror::Error;

const DEFAULT_SEGMENT_SIZE: usize = 1024 * 16; // 16 KB

pub(crate) type AllocBytesMutFn = Arc<dyn Fn() -> BytesMut + std::marker::Send + std::marker::Sync>;

type BufFut = Pin<Box<dyn Future<Output = Option<Reusable<BytesMut>>> + std::marker::Send>>;

// TODO: expose size when const generics become available
#[derive(PartialEq)]
pub struct SegmentedBuf<T> {
    bufs: SmallVec<[T; 4]>,
    pos: usize,
    offset: usize,
    read_pos: usize,
    read_offset: usize,
    segment_size: usize,
}

impl<T> SegmentedBuf<T> {
    pub fn new() -> Self {
        Self {
            bufs: SmallVec::new(),
            pos: 0,
            offset: 0,
            read_pos: 0,
            read_offset: 0,
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }

    pub fn with_segment_size(segment_size: usize) -> Self {
        Self {
            bufs: SmallVec::new(),
            pos: 0,
            offset: 0,
            read_pos: 0,
            read_offset: 0,
            segment_size,
        }
    }

    pub fn attach(&mut self, buf: T) {
        self.bufs.push(buf)
    }

    pub fn reset_read(&mut self) {
        self.read_pos = 0;
        self.read_offset = 0;
    }
}

impl SegmentedBuf<Reusable<BytesMut>> {
    pub fn len(&self) -> usize {
        let mut pos = self.pos;
        let mut rem = self.bufs[pos].len() - self.offset;
        pos += 1;

        while pos < self.bufs.len() {
            rem += self.bufs[pos].len();
            pos += 1;
        }

        rem
    }
}

impl<T> Default for SegmentedBuf<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl Buf for SegmentedBuf<Reusable<BytesMut>> {
    fn remaining(&self) -> usize {
        /*
        Implementations of remaining should ensure that the return value
        does not change unless a call is made to advance or any other
        function that is documented to change the Buf's current position.
         */

        let mut pos = self.read_pos;
        let mut rem = self.bufs[pos].len() - self.read_offset;
        pos += 1;

        while pos < self.bufs.len() {
            rem += self.bufs[pos].len();
            pos += 1;
        }

        rem
    }

    fn bytes(&self) -> &[u8] {
        /*
        This function should never panic. Once the end of the buffer is
        reached, i.e., Buf::remaining returns 0, calls to bytes should
        return an empty slice.
         */

        let end = self.bufs[self.read_pos].len();
        self.bufs[self.read_pos][self.read_offset..end].as_ref()
    }

    fn advance(&mut self, cnt: usize) {
        /*
        It is recommended for implementations of advance to panic
        if cnt > self.remaining(). If the implementation does not panic,
        the call must behave as if cnt == self.remaining().

        A call with cnt == 0 should never panic and be a no-op.
         */

        if cnt > self.remaining() {
            panic!("cnt is larger than the remaining bytes")
        }

        if cnt == 0 {
            return;
        };

        let mut rem = cnt;

        while rem > 0 {
            let avail = self.bufs[self.read_pos].len() - self.read_offset;
            if avail >= rem {
                self.read_offset += rem;
                rem = 0;
            } else {
                self.read_pos += 1;
                self.read_offset = 0;
                rem -= avail
            }
        }
    }
}

impl std::io::Write for SegmentedBuf<Reusable<BytesMut>> {
    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let mut total_written = 0;
        loop {
            if !self.bufs.is_empty() {
                let mut target_buf = self.bufs[self.pos]
                    .deref_mut()
                    .limit(self.segment_size)
                    .writer();
                let written = std::io::Write::write(&mut target_buf, buf)?;

                total_written += written;
                if total_written < buf.len() {
                    self.pos += 1;
                    self.offset = 0;
                    if self.pos + 1 > self.bufs.len() {
                        break Ok(total_written);
                    }
                } else {
                    self.offset += written;
                    break Ok(total_written);
                }
            } else {
                break Ok(total_written);
            }
        }
    }
}

impl futures::io::AsyncRead for SegmentedBuf<Reusable<BytesMut>> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<futures::io::Result<usize>> {
        let mut total_written = 0;
        while total_written < buf.len() {
            let written: usize = buf.write(self.bytes())?;
            self.deref_mut().advance(written);
            total_written += written;
        }
        Poll::Ready(Ok(total_written))
    }
}

impl futures::io::AsyncBufRead for SegmentedBuf<Reusable<BytesMut>> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<&[u8], futures::io::Error>> {
        let this = self.get_mut();
        let end = this.bufs[this.read_pos].len();
        let b = this.bufs[this.pos][this.read_offset..end].as_ref();

        Poll::Ready(Ok(b))
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.deref_mut().advance(amt)
    }
}

#[pin_project]
pub struct SegmentedPoolBuf<Fut, T, Fi> {
    #[pin]
    pool: Pool<Fi, T>,
    #[pin]
    pub buf: SegmentedBuf<Reusable<T>>,
    #[pin]
    buf_fut: Option<Fut>,
    total_written: Option<usize>,
    pool_buf_max_size: Option<usize>,
}

#[derive(Debug, Error)]
pub enum SegmentedPoolBufError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("Buffer is Full")]
    BufferFull(),
}

impl From<SegmentedPoolBufError> for std::io::Error {
    fn from(err: SegmentedPoolBufError) -> std::io::Error {
        match err {
            SegmentedPoolBufError::Io(e) => e,
            e => std::io::Error::new(std::io::ErrorKind::Other, Box::new(e)),
        }
    }
}

impl<F, T, Fi> SegmentedPoolBuf<F, T, Arc<Fi>>
where
    T: std::marker::Send,
    Fi: Fn() -> T + std::marker::Send + std::marker::Sync + 'static + ?Sized,
{
    pub fn iter(&self) -> SegmentedPoolBufIter<F, T, Arc<Fi>> {
        SegmentedPoolBufIter {
            pool: self,
            buf: 0,
            offset: 0,
        }
    }

    pub fn reset_read(&mut self) {
        self.buf.reset_read()
    }
}

impl<F> SegmentedPoolBuf<F, BytesMut, AllocBytesMutFn> {
    pub fn into_bytes_stream(self) -> SegmentedBufBytes {
        SegmentedBufBytes {
            bufs: self
                .buf
                .bufs
                .into_iter()
                .map(move |mut b| Some(b.split().freeze()))
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

impl<F> Buf for SegmentedPoolBuf<F, BytesMut, AllocBytesMutFn> {
    fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    fn bytes(&self) -> &[u8] {
        self.buf.bytes()
    }
    fn advance(&mut self, cnt: usize) {
        self.buf.advance(cnt)
    }
}

impl<F> std::io::Write for SegmentedPoolBuf<F, BytesMut, AllocBytesMutFn> {
    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let mut total_written = 0;
        loop {
            // TODO: debug
            let written = self.buf.write(&buf[total_written..])?;
            total_written += written;
            if total_written == buf.len() {
                break Ok(total_written);
            } else {
                loop {
                    match self.pool.try_pull() {
                        Ok(mut new_buf) => {
                            // clear the BytesMut
                            new_buf.clear();
                            // reset BytesMut to full size
                            let cap = new_buf.capacity();
                            if cap < self.buf.segment_size {
                                new_buf.reserve(self.buf.segment_size - cap);

                                if let Some(max_size) = self.pool_buf_max_size {
                                    // we're encountering bufs that are still in use, relieve the pressure a bit
                                    if self.buf.bufs.len() * self.buf.segment_size
                                        + self.buf.segment_size
                                        > max_size
                                    {
                                        return Err(SegmentedPoolBufError::BufferFull {}.into());
                                    }
                                };
                                self.pool.expand().unwrap();
                            };
                            self.buf.attach(new_buf);
                            break;
                        }
                        Err(_) => {
                            if let Some(max_size) = self.pool_buf_max_size {
                                if self.buf.bufs.len() * self.buf.segment_size
                                    + self.buf.segment_size
                                    > max_size
                                {
                                    return Err(SegmentedPoolBufError::BufferFull {}.into());
                                }
                            };
                            self.pool.expand().unwrap();
                        }
                    }
                }
            }
        }
    }
}

impl AsyncWrite
    for SegmentedPoolBuf<
        Pin<Box<dyn Future<Output = Option<Reusable<BytesMut>>> + std::marker::Send>>,
        BytesMut,
        AllocBytesMutFn,
    >
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let mut this = self.project();
        Poll::Ready(loop {
            let buf_fut = this.buf_fut.as_mut().as_pin_mut();
            match buf_fut {
                Some(mut fut) => {
                    let b = fut.as_mut().poll(cx);
                    match b {
                        Poll::Ready(Some(new_buf)) => {
                            this.buf_fut.set(None);
                            this.buf.attach(new_buf);
                        }
                        Poll::Ready(None) => {
                            unreachable!();
                        }
                        Poll::Pending => {
                            // allocate
                            // TODO add a soft limit:
                            //
                            this.pool.expand().unwrap(); //?
                            return Poll::Pending;
                        }
                    }
                }
                None => {
                    let mut total_written = this.total_written.unwrap_or(0);
                    let written = this.buf.write(&buf[total_written..])?;
                    total_written += written;
                    if total_written == buf.len() {
                        *this.total_written = None;
                        break Ok(total_written);
                    } else {
                        if let Some(max_size) = this.pool_buf_max_size {
                            if this.buf.bufs.len() * this.buf.segment_size + this.buf.segment_size
                                > *max_size
                            {
                                return Poll::Ready(Err(
                                    SegmentedPoolBufError::BufferFull {}.into()
                                ));
                            }
                        };

                        let pool = this.pool.clone();
                        let segment_size = this.buf.segment_size;

                        this.buf_fut.set(Some(Box::pin(async move {
                            pool.pull().await.map(move |mut new_buf| {
                                // clear the BytesMut
                                new_buf.clear();
                                // reset BytesMut to full size

                                let cap = new_buf.capacity();
                                if cap < segment_size {
                                    new_buf.reserve(segment_size - cap);
                                };
                                new_buf
                            })
                        })));
                    }
                    *this.total_written = Some(total_written)
                }
            }
        })
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(std::io::Write::flush(&mut self.buf))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        self.poll_flush(cx)
    }
}

pub struct SegmentedPoolBufBuilder {
    initial_capacity: Option<usize>,
    segment_size: Option<usize>,
    max_size: Option<usize>,
}

impl SegmentedPoolBufBuilder {
    pub fn new() -> Self {
        Self {
            initial_capacity: None,
            segment_size: None,
            max_size: None,
        }
    }

    pub fn segment_size(mut self, segment_size: usize) -> Self {
        self.segment_size = Some(segment_size);
        self
    }

    pub fn initial_capacity(mut self, initial_capacity: usize) -> Self {
        self.initial_capacity = Some(initial_capacity);
        self
    }

    /// Set the maximum size of the buffer, useful to implement backpressure on buffer consumers
    #[allow(dead_code)]
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    pub fn build(self) -> SegmentedPoolBuf<BufFut, BytesMut, AllocBytesMutFn> {
        let segment_size = self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);
        let pool = Pool::<
            Arc<dyn Fn() -> BytesMut + std::marker::Send + std::marker::Sync>,
            BytesMut,
        >::new(
            self.initial_capacity.unwrap_or(DEFAULT_SEGMENT_SIZE) / segment_size + 1,
            Arc::new(move || BytesMut::with_capacity(segment_size)),
        );
        self.with_pool(pool)
    }

    pub fn with_pool(
        self,
        pool: Pool<AllocBytesMutFn, BytesMut>,
    ) -> SegmentedPoolBuf<BufFut, BytesMut, AllocBytesMutFn> {
        let segment_size = self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);
        let buf = SegmentedBuf::with_segment_size(segment_size);
        SegmentedPoolBuf {
            pool,
            buf,
            buf_fut: None,
            total_written: None,
            pool_buf_max_size: self.max_size,
        }
    }
}

impl Default for SegmentedPoolBufBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct SegmentedBufBytes {
    bufs: SmallVec<[Option<Bytes>; 4]>,
}

impl SegmentedBufBytes {
    pub fn stream(&self) -> SegmentedBufStream {
        SegmentedBufStream {
            buf: self.bufs.clone(),
            pos: 0,
        }
    }

    pub fn reader(&self) -> SegmentedBufBytesReader {
        SegmentedBufBytesReader {
            buf: self,
            read_pos: 0,
            read_offset: 0,
        }
    }
}
#[derive(Clone)]
pub struct SegmentedBufBytesReader<'a> {
    buf: &'a SegmentedBufBytes,
    read_pos: usize,
    read_offset: usize,
}

impl Buf for SegmentedBufBytesReader<'_> {
    fn remaining(&self) -> usize {
        let mut pos = self.read_pos;

        let mut rem = self.buf.bufs[pos].as_ref().unwrap().len() - self.read_offset;
        pos += 1;

        while pos < self.buf.bufs.len() {
            rem += self.buf.bufs[pos].as_ref().unwrap().len();
            pos += 1;
        }

        rem
    }

    fn bytes(&self) -> &[u8] {
        let end = self.buf.bufs[self.read_pos].as_ref().unwrap().len();
        self.buf.bufs[self.read_pos].as_ref().unwrap()[self.read_offset..end].as_ref()
    }

    fn advance(&mut self, cnt: usize) {
        if cnt > self.remaining() {
            panic!("cnt is larger than the remaining bytes")
        }

        if cnt == 0 {
            return;
        };

        let mut rem = cnt;

        while rem > 0 {
            let avail = self.buf.bufs[self.read_pos].as_ref().unwrap().len() - self.read_offset;
            if avail >= rem {
                self.read_offset += rem;
                rem = 0;
            } else {
                self.read_pos += 1;
                self.read_offset = 0;
                rem -= avail
            }
        }
    }
}

impl std::io::Read for SegmentedBufBytesReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_written = 0;
        while total_written < buf.len() {
            let bytes: &[u8] = bytes::Buf::bytes(self);
            let amt = std::cmp::min(buf.len(), bytes.len());
            if amt == 0 {
                break;
            }
            buf[total_written..amt].copy_from_slice(&bytes[..amt]);
            self.advance(amt);
            total_written += amt;
        }
        Ok(total_written)
    }
}

impl std::io::BufRead for SegmentedBufBytesReader<'_> {
    fn fill_buf(&mut self) -> Result<&[u8], std::io::Error> {
        let end = self.buf.bufs[self.read_pos].as_ref().unwrap().len();
        let b = self.buf.bufs[self.read_pos].as_ref().unwrap()[self.read_offset..end].as_ref();

        Ok(b)
    }

    fn consume(&mut self, amt: usize) {
        self.advance(amt)
    }
}

impl futures::io::AsyncRead for SegmentedBufBytesReader<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<futures::io::Result<usize>> {
        let mut total_written = 0;
        while total_written < buf.len() {
            let written: usize = buf.write(self.bytes())?;
            self.deref_mut().advance(written);
            total_written += written;
        }
        Poll::Ready(Ok(total_written))
    }
}

impl futures::io::AsyncBufRead for SegmentedBufBytesReader<'_> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<&[u8], futures::io::Error>> {
        let this = self.get_mut();
        let end = this.buf.bufs[this.read_pos].as_ref().unwrap().len();
        let b = this.buf.bufs[this.read_pos].as_ref().unwrap()[this.read_offset..end].as_ref();

        Poll::Ready(Ok(b))
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.deref_mut().advance(amt)
    }
}

#[derive(Clone)]
pub struct SegmentedBufStream {
    buf: SmallVec<[Option<Bytes>; 4]>,
    pos: usize,
}

impl futures::stream::Stream for SegmentedBufStream {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Bytes>> {
        if self.pos < self.buf.len() {
            let cur_pos = self.pos;
            let ret = Poll::Ready(Some(self.buf[cur_pos].take().unwrap()));
            self.pos += 1;
            ret
        } else {
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.buf.len(), Some(self.buf.len()))
    }
}
pub struct SegmentedPoolBufIter<'a, F, T, Fi>
where
    T: std::marker::Send,
{
    pool: &'a SegmentedPoolBuf<F, T, Fi>,
    buf: usize,
    offset: usize,
}

impl<'a, F, T, Fi> std::iter::Iterator for SegmentedPoolBufIter<'a, F, T, Fi>
where
    T: AsRef<[u8]> + Unpin + Send,
{
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        loop {
            if self.buf == self.pool.buf.bufs.len() {
                break None;
            } else if self.buf == self.pool.buf.bufs.len() - 1 {
                match self.offset == self.pool.buf.offset {
                    true => break None,
                    false => {
                        let ret = self.pool.buf.bufs[self.buf].as_ref()[self.offset];
                        self.offset += 1;
                        break Some(ret);
                    }
                }
            } else {
                match self.offset == self.pool.buf.bufs[self.buf].as_ref().len() {
                    true => {
                        self.offset = 0;
                        self.buf += 1;
                    }
                    false => {
                        let ret = self.pool.buf.bufs[self.buf].as_ref()[self.offset];
                        self.offset += 1;
                        break Some(ret);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio_test;

    use futures::stream::StreamExt;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    use proptest::prelude::*;

    #[cfg(test)]
    proptest! {
        #[test]
        fn write_to_segmented_bool_buf(
            inp in (0..100*1024usize)
                .prop_flat_map(|size|(Just(size),
                                      proptest::collection::vec(proptest::num::u8::ANY, size)))){
            let mut buf = SegmentedPoolBufBuilder::new().segment_size(2048).initial_capacity(8192).build();
            use std::io::Write;
            buf.write(&inp.1).unwrap();

            assert_eq!(buf.iter()
                       .zip(inp.1.iter())
                       .fold(true,
                             |acc, (a, b)|{
                                 acc && (a == *b)
                             }),
                       true);
            assert_eq!(inp.0, buf.iter().count());

        }

    }

    #[cfg(test)]
    proptest! {
        #[test]
        fn async_write_to_segmented_bool_buf(
            inp in (0..100*1024usize)
                .prop_flat_map(|size|(Just(size),
                                      proptest::collection::vec(proptest::num::u8::ANY, size)))){

            let buf = aw!(async {
                let mut buf = SegmentedPoolBufBuilder::new().segment_size(2048).initial_capacity(8192).build();

                futures::AsyncWriteExt::write(&mut buf, &inp.1).await.unwrap();
                buf
            });

            assert_eq!(buf.iter()
                       .zip(inp.1.iter())
                       .fold(true,
                             |acc, (a, b)|{
                                 acc && (a == *b)
                             }),
                       true);
            assert_eq!(inp.0, buf.iter().count());

        }

    }

    #[cfg(test)]
    proptest! {
        #[test]
        fn async_write_to_too_small_segmented_pool_buf(
            inp in (0..100*1024usize)
                .prop_flat_map(|size|(Just(size),
                                      proptest::collection::vec(proptest::num::u8::ANY, size)))){

            let mut buf = SegmentedPoolBufBuilder::new().segment_size(2048).initial_capacity(4096).max_size(8192).build();
            let res = aw!(async {
                futures::AsyncWriteExt::write(&mut buf, &inp.1).await
            });
            if inp.0 > 8192{
                assert!(res.is_err());
            } else {{
                res.unwrap();
                assert_eq!(buf.iter()
                           .zip(inp.1.iter())
                           .fold(true,
                                 |acc, (a, b)|{
                                     acc && (a == *b)
                                 }),
                           true);
                assert_eq!(inp.0, buf.iter().count());
            }}

        }

    }

    #[cfg(test)]
    proptest! {
        #[test]
        fn segmentedbufstream_stream(
            inp in (0..100*1024usize)
                .prop_flat_map(|size|(Just(size),
                                      proptest::collection::vec(proptest::num::u8::ANY, size)))){

            let mut buf = SegmentedPoolBufBuilder::new().segment_size(2048).initial_capacity(4096).build();
            let _bytes = aw!(async {
                futures::AsyncWriteExt::write(&mut buf, &inp.1).await.unwrap();

                buf.into_bytes_stream().stream().collect::<Vec<Bytes>>().await
            });
            // TODO
            /*assert_eq!(buf.iter()
                        .zip(inp.1.iter())
                        .fold(true,
                                |acc, (a, b)|{
                                    acc && (a == *b)
                                }),
                        true);
                        */

            //assert_eq!(inp.0, buf.iter().count());
        }

    }
}
