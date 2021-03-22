use std::future::Future;
use std::io::Write;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_buf_pool::{ClearBuf, Pool, Reusable};
use bytes::buf::Buf;
use bytes::buf::BufMut;
use bytes::buf::Limit;
use bytes::BytesMut;

use futures::AsyncWrite;
use pin_project::pin_project;

use smallvec::SmallVec;
use thiserror::Error;

const DEFAULT_SEGMENT_SIZE: usize = 1024 * 16; // 16 KB
const SERIALIZATION_BUF_RESERVE_SEGMENTS: usize = 100;

pub(crate) type AllocBufferFn = Arc<dyn Fn() -> Buffer + std::marker::Send + std::marker::Sync>;

pub(crate) type BufFut =
    Pin<Box<dyn Future<Output = Option<Reusable<Buffer>>> + std::marker::Send + std::marker::Sync>>;

pub struct Buffer {
    pub(crate) buf: BytesMut,
    _c: countme::Count<Self>,
}

impl Buffer {
    pub fn new(bm: BytesMut) -> Self {
        Buffer {
            buf: bm,
            _c: countme::Count::new(),
        }
    }
}

impl Buffer {
    fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn inner(&self) -> &[u8] {
        &self.buf
    }

    fn limit(&mut self, limit: usize) -> Limit<&mut BytesMut> {
        (&mut self.buf).limit(limit)
    }
}

impl ClearBuf for Buffer {
    fn clear(&mut self) {
        self.buf.clear()
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.inner()
    }
}

impl Buf for Buffer {
    fn remaining(&self) -> usize {
        /*
        Implementations of remaining should ensure that the return value
        does not change unless a call is made to advance or any other
        function that is documented to change the Buf's current position.
         */
        self.buf.remaining()
    }

    fn chunk(&self) -> &[u8] {
        /*
        This function should never panic. Once the end of the buffer is
        reached, i.e., Buf::remaining returns 0, calls to bytes should
        return an empty slice.
         */
        self.buf.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        /*
        It is recommended for implementations of advance to panic
        if cnt > self.remaining(). If the implementation does not panic,
        the call must behave as if cnt == self.remaining().

        A call with cnt == 0 should never panic and be a no-op.
         */

        self.buf.advance(cnt)
    }
}

// TODO: expose size when const generics become available
#[derive(PartialEq)]
pub struct SegmentedBuf<T> {
    _c: countme::Count<Self>,
    pub(crate) bufs: SmallVec<[T; 4]>,
    pos: usize,
    offset: usize,
    read_pos: usize,
    read_offset: usize,
    segment_size: usize,
}

impl<T> SegmentedBuf<T> {
    pub fn new() -> Self {
        Self {
            _c: countme::Count::new(),
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
            _c: countme::Count::new(),
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

impl SegmentedBuf<Reusable<Buffer>> {
    pub fn len(&self) -> usize {
        let mut pos = 0;
        let mut rem = 0;
        // Count the full buffers
        while pos < self.pos {
            rem += self.bufs[pos].len();
            pos += 1;
        }
        // Add on the last, partial buffer
        rem += self.offset;
        rem
    }

    pub fn reader(&self) -> SegmentedBufBytesReader {
        SegmentedBufBytesReader {
            buf: &self.bufs,
            read_pos: 0,
            read_offset: 0,
        }
    }
}

impl<T> Default for SegmentedBuf<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl Buf for SegmentedBuf<Reusable<Buffer>> {
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

    fn chunk(&self) -> &[u8] {
        /*
        This function should never panic. Once the end of the buffer is
        reached, i.e., Buf::remaining returns 0, calls to bytes should
        return an empty slice.
         */

        let end = self.bufs[self.read_pos].len();
        self.bufs[self.read_pos].inner()[self.read_offset..end].as_ref()
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

impl std::io::Write for SegmentedBuf<Reusable<Buffer>> {
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

impl futures::io::AsyncRead for SegmentedBuf<Reusable<Buffer>> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<futures::io::Result<usize>> {
        let mut total_written = 0;
        while total_written < buf.len() {
            let written: usize = buf.write(self.chunk())?;
            self.deref_mut().advance(written);
            total_written += written;
        }
        Poll::Ready(Ok(total_written))
    }
}

impl futures::io::AsyncBufRead for SegmentedBuf<Reusable<Buffer>> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<&[u8], futures::io::Error>> {
        let this = self.get_mut();
        let end = this.bufs[this.read_pos].len();
        let b = this.bufs[this.pos].inner()[this.read_offset..end].as_ref();

        Poll::Ready(Ok(b))
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.deref_mut().advance(amt)
    }
}

#[pin_project]
pub struct SegmentedPoolBuf<Fut, T, Fi>
where
    T: ClearBuf,
{
    #[pin]
    pub(crate) pool: Pool<Fi, T>,
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
    T: std::marker::Send + ClearBuf,
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

impl<F> SegmentedPoolBuf<F, Buffer, AllocBufferFn> {
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    fn duplicate(&self) -> Self {
        let buf = SegmentedBuf::with_segment_size(self.buf.segment_size);
        Self {
            pool: self.pool.clone(),
            buf,
            buf_fut: None,
            total_written: None,
            pool_buf_max_size: self.pool_buf_max_size,
        }
    }
}

impl<F> Clone for SegmentedPoolBuf<F, Buffer, AllocBufferFn> {
    fn clone(&self) -> Self {
        let mut reader = (&self.buf).reader();
        let mut ret = self.duplicate();
        std::io::copy(&mut reader, &mut ret).unwrap();
        ret
    }
}

impl<F> Buf for SegmentedPoolBuf<F, Buffer, AllocBufferFn> {
    fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.buf.chunk()
    }
    fn advance(&mut self, cnt: usize) {
        self.buf.advance(cnt)
    }
}

impl<F> std::io::Write for SegmentedPoolBuf<F, Buffer, AllocBufferFn> {
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
                        Ok(new_buf) => {
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

impl AsyncWrite for SegmentedPoolBuf<BufFut, Buffer, AllocBufferFn> {
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

                        this.buf_fut
                            .set(Some(Box::pin(async move { pool.pull().await })));
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
    pub fn max_capacity(mut self, max_size: Option<usize>) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn build(self) -> SegmentedPoolBuf<BufFut, Buffer, AllocBufferFn> {
        let segment_size = self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);
        let pool =
            Pool::<Arc<dyn Fn() -> Buffer + std::marker::Send + std::marker::Sync>, Buffer>::with_max_reserve(
                self.initial_capacity.unwrap_or(DEFAULT_SEGMENT_SIZE) / segment_size + 1,
                SERIALIZATION_BUF_RESERVE_SEGMENTS,
                Arc::new(move || Buffer::new(BytesMut::with_capacity(segment_size))),
            ).unwrap();
        self.with_pool(pool)
    }

    pub fn with_pool(
        self,
        pool: Pool<AllocBufferFn, Buffer>,
    ) -> SegmentedPoolBuf<BufFut, Buffer, AllocBufferFn> {
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
pub struct SegmentedBufBytesReader<'a> {
    buf: &'a SmallVec<[Reusable<Buffer>; 4]>,
    read_pos: usize,
    read_offset: usize,
}

impl Buf for SegmentedBufBytesReader<'_> {
    fn remaining(&self) -> usize {
        let mut pos = self.read_pos;

        let mut rem = self.buf[pos].len() - self.read_offset;
        pos += 1;

        while pos < self.buf.len() {
            rem += self.buf[pos].len();
            pos += 1;
        }

        rem
    }

    fn chunk(&self) -> &[u8] {
        let end = self.buf[self.read_pos].len();
        self.buf[self.read_pos].inner()[self.read_offset..end].as_ref()
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
            let avail = self.buf[self.read_pos].len() - self.read_offset;
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
            let bytes: &[u8] = bytes::Buf::chunk(self);
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
        let end = self.buf[self.read_pos].len();
        let b = self.buf[self.read_pos].inner()[self.read_offset..end].as_ref();

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
            let written: usize = buf.write(self.chunk())?;
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
        let end = this.buf[this.read_pos].len();
        let b = this.buf[this.read_pos].inner()[this.read_offset..end].as_ref();

        Poll::Ready(Ok(b))
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.deref_mut().advance(amt)
    }
}

pub struct SegmentedPoolBufIter<'a, F, T, Fi>
where
    T: std::marker::Send + ClearBuf,
{
    pool: &'a SegmentedPoolBuf<F, T, Fi>,
    buf: usize,
    offset: usize,
}

impl<'a, F, T, Fi> std::iter::Iterator for SegmentedPoolBufIter<'a, F, T, Fi>
where
    T: AsRef<[u8]> + ClearBuf + Unpin + Send,
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
    use serial_test::serial;

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
                                      proptest::collection::vec(proptest::num::u8::ANY, size)))) {

            let mut buf = SegmentedPoolBufBuilder::new().segment_size(2048).initial_capacity(8192).build();

            use std::io::Write;
            buf.write_all(&inp.1).unwrap();

            assert_eq!(buf.len(), inp.0);
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

            assert_eq!(buf.len(), inp.0);
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

            let mut buf = SegmentedPoolBufBuilder::new().segment_size(2048).initial_capacity(4096).max_capacity(Some(8192)).build();
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

    #[test]
    #[serial]
    fn write_to_segmented_bool_buf_no_garbage_in_pool() {
        let inp = vec![0; 16384];

        countme::enable(true);
        let serialization_buf_reserve_segments = 100;

        let initial_pool_size = 2048;
        let segment_size = 256;

        {
            let b = Buffer::new(BytesMut::new());
            drop(b);
            // Ensure we havn't allocated any bufs yet
            let counts = countme::get::<Buffer>();
            assert_eq!(counts.live, 0);
        }
        let counts = countme::get::<Buffer>();
        let base_total = counts.total;

        let mut buf = SegmentedPoolBufBuilder::new()
            .segment_size(segment_size)
            .initial_capacity(initial_pool_size)
            .build();
        // Keep a reference to the pool around
        let pool = buf.pool.clone();

        // Ensure we havn't allocated more bufs than necessary
        let counts = countme::get::<Buffer>();
        assert!(counts.live > 0);
        assert!(counts.live <= initial_pool_size / segment_size + 1);

        use std::io::Write;
        buf.write_all(&inp).unwrap();

        assert_eq!(
            buf.iter()
                .zip(inp.iter())
                .fold(true, |acc, (a, b)| { acc && (a == *b) }),
            true
        );

        // Ensure we never allocated more buffers than were needed to hold the total elements
        let counts = countme::get::<Buffer>();
        assert!(
            counts.total - base_total
                <= std::cmp::max(
                    inp.len() / segment_size + 1,
                    initial_pool_size / segment_size + 1
                )
        );
        assert_eq!(inp.len(), buf.iter().count());

        let mut count = 0;
        while count < serialization_buf_reserve_segments * segment_size + 1 {
            count += inp.len();
            buf.write_all(&inp).unwrap();
        }

        drop(buf);
        let counts = countme::get::<Buffer>();

        // Ensure pool is cleared up
        assert!(counts.live <= serialization_buf_reserve_segments);

        drop(pool);
        let counts = countme::get::<Buffer>();
        assert!(counts.live == 0);
    }
}
