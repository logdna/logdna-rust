use futures::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use serde::{Serialize, Serializer};
use serde_json::ser::{CharEscape, Formatter};
use thiserror::Error;

use crate::segmented_buffer::AllocBytesMutFn;

pub type IngestBuffer = crate::segmented_buffer::SegmentedPoolBuf<
    Pin<Box<dyn Future<Output = Option<async_buf_pool::Reusable<BytesMut>>> + std::marker::Send>>,
    BytesMut,
    AllocBytesMutFn,
>;

#[derive(Debug, Error)]
pub enum IngestLineSerializeError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),
}

// Trait to allow a type containing Line data to serialize itself into a caller provided buffer
#[async_trait]
pub trait IngestLineSerialize<T, U, V>
where
    T: std::marker::Send,
    U: std::marker::Send,
{
    type Ok;

    fn has_annotations(&self) -> bool;
    async fn annotations<S>(
        &mut self,
        writer: &mut S,
    ) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeMap<V> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_app(&self) -> bool;
    async fn app<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<T> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_env(&self) -> bool;
    async fn env<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<T> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_file(&self) -> bool;
    async fn file<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<T> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_host(&self) -> bool;
    async fn host<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<T> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_labels(&self) -> bool;
    async fn labels<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeMap<V> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_level(&self) -> bool;
    async fn level<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<T> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn has_meta(&self) -> bool;
    async fn meta<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeValue + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    async fn line<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeUtf8<U> + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    async fn timestamp<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeI64 + std::marker::Send,
        T: 'async_trait,
        U: 'async_trait;
    fn field_count(&self) -> usize;
}

pub trait SerializeUtf8<T: ?Sized> {
    type Ok;
    fn serialize_utf8(&mut self, key: T) -> Result<Self::Ok, IngestLineSerializeError>;
}

pub trait SerializeStr<T: ?Sized> {
    type Ok;
    fn serialize_str(&mut self, key: &T) -> Result<Self::Ok, IngestLineSerializeError>;
}

pub trait SerializeI64 {
    type Ok;
    fn serialize_i64(&mut self, key: &i64) -> Result<Self::Ok, IngestLineSerializeError>;
}

pub trait SerializeValue {
    type Ok;
    fn serialize(&mut self, key: &serde_json::Value) -> Result<Self::Ok, IngestLineSerializeError>;
}

pub trait SerializeMap<T: ?Sized> {
    type Ok;
    fn serialize_map(&mut self, key: &T) -> Result<Self::Ok, IngestLineSerializeError>;
}

pub struct IngestBytesSerializer {
    pub(crate) ser: Option<IngestLineSerializer>,
}

impl IngestBytesSerializer {
    fn into_buffer(self) -> Option<IngestBuffer> {
        self.ser.map(move |ser| ser.buf.into_inner())
    }
}

impl<T> SerializeStr<T> for IngestBytesSerializer
where
    T: AsRef<str>,
{
    type Ok = ();

    fn serialize_str(&mut self, bytes: &T) -> Result<Self::Ok, IngestLineSerializeError> {
        // Infallible
        let mut ser = self.ser.take().unwrap();
        bytes.as_ref().serialize(&mut ser.buf)?;
        self.ser = Some(ser);
        Ok(())
    }
}

impl<I, K, V> SerializeMap<I> for IngestBytesSerializer
where
    for<'a> &'a I: IntoIterator<Item = (&'a K, &'a V)>,
    K: Serialize,
    V: Serialize,
{
    type Ok = ();

    fn serialize_map(&mut self, bytes: &I) -> Result<Self::Ok, IngestLineSerializeError> {
        // Infallible
        use serde::ser::SerializeMap;
        let mut _ser = self.ser.take().unwrap();
        let mut ser = _ser.buf.serialize_map(None)?;
        for (k, v) in bytes.into_iter() {
            ser.serialize_entry(k, v)?;
        }
        ser.end()?;
        self.ser = Some(_ser);
        Ok(())
    }
}

impl SerializeI64 for IngestBytesSerializer {
    type Ok = ();

    fn serialize_i64(&mut self, i: &i64) -> Result<Self::Ok, IngestLineSerializeError> {
        // Infallible
        let mut ser = self.ser.take().unwrap();
        i.serialize(&mut ser.buf)?;
        self.ser = Some(ser);
        Ok(())
    }
}

impl SerializeValue for IngestBytesSerializer {
    type Ok = ();

    fn serialize(&mut self, i: &serde_json::Value) -> Result<Self::Ok, IngestLineSerializeError> {
        // Infallible
        let mut ser = self.ser.take().unwrap();
        i.serialize(&mut ser.buf)?;
        self.ser = Some(ser);
        Ok(())
    }
}

impl<T> SerializeUtf8<T> for IngestBytesSerializer
where
    T: bytes::buf::Buf,
{
    type Ok = ();

    fn serialize_utf8(&mut self, mut bytes: T) -> Result<Self::Ok, IngestLineSerializeError> {
        //let mut bytes = bytes.buf;
        let mut fmt = serde_json::ser::CompactFormatter {};
        let mut wtr = self.ser.take().unwrap().buf.into_inner();

        fmt.begin_string(&mut wtr)?;

        while bytes.remaining() != 0 {
            let chunk = bytes.bytes();
            let chunk_len = chunk.len();
            utf8::LossyDecoder::new(|s| {
                format_escaped_str_contents(&mut wtr, &mut fmt, s).expect("Buf write can't fail")
            })
            .feed(chunk);
            bytes.advance(chunk_len)
        }
        fmt.end_string(&mut wtr)?;

        self.ser = Some(IngestLineSerializer::from_buffer(wtr));
        Ok(())
    }
}

#[inline]
fn from_escape_table(escape: u8, byte: u8) -> CharEscape {
    match escape {
        self::BB => CharEscape::Backspace,
        self::TT => CharEscape::Tab,
        self::NN => CharEscape::LineFeed,
        self::FF => CharEscape::FormFeed,
        self::RR => CharEscape::CarriageReturn,
        self::QU => CharEscape::Quote,
        self::BS => CharEscape::ReverseSolidus,
        self::UU => CharEscape::AsciiControl(byte),
        _ => unreachable!(),
    }
}

// Lifted directly from serde as they aren't public
#[inline]
fn format_escaped_str_contents<W, F>(
    writer: &mut W,
    formatter: &mut F,
    value: &str,
) -> io::Result<()>
where
    W: ?Sized + io::Write,
    F: ?Sized + Formatter,
{
    let bytes = value.as_bytes();

    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = ESCAPE[byte as usize];
        if escape == 0 {
            continue;
        }

        if start < i {
            formatter.write_string_fragment(writer, &value[start..i])?;
        }

        let char_escape = from_escape_table(escape, byte);
        formatter.write_char_escape(writer, char_escape)?;

        start = i + 1;
    }

    if start != bytes.len() {
        formatter.write_string_fragment(writer, &value[start..])?;
    }

    Ok(())
}

const BB: u8 = b'b'; // \x08
const TT: u8 = b't'; // \x09
const NN: u8 = b'n'; // \x0A
const FF: u8 = b'f'; // \x0C
const RR: u8 = b'r'; // \x0D
const QU: u8 = b'"'; // \x22
const BS: u8 = b'\\'; // \x5C
const UU: u8 = b'u'; // \x00...\x1F except the ones above
const __: u8 = 0;

// Lookup table of escape sequences. A value of b'x' at index i means that byte
// i is escaped as "\x" in JSON. A value of 0 means that byte i is not escaped.
static ESCAPE: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 3
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
    __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __, // 5
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

pub struct IngestLineSerializer {
    pub(crate) buf: serde_json::Serializer<IngestBuffer>,
}

fn serde_serialize_key_to_buf<F, T>(
    fmt: &mut F,
    mut wtr: T,
    first: &mut bool,
    key: &str,
) -> Result<T, IngestLineSerializeError>
where
    F: serde_json::ser::Formatter,
    T: std::io::Write,
{
    fmt.begin_object_key(&mut wtr, *first)?;
    *first = false;

    let mut ser = serde_json::Serializer::new(wtr);
    ser.serialize_str(key)?;
    let mut wtr = ser.into_inner();

    fmt.end_object_key(&mut wtr)?;
    fmt.begin_object_value(&mut wtr)?;
    Ok(wtr)
}

macro_rules! serialize {
    ($a:ident, $b:ident, $c:ident, $d:literal, $f:ident) => {
        let mut fmt = serde_json::ser::CompactFormatter {};

        let wtr = serde_serialize_key_to_buf(&mut fmt, $a, &mut $f, $d)?;
        let mut ser = IngestLineSerializer::from_buffer(wtr).into_serialize_value();

        $b.$c(&mut ser).await?;

        let mut wtr = ser.into_buffer().unwrap();
        fmt.end_object_value(&mut wtr)?;

        $a = wtr;
    };
}

impl IngestLineSerializer {
    pub fn from_buffer(buf: IngestBuffer) -> Self {
        Self {
            buf: serde_json::Serializer::new(buf),
        }
    }

    pub fn into_inner(self) -> IngestBuffer {
        self.buf.into_inner()
    }

    pub fn into_serialize_value(self) -> IngestBytesSerializer {
        IngestBytesSerializer { ser: Some(self) }
    }

    pub async fn write_line<T, U, I>(
        self,
        mut from: impl IngestLineSerialize<T, U, I>,
    ) -> Result<IngestBuffer, IngestLineSerializeError>
    where
        T: AsRef<str> + std::marker::Send,
        U: bytes::buf::Buf + std::marker::Send,
        for<'a> &'a I: IntoIterator<Item = (&'a String, &'a String)> + std::marker::Send,
    {
        let mut fmt = serde_json::ser::CompactFormatter {};
        let mut first = true;
        let mut s_wtr = self.into_inner();
        fmt.begin_object(&mut s_wtr)?;

        if from.has_annotations() {
            serialize!(s_wtr, from, annotations, "annotation", first);
        }

        if from.has_app() {
            serialize!(s_wtr, from, app, "app", first);
        }

        if from.has_env() {
            serialize!(s_wtr, from, env, "env", first);
        }

        if from.has_file() {
            serialize!(s_wtr, from, file, "file", first);
        }

        if from.has_host() {
            serialize!(s_wtr, from, host, "host", first);
        }

        if from.has_labels() {
            serialize!(s_wtr, from, labels, "label", first);
        }

        if from.has_level() {
            serialize!(s_wtr, from, level, "level", first);
        }

        if from.has_meta() {
            serialize!(s_wtr, from, meta, "meta", first);
        }

        serialize!(s_wtr, from, line, "line", first);
        serialize!(s_wtr, from, timestamp, "timestamp", first);

        fmt.end_object(&mut s_wtr)?;
        Ok(s_wtr)
    }
}

pub struct IngestBodySerializer {
    pub(crate) buf: Option<IngestBuffer>,
    first: bool,
}

impl IngestBodySerializer {
    pub fn from_buffer(mut buf: IngestBuffer) -> Result<Self, IngestLineSerializeError> {
        let mut fmt = serde_json::ser::CompactFormatter {};
        fmt.begin_object(&mut buf)?;

        fmt.begin_object_key(&mut buf, true)?;
        fmt.begin_string(&mut buf)?;
        fmt.write_string_fragment(&mut buf, "lines")?;
        fmt.end_string(&mut buf)?;
        fmt.end_object_key(&mut buf)?;

        fmt.begin_object_value(&mut buf)?;
        fmt.begin_array(&mut buf)?;

        Ok(Self {
            buf: Some(buf),
            first: true,
        })
    }

    pub async fn write_line<T, U, I>(
        &mut self,
        from: impl IngestLineSerialize<T, U, I>,
    ) -> Result<(), IngestLineSerializeError>
    where
        T: AsRef<str> + std::marker::Send,
        U: bytes::buf::Buf + std::marker::Send,
        for<'a> &'a I: IntoIterator<Item = (&'a String, &'a String)> + std::marker::Send,
    {
        let mut fmt = serde_json::ser::CompactFormatter {};

        // Infallible
        let mut buf = self.buf.take().unwrap();
        fmt.begin_array_value(&mut buf, self.first)?;
        self.first = false;
        let ser = IngestLineSerializer::from_buffer(buf);
        let mut buf = ser.write_line(from).await?;
        fmt.end_array_value(&mut buf)?;
        self.buf = Some(buf);
        Ok(())
    }

    pub fn end(mut self) -> Result<IngestBuffer, IngestLineSerializeError> {
        let mut fmt = serde_json::ser::CompactFormatter {};
        // Infallible
        let mut wtr = self.buf.take().unwrap();
        fmt.end_array(&mut wtr)?;
        fmt.end_object_value(&mut wtr)?;

        fmt.end_object(&mut wtr)?;
        Ok(wtr)
    }
}

pub fn buffer_source(
    segment_size: usize,
    initial_capacity: usize,
) -> impl futures::stream::Stream<Item = IngestLineSerializer> {
    let segment_size2 = segment_size;
    let initial_capacity2 = segment_size;
    futures::stream::unfold(
        async_buf_pool::Pool::<AllocBytesMutFn, BytesMut>::new(
            initial_capacity,
            Arc::new(move || BytesMut::with_capacity(segment_size)),
        ),
        move |pool| async move {
            Some((
                IngestLineSerializer {
                    buf: serde_json::Serializer::new(
                        crate::segmented_buffer::SegmentedPoolBufBuilder::new()
                            .segment_size(segment_size2)
                            .initial_capacity(initial_capacity2)
                            .with_pool(pool.clone()),
                    ),
                },
                pool,
            ))
        },
    )
}
