use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{self, Poll};

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use pin_project::pin_project;

use crate::error::{IngestBufError, LineError, LineMetaError};
use crate::serialize::{
    IngestBuffer, IngestLineSerialize, IngestLineSerializeError, SerializeI64, SerializeMap,
    SerializeStr, SerializeUtf8, SerializeValue,
};

use crate::segmented_buffer::{Buffer, SegmentedPoolBufBuilder};

#[pin_project]
pub struct IngestBodyBuffer {
    #[pin]
    pub(crate) buf: IngestBuffer,
}

impl core::fmt::Debug for IngestBodyBuffer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let buf = self
            .buf
            .buf
            .bytes_reader()
            .bytes()
            .collect::<Result<Vec<u8>, _>>()
            .unwrap();
        if let Ok(b) = std::str::from_utf8(&buf) {
            write!(f, "IngestBodyBuffer: {}", b)
        } else {
            write!(f, "IngestBodyBuffer: {:?}", buf)
        }
    }
}

impl PartialEq for IngestBodyBuffer {
    fn eq(&self, other: &Self) -> bool {
        for (a, b) in self.reader().bytes().zip(other.reader().bytes()) {
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    if a != b {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }
}

impl IngestBodyBuffer {
    pub fn from_buffer(ingest_buffer: IngestBuffer) -> Self {
        Self { buf: ingest_buffer }
    }

    pub fn reader(&self) -> impl std::io::Read + futures::AsyncBufRead + '_ {
        self.buf.buf.bytes_reader()
    }
}

impl Clone for IngestBodyBuffer {
    fn clone(&self) -> Self {
        IngestBodyBuffer::from_buffer(self.buf.clone())
    }
}

// TODO add test
impl hyper::body::HttpBody for IngestBodyBuffer {
    type Data = async_buf_pool::Reusable<Buffer>;
    type Error = Box<IngestBufError>;

    fn poll_data(
        self: Pin<&mut Self>,
        _: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        let mut this = self.project();
        Poll::Ready(this.buf.buf.bufs.pop().map(Ok))
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Result<Option<hyper::HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }
}

/// Type used to construct a body for an IngestRequest
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct IngestBody {
    lines: Vec<Line>,
}

impl IngestBody {
    /// Create a new IngestBody
    pub fn new(lines: Vec<Line>) -> Self {
        Self { lines }
    }
}

#[async_trait]
pub trait IntoIngestBodyBuffer {
    type Error: std::error::Error;

    async fn into(self) -> Result<IngestBodyBuffer, Self::Error>;
}

#[async_trait]
impl IntoIngestBodyBuffer for IngestBodyBuffer {
    type Error = serde_json::error::Error;

    async fn into(self) -> Result<IngestBodyBuffer, Self::Error> {
        Ok(self)
    }
}

#[async_trait]
impl IntoIngestBodyBuffer for IngestBody {
    type Error = serde_json::error::Error;

    async fn into(self) -> Result<IngestBodyBuffer, Self::Error> {
        let mut buf = SegmentedPoolBufBuilder::new()
            .segment_size(2048)
            .initial_capacity(8192)
            .build();

        serde_json::to_writer(&mut buf, &self)?;
        Ok(IngestBodyBuffer::from_buffer(buf))
    }
}

#[async_trait]
impl<'a> IntoIngestBodyBuffer for &'a IngestBody {
    type Error = serde_json::error::Error;

    async fn into(self) -> Result<IngestBodyBuffer, Self::Error> {
        let mut buf = SegmentedPoolBufBuilder::new()
            .segment_size(2048)
            .initial_capacity(8192)
            .build();

        serde_json::to_writer(&mut buf, &self)?;
        Ok(IngestBodyBuffer::from_buffer(buf))
    }
}

pub trait LineMeta {
    fn get_annotations(&self) -> Option<&KeyValueMap>;
    fn get_app(&self) -> Option<&str>;
    fn get_env(&self) -> Option<&str>;
    fn get_file(&self) -> Option<&str>;
    fn get_host(&self) -> Option<&str>;
    fn get_labels(&self) -> Option<&KeyValueMap>;
    fn get_level(&self) -> Option<&str>;
    fn get_meta(&self) -> Option<&Value>;
}

pub trait LineMetaMut: LineMeta {
    fn set_annotations(&mut self, annotations: KeyValueMap) -> Result<(), LineMetaError>;
    fn set_app(&mut self, app: String) -> Result<(), LineMetaError>;
    fn set_env(&mut self, env: String) -> Result<(), LineMetaError>;
    fn set_file(&mut self, file: String) -> Result<(), LineMetaError>;
    fn set_host(&mut self, host: String) -> Result<(), LineMetaError>;
    fn set_labels(&mut self, labels: KeyValueMap) -> Result<(), LineMetaError>;
    fn set_level(&mut self, level: String) -> Result<(), LineMetaError>;
    fn set_meta(&mut self, meta: Value) -> Result<(), LineMetaError>;
}

/// Represents a line that provides accessor to the underlying data buffer for manipulation.
pub trait LineBufferMut: LineMetaMut {
    fn get_line_buffer(&mut self) -> Option<&[u8]>;
    fn set_line_buffer(&mut self, line: Vec<u8>) -> Result<(), LineMetaError>;
}

/// Defines a log line, marking none required fields as Option
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Line {
    /// The annotations field, which is a key value map
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "annotation")]
    pub annotations: Option<KeyValueMap>,
    /// The app field, e.g hello-world-service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    /// The env field, e.g kubernetes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// The file field, e.g /var/log/syslog
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// The host field, e.g node-us-0001
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The labels field, which is a key value map
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "label")]
    pub labels: Option<KeyValueMap>,
    /// The level field, e.g INFO
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    /// The meta field, can be any json value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    /// The line field, e.g 28/Jul/2006:10:27:32 -0300 LogDNA is awesome!
    pub line: String,
    /// The timestamp of when the log line is constructed e.g, 342t783264
    pub timestamp: i64,
}

#[async_trait]
impl<'a> IngestLineSerialize<String, bytes::Bytes, HashMap<String, String>> for &'a Line {
    type Ok = ();

    fn has_annotations(&self) -> bool {
        self.annotations.is_some()
    }
    async fn annotations<'b, S>(
        &mut self,
        ser: &mut S,
    ) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeMap<'b, HashMap<String, String>> + std::marker::Send,
    {
        if let Some(ref annotations) = self.annotations {
            ser.serialize_map(&annotations.0).await?;
        }
        Ok(())
    }
    fn has_app(&self) -> bool {
        self.app.is_some()
    }
    async fn app<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<String> + std::marker::Send,
    {
        if let Some(app) = self.app.as_ref() {
            writer.serialize_str(app).await?;
        };
        Ok(())
    }
    fn has_env(&self) -> bool {
        self.env.is_some()
    }
    async fn env<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<String> + std::marker::Send,
    {
        if let Some(env) = self.env.as_ref() {
            writer.serialize_str(env).await?;
        };
        Ok(())
    }
    fn has_file(&self) -> bool {
        self.file.is_some()
    }
    async fn file<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<String> + std::marker::Send,
    {
        if let Some(file) = self.file.as_ref() {
            writer.serialize_str(file).await?;
        };
        Ok(())
    }
    fn has_host(&self) -> bool {
        self.host.is_some()
    }
    async fn host<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<String> + std::marker::Send,
    {
        if let Some(host) = self.host.as_ref() {
            writer.serialize_str(host).await?;
        };
        Ok(())
    }
    fn has_labels(&self) -> bool {
        self.labels.is_some()
    }
    async fn labels<'b, S>(&mut self, ser: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeMap<'b, HashMap<String, String>> + std::marker::Send,
    {
        if let Some(ref labels) = self.labels {
            ser.serialize_map(&labels.0).await?;
        }
        Ok(())
    }
    fn has_level(&self) -> bool {
        self.level.is_some()
    }
    async fn level<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeStr<String> + std::marker::Send,
    {
        if let Some(level) = self.level.as_ref() {
            writer.serialize_str(level).await?;
        };
        Ok(())
    }
    fn has_meta(&self) -> bool {
        self.meta.is_some()
    }
    async fn meta<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeValue + std::marker::Send,
    {
        if let Some(meta) = self.meta.as_ref() {
            writer.serialize(meta).await?;
        };
        Ok(())
    }
    async fn line<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeUtf8<bytes::Bytes> + std::marker::Send,
    {
        let bytes = bytes::Bytes::copy_from_slice(self.line.as_bytes());
        writer.serialize_utf8(bytes).await?;

        Ok(())
    }
    async fn timestamp<S>(&mut self, writer: &mut S) -> Result<Self::Ok, IngestLineSerializeError>
    where
        S: SerializeI64 + std::marker::Send,
    {
        writer.serialize_i64(&self.timestamp).await?;

        Ok(())
    }
    fn field_count(&self) -> usize {
        2 + if Option::is_none(&self.annotations) {
            0
        } else {
            1
        } + if Option::is_none(&self.app) { 0 } else { 1 }
            + if Option::is_none(&self.env) { 0 } else { 1 }
            + if Option::is_none(&self.file) { 0 } else { 1 }
            + if Option::is_none(&self.host) { 0 } else { 1 }
            + if Option::is_none(&self.labels) { 0 } else { 1 }
            + if Option::is_none(&self.level) { 0 } else { 1 }
            + if Option::is_none(&self.meta) { 0 } else { 1 }
    }
}

impl Line {
    /// create a new line builder
    pub fn builder() -> LineBuilder {
        LineBuilder::new()
    }
}

/// Used to build a log line
///
/// # Example
///
/// ```rust
/// # use logdna_client::body::Line;
/// Line::builder()
///    .line("this is a test")
///    .app("rust-client")
///    .level("INFO")
///    .build()
///    .expect("Line::builder()");
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LineBuilder {
    pub annotations: Option<KeyValueMap>,
    pub app: Option<String>,
    pub env: Option<String>,
    pub file: Option<String>,
    pub host: Option<String>,
    pub labels: Option<KeyValueMap>,
    pub level: Option<String>,
    pub line: Option<String>,
    pub meta: Option<Value>,
}

impl LineBuilder {
    /// Creates a new line builder
    pub fn new() -> Self {
        Self {
            annotations: None,
            app: None,
            env: None,
            file: None,
            host: None,
            labels: None,
            level: None,
            line: None,
            meta: None,
        }
    }
    /// Set the annotations field in the builder
    pub fn annotations<T: Into<KeyValueMap>>(mut self, annotations: T) -> Self {
        self.annotations = Some(annotations.into());
        self
    }
    /// Set the app field in the builder
    pub fn app<T: Into<String>>(mut self, app: T) -> Self {
        self.app = Some(app.into());
        self
    }
    /// Set the env field in the builder
    pub fn env<T: Into<String>>(mut self, env: T) -> Self {
        self.env = Some(env.into());
        self
    }
    /// Set the file field in the builder
    pub fn file<T: Into<String>>(mut self, file: T) -> Self {
        self.file = Some(file.into());
        self
    }
    /// Set the host field in the builder
    pub fn host<T: Into<String>>(mut self, host: T) -> Self {
        self.host = Some(host.into());
        self
    }
    /// Set the level field in the builder
    pub fn labels<T: Into<KeyValueMap>>(mut self, labels: T) -> Self {
        self.labels = Some(labels.into());
        self
    }
    /// Set the level field in the builder
    pub fn level<T: Into<String>>(mut self, level: T) -> Self {
        self.level = Some(level.into());
        self
    }
    /// Set the line field in the builder
    pub fn line<T: Into<String>>(mut self, line: T) -> Self {
        self.line = Some(line.into());
        self
    }
    /// Set the meta field in the builder
    pub fn meta<T: Into<Value>>(mut self, meta: T) -> Self {
        self.meta = Some(meta.into());
        self
    }
    /// Construct a log line from the contents of this builder
    ///
    /// Returning an error if required fields are missing
    pub fn build(self) -> Result<Line, LineError> {
        Ok(Line {
            annotations: self.annotations,
            app: self.app,
            env: self.env,
            file: self.file,
            host: self.host,
            labels: self.labels,
            level: self.level,
            meta: self.meta,
            line: self
                .line
                .ok_or_else(|| LineError::RequiredField("line field is required".into()))?,
            timestamp: Utc::now().timestamp(),
        })
    }
}

impl LineMeta for LineBuilder {
    fn get_annotations(&self) -> Option<&KeyValueMap> {
        self.annotations.as_ref()
    }
    fn get_app(&self) -> Option<&str> {
        self.app.as_deref()
    }
    fn get_env(&self) -> Option<&str> {
        self.env.as_deref()
    }
    fn get_file(&self) -> Option<&str> {
        self.file.as_deref()
    }
    fn get_host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn get_labels(&self) -> Option<&KeyValueMap> {
        self.labels.as_ref()
    }
    fn get_level(&self) -> Option<&str> {
        self.level.as_deref()
    }
    fn get_meta(&self) -> Option<&Value> {
        self.meta.as_ref()
    }
}

impl LineMeta for &LineBuilder {
    fn get_annotations(&self) -> Option<&KeyValueMap> {
        self.annotations.as_ref()
    }
    fn get_app(&self) -> Option<&str> {
        self.app.as_deref()
    }
    fn get_env(&self) -> Option<&str> {
        self.env.as_deref()
    }
    fn get_file(&self) -> Option<&str> {
        self.file.as_deref()
    }
    fn get_host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn get_labels(&self) -> Option<&KeyValueMap> {
        self.labels.as_ref()
    }
    fn get_level(&self) -> Option<&str> {
        self.level.as_deref()
    }
    fn get_meta(&self) -> Option<&Value> {
        self.meta.as_ref()
    }
}

impl LineMeta for &mut LineBuilder {
    fn get_annotations(&self) -> Option<&KeyValueMap> {
        self.annotations.as_ref()
    }
    fn get_app(&self) -> Option<&str> {
        self.app.as_deref()
    }
    fn get_env(&self) -> Option<&str> {
        self.env.as_deref()
    }
    fn get_file(&self) -> Option<&str> {
        self.file.as_deref()
    }
    fn get_host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn get_labels(&self) -> Option<&KeyValueMap> {
        self.labels.as_ref()
    }
    fn get_level(&self) -> Option<&str> {
        self.level.as_deref()
    }
    fn get_meta(&self) -> Option<&Value> {
        self.meta.as_ref()
    }
}

impl LineMetaMut for LineBuilder {
    fn set_annotations(&mut self, annotations: KeyValueMap) -> Result<(), LineMetaError> {
        self.annotations = Some(annotations);
        Ok(())
    }
    fn set_app(&mut self, app: String) -> Result<(), LineMetaError> {
        self.app = Some(app);
        Ok(())
    }
    fn set_env(&mut self, env: String) -> Result<(), LineMetaError> {
        self.env = Some(env);
        Ok(())
    }
    fn set_file(&mut self, file: String) -> Result<(), LineMetaError> {
        self.file = Some(file);
        Ok(())
    }
    fn set_host(&mut self, host: String) -> Result<(), LineMetaError> {
        self.host = Some(host);
        Ok(())
    }
    fn set_labels(&mut self, labels: KeyValueMap) -> Result<(), LineMetaError> {
        self.labels = Some(labels);
        Ok(())
    }
    fn set_level(&mut self, level: String) -> Result<(), LineMetaError> {
        self.level = Some(level);
        Ok(())
    }
    fn set_meta(&mut self, meta: Value) -> Result<(), LineMetaError> {
        self.meta = Some(meta);
        Ok(())
    }
}

impl LineBufferMut for LineBuilder {
    fn get_line_buffer(&mut self) -> Option<&[u8]> {
        self.line.as_deref().map(|x| x.as_bytes())
    }

    fn set_line_buffer(&mut self, line: Vec<u8>) -> Result<(), LineMetaError> {
        self.line = Some(
            String::from_utf8(line)
                // Only accept UTF-8 representations of the data for LineBuilder
                .map_err(|_| LineMetaError::Failed("line is not a UTF-8 string"))?,
        );
        Ok(())
    }
}

impl LineMetaMut for &mut LineBuilder {
    fn set_annotations(&mut self, annotations: KeyValueMap) -> Result<(), LineMetaError> {
        self.annotations = Some(annotations);
        Ok(())
    }
    fn set_app(&mut self, app: String) -> Result<(), LineMetaError> {
        self.app = Some(app);
        Ok(())
    }
    fn set_env(&mut self, env: String) -> Result<(), LineMetaError> {
        self.env = Some(env);
        Ok(())
    }
    fn set_file(&mut self, file: String) -> Result<(), LineMetaError> {
        self.file = Some(file);
        Ok(())
    }
    fn set_host(&mut self, host: String) -> Result<(), LineMetaError> {
        self.host = Some(host);
        Ok(())
    }
    fn set_labels(&mut self, labels: KeyValueMap) -> Result<(), LineMetaError> {
        self.labels = Some(labels);
        Ok(())
    }
    fn set_level(&mut self, level: String) -> Result<(), LineMetaError> {
        self.level = Some(level);
        Ok(())
    }
    fn set_meta(&mut self, meta: Value) -> Result<(), LineMetaError> {
        self.meta = Some(meta);
        Ok(())
    }
}

impl LineBufferMut for &mut LineBuilder {
    fn get_line_buffer(&mut self) -> Option<&[u8]> {
        self.line.as_deref().map(|x| x.as_bytes())
    }
    fn set_line_buffer(&mut self, line: Vec<u8>) -> Result<(), LineMetaError> {
        self.line = Some(
            String::from_utf8(line)
                // Only accept UTF-8 representations of the data for LineBuilder
                .map_err(|_| LineMetaError::Failed("line is not a UTF-8 string"))?,
        );
        Ok(())
    }
}

impl LineMeta for Line {
    fn get_annotations(&self) -> Option<&KeyValueMap> {
        self.annotations.as_ref()
    }
    fn get_app(&self) -> Option<&str> {
        self.app.as_deref()
    }
    fn get_env(&self) -> Option<&str> {
        self.env.as_deref()
    }
    fn get_file(&self) -> Option<&str> {
        self.file.as_deref()
    }
    fn get_host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn get_labels(&self) -> Option<&KeyValueMap> {
        self.labels.as_ref()
    }
    fn get_level(&self) -> Option<&str> {
        self.level.as_deref()
    }
    fn get_meta(&self) -> Option<&Value> {
        self.meta.as_ref()
    }
}

impl<'a> LineMeta for &'a Line {
    fn get_annotations(&self) -> Option<&KeyValueMap> {
        self.annotations.as_ref()
    }
    fn get_app(&self) -> Option<&str> {
        self.app.as_deref()
    }
    fn get_env(&self) -> Option<&str> {
        self.env.as_deref()
    }
    fn get_file(&self) -> Option<&str> {
        self.file.as_deref()
    }
    fn get_host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn get_labels(&self) -> Option<&KeyValueMap> {
        self.labels.as_ref()
    }
    fn get_level(&self) -> Option<&str> {
        self.level.as_deref()
    }
    fn get_meta(&self) -> Option<&Value> {
        self.meta.as_ref()
    }
}

impl Default for LineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<IngestBody> for IngestBody {
    fn as_ref(&self) -> &IngestBody {
        self
    }
}

/// Json key value map (json object with a depth of 1)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct KeyValueMap(HashMap<String, String>);

impl Deref for KeyValueMap {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KeyValueMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl KeyValueMap {
    /// Create an empty key value map
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    /// Add key value pair to the map
    pub fn add<T: Into<String>>(mut self, key: T, value: T) -> Self {
        self.0.insert(key.into(), value.into());
        self
    }
    /// Remove key value pair from map
    pub fn remove<'a, T: Into<&'a String>>(mut self, key: T) -> Self {
        self.0.remove(key.into());
        self
    }
}

impl Default for KeyValueMap {
    fn default() -> Self {
        Self::new()
    }
}
impl From<BTreeMap<String, String>> for KeyValueMap {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self {
            0: HashMap::from_iter(map),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    use std::io::Read;

    use bytes::buf::Buf;

    use proptest::collection::hash_map;
    use proptest::option::of;
    use proptest::prelude::*;
    use proptest::string::string_regex;

    pub fn key_value_map_st(max_entries: usize) -> impl Strategy<Value = KeyValueMap> {
        hash_map(
            string_regex(".{1,64}").unwrap(),
            string_regex(".{1,64}").unwrap(),
            0..max_entries,
        )
        .prop_map(KeyValueMap)
    }

    //recursive JSON type
    pub fn json_st(depth: u32) -> impl Strategy<Value = serde_json::Value> {
        let leaf = prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(|o| serde_json::to_value(o).unwrap()),
            any::<f64>().prop_map(|o| serde_json::to_value(o).unwrap()),
            ".{1,64}".prop_map(|o| serde_json::to_value(o).unwrap()),
        ];
        leaf.prop_recursive(depth, 256, 10, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..10)
                    .prop_map(|o| serde_json::to_value(o).unwrap()),
                prop::collection::hash_map(".*", inner, 0..10)
                    .prop_map(|o| serde_json::to_value(o).unwrap()),
            ]
        })
    }
    pub fn line_st() -> impl Strategy<Value = Line> {
        (
            of(key_value_map_st(5)),
            of(string_regex(".{1,64}").unwrap()),
            of(string_regex(".{1,64}").unwrap()),
            of(string_regex(".{1,64}").unwrap()),
            of(string_regex(".{1,64}").unwrap()),
            of(key_value_map_st(5)),
            of(string_regex(".{1,64}").unwrap()),
            of(json_st(3)),
            string_regex(".{1,64}").unwrap(),
            (0..i64::MAX),
        )
            .prop_map(
                |(annotations, app, env, file, host, labels, level, meta, line, timestamp)| Line {
                    annotations,
                    app,
                    env,
                    file,
                    host,
                    labels,
                    level,
                    meta,
                    line,
                    timestamp,
                },
            )
    }
    proptest! {
        #[test]
        fn serialize_line(line in line_st()) {
            use crate::serialize::IngestLineSerializer;

            let buf = SegmentedPoolBufBuilder::new().segment_size(2048).build();
            let se = IngestLineSerializer {
                buf: serde_json::Serializer::new(buf),
            };

            let serde_serialized = serde_json::to_string(&line).unwrap();

            let serialized = tokio_test::block_on(se.write_line(&line)).unwrap();
            let mut buf = String::new();
            serialized.reader().read_to_string(&mut buf).unwrap();
            assert_eq!(
                serde_serialized,
                buf
            );

        // Create Ingestbuffer
    }

    }

    proptest! {
        #[test]
        fn serialize_lines(lines in proptest::collection::vec(line_st(), 5)) {
            use crate::serialize::IngestBodySerializer;

            let buf = SegmentedPoolBufBuilder::new()
                .segment_size(2048)
                .initial_capacity(8192)
                .build();

            let ingest_body = IngestBody{lines};
            let serde_serialized = serde_json::to_string(&ingest_body).unwrap();

            let mut se = IngestBodySerializer::from_buffer(buf).unwrap();
            for line in ingest_body.lines.iter() {
                tokio_test::block_on(se.write_line(line)).unwrap();
            }
            let serialized = se.end().unwrap();
            let mut buf = String::new();
            serialized.reader().read_to_string(&mut buf).unwrap();

            assert_eq!(
                serde_serialized,
                buf
            );

            // Fails because float parsing is dodgy
            //assert_eq!(serde_json::from_str::<IngestBody>(&buf).unwrap(), ingest_body);
        }
    }
    proptest! {

        #[test]
        fn ingest_body_buffer_http_body(lines in proptest::collection::vec(line_st(), 5)) {
            let ingest_body = IngestBody{lines};
            let serde_serialized = serde_json::to_string(&ingest_body).unwrap();

            let ingest_body_buffer: IngestBodyBuffer = tokio_test::block_on(IntoIngestBodyBuffer::into(&ingest_body)).unwrap();

            let mut buf = String::new();
            ingest_body_buffer.reader().read_to_string(&mut buf).unwrap();
            assert_eq!(serde_serialized.len(), buf.len());
        }
    }
}
