use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};

use chrono::Utc;
use flate2::write::GzEncoder;
use hyper::Body;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::BodyError;
use crate::error::LineError;
use crate::request::Encoding;

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

    /// Serializes (and compresses, depending on Encoding type) itself to prepare for http transport
    pub fn as_http_body(&self, encoding: &Encoding) -> Result<Body, BodyError> {
        match encoding {
            Encoding::GzipJson(level) => {
                let mut encoder = GzEncoder::new(Vec::new(), *level);
                serde_json::to_writer(&mut encoder, self)?;
                Ok(Body::from(encoder.finish()?))
            }
            Encoding::Json => {
                let bytes = serde_json::to_vec(self)?;
                Ok(Body::from(bytes))
            }
        }
    }
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
