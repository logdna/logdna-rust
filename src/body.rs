use chrono::Utc;
use flate2::write::GzEncoder;
use futures::future::{Future, IntoFuture};
use futures::future;
use hyper::Body;
use serde::{Deserialize, Serialize};

use crate::error::BodyError;
use crate::error::LineError;
use crate::request::Encoding;

/// HTTP body type alias
pub type HttpBody = Box<Future<Item=Body, Error=BodyError> + Send + 'static>;

/// Type used to construct a body for an IngestRequest
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestBody {
    lines: Vec<Line>
}

impl IngestBody {
    /// Create a new IngestBody
    pub fn new(lines: Vec<Line>) -> Self {
        Self { lines }
    }

    /// Serializes (and compresses, depending on Encoding type) itself to prepare for http transport
    pub fn into_http_body(self, encoding: Encoding) -> HttpBody {
        match encoding {
            Encoding::GzipJson(level) =>
                Box::new(
                    future::ok(GzEncoder::new(Vec::new(), level))
                        .and_then(move |mut encoder|
                            serde_json::to_writer(&mut encoder, &self)
                                .map_err(BodyError::from)
                                .and_then(move |_| encoder.finish().map_err(Into::into))
                        )
                        .map(|bytes| Body::from(bytes))
                ),
            Encoding::Json =>
                Box::new(
                    serde_json::to_vec(&self)
                        .map(|bytes| Body::from(bytes))
                        .map_err(BodyError::from)
                        .into_future()
                )
        }
    }
}

/// Defines a log line, marking none required fields as Option
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Line {
    /// The app field, e.g hello-world-service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    /// The env field, e.g kubernetes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// The file field, e.g /var/log/syslog
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// The level field, e.g INFO
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
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
pub struct LineBuilder {
    app: Option<String>,
    env: Option<String>,
    file: Option<String>,
    level: Option<String>,
    line: Option<String>,
}

impl LineBuilder {
    /// Creates a new line builder
    pub fn new() -> Self {
        Self {
            app: None,
            env: None,
            file: None,
            level: None,
            line: None,
        }
    }
    /// Set the app field in the builder
    pub fn app<T: Into<String>>(&mut self, app: T) -> &mut Self {
        self.app = Some(app.into());
        self
    }
    /// Set the env field in the builder
    pub fn env<T: Into<String>>(&mut self, env: T) -> &mut Self {
        self.env = Some(env.into());
        self
    }
    /// Set the file field in the builder
    pub fn file<T: Into<String>>(&mut self, file: T) -> &mut Self {
        self.file = Some(file.into());
        self
    }
    /// Set the level field in the builder
    pub fn level<T: Into<String>>(&mut self, level: T) -> &mut Self {
        self.level = Some(level.into());
        self
    }
    /// Set the line field in the builder
    pub fn line<T: Into<String>>(&mut self, line: T) -> &mut Self {
        self.line = Some(line.into());
        self
    }
    /// Construct a log line from the contents of this builder
    ///
    /// Returning an error if required fields are missing
    pub fn build(&mut self) -> Result<Line, LineError> {
        Ok(Line {
            app: self.app.clone(),
            env: self.env.clone(),
            file: self.file.clone(),
            level: self.level.clone(),
            line: self.line.clone()
                .ok_or(LineError::RequiredField("line is required in a LineBuilder".into()))?,
            timestamp: Utc::now().timestamp(),
        })
    }
}