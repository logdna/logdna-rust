use chrono::Utc;
use flate2::write::GzEncoder;
use futures::future::{Future, IntoFuture};
use futures::future;
use hyper::Body;
use serde::{Deserialize, Serialize};

use crate::error::BodyError;
use crate::error::LineError;
use crate::request::Encoding;

pub type HttpBody = Box<Future<Item=Body, Error=BodyError> + Send + 'static>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestBody {
    lines: Vec<Line>
}

impl IngestBody {
    pub fn new(lines: Vec<Line>) -> Self {
        Self { lines }
    }

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Line {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    pub line: String,
    pub timestamp: i64,
}

impl Line {
    pub fn builder() -> LineBuilder {
        LineBuilder::new()
    }
}

pub struct LineBuilder {
    app: Option<String>,
    env: Option<String>,
    file: Option<String>,
    level: Option<String>,
    line: Option<String>,
}

impl LineBuilder {
    pub fn new() -> Self {
        Self {
            app: None,
            env: None,
            file: None,
            level: None,
            line: None,
        }
    }

    pub fn app<T: Into<String>>(&mut self, app: T) -> &mut Self {
        self.app = Some(app.into());
        self
    }

    pub fn env<T: Into<String>>(&mut self, env: T) -> &mut Self {
        self.env = Some(env.into());
        self
    }

    pub fn file<T: Into<String>>(&mut self, file: T) -> &mut Self {
        self.file = Some(file.into());
        self
    }

    pub fn level<T: Into<String>>(&mut self, level: T) -> &mut Self {
        self.level = Some(level.into());
        self
    }

    pub fn line<T: Into<String>>(&mut self, line: T) -> &mut Self {
        self.line = Some(line.into());
        self
    }

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