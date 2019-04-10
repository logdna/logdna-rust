use flate2::Compression;
use futures::Future;
use http::{HttpTryFrom, Method};
use http::header::HeaderValue;
use hyper::{Body, Request};

use crate::error::{BuildError, ReuquestError};
use crate::params::Params;

pub type IngestRequest = Box<Future<Item=Request<Body>, Error=ReuquestError> + Send + 'static>;

pub enum Encoding {
    Json,
    GzipJson(Compression),
}

pub struct RequestTemplate {
    method: Method,
    charset: HeaderValue,
    encoding: Encoding,
    api_key: String,
}

impl RequestTemplate {
    pub fn builder() -> TemplateBuilder {
        TemplateBuilder::new()
    }
}

pub struct TemplateBuilder {
    method: Method,
    charset: HeaderValue,
    encoding: Encoding,
    api_key: Option<String>,
    params: Option<Params>,
    err: Option<BuildError>,
}

impl TemplateBuilder {
    pub fn new() -> Self {
        Self {
            method: Method::POST,
            charset: HeaderValue::from_str("utf8").expect("HeaderValue::from_str(utf8)"),
            encoding: Encoding::GzipJson(Compression::new(2)),
            api_key: None,
            params: None,
            err: None,
        }
    }

    pub fn method<T: Into<Method>>(&mut self, method: T) -> &mut Self {
        self.method = method.into();
        self
    }

    pub fn charset<T>(&mut self, charset: T) -> &mut Self
        where HeaderValue: HttpTryFrom<T>
    {
        self.charset = match HttpTryFrom::try_from(charset) {
            Ok(v) => v,
            Err(e) => {
                self.err = Some(BuildError::InvalidHeader(e.into()));
                return self;
            }
        };
        self
    }

    pub fn encoding<T: Into<Encoding>>(&mut self, encoding: T) -> &mut Self {
        self.encoding = encoding.into();
        self
    }

    pub fn api_key<T: Into<String>>(&mut self, api_key: T) -> &mut Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn params<T: Into<Params>>(&mut self, params: T) -> &mut Self {
        self.params = Some(params.into());
        self
    }

    pub fn build(self) -> Result<RequestTemplate, BuildError> {
        Ok(RequestTemplate {
            method: self.method,
            charset: self.charset,
            encoding: self.encoding,
            api_key: self.api_key.ok_or("api_key is required in a TemplateBuilder".into())?,
        })
    }
}