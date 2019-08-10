use chrono::Utc;
use flate2::Compression;
use futures::Future;
use http::{HttpTryFrom, Method};
use http::header::ACCEPT_CHARSET;
use http::header::CONTENT_ENCODING;
use http::header::CONTENT_TYPE;
use http::header::HeaderValue;
use http::request::Builder as RequestBuilder;
use hyper::{Body, Request};

use crate::body::{IngestBody, into_http_body};
use crate::error::{RequestError, TemplateError};
use crate::params::Params;

///type alias for a request used by the client
pub type IngestRequest = Box<Future<Item=Request<Body>, Error=RequestError> + Send + 'static>;

/// A reusable template to generate requests from
#[derive(Debug)]
pub struct RequestTemplate {
    /// HTTP method, default is POST
    pub method: Method,
    /// Content charset, default is utf8
    pub charset: HeaderValue,
    /// Content type, default is application/json
    pub content: HeaderValue,
    /// Content encoding, default is gzip
    pub encoding: Encoding,
    /// Http schema, default is https
    pub schema: Schema,
    /// Host / domain, default is logs.logdna.com
    pub host: String,
    /// Ingest endpoint, default is /logs/ingest
    pub endpoint: String,
    /// Query parameters appended to the url
    pub params: Params,
    /// LogDNA ingestion key
    pub api_key: String,
}

impl RequestTemplate {
    /// Constructs a new TemplateBuilder
    pub fn builder() -> TemplateBuilder {
        TemplateBuilder::new()
    }
    /// Uses the template to create a new request
    pub fn new_request<T: AsRef<IngestBody> + Send + 'static>(&self, body: T) -> IngestRequest {
        let mut builder = RequestBuilder::new();

        let params = serde_urlencoded::to_string(self.params.clone().set_now(Utc::now().timestamp()))
            .expect("cant'fail!");

        builder.method(self.method.clone())
            .header(ACCEPT_CHARSET, self.charset.clone())
            .header(CONTENT_TYPE, self.content.clone())
            .header("apiKey", self.api_key.clone())
            .uri(self.schema.to_string() + &self.host + &self.endpoint + "?" + &params);

        self.encoding.set_builder_encoding(&mut builder);

        Box::new(
            into_http_body(body, self.encoding.clone())
                .map_err(RequestError::from)
                .and_then(move |body| builder.body(body).map_err(Into::into))
        )
    }
}

/// Used to build an instance of a RequestTemplate
pub struct TemplateBuilder {
    method: Method,
    charset: HeaderValue,
    content: HeaderValue,
    encoding: Encoding,
    schema: Schema,
    host: String,
    endpoint: String,
    params: Option<Params>,
    api_key: Option<String>,
    err: Option<TemplateError>,
}

/// Represents the encoding to be used when sending an IngestRequest
#[derive(Debug, Clone)]
pub enum Encoding {
    Json,
    GzipJson(Compression),
}

impl TemplateBuilder {
    /// Constructs a new TemplateBuilder
    pub fn new() -> Self {
        Self {
            method: Method::POST,
            charset: HeaderValue::from_str("utf8").expect("charset::from_str()"),
            content: HeaderValue::from_str("application/json").expect("content::from_str()"),
            encoding: Encoding::GzipJson(Compression::new(2)),
            schema: Schema::Https,
            host: "logs.logdna.com".into(),
            endpoint: "/logs/ingest".into(),
            params: None,
            api_key: None,
            err: None,
        }
    }
    /// Set the method field
    pub fn method<T: Into<Method>>(&mut self, method: T) -> &mut Self {
        self.method = method.into();
        self
    }
    /// Set the charset field
    pub fn charset<T>(&mut self, charset: T) -> &mut Self
        where HeaderValue: HttpTryFrom<T>
    {
        self.charset = match HttpTryFrom::try_from(charset) {
            Ok(v) => v,
            Err(e) => {
                self.err = Some(TemplateError::InvalidHeader(e.into()));
                return self;
            }
        };
        self
    }
    /// Set the content field
    pub fn content<T>(&mut self, content: T) -> &mut Self
        where HeaderValue: HttpTryFrom<T>
    {
        self.content = match HttpTryFrom::try_from(content) {
            Ok(v) => v,
            Err(e) => {
                self.err = Some(TemplateError::InvalidHeader(e.into()));
                return self;
            }
        };
        self
    }
    /// Set the encoding field
    pub fn encoding<T: Into<Encoding>>(&mut self, encoding: T) -> &mut Self {
        self.encoding = encoding.into();
        self
    }
    /// Set the schema field
    pub fn schema<T: Into<Schema>>(&mut self, schema: T) -> &mut Self {
        self.schema = schema.into();
        self
    }
    /// Set the host field
    pub fn host<T: Into<String>>(&mut self, host: T) -> &mut Self {
        self.host = host.into();
        self
    }
    /// Set the endpoint field
    pub fn endpoint<T: Into<String>>(&mut self, endpoint: T) -> &mut Self {
        self.endpoint = endpoint.into();
        self
    }
    /// Set the api_key field
    pub fn api_key<T: Into<String>>(&mut self, api_key: T) -> &mut Self {
        self.api_key = Some(api_key.into());
        self
    }
    /// Set the params field
    pub fn params<T: Into<Params>>(&mut self, params: T) -> &mut Self {
        self.params = Some(params.into());
        self
    }
    /// Build a RequestTemplate using the current builder
    pub fn build(&mut self) -> Result<RequestTemplate, TemplateError> {
        Ok(RequestTemplate {
            method: self.method.clone(),
            charset: self.charset.clone(),
            content: self.content.clone(),
            encoding: self.encoding.clone(),
            schema: self.schema.clone(),
            host: self.host.clone(),
            endpoint: self.endpoint.clone(),
            params: self.params.clone()
                .ok_or(TemplateError::RequiredField("params is required in a TemplateBuilder".into()))?,
            api_key: self.api_key.clone()
                .ok_or(TemplateError::RequiredField("api_key is required in a TemplateBuilder".to_string()))?,
        })
    }
}

impl Encoding {
    fn set_builder_encoding<'a>(&self, builder: &'a mut RequestBuilder) -> &'a mut RequestBuilder {
        use crate::request::Encoding::*;

        match self {
            GzipJson(_) => builder.header(CONTENT_ENCODING, "gzip"),
            Json => builder,
        }
    }
}

/// Represents HTTP vs HTTPS for requests
#[derive(Debug, Clone)]
pub enum Schema {
    Http,
    Https,
}

impl Schema {
    fn to_string(&self) -> String {
        use crate::request::Schema::*;

        match self {
            Http => "http://".to_string(),
            Https => "https://".to_string(),
        }
    }
}