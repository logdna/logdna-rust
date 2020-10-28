use chrono::Utc;
use flate2::Compression;
use http::header::HeaderValue;
use http::header::ACCEPT_CHARSET;
use http::header::CONTENT_ENCODING;
use http::header::CONTENT_TYPE;
use http::header::USER_AGENT;
use http::request::Builder as RequestBuilder;
use http::Method;
use hyper::{Body, Request};

use std::convert::{Into, TryInto};

use crate::body::IngestBody;
use crate::error::{RequestError, TemplateError};
use crate::params::Params;

/// A reusable template to generate requests from
#[derive(Debug)]
pub struct RequestTemplate {
    /// HTTP method, default is POST
    pub method: Method,
    /// Content charset, default is utf8
    pub charset: HeaderValue,
    /// Content type, default is application/json
    pub content: HeaderValue,
    /// User agent header
    pub user_agent: HeaderValue,
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
    pub fn new_request(&self, body: &IngestBody) -> Result<Request<Body>, RequestError> {
        let builder = RequestBuilder::new();

        let params =
            serde_urlencoded::to_string(self.params.clone().set_now(Utc::now().timestamp()))
                .expect("cant'fail!");

        let mut builder = builder
            .method(self.method.clone())
            .header(ACCEPT_CHARSET, self.charset.clone())
            .header(CONTENT_TYPE, self.content.clone())
            .header(USER_AGENT, self.user_agent.clone())
            .header("apiKey", self.api_key.clone())
            .uri(self.schema.to_string() + &self.host + &self.endpoint + "?" + &params);

        self.encoding.set_builder_encoding(&mut builder);
        let body = body.as_http_body(&self.encoding)?;

        Ok(builder.body(body)?)
    }
}

/// Used to build an instance of a RequestTemplate
pub struct TemplateBuilder {
    method: Method,
    charset: HeaderValue,
    content: HeaderValue,
    user_agent: HeaderValue,
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
            user_agent: HeaderValue::from_static(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            )),
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
    where
        T: TryInto<HeaderValue, Error = http::Error>,
    {
        self.charset = match charset.try_into() {
            Ok(v) => v,
            Err(e) => {
                self.err = Some(TemplateError::InvalidHeader(e));
                return self;
            }
        };
        self
    }
    /// Set the content field
    pub fn content<T>(&mut self, content: T) -> &mut Self
    where
        T: TryInto<HeaderValue, Error = http::Error>,
    {
        self.content = match content.try_into() {
            Ok(v) => v,
            Err(e) => {
                self.err = Some(TemplateError::InvalidHeader(e));
                return self;
            }
        };
        self
    }
    /// Set the user-agent field
    pub fn user_agent<T>(&mut self, user_agent: T) -> &mut Self
    where
        T: TryInto<HeaderValue, Error = http::Error>,
    {
        self.user_agent = match user_agent.try_into() {
            Ok(v) => v,
            Err(e) => {
                self.err = Some(TemplateError::InvalidHeader(e));
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
            user_agent: self.user_agent.clone(),
            encoding: self.encoding.clone(),
            schema: self.schema.clone(),
            host: self.host.clone(),
            endpoint: self.endpoint.clone(),
            params: self.params.clone().ok_or_else(|| {
                TemplateError::RequiredField("params is required in a TemplateBuilder".into())
            })?,
            api_key: self.api_key.clone().ok_or_else(|| {
                TemplateError::RequiredField("api_key is required in a TemplateBuilder".to_string())
            })?,
        })
    }
}

impl Default for TemplateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoding {
    fn set_builder_encoding<'a>(&self, builder: &'a mut RequestBuilder) -> &'a mut RequestBuilder {
        use crate::request::Encoding::*;
        {
            let headers = builder.headers_mut().unwrap();

            match self {
                GzipJson(_) => {
                    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                    builder
                }
                Json => builder,
            }
        }
    }
}

/// Represents HTTP vs HTTPS for requests
#[derive(Debug, Clone)]
pub enum Schema {
    Http,
    Https,
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::request::Schema::*;

        match self {
            Http => write!(f, "http://"),
            Https => write!(f, "https://"),
        }
    }
}
