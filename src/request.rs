use std::convert::{Into, TryInto};
use std::sync::Arc;

use async_compression::futures::write::GzipEncoder;
use async_compression::Level;
use derivative::Derivative;
use futures::io::AsyncWriteExt;
use http::header::HeaderValue;
use http::header::ACCEPT_CHARSET;
use http::header::CONTENT_ENCODING;
use http::header::CONTENT_TYPE;
use http::header::USER_AGENT;
use http::request::Builder as RequestBuilder;
use http::Method;
use hyper::Request;
use time::OffsetDateTime;

use crate::error::{RequestError, TemplateError};
use crate::params::Params;
use crate::segmented_buffer::{AllocBufferFn, Buffer};

const SERIALIZATION_BUF_SEGMENT_SIZE: usize = 1024 * 16;

const SERIALIZATION_BUF_RESERVE_SEGMENTS: usize = 100;

const SERIALIZATION_BUF_INITIAL_CAPACITY: usize = 1024 * 64 / SERIALIZATION_BUF_SEGMENT_SIZE;

/// A reusable template to generate requests from
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RequestTemplate {
    #[derivative(Debug = "ignore")]
    pool: async_buf_pool::Pool<AllocBufferFn, Buffer>,
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
    pub async fn new_request(
        &self,
        body: &crate::body::IngestBodyBuffer,
    ) -> Result<Request<crate::body::IngestBodyBuffer>, RequestError> {
        let builder = RequestBuilder::new();

        let params = serde_urlencoded::to_string(
            self.params
                .clone()
                .set_now(OffsetDateTime::now_utc().unix_timestamp()),
        )
        .expect("cant'fail!");

        let mut builder = builder
            .method(self.method.clone())
            .header(ACCEPT_CHARSET, self.charset.clone())
            .header(CONTENT_TYPE, self.content.clone())
            .header(USER_AGENT, self.user_agent.clone())
            .header("apiKey", self.api_key.clone())
            .uri(self.schema.to_string() + &self.host + &self.endpoint + "?" + &params);

        self.encoding.set_builder_encoding(&mut builder);

        match &self.encoding {
            Encoding::GzipJson(level) => {
                let buf = crate::segmented_buffer::SegmentedPoolBufBuilder::new()
                    .segment_size(SERIALIZATION_BUF_SEGMENT_SIZE)
                    .initial_capacity(SERIALIZATION_BUF_SEGMENT_SIZE)
                    .with_pool(self.pool.clone());

                let mut encoder = GzipEncoder::with_quality(buf, *level);

                let _written = futures::io::copy_buf(body.reader(), &mut encoder)
                    .await
                    .map_err(RequestError::BuildIo)?;
                encoder.close().await?;

                let body: crate::body::IngestBodyBuffer =
                    crate::body::IngestBodyBuffer::from_buffer(encoder.into_inner());

                Ok(builder.body(body)?)
            }
            Encoding::Json => Ok(builder.body(body.clone())?),
        }
    }
}

#[test]
fn test_builder() {}

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
    GzipJson(Level),
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
            encoding: Encoding::GzipJson(Level::Precise(2)),
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
        T: TryInto<HeaderValue, Error = http::header::InvalidHeaderValue>,
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
        T: TryInto<HeaderValue, Error = http::header::InvalidHeaderValue>,
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
        T: TryInto<HeaderValue, Error = http::header::InvalidHeaderValue>,
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
        if let Some(e) = self.err.take() {
            return Err(e);
        };
        Ok(RequestTemplate {
            pool: async_buf_pool::Pool::<AllocBufferFn, Buffer>::with_max_reserve(
                SERIALIZATION_BUF_INITIAL_CAPACITY,
                SERIALIZATION_BUF_RESERVE_SEGMENTS,
                Arc::new(|| {
                    Buffer::new(bytes::BytesMut::with_capacity(
                        SERIALIZATION_BUF_SEGMENT_SIZE,
                    ))
                }),
            )
            .unwrap(),
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::body::test::line_st;
    use crate::body::{IngestBody, IngestBodyBuffer, IntoIngestBodyBuffer};
    use proptest::prelude::*;

    use flate2::read::GzDecoder;

    proptest! {
        #[test]
        fn request_template_body_round_trip(lines in proptest::collection::vec(line_st(), 5)) {
            use bytes::buf::Buf;
            use std::io::Read;
            let params = Params::builder()
                .hostname("rust-client-test")
                .build()
                .expect("Params::builder()");

            let mut request_template_builder = RequestTemplate::builder();
            let request_template = request_template_builder.params(params).api_key("12345").build().unwrap();

            let ingest_body = IngestBody::new(lines);
            let serde_serialized = serde_json::to_string(&ingest_body).unwrap();

            let body: IngestBodyBuffer = tokio_test::block_on(IntoIngestBodyBuffer::into(&ingest_body)).unwrap();

            let mut request = tokio_test::block_on(request_template.new_request(&body)).unwrap();
            let req_body_bytes= tokio_test::block_on( hyper::body::to_bytes(request.body_mut())).unwrap();

            let mut d = GzDecoder::new(req_body_bytes.reader());

            let mut s = String::new();
            d.read_to_string(&mut s).unwrap();
            assert_eq!(s, serde_serialized);
        }
    }
}
