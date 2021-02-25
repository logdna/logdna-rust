use std::time::Duration;

use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
pub use hyper::{body, client::Builder as HyperBuilder, Client as HyperClient};
use hyper_rustls::HttpsConnector;
use tokio::time::timeout;

use crate::body::IngestBodyBuffer;
use crate::error::HttpError;
use crate::request::RequestTemplate;
use crate::response::{IngestResponse, Response};

/// Client for sending IngestRequests to LogDNA
pub struct Client {
    hyper: HyperClient<HttpsConnector<HttpConnector<GaiResolver>>, IngestBodyBuffer>,
    template: RequestTemplate,
    timeout: Duration,
}

impl Client {
    /// Create a new client taking a RequestTemplate and Tokio Runtime
    ///
    /// #  Example
    ///
    /// ```rust
    /// # use logdna_client::client::Client;
    /// # use tokio::runtime::Runtime;
    /// # use logdna_client::params::{Params, Tags};
    /// # use logdna_client::request::RequestTemplate;
    ///
    /// let mut rt = Runtime::new().expect("Runtime::new()");
    /// let params = Params::builder()
    ///     .hostname("rust-client-test")
    ///     .tags(Tags::parse("this,is,a,test"))
    ///     .build()
    ///     .expect("Params::builder()");
    /// let request_template = RequestTemplate::builder()
    ///     .params(params)
    ///     .api_key("<your ingestion key>")
    ///     .build()
    ///     .expect("RequestTemplate::builder()");
    /// let client = Client::new(request_template);
    /// ```
    pub fn new(template: RequestTemplate) -> Self {
        let http_connector = {
            let mut connector = HttpConnector::new_with_resolver(GaiResolver::new());
            connector.enforce_http(false); // this is needed or https:// urls will error
            connector
        };

        let mut tls = rustls::ClientConfig::new();
        tls.root_store =
            rustls_native_certs::load_native_certs().expect("could not load platform certs");
        let https_connector = hyper_rustls::HttpsConnector::from((http_connector, tls));

        Client {
            hyper: HyperClient::builder()
                .pool_max_idle_per_host(20)
                .build(https_connector),
            template,
            timeout: Duration::from_secs(5),
        }
    }
    /// Sets the request timeout
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout
    }
    /// Send an IngestBody to the LogDNA Ingest API
    ///
    /// Returns an IngestResponse, which is a future that must be run on the Tokio Runtime
    pub async fn send<T>(&self, body: T) -> IngestResponse
    where
        T: crate::body::IntoIngestBodyBuffer + Send + Sync,
        T::Error: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        let body = body
            .into()
            .await
            .map_err(move |e| HttpError::Other(Box::new(e)))?;

        let counts = countme::get::<
            crate::segmented_buffer::SegmentedBuf<async_buf_pool::Reusable<bytes::BytesMut>>,
        >();
        log::debug!(
            "live: {}, max_live: {}, total: {}",
            counts.live,
            counts.max_live,
            counts.total
        );

        let request = self.template.new_request(&body).await?;
        let timeout = timeout(self.timeout, self.hyper.request(request));

        let result = match timeout.await {
            Ok(result) => result,
            Err(_) => {
                return Err(HttpError::Timeout(body));
            }
        };

        let response = match result {
            Ok(response) => response,
            Err(e) => {
                return Err(HttpError::Send(body, e));
            }
        };

        let counts = countme::get::<
            crate::segmented_buffer::SegmentedBuf<async_buf_pool::Reusable<bytes::BytesMut>>,
        >();
        log::debug!(
            "live: {}, max_live: {}, total: {}",
            counts.live,
            counts.max_live,
            counts.total
        );

        let status_code = response.status();
        let status = status_code.as_u16();
        if !(200..300).contains(&status) {
            let body_bytes = body::to_bytes(response.into_body()).await?;
            Ok(Response::Failed(
                Box::new(body),
                status_code,
                std::str::from_utf8(&body_bytes)?.to_string(),
            ))
        } else {
            Ok(Response::Sent)
        }
    }
}
