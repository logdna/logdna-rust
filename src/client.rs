use std::sync::Arc;
use std::time::Duration;

pub use hyper::{Client as HyperClient, client::Builder as HyperBuilder};
use hyper::client::connect::dns::TokioThreadpoolGaiResolver;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use tokio::timer::Timeout;

use crate::body::IngestBody;
use crate::error::HttpError;
use crate::request::RequestTemplate;
use crate::response::{IngestResponse, Response};

/// Client for sending IngestRequests to LogDNA
pub struct Client {
    hyper: Arc<HyperClient<HttpsConnector<HttpConnector<TokioThreadpoolGaiResolver>>>>,
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
            let mut connector = HttpConnector::new_with_tokio_threadpool_resolver();
            connector.enforce_http(false); // this is needed or https:// urls will error
            connector
        };

        let tls = native_tls::TlsConnector::new().expect("TlsConnector::new()");
        let https_connector = HttpsConnector::from((http_connector, tls.into()));

        Client {
            hyper: Arc::new(
                HyperClient::builder()
                    .max_idle_per_host(20)
                    .build(https_connector)
            ),
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
    pub async fn send<T: AsRef<IngestBody>>(&self, body: T) -> IngestResponse<T> {
        let request = self.template.new_request(body.as_ref())?;
        let timeout = Timeout::new(
            self.hyper.request(request),
            self.timeout,
        );

        let result = match timeout.await {
            Ok(result) => result,
            Err(_) => {
                return Err(HttpError::Timeout(body));
            }
        };

        let mut response = match result {
            Ok(response) => response,
            Err(e) => {
                return Err(HttpError::Send(body, e));
            }
        };

        let status = response.status().as_u16();
        if status < 200 || status >= 300 {
            let mut response_body = Vec::new();
            while let Some(chunk) = response.body_mut().next().await {
                if let Ok(chunk) = chunk {
                    response_body.extend_from_slice(&chunk)
                }
            };
            Ok(Response::Failed(body, response.status(), String::from_utf8(response_body)?))
        } else {
            Ok(Response::Sent)
        }
    }
}