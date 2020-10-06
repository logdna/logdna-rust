use std::time::Duration;

use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
pub use hyper::{body, client::Builder as HyperBuilder, Client as HyperClient};
use hyper_tls::HttpsConnector;
use tokio::time::timeout;

use crate::body::IngestBody;
use crate::error::HttpError;
use crate::request::RequestTemplate;
use crate::response::{IngestResponse, Response};

/// Client for sending IngestRequests to LogDNA
pub struct Client {
    hyper: HyperClient<HttpsConnector<HttpConnector<GaiResolver>>>,
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

        let tls = native_tls::TlsConnector::new().expect("TlsConnector::new()");
        let https_connector = HttpsConnector::from((http_connector, tls.into()));

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
    pub async fn send<T: AsRef<IngestBody>>(&self, body: T) -> IngestResponse<T> {
        let request = self.template.new_request(body.as_ref())?;
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

        let status_code = response.status();
        let status = status_code.as_u16();
        if status < 200 || status >= 300 {
            let body_bytes = body::to_bytes(response.into_body()).await?;
            Ok(Response::Failed(
                body,
                status_code,
                std::str::from_utf8(&body_bytes)?.to_string(),
            ))
        } else {
            Ok(Response::Sent)
        }
    }
}
