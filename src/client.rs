use std::time::Duration;

use http_body_util::BodyExt;
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use rustls::client::ClientConfig as TlsClientConfig;
use tokio::time::timeout;

use crate::body::IngestBodyBuffer;
use crate::dns::HickoryDnsResolver;
use crate::error::HttpError;
use crate::request::RequestTemplate;
use crate::response::{IngestResponse, Response};

/// Client for sending IngestRequests to LogDNA
pub struct Client {
    hyper: HyperClient<HttpsConnector<HttpConnector<HickoryDnsResolver>>, IngestBodyBuffer>,
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
    pub fn new(
        template: RequestTemplate,
        require_tls: Option<bool>,
    ) -> Result<Self, std::io::Error> {
        let dns_resolver =
            HickoryDnsResolver::new().expect("Could not read system DNS configuration");
        let http_connector = {
            let mut connector = HttpConnector::new_with_resolver(dns_resolver);
            connector.enforce_http(false); // this is needed or https:// urls will error
            connector.set_reuse_address(true);
            connector.set_keepalive(Some(std::time::Duration::from_secs(120)));
            connector
        };

        let tls_config: rustls::ConfigBuilder<_, _> =
            TlsClientConfig::builder().with_native_roots()?;
        let tls_config = tls_config.with_no_client_auth();

        let https_connector_builder =
            hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(tls_config);
        let https_connector_builder = if require_tls.unwrap_or(true) {
            https_connector_builder.https_only()
        } else {
            https_connector_builder.https_or_http()
        };
        let https_connector_builder = https_connector_builder.enable_http1().enable_http2();

        let https_connector = https_connector_builder.wrap_connector(http_connector);

        Ok(Client {
            hyper: HyperClient::builder(hyper_util::rt::TokioExecutor::new())
                .pool_max_idle_per_host(20)
                .build(https_connector),
            template,
            timeout: Duration::from_secs(5),
        })
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
            crate::segmented_buffer::SegmentedBuf<
                async_buf_pool::Reusable<crate::segmented_buffer::Buffer>,
            >,
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
            crate::segmented_buffer::SegmentedBuf<
                async_buf_pool::Reusable<crate::segmented_buffer::Buffer>,
            >,
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
            let body_bytes = response.into_body().collect().await?.to_bytes();
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
