use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use futures::{future, Stream};
use futures::future::Future;
pub use hyper::{Client as HyperClient, client::Builder as HyperBuilder};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use rustls::ClientConfig as TlsConfig;
use tokio::timer::Timeout;

use crate::body::IngestBody;
use crate::error::HttpError;
use crate::request::RequestTemplate;
use crate::response::{IngestResponse, Response};
use hyper::client::connect::dns::TokioThreadpoolGaiResolver;

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

        let tls_config = {
            let mut cfg = TlsConfig::new();
            cfg.root_store.add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
            cfg.ct_logs = Some(&ct_logs::LOGS);
            cfg
        };

        let https_connector = HttpsConnector::from((http_connector, tls_config));

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
    pub fn send<T>(&self, body: T) -> IngestResponse<T>
        where T: Deref<Target=IngestBody> + Send + 'static,
              T: Clone,
    {
        let hyper = self.hyper.clone();
        let tmp_body = body.clone();
        let tmp_body1 = body.clone();
        let timeout = self.timeout.clone();
        Box::new(
            self.template.new_request(body.clone())
                .map_err(HttpError::from)
                .and_then(move |req|
                    Timeout::new(
                        hyper.request(req)
                            .map_err(move |e| HttpError::Send(body, e)),
                        timeout,
                    ).map_err(move |e| {
                        match e.into_inner() {
                            Some(e) => e,
                            None => HttpError::Timeout(tmp_body),
                        }
                    })
                )
                .and_then(|res| {
                    let status = res.status();
                    res.into_body()
                        .map_err(Into::into)
                        .fold(Vec::new(), |mut vec, chunk| {
                            vec.extend_from_slice(&*chunk);
                            future::ok::<_, HttpError<T>>(vec)
                        })
                        .and_then(|body| String::from_utf8(body).map_err(Into::into))
                        .map(move |reason| (status, reason))
                })
                .map(move |(status, reason)| {
                    if status.as_u16() < 200 || status.as_u16() >= 300 {
                        Response::Failed(tmp_body1, status, reason)
                    } else {
                        Response::Sent
                    }
                })
        )
    }
}