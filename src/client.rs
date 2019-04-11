use std::sync::Arc;

use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use hyper::Client as HyperClient;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use rustls::ClientConfig as TlsConfig;
use tokio::runtime::Runtime;

use crate::body::IngestBody;
use crate::error::ResponseError;
use crate::request::RequestTemplate;
use crate::response::{IngestResponse, Response};

/// Client for sending IngestRequests to LogDNA
pub struct Client {
    hyper: Arc<HyperClient<HttpsConnector<HttpConnector>>>,
    template: RequestTemplate,
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
    /// let client = Client::new(request_template, &mut rt);
    /// ```
    pub fn new(template: RequestTemplate, runtime: &mut Runtime) -> Self {
        let exec = runtime.executor();
        let reactor = runtime.reactor().clone();

        let http_connector = {
            let mut connector = HttpConnector::new_with_executor(
                exec, Some(reactor),
            );
            connector.enforce_http(false); // this is need or https:// urls will error
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
            hyper: Arc::new(HyperClient::builder().build(https_connector)),
            template,
        }
    }

    /// Send an IngestBody to the LogDNA Ingest API
    ///
    /// Returns an IngestResponse, which is a future that must be run on the Tokio Runtime
    pub fn send(&self, body: IngestBody) -> IngestResponse {
        let hyper = self.hyper.clone();
        Box::new(
            self.template.new_request(body)
                .map_err(ResponseError::from)
                .and_then(move |req| hyper.request(req).map_err(Into::into))
                .and_then(|res| {
                    let status = res.status();
                    res.into_body()
                        .map_err(Into::into)
                        .fold(Vec::new(), |mut vec, chunk| {
                            vec.extend_from_slice(&*chunk);
                            future::ok::<_, ResponseError>(vec)
                        })
                        .and_then(|body| String::from_utf8(body).map_err(Into::into))
                        .map(move |reason| (status, reason))
                })
                .map(|(status, reason)| {
                    println!("{},{}", status, reason);
                    if status != 200 {
                        Response::Failed(status, reason)
                    } else {
                        Response::Sent
                    }
                })
        )
    }
}