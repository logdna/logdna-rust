use std::sync::Arc;

use hyper::Client as HyperClient;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use rustls::ClientConfig as TlsConfig;
use tokio::reactor::Handle;
use tokio::runtime::TaskExecutor;

use crate::config::ClientConfig;

pub struct Client {
    hyper: Arc<HyperClient<HttpsConnector<HttpConnector>>>,
    config: ClientConfig,
}

impl Client {
    pub fn new(exec: TaskExecutor, reactor: Handle) -> Self {
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
            config: ClientConfig,
        }
    }

    /// construct a future that represents a request to the logdna ingest api
    pub fn send(&self, body: IngestBody) -> IngestResponse {}
}