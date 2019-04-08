//#![warn(missing_docs)]

//! A client library for communicating with [LogDNA]'s [Ingest API]
//!
//! This crate heavily relies on [Hyper] and [Tokio] for it's operation.
//! It is strongly recommend to read their respective docs for advanced usage of this crate.
//!
//! # Overview
//! The general flow is quite simple, first create a new client with [`Client::new`](struct.Client.html#method.new).
//!
//! Then call [`Client::make_request`](struct.Client.html#method.make_request) as many times a you would like.
//!
//! [LogDNA]: https://logdna.com/
//! [Ingest API]: https://docs.logdna.com/v1.0/reference#api
//! [Hyper]: https://github.com/hyperium/hyper
//! [Tokio]: https://github.com/tokio-rs/tokio

#[macro_use]
extern crate quick_error;

use futures::future::{self, Future, IntoFuture};
use http::header::{ACCEPT_CHARSET, CONTENT_ENCODING};
use http::Method;
use hyper::{Client as HyperClient, Request, StatusCode};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use rustls::ClientConfig as TlsConfig;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::reactor::Handle;
use tokio::runtime::TaskExecutor;

/// Contains all the errors types for this crate
pub mod error;
pub mod config;

pub type IngestResponse = Box<Future<Item=Response, Error=()> + Send>;

/// Provides an HTTP(s) client for communicating with logdnas ingest api
pub struct Client {
    hyper: HyperClient<HttpsConnector<HttpConnector>>,
//    config: ClientConfig
}

impl Client {
    /// construct a new HTTP(s) client from a tokio TaskExecutor and tokio reactor Handle
    pub fn new(exec: TaskExecutor, reactor: Handle) -> Self {
        let http_connector = {
            // build an http connector that uses our runtime
            let mut connector = HttpConnector::new_with_executor(
                exec, Some(reactor),
            );
            connector.enforce_http(false); // enable https urls
            connector
        };
        //construct tls config got https
        let tls_config = {
            let mut cfg = TlsConfig::new(); //create a new tls config
            // add trust anchors and ct logs to config
            cfg.root_store.add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
            cfg.ct_logs = Some(&ct_logs::LOGS);
            cfg
        };
        // build https connector from http connector and tls config
        let https_connector = HttpsConnector::from((http_connector, tls_config));

        Client {
            hyper: HyperClient::builder().build(https_connector),
        }
    }

    /// construct a future that represents a request to the logdna ingest api
    pub fn make_request<T: Serialize>(&mut self, ingest_body: T) -> IngestResponse {
        //todo ingestion key
        future::ok(Request::builder()
            .method(Method::POST)
            .header(ACCEPT_CHARSET, "utf8")
            .header(CONTENT_ENCODING, "gzip")
            .header("apiKey", "")
        ).and_then(|builder| {
            serde_json::to_string(&ingest_body)
                .into_future()
                .map_err(|_| ())
                .and_then(move |json| builder.body(json).into_future().map_err(|_| ()))
        });
        unimplemented!()
    }
}

pub struct IngestBody {}

pub enum Response {
    Sent,
    Failed(StatusCode, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
