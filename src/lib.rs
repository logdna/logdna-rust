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

use std::sync::Arc;

use flate2::Compression;
use flate2::write::GzEncoder;
use futures::future::{self, Err, Future, IntoFuture};
use futures::stream::Stream;
use http::header::{ACCEPT_CHARSET, CONTENT_ENCODING};
use http::Method;
use http::request::Builder;
use hyper::{Client as HyperClient, Request, StatusCode};
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use rustls::ClientConfig as TlsConfig;
use serde::{Deserialize, Serialize};
use serde_json::ser::Serializer;
use tokio::reactor::Handle;
use tokio::runtime::TaskExecutor;

use crate::error::Error;

/// Contains all the errors types for this crate
pub mod error;
pub mod config;

pub type IngestResponse = Box<Future<Item=Response, Error=Error> + Send + 'static>;

/// Provides an HTTP(s) client for communicating with logdnas ingest api
pub struct Client {
    hyper: Arc<HyperClient<HttpsConnector<HttpConnector>>>,
//    config: ClientConfig,
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
        }
    }

    /// construct a future that represents a request to the logdna ingest api
    pub fn send(&self, body: IngestBody) -> IngestResponse {
        let hyper = self.hyper.clone(); // get a new ref to the hyper client

        let req = {
            self.build_template(self.new_template(), body)
                .and_then(move |req| hyper.request(req).map_err(Into::into))
                .and_then(move |res| {
                    let status = res.status();
                    res.into_body()
                        .map_err(Error::Hyper)
                        .fold(Vec::new(), |mut vec, chunk| {
                            vec.extend_from_slice(&*chunk);
                            future::ok::<_, Error>(vec)
                        })
                        .and_then(move |bytes| String::from_utf8(bytes).map_err(Into::into))
                        .map(move |reason| {
                            if status != 200 {
                                Response::Failed(status, reason)
                            } else {
                                Response::Sent
                            }
                        })
                })
        };

        Box::new(req)
    }

    fn new_template(&self) -> Builder {
        let mut b = Request::builder();
        b.method(Method::POST)
            .header(ACCEPT_CHARSET, "utf8")
            .header(CONTENT_ENCODING, "gzip")
            .header("apiKey", "");
        b
    }

    fn build_template(&self, mut template: Builder, body: IngestBody) -> Box<Future<Item=Request<Body>, Error=Error> + Send> {
        if true {
            Box::new(
                future::ok(GzEncoder::new(Vec::new(), Compression::default()))
                    .and_then(move |mut encoder| {
                        serde_json::to_writer(&mut encoder, &body)
                            .map(|_| encoder)
                            .map_err(Error::Json)
                    })
                    .and_then(|encoder| encoder.finish().map_err(Into::into))
                    .and_then(move |vec| template.body(Body::from(vec)).map_err(Into::into))
            )
        } else {
            Box::new(
                serde_json::to_vec(&body)
                    .map_err(Error::Json)
                    .into_future()
                    .and_then(move |vec| template.body(Body::from(vec)).map_err(Into::into))
            )
        }
    }
}

#[derive(Serialize, Deserialize)]
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
