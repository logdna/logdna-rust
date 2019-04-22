//#![warn(missing_docs)]

//! A client library for communicating with [LogDNA]'s [Ingest API]
//!
//! This crate heavily relies on [Hyper] and [Tokio] for it's operation.
//! It is strongly recommend to read their respective docs for advanced usage of this crate.
//!
//! # Overview
//! The general flow is quite simple, first create a new client with [`Client::new`](struct.Client.html#method.new).
//!
//! Then call [`Client::send`](struct.Client.html#method.send) as many times a you would like.
//!
//! [LogDNA]: https://logdna.com/
//! [Ingest API]: https://docs.logdna.com/v1.0/reference#api
//! [Hyper]: https://github.com/hyperium/hyper
//! [Tokio]: https://github.com/tokio-rs/tokio

#[macro_use]
extern crate quick_error;

/// Log line and body types
pub mod body;
/// Http client
pub mod client;
/// Error types
pub mod error;
/// Query parameters
pub mod params;
/// Request types
pub mod request;
/// Response types
pub mod response;

#[cfg(test)]
mod tests {
    use std::env;

    use tokio::runtime::Runtime;

    use crate::body::{IngestBody, Labels, Line};
    use crate::client::Client;
    use crate::params::{Params, Tags};
    use crate::request::RequestTemplate;

    #[test]
    fn it_works() {
        let mut rt = Runtime::new().expect("Runtime::new()");
        let params = Params::builder()
            .hostname("rust-client-test")
            .ip("127.0.0.1")
            .tags(Tags::parse("this,is,a,test"))
            .build().expect("Params::builder()");
        let request_template = RequestTemplate::builder()
            .host("logs-k8s.logdna.com")
            .params(params)
            .api_key(env::var("API_KEY").unwrap())
            .build().expect("RequestTemplate::builder()");
        let client = Client::new(request_template, &mut rt);
        let labels = Labels::new()
            .add("app", "test")
            .add("workload", "test");
        let line = Line::builder()
            .line("this is a test")
            .app("rust-client")
            .level("INFO")
            .labels(labels)
            .build().expect("Line::builder()");
        println!("{}", serde_json::to_string(&IngestBody::new(vec![line.clone()])).unwrap());
        println!("{:?}",
                 rt.block_on(
                     client.send(IngestBody::new(vec![line]))
                 ).unwrap()
        )
    }
}
