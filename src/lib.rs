//#![warn(missing_docs)]

//! A client library for communicating with [LogDNA]'s [Ingest API]
//!
//! This crate heavily relies on [Hyper] and [Tokio] for it's operation.
//! It is strongly recommend to read their respective docs for advanced usage of this crate.
//!
//! # Overview
//! The general flow is quite simple, first create a new client with [`Client::builder`](client/struct.Client.html#method.builder).
//!
//! Then call [`Client::send`](client/struct.Client.html#method.send) as many times a you would like.
//!
//! # Example
//! You first need a [Tokio Runtume]
//! ```
//! # use tokio::runtime::Runtime;
//! let mut rt = Runtime::new().expect("Runtime::new()");
//! ```
//!
//! The client requires a request template to generate new requests from
//! ```
//! # use logdna_client::params::Params;
//! # use logdna_client::request::RequestTemplate;
//! # use std::env;
//! let params = Params::builder()
//!     .hostname("rust-client-test")
//!     .ip("127.0.0.1")
//!     .tags("this,is,a,test")
//!     .build()
//!     .expect("Params::builder()");
//!
//! let template = RequestTemplate::builder()
//!     .host("logs.logdna.com")
//!     .params(params)
//!     .api_key("ingestion key goes here")
//!     .build()
//!     .expect("RequestTemplate::builder()");
//! ```
//! Now you have everything to create a client
//! ```
//! # use logdna_client::client::Client;
//! let client = Client::new(request_template);
//! ```
//! To use a client, we need to call [`Client::send`](client/struct.Client.html#method.send)
//!
//! [`Client::send`](client/struct.Client.html#method.send) requires an [`IngestBody`](body/struct.IngestBody.html), so let's create one
//! ```
//! # use logdna_client::body::{Labels, Line, IngestBody};
//! // Lets build a line, note that only line is required
//! let labels = Labels::new()
//!      .add("app", "test")
//!      .add("workload", "test");
//!
//! let line1 = Line::builder()
//!      .line("this is a test")
//!      .app("rust-client")
//!      .level("INFO")
//!      .labels(labels)
//!      .build()
//!      .expect("Line::builder()");
//!
//! let line2 = Line::builder()
//!      .line("this is also a test")
//!      .app("rust-client")
//!      .level("ERROR")
//!      .build()
//!      .expect("Line::builder()");
//!
//! let body = IngestBody::new(vec![line1,line2]);
//! ```
//! Now we just have to send the body using an the client you created above
//! ```
//! # use  logdna_client::body::IngestBody;
//! let response = client.send(IngestBody::new(vec![line]));
//! ```
//! The response needs to be spawned on the runtime you created earlier
//!
//! If the reponse is not polled (spawned on a runtime) nothing will happen
//! ```
//! # use logdna_client::response::Response;
//! assert_eq!(Response::Sent, rt.block_on(response).unwrap())
//! ```
//! [LogDNA]: https://logdna.com/
//! [Ingest API]: https://docs.logdna.com/v1.0/reference#api
//! [Hyper]: https://github.com/hyperium/hyper
//! [Tokio]: https://github.com/tokio-rs/tokio
//! [Tokio Runtume]: https://docs.rs/tokio/latest/tokio/runtime/index.html

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
    use std::sync::Arc;

    use tokio::runtime::Runtime;

    use crate::body::{IngestBody, KeyValueMap, Line};
    use crate::client::Client;
    use crate::params::{Params, Tags};
    use crate::request::RequestTemplate;
    use crate::response::Response;

    #[test]
    fn it_works() {
        let mut rt = Runtime::new().expect("Runtime::new()");
        let params = Params::builder()
            .hostname("rust-client-test")
            .ip("127.0.0.1")
            .tags(Tags::parse("this,is,a,test"))
            .build()
            .expect("Params::builder()");
        let request_template = RequestTemplate::builder()
            .host("logs.logdna.com")
            .params(params)
            .api_key(env::var("API_KEY").expect("api key missing"))
            .build()
            .expect("RequestTemplate::builder()");
        let client = Client::new(request_template);
        let labels = KeyValueMap::new()
            .add("app", "test")
            .add("workload", "test");
        let annotations = KeyValueMap::new()
            .add("app", "test")
            .add("workload", "test");
        let line = Line::builder()
            .line("this is a test")
            .app("rust-client")
            .level("INFO")
            .labels(labels)
            .annotations(annotations)
            .build()
            .expect("Line::builder()");
        println!(
            "{}",
            serde_json::to_string(&IngestBody::new(vec![line.clone()])).unwrap()
        );
        assert_eq!(
            Response::Sent,
            rt.block_on(client.send(IngestBody::new(vec![line])))
                .unwrap()
        )
    }
}
