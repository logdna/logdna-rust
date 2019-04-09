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
extern crate derive_builder;
#[macro_use]
extern crate quick_error;

pub mod body;
pub mod client;
pub mod config;
pub mod error;
pub mod params;
pub mod request;
pub mod response;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
