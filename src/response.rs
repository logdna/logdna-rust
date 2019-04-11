use futures::Future;
use http::StatusCode;

use crate::error::ResponseError;

/// A response from the LogDNA Ingest API
#[derive(Debug)]
pub enum Response {
    Sent,
    Failed(StatusCode, String),
}

/// Type alias for a response from `Client::send`
pub type IngestResponse = Box<Future<Item=Response, Error=ResponseError> + Send + 'static>;