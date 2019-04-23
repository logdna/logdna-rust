use futures::Future;
use http::StatusCode;

use crate::error::ResponseError;

/// A response from the LogDNA Ingest API
#[derive(Debug, PartialEq)]
pub enum Response {
    Sent,
    // contains a status code and a reason (String)
    Failed(StatusCode, String),
}

/// Type alias for a response from `Client::send`
pub type IngestResponse = Box<Future<Item=Response, Error=ResponseError> + Send + 'static>;