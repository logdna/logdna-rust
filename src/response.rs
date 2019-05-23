use std::sync::Arc;

use futures::Future;
use http::StatusCode;

use crate::body::IngestBody;
use crate::error::HttpError;

/// A response from the LogDNA Ingest API
#[derive(Debug, PartialEq)]
pub enum Response {
    Sent,
    // contains the failed body, a status code and a reason the request failed(String)
    Failed(Arc<IngestBody>, StatusCode, String),
}

/// Type alias for a response from `Client::send`
pub type IngestResponse = Box<Future<Item=Response, Error=HttpError> + Send + 'static>;