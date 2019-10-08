use std::ops::Deref;

use futures::Future;
use http::StatusCode;

use crate::body::IngestBody;
use crate::error::HttpError;
use serde::Serialize;

/// A response from the LogDNA Ingest API
#[derive(Debug, PartialEq)]
pub enum Response<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    Sent,
    // contains the failed body, a status code and a reason the request failed(String)
    Failed(T, StatusCode, String),
}

/// Type alias for a response from `Client::send`
pub type IngestResponse<T> = Box<dyn Future<Item=Response<T>, Error=HttpError<T>> + Send + 'static>;