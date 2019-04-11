use futures::Future;
use http::StatusCode;

use crate::error::ResponseError;

#[derive(Debug)]
pub enum Response {
    Sent,
    Failed(StatusCode, String),
}

pub type IngestResponse = Box<Future<Item=Response, Error=ResponseError> + Send + 'static>;