use futures::Future;
use http::StatusCode;

use crate::error::Error;

pub enum Response {
    Sent,
    Failed(StatusCode, String),
}

pub type IngestResponse = Box<Future<Item=Response, Error=Error> + Send + 'static>;