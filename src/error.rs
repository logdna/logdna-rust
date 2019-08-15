use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::sync::Arc;

use crate::body::IngestBody;

quick_error! {
     #[derive(Debug)]
     pub enum RequestError {
        Build(err: http::Error) {
             from()
        }
        Body(err: BodyError) {
             from()
        }
     }
}

pub enum HttpError {
    Build(RequestError),
    Send(Arc<IngestBody>, hyper::error::Error),
    Timeout(Arc<IngestBody>),
    Hyper(hyper::error::Error),
    Utf8(std::string::FromUtf8Error),
}

impl From<RequestError> for HttpError {
    fn from(e: RequestError) -> HttpError {
        HttpError::Build(e)
    }
}

impl From<hyper::error::Error> for HttpError {
    fn from(e: hyper::error::Error) -> HttpError {
        HttpError::Hyper(e)
    }
}

impl From<std::string::FromUtf8Error> for HttpError {
    fn from(e: std::string::FromUtf8Error) -> HttpError {
        HttpError::Utf8(e)
    }
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            HttpError::Send(_, ref e) => { write!(f, "{}", e) }
            HttpError::Timeout(_) => { write!(f, "request timed out!") }
            HttpError::Hyper(ref e) => { write!(f, "{}", e) }
            HttpError::Build(ref e) => { write!(f, "{}", e) }
            HttpError::Utf8(ref e) => { write!(f, "{}", e) }
        }
    }
}

impl Debug for HttpError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            HttpError::Send(_, ref e) => { write!(f, "{}", e) }
            HttpError::Timeout(_) => { write!(f, "request timed out!") }
            HttpError::Hyper(ref e) => { write!(f, "{}", e) }
            HttpError::Build(ref e) => { write!(f, "{}", e) }
            HttpError::Utf8(ref e) => { write!(f, "{}", e) }
        }
    }
}

quick_error! {
     #[derive(Debug)]
     pub enum BodyError {
        Json(err: serde_json::Error) {
             from()
        }
        Gzip(err: std::io::Error) {
             from()
        }
        Canceled(err: futures::sync::oneshot::Canceled) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum TemplateError {
        InvalidHeader(err: http::Error) {
             from()
        }
        RequiredField(err: std::string::String) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum ParamsError {
        RequiredField(err: std::string::String) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum LineError {
        RequiredField(err: std::string::String) {
             from()
        }
     }
}