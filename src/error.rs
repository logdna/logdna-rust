use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::ops::Deref;

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

pub enum HttpError<T: AsRef<IngestBody>> {
    Build(RequestError),
    Send(T, hyper::error::Error),
    Timeout(T),
    Hyper(hyper::error::Error),
    Utf8(std::string::FromUtf8Error),
}

impl<T: AsRef<IngestBody>> From<RequestError> for HttpError<T>
{
    fn from(e: RequestError) -> HttpError<T> {
        HttpError::Build(e)
    }
}

impl<T: AsRef<IngestBody>> From<hyper::error::Error> for HttpError<T> {
    fn from(e: hyper::error::Error) -> HttpError<T> {
        HttpError::Hyper(e)
    }
}

impl<T: AsRef<IngestBody>> From<std::string::FromUtf8Error> for HttpError<T> {
    fn from(e: std::string::FromUtf8Error) -> HttpError<T> {
        HttpError::Utf8(e)
    }
}

impl<T: AsRef<IngestBody>> Display for HttpError<T> {
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

impl<T: AsRef<IngestBody>> Debug for HttpError<T> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        Display::fmt(self, f)
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