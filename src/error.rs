use std::fmt::{Debug, Display, Error as FmtError, Formatter};

use serde::Serialize;

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

pub enum HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    Build(RequestError),
    Send(T, hyper::error::Error),
    Timeout(T),
    Hyper(hyper::error::Error),
    Utf8(std::str::Utf8Error),
    FromUtf8(std::string::FromUtf8Error),
}

impl<T> From<RequestError> for HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    fn from(e: RequestError) -> HttpError<T> {
        HttpError::Build(e)
    }
}

impl<T> From<hyper::error::Error> for HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    fn from(e: hyper::error::Error) -> HttpError<T> {
        HttpError::Hyper(e)
    }
}

impl<T> From<std::string::FromUtf8Error> for HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    fn from(e: std::string::FromUtf8Error) -> HttpError<T> {
        HttpError::FromUtf8(e)
    }
}

impl<T> From<std::str::Utf8Error> for HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    fn from(e: std::str::Utf8Error) -> HttpError<T> {
        HttpError::Utf8(e)
    }
}

impl<T> Display for HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            HttpError::Send(_, ref e) => write!(f, "{}", e),
            HttpError::Timeout(_) => write!(f, "request timed out!"),
            HttpError::Hyper(ref e) => write!(f, "{}", e),
            HttpError::Build(ref e) => write!(f, "{}", e),
            HttpError::Utf8(ref e) => write!(f, "{}", e),
            HttpError::FromUtf8(ref e) => write!(f, "{}", e),
        }
    }
}

impl<T> Debug for HttpError<T>
    where T: Serialize + Send + 'static,
          T: Clone,
{
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
