use std::fmt::{Debug, Display, Error as FmtError, Formatter};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("{0}")]
    Build(#[from] http::Error),
    #[error("{0}")]
    BuildIo(#[from] std::io::Error),
    #[error("{0}")]
    Body(#[from] BodyError),
}

#[derive(Debug, Error)]
pub enum IngestBufError {
    #[error("{0}")]
    Any(&'static str),
}

pub enum HttpError<T>
where
    T: Send + 'static,
{
    Build(RequestError),
    Send(T, hyper::Error),
    Timeout(T),
    Hyper(hyper::Error),
    Utf8(std::str::Utf8Error),
    FromUtf8(std::string::FromUtf8Error),
    Serialization(serde_json::Error),
    Other(Box<dyn std::error::Error>),
}

impl<T> From<RequestError> for HttpError<T>
where
    T: Send + 'static,
{
    fn from(e: RequestError) -> HttpError<T> {
        HttpError::Build(e)
    }
}

impl<T> From<hyper::Error> for HttpError<T>
where
    T: Send + 'static,
{
    fn from(e: hyper::Error) -> HttpError<T> {
        HttpError::Hyper(e)
    }
}

impl<T> From<std::string::FromUtf8Error> for HttpError<T>
where
    T: Send + 'static,
{
    fn from(e: std::string::FromUtf8Error) -> HttpError<T> {
        HttpError::FromUtf8(e)
    }
}

impl<T> From<std::str::Utf8Error> for HttpError<T>
where
    T: Send + 'static,
{
    fn from(e: std::str::Utf8Error) -> HttpError<T> {
        HttpError::Utf8(e)
    }
}

impl<T> From<serde_json::Error> for HttpError<T>
where
    T: Send + 'static,
{
    fn from(e: serde_json::Error) -> HttpError<T> {
        HttpError::Serialization(e)
    }
}

impl<T> Display for HttpError<T>
where
    T: Send + 'static,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            HttpError::Send(_, ref e) => write!(f, "{}", e),
            HttpError::Timeout(_) => write!(f, "request timed out!"),
            HttpError::Hyper(ref e) => write!(f, "{}", e),
            HttpError::Build(ref e) => write!(f, "{}", e),
            HttpError::Utf8(ref e) => write!(f, "{}", e),
            HttpError::FromUtf8(ref e) => write!(f, "{}", e),
            HttpError::Serialization(ref e) => write!(f, "{}", e),
            HttpError::Other(ref e) => write!(f, "{}", e),
        }
    }
}

impl<T> Debug for HttpError<T>
where
    T: Send + 'static,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        Display::fmt(self, f)
    }
}

#[derive(Debug, Error)]
pub enum BodyError {
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Gzip(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum TemplateError {
    #[error("{0}")]
    InvalidHeader(#[from] http::header::InvalidHeaderValue),
    #[error("{0}")]
    RequiredField(std::string::String),
}

#[derive(Debug, Error)]
pub enum ParamsError {
    #[error("{0}")]
    RequiredField(std::string::String),
}

#[derive(Debug, Error)]
pub enum LineError {
    #[error("{0}")]
    RequiredField(std::string::String),
}

#[derive(Debug, Error)]
pub enum LineMetaError {
    #[error("{0}")]
    Failed(&'static str),
}
