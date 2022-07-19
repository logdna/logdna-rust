use std::fmt;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ParamsError;

/// Represents the query parameters that are passed to the IngestAPI
///
/// e.g `?hostname=test&now=42343234234`
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Params {
    /// the hostname parameter, e.g `node-001`
    pub hostname: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// the mac parameter (optional), e.g `C0:FF:EE:C0:FF:EE`
    pub mac: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// the ip parameter (optional), e.g `127.0.0.1`
    pub ip: Option<String>,
    /// the now parameter, e.g `435435875675`
    ///
    /// Note this is set by the client upon every request
    pub now: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// the tags parameter (optional), e.g `this,is,a,test,tag`
    pub tags: Option<Tags>,
}

impl Params {
    /// Constructs a new ParamsBuilder
    pub fn builder() -> ParamsBuilder {
        ParamsBuilder::new()
    }
    /// Sets the now field, this is for internal (crate) use
    pub(crate) fn set_now(&mut self, now: i64) -> &mut Self {
        self.now = now;
        self
    }
}

/// Used to build an instance of Params
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ParamsBuilder {
    hostname: Option<String>,
    mac: Option<String>,
    ip: Option<String>,
    tags: Option<Tags>,
}

impl ParamsBuilder {
    /// Constructs a new ParamsBuilder
    pub fn new() -> Self {
        Self {
            hostname: None,
            mac: None,
            ip: None,
            tags: None,
        }
    }
    /// Sets the hostname field, required
    pub fn hostname<T: Into<String>>(&mut self, hostname: T) -> &mut Self {
        self.hostname = Some(hostname.into());
        self
    }
    /// Sets the mac field, optional
    pub fn mac<T: Into<String>>(&mut self, mac: T) -> &mut Self {
        self.mac = Some(mac.into());
        self
    }
    /// Sets the ip field, optional
    pub fn ip<T: Into<String>>(&mut self, ip: T) -> &mut Self {
        self.hostname = Some(ip.into());
        self
    }
    /// Sets the tags field, optional
    pub fn tags<T: Into<Tags>>(&mut self, tags: T) -> &mut Self {
        self.tags = Some(tags.into());
        self
    }
    /// Builds a Params instance from the current ParamsBuilder
    pub fn build(&mut self) -> Result<Params, ParamsError> {
        Ok(Params {
            hostname: self.hostname.clone().ok_or_else(|| {
                ParamsError::RequiredField("hostname is required in a ParamsBuilder".into())
            })?,
            mac: self.mac.clone(),
            ip: self.ip.clone(),
            now: 0,
            tags: self.tags.clone(),
        })
    }
}

impl Default for ParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Defines a comma separated list of tags, e.g `this,is,a,test`
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Tags {
    inner: Vec<String>,
}

impl Tags {
    /// Constructs an empty instance
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }
    /// Parses a comma separated list of tags into a Tags instance, e.g `this,is,a,test`
    pub fn parse<T: Into<String>>(tags: T) -> Self {
        Self {
            inner: tags
                .into()
                .split_terminator(',')
                .map(|s| s.to_string())
                .collect(),
        }
    }
    /// Manually adds a tag to the list of tags
    pub fn add<T: Into<String>>(&mut self, tag: T) -> &mut Self {
        self.inner.push(tag.into());
        self
    }
}

impl Default for Tags {
    fn default() -> Self {
        Self::new()
    }
}

impl From<String> for Tags {
    fn from(input: String) -> Self {
        Tags::parse(input)
    }
}

impl<'a> From<&'a str> for Tags {
    fn from(input: &'a str) -> Self {
        Tags::parse(input)
    }
}

impl From<Vec<String>> for Tags {
    fn from(input: Vec<String>) -> Self {
        let mut tags = Tags::new();
        input.into_iter().for_each(|t| {
            tags.add(t);
        });
        tags
    }
}

impl Serialize for Tags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.inner.join(","))
    }
}

impl<'de> Deserialize<'de> for Tags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrVisitor {}

        impl<'de> Visitor<'de> for StrVisitor {
            type Value = Tags;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("comma separated string, e.g a,b,c")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Tags {
                    inner: v.split_terminator(',').map(|s| s.to_string()).collect(),
                })
            }
        }

        deserializer.deserialize_str(StrVisitor {})
    }
}
