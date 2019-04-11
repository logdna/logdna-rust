use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;

use crate::error::ParamsError;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Params {
    pub hostname: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    pub(crate) now: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Tags>,
}

impl Params {
    pub fn builder() -> ParamsBuilder {
        ParamsBuilder::new()
    }

    pub(crate) fn set_now(&mut self, now: i64) -> &mut Self {
        self.now = now;
        self
    }
}


pub struct ParamsBuilder {
    hostname: Option<String>,
    mac: Option<String>,
    ip: Option<String>,
    tags: Option<Tags>,
}

impl ParamsBuilder {
    pub fn new() -> Self {
        Self {
            hostname: None,
            mac: None,
            ip: None,
            tags: None,
        }
    }

    pub fn hostname<T: Into<String>>(&mut self, hostname: T) -> &mut Self {
        self.hostname = Some(hostname.into());
        self
    }

    pub fn mac<T: Into<String>>(&mut self, mac: T) -> &mut Self {
        self.mac = Some(mac.into());
        self
    }

    pub fn ip<T: Into<String>>(&mut self, ip: T) -> &mut Self {
        self.hostname = Some(ip.into());
        self
    }

    pub fn tags<T: Into<Tags>>(&mut self, tags: T) -> &mut Self {
        self.tags = Some(tags.into());
        self
    }

    pub fn build(&mut self) -> Result<Params, ParamsError> {
        Ok(Params {
            hostname: self.hostname.clone()
                .ok_or(ParamsError::RequiredField("hostname is required in a ParamsBuilder".into()))?,
            mac: self.mac.clone(),
            ip: self.ip.clone(),
            now: 0,
            tags: self.tags.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Tags {
    inner: Vec<String>
}

impl Tags {
    pub fn new() -> Self {
        Self {
            inner: Vec::new()
        }
    }

    pub fn parse<T: Into<String>>(tags: T) -> Self {
        Self {
            inner: tags.into().split_terminator(",").map(|s| s.to_string()).collect()
        }
    }

    pub fn add<T: Into<String>>(&mut self, tag: T) -> &mut Self {
        self.inner.push(tag.into());
        self
    }
}

impl Serialize for Tags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        serializer.serialize_str(&self.inner.join(","))
    }
}

impl<'de> Deserialize<'de> for Tags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        struct StrVisitor {}

        impl<'de> Visitor<'de> for StrVisitor {
            type Value = Tags;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("comma separated string, e.g a,b,c")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Tags {
                    inner: v.split_terminator(",").map(|s| s.to_string()).collect()
                })
            }
        }

        deserializer.deserialize_str(StrVisitor {})
    }
}