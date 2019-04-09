use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;

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
    pub fn new() -> Self {
        Self {
            hostname: String::new(),
            mac: None,
            ip: None,
            now: 0,
            tags: None,
        }
    }

    pub fn set_hostname(&mut self, hostname: &str) -> &mut Self {
        self.hostname = hostname.into();
        self
    }

    pub fn set_mac(&mut self, mac: Option<&str>) -> &mut Self {
        self.mac = mac.map(|v| v.into());
        self
    }

    pub fn set_ip(&mut self, ip: Option<&str>) -> &mut Self {
        self.ip = ip.map(|v| v.into());
        self
    }

    pub(crate) fn set_now(&mut self, now: i64) -> &mut Self {
        self.now = now;
        self
    }

    pub fn set_tags(&mut self, tags: Option<Tags>) -> &mut Self {
        self.tags = tags;
        self
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

    pub fn parse(tags: &str) -> Self {
        Self {
            inner: tags.split_terminator(",").map(|s| s.to_string()).collect()
        }
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