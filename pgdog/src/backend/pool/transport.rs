use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

/// Transport enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Transport {
    TCP(String),
    Unix(PathBuf),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransportError {
    #[error("Expected a TCP host but address is a unix socket directory {0}")]
    ExpectedTCP(PathBuf),

    #[error("Expected Unix socket directory but address is a TCP host {0}")]
    ExpectedUnix(String),
}

impl Transport {
    pub fn new(value: &str) -> Self {
        if value.starts_with('/') {
            Transport::Unix(value.into())
        } else {
            Transport::TCP(value.to_string())
        }
    }

    pub fn unix_socket_path(&self, port: &u16) -> Result<PathBuf, TransportError> {
        match self {
            Transport::TCP(host) => Err(TransportError::ExpectedUnix(host.clone())),
            Transport::Unix(dir) => Ok(dir.join(format!(".s.PGSQL.{}", port))),
        }
    }

    pub fn tcp(&self) -> Result<&str, TransportError> {
        match self {
            Transport::TCP(host) => Ok(host),
            Transport::Unix(path) => Err(TransportError::ExpectedTCP(path.clone())),
        }
    }

    pub fn unix(&self) -> Result<&Path, TransportError> {
        match self {
            Transport::TCP(host) => Err(TransportError::ExpectedUnix(host.clone())),
            Transport::Unix(path_buf) => Ok(path_buf),
        }
    }
}

impl Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::TCP(addr) => write!(f, "{}", addr),
            Transport::Unix(path_buf) => write!(f, "{}", path_buf.display()),
        }
    }
}
