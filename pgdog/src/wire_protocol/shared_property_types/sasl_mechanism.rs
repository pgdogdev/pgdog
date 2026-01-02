use std::error::Error as StdError;
use std::fmt;

// -----------------------------------------------------------------------------
// ----- Property --------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    ScramSha256,
    ScramSha256Plus,
}

// -----------------------------------------------------------------------------
// ----- Encoding/Decoding -----------------------------------------------------

impl SaslMechanism {
    pub fn from_str(s: &str) -> Result<Self, SaslMechanismError> {
        match s {
            "SCRAM-SHA-256" => Ok(Self::ScramSha256),
            "SCRAM-SHA-256-PLUS" => Ok(Self::ScramSha256Plus),
            other => Err(SaslMechanismError::UnsupportedMechanism(other.to_string())),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha256Plus => "SCRAM-SHA-256-PLUS",
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum SaslMechanismError {
    UnsupportedMechanism(String),
}

impl fmt::Display for SaslMechanismError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SaslMechanismError::UnsupportedMechanism(m) => {
                write!(f, "unsupported SASL mechanism: {m}")
            }
        }
    }
}

impl StdError for SaslMechanismError {}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
