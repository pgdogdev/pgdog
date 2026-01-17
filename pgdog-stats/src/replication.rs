use std::fmt::Display;
use std::str::FromStr;
use std::time::SystemTime;

use bytes::Bytes;
use pgdog_postgres_types::Error;
use pgdog_postgres_types::{Format, FromDataType, TimestampTz};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Lsn {
    pub high: i64,
    pub low: i64,
    pub lsn: i64,
}

impl Lsn {
    /// Get LSN from the 64-bit representation.
    pub fn from_i64(lsn: i64) -> Self {
        let high = ((lsn >> 32) as u32) as i64;
        let low = ((lsn & 0xFFFF_FFFF) as u32) as i64;
        Self { high, low, lsn }
    }
}

impl FromDataType for Lsn {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        let val = String::decode(bytes, encoding)?;
        Self::from_str(&val).map_err(|_| Error::NotPgLsn)
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::from(self.to_string())),
            Format::Binary => todo!(),
        }
    }
}

impl FromStr for Lsn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // This is not the right formula to get the LSN number but
        // it survives (de)serialization which is all we care about.
        //
        // TODO: maybe just save it as a string?
        let mut parts = s.split("/");
        let high = parts.next().ok_or(Error::LsnDecode)?;
        let high = i64::from_str_radix(high, 16)?;

        let low = parts.next().ok_or(Error::LsnDecode)?;
        let low = i64::from_str_radix(low, 16)?;

        let lsn = (high << 32) + low;

        Ok(Self { lsn, high, low })
    }
}

impl Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.high, self.low)
    }
}

/// LSN information.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LsnStats {
    /// pg_is_in_recovery()
    pub replica: bool,
    /// Replay LSN on replica, current LSN on primary.
    pub lsn: Lsn,
    /// LSN position in bytes from 0.
    pub offset_bytes: i64,
    /// Server timestamp.
    pub timestamp: TimestampTz,
    /// Our timestamp.
    pub fetched: SystemTime,
    /// Running on Aurora.
    pub aurora: bool,
}

impl Default for LsnStats {
    fn default() -> Self {
        Self {
            replica: true, // Replica unless proven otherwise.
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
    }
}
