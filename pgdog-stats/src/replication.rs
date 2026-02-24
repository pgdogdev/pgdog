use std::fmt::Display;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use pgdog_postgres_types::Error;
use pgdog_postgres_types::{Format, FromDataType, TimestampTz};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Default, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
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

    /// Replication lag in bytes.
    pub fn distance_bytes(&self, other: &Lsn) -> i64 {
        self.lsn - other.lsn
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

impl LsnStats {
    /// Stats contain real data.
    pub fn valid(&self) -> bool {
        self.aurora || self.lsn.lsn > 0
    }
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ReplicaLag {
    pub duration: Duration,
    pub bytes: i64,
}

impl ReplicaLag {
    /// A way to compare replica lag calculated by us.
    ///
    /// First, take bytes, they are the most accurate. Then, compare estimated
    /// duration.
    ///
    pub fn greater_or_eq(&self, other: &Self) -> bool {
        if self.bytes >= other.bytes {
            true
        } else {
            self.duration >= other.duration
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_config::General;

    #[test]
    fn test_default_replica_lag_never_exceeds_default_config_threshold() {
        let lag = ReplicaLag::default();

        let config = General::default();
        let default_threshold = ReplicaLag {
            bytes: config.ban_replica_lag_bytes.try_into().unwrap_or(i64::MAX),
            duration: Duration::from_millis(config.ban_replica_lag),
        };

        assert!(
            !lag.greater_or_eq(&default_threshold),
            "Default replica lag (no measurements) should never exceed default config threshold"
        );
    }

    #[test]
    fn test_greater_or_eq_only_bytes_configured() {
        // Duration threshold is MAX (not configured), only bytes matters
        let threshold = ReplicaLag {
            bytes: 1000,
            duration: Duration::MAX,
        };

        // Bytes exceeds threshold
        let lag_bad = ReplicaLag {
            bytes: 2000,
            duration: Duration::ZERO,
        };
        assert!(
            lag_bad.greater_or_eq(&threshold),
            "Should trigger when bytes exceeds threshold"
        );

        // Bytes below threshold
        let lag_ok = ReplicaLag {
            bytes: 500,
            duration: Duration::from_secs(3600), // High duration ignored since threshold is MAX
        };
        assert!(
            !lag_ok.greater_or_eq(&threshold),
            "Should not trigger when bytes below threshold"
        );
    }

    #[test]
    fn test_greater_or_eq_only_duration_configured() {
        // Bytes threshold is MAX (not configured), only duration matters
        let threshold = ReplicaLag {
            bytes: i64::MAX,
            duration: Duration::from_secs(5),
        };

        // Duration exceeds threshold
        let lag_bad = ReplicaLag {
            bytes: 1000,
            duration: Duration::from_secs(10),
        };
        assert!(
            lag_bad.greater_or_eq(&threshold),
            "Should trigger when duration exceeds threshold"
        );

        // Duration below threshold
        let lag_ok = ReplicaLag {
            bytes: 1000,
            duration: Duration::from_secs(1),
        };
        assert!(
            !lag_ok.greater_or_eq(&threshold),
            "Should not trigger when duration below threshold"
        );
    }

    #[test]
    fn test_greater_or_eq_both_configured_bytes_exceeds() {
        let threshold = ReplicaLag {
            bytes: 1000,
            duration: Duration::from_secs(5),
        };

        // Bytes exceeds, duration doesn't - should trigger (bytes takes priority)
        let lag = ReplicaLag {
            bytes: 2000,
            duration: Duration::from_secs(1),
        };
        assert!(
            lag.greater_or_eq(&threshold),
            "Should trigger when bytes exceeds (bytes takes priority)"
        );
    }

    #[test]
    fn test_greater_or_eq_both_configured_only_duration_exceeds() {
        let threshold = ReplicaLag {
            bytes: 1000,
            duration: Duration::from_secs(5),
        };

        // Bytes below, duration exceeds - should trigger
        let lag = ReplicaLag {
            bytes: 500,
            duration: Duration::from_secs(10),
        };
        assert!(
            lag.greater_or_eq(&threshold),
            "Should trigger when duration exceeds and bytes below"
        );
    }

    #[test]
    fn test_greater_or_eq_both_configured_neither_exceeds() {
        let threshold = ReplicaLag {
            bytes: 1000,
            duration: Duration::from_secs(5),
        };

        // Neither exceeds
        let lag = ReplicaLag {
            bytes: 500,
            duration: Duration::from_secs(1),
        };
        assert!(
            !lag.greater_or_eq(&threshold),
            "Should not trigger when neither exceeds"
        );
    }

    #[test]
    fn test_greater_or_eq_bytes_takes_priority() {
        let threshold = ReplicaLag {
            bytes: 1000,
            duration: Duration::from_secs(5),
        };

        // Bytes exactly at threshold - should trigger immediately without checking duration
        let lag = ReplicaLag {
            bytes: 1000,
            duration: Duration::ZERO,
        };
        assert!(
            lag.greater_or_eq(&threshold),
            "Should trigger when bytes equals threshold (>=)"
        );
    }
}
