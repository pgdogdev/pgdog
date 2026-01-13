use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Deserialize)]
struct RawReplicaLag {
    #[serde(default)]
    check_interval: Option<u64>,
    #[serde(default)]
    max_age: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaLag {
    pub check_interval: Duration,
    pub max_age: Duration,
}

impl ReplicaLag {
    fn default_max_age() -> Duration {
        Duration::from_millis(25)
    }

    fn default_check_interval() -> Duration {
        Duration::from_millis(1000)
    }

    /// Custom "all-or-none" deserializer that returns Option<Self>.
    pub fn deserialize_optional<'de, D>(de: D) -> Result<Option<Self>, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let maybe: Option<RawReplicaLag> = Option::deserialize(de)?;

        Ok(match maybe {
            None => None,

            Some(RawReplicaLag {
                check_interval: None,
                max_age: None,
            }) => None,

            Some(RawReplicaLag {
                check_interval: Some(ci_u64),
                max_age: Some(ma_u64),
            }) => Some(ReplicaLag {
                check_interval: Duration::from_millis(ci_u64),
                max_age: Duration::from_millis(ma_u64),
            }),

            Some(RawReplicaLag {
                check_interval: None,
                max_age: Some(ma_u64),
            }) => Some(ReplicaLag {
                check_interval: Self::default_check_interval(),
                max_age: Duration::from_millis(ma_u64),
            }),

            _ => {
                return Err(serde::de::Error::custom(
                    "replica_lag: cannot set check_interval without max_age",
                ))
            }
        })
    }
}

// NOTE: serialize and deserialize are not inverses.
// - Normally you'd expect ser <-> deser to round-trip, but here deser applies defaults...
//   for missing fields
// - Serializes takes those applied defaults into account so that ReplicaLag always reflects...
//   the actual effective values.
// - This ensures pgdog.admin sees the true config that is applied, not just what was configured.

impl Serialize for ReplicaLag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ReplicaLag", 2)?;
        state.serialize_field("check_interval", &(self.check_interval.as_millis() as u64))?;
        state.serialize_field("max_age", &(self.max_age.as_millis() as u64))?;
        state.end()
    }
}

impl Default for ReplicaLag {
    fn default() -> Self {
        Self {
            check_interval: Self::default_check_interval(),
            max_age: Self::default_max_age(),
        }
    }
}

/// Replication configuration.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Replication {
    /// Path to the pg_dump executable.
    #[serde(default = "Replication::pg_dump_path")]
    pub pg_dump_path: PathBuf,
}

impl Replication {
    fn pg_dump_path() -> PathBuf {
        PathBuf::from("pg_dump")
    }
}

impl Default for Replication {
    fn default() -> Self {
        Self {
            pg_dump_path: Self::pg_dump_path(),
        }
    }
}

/// Mirroring configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Mirroring {
    /// Source database name to mirror from.
    pub source_db: String,
    /// Destination database name to mirror to.
    pub destination_db: String,
    /// Queue length for this mirror (overrides global mirror_queue).
    pub queue_length: Option<usize>,
    /// Exposure for this mirror (overrides global mirror_exposure).
    pub exposure: Option<f32>,
}

impl FromStr for Mirroring {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut source_db = None;
        let mut destination_db = None;
        let mut queue_length = None;
        let mut exposure = None;

        for pair in s.split('&') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid key=value pair: {}", pair));
            }

            match parts[0] {
                "source_db" => source_db = Some(parts[1].to_string()),
                "destination_db" => destination_db = Some(parts[1].to_string()),
                "queue_length" => {
                    queue_length = Some(
                        parts[1]
                            .parse::<usize>()
                            .map_err(|_| format!("Invalid queue_length: {}", parts[1]))?,
                    );
                }
                "exposure" => {
                    exposure = Some(
                        parts[1]
                            .parse::<f32>()
                            .map_err(|_| format!("Invalid exposure: {}", parts[1]))?,
                    );
                }
                _ => return Err(format!("Unknown parameter: {}", parts[0])),
            }
        }

        let source_db = source_db.ok_or("Missing required parameter: source_db")?;
        let destination_db = destination_db.ok_or("Missing required parameter: destination_db")?;

        Ok(Mirroring {
            source_db,
            destination_db,
            queue_length,
            exposure,
        })
    }
}

/// Runtime mirror configuration with resolved values.
#[derive(Debug, Clone)]
pub struct MirrorConfig {
    /// Queue length for this mirror.
    pub queue_length: usize,
    /// Exposure for this mirror.
    pub exposure: f32,
}
