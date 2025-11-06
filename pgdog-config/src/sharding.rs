use serde::{Deserialize, Serialize};
use std::collections::{hash_map::DefaultHasher, HashSet};
use std::hash::{Hash, Hasher as StdHasher};
use std::path::PathBuf;
use tracing::{info, warn};

use super::error::Error;
use crate::Vector;

/// Sharded table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShardedTable {
    /// Database this table belongs to.
    pub database: String,
    /// Table name. If none specified, all tables with the specified
    /// column are considered sharded.
    #[serde(default)]
    pub name: Option<String>,
    /// Schema name. If not specified, will match all schemas.
    #[serde(default)]
    pub schema: Option<String>,
    /// Table sharded on this column.
    #[serde(default)]
    pub column: String,
    /// This table is the primary sharding anchor (e.g. "users").
    #[serde(default)]
    pub primary: bool,
    /// Centroids for vector sharding.
    #[serde(default)]
    pub centroids: Vec<Vector>,
    #[serde(default)]
    pub centroids_path: Option<PathBuf>,
    /// Data type of the column.
    #[serde(default)]
    pub data_type: DataType,
    /// How many centroids to probe.
    #[serde(default)]
    pub centroid_probes: usize,
    /// Hasher function.
    #[serde(default)]
    pub hasher: Hasher,
}

impl ShardedTable {
    /// Load centroids from file, if provided.
    ///
    /// Centroids can be very large vectors (1000+ columns).
    /// Hardcoding them in pgdog.toml is then impractical.
    pub fn load_centroids(&mut self) -> Result<(), Error> {
        if let Some(centroids_path) = &self.centroids_path {
            if let Ok(f) = std::fs::read_to_string(centroids_path) {
                let centroids: Vec<Vector> = serde_json::from_str(&f)?;
                self.centroids = centroids;
                info!("loaded {} centroids", self.centroids.len());
            } else {
                warn!(
                    "centroids at path \"{}\" not found",
                    centroids_path.display()
                );
            }
        }

        if self.centroid_probes < 1 {
            self.centroid_probes = (self.centroids.len() as f32).sqrt().ceil() as usize;
            if self.centroid_probes > 0 {
                info!("setting centroid probes to {}", self.centroid_probes);
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Hasher {
    #[default]
    Postgres,
    Sha1,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Copy, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    Bigint,
    Uuid,
    Vector,
    Varchar,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShardedMapping {
    pub database: String,
    pub column: String,
    pub table: Option<String>,
    pub schema: Option<String>,
    pub kind: ShardedMappingKind,
    pub start: Option<FlexibleType>,
    pub end: Option<FlexibleType>,
    #[serde(default)]
    pub values: HashSet<FlexibleType>,
    pub shard: usize,
}

impl Hash for ShardedMapping {
    fn hash<H: StdHasher>(&self, state: &mut H) {
        self.database.hash(state);
        self.column.hash(state);
        self.table.hash(state);
        self.kind.hash(state);
        self.start.hash(state);
        self.end.hash(state);

        // Hash the values in a deterministic way by XORing their individual hashes
        let mut values_hash = 0u64;
        for value in &self.values {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            values_hash ^= hasher.finish();
        }
        values_hash.hash(state);

        self.shard.hash(state);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq, Hash)]
pub struct ShardedMappingKey {
    database: String,
    column: String,
    table: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Hash, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ShardedMappingKind {
    #[default]
    List,
    Range,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash)]
#[serde(untagged)]
pub enum FlexibleType {
    Integer(i64),
    Uuid(uuid::Uuid),
    String(String),
}

impl From<i64> for FlexibleType {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<uuid::Uuid> for FlexibleType {
    fn from(value: uuid::Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<String> for FlexibleType {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OmnishardedTables {
    pub database: String,
    pub tables: Vec<String>,
}

/// Queries with manual routing rules.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManualQuery {
    pub fingerprint: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardedSchema {
    /// Database name.
    pub database: String,
    /// Schema name.
    pub name: String,
    #[serde(default)]
    pub shard: usize,
}
