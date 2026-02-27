use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::{hash_map::DefaultHasher, HashSet};
use std::fmt::Display;
use std::hash::{Hash, Hasher as StdHasher};
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{info, warn};

use super::error::Error;
use pgdog_vector::Vector;

/// Configuration for sharding databases. Each entry tells PgDog which column to use as the sharding key for a given table.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShardedTable {
    /// The name of the database in `[[databases]]` section in which the table is located.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#database
    pub database: String,

    /// The name of the PostgreSQL table. Only columns explicitly referencing that table will be sharded. If not specified, all tables with the specified column are considered sharded.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#name
    #[serde(default)]
    pub name: Option<String>,

    /// The name of the PostgreSQL schema where the sharded table is located. This is optional.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#schema
    #[serde(default)]
    pub schema: Option<String>,

    /// The name of the sharded column.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#column
    #[serde(default)]
    pub column: String,

    /// Marks this table as the primary sharding anchor (e.g. `users`). PgDog uses the primary table to resolve foreign-key relationships when routing queries.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#primary
    #[serde(default)]
    pub primary: bool,

    /// For vector sharding, specify the centroid vectors directly in the configuration.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#centroids
    #[serde(default)]
    pub centroids: Vec<Vector>,

    /// Path to a JSON file containing centroid vectors. This is useful when centroids are large (1000+ dimensions).
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#centroids_path
    #[serde(default)]
    pub centroids_path: Option<PathBuf>,

    /// The data type of the column. Currently supported options are: `bigint`, `uuid`, `varchar`, `vector`.
    ///
    /// _Default:_ `bigint`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#data_type
    #[serde(default)]
    pub data_type: DataType,

    /// Number of centroids to probe during vector similarity search. If not specified, defaults to the square root of the number of centroids.
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#centroid_probes
    #[serde(default)]
    pub centroid_probes: usize,

    /// The hash function to use for sharding.
    ///
    /// _Default:_ `postgres`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#hasher
    #[serde(default)]
    pub hasher: Hasher,

    /// Resolved explicit routing rules derived from `[[sharded_mappings]]`. Not configurable directly in TOML.
    #[serde(skip, default)]
    pub mapping: Option<Mapping>,
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

/// Hash function used to map a sharding key value to a shard number.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#hasher
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Hasher {
    /// Uses the same hash function as PostgreSQL's `hashint8` / `hashtext` (default).
    #[default]
    Postgres,
    /// SHA-1 based hashing.
    Sha1,
}

/// Data type of the sharding column.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#data_type
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Copy, Eq, Hash, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    /// 64-bit integer (default).
    #[default]
    Bigint,
    /// UUID.
    Uuid,
    /// Vector embedding (for vector similarity sharding).
    Vector,
    /// Variable-length text.
    Varchar,
}

/// Explicit routing rule mapping specific column values or ranges to a shard.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/#mapping-fields
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShardedMapping {
    /// Database name from the `[[databases]]` section.
    pub database: String,
    /// Must match a column defined in `[[sharded_tables]]`.
    pub column: String,
    /// Optional; must match a `name` in `[[sharded_tables]]` if specified.
    pub table: Option<String>,
    /// Optional; must match a `schema` in `[[sharded_tables]]` if specified.
    pub schema: Option<String>,
    /// Mapping strategy: `list`, `range`, or `default`.
    pub kind: ShardedMappingKind,
    /// Inclusive lower bound for range mappings.
    pub start: Option<FlexibleType>,
    /// Exclusive upper bound for range mappings.
    pub end: Option<FlexibleType>,
    /// Set of values for list mappings.
    #[serde(default)]
    pub values: HashSet<FlexibleType>,
    /// Target shard number for matched queries.
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Eq, Hash, JsonSchema)]
pub struct ShardedMappingKey {
    database: String,
    column: String,
    table: Option<String>,
}

/// Strategy used to match column values to a shard.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Hash, Eq, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ShardedMappingKind {
    /// Match an explicit set of values (default).
    #[default]
    List,
    /// Match a contiguous range of values (inclusive start, exclusive end).
    Range,
    /// Catch-all fallback for values not matched by any other rule.
    Default,
}

/// A sharding key value that can be an integer, UUID, or string.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, JsonSchema)]
#[serde(untagged)]
pub enum FlexibleType {
    /// 64-bit signed integer.
    Integer(i64),
    /// UUID.
    #[schemars(with = "String")]
    Uuid(uuid::Uuid),
    /// Text string.
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

/// A group of tables that are replicated across all shards (omnisharded) for a given database.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#omnisharded_sticky
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Eq, Hash, JsonSchema)]
pub struct OmnishardedTables {
    /// Database name from the `[[databases]]` section.
    pub database: String,
    /// List of table names that are replicated across all shards.
    pub tables: Vec<String>,
    /// If true, queries to these tables are pinned to the same shard for the duration of the client connection.
    #[serde(default)]
    pub sticky: bool,
}

#[derive(PartialEq, Debug, Clone, Default)]
pub struct OmnishardedTable {
    pub name: String,
    pub sticky_routing: bool,
}

/// Queries with manual routing rules.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, JsonSchema)]
pub struct ManualQuery {
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default, JsonSchema)]
pub struct ShardedSchema {
    /// Database name.
    pub database: String,
    /// Schema name.
    pub name: Option<String>,
    #[serde(default)]
    pub shard: usize,
    /// All shards.
    #[serde(default)]
    pub all: bool,
}

impl ShardedSchema {
    /// This schema mapping is used to route all other queries.
    pub fn is_default(&self) -> bool {
        self.name.is_none()
    }

    pub fn name(&self) -> &str {
        self.name.as_deref().unwrap_or("*")
    }

    pub fn shard(&self) -> Option<usize> {
        if self.all {
            None
        } else {
            Some(self.shard)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListShards {
    mapping: HashMap<FlexibleType, usize>,
    default: Option<usize>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Mapping {
    Range(Vec<ShardedMapping>), // TODO: optimize with a BTreeMap.
    List(ListShards),           // Optimized.
}

impl Hash for ListShards {
    fn hash<H: StdHasher>(&self, state: &mut H) {
        // Hash the mapping in a deterministic way by XORing individual key-value hashes
        let mut mapping_hash = 0u64;
        for (key, value) in &self.mapping {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            value.hash(&mut hasher);
            mapping_hash ^= hasher.finish();
        }
        mapping_hash.hash(state);
    }
}

impl Mapping {
    pub fn new(mappings: &[ShardedMapping]) -> Option<Self> {
        let range = mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::Range)
            .cloned()
            .collect::<Vec<_>>();
        let list = mappings.iter().any(|m| {
            matches!(
                m.kind,
                ShardedMappingKind::List | ShardedMappingKind::Default
            )
        });

        if !range.is_empty() {
            Some(Self::Range(range))
        } else if list {
            Some(Self::List(ListShards::new(mappings)))
        } else {
            None
        }
    }
}

impl ListShards {
    pub fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    pub fn new(mappings: &[ShardedMapping]) -> Self {
        let mut mapping = HashMap::new();

        for map in mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::List)
        {
            for value in &map.values {
                mapping.insert(value.clone(), map.shard);
            }
        }

        Self {
            mapping,
            default: mappings
                .iter()
                .find(|mapping| mapping.kind == ShardedMappingKind::Default)
                .map(|mapping| mapping.shard),
        }
    }

    pub fn shard(&self, value: &FlexibleType) -> Result<Option<usize>, Error> {
        if let Some(shard) = self.mapping.get(value) {
            Ok(Some(*shard))
        } else if let Some(default) = self.default {
            Ok(Some(default))
        } else {
            Ok(None)
        }
    }
}

/// Controls when the query parser is active.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#query_parser
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryParserLevel {
    /// Always enable the query parser.
    On,
    /// Enable automatically when sharding or read/write splitting is configured (default).
    #[default]
    Auto,
    /// Always disable the query parser.
    Off,
}

/// Underlying parser implementation used to analyze SQL queries.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryParserEngine {
    /// Use the protobuf parse tree from `pg_query` (default).
    #[default]
    PgQueryProtobuf,
    /// Use the raw JSON parse tree from `pg_query`.
    PgQueryRaw,
}

/// Controls how system catalog tables (like `pg_database`, `pg_class`, etc.) are treated by the query router.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#system_catalogs
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SystemCatalogsBehavior {
    /// Send catalog queries to all shards and merge the results.
    Omnisharded,
    /// Send catalog queries to all shards but pin each client connection to the same shard (default).
    #[default]
    OmnishardedSticky,
    /// Route catalog queries using the normal sharding key, like any other table.
    Sharded,
}

impl FromStr for SystemCatalogsBehavior {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "omnisharded" => Self::Omnisharded,
            "omnisharded_sticky" => Self::OmnishardedSticky,
            "sharded" => Self::Sharded,
            _ => return Err(()),
        })
    }
}

/// Format used for `COPY` statements during resharding.
///
/// **Note:** Text format is required when migrating from `INTEGER` to `BIGINT` primary keys during resharding.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#resharding_copy_format
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CopyFormat {
    /// PostgreSQL text format; required for `INTEGER` → `BIGINT` primary key migrations.
    Text,
    /// PostgreSQL binary format; faster but incompatible with type migrations (default).
    #[default]
    Binary,
}

impl Display for CopyFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Binary => write!(f, "binary"),
            Self::Text => write!(f, "text"),
        }
    }
}

/// Controls whether PgDog loads the database schema at startup for query routing.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#load_schema
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LoadSchema {
    /// Always load the schema at startup.
    On,
    /// Never load the schema.
    Off,
    /// Load only when sharding is configured (default).
    #[default]
    Auto,
}

impl FromStr for LoadSchema {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "on" => Self::On,
            "auto" => Self::Auto,
            "off" => Self::Off,
            _ => return Err(()),
        })
    }
}

/// Action to take when the cutover timeout is reached during online resharding.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CutoverTimeoutAction {
    /// Abort the cutover and leave the old configuration in place (default).
    #[default]
    Abort,
    /// Force the cutover to proceed despite the timeout.
    Cutover,
}

impl FromStr for CutoverTimeoutAction {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "abort" => Self::Abort,
            "cutover" => Self::Cutover,
            _ => return Err(()),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_list_shards_with_default() {
        let mappings = vec![
            ShardedMapping {
                values: [FlexibleType::Integer(1), FlexibleType::Integer(2)]
                    .into_iter()
                    .collect(),
                shard: 0,
                ..Default::default()
            },
            ShardedMapping {
                values: [FlexibleType::Integer(3)].into_iter().collect(),
                shard: 1,
                ..Default::default()
            },
            ShardedMapping {
                kind: ShardedMappingKind::Default,
                shard: 2,
                ..Default::default()
            },
        ];

        let list = ListShards::new(&mappings);

        // Explicitly mapped values go to their configured shard
        assert_eq!(list.shard(&FlexibleType::Integer(1)).unwrap(), Some(0));
        assert_eq!(list.shard(&FlexibleType::Integer(2)).unwrap(), Some(0));
        assert_eq!(list.shard(&FlexibleType::Integer(3)).unwrap(), Some(1));

        // Unmapped values fall back to the default shard
        assert_eq!(list.shard(&FlexibleType::Integer(999)).unwrap(), Some(2));
        assert_eq!(list.shard(&FlexibleType::Integer(-1)).unwrap(), Some(2));
    }

    #[test]
    fn test_list_shards_without_default() {
        let mappings = vec![ShardedMapping {
            values: [FlexibleType::Integer(1)].into_iter().collect(),
            ..Default::default()
        }];

        let list = ListShards::new(&mappings);

        // Explicitly mapped value
        assert_eq!(list.shard(&FlexibleType::Integer(1)).unwrap(), Some(0));

        // Unmapped value returns None when no default
        assert_eq!(list.shard(&FlexibleType::Integer(999)).unwrap(), None);
    }
}
