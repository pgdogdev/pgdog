use derive_more::Display;
use pgdog_config::{
    OmnishardedTables, ShardedMappingDeprecated, ShardedSchema, ShardedTableConfig,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::super::Error;
use crate::backend::Cluster;

/// Represent the cluster configuration that could affect
/// the resharding if changed
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct ClusterSnapshot {
    shards: usize,
    sharded_tables: Vec<Value>,
    sharded_mappings: Vec<Value>,
    omnisharded_tables: Vec<Value>,
    sharded_schemas: Vec<Value>,
}

impl ClusterSnapshot {
    pub(crate) fn capture(cluster: &Cluster, config: &pgdog_config::Config) -> Result<Self, Error> {
        let name = cluster.name();
        Ok(Self {
            shards: cluster.shards().len(),
            sharded_tables: sorted_rules(
                config.sharded_tables.iter().filter(|t| t.database == name),
            )?,
            sharded_mappings: sorted_rules(
                config
                    .sharded_mappings
                    .iter()
                    .filter(|m| m.database == name),
            )?,
            omnisharded_tables: sorted_rules(
                config
                    .omnisharded_tables
                    .iter()
                    .filter(|t| t.database == name),
            )?,
            sharded_schemas: sorted_rules(
                config.sharded_schemas.iter().filter(|s| s.database == name),
            )?,
        })
    }

    fn changed_fields(&self, current: &Self) -> Vec<ConfigField> {
        [
            (ConfigField::Shards, self.shards != current.shards),
            (
                ConfigField::ShardedTables,
                !same_rules::<ShardedTableConfig>(&self.sharded_tables, &current.sharded_tables),
            ),
            (
                ConfigField::ShardedMappings,
                !same_rules::<ShardedMappingDeprecated>(
                    &self.sharded_mappings,
                    &current.sharded_mappings,
                ),
            ),
            (
                ConfigField::OmnishardedTables,
                !same_rules::<OmnishardedTables>(
                    &self.omnisharded_tables,
                    &current.omnisharded_tables,
                ),
            ),
            (
                ConfigField::ShardedSchemas,
                !same_rules::<ShardedSchema>(&self.sharded_schemas, &current.sharded_schemas),
            ),
        ]
        .into_iter()
        .filter_map(|(field, changed)| changed.then_some(field))
        .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display(rename_all = "snake_case")]
pub(crate) enum ConfigField {
    Shards,
    ShardedTables,
    ShardedMappings,
    OmnishardedTables,
    ShardedSchemas,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum ConfigMismatch {
    #[display("source.{_0}")]
    Source(ConfigField),
    #[display("destination.{_0}")]
    Destination(ConfigField),
}

/// Combined snapshot of source and destination cluster to
/// catch any misaligns during resharding reruns.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct ConfigSnapshot {
    source: ClusterSnapshot,
    destination: ClusterSnapshot,
}

impl ConfigSnapshot {
    pub(crate) fn capture(
        source: &Cluster,
        destination: &Cluster,
        config: &pgdog_config::Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            source: ClusterSnapshot::capture(source, config)?,
            destination: ClusterSnapshot::capture(destination, config)?,
        })
    }

    pub(crate) fn validate_equal(
        &self,
        current: &ConfigSnapshot,
    ) -> Result<(), Vec<ConfigMismatch>> {
        let mismatches: Vec<_> = self
            .source
            .changed_fields(&current.source)
            .into_iter()
            .map(ConfigMismatch::Source)
            .chain(
                self.destination
                    .changed_fields(&current.destination)
                    .into_iter()
                    .map(ConfigMismatch::Destination),
            )
            .collect();

        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(mismatches)
        }
    }
}

fn sorted_rules<T: Serialize>(items: impl Iterator<Item = T>) -> Result<Vec<Value>, Error> {
    let mut rules = items
        .map(|item| {
            serde_json::to_value(item).map_err(|err| Error::ReshardingFormat(err.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rules.sort_by_cached_key(|rule| rule.to_string());
    Ok(rules)
}

/// Compare two rule lists as config values of type `T`. Rules that
/// `T` cannot read are compared as JSON.
fn same_rules<T: DeserializeOwned + PartialEq>(stored: &[Value], current: &[Value]) -> bool {
    let parse = |rules: &[Value]| {
        rules
            .iter()
            .map(T::deserialize)
            .collect::<Result<Vec<T>, _>>()
    };
    match (parse(stored), parse(current)) {
        (Ok(stored), Ok(current)) => stored == current,
        _ => stored == current,
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::super::super::RecordMismatch;

    use super::*;

    fn table(name: &str) -> ShardedTableConfig {
        ShardedTableConfig {
            database: "destination".into(),
            name: Some(name.into()),
            column: "id".into(),
            ..Default::default()
        }
    }

    fn snapshot() -> ConfigSnapshot {
        let cluster = |shards| ClusterSnapshot {
            shards,
            sharded_tables: vec![json!({"name": "users"})],
            sharded_mappings: vec![],
            omnisharded_tables: vec![],
            sharded_schemas: vec![],
        };
        ConfigSnapshot {
            source: cluster(1),
            destination: cluster(2),
        }
    }

    fn rules() -> Vec<Value> {
        vec![json!({"name": "orders"})]
    }

    #[test]
    fn sorted_rules_ignore_config_order() {
        let forward = sorted_rules([table("users"), table("orders")].iter()).unwrap();
        let backward = sorted_rules([table("orders"), table("users")].iter()).unwrap();
        assert_eq!(forward, backward);
        assert_eq!(forward.len(), 2);
    }

    #[test]
    fn snapshot_survives_json_round_trip() {
        let left = snapshot();
        let right: ConfigSnapshot =
            serde_json::from_str(&serde_json::to_string(&left).unwrap()).unwrap();
        assert_eq!(left, right);
        assert_eq!(left.validate_equal(&right), Ok(()));
    }

    #[test]
    fn stored_rules_read_missing_fields_as_defaults() {
        let current = sorted_rules([table("users")].iter()).unwrap();
        let mut stored = current.clone();
        stored[0].as_object_mut().unwrap().remove("lookup_query");
        assert!(same_rules::<ShardedTableConfig>(&stored, &current));

        let lookup = ShardedTableConfig {
            lookup_query: Some("SELECT shard FROM users".into()),
            ..table("users")
        };
        let current = sorted_rules([lookup].iter()).unwrap();
        assert!(!same_rules::<ShardedTableConfig>(&stored, &current));
    }

    #[test]
    fn centroids_read_back_from_json_text_report_no_change() {
        let vector = ShardedTableConfig {
            centroids: (1..=2000)
                .map(|value| vec![value as f32 / 7.0, value as f32 / 3.0].into())
                .collect(),
            ..table("embeddings")
        };
        let current = sorted_rules([vector].iter()).unwrap();
        let stored: Vec<Value> =
            serde_json::from_str(&serde_json::to_string(&current).unwrap()).unwrap();
        assert!(same_rules::<ShardedTableConfig>(&stored, &current));
    }

    #[test]
    fn validate_equal_finds_every_changed_field() {
        let left = snapshot();
        let mut right = snapshot();
        for (cluster, shards) in [(&mut right.source, 2), (&mut right.destination, 3)] {
            cluster.shards = shards;
            cluster.sharded_tables = rules();
            cluster.sharded_mappings = rules();
            cluster.omnisharded_tables = rules();
            cluster.sharded_schemas = rules();
        }

        let fields = [
            ConfigField::Shards,
            ConfigField::ShardedTables,
            ConfigField::ShardedMappings,
            ConfigField::OmnishardedTables,
            ConfigField::ShardedSchemas,
        ];
        assert_eq!(
            left.validate_equal(&right),
            Err(fields
                .map(ConfigMismatch::Source)
                .into_iter()
                .chain(fields.map(ConfigMismatch::Destination))
                .collect())
        );
    }

    #[test]
    fn validate_equal_error_names_the_fields() {
        let left = snapshot();
        let mut right = snapshot();
        right.source.shards = 2;
        right.destination.sharded_tables = vec![];

        let error = RecordMismatch::Config(left.validate_equal(&right).unwrap_err());
        assert_eq!(
            error.to_string(),
            "sharding config changed since the resharding started: source.shards, destination.sharded_tables"
        );
    }
}
