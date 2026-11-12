use derive_more::Display;
use serde::{Deserialize, Serialize};
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
                self.sharded_tables != current.sharded_tables,
            ),
            (
                ConfigField::ShardedMappings,
                self.sharded_mappings != current.sharded_mappings,
            ),
            (
                ConfigField::OmnishardedTables,
                self.omnisharded_tables != current.omnisharded_tables,
            ),
            (
                ConfigField::ShardedSchemas,
                self.sharded_schemas != current.sharded_schemas,
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

    #[allow(dead_code)]
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

#[cfg(test)]
mod test {
    use pgdog_config::ShardedTableConfig;
    use serde_json::json;

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
    fn validate_equal_finds_source_mismatches() {
        let left = snapshot();
        let mut right = snapshot();
        right.source.shards = 2;
        right.source.sharded_tables = rules();
        right.source.sharded_mappings = rules();
        right.source.omnisharded_tables = rules();
        right.source.sharded_schemas = rules();

        assert_eq!(
            left.validate_equal(&right),
            Err(vec![
                ConfigMismatch::Source(ConfigField::Shards),
                ConfigMismatch::Source(ConfigField::ShardedTables),
                ConfigMismatch::Source(ConfigField::ShardedMappings),
                ConfigMismatch::Source(ConfigField::OmnishardedTables),
                ConfigMismatch::Source(ConfigField::ShardedSchemas),
            ])
        );
    }

    #[test]
    fn validate_equal_finds_destination_mismatches() {
        let left = snapshot();
        let mut right = snapshot();
        right.destination.shards = 3;
        right.destination.sharded_tables = rules();
        right.destination.sharded_mappings = rules();
        right.destination.omnisharded_tables = rules();
        right.destination.sharded_schemas = rules();

        assert_eq!(
            left.validate_equal(&right),
            Err(vec![
                ConfigMismatch::Destination(ConfigField::Shards),
                ConfigMismatch::Destination(ConfigField::ShardedTables),
                ConfigMismatch::Destination(ConfigField::ShardedMappings),
                ConfigMismatch::Destination(ConfigField::OmnishardedTables),
                ConfigMismatch::Destination(ConfigField::ShardedSchemas),
            ])
        );
    }

    #[test]
    fn validate_equal_error_names_the_fields() {
        let left = snapshot();
        let mut right = snapshot();
        right.source.shards = 2;
        right.destination.sharded_tables = vec![];

        let error = Error::ConfigChanged(left.validate_equal(&right).unwrap_err());
        assert_eq!(
            error.to_string(),
            "sharding config changed since the resharding started: source.shards, destination.sharded_tables"
        );
    }
}
