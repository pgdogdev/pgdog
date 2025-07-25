use crate::{
    backend::ShardingSchema,
    config::{DataType, Hasher as HasherConfig, ShardedTable},
};

use super::{Centroids, Context, Data, Error, Hasher, Lists, Operator, Ranges, Value};

#[derive(Debug)]
pub struct ContextBuilder<'a> {
    data_type: DataType,
    value: Option<Value<'a>>,
    operator: Option<Operator<'a>>,
    centroids: Option<Centroids<'a>>,
    ranges: Option<Ranges<'a>>,
    lists: Option<Lists<'a>>,
    probes: usize,
    hasher: Hasher,
    #[allow(dead_code)]
    array: bool,
}

impl<'a> ContextBuilder<'a> {
    pub fn new(table: &'a ShardedTable) -> Self {
        Self {
            data_type: table.data_type,
            centroids: if table.centroids.is_empty() {
                None
            } else {
                Some(Centroids::from(&table.centroids))
            },
            probes: table.centroid_probes,
            operator: None,
            value: None,
            hasher: match table.hasher {
                HasherConfig::Sha1 => Hasher::Sha1,
                HasherConfig::Postgres => Hasher::Postgres,
            },
            ranges: Ranges::new(&table.mapping),
            lists: Lists::new(&table.mapping),
            array: false,
        }
    }

    pub fn shards(mut self, shards: usize) -> Self {
        if let Some(centroids) = self.centroids.take() {
            self.operator = Some(Operator::Centroids {
                shards,
                probes: self.probes,
                centroids,
            });
        } else if let Some(ranges) = self.ranges.take() {
            self.operator = Some(Operator::Range(ranges));
        } else if let Some(lists) = self.lists.take() {
            self.operator = Some(Operator::List(lists));
        } else {
            self.operator = Some(Operator::Shards(shards))
        }
        self
    }

    pub fn data(mut self, data: impl Into<Data<'a>>) -> Self {
        self.value = Some(Value::new(data, self.data_type));
        self
    }

    pub fn value(mut self, value: Value<'a>) -> Self {
        self.value = Some(value);
        self
    }

    pub fn build(mut self) -> Result<Context<'a>, Error> {
        let operator = self.operator.take().ok_or(Error::IncompleteContext)?;
        let value = self.value.take().ok_or(Error::IncompleteContext)?;

        Ok(Context {
            operator,
            value,
            hasher: self.hasher,
        })
    }
}
// -------------------------------------------------------------------------------------------------
// ----- ContextBuilder :: from_str ----------------------------------------------------------------

impl<'a> ContextBuilder<'a> {
    pub fn from_str(value: &'a str, schema: &'a ShardingSchema) -> Result<Self, Error> {
        if schema.has_global_sharding_key_with_mappings() {
            return Self::from_str_with_global_shading_key_mappings(value, schema);
        }

        let bigint = Value::new(value, DataType::Bigint);
        let uuid = Value::new(value, DataType::Uuid);

        if bigint.valid() {
            Ok(Self {
                data_type: DataType::Bigint,
                value: Some(bigint),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                array: false,
                ranges: None,
                lists: None,
            })
        } else if uuid.valid() {
            Ok(Self {
                data_type: DataType::Uuid,
                value: Some(uuid),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                array: false,
                ranges: None,
                lists: None,
            })
        } else {
            Err(Error::IncompleteContext)
        }
    }

    fn from_str_with_global_shading_key_mappings(
        value: &'a str,
        schema: &'a ShardingSchema,
    ) -> Result<Self, Error> {
        // force the borrow to live for 'a
        let table: &'a ShardedTable = schema
            .tables
            .tables()
            .iter()
            .find(|t| t.name.is_none() && t.mapping.is_some())
            .ok_or(Error::IncompleteContext)?;

        Ok(ContextBuilder::new(table).data(value))
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Tests -------------------------------------------------------------------------------------

#[cfg(test)]
mod test_from_str {

    use super::*;
    use crate::backend::ShardedTables;
    use crate::config::{FlexibleType, Hasher, ShardedMapping, ShardedMappingKind};
    use crate::frontend::router::{parser::Shard, sharding::Mapping};
    use std::collections::HashSet;

    fn build_global_hash_schema() -> ShardingSchema {
        let sharded_table = ShardedTable {
            database: "hash_db".to_string(),
            name: None,
            column: "tenant_id".to_string(),
            primary: true,
            centroids: vec![],
            centroids_path: None,
            data_type: DataType::Bigint,
            centroid_probes: 0,
            hasher: HasherConfig::Postgres,
            mapping: None,
        };

        let tables = vec![sharded_table];
        let omnisharded_tables = vec![];
        let sharded_tables = ShardedTables::new(tables, omnisharded_tables, false);

        ShardingSchema {
            shards: 3,
            tables: sharded_tables,
        }
    }

    fn build_global_range_schema() -> ShardingSchema {
        let elo_ranges = vec![(0, 1199), (1200, 1800), (1801, 9999)];
        let sharded_mappings: Vec<ShardedMapping> = elo_ranges
            .iter()
            .enumerate()
            .map(|(i, &(start, end))| ShardedMapping {
                database: "chess_db".to_string(),
                table: None,
                column: "elo".to_string(),
                kind: ShardedMappingKind::Range,
                start: Some(FlexibleType::Integer(start)),
                end: Some(FlexibleType::Integer(end)),
                values: HashSet::new(),
                shard: i as usize,
            })
            .collect();

        let sharded_table = ShardedTable {
            database: "chess_db".to_string(),
            name: None,
            column: "elo".to_string(),
            primary: true,
            centroids: vec![],
            centroids_path: None,
            data_type: DataType::Bigint,
            centroid_probes: 0,
            hasher: Hasher::Postgres,
            mapping: Some(Mapping::Range(sharded_mappings.clone())),
        };

        let tables = vec![sharded_table];
        let omnisharded_tables = vec![];

        let sharded_tables = ShardedTables::new(tables, omnisharded_tables, false);

        let schema = ShardingSchema {
            shards: 3,
            tables: sharded_tables,
        };

        schema
    }

    fn build_global_list_schema() -> ShardingSchema {
        let houses = vec!["gryffindor", "slytherin", "hufflepuff", "ravenclaw"];

        let sharded_mappings: Vec<ShardedMapping> = houses
            .iter()
            .enumerate()
            .map(|(shard, house)| ShardedMapping {
                database: "wizard_db".to_string(),
                table: None,
                column: "house".to_string(),
                kind: ShardedMappingKind::List,
                start: None,
                end: None,
                values: [FlexibleType::String(house.to_string())]
                    .iter()
                    .cloned()
                    .collect(),
                shard: shard as usize,
            })
            .collect();

        let sharded_table = ShardedTable {
            database: "wizard_db".to_string(),
            name: None,
            column: "house".to_string(),
            primary: true,
            centroids: vec![],
            centroids_path: None,
            data_type: DataType::Varchar,
            centroid_probes: 0,
            hasher: HasherConfig::Sha1,
            mapping: Mapping::new(&sharded_mappings),
        };

        let tables = vec![sharded_table];
        let omnisharded_tables = vec![];

        let sharded_tables = ShardedTables::new(tables, omnisharded_tables, false);

        let schema = ShardingSchema {
            shards: 4,
            tables: sharded_tables,
        };

        schema
    }

    #[test]
    fn test_hash_schema() {
        let schema = build_global_hash_schema();

        let test_cases = vec![("1", 2), ("2", 0), ("3", 1), ("4", 0)];
        for (tenant_id, expected_shard) in test_cases {
            let shard = ContextBuilder::from_str(tenant_id, &schema)
                .unwrap()
                .shards(schema.shards)
                .build()
                .unwrap()
                .apply()
                .unwrap();

            assert_eq!(shard, Shard::Direct(expected_shard));
        }
    }

    #[test]
    fn test_range_schema_with_mappings() {
        let schema = build_global_range_schema();

        let elos_low = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
        for elo in elos_low {
            let elo_as_str = elo.to_string();
            let shard = ContextBuilder::from_str(&elo_as_str, &schema)
                .unwrap()
                .shards(schema.shards)
                .build()
                .unwrap()
                .apply()
                .unwrap();

            assert_eq!(shard, Shard::Direct(0));
        }

        let elos_mid = [1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210];
        for elo in elos_mid {
            let elo_as_str = elo.to_string();
            let shard = ContextBuilder::from_str(&elo_as_str, &schema)
                .unwrap()
                .shards(schema.shards)
                .build()
                .unwrap()
                .apply()
                .unwrap();

            assert_eq!(shard, Shard::Direct(1));
        }

        let elos_high = [1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810];
        for elo in elos_high {
            let elo_as_str = elo.to_string();
            let shard = ContextBuilder::from_str(&elo_as_str, &schema)
                .unwrap()
                .shards(schema.shards)
                .build()
                .unwrap()
                .apply()
                .unwrap();

            assert_eq!(shard, Shard::Direct(2));
        }

        let negative_elo_str = "-1000";
        let shard = ContextBuilder::from_str(&negative_elo_str, &schema)
            .unwrap()
            .shards(schema.shards)
            .build()
            .unwrap()
            .apply()
            .unwrap();

        assert_eq!(shard, Shard::All);
    }

    #[test]
    fn test_list_schema_with_mappings() {
        let houses = ["gryffindor", "slytherin", "hufflepuff", "ravenclaw"];
        let schema = build_global_list_schema();

        let house_indexes = [0, 1, 2, 3];
        for index in house_indexes {
            let house = houses[index];
            let shard = ContextBuilder::from_str(&house, &schema)
                .unwrap()
                .shards(schema.shards)
                .build()
                .unwrap()
                .apply()
                .unwrap();

            assert_eq!(shard, Shard::Direct(index));
        }

        let unknown_house = "hello kitty island adventure";
        let shard = ContextBuilder::from_str(&unknown_house, &schema)
            .unwrap()
            .shards(schema.shards)
            .build()
            .unwrap()
            .apply()
            .unwrap();

        assert_eq!(shard, Shard::All);
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
