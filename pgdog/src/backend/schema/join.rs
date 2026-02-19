use std::collections::{HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::config::ShardedTable;
use crate::{backend::ShardingSchema, frontend::router::parser::OwnedColumn};

use super::Error;
use super::Schema;
use super::StatsRelation;

/// A step in the join path from start table to the sharding key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinStep {
    /// Source column (includes table/schema info via the FK column).
    pub from: OwnedColumn,
    /// Target column (the referenced column in the target table).
    pub to: OwnedColumn,
}

/// Result of constructing a join path to find a sharding key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    /// The path of joins from start table to the table containing the sharding key.
    pub path: Vec<JoinStep>,
    /// The sharding key column in the final table.
    pub sharding_column: OwnedColumn,
    /// The SQL query to fetch the sharding key value using the FK value.
    pub query: String,
    /// The sharded table configuration.
    pub sharded_table: ShardedTable,
}

impl Join {
    /// Get the FK column name for FK lookup.
    /// Returns the first FK column in the path that should be used for lookup.
    pub fn fk_column(&self) -> Option<&str> {
        self.path.first().map(|step| step.from.name.as_str())
    }
}

impl Schema {
    /// Construct a SELECT ... JOIN ... JOIN ...
    /// query that fetches the value of the sharding key
    /// from a table that has a foreign key relationship to "start" table.
    ///
    /// The relationship can span multiple tables.
    /// Uses BFS to find the shortest path.
    pub fn construct_join(
        &self,
        start: &StatsRelation,
        sharding: &ShardingSchema,
    ) -> Result<Join, Error> {
        let start_schema = start.schema();
        let start_table = &start.name;

        // Find primary key of start table
        let start_pk = Self::find_primary_key_in_relation(start);

        // Check if start table itself has the sharding key
        if let Some((sharding_col, sharded_table)) =
            Self::find_sharding_column_in_relation(start, sharding)
        {
            let query = self.build_direct_query(
                start_schema,
                start_table,
                &sharding_col,
                start_pk.as_deref(),
            );
            return Ok(Join {
                path: vec![],
                sharding_column: OwnedColumn {
                    name: sharding_col,
                    table: Some(start_table.clone()),
                    schema: Some(start_schema.to_string()),
                },
                query,
                sharded_table,
            });
        }

        // BFS to find shortest path
        let mut visited: HashSet<(&str, &str)> = HashSet::new();
        let mut queue: VecDeque<(&str, &str, Vec<JoinStep>)> = VecDeque::new();

        queue.push_back((start_schema, start_table, vec![]));
        visited.insert((start_schema, start_table));

        while let Some((current_schema, current_table, path)) = queue.pop_front() {
            let relation = match self.inner.get(current_schema, current_table) {
                Some(r) => r,
                None => continue,
            };

            // Check each column for foreign keys
            for column in relation.columns.values() {
                for fk in &column.foreign_keys {
                    let target_schema = &fk.schema;
                    let target_table = &fk.table;
                    let target_column = &fk.column;

                    // Check if we've already visited this table
                    let key = (target_schema.as_str(), target_table.as_str());
                    if visited.contains(&key) {
                        continue;
                    }

                    // Look up target relation
                    let target_relation = match self.inner.get(target_schema, target_table) {
                        Some(r) => r,
                        None => continue,
                    };

                    // Build the new path
                    let mut new_path = path.clone();
                    new_path.push(JoinStep {
                        from: OwnedColumn {
                            name: column.column_name.clone(),
                            table: Some(current_table.to_string()),
                            schema: Some(current_schema.to_string()),
                        },
                        to: OwnedColumn {
                            name: target_column.clone(),
                            table: Some(target_table.clone()),
                            schema: Some(target_schema.clone()),
                        },
                    });

                    // Check if target table has the sharding key
                    if let Some((sharding_col, sharded_table)) =
                        Self::find_sharding_column_in_relation(target_relation, sharding)
                    {
                        // Build query that directly looks up sharding key via FK value
                        let query = Self::build_fk_lookup_query(&new_path, &sharding_col);
                        return Ok(Join {
                            path: new_path,
                            sharding_column: OwnedColumn {
                                name: sharding_col,
                                table: Some(target_table.clone()),
                                schema: Some(target_schema.clone()),
                            },
                            query,
                            sharded_table,
                        });
                    }

                    // Add to queue
                    visited.insert((target_relation.schema(), &target_relation.name));
                    queue.push_back((target_relation.schema(), &target_relation.name, new_path));
                }
            }
        }

        Err(Error::NoForeignKeyPath {
            table: format!("{}.{}", start_schema, start_table),
        })
    }

    /// Find the primary key column of a relation.
    fn find_primary_key_in_relation(relation: &StatsRelation) -> Option<String> {
        relation
            .columns
            .values()
            .find(|col| col.is_primary_key)
            .map(|col| col.column_name.clone())
    }

    /// Check if a relation has a sharding key column.
    /// Returns the column name and the matching ShardedTable config.
    fn find_sharding_column_in_relation(
        relation: &StatsRelation,
        sharding: &ShardingSchema,
    ) -> Option<(String, ShardedTable)> {
        for sharded_table in sharding.tables.tables() {
            // Match by schema if specified
            if let Some(ref schema) = sharded_table.schema {
                if schema != relation.schema() {
                    continue;
                }
            }

            // Match by table name if specified
            if let Some(ref name) = sharded_table.name {
                if name != &relation.name {
                    continue;
                }
            }

            // Check if the table has the sharding column
            if relation.has_column(&sharded_table.column) {
                return Some((sharded_table.column.clone(), sharded_table.clone()));
            }
        }

        None
    }

    /// Build a simple SELECT query for direct access.
    fn build_direct_query(
        &self,
        schema: &str,
        table: &str,
        column: &str,
        primary_key: Option<&str>,
    ) -> String {
        let mut query = format!(
            "SELECT \"{}\".\"{}\".\"{column}\" FROM \"{}\".\"{}\"",
            schema, table, schema, table
        );

        if let Some(pk) = primary_key {
            query.push_str(&format!(
                " WHERE \"{}\".\"{}\".\"{}\" = $1",
                schema, table, pk
            ));
        }

        query
    }

    /// Build a query for FK lookup.
    /// Takes FK value as $1 and queries parent table(s) to get sharding key.
    fn build_fk_lookup_query(path: &[JoinStep], sharding_col: &str) -> String {
        if path.is_empty() {
            return String::new();
        }

        // For single-hop: SELECT sharding_col FROM parent WHERE pk = $1
        if path.len() == 1 {
            let step = &path[0];
            let to_schema = step.to.schema.as_deref().unwrap_or("public");
            let to_table = step.to.table.as_deref().unwrap_or("");
            return format!(
                "SELECT \"{}\".\"{}\".\"{}\" FROM \"{}\".\"{}\" WHERE \"{}\".\"{}\".\"{}\" = $1",
                to_schema,
                to_table,
                sharding_col,
                to_schema,
                to_table,
                to_schema,
                to_table,
                step.to.name
            );
        }

        // For multi-hop: start from first target and join to final table
        let first_step = &path[0];
        let first_schema = first_step.to.schema.as_deref().unwrap_or("public");
        let first_table = first_step.to.table.as_deref().unwrap_or("");

        let last_step = path.last().unwrap();
        let target_schema = last_step.to.schema.as_deref().unwrap_or("public");
        let target_table = last_step.to.table.as_deref().unwrap_or("");

        let mut query = format!(
            "SELECT \"{}\".\"{}\".\"{}\" FROM \"{}\".\"{}\"",
            target_schema, target_table, sharding_col, first_schema, first_table
        );

        // Build joins from path[1..] (skip first step, we're starting from its target)
        for step in &path[1..] {
            let from_schema = step.from.schema.as_deref().unwrap_or("public");
            let from_table = step.from.table.as_deref().unwrap_or("");
            let to_schema = step.to.schema.as_deref().unwrap_or("public");
            let to_table = step.to.table.as_deref().unwrap_or("");

            query.push_str(&format!(
                " JOIN \"{}\".\"{}\" ON \"{}\".\"{}\".\"{}\" = \"{}\".\"{}\".\"{}\"",
                to_schema,
                to_table,
                from_schema,
                from_table,
                step.from.name,
                to_schema,
                to_table,
                step.to.name
            ));
        }

        // WHERE uses the first target's referenced column
        query.push_str(&format!(
            " WHERE \"{}\".\"{}\".\"{}\" = $1",
            first_schema, first_table, first_step.to.name
        ));

        query
    }
}

#[cfg(test)]
mod test {
    use super::super::test_helpers::prelude::*;

    /// Build the standard test schema:
    ///   users (id PK, user_id - sharding key)
    ///   orders (id PK, user_id FK -> users.id)
    ///   order_items (id PK, order_id FK -> orders.id)
    fn build_test_schema() -> (
        super::super::Schema,
        crate::backend::pool::cluster::ShardingSchema,
    ) {
        let db_schema = schema()
            .relation(
                table("users")
                    .oid(1001)
                    .column(pk("id"))
                    .column(col("user_id")),
            )
            .relation(
                table("orders")
                    .oid(1002)
                    .column(pk("id"))
                    .column(fk("user_id", "users", "id")),
            )
            .relation(
                table("order_items")
                    .oid(1003)
                    .column(pk("id"))
                    .column(fk("order_id", "orders", "id")),
            )
            .build();

        let sharding_schema = sharding().sharded_table("users", "user_id").build();

        (db_schema, sharding_schema)
    }

    #[test]
    fn test_construct_join_direct_table_has_sharding_key() {
        let (db_schema, sharding_schema) = build_test_schema();

        let relation = db_schema.inner.get("public", "users").unwrap();
        let join = db_schema
            .construct_join(relation, &sharding_schema)
            .unwrap();

        assert!(join.path.is_empty());
        assert_eq!(join.sharding_column.name, "user_id");
        assert_eq!(
            join.query,
            r#"SELECT "public"."users"."user_id" FROM "public"."users" WHERE "public"."users"."id" = $1"#
        );
    }

    #[test]
    fn test_construct_join_one_hop() {
        let (db_schema, sharding_schema) = build_test_schema();

        let relation = db_schema.inner.get("public", "orders").unwrap();
        let join = db_schema
            .construct_join(relation, &sharding_schema)
            .unwrap();

        assert_eq!(join.path.len(), 1);
        assert_eq!(join.path[0].from.table, Some("orders".into()));
        assert_eq!(join.path[0].from.name, "user_id");
        assert_eq!(join.path[0].to.table, Some("users".into()));
        assert_eq!(join.path[0].to.name, "id");
        assert_eq!(join.sharding_column.name, "user_id");
        // Query directly looks up sharding key from parent table using FK value
        assert_eq!(
            join.query,
            r#"SELECT "public"."users"."user_id" FROM "public"."users" WHERE "public"."users"."id" = $1"#
        );
    }

    #[test]
    fn test_construct_join_two_hops() {
        let (db_schema, sharding_schema) = build_test_schema();

        let relation = db_schema.inner.get("public", "order_items").unwrap();
        let join = db_schema
            .construct_join(relation, &sharding_schema)
            .unwrap();

        assert_eq!(join.path.len(), 2);
        assert_eq!(join.path[0].from.table, Some("order_items".into()));
        assert_eq!(join.path[0].from.name, "order_id");
        assert_eq!(join.path[0].to.table, Some("orders".into()));
        assert_eq!(join.path[1].from.table, Some("orders".into()));
        assert_eq!(join.path[1].to.table, Some("users".into()));
        assert_eq!(join.sharding_column.name, "user_id");
        // Query starts from first target (orders) and joins to find sharding key
        assert_eq!(
            join.query,
            r#"SELECT "public"."users"."user_id" FROM "public"."orders" JOIN "public"."users" ON "public"."orders"."user_id" = "public"."users"."id" WHERE "public"."orders"."id" = $1"#
        );
    }

    #[test]
    fn test_construct_join_no_path() {
        let sharding_schema = sharding().sharded_table("users", "user_id").build();

        let db_schema = schema()
            .relation(table("isolated").oid(9999).column(pk("id")))
            .build();

        let relation = db_schema.inner.get("public", "isolated").unwrap();
        let result = db_schema.construct_join(relation, &sharding_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_joins_and_get_join() {
        let (mut db_schema, sharding_schema) = build_test_schema();

        db_schema.computed_sharded_joins(&sharding_schema);

        // users has sharding key directly - no join stored but is_sharded = true
        let users_relation = db_schema.inner.get("public", "users").unwrap();
        assert!(db_schema.get_sharded_join(users_relation).is_none());
        assert!(users_relation.is_sharded);

        // orders needs a join to get to users - is_sharded = true
        let orders_relation = db_schema.inner.get("public", "orders").unwrap();
        let orders_join = db_schema.get_sharded_join(orders_relation);
        assert!(orders_join.is_some());
        assert_eq!(orders_join.unwrap().path.len(), 1);
        assert!(orders_relation.is_sharded);

        // order_items needs two joins to get to users - is_sharded = true
        let order_items_relation = db_schema.inner.get("public", "order_items").unwrap();
        let order_items_join = db_schema.get_sharded_join(order_items_relation);
        assert!(order_items_join.is_some());
        assert_eq!(order_items_join.unwrap().path.len(), 2);
        assert!(order_items_relation.is_sharded);

        assert_eq!(db_schema.joins_count(), 2);
    }

    #[test]
    fn test_is_sharded_flag_not_set_for_isolated_table() {
        let sharding_schema = sharding().sharded_table("users", "user_id").build();

        let mut db_schema = schema()
            .relation(table("isolated").oid(9999).column(pk("id")))
            .build();

        db_schema.computed_sharded_joins(&sharding_schema);

        let relation = db_schema.inner.get("public", "isolated").unwrap();
        assert!(!relation.is_sharded);
    }
}
