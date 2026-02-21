use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, ops::Deref, sync::Arc};

/// Schema name -> Table name -> Relation
pub type Relations = HashMap<String, HashMap<String, Relation>>;

/// The action to take when a referenced row is deleted or updated.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ForeignKeyAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl ForeignKeyAction {
    /// Parse from PostgreSQL's information_schema representation.
    pub fn from_pg_string(s: &str) -> Self {
        match s {
            "CASCADE" => Self::Cascade,
            "SET NULL" => Self::SetNull,
            "SET DEFAULT" => Self::SetDefault,
            "RESTRICT" => Self::Restrict,
            "NO ACTION" | _ => Self::NoAction,
        }
    }
}

/// A foreign key reference to another table's column.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Hash)]
pub struct ForeignKey {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Hash)]
pub struct Column {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
    pub column_default: String,
    pub is_nullable: bool,
    pub data_type: String,
    pub ordinal_position: i32,
    pub is_primary_key: bool,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Relation {
    pub schema: String,
    pub name: String,
    pub type_: String,
    pub owner: String,
    pub persistence: String,
    pub access_method: String,
    pub description: String,
    pub oid: i32,
    /// Columns indexed by name, ordered by ordinal position.
    pub columns: IndexMap<String, Column>,
    /// Whether this relation is sharded.
    #[serde(default)]
    pub is_sharded: bool,
}

impl Hash for Relation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
        self.type_.hash(state);
        self.owner.hash(state);
        self.persistence.hash(state);
        self.access_method.hash(state);
        self.description.hash(state);
        self.oid.hash(state);
        for (key, value) in &self.columns {
            key.hash(state);
            value.hash(state);
        }
        self.is_sharded.hash(state);
    }
}

impl Relation {
    /// Get schema where the table is located.
    pub fn schema(&self) -> &str {
        if self.schema.is_empty() {
            "public"
        } else {
            &self.schema
        }
    }

    /// This is an index.
    pub fn is_index(&self) -> bool {
        matches!(self.type_.as_str(), "index" | "partitioned index")
    }

    /// This is a table.
    pub fn is_table(&self) -> bool {
        matches!(self.type_.as_str(), "table" | "partitioned table")
    }

    /// This is a sequence.
    pub fn is_sequence(&self) -> bool {
        self.type_ == "sequence"
    }

    /// Columns by name, in ordinal position order.
    pub fn columns(&self) -> &IndexMap<String, Column> {
        &self.columns
    }

    /// Get ordered column names (in ordinal position order).
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|s| s.as_str())
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SchemaInner {
    pub search_path: Vec<String>,
    pub relations: Relations,
}

impl Hash for SchemaInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.search_path.hash(state);
        let mut entries: Vec<_> = self.relations.iter().collect();
        entries.sort_by_key(|(k, _)| *k);
        for (schema, tables) in entries {
            schema.hash(state);
            let mut table_entries: Vec<_> = tables.iter().collect();
            table_entries.sort_by_key(|(k, _)| *k);
            for (name, relation) in table_entries {
                name.hash(state);
                relation.hash(state);
            }
        }
    }
}

/// Load schema from database.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    inner: Arc<SchemaInner>,
}

impl Hash for Schema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl Schema {
    pub fn tables(&self) -> &Relations {
        &self.relations
    }

    /// Get a relation by schema and table name.
    pub fn get(&self, schema: &str, name: &str) -> Option<&Relation> {
        self.inner
            .relations
            .get(schema)
            .and_then(|tables| tables.get(name))
    }
}

impl Deref for Schema {
    type Target = SchemaInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Schema {
    pub fn new(inner: SchemaInner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
