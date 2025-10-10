use pgdog_plugin::pg_query::protobuf::UpdateStmt;

use crate::frontend::router::Route;

use super::super::table::OwnedTable;

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentValue {
    Parameter(i32),
    Integer(i64),
    String(String),
    Boolean(bool),
    Null,
    Column(String),
}

impl AssignmentValue {
    pub fn as_parameter(&self) -> Option<i32> {
        if let Self::Parameter(param) = self {
            Some(*param)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    column: String,
    value: AssignmentValue,
}

impl Assignment {
    pub fn new(column: String, value: AssignmentValue) -> Self {
        Self { column, value }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn value(&self) -> &AssignmentValue {
        &self.value
    }
}

#[derive(Debug, Clone)]
pub struct ShardKeyRewritePlan {
    table: OwnedTable,
    route: Route,
    new_shard: Option<usize>,
    statement: UpdateStmt,
    assignments: Vec<Assignment>,
}

impl ShardKeyRewritePlan {
    pub fn new(
        table: OwnedTable,
        route: Route,
        new_shard: Option<usize>,
        statement: UpdateStmt,
        assignments: Vec<Assignment>,
    ) -> Self {
        Self {
            table,
            route,
            new_shard,
            statement,
            assignments,
        }
    }

    pub fn table(&self) -> &OwnedTable {
        &self.table
    }

    pub fn route(&self) -> &Route {
        &self.route
    }

    pub fn new_shard(&self) -> Option<usize> {
        self.new_shard
    }

    pub fn statement(&self) -> &UpdateStmt {
        &self.statement
    }

    pub fn assignments(&self) -> &[Assignment] {
        &self.assignments
    }

    pub fn set_new_shard(&mut self, shard: usize) {
        self.new_shard = Some(shard);
    }
}
