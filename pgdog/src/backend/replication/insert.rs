use crate::net::messages::replication::{Insert, Relation};

use super::Error;

#[derive(Debug, Default)]
pub(super) struct InsertBuffer {
    messages: Vec<Insert>,
}

impl InsertBuffer {
    pub(super) fn add(&mut self, message: Insert) {
        self.messages.push(message);
    }

    pub(super) fn to_sql(&self, relation: &Relation) -> Result<String, Error> {
        let values = self
            .messages
            .iter()
            .map(|m| m.tuple_data.to_sql())
            .collect::<Result<Vec<_>, crate::net::Error>>()?
            .join(", ");
        let sql = format!(r#"INSERT INTO {} VALUES {}"#, relation.to_sql()?, values,);
        Ok(sql)
    }
}
