use std::collections::HashSet;

use super::{Error, Shard, Value};

use crate::config::{ShardedMappingKind, ShardedTable};

#[derive(Debug)]
pub struct Lists<'a> {
    table: &'a ShardedTable,
}

impl<'a> Lists<'a> {
    pub(super) fn new(table: &'a ShardedTable) -> Option<Self> {
        if table
            .mappings
            .iter()
            .any(|m| m.kind == ShardedMappingKind::List)
        {
            Some(Self { table })
        } else {
            None
        }
    }

    pub(super) fn shard(&self, value: &Value) -> Result<Shard, Error> {
        let integer = value.integer()?;
        let varchar = value.varchar()?;

        for mapping in self
            .table
            .mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::List)
        {
            let list = List {
                values_str: &mapping.values_str,
                values_integer: &mapping.values_integer,
                shard: mapping.shard,
            };

            if let Some(integer) = &integer {
                if list.integer(integer)? {
                    return Ok(Shard::Direct(list.shard));
                }
            }

            if let Some(varchar) = varchar {
                if list.varchar(varchar)? {
                    return Ok(Shard::Direct(list.shard));
                }
            }
        }

        Ok(Shard::All)
    }
}

pub struct List<'a> {
    values_str: &'a HashSet<String>,
    values_integer: &'a HashSet<i64>,
    shard: usize,
}

impl<'a> List<'a> {
    fn integer(&self, value: &i64) -> Result<bool, Error> {
        Ok(self.values_integer.contains(value))
    }

    fn varchar(&self, value: &str) -> Result<bool, Error> {
        Ok(self.values_str.contains(value))
    }
}
