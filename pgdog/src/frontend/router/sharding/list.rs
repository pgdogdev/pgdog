use std::collections::{HashMap, HashSet};

use super::{Error, Shard, Value};

use crate::config::{FlexibleType, ShardedMappingKind, ShardedTable};

#[derive(Debug)]
pub struct Lists<'a> {
    table: &'a ShardedTable,
}

impl<'a> Lists<'a> {
    pub fn new(table: &'a ShardedTable) -> Option<Self> {
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

    pub fn valid(&self) -> bool {
        let a = self
            .table
            .mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::List);

        let b = a.clone();

        for a in a {
            let mut matches = 0;
            for b in b.clone() {
                for va in &a.values_integer {
                    if b.values_integer.contains(va) {
                        matches += 1;
                        break;
                    }
                }

                for va in &a.values_str {
                    if b.values_str.contains(va) {
                        matches += 1;
                        break;
                    }
                }
            }

            if matches > 1 {
                return false;
            }
        }

        true
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

#[derive(Debug)]
pub struct ListShards {
    mapping: HashMap<FlexibleType, usize>,
}

impl ListShards {
    pub fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    pub fn new(table: &ShardedTable) -> Self {
        let mut mapping = HashMap::new();
        for map in table
            .mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::List)
        {
            for value in &map.values {
                mapping.insert(value.clone(), map.shard);
            }
        }

        Self { mapping }
    }

    pub fn shard(&self, value: &FlexibleType) -> Result<Shard, Error> {
        if let Some(shard) = self.mapping.get(value) {
            Ok(Shard::Direct(*shard))
        } else {
            Ok(Shard::All)
        }
    }
}
