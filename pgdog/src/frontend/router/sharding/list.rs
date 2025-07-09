use std::collections::HashMap;

use super::{Error, Mapping, Shard, Value};

use crate::config::{FlexibleType, ShardedMapping, ShardedMappingKind};

#[derive(Debug)]
pub struct Lists<'a> {
    list: &'a ListShards,
}

impl<'a> Lists<'a> {
    pub fn new(mapping: &'a Option<Mapping>) -> Option<Self> {
        if let Some(Mapping::List(list)) = mapping {
            Some(Self { list })
        } else {
            None
        }
    }

    pub(super) fn shard(&self, value: &Value) -> Result<Shard, Error> {
        let integer = value.integer()?;
        let varchar = value.varchar()?;
        let uuid = value.uuid()?;

        if let Some(integer) = integer {
            self.list.shard(&FlexibleType::Integer(integer))
        } else if let Some(uuid) = uuid {
            println!("\n\n-> Got a match by uuid\n");
            self.list.shard(&FlexibleType::Uuid(uuid))
        } else if let Some(varchar) = varchar {
            self.list.shard(&FlexibleType::String(varchar.to_string()))
        } else {
            Ok(Shard::All)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListShards {
    mapping: HashMap<FlexibleType, usize>,
}

impl ListShards {
    pub fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    pub fn new(mappings: &[ShardedMapping]) -> Self {
        let mut mapping = HashMap::new();

        for map in mappings {
            println!(
                "incoming mapping: kind={:?} shard={} values={:?}",
                map.kind, map.shard, map.values
            );
        }

        for map in mappings
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
