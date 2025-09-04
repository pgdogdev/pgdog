use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};

use super::{Error, Mapping, Shard, Value};

use crate::config::{FlexibleType, ShardedMapping, ShardedMappingKind};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Lists<'a> {
    list: &'a ListShards,
}

impl<'a> Lists<'a> {
    pub(crate) fn new(mapping: &'a Option<Mapping>) -> Option<Self> {
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
            self.list.shard(&FlexibleType::Uuid(uuid))
        } else if let Some(varchar) = varchar {
            self.list.shard(&FlexibleType::String(varchar.to_string()))
        } else {
            Ok(Shard::All)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListShards {
    mapping: HashMap<FlexibleType, usize>,
}

impl Hash for ListShards {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the mapping in a deterministic way by XORing individual key-value hashes
        let mut mapping_hash = 0u64;
        for (key, value) in &self.mapping {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            value.hash(&mut hasher);
            mapping_hash ^= hasher.finish();
        }
        mapping_hash.hash(state);
    }
}

impl ListShards {
    pub(crate) fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    pub(crate) fn new(mappings: &[ShardedMapping]) -> Self {
        let mut mapping = HashMap::new();

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

    pub(crate) fn shard(&self, value: &FlexibleType) -> Result<Shard, Error> {
        if let Some(shard) = self.mapping.get(value) {
            Ok(Shard::Direct(*shard))
        } else {
            Ok(Shard::All)
        }
    }
}
