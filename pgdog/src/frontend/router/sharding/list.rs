use super::{Error, Mapping, Shard, Value};

use crate::config::FlexibleType;

pub use pgdog_config::sharding::ListShards;

#[derive(Debug, PartialEq, Eq)]
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
            Ok(self.list.shard(&FlexibleType::Integer(integer))?.into())
        } else if let Some(uuid) = uuid {
            Ok(self.list.shard(&FlexibleType::Uuid(uuid))?.into())
        } else if let Some(varchar) = varchar {
            Ok(self
                .list
                .shard(&FlexibleType::String(varchar.to_string()))?
                .into())
        } else {
            Ok(Shard::All)
        }
    }
}
