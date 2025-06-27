use super::{Error, Value};
use crate::{
    config::{FlexibleType, ShardedMapping, ShardedMappingKind, ShardedTable},
    frontend::router::parser::Shard,
};

#[derive(Debug)]
pub struct Ranges<'a> {
    table: &'a ShardedTable,
}

impl<'a> Ranges<'a> {
    pub(super) fn new(table: &'a ShardedTable) -> Option<Self> {
        if table
            .mappings
            .iter()
            .any(|m| m.kind == ShardedMappingKind::Range)
        {
            Some(Self { table })
        } else {
            None
        }
    }

    pub(super) fn shard(&self, value: &Value) -> Result<Shard, Error> {
        // These are quick and return None if the datatype isn't right.
        let integer = value.integer()?;
        let varchar = value.varchar()?;

        for mapping in self
            .table
            .mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::Range)
        {
            let range = Range::new(mapping);
            if let Some(integer) = &integer {
                if range.integer(integer) {
                    return Ok(Shard::Direct(range.shard));
                }
            }

            if let Some(varchar) = &varchar {
                if range.varchar(varchar) {
                    return Ok(Shard::Direct(range.shard));
                }
            }
        }

        Ok(Shard::All)
    }
}

#[derive(Debug)]
pub struct Range<'a> {
    start: &'a Option<FlexibleType>,
    end: &'a Option<FlexibleType>,
    shard: usize,
}

impl<'a> Range<'a> {
    pub fn new(mapping: &'a ShardedMapping) -> Self {
        Self {
            start: &mapping.start,
            end: &mapping.end,
            shard: mapping.shard,
        }
    }

    #[allow(dead_code)]
    fn matches(&self, value: &Value) -> Result<bool, Error> {
        if let Some(integer) = value.integer()? {
            Ok(self.integer(&integer))
        } else if let Some(varchar) = value.varchar()? {
            Ok(self.varchar(varchar))
        } else {
            Ok(false)
        }
    }

    fn integer(&self, value: &i64) -> bool {
        if let Some(FlexibleType::Integer(start)) = self.start {
            if let Some(FlexibleType::Integer(end)) = self.end {
                value >= start && value < end
            } else {
                value >= start
            }
        } else if let Some(FlexibleType::Integer(end)) = self.end {
            value < end
        } else {
            false
        }
    }

    fn varchar(&self, value: &str) -> bool {
        if let Some(FlexibleType::String(start)) = self.start {
            if let Some(FlexibleType::String(end)) = self.end {
                value >= start.as_str() && value < end.as_str()
            } else {
                value >= start.as_str()
            }
        } else if let Some(FlexibleType::String(end)) = self.end {
            value < end.as_str()
        } else {
            false
        }
    }
}
