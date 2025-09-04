use super::{Error, Mapping, Value};
use crate::{
    config::{FlexibleType, ShardedMapping, ShardedMappingKind},
    frontend::router::parser::Shard,
};

#[derive(Debug)]
pub(crate) struct Ranges<'a> {
    mappings: &'a [ShardedMapping],
}

impl<'a> Ranges<'a> {
    pub(crate) fn new(mapping: &'a Option<Mapping>) -> Option<Self> {
        if let Some(Mapping::Range(mappings)) = mapping {
            Some(Self { mappings })
        } else {
            None
        }
    }

    pub(crate) fn valid(&self) -> bool {
        let bounds = self
            .mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::Range)
            .map(|m| [m.start.clone(), m.end.clone()]);

        let ranges: Vec<_> = self
            .mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::Range)
            .map(Range::new)
            .collect();

        for bound in bounds {
            for range in &ranges {
                let mut matches = 0;
                for value in &bound {
                    match value {
                        Some(FlexibleType::String(s)) => {
                            if range.varchar(s) {
                                matches += 1;
                            }
                        }
                        Some(FlexibleType::Integer(i)) => {
                            if range.integer(i) {
                                matches += 1;
                            }
                        }
                        _ => (),
                    }
                }

                if matches > 1 {
                    return false;
                }
            }
        }

        true
    }

    pub(super) fn shard(&self, value: &Value) -> Result<Shard, Error> {
        // These are quick and return None if the datatype isn't right.
        let integer = value.integer()?;
        let varchar = value.varchar()?;

        for mapping in self
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
pub(crate) struct Range<'a> {
    start: &'a Option<FlexibleType>,
    end: &'a Option<FlexibleType>,
    shard: usize,
}

impl<'a> Range<'a> {
    pub(crate) fn new(mapping: &'a ShardedMapping) -> Self {
        Self {
            start: &mapping.start,
            end: &mapping.end,
            shard: mapping.shard,
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
