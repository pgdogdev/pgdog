use super::ListShards;
use crate::{
    config::{ShardedMapping, ShardedMappingKind},
    frontend::router::Ranges,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Mapping {
    Range(Vec<ShardedMapping>), // TODO: optimize with a BTreeMap.
    List(ListShards),           // Optimized.
}

impl Mapping {
    pub fn new(mappings: &[ShardedMapping]) -> Option<Self> {
        let range = mappings
            .iter()
            .filter(|m| m.kind == ShardedMappingKind::Range)
            .cloned()
            .collect::<Vec<_>>();
        let list = mappings.iter().any(|m| m.kind == ShardedMappingKind::List);

        if !range.is_empty() {
            Some(Self::Range(range))
        } else if list {
            Some(Self::List(ListShards::new(mappings)))
        } else {
            None
        }
    }

    pub fn valid(&self) -> bool {
        match self {
            Self::Range(_) => Ranges::new(&Some(self.clone()))
                .map(|r| r.valid())
                .unwrap_or(false),
            Self::List(_) => true,
        }
    }
}
