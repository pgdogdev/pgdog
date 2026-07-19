use crate::frontend::router::sharding::mapping::MappingResolver;

use super::Centroids;

#[derive(Debug)]
pub enum Operator<'a> {
    Shards(usize),
    Centroids {
        shards: usize,
        probes: usize,
        centroids: Centroids<'a>,
    },
    Mapping(MappingResolver<'a>),
}
