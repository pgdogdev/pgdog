use super::Centroids;
use crate::config::{ShardListMap, ShardRangeMap};

#[derive(Debug)]
pub enum Operator<'a> {
    Shards(usize),
    Centroids {
        shards: usize,
        probes: usize,
        centroids: Centroids<'a>,
    },
    Lists(ShardListMap),
    Ranges(ShardRangeMap),
}
