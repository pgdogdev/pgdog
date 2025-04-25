use crate::{
    backend::ShardingSchema,
    config::{DataType, ShardedTable},
    net::Vector,
};

#[derive(Debug, Clone)]
pub struct Context<'a> {
    shards: usize,
    data_type: DataType,
    centroids: &'a [Vector],
    centroid_probes: usize,
}

impl<'a> Context<'a> {
    pub(crate) fn from_table(table: &'a ShardedTable, schema: &ShardingSchema) -> Self {
        Self {
            shards: schema.shards,
            data_type: table.data_type,
            centroids: &table.centroids,
            centroid_probes: table.centroid_probes,
        }
    }

    pub(crate) fn from_str(vale: &str, schema: &ShardingSchema) -> Self {
        Self {
            shards: schema.shards,
            centroids: &[],
            centroid_probes: 0,
            data_type: DataType::Bigint,
        }
    }
}
