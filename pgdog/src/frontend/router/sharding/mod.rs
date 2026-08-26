use uuid::Uuid;

pub(crate) mod context;
pub(crate) mod context_builder;
pub(crate) mod error;
pub(crate) mod ffi;
pub(crate) mod hasher;
pub(crate) mod lookup;
pub(crate) mod mapping;
pub(crate) mod operator;
pub(crate) mod schema;
pub(crate) mod tables;
#[cfg(test)]
pub(crate) mod test;
pub(crate) mod value;

pub(crate) use context::*;
pub(crate) use context_builder::*;
pub(crate) use error::Error;
pub(crate) use hasher::Hasher;
pub(crate) use lookup::{
    LookupCache, LookupStats, LookupTable, PendingLookup, ResolvedLookups, ShardOrLookup,
};
pub(crate) use mapping::Mapping;
pub(crate) use operator::*;
pub(crate) use pgdog_vector::Centroids;
pub(crate) use schema::SchemaSharder;
pub(crate) use tables::*;
pub(crate) use value::*;

/// Hash `BIGINT`.
pub(crate) fn bigint(id: i64) -> u64 {
    unsafe { ffi::hash_combine64(0, ffi::hashint8extended(id)) }
}

/// Hash UUID.
pub(crate) fn uuid(uuid: Uuid) -> u64 {
    unsafe {
        ffi::hash_combine64(
            0,
            ffi::hash_bytes_extended(uuid.as_bytes().as_ptr(), uuid.as_bytes().len() as i64),
        )
    }
}

/// Hash VARCHAR.
pub(crate) fn varchar(s: &[u8]) -> u64 {
    unsafe { ffi::hash_combine64(0, ffi::hash_bytes_extended(s.as_ptr(), s.len() as i64)) }
}

#[cfg(test)]
pub(crate) use test_impls::shard_value;
#[cfg(test)]
mod test_impls {
    use super::{Centroids, bigint, uuid, varchar};
    use crate::config::DataType;
    use crate::frontend::router::parser::Shard;
    use crate::net::{messages::Vector, vector::str_to_vector};

    /// Shard a value that's coming out of the query text directly.
    pub(crate) fn shard_value(
        value: &str,
        data_type: &DataType,
        shards: usize,
        centroids: &Vec<Vector>,
        centroid_probes: usize,
    ) -> Shard {
        match data_type {
            DataType::Bigint => value
                .parse()
                .map(|v| bigint(v) as usize % shards)
                .ok()
                .map(Shard::Direct)
                .unwrap_or(Shard::All),
            DataType::Uuid => value
                .parse()
                .map(|v| uuid(v) as usize % shards)
                .ok()
                .map(Shard::Direct)
                .unwrap_or(Shard::All),
            DataType::Vector => str_to_vector(value)
                .ok()
                .map(|v| {
                    Centroids::from(centroids)
                        .shard(&v, shards, centroid_probes)
                        .into()
                })
                .unwrap_or(Shard::All),
            DataType::Varchar => Shard::Direct(varchar(value.as_bytes()) as usize % shards),
        }
    }
}
