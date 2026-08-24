use uuid::Uuid;

// pub mod context;
#[cfg(test)]
pub mod benchmark_simd;
pub mod context;
pub mod context_builder;
pub mod distance_simd_rust;
pub mod error;
pub mod ffi;
pub mod hasher;
pub mod lookup;
pub mod mapping;
pub mod operator;
pub mod schema;
pub mod tables;
#[cfg(test)]
pub mod test;
pub mod value;
pub mod vector;

pub use context::*;
pub use context_builder::*;
pub use error::Error;
pub use hasher::Hasher;
pub use lookup::{
    LookupCache, LookupStats, LookupTable, PendingLookup, ResolvedLookups, ShardOrLookup,
};
pub use mapping::Mapping;
pub use operator::*;
pub use pgdog_vector::Centroids;
pub use schema::SchemaSharder;
pub use tables::*;
pub use value::*;

/// Hash `BIGINT`.
pub fn bigint(id: i64) -> u64 {
    unsafe { ffi::hash_combine64(0, ffi::hashint8extended(id)) }
}

/// Hash UUID.
pub fn uuid(uuid: Uuid) -> u64 {
    unsafe {
        ffi::hash_combine64(
            0,
            ffi::hash_bytes_extended(uuid.as_bytes().as_ptr(), uuid.as_bytes().len() as i64),
        )
    }
}

/// Hash VARCHAR.
pub fn varchar(s: &[u8]) -> u64 {
    unsafe { ffi::hash_combine64(0, ffi::hash_bytes_extended(s.as_ptr(), s.len() as i64)) }
}

/// Shard a value that's coming out of the query text directly.
#[cfg(test)]
pub(crate) fn shard_value(
    value: &str,
    data_type: &crate::config::DataType,
    shards: usize,
    centroids: &Vec<crate::net::vector::Vector>,
    centroid_probes: usize,
) -> super::parser::Shard {
    use super::parser::Shard;
    use crate::config::DataType;
    use crate::net::vector::str_to_vector;

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
