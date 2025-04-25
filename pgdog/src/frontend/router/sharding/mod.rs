use uuid::Uuid;

use crate::{
    backend::ShardingSchema,
    config::{DataType, ShardedTable},
    net::messages::{Format, FromDataType, ParameterWithFormat, Vector},
};

// pub mod context;
pub mod ffi;
pub mod vector;

pub use vector::{Centroids, Distance};

use super::parser::Shard;

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

/// Shard an integer.
pub fn shard_int(value: i64, schema: &ShardingSchema) -> Shard {
    Shard::direct(bigint(value) as usize % schema.shards)
}

/// Shard a string value, parsing out a BIGINT, UUID, or vector.
///
/// TODO: This is really not great, we should pass in the type oid
/// from RowDescription in here to avoid guessing.
pub fn shard_str(
    value: &str,
    schema: &ShardingSchema,
    centroids: &Vec<Vector>,
    centroid_probes: usize,
) -> Shard {
    let data_type = if value.starts_with('[') && value.ends_with(']') {
        DataType::Vector
    } else if value.parse::<i64>().is_ok() {
        DataType::Bigint
    } else {
        DataType::Uuid
    };
    shard_value(value, &data_type, schema.shards, centroids, centroid_probes)
}

/// Shard a value that's coming out of the query text directly.
pub fn shard_value(
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
        DataType::Vector => Vector::try_from(value)
            .ok()
            .map(|v| Centroids::from(centroids).shard(&v, shards, centroid_probes))
            .unwrap_or(Shard::All),
    }
}

pub fn shard_binary(
    bytes: &[u8],
    data_type: &DataType,
    shards: usize,
    centroids: &Vec<Vector>,
    centroid_probes: usize,
) -> Shard {
    match data_type {
        DataType::Bigint => i64::decode(bytes, Format::Binary)
            .ok()
            .map(|i| Shard::direct(bigint(i) as usize % shards))
            .unwrap_or(Shard::All),
        DataType::Uuid => Uuid::decode(bytes, Format::Binary)
            .ok()
            .map(|u| Shard::direct(uuid(u) as usize % shards))
            .unwrap_or(Shard::All),
        DataType::Vector => Vector::decode(bytes, Format::Binary)
            .ok()
            .map(|v| Centroids::from(centroids).shard(&v, shards, centroid_probes))
            .unwrap_or(Shard::All),
    }
}

/// Shard query parameter.
pub fn shard_param(value: &ParameterWithFormat, table: &ShardedTable, shards: usize) -> Shard {
    match value.format() {
        Format::Binary => shard_binary(
            value.data(),
            &table.data_type,
            shards,
            &table.centroids,
            table.centroid_probes,
        ),
        Format::Text => value
            .text()
            .map(|v| {
                shard_value(
                    v,
                    &table.data_type,
                    shards,
                    &table.centroids,
                    table.centroid_probes,
                )
            })
            .unwrap_or(Shard::All),
    }
}
