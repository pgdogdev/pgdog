use uuid::Uuid;

use crate::{
    backend::ShardingSchema,
    config::{DataType, ShardedTable},
    net::messages::{Format, FromDataType, ParameterWithFormat, Vector},
};

pub mod ffi;
pub mod vector;

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
pub fn shard_int(value: i64, schema: &ShardingSchema) -> usize {
    bigint(value) as usize % schema.shards
}

/// Shard a string value, parsing out a BIGINT, UUID, or vector.
///
/// TODO: This is really not great, we should pass in the type oid
/// from RowDescription in here to avoid guessing.
pub fn shard_str(value: &str, schema: &ShardingSchema) -> Option<usize> {
    let shards = schema.shards;
    if value.starts_with('[') && value.ends_with(']') {
        let vector = Vector::decode(value.as_bytes(), Format::Text).ok();
        if let Some(_vector) = vector {
            // TODO: make sharding work.
        }
    }
    Some(match value.parse::<i64>() {
        Ok(value) => bigint(value) as usize % shards,
        Err(_) => match value.parse::<Uuid>() {
            Ok(value) => uuid(value) as usize % shards,
            Err(_) => return None,
        },
    })
}

/// Shard a value that's coming out of the query text directly.
pub fn shard_value(value: &str, table: &ShardedTable, shards: usize) -> Option<usize> {
    match table.data_type {
        DataType::Bigint => value.parse().map(|v| bigint(v) as usize % shards).ok(),
        DataType::Uuid => value.parse().map(|v| uuid(v) as usize % shards).ok(),
    }
}

/// Shard query parameter.
pub fn shard_param(
    value: &ParameterWithFormat,
    table: &ShardedTable,
    shards: usize,
) -> Option<usize> {
    match table.data_type {
        DataType::Bigint => value.bigint().map(|i| bigint(i) as usize % shards),
        DataType::Uuid => value.uuid().map(|v| uuid(v) as usize % shards),
    }
}
