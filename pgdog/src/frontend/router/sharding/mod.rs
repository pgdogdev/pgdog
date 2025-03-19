use uuid::Uuid;

use crate::{
    backend::ShardingSchema,
    net::messages::{Format, FromDataType, Vector},
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
    if value.starts_with('[') {
        let vector = Vector::decode(value.as_bytes(), Format::Text).ok();
        if let Some(vector) = vector {
            return schema.shard_by_distance_l2(&vector);
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_shard_str() {
        let schema = ShardingSchema {
            shards: 2,
            centroids: vec![
                Some(Vector::from(&[0.0, 1.0, 2.0][..])),
                Some(Vector::from(&[1.0, 2.0, 3.0][..])),
            ],
            ..Default::default()
        };
        let shard = shard_str("[1,2,3]", &schema);
        assert_eq!(shard, Some(1));
        let shard = shard_str("[0.0,0.5,0.1]", &schema);
        assert_eq!(shard, Some(0));
    }
}
