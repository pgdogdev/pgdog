use uuid::Uuid;

pub mod ffi;

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

/// Shard a string value, parsing out a BIGINT or UUID.
pub fn shard_str(value: &str, shards: usize) -> Option<usize> {
    Some(match value.parse::<i64>() {
        Ok(value) => bigint(value) as usize % shards,
        Err(_) => match value.parse::<Uuid>() {
            Ok(value) => uuid(value) as usize % shards,
            Err(_) => return None,
        },
    })
}
