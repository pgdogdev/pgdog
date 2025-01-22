#[link(name = "postgres_hash")]
extern "C" {
    /// Hash any size data using its bytes representation.
    pub(super) fn hash_bytes_extended(k: *const u8, keylen: i64) -> u64;
    /// Special hashing function for BIGINT (i64).
    pub(super) fn hashint8extended(k: i64) -> u64;
    /// Combine multiple hashes into one in the case of multi-column hashing keys.
    pub(super) fn hash_combine64(a: u64, b: u64) -> u64;
}
