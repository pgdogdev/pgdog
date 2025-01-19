//! PostgreSQL hash functions.
//!
//! This module delegates most of the hashing work directly
//! to PostgreSQL internal functions that we copied in `postgres_hash` C library.
//!

use uuid::Uuid;

#[link(name = "postgres_hash")]
extern "C" {
    /// Hash any size data using its bytes representation.
    fn hash_bytes_extended(k: *const u8, keylen: i64) -> u64;
    /// Special hashing function for BIGINT (i64).
    fn hashint8extended(k: i64) -> u64;
    /// Combine multiple hashes into one in the case of multi-column hashing keys.
    fn hash_combine64(a: u64, b: u64) -> u64;
}

/// Safe wrapper around `hash_bytes_extended`.
fn hash_slice(k: &[u8]) -> u64 {
    unsafe { hash_bytes_extended(k.as_ptr(), k.len() as i64) }
}

/// Calculate shard for a BIGINT value.
pub fn bigint(value: i64, shards: usize) -> usize {
    let hash = unsafe { hashint8extended(value) };
    let combined = unsafe { hash_combine64(0, hash as u64) };

    combined as usize % shards
}

/// Calculate shard for a UUID value.
pub fn uuid(value: Uuid, shards: usize) -> usize {
    let hash = hash_slice(value.as_bytes().as_slice());
    let combined = unsafe { hash_combine64(0, hash) };

    combined as usize % shards
}

#[cfg(test)]
mod test {
    use super::*;
    use postgres::{Client, NoTls};
    use rand::Rng;

    #[test]
    fn test_bigint() {
        let tables = r#"
        BEGIN;

        DROP TABLE IF EXISTS sharding_func CASCADE;

        CREATE TABLE sharding_func (id BIGINT)
        PARTITION BY HASH(id);

        CREATE TABLE sharding_func_0
        PARTITION OF sharding_func
        FOR VALUES WITH (modulus 4, remainder 0);

        CREATE TABLE sharding_func_1
        PARTITION OF sharding_func
        FOR VALUES WITH (modulus 4, remainder 1);

        CREATE TABLE sharding_func_2
        PARTITION OF sharding_func
        FOR VALUES WITH (modulus 4, remainder 2);

        CREATE TABLE sharding_func_3
        PARTITION OF sharding_func
        FOR VALUES WITH (modulus 4, remainder 3);
        "#;

        let mut client = Client::connect(
            "host=localhost user=pgdog password=pgdog dbname=pgdog",
            NoTls,
        )
        .expect("client to connect");

        client.batch_execute(tables).expect("create tables");

        for _ in 0..4096 {
            let v = rand::thread_rng().gen::<i64>();
            // Our hashing function.
            let shard = bigint(v as i64, 4);

            // Check that Postgres did the same thing.
            // Note: we are inserting directly into the subtable.
            let table = format!("sharding_func_{}", shard);
            client
                .query(&format!("INSERT INTO {} (id) VALUES ($1)", table), &[&v])
                .expect("insert");
        }
    }

    #[test]
    fn test_uuid() {
        let tables = r#"
        BEGIN;

        DROP TABLE IF EXISTS sharding_func_uuid CASCADE;

        CREATE TABLE sharding_func_uuid (id UUID)
        PARTITION BY HASH(id);

        CREATE TABLE sharding_func_uuid_0
        PARTITION OF sharding_func_uuid
        FOR VALUES WITH (modulus 4, remainder 0);

        CREATE TABLE sharding_func_uuid_1
        PARTITION OF sharding_func_uuid
        FOR VALUES WITH (modulus 4, remainder 1);

        CREATE TABLE sharding_func_uuid_2
        PARTITION OF sharding_func_uuid
        FOR VALUES WITH (modulus 4, remainder 2);

        CREATE TABLE sharding_func_uuid_3
        PARTITION OF sharding_func_uuid
        FOR VALUES WITH (modulus 4, remainder 3);
        "#;

        let mut client = Client::connect(
            "host=localhost user=pgdog password=pgdog dbname=pgdog",
            NoTls,
        )
        .expect("client to connect");

        client.batch_execute(tables).expect("create tables");

        for _ in 0..4096 {
            let v = Uuid::new_v4();
            // Our hashing function.
            let shard = uuid(v, 4);

            // Check that Postgres did the same thing.
            // Note: we are inserting directly into the subtable.
            let table = format!("sharding_func_uuid_{}", shard);
            client
                .query(&format!("INSERT INTO {} (id) VALUES ($1)", table), &[&v])
                .expect("insert");
        }
    }
}
