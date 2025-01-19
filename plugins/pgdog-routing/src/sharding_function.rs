// PostgreSQL hash function.

#[link(name = "postgres_hash")]
extern "C" {
    fn hash_bytes_extended(k: *const u8, keylen: i64) -> u64;
}

fn hash(k: &[u8]) -> u64 {
    unsafe { hash_bytes_extended(k.as_ptr(), k.len() as i64) }
}

/// Calculate shard for a BIGINT value.
pub fn bigint(value: i64, shards: usize) -> usize {
    hash(&value.to_ne_bytes()) as usize % shards
}

/// Calculate shard for a string value.
pub fn string(value: &str, shards: usize) -> usize {
    hash(&value.as_bytes()) as usize % shards
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

        for _ in 0..2048 {
            let v = rand::thread_rng().gen::<i64>();
            // Our hashing function.
            let shard = bigint(v as i64, 4);

            // Check that Postgres did the same thing.
            // Note: we are selecting from the subtable, not the parent table.
            let table = format!("sharding_func_{}", shard);
            let query = format!("SELECT id FROM {} WHERE id = {}", table, v);
            let count = client.query(&query, &[]).expect("query");
            for row in count {
                let id: i64 = row.get(0);
                assert_eq!(id, v);
            }
        }
    }
}
