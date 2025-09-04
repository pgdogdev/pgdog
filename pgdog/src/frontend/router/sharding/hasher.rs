//! Sharding hasher.

use sha1::{Digest, Sha1};
use uuid::Uuid;

use super::{bigint, uuid, varchar};

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum Hasher {
    Postgres,
    Sha1,
}

impl Hasher {
    pub(crate) fn bigint(&self, value: i64) -> u64 {
        match self {
            Hasher::Postgres => bigint(value),
            Hasher::Sha1 => Self::sha1(value.to_string().as_bytes()),
        }
    }

    pub(crate) fn uuid(&self, value: Uuid) -> u64 {
        match self {
            Hasher::Postgres => uuid(value),
            Hasher::Sha1 => Self::sha1(value.as_bytes()),
        }
    }

    pub(crate) fn varchar(&self, value: &[u8]) -> u64 {
        match self {
            Hasher::Postgres => varchar(value),
            Hasher::Sha1 => Self::sha1(value),
        }
    }

    fn sha1(bytes: &[u8]) -> u64 {
        let mut hasher = Sha1::new();
        hasher.update(bytes);
        let hash = hasher.finalize();

        let hex = format!("{:x}", hash);
        let key = i64::from_str_radix(&hex[hex.len() - 8..], 16).unwrap();

        key as u64
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sha1_hash() {
        let ids = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let shards = [
            4, 7, 8, 3, 6, 0, 0, 10, 3, 11, 1, 7, 4, 4, 11, 2, 5, 0, 8, 3,
        ];

        for (id, expected) in ids.iter().zip(shards.iter()) {
            let hash = Hasher::Sha1.bigint(*id as i64);
            let shard = hash % 12;
            assert_eq!(shard as u64, *expected as u64);
        }
    }
}
