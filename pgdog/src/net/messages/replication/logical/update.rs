use pgdog_postgres_types::Oid;

use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::{Column, Identifier, TupleData};

/// WAL UPDATE record. Use with [`Table::update`](crate::backend::replication::logical::publisher::Table::update)
/// or [`Table::update_partial`](crate::backend::replication::logical::publisher::Table::update_partial).
#[derive(Debug, Clone)]
pub struct Update {
    pub oid: Oid,
    pub key: Option<TupleData>,
    pub old: Option<TupleData>,
    pub new: TupleData,
}

impl Update {
    /// Get column at index.
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.new.columns.get(index)
    }

    /// Filters unchanged-TOAST columns out of `new` for use with
    /// [`Table::update_partial`](crate::backend::replication::logical::publisher::Table::update_partial).
    pub fn partial_new(&self) -> TupleData {
        TupleData {
            columns: self
                .new
                .columns
                .iter()
                .filter(|c| c.identifier != Identifier::Toasted)
                .cloned()
                .collect(),
        }
    }
}

impl FromBytes for Update {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'U');
        let oid = Oid(bytes.get_u32());
        let identifier = bytes.get_u8() as char;

        let key = if identifier == 'K' {
            let key = TupleData::from_buffer(&mut bytes)?;
            Some(key)
        } else {
            None
        };

        let old = if identifier == 'O' {
            let old = TupleData::from_buffer(&mut bytes)?;
            Some(old)
        } else {
            None
        };

        let new = if identifier == 'N' {
            TupleData::from_bytes(bytes)?
        } else {
            code!(bytes, 'N');
            TupleData::from_bytes(bytes)?
        };

        Ok(Self { oid, key, old, new })
    }
}

impl ToBytes for Update {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        use bytes::BufMut;
        let mut buf = bytes::BytesMut::new();
        buf.put_u8(b'U');
        buf.put_u32(self.oid.0);
        if let Some(ref key) = self.key {
            buf.put_u8(b'K');
            buf.put(key.to_bytes()?);
        } else if let Some(ref old) = self.old {
            buf.put_u8(b'O');
            buf.put(old.to_bytes()?);
        }
        buf.put_u8(b'N');
        buf.put(self.new.to_bytes()?);
        Ok(buf.freeze())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::messages::replication::logical::tuple_data::{
        text_col, toasted_col, TupleData,
    };
    use pgdog_postgres_types::Oid;

    fn update(columns: Vec<super::Column>) -> Update {
        Update {
            oid: Oid(1),
            key: None,
            old: None,
            new: TupleData { columns },
        }
    }

    #[test]
    fn partial_new_all_present() {
        // No TOAST columns — every column passes through unchanged.
        let result = update(vec![text_col("1"), text_col("x"), text_col("y")]).partial_new();
        assert_eq!(result.columns.len(), 3);
        assert!(result
            .columns
            .iter()
            .all(|c| c.identifier != Identifier::Toasted));
        let bind = result.to_bind("__pgdog");
        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("1"));
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("x"));
        assert_eq!(bind.parameter(2).unwrap().unwrap().text(), Some("y"));
    }

    #[test]
    fn partial_new_filters_toasted_columns() {
        // id, a, b (TOAST), c — b is dropped; bind order is id($1), a($2), c($3).
        let bind = update(vec![
            text_col("42"),
            text_col("aa"),
            toasted_col(),
            text_col("cc"),
        ])
        .partial_new()
        .to_bind("__pgdog_shape");
        assert_eq!(bind.statement(), "__pgdog_shape");
        assert_eq!(bind.parameter(0).unwrap().unwrap().bigint(), Some(42)); // id
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("aa")); // a
        assert_eq!(bind.parameter(2).unwrap().unwrap().text(), Some("cc")); // c
        assert!(matches!(bind.parameter(3), Ok(None) | Err(_))); // b was dropped
    }

    #[test]
    fn partial_new_identity_in_middle() {
        // a, id (middle), b (TOAST) — b is dropped; bind order is a($1), id($2).
        let bind = update(vec![text_col("av"), text_col("1"), toasted_col()])
            .partial_new()
            .to_bind("__pgdog");
        assert_eq!(bind.statement(), "__pgdog");
        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("av")); // a
        assert_eq!(bind.parameter(1).unwrap().unwrap().bigint(), Some(1)); // id
        assert!(matches!(bind.parameter(2), Ok(None) | Err(_)));
    }

    #[test]
    fn partial_new_multiple_identity_columns() {
        // id1, id2, a (TOAST), b — a is dropped; bind order is id1($1), id2($2), b($3).
        let bind = update(vec![
            text_col("1"),
            text_col("2"),
            toasted_col(),
            text_col("bv"),
        ])
        .partial_new()
        .to_bind("__pgdog");
        assert_eq!(bind.statement(), "__pgdog");
        assert_eq!(bind.parameter(0).unwrap().unwrap().bigint(), Some(1)); // id1
        assert_eq!(bind.parameter(1).unwrap().unwrap().bigint(), Some(2)); // id2
        assert_eq!(bind.parameter(2).unwrap().unwrap().text(), Some("bv")); // b
        assert!(matches!(bind.parameter(3), Ok(None) | Err(_))); // a was dropped
    }

    #[test]
    fn partial_new_two_identity_interleaved_two_toasted() {
        // a (TOAST), id1, b, c (TOAST), id2 — a and c dropped; bind order is id1($1), b($2), id2($3).
        let bind = update(vec![
            toasted_col(),  // a — dropped
            text_col("1"),  // id1
            text_col("bv"), // b
            toasted_col(),  // c — dropped
            text_col("2"),  // id2
        ])
        .partial_new()
        .to_bind("__pgdog");
        assert_eq!(bind.statement(), "__pgdog");
        assert_eq!(bind.parameter(0).unwrap().unwrap().bigint(), Some(1)); // id1
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("bv")); // b
        assert_eq!(bind.parameter(2).unwrap().unwrap().bigint(), Some(2)); // id2
        assert!(matches!(bind.parameter(3), Ok(None) | Err(_))); // no 4th param
    }
}
