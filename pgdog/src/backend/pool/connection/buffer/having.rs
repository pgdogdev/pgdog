//! Merged `HAVING` operands, sent back to PostgreSQL for evaluation.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};

use crate::net::{
    Decoder,
    messages::{DataRow, Format, bind::Parameter},
};

use super::super::aggregate::Error;

/// One merged group per row, one column per `HAVING` operand.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HavingInputs {
    rows: usize,
    columns: Vec<HavingColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HavingColumn {
    type_oid: i32,
    format: Format,
    values: Vec<Option<Bytes>>,
}

impl HavingInputs {
    pub(super) fn new(
        rows: &VecDeque<DataRow>,
        aliases: &[String],
        decoder: &Decoder,
    ) -> Result<Self, Error> {
        let row_description = decoder.row_description();
        let columns = aliases
            .iter()
            .map(|alias| {
                let index = row_description.field_index(alias).ok_or_else(|| {
                    Error::InvalidHaving(format!("missing HAVING column \"{alias}\""))
                })?;
                let field = row_description
                    .field(index)
                    .expect("field_index returned a valid column");
                Ok(HavingColumn {
                    type_oid: field.type_oid,
                    format: decoder.get_format(index),
                    values: rows
                        .iter()
                        .map(|row| {
                            row.get_raw(index)
                                .filter(|data| !data.is_null)
                                .map(|data| data.data.clone())
                        })
                        .collect(),
                })
            })
            .collect::<Result<_, Error>>()?;

        Ok(Self {
            rows: rows.len(),
            columns,
        })
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn columns(&self) -> &[HavingColumn] {
        &self.columns
    }
}

impl HavingColumn {
    /// Array type for this column, if it's a built-in type.
    pub(crate) fn array_oid(&self) -> Option<u32> {
        array_oid(self.type_oid)
    }

    /// Encode every value of this column as a one-dimensional array parameter.
    pub(crate) fn array(&self) -> (Parameter, Format) {
        let mut array = BytesMut::new();
        match self.format {
            Format::Binary => {
                array.put_i32(1);
                array.put_i32(self.values.iter().any(Option::is_none) as i32);
                array.put_i32(self.type_oid);
                array.put_i32(self.values.len() as i32);
                array.put_i32(1);
                for value in &self.values {
                    match value {
                        Some(value) => {
                            array.put_i32(value.len() as i32);
                            array.put_slice(value);
                        }
                        None => array.put_i32(-1),
                    }
                }
            }
            Format::Text => {
                array.put_u8(b'{');
                for (position, value) in self.values.iter().enumerate() {
                    if position > 0 {
                        array.put_u8(b',');
                    }
                    let Some(value) = value else {
                        array.put_slice(b"NULL");
                        continue;
                    };
                    array.put_u8(b'"');
                    for &byte in value.iter() {
                        if matches!(byte, b'"' | b'\\') {
                            array.put_u8(b'\\');
                        }
                        array.put_u8(byte);
                    }
                    array.put_u8(b'"');
                }
                array.put_u8(b'}');
            }
        }
        (Parameter::new(&array), self.format)
    }
}

/// Array OIDs of built-in types whose text array delimiter is a comma.
fn array_oid(element: i32) -> Option<u32> {
    Some(match element {
        16 => 1000,   // bool
        20 => 1016,   // int8
        21 => 1005,   // int2
        23 => 1007,   // int4
        25 => 1009,   // text
        700 => 1021,  // float4
        701 => 1022,  // float8
        1042 => 1014, // bpchar
        1043 => 1015, // varchar
        1082 => 1182, // date
        1114 => 1115, // timestamp
        1184 => 1185, // timestamptz
        1186 => 1187, // interval
        1700 => 1231, // numeric
        2950 => 2951, // uuid
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_text_and_binary_arrays_with_nulls() {
        let text = HavingColumn {
            type_oid: 25,
            format: Format::Text,
            values: vec![Some(Bytes::from_static(br#"a"b\c"#)), None],
        };
        assert_eq!(text.array_oid(), Some(1009));
        assert_eq!(text.array().0.data.as_ref(), br#"{"a\"b\\c",NULL}"#);

        let binary = HavingColumn {
            type_oid: 20,
            format: Format::Binary,
            values: vec![Some(Bytes::copy_from_slice(&7_i64.to_be_bytes())), None],
        };
        let mut expected = Vec::new();
        for header in [1_i32, 1, 20, 2, 1, 8] {
            expected.extend_from_slice(&header.to_be_bytes());
        }
        expected.extend_from_slice(&7_i64.to_be_bytes());
        expected.extend_from_slice(&(-1_i32).to_be_bytes());
        assert_eq!(binary.array().0.data.as_ref(), expected.as_slice());
    }
}
