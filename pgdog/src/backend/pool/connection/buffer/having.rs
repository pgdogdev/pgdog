//! Merged `HAVING` operands, sent back to PostgreSQL for evaluation.

use std::collections::VecDeque;

use bytes::{BufMut, BytesMut};

use crate::net::{
    Decoder,
    messages::{DataRow, Format, bind::Parameter},
};

use super::super::aggregate::Error;

/// One array parameter per `HAVING` operand, with one element per merged group.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HavingInputs {
    rows: usize,
    columns: Vec<HavingColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HavingColumn {
    element_oid: i32,
    format: Format,
    array: Parameter,
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
                let index = row_description
                    .field_index(alias)
                    .ok_or_else(|| Error::HavingColumnMissing(alias.clone()))?;
                let element_oid = row_description
                    .field(index)
                    .expect("field_index returned a valid column")
                    .type_oid;
                let format = decoder.get_format(index);
                Ok(HavingColumn {
                    element_oid,
                    format,
                    array: encode_array(rows, index, element_oid, format),
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
        array_oid(self.element_oid)
    }

    /// Every value of this column as a one-dimensional array parameter.
    pub(crate) fn array(&self) -> (Parameter, Format) {
        (self.array.clone(), self.format)
    }
}

/// Copy column `index` of every row into an array in the shards' wire format.
fn encode_array(
    rows: &VecDeque<DataRow>,
    index: usize,
    element_oid: i32,
    format: Format,
) -> Parameter {
    let values = rows.iter().map(|row| {
        row.get_raw(index)
            .filter(|data| !data.is_null)
            .map(|data| &data.data[..])
    });
    let mut array = BytesMut::new();
    match format {
        Format::Binary => {
            array.put_i32(1);
            array.put_i32(0);
            array.put_i32(element_oid);
            array.put_i32(rows.len() as i32);
            array.put_i32(1);
            let mut has_null = false;
            for value in values {
                match value {
                    Some(value) => {
                        array.put_i32(value.len() as i32);
                        array.put_slice(value);
                    }
                    None => {
                        has_null = true;
                        array.put_i32(-1);
                    }
                }
            }
            array[4..8].copy_from_slice(&i32::from(has_null).to_be_bytes());
        }
        Format::Text => {
            array.put_u8(b'{');
            for (position, value) in values.enumerate() {
                if position > 0 {
                    array.put_u8(b',');
                }
                let Some(value) = value else {
                    array.put_slice(b"NULL");
                    continue;
                };
                array.put_u8(b'"');
                for &byte in value {
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
    Parameter {
        len: array.len() as i32,
        data: array.freeze(),
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
    use bytes::Bytes;
    use pgdog_postgres_types::Datum;

    use super::*;

    #[test]
    fn encodes_text_and_binary_arrays_with_nulls() {
        let rows = |value: Bytes| {
            let mut present = DataRow::new();
            present.add(value);
            let mut null = DataRow::new();
            null.add(Datum::Null);
            VecDeque::from([present, null])
        };

        let text = encode_array(&rows(Bytes::from_static(br#"a"b\c"#)), 0, 25, Format::Text);
        assert_eq!(text.data.as_ref(), br#"{"a\"b\\c",NULL}"#);

        let binary = encode_array(
            &rows(Bytes::copy_from_slice(&7_i64.to_be_bytes())),
            0,
            20,
            Format::Binary,
        );
        let mut expected = Vec::new();
        for header in [1_i32, 1, 20, 2, 1, 8] {
            expected.extend_from_slice(&header.to_be_bytes());
        }
        expected.extend_from_slice(&7_i64.to_be_bytes());
        expected.extend_from_slice(&(-1_i32).to_be_bytes());
        assert_eq!(binary.data.as_ref(), expected.as_slice());
    }
}
