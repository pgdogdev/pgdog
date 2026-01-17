use crate::{Data, Error, Format};
use bytes::Bytes;

pub trait FromDataType: Sized + PartialOrd + Ord + PartialEq {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error>;
    fn encode(&self, encoding: Format) -> Result<Bytes, Error>;
}

/// Convert value to data row column
/// using text formatting.
pub trait ToDataRowColumn {
    fn to_data_row_column(&self) -> Data;
}

impl ToDataRowColumn for Bytes {
    fn to_data_row_column(&self) -> Data {
        self.clone().into()
    }
}

impl ToDataRowColumn for Data {
    fn to_data_row_column(&self) -> Data {
        self.clone()
    }
}

impl ToDataRowColumn for String {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.as_bytes()).into()
    }
}

impl ToDataRowColumn for &String {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.as_bytes()).into()
    }
}

impl ToDataRowColumn for &str {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.as_bytes()).into()
    }
}

impl ToDataRowColumn for i64 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for Option<i64> {
    fn to_data_row_column(&self) -> Data {
        match self {
            Some(value) => ToDataRowColumn::to_data_row_column(value),
            None => Data::null(),
        }
    }
}

impl ToDataRowColumn for usize {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for u64 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for bool {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(if *self { b"t" } else { b"f" }).into()
    }
}

impl ToDataRowColumn for f64 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for u128 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}
