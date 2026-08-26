use pgdog_postgres_types::{Format, FromDataType};
pub(crate) use pgdog_vector::Vector;

use crate::net::Error;

pub(crate) fn str_to_vector(value: &str) -> Result<Vector, Error> {
    Ok(FromDataType::decode(value.as_bytes(), Format::Text)?)
}
