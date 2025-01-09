//! Simple routing plugin example using Rust.
use pgdog_plugin::*;

/// Route query.
#[no_mangle]
pub extern "C" fn pgdog_route_query(input: Input) -> Output {
    if let Some(query) = input.query() {
        let _id = if let Some(id) = query.parameter(0) {
            if let Some(id) = id.as_str() {
                id.parse::<i64>().ok()
            } else if let Ok(id) = id.as_bytes().try_into() {
                Some(i64::from_be_bytes(id))
            } else {
                None
            }
        } else {
            None
        };
    }

    Output::skip()
}
