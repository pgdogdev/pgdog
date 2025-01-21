//! Simple routing plugin example using Rust.
use pgdog_plugin::*;

/// Route query.
#[no_mangle]
pub extern "C" fn pgdog_route_query(input: Input) -> Output {
    let is_read = input
        .query()
        .map(|query| query.query().to_lowercase().trim().starts_with("select"))
        .unwrap_or(false);

    // This is just an example of extracing a parameter from
    // the query. In the future, we'll use this to shard transactions.
    let _parameter = input.query().map(|query| {
        query.parameter(0).map(|parameter| {
            let id = parameter.as_str().map(|str| str.parse::<i64>());
            match id {
                Some(Ok(id)) => id,
                _ => i64::from_be_bytes(parameter.as_bytes().try_into().unwrap_or([0u8; 8])),
            }
        })
    });

    if is_read {
        Output::new_forward(Route::read_any())
    } else {
        Output::new_forward(Route::write_any())
    }
}
