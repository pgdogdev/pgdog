use pgdog_plugin::{Query, Route};

/// Route query.
#[no_mangle]
pub unsafe extern "C" fn route(query: Query) -> Route {
    let query = query.query();
    if query.to_lowercase().starts_with("select") {
        Route::ReadAny
    } else {
        Route::WriteAny
    }
}
