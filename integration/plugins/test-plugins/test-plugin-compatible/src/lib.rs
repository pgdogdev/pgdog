//! Tests that the plugin works as expected.
//! The plugin uses the same version of pgdog-plugin as PgDog and the same rustc version,
//! so it should be loaded and executed.

use pgdog_plugin::{macros, Context, Route};
use std::sync::OnceLock;

macros::plugin!();

static ROUTE_CALLED: OnceLock<()> = OnceLock::new();

#[macros::route]
fn route(context: Context) -> Route {
    assertions::assert_context_compatible!(context);

    // Write to output file on first call only
    ROUTE_CALLED.get_or_init(|| {
        let file_path = std::path::Path::new(&env!("CARGO_MANIFEST_DIR"))
            .join("route-called.test");
        std::fs::write(&file_path, "route method was called").unwrap();
    });

    Route::unknown()
}
