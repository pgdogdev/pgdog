//! Tests that the plugin that uses outdated version of pgdog-plugin is skipped.
//! If the plugin is loaded, the test will fail because of the panic.

use pgdog_plugin::{Context, Route, macros};

macros::plugin!();

#[macros::route]
fn route(_context: Context) -> Route {
    // TODO: enable panic after forcing the pgdog-plugin version compatibility check
    // panic!("The outdated plugin should not be loaded");
    Route::unknown()
}
