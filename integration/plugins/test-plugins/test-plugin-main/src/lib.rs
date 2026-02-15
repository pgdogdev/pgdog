//! Tests that the plugin is compatible with current main branch of PgDog based on pgdog-plugin version.
//! The plugin from main branch either should be loaded and in this case it should assert the compatibility
//! of the context (that would mean there is no breaking changes in the API) or it should be skipped completely
//! when running pgdog.
use pgdog_plugin::{Context, Route, macros};

macros::plugin!();

#[macros::route]
fn route(context: Context) -> Route {
    assertions::assert_context_compatible!(&context);

    Route::unknown()
}
