//! Tests that the plugin works as expected.
//! The plugin uses the same version of pgdog-plugin as PgDog and the same rustc version,
//! so it should be loaded and executed.

use pgdog_plugin::{plugin, Context, PdStr, Plugin, Route};
use std::sync::OnceLock;

plugin!(TestPlugin);

static ROUTE_CALLED: OnceLock<()> = OnceLock::new();

struct TestPlugin;

impl Plugin for TestPlugin {
    extern "C-unwind" fn version() -> PdStr<'static> {
        "0".into()
    }

    fn route(context: Context<'_>) -> Route {
        assert!(!context.read_only());
        assert!(context.has_primary());
        assert!(context.has_replicas());
        assert_eq!(context.shards(), 1);
        assert!(!context.sharded());
        assert!(!context.write_override());

        // Parameters should be accessible and not panic
        let params = context.parameters();

        assert!(params.parameters.len() >= 1);

        // query should be accessible
        let query = context.query;
        assert!(query.nodes().len() >= 1);

        // Write to output file on first call only
        ROUTE_CALLED.get_or_init(|| {
            let file_path =
                std::path::Path::new(&env!("CARGO_MANIFEST_DIR")).join("route-called.test");
            std::fs::write(&file_path, "route method was called").unwrap();
        });

        Route::unknown()
    }
}
