/// Asserts that the plugin version of pgdog-plugin is compatible with PgDog's version.
#[macro_export]
// use macro to be able to use the version of pgdog-plugin defined the plugin crate itself
// not this crate's version
macro_rules! assert_context_compatible {
    ($context:expr) => {
        assert!(!$context.read_only());
        assert!($context.has_primary());
        assert!($context.has_replicas());
        assert_eq!($context.shards(), 1);
        assert!(!$context.sharded());
        assert!(!$context.write_override());

        // Parameters should be accessible and not panic
        let params = $context.parameters();

        assert!(params.len() >= 1);

        // query should be accessible
        let query = $context.statement().protobuf();
        assert!(query.nodes().len() >= 1);
    };
}
