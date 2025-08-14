use crate::{
    backend::Cluster,
    frontend::{
        buffer::BufferedQuery,
        client::transaction::Transaction,
        router::{parser::route::Route, parser::route::Shard},
    },
    net::{Parse, Query},
};

#[test]
fn test_route_management() {
    let cluster = Cluster::new_test();
    let buffered_query = BufferedQuery::Query(Query::new("BEGIN"));
    let mut transaction = Transaction::new(buffered_query, &cluster);

    // Test initial state
    assert!(!transaction.started());
    assert!(transaction.buffered());

    // Test route setting with conservative strategy (default)
    let read_route = Route::read(Shard::Direct(0));
    transaction.set_route(&read_route);

    let write_route = Route::write(Shard::Direct(1));
    transaction.set_route(&write_route);

    // Should use default route for conservative strategy when buffered
    let route = transaction.transaction_route();
    assert!(route.is_write()); // Default route is write
    assert_eq!(route.shard(), &Shard::All); // Default route targets all shards

    // Test after starting transaction (still buffered until take_begin())
    transaction.server_start();
    assert!(transaction.started());
    assert!(transaction.buffered()); // Still buffered until take_begin() called

    // Manually call take_begin to simulate what happens in real usage
    transaction.take_begin();
    assert!(!transaction.buffered());

    let route_after_start = transaction.transaction_route();
    assert_eq!(route_after_start.shard(), write_route.shard());
    assert_eq!(route_after_start.is_read(), read_route.is_read()); // Preserves read from first route

    // Test finish
    transaction.finish();
    assert!(!transaction.started());
    assert!(!transaction.buffered());
}

#[test]
fn test_route_read_write_preservation() {
    let cluster = Cluster::new_test();
    let buffered_query = BufferedQuery::Prepared(Parse::named("test", "BEGIN"));
    let mut transaction = Transaction::new(buffered_query, &cluster);

    // Set initial read route
    let read_route = Route::read(Shard::Direct(0));
    transaction.set_route(&read_route);

    // Set subsequent write route - should preserve read property from first route
    let write_route = Route::write(Shard::Direct(1));
    transaction.set_route(&write_route);

    // Start transaction and consume buffered query to get the actual route (not default)
    transaction.server_start();
    transaction.take_begin(); // Consume buffered query
    let route = transaction.transaction_route();
    assert!(route.is_read()); // Should preserve read from first route
    assert_eq!(route.shard(), &Shard::Direct(1)); // Should use shard from second route

    // Test opposite case
    let mut transaction2 = Transaction::new(BufferedQuery::Query(Query::new("BEGIN")), &cluster);

    // Set initial write route
    let write_route = Route::write(Shard::Direct(0));
    transaction2.set_route(&write_route);

    // Set subsequent read route - should preserve write property from first route
    let read_route = Route::read(Shard::Direct(1));
    transaction2.set_route(&read_route);

    // Start transaction and consume buffered query to get the actual route (not default)
    transaction2.server_start();
    transaction2.take_begin(); // Consume buffered query
    let route = transaction2.transaction_route();
    assert!(route.is_write()); // Should preserve write from first route
    assert_eq!(route.shard(), &Shard::Direct(1)); // Should use shard from second route
}

#[test]
fn test_cross_shard_routes() {
    let cluster = Cluster::new_test();
    let buffered_query = BufferedQuery::Query(Query::new("BEGIN"));
    let mut transaction = Transaction::new(buffered_query, &cluster);

    // Test all shards route
    let all_route = Route::read(Shard::All);
    transaction.set_route(&all_route);

    let route = transaction.transaction_route();
    assert!(route.is_cross_shard());

    // Test multi shard route
    transaction.finish();
    let mut transaction2 = Transaction::new(BufferedQuery::Query(Query::new("BEGIN")), &cluster);

    let multi_route = Route::write(Shard::Multi(vec![0, 2]));
    transaction2.set_route(&multi_route);

    let route = transaction2.transaction_route();
    assert!(route.is_cross_shard());
}
