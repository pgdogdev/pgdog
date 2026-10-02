use super::super::*;
use crate::frontend::router::parser::ShardWithPriority;

pub(crate) fn route(shard: Shard, is_read: bool) -> Route {
    if is_read {
        Route::read(ShardWithPriority::new_table(shard))
    } else {
        Route::write(ShardWithPriority::new_table(shard))
    }
}

pub(crate) fn test_connection() -> Connection {
    crate::config::load_test_sharded_3();
    let cluster = Cluster::new_test(&crate::config());

    Connection::new(
        &cluster.identifier().user,
        &cluster.identifier().database,
        false,
    )
    .unwrap()
}
