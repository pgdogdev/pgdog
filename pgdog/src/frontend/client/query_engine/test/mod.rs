use pgdog_config::General;

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, load_test_sharded, set},
    frontend::Client,
    net::{Parameters, Stream},
};

mod close_parse;
mod close_parse_global_cache;
mod extended;
mod fatal_error;
mod graceful_disconnect;
mod graceful_shutdown;
mod idle_in_transaction_recovery;
mod lock_session;
mod omni;
pub mod prelude;
mod prepared_syntax_error;
mod replicas;
mod rewrite_extended;
mod rewrite_insert_split;
mod rewrite_offset;
mod rewrite_simple_prepared;
mod schema_changed;
mod set;
mod set_schema_sharding;
mod sharded;
mod spliced;
mod transaction_state;

pub(super) fn test_client() -> Client {
    load_test();
    Client::new_test(Stream::dev_null(), Parameters::default())
}

pub(super) fn test_sharded_client() -> Client {
    load_test_sharded();
    Client::new_test(Stream::dev_null(), Parameters::default())
}

pub(super) fn change_config(f: impl FnOnce(&mut General)) {
    let mut config = (*config()).clone();
    f(&mut config.config.general);
    set(config).unwrap();
    reload_from_existing().unwrap();
}
