use crate::{
    config::{load_test, load_test_sharded},
    frontend::Client,
    net::{Parameters, Stream},
};

pub mod prelude;
mod rewrite_extended;
mod rewrite_insert_split;

pub(super) fn test_client() -> Client {
    load_test();
    Client::new_test(Stream::dev_null(), Parameters::default())
}

pub(super) fn test_sharded_client() -> Client {
    load_test_sharded();
    Client::new_test(Stream::dev_null(), Parameters::default())
}
