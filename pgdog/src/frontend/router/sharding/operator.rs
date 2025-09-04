use super::{Centroids, Lists, Ranges};

#[derive(Debug)]
pub(crate) enum Operator<'a> {
    Shards(usize),
    Centroids {
        shards: usize,
        probes: usize,
        centroids: Centroids<'a>,
    },
    Range(Ranges<'a>),
    List(Lists<'a>),
}
