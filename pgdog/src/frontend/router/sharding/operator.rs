use super::Centroids;

#[derive(Debug)]
pub enum Operator<'a> {
    Shards(usize),
    Centroids {
        shards: usize,
        probes: usize,
        centroids: Centroids<'a>,
    },
}
