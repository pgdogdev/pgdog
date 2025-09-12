use crate::{frontend::router::parser::Shard, net::messages::Vector};

pub enum Distance<'a> {
    Euclidean(&'a Vector, &'a Vector),
}

impl Distance<'_> {
    pub fn distance(&self) -> f32 {
        match self {
            // TODO: SIMD this.
            Self::Euclidean(p, q) => {
                assert_eq!(p.len(), q.len());
                p.iter()
                    .zip(q.iter())
                    .map(|(p, q)| (q.0 - p.0).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
        }
    }
}

#[derive(Debug)]
pub struct Centroids<'a> {
    centroids: &'a [Vector],
}

impl Centroids<'_> {
    /// Find the shards with the closest centroids,
    /// according to the number of probes.
    pub fn shard(&self, vector: &Vector, shards: usize, probes: usize) -> Shard {
        let mut selected = vec![];
        let mut centroids = self.centroids.iter().enumerate().collect::<Vec<_>>();
        centroids.sort_by(|(_, a), (_, b)| {
            a.distance_l2(vector)
                .partial_cmp(&b.distance_l2(vector))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let centroids = centroids.into_iter().take(probes);
        for (i, _) in centroids {
            selected.push(i % shards);
        }

        Shard::Multi(selected)
    }
}

impl<'a> From<&'a Vec<Vector>> for Centroids<'a> {
    fn from(centroids: &'a Vec<Vector>) -> Self {
        Centroids { centroids }
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::Vector;

    use super::Distance;

    #[test]
    fn test_euclidean() {
        let v1 = Vector::from(&[1.0, 2.0, 3.0][..]);
        let v2 = Vector::from(&[1.5, 2.0, 3.0][..]);
        let distance = Distance::Euclidean(&v1, &v2).distance();
        assert_eq!(distance, 0.5);
    }
}
