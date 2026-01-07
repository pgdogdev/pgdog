pub use pgdog_config::sharding::Mapping;

use crate::frontend::router::Ranges;

pub fn mapping_valid(mapping: &Mapping) -> bool {
    match mapping {
        Mapping::Range(_) => Ranges::new(&Some(mapping.clone()))
            .map(|r| r.valid())
            .unwrap_or(false),
        Mapping::List(_) => true,
    }
}
