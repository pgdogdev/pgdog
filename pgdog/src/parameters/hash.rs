//! Compute hash of parameters.

use std::hash::{DefaultHasher, Hash, Hasher};

use super::{ParameterName, ParameterValue, is_untracked};

/// Hash a list of parameters to produce a stable
/// hash value. This can be used to quickly check if a cached
/// version of parameters is identical to another.
pub(super) fn hash_parameters<'a>(
    params: impl IntoIterator<Item = (&'a ParameterName, &'a ParameterValue)>,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    let mut entries = 0;

    for (k, v) in params {
        if is_untracked(k.as_str()) {
            continue;
        }
        entries += 1;

        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }

    if entries > 0 { hasher.finish() } else { 0 }
}
