use indexmap::{IndexMap, map::Entry};

use crate::net::ParameterStatus;

use super::{ParameterName, ParameterValue};

/// Coalesces parameter reports while retaining each parameter's original value.
#[derive(Debug, Default)]
pub(crate) struct ParameterStatusBuffer<'a> {
    statuses: IndexMap<ParameterName, (&'a ParameterStatus, Option<&'a ParameterValue>)>,
}

impl<'a> ParameterStatusBuffer<'a> {
    /// Keep the latest status and the value preceding the first change.
    pub(crate) fn insert(
        &mut self,
        status: &'a ParameterStatus,
        previous: Option<&'a ParameterValue>,
    ) {
        match self.statuses.entry(ParameterName::new(&status.name)) {
            Entry::Occupied(mut entry) => entry.get_mut().0 = status,
            Entry::Vacant(entry) => {
                entry.insert((status, previous));
            }
        }
    }

    /// Report changed final values in reverse order of their first change.
    pub(crate) fn into_statuses(self) -> impl Iterator<Item = &'a ParameterStatus> {
        self.statuses
            .into_values()
            .rev()
            .filter_map(|(status, previous)| (previous != Some(&status.value)).then_some(status))
    }
}
