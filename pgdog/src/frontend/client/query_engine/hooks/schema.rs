use crate::backend::{databases::reload_from_existing, Error};

pub(crate) fn schema_changed() -> Result<(), Error> {
    reload_from_existing()
}
