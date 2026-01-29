use once_cell::sync::Lazy;
use std::{collections::HashSet, ops::Deref};

const CATALOGS: &[&str] = &[
    "pg_class",
    "pg_attribute",
    "pg_attrdef",
    "pg_index",
    "pg_constraint",
    "pg_namespace",
    "pg_database",
    "pg_tablespace",
    "pg_type",
    "pg_proc",
    "pg_operator",
    "pg_cast",
    "pg_enum",
    "pg_range",
    "pg_authid",
    "pg_am",
];

static SYSTEM_CATALOGS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| CATALOGS.into_iter().map(|s| *s).collect());

/// Get a list of system catalogs that we care about.
pub fn system_catalogs() -> &'static HashSet<&'static str> {
    SYSTEM_CATALOGS.deref()
}
