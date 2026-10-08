pub(crate) mod azure_workload_identity;
pub(crate) mod rds_iam;
#[cfg(any(feature = "fips", test))]
pub(crate) mod sigv4;
pub(crate) mod vault;
