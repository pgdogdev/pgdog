//! EE hooks.
#![allow(dead_code, unused)]

use pgdog_stats::{Lsn, ReplicationSlot, SchemaStatementTask};

use crate::net::ErrorResponse;

use super::*;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) enum OrchestratorState {
    SchemSyncPre,
    SchemaSyncPost,
    SchemaSyncCutover,
    SchemaSyncPostCutover,
    DataSync,
    Replication,
}

pub(crate) fn orchestrator_state(state: OrchestratorState) {}

pub(crate) fn schema_sync_task(task: &SchemaStatementTask) {}

pub(crate) fn replication_slot_create(slot: &ReplicationSlot) {}

pub(crate) fn replication_slot_drop(slot: &ReplicationSlot) {}

pub(crate) fn replication_slot_update(slot: &ReplicationSlot) {}

pub(crate) fn replication_slot_error(slot: &ReplicationSlot, err: &ErrorResponse) {}
