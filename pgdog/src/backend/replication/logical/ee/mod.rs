//! EE hooks.
#![allow(dead_code, unused)]

use pgdog_stats::Lsn;

use super::*;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) enum CutoverState {
    WaitingForReplication { lag: u64 },
    WaitForCutover { action: CutoverAction },
    Abort { error: String },
    Complete,
}

#[derive(Debug, Clone)]
pub(crate) enum OrchestratorState {
    SchemSyncPre,
    SchemaSyncPost,
    SchemaSyncCutover,
    SchemaSyncPostCutover,
    DataSync,
    Replication,
    Cutover(CutoverState),
}

pub(crate) fn cutover_state(state: CutoverState) {
    orchestrator_state(OrchestratorState::Cutover(state));
}

pub(crate) fn orchestrator_state(state: OrchestratorState) {}
