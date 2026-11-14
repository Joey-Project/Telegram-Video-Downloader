use serde::{Deserialize, Serialize};
use std::fmt;

use crate::library::PhysicalItemIdentity;
use crate::queue::TaskStatus;

pub(crate) const MAX_STAGING_CLEANUP_SCAN_BYTES: usize = 512 * 1024;
pub(crate) const MAX_STAGING_CLEANUP_PREVIEW_BYTES: usize = 512 * 1024;
pub(crate) const MAX_STAGING_CLEANUP_ITEMS: usize = 5_000;
pub(crate) const MAX_STAGING_CLEANUP_TITLE_BYTES: usize = 256;
pub(crate) const MAX_STAGING_CLEANUP_PATH_BYTES: usize = 512;

/// Runtime uses this marker to distinguish a retryable writer race from a rejected preview.
#[derive(Debug)]
pub(crate) struct StagingCleanupBusy;

impl fmt::Display for StagingCleanupBusy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("video output is busy; retry staging cleanup")
    }
}

impl std::error::Error for StagingCleanupBusy {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StagingScan {
    pub(crate) library_id: String,
    pub(crate) root_binding: String,
    pub(crate) revision: String,
    pub(crate) scanned_at: u64,
    pub(crate) items: Vec<StagingItem>,
    pub(crate) warnings: Vec<String>,
    pub(crate) truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StagingItem {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) task_id: String,
    pub(crate) task_status: TaskStatus,
    pub(crate) relative_path: String,
    pub(crate) bytes: Option<u64>,
    pub(crate) file_count: Option<usize>,
    pub(crate) eligible: bool,
    pub(crate) reason: Option<String>,
}

/// A cleanup preview is a serialized, restart-safe authorization snapshot. Its private proofs are
/// deliberately omitted from the public summary but remain in the serde representation stored by
/// the runtime until confirmation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CleanupPreview {
    pub(crate) id: String,
    pub(crate) revision: String,
    pub(crate) library_id: String,
    pub(crate) root_binding: String,
    pub(crate) created_at: u64,
    pub(crate) items: Vec<StagingItem>,
    pub(crate) total_bytes: u64,
    proofs: Vec<CleanupProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CleanupProof {
    pub(crate) task_id: String,
    pub(crate) task_generation: u64,
    pub(crate) task_revision: u64,
    pub(crate) task_status: TaskStatus,
    pub(crate) task_chat_id: i64,
    pub(crate) relative_path: String,
    pub(crate) root_identity: PhysicalItemIdentity,
    pub(crate) control_identity: PhysicalItemIdentity,
    pub(crate) control_owner_identity: PhysicalItemIdentity,
    pub(crate) output_lock_identity: PhysicalItemIdentity,
    pub(crate) recovery_state_identity: PhysicalItemIdentity,
    pub(crate) staging_root_identity: PhysicalItemIdentity,
    pub(crate) staging_identity: PhysicalItemIdentity,
    pub(crate) owner_identity: PhysicalItemIdentity,
    pub(crate) inventory_sha256: String,
    pub(crate) bytes: u64,
    pub(crate) file_count: usize,
    pub(crate) inventory_entry_count: usize,
}

impl CleanupPreview {
    // Keep the persisted summary and private proof bundle explicit at this serialization boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: String,
        revision: String,
        library_id: String,
        root_binding: String,
        created_at: u64,
        items: Vec<StagingItem>,
        total_bytes: u64,
        proofs: Vec<CleanupProof>,
    ) -> Self {
        Self {
            id,
            revision,
            library_id,
            root_binding,
            created_at,
            items,
            total_bytes,
            proofs,
        }
    }

    pub(crate) fn proofs(&self) -> &[CleanupProof] {
        &self.proofs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CleanupFailure {
    pub(crate) id: String,
    pub(crate) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct CleanupResult {
    pub(crate) removed: Vec<String>,
    pub(crate) failed: Vec<CleanupFailure>,
    pub(crate) bytes: u64,
}
