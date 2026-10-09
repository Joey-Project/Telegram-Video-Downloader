use serde::{Deserialize, Serialize};

/// A rebuildable view of the media reachable under the configured download root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibrarySnapshot {
    pub revision: String,
    pub scanned_at: u64,
    pub root_label: String,
    pub items: Vec<LibraryItem>,
    pub categories: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryItem {
    /// A persistent identity for this physical copy, independent of its current path.
    pub id: String,
    pub title: String,
    pub relative_path: String,
    pub bytes: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub codec: Option<String>,
    pub source_ids: Vec<String>,
    pub collection: Option<CollectionMetadata>,
    pub attachments: Vec<LibraryAttachment>,
    pub confidence: Confidence,
    pub metadata_status: MetadataStatus,
    pub integrity_status: IntegrityStatus,
    pub presence: Presence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMetadata {
    pub id: Option<String>,
    pub title: String,
    pub kind: String,
    pub order: Option<u32>,
    pub part_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryAttachment {
    pub relative_path: String,
    pub kind: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    High,
    Medium,
    Low,
    NeedsConfirmation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataStatus {
    Complete,
    Partial,
    Unknown,
    Unreadable,
    ProviderUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityStatus {
    Unverified,
    Verified,
    Warning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Present,
    Unavailable,
    Missing,
}

/// Metadata supplied by the queue or by a user-provided legacy import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHint {
    #[serde(default)]
    pub source_ids: Vec<String>,
    pub title: Option<String>,
    pub relative_path: Option<String>,
    pub collection: Option<CollectionMetadata>,
    pub origin: HintOrigin,
    #[serde(default)]
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HintOrigin {
    Queue,
    LegacyText,
    LegacyTelegramJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyImportReport {
    pub hints: Vec<SourceHint>,
    pub warnings: Vec<String>,
    pub needs_confirmation: bool,
    pub revision: Option<String>,
    pub candidates: Vec<LegacyCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCandidate {
    pub hint_index: usize,
    pub item_id: String,
    pub title: String,
    pub relative_path: String,
    pub match_kind: String,
    pub ambiguous: bool,
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovePreview {
    pub id: String,
    pub revision: String,
    pub target_relative_dir: Option<String>,
    pub rename: bool,
    pub conflict: ConflictPolicy,
    pub items: Vec<MovePreviewItem>,
    #[serde(default)]
    pub metadata_patches: Vec<MetadataPatchPreview>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPatch {
    pub item_id: String,
    pub title: Option<String>,
    #[serde(default)]
    pub source_ids: Vec<String>,
    pub collection: Option<CollectionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPatchPreview {
    pub item_id: String,
    pub nfo_path: String,
    pub changed: bool,
    pub added_source_ids: Vec<String>,
    #[serde(default)]
    pub changes: Vec<MetadataFieldChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataFieldChange {
    pub field: String,
    pub before: Option<String>,
    pub after: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovePreviewItem {
    pub item_id: String,
    pub source_path: String,
    pub target_path: String,
    pub outcome: MoveOutcome,
    pub files: Vec<FileMovePreview>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMovePreview {
    pub source_path: String,
    pub target_path: String,
    pub bytes: u64,
    /// Opaque device/inode/type/size token used to reject a stale preview.
    pub identity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    Skip,
    KeepBoth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveOutcome {
    Move,
    Skip,
    NoOp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMoveResult {
    pub id: String,
    pub revision: String,
    pub status: BatchStatus,
    pub items: Vec<MoveResultItem>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    Complete,
    Partial,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveResultItem {
    pub item_id: String,
    pub outcome: MoveOutcome,
    pub moved_files: usize,
    pub total_files: usize,
    #[serde(default)]
    pub metadata_patched: bool,
    pub error: Option<String>,
}
