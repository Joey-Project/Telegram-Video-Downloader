use serde_json::json;

use super::{FilePreviewPayload, MAX_CLOUD_SELECTED_ITEMS, validate_file_preview_payload};

fn file_preview_payload(item_count: usize) -> FilePreviewPayload {
    serde_json::from_value(json!({
        "item_ids": (0..item_count)
            .map(|index| format!("copy-{:024x}", index))
            .collect::<Vec<_>>(),
        "target_relative_dir": "",
    }))
    .expect("file preview payload should deserialize")
}

#[test]
fn file_preview_accepts_the_5000_item_selection_limit() {
    let prepared = validate_file_preview_payload(file_preview_payload(MAX_CLOUD_SELECTED_ITEMS))
        .expect("5,000 selected media items should be accepted");

    assert_eq!(prepared.item_ids.len(), MAX_CLOUD_SELECTED_ITEMS);
}

#[test]
fn file_preview_rejects_5001_selected_items() {
    let error = validate_file_preview_payload(file_preview_payload(MAX_CLOUD_SELECTED_ITEMS + 1))
        .err()
        .expect("5,001 selected media items should be rejected");

    assert!(error.contains("at most 5000 items"));
}
