use std::path::PathBuf;

use crate::telegram::InlineKeyboardButton;
use crate::torrent_inline::{
    render_torrent_inline, render_torrent_inline_with_miniapp, render_torrent_inline_with_options,
};
use crate::torrent_types::{
    TorrentFileSnapshot, TorrentFolder, TorrentMusicStage, TorrentPhase, TorrentSnapshot,
};

const TASK_ID: &str = "0123456789abcdef0123456789abcdef";

fn snapshot(inventory_verified: bool, supported_music: bool) -> TorrentSnapshot {
    TorrentSnapshot {
        task_id: TASK_ID.to_string(),
        chat_id: 123,
        user_id: 456,
        upload_message_id: 789,
        card_message_id: Some(901),
        inventory_revision: 7,
        selection_revision: 9,
        render_revision: 11,
        name: "Album".to_string(),
        identity: None,
        phase: TorrentPhase::Downloading,
        state: Some("downloading".to_string()),
        progress: 0.25,
        inventory_verified,
        cursor_folder_id: 0,
        cursor_page: 0,
        review_selection: false,
        folders: vec![TorrentFolder {
            id: 0,
            relative_path: PathBuf::new(),
            file_count: 3,
        }],
        files: vec![
            file(0, "01.m4a", supported_music),
            file(1, "02.flac", supported_music),
            file(2, "cover.jpg", false),
        ],
        selected_count: 0,
        import_count: 0,
        error: None,
    }
}

fn file(index: u32, path: &str, supported_music: bool) -> TorrentFileSnapshot {
    TorrentFileSnapshot {
        index,
        relative_path: PathBuf::from(path),
        size: 1024,
        progress: 0.25,
        complete: false,
        selected: false,
        supported_music,
        needs_reconfirm: false,
        music_stage: None,
    }
}

fn button<'a>(
    buttons: &'a [InlineKeyboardButton],
    label: &str,
) -> Option<&'a InlineKeyboardButton> {
    buttons.iter().find(|button| button.text == label)
}

fn buttons(
    presentation: &crate::torrent_inline::TorrentInlinePresentation,
) -> Vec<InlineKeyboardButton> {
    presentation
        .reply_markup
        .inline_keyboard
        .iter()
        .flatten()
        .cloned()
        .collect()
}

#[test]
fn add_all_music_requires_verified_inventory_and_appears_alongside_mini_app() {
    let pending = snapshot(false, true);
    let pending_native = render_torrent_inline(&pending).expect("render pending native card");
    assert!(
        button(&buttons(&pending_native), "添加所有音乐").is_none(),
        "metainfo-only files must not expose Add All Music before qBittorrent inventory verification"
    );
    let pending_cloud =
        render_torrent_inline_with_miniapp(&pending, Some("https://worker.example.test"))
            .expect("render pending cloud card");
    let pending_cloud_buttons = buttons(&pending_cloud);
    assert!(
        button(&pending_cloud_buttons, "添加所有音乐").is_none(),
        "the compact cloud card must not expose Add All Music before inventory verification"
    );
    assert!(
        pending_cloud_buttons
            .iter()
            .any(|button| button.text == "Manage torrent" && button.web_app.is_some()),
        "the existing Mini App entry stays available while inventory is pending"
    );

    let verified = snapshot(true, true);
    let expected_callback = format!("t1:{TASK_ID}:0000000000000009:a");
    let verified_native = render_torrent_inline(&verified).expect("render verified native card");
    let native_buttons = buttons(&verified_native);
    assert_eq!(
        button(&native_buttons, "添加所有音乐").map(|button| button.callback_data.as_str()),
        Some(expected_callback.as_str()),
        "native Add All Music uses the current selection revision and the `a` callback"
    );

    let verified_cloud =
        render_torrent_inline_with_miniapp(&verified, Some("https://worker.example.test"))
            .expect("render verified cloud card");
    let cloud_buttons = buttons(&verified_cloud);
    assert_eq!(
        button(&cloud_buttons, "添加所有音乐").map(|button| button.callback_data.as_str()),
        Some(expected_callback.as_str()),
        "the cloud-enabled Telegram card places Add All Music alongside its Mini App button"
    );
    assert!(
        cloud_buttons
            .iter()
            .any(|button| button.text == "Manage torrent" && button.web_app.is_some()),
        "the Mini App button remains present next to Add All Music"
    );

    let mut scheduled = verified.clone();
    scheduled.selected_count = 2;
    scheduled.import_count = 2;
    for file in &mut scheduled.files[..2] {
        file.selected = true;
        file.music_stage = Some(TorrentMusicStage::Pending);
    }
    for presentation in [
        render_torrent_inline(&scheduled).expect("render scheduled native card"),
        render_torrent_inline_with_miniapp(&scheduled, Some("https://worker.example.test"))
            .expect("render scheduled cloud card"),
    ] {
        assert!(presentation.text.contains("已安排添加 2 个音乐文件"));
        assert!(
            button(&buttons(&presentation), "添加所有音乐").is_none(),
            "the one-time Add All action disappears after all supported files are persisted"
        );
    }
}

#[test]
fn disabled_music_status_hides_add_confirm_and_retry_controls() {
    let mut source = snapshot(true, true);
    source.selected_count = 2;
    source.import_count = 2;
    for file in &mut source.files[..2] {
        file.selected = true;
        file.music_stage = Some(TorrentMusicStage::Failed);
    }

    let mut review = source.clone();
    review.review_selection = true;
    let disabled_review = render_torrent_inline_with_options(&review, None, false)
        .expect("render disabled review card");
    assert!(disabled_review.text.contains("Music imports are disabled"));
    let review_buttons = buttons(&disabled_review);
    assert!(button(&review_buttons, "添加所有音乐").is_none());
    assert!(button(&review_buttons, "Confirm import").is_none());

    let disabled_browser = render_torrent_inline_with_options(&source, None, false)
        .expect("render disabled browser card");
    assert!(disabled_browser.text.contains("Music imports are disabled"));
    assert!(
        buttons(&disabled_browser)
            .iter()
            .all(|button| !button.text.starts_with("Retry ")),
        "disabled Music must not expose retry callbacks"
    );

    let disabled_cloud =
        render_torrent_inline_with_options(&source, Some("https://worker.example.test"), false)
            .expect("render disabled cloud card");
    assert!(disabled_cloud.text.contains("Music imports are disabled"));
    let cloud_buttons = buttons(&disabled_cloud);
    assert!(button(&cloud_buttons, "添加所有音乐").is_none());
    assert!(button(&cloud_buttons, "Manage torrent").is_some());
}

#[test]
fn add_all_music_is_hidden_when_verified_inventory_has_no_supported_audio() {
    let unsupported = snapshot(true, false);
    for presentation in [
        render_torrent_inline(&unsupported).expect("render unsupported native card"),
        render_torrent_inline_with_miniapp(&unsupported, Some("https://worker.example.test"))
            .expect("render unsupported cloud card"),
    ] {
        assert!(
            button(&buttons(&presentation), "添加所有音乐").is_none(),
            "a verified inventory without supported audio must not offer Add All Music"
        );
    }
}
