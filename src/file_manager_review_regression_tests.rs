#[test]
fn review78_secondary_nfo_attachment_references_move_with_their_owners() {
    let root = TemporaryRoot::new("review78-secondary-nfo-attachments");
    let film_dir = root.path().join("Film");
    let other_dir = root.path().join("Other");
    let destination = root.path().join("Organized");
    fs::create_dir_all(&film_dir).expect("Film source directory should create");
    fs::create_dir_all(&other_dir).expect("Other source directory should create");
    fs::create_dir_all(&destination).expect("destination directory should create");

    fs::write(film_dir.join("Film.mkv"), b"film media").expect("Film media should write");
    fs::write(film_dir.join("poster.jpg"), b"Film poster bytes").expect("Film poster should write");
    fs::write(film_dir.join("art.jpg"), b"Film secondary art bytes")
        .expect("Film secondary art should write");
    fs::write(
        film_dir.join("Film.nfo"),
        concat!(
            "<movie><title>Old Film title</title>",
            "<uniqueid type=\"bilibili\">BV1Film123</uniqueid>",
            "<tvd_attachment role=\"cover\">poster.jpg</tvd_attachment>",
            "<thumb>poster.jpg</thumb>",
            "<futureField owner=\"authoritative\">preserve auth field</futureField></movie>"
        ),
    )
    .expect("authoritative Film NFO should write");
    fs::write(
        film_dir.join("Film.extra.nfo"),
        concat!(
            "<movie><title>Secondary title stays</title>",
            "<uniqueid type=\"youtube\">abcdefghijk</uniqueid>",
            "<tvd_attachment role=\"cover\">art.jpg</tvd_attachment>",
            "<thumb>art.jpg</thumb>",
            "<futureField owner=\"secondary\">preserve secondary field</futureField></movie>"
        ),
    )
    .expect("secondary Film NFO should write");

    fs::write(other_dir.join("Other.mkv"), b"unrelated media")
        .expect("unrelated media should write");
    fs::write(other_dir.join("other-cover.jpg"), b"unrelated cover bytes")
        .expect("unrelated cover should write");
    let unrelated_nfo = concat!(
        "<movie><title>Other title</title>",
        "<uniqueid type=\"bilibili-cid\">cid9090</uniqueid>",
        "<tvd_attachment role=\"cover\">other-cover.jpg</tvd_attachment>",
        "<futureField>untouched other NFO</futureField></movie>"
    );
    fs::write(other_dir.join("Other.nfo"), unrelated_nfo).expect("unrelated NFO should write");

    fs::write(
        destination.join("poster.jpg"),
        b"pre-existing poster collision",
    )
    .expect("poster collision should write");
    fs::write(destination.join("art.jpg"), b"pre-existing art collision")
        .expect("art collision should write");
    let original_authoritative_nfo =
        fs::read_to_string(film_dir.join("Film.nfo")).expect("authoritative NFO should read");
    let original_secondary_nfo =
        fs::read_to_string(film_dir.join("Film.extra.nfo")).expect("secondary NFO should read");
    let original_other_cover =
        fs::read(other_dir.join("other-cover.jpg")).expect("unrelated cover should read");

    let manager = manager(&root);
    let snapshot = manager.scan(&[]).expect("source library should scan");
    let film = item_at(&snapshot, "Film/Film.mkv");
    assert_eq!(
        film.authoritative_nfo_path.as_deref(),
        Some("Film/Film.nfo")
    );
    for relative_path in ["Film/poster.jpg", "Film/art.jpg"] {
        assert!(film.attachments.iter().any(|attachment| {
            attachment.relative_path == relative_path && attachment.kind == "cover"
        }));
    }
    let other_before = item_at(&snapshot, "Other/Other.mkv");
    assert!(other_before.attachments.iter().any(|attachment| {
        attachment.relative_path == "Other/other-cover.jpg" && attachment.kind == "cover"
    }));
    let title_patch = MetadataPatch {
        item_id: film.id.clone(),
        title: Some("Confirmed Film title".to_string()),
        source_ids: Vec::new(),
        collection: None,
        fill_missing_only: false,
        organization_status: None,
    };
    let preview = manager
        .preview_with_hints_and_patches(
            std::slice::from_ref(&film.id),
            Some("Organized"),
            false,
            ConflictPolicy::KeepBoth,
            &[],
            &[title_patch],
        )
        .expect("all declared attachments should have a KeepBoth preview");
    let file_target = |source: &str| {
        preview.items[0]
            .files
            .iter()
            .find(|file| file.source_path == source)
            .unwrap_or_else(|| panic!("preview should include {source}"))
            .target_path
            .clone()
    };
    let media_target = file_target("Film/Film.mkv");
    let poster_target = file_target("Film/poster.jpg");
    let art_target = file_target("Film/art.jpg");
    let authoritative_target = file_target("Film/Film.nfo");
    let secondary_target = file_target("Film/Film.extra.nfo");
    assert_eq!(preview.items[0].outcome, MoveOutcome::Move);
    assert_ne!(poster_target, "Organized/poster.jpg");
    assert_ne!(art_target, "Organized/art.jpg");
    assert_ne!(media_target, "Organized/Film.mkv");
    assert!(preview.metadata_patches.iter().any(|patch| {
        patch.nfo_path == authoritative_target
            && patch
                .changes
                .iter()
                .any(|change| change.field == "title" && change.after == "Confirmed Film title")
    }));
    assert!(
        preview
            .metadata_patches
            .iter()
            .any(|patch| patch.nfo_path == secondary_target)
    );
    assert_eq!(
        fs::read_to_string(film_dir.join("Film.nfo")).unwrap(),
        original_authoritative_nfo,
        "preview must not write the requested metadata before confirmation"
    );
    assert_eq!(
        fs::read_to_string(film_dir.join("Film.extra.nfo")).unwrap(),
        original_secondary_nfo,
        "preview must not rewrite secondary NFO references before confirmation"
    );

    let result = manager
        .execute_preview(&preview.id, &preview.revision)
        .expect("confirmed multi-NFO KeepBoth organization should complete");
    assert_eq!(result.status, BatchStatus::Complete);
    let authoritative = fs::read_to_string(root.path().join(&authoritative_target))
        .expect("moved authoritative NFO should read");
    let secondary = fs::read_to_string(root.path().join(&secondary_target))
        .expect("moved secondary NFO should read");
    let poster_filename = Path::new(&poster_target)
        .file_name()
        .and_then(OsStr::to_str)
        .expect("poster target filename should be UTF-8");
    let art_filename = Path::new(&art_target)
        .file_name()
        .and_then(OsStr::to_str)
        .expect("art target filename should be UTF-8");
    assert!(authoritative.contains(&format!(
        "<tvd_attachment role=\"cover\">{poster_filename}</tvd_attachment>"
    )));
    assert!(authoritative.contains(&format!("<thumb>{poster_filename}</thumb>")));
    assert!(authoritative.contains("<title>Confirmed Film title</title>"));
    assert!(authoritative.contains("<uniqueid type=\"bilibili\">BV1Film123</uniqueid>"));
    assert!(
        authoritative
            .contains("<futureField owner=\"authoritative\">preserve auth field</futureField>")
    );
    assert!(secondary.contains(&format!(
        "<tvd_attachment role=\"cover\">{art_filename}</tvd_attachment>"
    )));
    assert!(secondary.contains(&format!("<thumb>{art_filename}</thumb>")));
    assert!(secondary.contains("<title>Secondary title stays</title>"));
    assert!(secondary.contains("<uniqueid type=\"youtube\">abcdefghijk</uniqueid>"));
    assert!(
        secondary
            .contains("<futureField owner=\"secondary\">preserve secondary field</futureField>")
    );
    assert_eq!(
        fs::read_to_string(other_dir.join("Other.nfo")).unwrap(),
        unrelated_nfo,
        "moving Film must not rewrite a different media item's NFO"
    );
    assert_eq!(
        fs::read(other_dir.join("other-cover.jpg")).unwrap(),
        original_other_cover,
        "moving Film must not change a different media item's artwork"
    );

    let rescanned = manager.scan(&[]).expect("organized library should rescan");
    let moved_film = item_at(&rescanned, &media_target);
    assert_eq!(
        moved_film.authoritative_nfo_path.as_deref(),
        Some(authoritative_target.as_str())
    );
    for target in [&poster_target, &art_target] {
        assert!(
            moved_film.attachments.iter().any(|attachment| {
                attachment.relative_path == *target && attachment.kind == "cover"
            }),
            "rescan should resolve custom art {target} to Film"
        );
    }
    let moved_other = item_at(&rescanned, "Other/Other.mkv");
    assert!(moved_other.attachments.iter().any(|attachment| {
        attachment.relative_path == "Other/other-cover.jpg" && attachment.kind == "cover"
    }));
    assert_eq!(
        fs::read(root.path().join(&poster_target)).unwrap(),
        b"Film poster bytes"
    );
    assert_eq!(
        fs::read(root.path().join(&art_target)).unwrap(),
        b"Film secondary art bytes"
    );
}

#[test]
fn review78_generated_nfo_conflicts_are_planned_before_skip_or_keep_both() {
    let root = TemporaryRoot::new("review78-generated-nfo-destination-conflict");
    let film_dir = root.path().join("Film");
    let destination = root.path().join("Organized");
    fs::create_dir_all(&film_dir).expect("Film source directory should create");
    fs::create_dir_all(&destination).expect("destination directory should create");
    fs::write(film_dir.join("Film.mp4"), b"fixture media").expect("source media should write");
    fs::write(film_dir.join("cover.jpg"), b"fixture cover").expect("source cover should write");
    let foreign_nfo = concat!(
        "<movie><title>Unrelated orphan NFO</title>",
        "<uniqueid type=\"youtube\">orphan12345</uniqueid>",
        "<futureField>must remain untouched</futureField></movie>"
    );
    fs::write(destination.join("Film.nfo"), foreign_nfo)
        .expect("pre-existing orphan NFO should write");

    let manager = manager(&root);
    let snapshot = manager.scan(&[]).expect("source library should scan");
    let film = item_at(&snapshot, "Film/Film.mp4");
    let skip_preview = manager
        .preview(
            std::slice::from_ref(&film.id),
            "Organized",
            false,
            ConflictPolicy::Skip,
        )
        .expect("Skip preview should include synthesized NFO conflict checks");
    assert_eq!(skip_preview.items[0].outcome, MoveOutcome::Skip);
    let skip_result = manager
        .execute_preview(&skip_preview.id, &skip_preview.revision)
        .expect("Skip confirmation should record the conflict without moving files");
    assert_eq!(skip_result.status, BatchStatus::Failed);
    assert!(film_dir.join("Film.mp4").exists());
    assert!(film_dir.join("cover.jpg").exists());
    assert!(!destination.join("Film.mp4").exists());
    assert!(!destination.join("cover.jpg").exists());
    assert_eq!(
        fs::read_to_string(destination.join("Film.nfo")).unwrap(),
        foreign_nfo,
        "Skip must preserve the unrelated NFO byte-for-byte"
    );

    let keep_both = manager
        .preview(
            std::slice::from_ref(&film.id),
            "Organized",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("KeepBoth should plan a consistent media and synthesized-NFO target");
    assert_eq!(keep_both.items[0].outcome, MoveOutcome::Move);
    let media_target = keep_both.items[0].target_path.clone();
    let expected_nfo_target = Path::new(&media_target)
        .with_extension("nfo")
        .to_string_lossy()
        .into_owned();
    let patch_preview = keep_both
        .metadata_patches
        .iter()
        .find(|patch| patch.item_id == film.id)
        .expect("KeepBoth preview should include its synthesized-NFO patch");
    assert_eq!(patch_preview.nfo_path, expected_nfo_target);
    assert_ne!(patch_preview.nfo_path, "Organized/Film.nfo");
    assert_ne!(media_target, "Organized/Film.mp4");
    let cover_target = keep_both.items[0]
        .files
        .iter()
        .find(|file| file.source_path == "Film/cover.jpg")
        .expect("KeepBoth preview should include the declared legacy cover")
        .target_path
        .clone();
    assert!(!root.path().join(&media_target).exists());
    assert!(!root.path().join(&cover_target).exists());
    assert!(!root.path().join(&expected_nfo_target).exists());

    let result = manager
        .execute_preview(&keep_both.id, &keep_both.revision)
        .expect("confirmed KeepBoth move should publish the media, cover, and NFO together");
    assert_eq!(result.status, BatchStatus::Complete);
    assert!(!film_dir.join("Film.mp4").exists());
    assert!(!film_dir.join("cover.jpg").exists());
    assert_eq!(
        fs::read(root.path().join(&media_target)).unwrap(),
        b"fixture media"
    );
    assert_eq!(
        fs::read(root.path().join(&cover_target)).unwrap(),
        b"fixture cover"
    );
    assert_eq!(
        fs::read_to_string(destination.join("Film.nfo")).unwrap(),
        foreign_nfo,
        "generated metadata must never take over the pre-existing orphan NFO"
    );
    let generated_nfo = fs::read_to_string(root.path().join(&expected_nfo_target))
        .expect("generated NFO should be written at the previewed target");
    let media_filename = Path::new(&media_target)
        .file_name()
        .and_then(OsStr::to_str)
        .expect("media target filename should be UTF-8");
    let cover_filename = Path::new(&cover_target)
        .file_name()
        .and_then(OsStr::to_str)
        .expect("cover target filename should be UTF-8");
    assert!(generated_nfo.contains(&format!("<localfilename>{media_filename}</localfilename>")));
    assert!(generated_nfo.contains(&format!(
        "<tvd_attachment role=\"cover\">{cover_filename}</tvd_attachment>"
    )));

    let rescanned = manager.scan(&[]).expect("KeepBoth result should rescan");
    let moved = item_at(&rescanned, &media_target);
    assert_eq!(
        moved.authoritative_nfo_path.as_deref(),
        Some(expected_nfo_target.as_str())
    );
    assert!(
        !moved
            .source_ids
            .iter()
            .any(|id| id == "youtube:orphan12345")
    );
    assert!(moved.attachments.iter().any(|attachment| {
        attachment.relative_path == cover_target && attachment.kind == "cover"
    }));
    assert!(
        rescanned
            .items
            .iter()
            .all(|item| item.relative_path != "Organized/Film.mp4")
    );
}

#[test]
fn review78_empty_uniqueid_placeholders_receive_requested_source_ids() {
    let root = TemporaryRoot::new("review78-empty-uniqueid-placeholders");
    fs::write(root.path().join("clip.mkv"), b"fixture media").expect("media should write");
    let original_nfo = concat!(
        "<movie><title>Clip</title>",
        "<uniqueid type=\"bilibili\">BV1234567</uniqueid>",
        "<uniqueid type=\"bilibili-cid\"/>",
        "<uniqueid type=\"youtube\"> \t\n </uniqueid>",
        "<futureField>preserve placeholder context</futureField></movie>"
    );
    fs::write(root.path().join("clip.nfo"), original_nfo).expect("NFO should write");
    fs::set_permissions(
        root.path().join("clip.nfo"),
        fs::Permissions::from_mode(0o600),
    )
    .expect("NFO should be private");

    let manager = manager(&root);
    let snapshot = manager.scan(&[]).expect("library should scan");
    let clip = item_at(&snapshot, "clip.mkv");
    let patch = MetadataPatch {
        item_id: clip.id.clone(),
        title: None,
        source_ids: vec![
            "cid12345678".to_string(),
            "youtube:abcdefghijk".to_string(),
            "BV7654321".to_string(),
        ],
        collection: None,
        fill_missing_only: false,
        organization_status: None,
    };
    let preview = manager
        .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
        .expect("metadata-only source-ID preview should prepare");
    assert!(
        preview.items.is_empty(),
        "source-ID preview must not move media"
    );
    let patch_preview = preview
        .metadata_patches
        .iter()
        .find(|candidate| candidate.item_id == clip.id)
        .expect("preview should include the selected metadata patch");
    assert!(
        patch_preview
            .added_source_ids
            .contains(&"cid12345678".to_string())
    );
    assert!(
        patch_preview
            .added_source_ids
            .contains(&"youtube:abcdefghijk".to_string())
    );
    assert!(
        !patch_preview
            .added_source_ids
            .contains(&"BV7654321".to_string())
    );
    assert_eq!(
        fs::read_to_string(root.path().join("clip.nfo")).unwrap(),
        original_nfo,
        "preview must not rewrite empty placeholders before confirmation"
    );

    let result = manager
        .execute_preview(&preview.id, &preview.revision)
        .expect("confirmed metadata patch should write the requested missing IDs");
    assert_eq!(result.status, BatchStatus::Complete);
    let updated_nfo =
        fs::read_to_string(root.path().join("clip.nfo")).expect("confirmed NFO should read");
    assert!(updated_nfo.contains("<uniqueid type=\"bilibili\">BV1234567</uniqueid>"));
    assert!(updated_nfo.contains("<uniqueid type=\"bilibili-cid\">cid12345678</uniqueid>"));
    assert!(updated_nfo.contains("<uniqueid type=\"youtube\">abcdefghijk</uniqueid>"));
    assert!(!updated_nfo.contains("BV7654321"));
    assert!(updated_nfo.contains("<futureField>preserve placeholder context</futureField>"));

    let rescanned = manager.scan(&[]).expect("patched source IDs should rescan");
    let updated = item_at(&rescanned, "clip.mkv");
    for source_id in ["BV1234567", "cid12345678", "youtube:abcdefghijk"] {
        assert!(updated.source_ids.iter().any(|found| found == source_id));
    }
    assert!(!updated.source_ids.iter().any(|found| found == "BV7654321"));
}
