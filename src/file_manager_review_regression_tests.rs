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

#[test]
fn review78_extra_nfo_thumb_ownership_survives_multi_primary_organization() {
    let root = TemporaryRoot::new("review78-extra-nfo-thumb-organization");
    let destination = root.path().join("Organized");
    fs::create_dir(&destination).expect("destination directory should create");
    fs::write(root.path().join("A.mkv"), b"A media").expect("A media should write");
    fs::write(root.path().join("B.mkv"), b"B media").expect("B media should write");
    fs::write(root.path().join("A-cover.jpg"), b"A cover").expect("A cover should write");
    fs::write(destination.join("A-cover.jpg"), b"existing cover")
        .expect("cover collision should write");
    fs::write(
        root.path().join("A.nfo"),
        concat!(
            "<movie><title>Old A title</title>",
            "<localfilename>A.mkv</localfilename>",
            "<uniqueid type=\"bilibili\">BV1A1234567</uniqueid>",
            "<futureField owner=\"authoritative\">preserve A</futureField></movie>"
        ),
    )
    .expect("A authoritative NFO should write");
    fs::write(
        root.path().join("A.extra.nfo"),
        concat!(
            "<movie><title>Secondary A title</title>",
            "<thumb>A-cover.jpg</thumb>",
            "<futureField owner=\"secondary\">preserve extra</futureField></movie>"
        ),
    )
    .expect("A secondary NFO should write");
    fs::write(
        root.path().join("B.nfo"),
        "<movie><title>B title</title><localfilename>B.mkv</localfilename></movie>",
    )
    .expect("B NFO should write");

    let manager = manager(&root);
    let snapshot = manager.scan(&[]).expect("source library should scan");
    let a = item_at(&snapshot, "A.mkv");
    let b = item_at(&snapshot, "B.mkv");
    assert_eq!(a.authoritative_nfo_path.as_deref(), Some("A.nfo"));
    for path in ["A.nfo", "A.extra.nfo"] {
        assert!(
            a.attachments
                .iter()
                .any(|attachment| { attachment.relative_path == path && attachment.kind == "nfo" })
        );
    }
    assert!(a.attachments.iter().any(|attachment| {
        attachment.relative_path == "A-cover.jpg" && attachment.kind == "cover"
    }));
    assert!(b.attachments.iter().all(|attachment| {
        attachment.relative_path != "A-cover.jpg" && attachment.relative_path != "A.extra.nfo"
    }));

    let patch = MetadataPatch {
        item_id: a.id.clone(),
        title: Some("Confirmed A title".to_string()),
        source_ids: Vec::new(),
        collection: None,
        fill_missing_only: false,
        organization_status: None,
    };
    let preview = manager
        .preview_with_hints_and_patches(
            &[a.id.clone(), b.id.clone()],
            Some("Organized"),
            false,
            ConflictPolicy::KeepBoth,
            &[],
            &[patch],
        )
        .expect("both primaries and A's attached files should be previewable");
    assert_eq!(preview.items.len(), 2);
    assert!(
        preview
            .items
            .iter()
            .all(|item| item.outcome == MoveOutcome::Move)
    );
    let target_for = |source_path: &str| {
        preview
            .items
            .iter()
            .flat_map(|item| item.files.iter())
            .find(|file| file.source_path == source_path)
            .unwrap_or_else(|| panic!("preview should include {source_path}"))
            .target_path
            .clone()
    };
    let a_media_target = target_for("A.mkv");
    let a_cover_target = target_for("A-cover.jpg");
    let a_authoritative_target = target_for("A.nfo");
    let a_extra_target = target_for("A.extra.nfo");
    let b_media_target = target_for("B.mkv");
    assert_ne!(a_media_target, "Organized/A.mkv");
    assert_ne!(a_cover_target, "Organized/A-cover.jpg");
    assert!(preview.metadata_patches.iter().any(|metadata_patch| {
        metadata_patch.nfo_path == a_authoritative_target
            && metadata_patch
                .changes
                .iter()
                .any(|change| change.field == "title" && change.after == "Confirmed A title")
    }));
    assert!(
        preview
            .metadata_patches
            .iter()
            .any(|metadata_patch| metadata_patch.nfo_path == a_extra_target)
    );

    let result = manager
        .execute_preview(&preview.id, &preview.revision)
        .expect("confirmed multi-primary organization should complete");
    assert_eq!(result.status, BatchStatus::Complete);
    let authoritative = fs::read_to_string(root.path().join(&a_authoritative_target))
        .expect("moved authoritative NFO should read");
    let extra = fs::read_to_string(root.path().join(&a_extra_target))
        .expect("moved secondary NFO should read");
    let cover_filename = Path::new(&a_cover_target)
        .file_name()
        .and_then(OsStr::to_str)
        .expect("cover target basename should be UTF-8");
    let media_filename = Path::new(&a_media_target)
        .file_name()
        .and_then(OsStr::to_str)
        .expect("media target basename should be UTF-8");
    assert!(authoritative.contains("<title>Confirmed A title</title>"));
    assert!(authoritative.contains("<uniqueid type=\"bilibili\">BV1A1234567</uniqueid>"));
    assert!(authoritative.contains(&format!("<localfilename>{media_filename}</localfilename>")));
    assert!(
        authoritative.contains("<futureField owner=\"authoritative\">preserve A</futureField>")
    );
    assert!(
        !authoritative.contains("tvd_attachment") && !authoritative.contains("<thumb>"),
        "the authoritative NFO had no cover declaration and must not acquire one"
    );
    assert!(extra.contains(&format!("<thumb>{cover_filename}</thumb>")));
    assert!(extra.contains("<futureField owner=\"secondary\">preserve extra</futureField>"));

    let rescanned = manager
        .scan(&[])
        .expect("organized multiple-primary library should rescan");
    let moved_a = item_at(&rescanned, &a_media_target);
    assert_eq!(
        moved_a.authoritative_nfo_path.as_deref(),
        Some(a_authoritative_target.as_str())
    );
    for path in [&a_authoritative_target, &a_extra_target] {
        assert!(
            moved_a.attachments.iter().any(|attachment| {
                attachment.relative_path == *path && attachment.kind == "nfo"
            })
        );
    }
    assert!(moved_a.attachments.iter().any(|attachment| {
        attachment.relative_path == a_cover_target && attachment.kind == "cover"
    }));
    let moved_b = item_at(&rescanned, &b_media_target);
    assert!(moved_b.attachments.iter().all(|attachment| {
        attachment.relative_path != a_cover_target && attachment.relative_path != a_extra_target
    }));
}

#[test]
fn review78_conflicting_nfo_claims_stay_unattached_and_normalized_aliases_deduplicate() {
    let root = TemporaryRoot::new("review78-nfo-conflicting-claims");
    fs::write(root.path().join("A.mkv"), b"A media").expect("A media should write");
    fs::write(root.path().join("B.mkv"), b"B media").expect("B media should write");
    fs::write(root.path().join("same.ass"), b"shared ASS").expect("ASS sidecar should write");
    fs::write(root.path().join("shared.jpg"), b"shared cover").expect("shared cover should write");
    fs::write(
        root.path().join("A.nfo"),
        concat!(
            "<movie><tvd_attachment role=\"subtitle\">same.ass</tvd_attachment>",
            "<tvd_attachment role=\"cover\">shared.jpg</tvd_attachment></movie>"
        ),
    )
    .expect("A authoritative NFO should write");
    fs::write(
        root.path().join("A.extra.nfo"),
        "<movie><tvd_attachment role=\"danmaku\">same.ass</tvd_attachment></movie>",
    )
    .expect("A secondary NFO should write");
    fs::write(
        root.path().join("B.nfo"),
        "<movie><tvd_attachment role=\"cover\">shared.jpg</tvd_attachment></movie>",
    )
    .expect("B NFO should write");

    let manager = manager(&root);
    let snapshot = manager
        .scan(&[])
        .expect("conflicting declarations should scan safely");
    let a = item_at(&snapshot, "A.mkv");
    let b = item_at(&snapshot, "B.mkv");
    for path in ["A.nfo", "A.extra.nfo"] {
        assert!(
            a.attachments
                .iter()
                .any(|attachment| { attachment.relative_path == path && attachment.kind == "nfo" })
        );
    }
    assert!(
        b.attachments
            .iter()
            .any(|attachment| attachment.relative_path == "B.nfo" && attachment.kind == "nfo")
    );
    for item in [a, b] {
        assert!(item.attachments.iter().all(|attachment| {
            attachment.relative_path != "same.ass" && attachment.relative_path != "shared.jpg"
        }));
    }

    let semantics = TargetNameSemantics {
        normalization_insensitive: true,
        case_insensitive: false,
        hfs_ignorable: false,
    };
    let case_fold = |value: &str| Ok(value.to_string());
    let files = vec![
        round4_scanned_file("Media/A.mkv", 1),
        round4_scanned_file("Media/B.mkv", 2),
        round4_scanned_file("Media/A.nfo", 3),
        round4_scanned_file("Media/A.extra.nfo", 4),
        round4_scanned_file("Media/B.nfo", 5),
        round4_scanned_file("Media/same.ass", 6),
        round4_scanned_file("Media/shared.jpg", 7),
        round4_scanned_file("Media/Café.jpg", 8),
    ];
    let directory_semantics = round4_semantics_by_parent(&files, semantics);
    let mut sidecars = SidecarIndex::build_with_case_fold(
        &files,
        &BTreeSet::new(),
        &BTreeSet::new(),
        &directory_semantics,
        case_fold,
    )
    .expect("modeled filesystem semantics should build the sidecar index");
    let declarations = HashMap::from([
        (
            "Media/A.mkv".to_string(),
            vec![
                NfoAttachmentDeclaration {
                    role: "subtitle".to_string(),
                    filename: "same.ass".to_string(),
                },
                NfoAttachmentDeclaration {
                    role: "danmaku".to_string(),
                    filename: "same.ass".to_string(),
                },
                NfoAttachmentDeclaration {
                    role: "cover".to_string(),
                    filename: "shared.jpg".to_string(),
                },
                NfoAttachmentDeclaration {
                    role: "cover".to_string(),
                    filename: "Café.jpg".to_string(),
                },
                NfoAttachmentDeclaration {
                    role: "cover".to_string(),
                    filename: "Cafe\u{301}.jpg".to_string(),
                },
            ],
        ),
        (
            "Media/B.mkv".to_string(),
            vec![NfoAttachmentDeclaration {
                role: "cover".to_string(),
                filename: "shared.jpg".to_string(),
            }],
        ),
    ]);
    sidecars
        .add_declared_attachments_with_case_fold(
            &files,
            &declarations,
            &directory_semantics,
            case_fold,
        )
        .expect("same-owner aliases should deduplicate while ambiguous claims stay rejected");
    assert_eq!(
        sidecars.declared_role("Media/A.mkv", "Media/Café.jpg"),
        Some("cover")
    );
    assert_eq!(
        sidecars
            .attached_files(&files[0])
            .iter()
            .filter(|path| path.as_str() == "Media/Café.jpg")
            .count(),
        1,
        "normalization-equivalent same-role claims should attach the scanned spelling once"
    );
    for media in ["Media/A.mkv", "Media/B.mkv"] {
        assert_eq!(sidecars.declared_role(media, "Media/same.ass"), None);
        assert_eq!(sidecars.declared_role(media, "Media/shared.jpg"), None);
    }
}

#[test]
fn review78_nfo_declaration_deadline_preserves_prior_rows_and_recovers() {
    let root = TemporaryRoot::new("review78-nfo-declaration-deadline");
    fs::write(root.path().join("A.mkv"), b"A media").expect("A media should write");
    fs::write(root.path().join("B.mkv"), b"B media").expect("B media should write");
    let manager = manager(&root);
    let baseline = manager.scan(&[]).expect("baseline library should scan");
    let prior_a = item_at(&baseline, "A.mkv").clone();
    let prior_b = item_at(&baseline, "B.mkv").clone();

    fs::write(root.path().join("bonus.jpg"), b"declared bonus")
        .expect("declared bonus should write");
    fs::write(
        root.path().join("A.extra.nfo"),
        "<movie><thumb>bonus.jpg</thumb></movie>",
    )
    .expect("first NFO declaration source should write");
    fs::write(
        root.path().join("A.nfo"),
        "<movie><tvd_attachment role=\"cover\">../bad.jpg</tvd_attachment></movie>",
    )
    .expect("malformed second NFO declaration source should write");

    manager.expire_next_scan_during_nfo_declarations_after(1);
    let partial = manager
        .scan(&[])
        .expect("declaration deadline should return a safe partial snapshot");
    assert!(partial.warnings.iter().any(|warning| {
        warning.contains("120-second scan budget")
            && warning.contains("unvisited paths were not marked missing")
    }));
    assert!(
        partial
            .warnings
            .iter()
            .all(|warning| !warning.contains("invalid portable attachment declaration in A.nfo")),
        "the malformed second NFO should remain unread after the one-source cutoff"
    );
    let partial_a = item_at(&partial, "A.mkv");
    assert_eq!(partial_a.id, prior_a.id);
    assert_eq!(partial_a.presence, Presence::Unavailable);
    assert_eq!(partial_a.title, prior_a.title);
    assert_eq!(partial_a.attachments, prior_a.attachments);
    assert!(
        partial_a
            .attachments
            .iter()
            .all(|attachment| attachment.relative_path != "bonus.jpg")
    );
    let partial_b = item_at(&partial, "B.mkv");
    assert_eq!(partial_b.id, prior_b.id);
    assert_eq!(partial_b.presence, Presence::Unavailable);
    assert_eq!(partial_b.title, prior_b.title);

    manager.expire_next_scan_during_nfo_declarations_after(0);
    let zero_sources = manager
        .scan(&[])
        .expect("zero-source cutoff should also return a safe partial snapshot");
    assert!(zero_sources.warnings.iter().any(|warning| {
        warning.contains("120-second scan budget")
            && warning.contains("unvisited paths were not marked missing")
    }));
    assert!(
        zero_sources
            .warnings
            .iter()
            .all(|warning| !warning.contains("invalid portable attachment declaration in A.nfo"))
    );
    assert_eq!(
        item_at(&zero_sources, "A.mkv").presence,
        Presence::Unavailable
    );
    assert_eq!(
        item_at(&zero_sources, "B.mkv").presence,
        Presence::Unavailable
    );
    assert_eq!(item_at(&zero_sources, "A.mkv").id, prior_a.id);
    assert_eq!(item_at(&zero_sources, "B.mkv").id, prior_b.id);

    fs::write(
        root.path().join("A.nfo"),
        "<movie><title>Recovered A</title></movie>",
    )
    .expect("malformed NFO should be repaired before recovery scan");
    let recovered = manager
        .scan(&[])
        .expect("a complete rescan should recover the declared cover");
    let recovered_a = item_at(&recovered, "A.mkv");
    assert_eq!(recovered_a.id, prior_a.id);
    assert_eq!(recovered_a.presence, Presence::Present);
    assert_eq!(recovered_a.title, "Recovered A");
    assert!(recovered_a.attachments.iter().any(|attachment| {
        attachment.relative_path == "bonus.jpg" && attachment.kind == "cover"
    }));
    for path in ["A.nfo", "A.extra.nfo"] {
        assert!(
            recovered_a
                .attachments
                .iter()
                .any(|attachment| { attachment.relative_path == path && attachment.kind == "nfo" })
        );
    }
    assert_eq!(item_at(&recovered, "B.mkv").presence, Presence::Present);
    assert!(
        recovered
            .warnings
            .iter()
            .all(|warning| !warning.contains("120-second scan budget"))
    );
}
