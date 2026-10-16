use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use bbdown_core::{DownloadFileKind, DownloadReport};
use roxmltree::{Document, Node};

use crate::safe_fs::RootedFs;

const DANMAKU_SIDECAR_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default)]
struct XmlCommentCounts {
    all: usize,
    supported: usize,
    unsupported: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct AssCounts {
    dialogues: usize,
}

/// Validates Bilibili danmaku sidecars before a successful core download is published.
///
/// The protected property is the identity of the reported regular file inside the configured
/// output root. Bytes are read through a descriptor bound by `RootedFs`; metadata-only changes
/// are not treated as content changes.
pub(crate) fn validate_downloaded_danmaku(root: &RootedFs, report: &DownloadReport) -> Result<()> {
    for entry in &report.entries {
        let xml_files = entry
            .files
            .iter()
            .filter(|file| matches!(&file.kind, DownloadFileKind::Danmaku))
            .collect::<Vec<_>>();
        let ass_files = entry
            .files
            .iter()
            .filter(|file| matches!(&file.kind, DownloadFileKind::DanmakuAss))
            .collect::<Vec<_>>();

        if xml_files.is_empty() && ass_files.is_empty() {
            continue;
        }

        let mut validated_xml = Vec::with_capacity(xml_files.len());
        for file in xml_files {
            let path = rooted_report_path(root, &file.path)?;
            let bytes = read_reported_sidecar(root, &path)?;
            let xml = std::str::from_utf8(&bytes).with_context(|| {
                format!("danmaku XML is not valid UTF-8: {}", file.path.display())
            })?;
            let counts = validate_danmaku_xml(xml)
                .with_context(|| format!("invalid danmaku XML {}", file.path.display()))?;
            validated_xml.push((path, counts));
        }

        for file in ass_files {
            let path = rooted_report_path(root, &file.path)?;
            let bytes = read_reported_sidecar(root, &path)?;
            let ass = std::str::from_utf8(&bytes).with_context(|| {
                format!("danmaku ASS is not valid UTF-8: {}", file.path.display())
            })?;
            let ass_counts = validate_ass(ass)
                .with_context(|| format!("invalid danmaku ASS {}", file.path.display()))?;

            let matching_xml = validated_xml
                .iter()
                .find(|(xml_path, _)| xml_path.parent() == path.parent());
            if let Some((_, xml_counts)) = matching_xml {
                if xml_counts.supported > 0 && ass_counts.dialogues == 0 {
                    bail!(
                        "danmaku ASS {} has no Dialogue lines, but its XML contains {} converter-supported comments",
                        file.path.display(),
                        xml_counts.supported
                    );
                }
                if xml_counts.all > 0
                    && xml_counts.supported == 0
                    && xml_counts.unsupported == xml_counts.all
                    && ass_counts.dialogues == 0
                {
                    tracing::info!(
                        path = %file.path.display(),
                        unsupported_comments = xml_counts.unsupported,
                        "danmaku ASS is empty because its XML contains only converter-unsupported modes"
                    );
                }
            } else if !validated_xml.is_empty() {
                bail!(
                    "reported danmaku XML and ASS files do not share an output directory: {}",
                    file.path.display()
                );
            }
        }
    }

    Ok(())
}

fn validate_danmaku_xml(xml: &str) -> Result<XmlCommentCounts> {
    let document = Document::parse(xml).context("XML is not well-formed")?;
    let root = document.root_element();
    if root.tag_name().name() != "i" {
        bail!("XML root element must be <i>");
    }

    let mut counts = XmlCommentCounts::default();
    for comment in root.descendants().filter(|node| is_danmaku_element(*node)) {
        counts.all += 1;
        if let Some(mode) = converter_comment_mode(comment) {
            if matches!(mode, 7 | 8) {
                counts.unsupported += 1;
            } else {
                counts.supported += 1;
            }
        }
    }
    Ok(counts)
}

fn is_danmaku_element(node: Node<'_, '_>) -> bool {
    node.is_element() && node.tag_name().name() == "d"
}

/// Matches the pinned converter's eligibility rules before its mode-specific render step.
fn converter_comment_mode(comment: Node<'_, '_>) -> Option<u8> {
    let parameters = comment.attribute("p")?;
    let mut values = parameters.split(',');
    let start_seconds = values.next()?.parse::<f64>().ok()?;
    if !start_seconds.is_finite() {
        return None;
    }
    let mode = values.next()?.parse::<u8>().ok()?;
    if comment.text().unwrap_or_default().is_empty() {
        return None;
    }
    Some(mode)
}

fn validate_ass(ass: &str) -> Result<AssCounts> {
    let mut in_events = false;
    let mut has_script_info = false;
    let mut has_events = false;
    let mut has_event_format = false;
    let mut counts = AssCounts::default();

    for raw_line in ass.lines() {
        let line = raw_line.trim();
        if line.starts_with('[') && line.ends_with(']') {
            let section = &line[1..line.len() - 1];
            in_events = section.eq_ignore_ascii_case("Events");
            has_script_info |= section.eq_ignore_ascii_case("Script Info");
            has_events |= in_events;
            continue;
        }
        if in_events {
            if line
                .get(.."Format:".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("Format:"))
            {
                has_event_format = true;
            } else if let Some(payload) = line
                .get(.."Dialogue:".len())
                .filter(|prefix| prefix.eq_ignore_ascii_case("Dialogue:"))
                .map(|_| &line["Dialogue:".len()..])
                && payload.split(',').count() >= 10
            {
                counts.dialogues += 1;
            }
        }
    }

    if !has_script_info || !has_events || !has_event_format {
        bail!("ASS must contain [Script Info], [Events], and an Events Format line");
    }
    Ok(counts)
}

fn read_reported_sidecar(root: &RootedFs, path: &Path) -> Result<Vec<u8>> {
    let file = root
        .open_bound_file(path)?
        .with_context(|| format!("reported danmaku sidecar is missing: {}", path.display()))?;
    let length = file.byte_len()?;
    if length > DANMAKU_SIDECAR_LIMIT as u64 {
        bail!(
            "danmaku sidecar exceeds the {DANMAKU_SIDECAR_LIMIT}-byte validation limit: {}",
            path.display()
        );
    }
    file.read_limited(DANMAKU_SIDECAR_LIMIT)
        .with_context(|| format!("failed to read danmaku sidecar {}", path.display()))
}

fn rooted_report_path(root: &RootedFs, report_path: &Path) -> Result<PathBuf> {
    let relative = report_path
        .strip_prefix(root.logical_root_path())
        .or_else(|_| report_path.strip_prefix(root.root_path()))
        .with_context(|| {
            format!(
                "reported danmaku sidecar is outside the configured output root: {}",
                report_path.display()
            )
        })?;
    let mut normalized = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(name) => normalized.push(name),
            _ => bail!(
                "reported danmaku sidecar has an invalid output-root-relative path: {}",
                report_path.display()
            ),
        }
    }
    if normalized.as_os_str().is_empty() {
        bail!("reported danmaku sidecar does not name a file");
    }
    Ok(root.logical_root_path().join(normalized))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::Result;
    use bbdown_core::{DownloadFileKind, DownloadReport, DownloadedFile, EntryDownloadReport};
    use flate2::Compression;
    use flate2::write::DeflateEncoder;

    use super::validate_downloaded_danmaku;
    use crate::safe_fs::RootedFs;

    static TEST_DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(0);

    const VALID_EMPTY_XML: &str =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><i><chatserver>chat.example</chatserver></i>";
    const VALID_COMMENT_XML: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><i><d p=\"1,1,25,16777215,0,0,0,0\">hello</d></i>";
    const VALID_DIALOGUE_ASS: &str = "[Script Info]\nScriptType: v4.00+\n\n[V4+ Styles]\nFormat: Name, Fontname\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\nDialogue: 0,0:00:01.00,0:00:09.00,Danmaku,,0,0,0,,hello\n";
    const VALID_EMPTY_ASS: &str = "[Script Info]\nScriptType: v4.00+\n\n[V4+ Styles]\nFormat: Name, Fontname\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n";

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(name: &str) -> Self {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after the epoch")
                .as_nanos();
            let counter = TEST_DIRECTORY_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "telegram-video-downloader-{name}-{}-{timestamp}-{counter}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("test output root should be created");
            Self(path)
        }

        fn rooted_fs(&self) -> RootedFs {
            RootedFs::new(&self.0).expect("test output root should bind")
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn report(root: &RootedFs, files: Vec<DownloadedFile>) -> DownloadReport {
        DownloadReport {
            title: "test".to_owned(),
            output_dir: root.logical_root_path().to_path_buf(),
            entries: vec![EntryDownloadReport {
                index: 1,
                title: "test entry".to_owned(),
                directory: root.logical_root_path().to_path_buf(),
                files,
                mux: None,
            }],
        }
    }

    fn downloaded_file(kind: DownloadFileKind, path: PathBuf) -> DownloadedFile {
        let bytes_written = fs::metadata(&path).expect("fixture should exist").len();
        DownloadedFile {
            kind,
            path,
            bytes_written,
            resumed_from: 0,
        }
    }

    fn missing_reported_file(kind: DownloadFileKind, path: PathBuf) -> DownloadedFile {
        DownloadedFile {
            kind,
            path,
            bytes_written: 0,
            resumed_from: 0,
        }
    }

    fn write_fixture(path: &Path, contents: impl AsRef<[u8]>) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("fixture parent should exist");
        }
        fs::write(path, contents).expect("fixture should be written");
    }

    #[test]
    fn validates_a_nonempty_xml_and_ass_pair() -> Result<()> {
        let temp = TestRoot::new("danmaku-valid-pair");
        let root = temp.rooted_fs();
        let xml_path = temp.path("entry/danmaku.xml");
        let ass_path = temp.path("entry/danmaku.ass");
        write_fixture(&xml_path, VALID_COMMENT_XML);
        write_fixture(&ass_path, VALID_DIALOGUE_ASS);

        validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![
                    downloaded_file(DownloadFileKind::Danmaku, xml_path),
                    downloaded_file(DownloadFileKind::DanmakuAss, ass_path),
                ],
            ),
        )
    }

    #[test]
    fn rejects_empty_ass_when_xml_has_supported_comments() {
        let temp = TestRoot::new("danmaku-empty-ass");
        let root = temp.rooted_fs();
        let xml_path = temp.path("entry/danmaku.xml");
        let ass_path = temp.path("entry/danmaku.ass");
        write_fixture(&xml_path, VALID_COMMENT_XML);
        write_fixture(&ass_path, VALID_EMPTY_ASS);

        let result = validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![
                    downloaded_file(DownloadFileKind::Danmaku, xml_path),
                    downloaded_file(DownloadFileKind::DanmakuAss, ass_path),
                ],
            ),
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no Dialogue lines")
        );
    }

    #[test]
    fn accepts_well_formed_empty_xml_and_empty_ass() -> Result<()> {
        let temp = TestRoot::new("danmaku-empty-pair");
        let root = temp.rooted_fs();
        let xml_path = temp.path("entry/danmaku.xml");
        let ass_path = temp.path("entry/danmaku.ass");
        write_fixture(&xml_path, VALID_EMPTY_XML);
        write_fixture(&ass_path, VALID_EMPTY_ASS);

        validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![
                    downloaded_file(DownloadFileKind::Danmaku, xml_path),
                    downloaded_file(DownloadFileKind::DanmakuAss, ass_path),
                ],
            ),
        )
    }

    #[test]
    fn accepts_empty_ass_when_all_comments_use_unsupported_modes() -> Result<()> {
        let temp = TestRoot::new("danmaku-unsupported-modes");
        let root = temp.rooted_fs();
        let xml_path = temp.path("entry/danmaku.xml");
        let ass_path = temp.path("entry/danmaku.ass");
        let xml = "<i><d p=\"1,7,25,16777215,0,0,0,0\">advanced</d><d p=\"2,8,25,16777215,0,0,0,0\">code</d></i>";
        write_fixture(&xml_path, xml);
        write_fixture(&ass_path, VALID_EMPTY_ASS);

        validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![
                    downloaded_file(DownloadFileKind::Danmaku, xml_path),
                    downloaded_file(DownloadFileKind::DanmakuAss, ass_path),
                ],
            ),
        )
    }

    #[test]
    fn rejects_html_invalid_xml_and_non_utf8_xml() {
        for (name, contents) in [
            ("html", b"<html>error</html>".as_slice()),
            ("malformed", b"<i><d>broken</i>".as_slice()),
            ("invalid-utf8", b"<i>\xff</i>".as_slice()),
        ] {
            let temp = TestRoot::new(name);
            let root = temp.rooted_fs();
            let xml_path = temp.path("entry/danmaku.xml");
            write_fixture(&xml_path, contents);

            let result = validate_downloaded_danmaku(
                &root,
                &report(
                    &root,
                    vec![downloaded_file(DownloadFileKind::Danmaku, xml_path)],
                ),
            );
            assert!(result.is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn rejects_compressed_bytes_written_as_xml() {
        let temp = TestRoot::new("danmaku-raw-deflate");
        let root = temp.rooted_fs();
        let xml_path = temp.path("entry/danmaku.xml");
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(VALID_COMMENT_XML.as_bytes())
            .expect("fixture should compress");
        let compressed = encoder.finish().expect("compressed fixture should finish");
        write_fixture(&xml_path, compressed);

        let result = validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![downloaded_file(DownloadFileKind::Danmaku, xml_path)],
            ),
        );
        assert!(result.is_err());
    }

    #[test]
    fn validates_ass_only_reports_without_inferring_source_comment_count() -> Result<()> {
        let temp = TestRoot::new("danmaku-ass-only");
        let root = temp.rooted_fs();
        let ass_path = temp.path("entry/danmaku.ass");
        write_fixture(&ass_path, VALID_EMPTY_ASS);

        validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![downloaded_file(DownloadFileKind::DanmakuAss, ass_path)],
            ),
        )
    }

    #[test]
    fn rejects_missing_reported_xml_and_ass_sidecars() {
        for kind in [DownloadFileKind::Danmaku, DownloadFileKind::DanmakuAss] {
            let temp = TestRoot::new("danmaku-missing-sidecar");
            let root = temp.rooted_fs();
            let sidecar_path = temp.path(match &kind {
                DownloadFileKind::Danmaku => "entry/danmaku.xml",
                DownloadFileKind::DanmakuAss => "entry/danmaku.ass",
                _ => unreachable!("fixture only selects danmaku kinds"),
            });

            let result = validate_downloaded_danmaku(
                &root,
                &report(&root, vec![missing_reported_file(kind, sidecar_path)]),
            );
            assert!(result.is_err(), "missing reported sidecar should fail");
            assert!(result.unwrap_err().to_string().contains("missing"));
        }
    }

    #[test]
    fn rejects_malformed_ass() {
        let temp = TestRoot::new("danmaku-malformed-ass");
        let root = temp.rooted_fs();
        let ass_path = temp.path("entry/danmaku.ass");
        write_fixture(&ass_path, "this is not an ASS file\n");

        let result = validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![downloaded_file(DownloadFileKind::DanmakuAss, ass_path)],
            ),
        );
        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("[Script Info]"));
    }

    #[test]
    fn ignores_reports_without_danmaku_files() -> Result<()> {
        let temp = TestRoot::new("danmaku-unrelated-report");
        let root = temp.rooted_fs();
        let subtitle = temp.path("entry/subtitle.srt");
        write_fixture(&subtitle, "1\n00:00:00,000 --> 00:00:01,000\nhello\n");

        validate_downloaded_danmaku(
            &root,
            &report(
                &root,
                vec![downloaded_file(DownloadFileKind::Subtitle, subtitle)],
            ),
        )
    }
}
