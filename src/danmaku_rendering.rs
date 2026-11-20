use anyhow::{Context, Result};
use danmaku_renderer::{
    RenderOptions, RenderedAss, render_ass, render_ass_prefix, render_ass_prefix_v1,
};

use crate::config::BilibiliDanmakuConfig;

const MARKER_FAMILY: &str = "; TelegramVideoDownloader Danmaku XML ";

#[derive(Clone, Copy, PartialEq, Eq)]
enum LayoutVersion {
    V1,
    V2,
}

impl LayoutVersion {
    fn marker_suffix(self) -> &'static str {
        match self {
            Self::V1 => "v1: ",
            Self::V2 => "v2: ",
        }
    }
}

#[derive(PartialEq)]
struct SourceProfile {
    source_comments: usize,
    options: RenderOptions,
}

pub(crate) fn render_grouped_ass(xml: &str, settings: &BilibiliDanmakuConfig) -> Result<String> {
    let options = RenderOptions {
        font_scale: settings.font_scale,
        opacity: settings.opacity,
        display_area: settings.display_area,
        merge_window_ms: settings.merge_window_ms,
    };
    stamp(render_ass(xml, &options)?, &options, LayoutVersion::V2)
}

fn stamp(
    mut rendered: RenderedAss,
    options: &RenderOptions,
    version: LayoutVersion,
) -> Result<String> {
    let header = "[Script Info]\n";
    rendered
        .ass
        .strip_prefix(header)
        .context("renderer did not produce an ASS script header")?;
    let marker = format!(
        "{MARKER_FAMILY}{}source_comments={};font_scale={};opacity={};display_area={};merge_window_ms={}\n",
        version.marker_suffix(),
        rendered.source_comments,
        options.font_scale,
        options.opacity,
        options.display_area,
        options.merge_window_ms,
    );
    rendered.ass.insert_str(header.len(), &marker);
    Ok(rendered.ass)
}

/// Recognize an unmodified generated ASS, including an ASS left behind after XML was
/// durably extended. The prefix is selected in XML storage order before timestamp sorting.
/// The marker only selects the inputs to re-render. Matching requires the entire output.
pub(crate) fn grouped_ass_matches(ass: &str, xml: &str) -> Option<bool> {
    let marker = grouped_marker(ass)?;
    Some(
        parse_marker(marker)
            .is_some_and(|(version, profile)| reproduces_ass(ass, xml, version, &profile)),
    )
}

/// Decide whether a reported grouped sidecar can be replaced by a new layout for precisely the
/// same input and settings. A shorter generated prefix can be repaired by the refresh workflow,
/// but it is not a complete, styled download sidecar.
pub(crate) fn grouped_ass_can_upgrade(ass: &str, xml: &str, replacement: &str) -> Option<bool> {
    let marker = grouped_marker(ass)?;
    Some(parse_marker(marker).is_some_and(|(version, profile)| {
        version == LayoutVersion::V1
            && grouped_marker(replacement)
                .and_then(parse_marker)
                .is_some_and(|(new_version, new_profile)| {
                    new_version == LayoutVersion::V2 && new_profile == profile
                })
            && reproduces_ass(ass, xml, version, &profile)
    }))
}

fn grouped_marker(ass: &str) -> Option<&str> {
    ass.lines()
        .find_map(|line| line.strip_prefix(MARKER_FAMILY))
}

fn reproduces_ass(ass: &str, xml: &str, version: LayoutVersion, profile: &SourceProfile) -> bool {
    let renderer = match version {
        LayoutVersion::V1 => render_ass_prefix_v1,
        LayoutVersion::V2 => render_ass_prefix,
    };
    renderer(xml, &profile.options, profile.source_comments)
        .and_then(|rendered| stamp(rendered, &profile.options, version))
        .is_ok_and(|expected| expected == ass)
}

fn parse_marker(marker: &str) -> Option<(LayoutVersion, SourceProfile)> {
    let (version, fields) = [LayoutVersion::V1, LayoutVersion::V2]
        .into_iter()
        .find_map(|version| {
            marker
                .strip_prefix(version.marker_suffix())
                .map(|fields| (version, fields))
        })?;
    let mut fields = fields.split(';');
    let source_comments = marker_field(&mut fields, "source_comments=")?
        .parse()
        .ok()?;
    let options = RenderOptions {
        font_scale: marker_field(&mut fields, "font_scale=")?.parse().ok()?,
        opacity: marker_field(&mut fields, "opacity=")?.parse().ok()?,
        display_area: marker_field(&mut fields, "display_area=")?.parse().ok()?,
        merge_window_ms: marker_field(&mut fields, "merge_window_ms=")?
            .parse()
            .ok()?,
    };
    fields.next().is_none().then_some((
        version,
        SourceProfile {
            source_comments,
            options,
        },
    ))
}

fn marker_field<'a>(fields: &mut impl Iterator<Item = &'a str>, key: &str) -> Option<&'a str> {
    fields.next()?.strip_prefix(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safe_fs::RootedFs;

    const FIRST_COMMENT: &str = r#"<d p="1,1,25,16777215">one
two
three
four</d>"#;
    const NEXT_COMMENT: &str = r#"<d p="1.5,1,25,16777215">new comment</d>"#;

    const PROFILE: RenderOptions = RenderOptions {
        font_scale: 3.0,
        opacity: 0.7,
        display_area: 0.5,
        merge_window_ms: 1_500,
    };

    fn old_v1(xml: &str, source_comments: usize, options: &RenderOptions) -> Result<String> {
        stamp(
            render_ass_prefix_v1(xml, options, source_comments)?,
            options,
            LayoutVersion::V1,
        )
    }

    #[test]
    fn recognizes_unmodified_v1_prefixes_for_refresh_after_xml_is_extended() -> Result<()> {
        let xml = format!("<i>{FIRST_COMMENT}{NEXT_COMMENT}</i>");
        let old = old_v1(&xml, 1, &PROFILE)?;
        let new = render_grouped_ass(&xml, &BilibiliDanmakuConfig::default())?;

        assert_eq!(grouped_ass_matches(&old, &xml), Some(true));
        assert_eq!(grouped_ass_matches(&new, &xml), Some(true));
        assert_eq!(grouped_ass_can_upgrade(&old, &xml, &new), Some(false));
        assert_eq!(
            grouped_ass_matches(&old.replace("\\Nfour", "\\Nedited"), &xml),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn download_replaces_complete_v1_sidecar_once_and_preserves_user_edits() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let xml_path = directory.path().join("video.xml");
        let ass_path = directory.path().join("video.ass");
        let root = RootedFs::new(directory.path())?;
        let xml = format!("<i>{FIRST_COMMENT}{NEXT_COMMENT}</i>");
        let old = old_v1(&xml, 2, &PROFILE)?;
        let config = BilibiliDanmakuConfig::default();
        std::fs::write(&xml_path, &xml)?;
        std::fs::write(&ass_path, &old)?;

        let render =
            || crate::bilibili_danmaku::render_grouped_file(&root, &xml_path, &ass_path, &config);
        render()?;
        let new = std::fs::read_to_string(&ass_path)?;
        assert!(new.contains("Danmaku XML v2: source_comments=2"));
        assert_eq!(grouped_ass_matches(&new, &xml), Some(true));
        render()?;
        assert_eq!(std::fs::read_to_string(&ass_path)?, new);
        assert_eq!(std::fs::read_to_string(&xml_path)?, xml);

        let changed_profile = old_v1(
            &xml,
            2,
            &RenderOptions {
                opacity: 0.8,
                ..PROFILE
            },
        )?;
        assert_eq!(grouped_ass_matches(&changed_profile, &xml), Some(true));
        for rejected in [format!("{old}; user edit\n"), changed_profile] {
            std::fs::write(&ass_path, &rejected)?;
            assert!(render().is_err());
            assert_eq!(std::fs::read_to_string(&ass_path)?, rejected);
        }
        Ok(())
    }
}
