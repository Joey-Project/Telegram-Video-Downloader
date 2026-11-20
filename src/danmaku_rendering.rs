use anyhow::{Context, Result};
use danmaku_renderer::{RenderOptions, RenderedAss, render_ass, render_ass_prefix};

use crate::config::BilibiliDanmakuConfig;

const MARKER_FAMILY: &str = "; TelegramVideoDownloader Danmaku XML ";
const MARKER_V1: &str = "; TelegramVideoDownloader Danmaku XML v1: ";

pub(crate) fn render_grouped_ass(xml: &str, settings: &BilibiliDanmakuConfig) -> Result<String> {
    let options = RenderOptions {
        font_scale: settings.font_scale,
        opacity: settings.opacity,
        display_area: settings.display_area,
        merge_window_ms: settings.merge_window_ms,
    };
    stamp(render_ass(xml, &options)?, &options)
}

fn stamp(mut rendered: RenderedAss, options: &RenderOptions) -> Result<String> {
    let header = "[Script Info]\n";
    rendered
        .ass
        .strip_prefix(header)
        .context("renderer did not produce an ASS script header")?;
    let marker = format!(
        "{MARKER_V1}source_comments={};font_scale={};opacity={};display_area={};merge_window_ms={}\n",
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
    let marker = ass.lines().find(|line| line.starts_with(MARKER_FAMILY))?;
    let matches = parse_marker(marker)
        .and_then(|(count, options)| {
            let rendered = render_ass_prefix(xml, &options, count).ok()?;
            stamp(rendered, &options).ok()
        })
        .is_some_and(|expected| expected == ass);
    Some(matches)
}

fn parse_marker(marker: &str) -> Option<(usize, RenderOptions)> {
    let mut fields = marker.strip_prefix(MARKER_V1)?.split(';');
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
    fields
        .next()
        .is_none()
        .then_some((source_comments, options))
}

fn marker_field<'a>(fields: &mut impl Iterator<Item = &'a str>, key: &str) -> Option<&'a str> {
    fields.next()?.strip_prefix(key)
}
