//! Portable renderer for the plain-text subset of Bilibili danmaku XML.
//!
//! The renderer intentionally has no network, Telegram, filesystem, or application
//! configuration dependencies. It accepts ordinary `<i>` documents with direct
//! `<d p="...">text</d>` children and emits Advanced SubStation Alpha (ASS).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use roxmltree::{Document, Node};

mod layout;

const PLAY_RES_X: u32 = 1920;
const PLAY_RES_Y: u32 = 1080;
const DEFAULT_FONT_SIZE: f64 = 42.0;
const SCROLL_DURATION_SECONDS: f64 = 8.0;
const FIXED_DURATION_SECONDS: f64 = 4.0;
const TOP_MARGIN: f64 = 40.0;
const SCROLL_LANES: usize = 15;
const FIXED_LANES: usize = 8;
const MAX_COMMENTS: usize = 50_000;
const MAX_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_MERGE_WINDOW_MS: u64 = 10_000;

/// Maximum accepted source XML size, shared with callers that fetch XML.
pub const MAX_XML_BYTES: usize = 16 * 1024 * 1024;

/// Options controlling font size, opacity, visible screen area, and duplicate grouping.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RenderOptions {
    pub font_scale: f64,
    pub opacity: f64,
    pub display_area: f64,
    pub merge_window_ms: u64,
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self {
            font_scale: 1.0,
            opacity: 1.0,
            display_area: 1.0,
            merge_window_ms: 1_500,
        }
    }
}

/// Rendered ASS and the number of accepted source comments represented by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedAss {
    pub ass: String,
    pub source_comments: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Direction {
    RightToLeft,
    LeftToRight,
}

#[derive(Debug, Clone, PartialEq)]
struct Comment {
    original_index: usize,
    start_seconds: f64,
    mode: u8,
    font_size: f64,
    color: u32,
    text: String,
}

#[derive(Debug, Clone, PartialEq)]
struct Event<'a> {
    anchor: &'a Comment,
    count: usize,
    width: f64,
    height: f64,
    body_font_size: f64,
    counter_font_size: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutVersion {
    V1,
    V2,
    V3,
}

/// Render every accepted, plain-text comment in XML document order before time sorting.
pub fn render_ass(xml: &str, options: &RenderOptions) -> Result<RenderedAss> {
    render_ass_with_limit(xml, options, None, LayoutVersion::V3)
}

/// Render the first `source_comments` accepted comments in XML document order.
///
/// The prefix is selected before sorting or grouping. This makes a previously
/// generated prefix reproducible when later comments are appended to the XML.
pub fn render_ass_prefix(
    xml: &str,
    options: &RenderOptions,
    source_comments: usize,
) -> Result<RenderedAss> {
    render_ass_with_limit(xml, options, Some(source_comments), LayoutVersion::V3)
}

/// Render a source prefix using the original v1 sizing and track layout.
///
/// This compatibility entry point is used to validate refreshes of existing
/// grouped sidecars, whose bytes must continue to match the original renderer.
pub fn render_ass_prefix_v1(
    xml: &str,
    options: &RenderOptions,
    source_comments: usize,
) -> Result<RenderedAss> {
    render_ass_with_limit(xml, options, Some(source_comments), LayoutVersion::V1)
}

/// Render a source prefix using v2's byte-compatible sizing and track layout.
///
/// This compatibility entry point preserves the pre-lookahead 40-pixel margin
/// and greedy placement used to validate existing generated sidecars.
pub fn render_ass_prefix_v2(
    xml: &str,
    options: &RenderOptions,
    source_comments: usize,
) -> Result<RenderedAss> {
    render_ass_with_limit(xml, options, Some(source_comments), LayoutVersion::V2)
}

fn render_ass_with_limit(
    xml: &str,
    options: &RenderOptions,
    source_limit: Option<usize>,
    layout_version: LayoutVersion,
) -> Result<RenderedAss> {
    validate_options(options)?;
    ensure!(
        xml.len() <= MAX_XML_BYTES,
        "danmaku XML exceeds the {MAX_XML_BYTES}-byte limit"
    );

    let mut comments = parse_comments(xml)?;
    if let Some(limit) = source_limit {
        ensure!(
            limit <= comments.len(),
            "requested source prefix of {limit} comments, but only {} are renderable",
            comments.len()
        );
        comments.truncate(limit);
    }
    let source_comments = comments.len();
    comments.sort_by(|left, right| {
        left.start_seconds
            .partial_cmp(&right.start_seconds)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.original_index.cmp(&right.original_index))
    });

    let mut events = group_comments(&comments, options);
    for event in &mut events {
        match layout_version {
            LayoutVersion::V1 => size_event_v1(event, options),
            LayoutVersion::V2 | LayoutVersion::V3 => layout::size_event_v2(event, options),
        }
    }
    let ass = match layout_version {
        LayoutVersion::V1 => render_events_v1(&events, options)?,
        LayoutVersion::V2 => render_events_v2(&events, options)?,
        LayoutVersion::V3 => render_events_v3(&events, options)?,
    };
    Ok(RenderedAss {
        ass,
        source_comments,
    })
}

fn validate_options(options: &RenderOptions) -> Result<()> {
    ensure!(
        options.font_scale.is_finite() && options.font_scale > 0.0 && options.font_scale <= 10.0,
        "font_scale must be finite, greater than 0, and at most 10"
    );
    ensure!(
        options.opacity.is_finite() && (0.0..=1.0).contains(&options.opacity),
        "opacity must be finite and between 0 and 1"
    );
    ensure!(
        options.display_area.is_finite()
            && options.display_area > 0.0
            && options.display_area <= 1.0,
        "display_area must be finite, greater than 0, and at most 1"
    );
    ensure!(
        options.merge_window_ms <= MAX_MERGE_WINDOW_MS,
        "merge_window_ms must be at most {MAX_MERGE_WINDOW_MS}"
    );
    Ok(())
}

fn parse_comments(xml: &str) -> Result<Vec<Comment>> {
    let document = Document::parse(xml).context("danmaku XML is malformed or unsupported")?;
    let root = document.root_element();
    ensure!(
        root.tag_name().name() == "i",
        "danmaku XML root element must be <i>"
    );
    let root_namespace = root.tag_name().namespace().filter(|uri| !uri.is_empty());
    let mut comments = Vec::new();
    let mut p_node_count = 0usize;

    for node in root.children().filter(|node| node.is_element()) {
        let tag = node.tag_name();
        let namespace = tag.namespace().filter(|uri| !uri.is_empty());
        if tag.name() != "d" || (namespace.is_some() && namespace != root_namespace) {
            continue;
        }
        p_node_count += 1;
        ensure!(
            p_node_count <= MAX_COMMENTS,
            "danmaku XML exceeds the {MAX_COMMENTS}-node limit"
        );

        let Some(parameters) = node.attribute("p") else {
            continue;
        };
        let Some(text) = plain_text(node) else {
            continue;
        };
        if let Some(comment) = parse_comment(parameters, text, comments.len()) {
            comments.push(comment);
            ensure!(
                comments.len() <= MAX_COMMENTS,
                "danmaku XML exceeds the {MAX_COMMENTS}-comment limit"
            );
        }
    }
    Ok(comments)
}

/// Return text nodes only; actual nested XML elements are unsupported content.
/// Escaped markup remains a normal text node and is later escaped for ASS.
fn plain_text(node: Node<'_, '_>) -> Option<String> {
    if node.children().any(|child| child.is_element()) {
        return None;
    }
    let mut text = String::new();
    for child in node.children().filter(|child| child.is_text()) {
        if let Some(value) = child.text() {
            text.push_str(value);
        }
    }
    Some(text)
}

fn parse_comment(parameters: &str, text: String, original_index: usize) -> Option<Comment> {
    let mut parts = parameters.split(',');
    let start_seconds = parts.next()?.parse::<f64>().ok()?;
    if !start_seconds.is_finite() {
        return None;
    }
    let mode = parts.next()?.parse::<u8>().ok()?;
    if !matches!(mode, 1..=6) {
        return None;
    }

    let raw_font_size = parts
        .next()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(DEFAULT_FONT_SIZE);
    let font_size = if raw_font_size.is_finite() {
        raw_font_size.clamp(12.0, 96.0)
    } else {
        DEFAULT_FONT_SIZE
    };
    let color = parts
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0xFF_FF_FF)
        & 0xFF_FF_FF;
    let text = normalize_text(&text);
    if text.is_empty() || text.chars().all(char::is_whitespace) || ass_escape(&text).is_empty() {
        return None;
    }

    Some(Comment {
        original_index,
        start_seconds: start_seconds.max(0.0),
        mode,
        font_size,
        color,
        text,
    })
}

fn normalize_text(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn group_comments<'a>(comments: &'a [Comment], options: &RenderOptions) -> Vec<Event<'a>> {
    let mut events: Vec<Event<'a>> = Vec::new();
    let mut latest_group_by_key: HashMap<(&'a str, Direction), usize> = HashMap::new();

    for comment in comments {
        let direction = match comment.mode {
            1..=3 => Some(Direction::RightToLeft),
            6 => Some(Direction::LeftToRight),
            4 | 5 => None,
            _ => continue,
        };
        let group_index = if let Some(direction) = direction.filter(|_| options.merge_window_ms > 0)
        {
            latest_group_by_key
                .get(&(comment.text.as_str(), direction))
                .copied()
                .filter(|&index| {
                    within_merge_window(
                        events[index].anchor.start_seconds,
                        comment.start_seconds,
                        options.merge_window_ms,
                    )
                })
        } else {
            None
        };

        if let Some(index) = group_index {
            events[index].count += 1;
            continue;
        }

        let index = events.len();
        if let Some(direction) = direction.filter(|_| options.merge_window_ms > 0) {
            latest_group_by_key.insert((comment.text.as_str(), direction), index);
        }
        events.push(make_event(comment));
    }
    events
}

fn make_event(comment: &Comment) -> Event<'_> {
    Event {
        anchor: comment,
        count: 1,
        width: 0.0,
        height: 0.0,
        body_font_size: 1.0,
        counter_font_size: 1.0,
    }
}

fn size_event_v1(event: &mut Event<'_>, options: &RenderOptions) {
    event.body_font_size =
        (event.anchor.font_size * options.font_scale * count_growth(event.count))
            .round()
            .max(1.0);
    event.counter_font_size = (event.body_font_size * 0.6).round().max(1.0);
    event.width = estimated_text_width(&event.anchor.text, event.body_font_size);
    if event.count > 1 {
        let suffix = format!("({})", event.count);
        event.width +=
            estimated_text_width(&suffix, event.counter_font_size) + event.body_font_size * 0.3;
    }
    let lines = event.anchor.text.matches('\n').count() + 1;
    event.height = event.body_font_size * 1.25 * lines as f64 + 4.0;
}

fn within_merge_window(anchor_seconds: f64, comment_seconds: f64, merge_window_ms: u64) -> bool {
    let delta_ms = ((comment_seconds - anchor_seconds) * 1_000.0).max(0.0);
    // XML timestamps are decimal floats. A sub-microsecond tolerance preserves
    // inclusive millisecond boundaries despite binary floating-point rounding.
    delta_ms <= merge_window_ms as f64 + 0.000_001
}

fn count_growth(count: usize) -> f64 {
    if count < 10 {
        return 1.0;
    }
    let progress = ((count as f64 / 9.0).ln() / (100.0_f64 / 9.0).ln()).min(1.0);
    1.0 + 0.3 * progress
}

fn estimated_text_width(text: &str, font_size: f64) -> f64 {
    let em_width = text.chars().map(character_width_em).sum::<f64>();
    (em_width * font_size).max(font_size * 2.0)
}

fn character_width_em(character: char) -> f64 {
    let code = character as u32;
    if character.is_control() || is_combining_mark(code) {
        0.0
    } else if is_wide_character(code) {
        1.0
    } else if character.is_ascii_whitespace() {
        0.32
    } else if character.is_ascii_punctuation() {
        0.42
    } else if character.is_ascii_digit() {
        0.56
    } else if character.is_ascii_uppercase() {
        0.64
    } else if character.is_ascii_lowercase() {
        0.54
    } else {
        0.72
    }
}

fn is_wide_character(code: u32) -> bool {
    matches!(
        code,
        0x1100..=0x11FF
            | 0x2E80..=0xA4CF
            | 0xAC00..=0xD7AF
            | 0xF900..=0xFAFF
            | 0xFE10..=0xFE6F
            | 0xFF01..=0xFF60
            | 0xFFE0..=0xFFE6
            | 0x1F000..=0x1FAFF
            | 0x20000..=0x3FFFD
    )
}

fn is_combining_mark(code: u32) -> bool {
    matches!(
        code,
        0x0300..=0x036F
            | 0x1AB0..=0x1AFF
            | 0x1DC0..=0x1DFF
            | 0x20D0..=0x20FF
            | 0xFE20..=0xFE2F
    )
}

fn render_events_v1(events: &[Event<'_>], options: &RenderOptions) -> Result<String> {
    let region_height = (PLAY_RES_Y as f64 * options.display_area).floor().max(1.0);
    let margin = TOP_MARGIN.min(region_height / 8.0).floor();
    let max_height = events.iter().map(|event| event.height).fold(1.0, f64::max);
    let available_height = (region_height - 2.0 * margin).max(1.0);
    let fitting_lanes = (available_height / max_height).floor().max(1.0) as usize;
    let scroll_lanes = fitting_lanes.min(SCROLL_LANES);
    let fixed_lanes = fitting_lanes.min(FIXED_LANES);
    let alpha = ((1.0 - options.opacity) * 255.0).round() as u8;
    let mut output = String::new();
    write_header(&mut output, options, alpha)?;

    let mut scroll_index = 0usize;
    let mut fixed_top_index = 0usize;
    let mut fixed_bottom_index = 0usize;
    for event in events {
        let (alignment, movement) = match event.anchor.mode {
            4 => {
                let lane = fixed_bottom_index % fixed_lanes;
                fixed_bottom_index += 1;
                let y = (region_height - margin - lane as f64 * max_height).max(margin);
                ("\\an2", format!("\\pos({},{y:.0})", PLAY_RES_X / 2))
            }
            5 => {
                let lane = fixed_top_index % fixed_lanes;
                fixed_top_index += 1;
                let y = margin + lane as f64 * max_height;
                ("\\an8", format!("\\pos({},{y:.0})", PLAY_RES_X / 2))
            }
            6 => {
                let lane = scroll_index % scroll_lanes;
                scroll_index += 1;
                let y = margin + lane as f64 * max_height;
                let width = event.width;
                (
                    "\\an7",
                    format!(
                        "\\move({:.0},{y:.0},{:.0},{y:.0})",
                        -width,
                        f64::from(PLAY_RES_X) + width
                    ),
                )
            }
            _ => {
                let lane = scroll_index % scroll_lanes;
                scroll_index += 1;
                let y = margin + lane as f64 * max_height;
                let width = event.width;
                (
                    "\\an7",
                    format!(
                        "\\move({:.0},{y:.0},{:.0},{y:.0})",
                        f64::from(PLAY_RES_X) + width,
                        -width
                    ),
                )
            }
        };

        write_dialogue(
            &mut output,
            event,
            (alignment, &movement),
            alpha,
            region_height,
        )?;
    }
    Ok(output)
}

fn render_events_v2(events: &[Event<'_>], options: &RenderOptions) -> Result<String> {
    let placements = layout::place_events(events, options);
    render_events_with_placements(events, options, &placements)
}

fn render_events_v3(events: &[Event<'_>], options: &RenderOptions) -> Result<String> {
    let placements = layout::place_events_v3(events, options);
    render_events_with_placements(events, options, &placements)
}

fn render_events_with_placements(
    events: &[Event<'_>],
    options: &RenderOptions,
    placements: &[layout::Placement],
) -> Result<String> {
    let region_height = (PLAY_RES_Y as f64 * options.display_area).floor().max(1.0);
    let alpha = ((1.0 - options.opacity) * 255.0).round() as u8;
    let mut output = String::new();
    write_header(&mut output, options, alpha)?;

    for (event, placement) in events.iter().zip(placements) {
        let y = f64::from(placement.anchor_y);
        let (alignment, movement) = match event.anchor.mode {
            4 => ("\\an2", format!("\\pos({},{y:.0})", PLAY_RES_X / 2)),
            5 => ("\\an8", format!("\\pos({},{y:.0})", PLAY_RES_X / 2)),
            6 => (
                "\\an7",
                format!(
                    "\\move({:.0},{y:.0},{:.0},{y:.0})",
                    -event.width,
                    f64::from(PLAY_RES_X) + event.width
                ),
            ),
            _ => (
                "\\an7",
                format!(
                    "\\move({:.0},{y:.0},{:.0},{y:.0})",
                    f64::from(PLAY_RES_X) + event.width,
                    -event.width
                ),
            ),
        };

        write_dialogue(
            &mut output,
            event,
            (alignment, &movement),
            alpha,
            region_height,
        )?;
    }
    Ok(output)
}

fn write_dialogue(
    output: &mut String,
    event: &Event<'_>,
    (alignment, movement): (&str, &str),
    alpha: u8,
    region_height: f64,
) -> Result<()> {
    let (start_cs, end_cs) = event_times(event);
    let start = ass_timestamp(start_cs);
    let end = ass_timestamp(end_cs);
    let color = ass_color(event.anchor.color);
    let body_font = event.body_font_size;
    let counter_font = event.counter_font_size;
    write!(
        output,
        "Dialogue: 0,{start},{end},Danmaku,,0,0,0,,{{{alignment}{movement}\\fs{body_font:.0}\\c{color}\\alpha&H{alpha:02X}&\\clip(0,0,{PLAY_RES_X},{region_height:.0})}}"
    )?;
    escape_ass_into(output, &event.anchor.text);
    if event.count > 1 {
        write!(output, "{{\\fs{counter_font:.0}}} ({})", event.count)?;
    }
    output.push('\n');
    ensure!(
        output.len() <= MAX_OUTPUT_BYTES,
        "rendered ASS exceeds the {MAX_OUTPUT_BYTES}-byte limit"
    );
    Ok(())
}

fn write_header(output: &mut String, options: &RenderOptions, alpha: u8) -> Result<()> {
    output.push_str("[Script Info]\n");
    output.push_str("ScriptType: v4.00+\n");
    output.push_str("WrapStyle: 2\n");
    output.push_str("ScaledBorderAndShadow: yes\n");
    writeln!(output, "PlayResX: {PLAY_RES_X}")?;
    writeln!(output, "PlayResY: {PLAY_RES_Y}\n")?;
    output.push_str("[V4+ Styles]\n");
    output.push_str("Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n");
    let style_font_size = (DEFAULT_FONT_SIZE * options.font_scale).round().max(1.0);
    writeln!(
        output,
        "Style: Danmaku,Arial,{style_font_size:.0},&H{alpha:02X}FFFFFF,&H{alpha:02X}0000FF,&H{alpha:02X}000000,&H{alpha:02X}000000,0,0,0,0,100,100,0,0,1,2,0,7,20,20,20,1\n"
    )?;
    output.push_str("[Events]\n");
    output.push_str(
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
    );
    ensure!(
        output.len() <= MAX_OUTPUT_BYTES,
        "rendered ASS exceeds the {MAX_OUTPUT_BYTES}-byte limit"
    );
    Ok(())
}

fn event_times(event: &Event<'_>) -> (u64, u64) {
    let duration = if matches!(event.anchor.mode, 4 | 5) {
        FIXED_DURATION_SECONDS
    } else {
        SCROLL_DURATION_SECONDS
    };
    (
        ass_centiseconds(event.anchor.start_seconds),
        ass_centiseconds(event.anchor.start_seconds + duration),
    )
}

fn ass_centiseconds(seconds: f64) -> u64 {
    let safe_seconds = if seconds.is_finite() {
        seconds.clamp(0.0, 3_599_999.0)
    } else {
        0.0
    };
    (Duration::from_secs_f64(safe_seconds + 0.005).as_millis() / 10) as u64
}

fn ass_timestamp(centiseconds: u64) -> String {
    let total_seconds = centiseconds / 100;
    let centisecond = centiseconds % 100;
    let second = total_seconds % 60;
    let minute = (total_seconds / 60) % 60;
    let hour = total_seconds / 3600;
    format!("{hour}:{minute:02}:{second:02}.{centisecond:02}")
}

fn ass_color(color: u32) -> String {
    let red = (color >> 16) & 0xFF;
    let green = (color >> 8) & 0xFF;
    let blue = color & 0xFF;
    format!("&H{blue:02X}{green:02X}{red:02X}&")
}

fn ass_escape(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    escape_ass_into(&mut output, text);
    output
}

fn escape_ass_into(output: &mut String, text: &str) {
    for character in text.chars() {
        match character {
            '\n' => output.push_str("\\N"),
            '\\' => output.push('＼'),
            '{' => output.push('｛'),
            '}' => output.push('｝'),
            character if character.is_control() => {}
            character => output.push(character),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_XML_BYTES, RenderOptions, estimated_text_width, group_comments, layout, parse_comments,
        render_ass, render_ass_prefix, render_ass_prefix_v1, render_ass_prefix_v2,
    };

    fn xml(comments: &[(&str, &str)]) -> String {
        let body = comments
            .iter()
            .map(|(parameters, text)| format!("<d p=\"{parameters}\">{text}</d>"))
            .collect::<String>();
        format!("<i>{body}</i>")
    }

    fn dialogues(ass: &str) -> Vec<&str> {
        ass.lines()
            .filter(|line| line.starts_with("Dialogue: "))
            .collect()
    }

    fn dialogue_y(line: &str) -> i32 {
        let marker = if line.contains("\\move(") {
            "\\move("
        } else {
            "\\pos("
        };
        line.split_once(marker)
            .unwrap()
            .1
            .split_once(',')
            .unwrap()
            .1
            .split([',', ')'])
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }

    #[test]
    fn sorts_by_timestamp_and_preserves_xml_order_for_ties() {
        let source = xml(&[
            ("2,1,25,16711680", "later-first"),
            ("1,1,25,65280", "tie-first"),
            ("1,3,25,255", "tie-second"),
        ]);
        let rendered = render_ass(&source, &RenderOptions::default()).unwrap();
        let lines = dialogues(&rendered.ass);
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("0:00:01.00,0:00:09.00"));
        assert!(lines[0].ends_with("tie-first"));
        assert!(lines[1].ends_with("tie-second"));
        assert!(lines[2].ends_with("later-first"));
    }

    #[test]
    fn groups_same_text_with_interleaving_and_anchored_inclusive_boundary() {
        let source = xml(&[
            ("1,1,25,16711680", "repeat"),
            ("1.4,1,25,65280", "other"),
            ("2.5,2,30,255", "repeat"),
            ("2.5001,1,25,255", "repeat"),
        ]);
        let rendered = render_ass(&source, &RenderOptions::default()).unwrap();
        let lines = dialogues(&rendered.ass);
        assert_eq!(lines.len(), 3);
        let grouped = lines
            .iter()
            .find(|line| line.contains("0:00:01.00"))
            .unwrap();
        assert!(grouped.ends_with("repeat{\\fs15} (2)"));
        assert!(grouped.contains("\\c&H0000FF&"));
        assert!(lines.iter().any(|line| line.ends_with("repeat")));
    }

    #[test]
    fn zero_window_disables_merging_and_directions_remain_distinct() {
        let source = xml(&[
            ("1,1,25,16777215", "same"),
            ("1,3,25,16777215", "same"),
            ("1,6,25,16777215", "same"),
            ("1,6,25,16777215", "same"),
            ("1,4,25,16777215", "fixed"),
            ("1,4,25,16777215", "fixed"),
            ("1,5,25,16777215", "fixed"),
            ("1,7,25,16777215", "unsupported"),
        ]);
        let rendered = render_ass(
            &source,
            &RenderOptions {
                merge_window_ms: 0,
                ..RenderOptions::default()
            },
        )
        .unwrap();
        let lines = dialogues(&rendered.ass);
        assert_eq!(rendered.source_comments, 7);
        assert_eq!(lines.len(), 7);
        assert!(lines.iter().filter(|line| line.contains("\\move(")).count() == 4);
        assert!(
            lines
                .iter()
                .filter(|line| line.contains("\\an2\\pos("))
                .count()
                == 2
        );
        assert!(
            lines
                .iter()
                .filter(|line| line.contains("\\an8\\pos("))
                .count()
                == 1
        );
    }

    #[test]
    fn count_suffix_is_inline_smaller_and_body_growth_is_bounded() {
        let one = xml(&[("1,1,42,16777215", "wide文字")]);
        let ten = xml(&[("1,1,42,16777215", "wide文字"); 10]);
        let many = xml(&[("1,1,42,16777215", "wide文字"); 300]);
        let one_line =
            dialogues(&render_ass(&one, &RenderOptions::default()).unwrap().ass)[0].to_owned();
        let ten_line =
            dialogues(&render_ass(&ten, &RenderOptions::default()).unwrap().ass)[0].to_owned();
        let many_line =
            dialogues(&render_ass(&many, &RenderOptions::default()).unwrap().ass)[0].to_owned();
        let font = |line: &str| {
            let start = line.find("\\fs").unwrap() + 3;
            line[start..]
                .split(|character: char| !character.is_ascii_digit())
                .next()
                .unwrap()
                .parse::<u32>()
                .unwrap()
        };
        assert!(!one_line.contains(" ("));
        assert!(ten_line.contains("wide文字{\\fs26} (10)"));
        assert!(font(&ten_line) > font(&one_line));
        assert!(font(&many_line) <= (42.0_f64 * 1.3).ceil() as u32);
        assert!(many_line.contains("{\\fs33} (300)"));
        assert!(many_line.contains("\\move("));
    }

    #[test]
    fn multiline_outlier_only_occupies_its_rows_while_visible() {
        let source = xml(&[
            (
                "1,1,96,16777215",
                "first line\nsecond line\nthird line\nfourth line",
            ),
            ("20,1,25,16777215", "shorta"),
            ("20,1,25,16777215", "shortb"),
            ("20,1,25,16777215", "shortc"),
        ]);
        let options = RenderOptions {
            font_scale: 4.0,
            ..RenderOptions::default()
        };
        let rendered = render_ass(&source, &options).unwrap();
        let v1 = render_ass_prefix_v1(&source, &options, 4).unwrap();
        let v2_lines = dialogues(&rendered.ass);
        let v1_lines = dialogues(&v1.ass);
        let v2_short_y = v2_lines[1..]
            .iter()
            .map(|line| dialogue_y(line))
            .collect::<Vec<_>>();
        let v1_short_y = v1_lines[1..]
            .iter()
            .map(|line| dialogue_y(line))
            .collect::<Vec<_>>();

        assert!(v2_short_y.windows(2).all(|pair| pair[0] != pair[1]));
        assert!(v2_short_y.iter().all(|&y| y < 500));
        assert!(v1_short_y.iter().all(|&y| y == 40));
    }

    #[test]
    fn multiline_group_width_uses_longest_line_and_final_suffix() {
        let source = xml(&[
            ("1,1,42,16777215", "MMMM\nx"),
            ("1.2,1,42,65280", "MMMM\nx"),
        ]);
        let options = RenderOptions::default();
        let comments = parse_comments(&source).unwrap();
        let mut events = group_comments(&comments, &options);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].count, 2);
        layout::size_event_v2(&mut events[0], &options);

        let expected = estimated_text_width("x", events[0].body_font_size)
            + estimated_text_width("(2)", events[0].counter_font_size)
            + events[0].body_font_size * 0.3;
        assert_eq!(events[0].width, expected.round_ties_even());
        assert!(events[0].width > estimated_text_width("MMMM", events[0].body_font_size));
    }

    #[test]
    fn allocator_separates_catch_up_opposing_and_fixed_events() {
        let wide_text = "漢".repeat(72);
        let catchup = xml(&[("0,1,42,16777215", "a"), ("2,1,42,16777215", &wide_text)]);
        let catchup_render = render_ass(&catchup, &RenderOptions::default()).unwrap();
        let catchup_lines = dialogues(&catchup_render.ass);
        assert_ne!(dialogue_y(catchup_lines[0]), dialogue_y(catchup_lines[1]));

        let mixed = xml(&[
            ("1,1,42,16777215", "alpha"),
            ("1,6,42,16777215", "bravo"),
            ("1,5,42,16777215", "charlie"),
            ("1,4,42,16777215", "delta"),
        ]);
        let mixed_render = render_ass(&mixed, &RenderOptions::default()).unwrap();
        let lines = dialogues(&mixed_render.ass);
        let anchors = lines
            .iter()
            .map(|line| dialogue_y(line))
            .collect::<Vec<_>>();
        let unique = anchors
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), anchors.len());
        assert!(lines[3].contains("\\an2\\pos(960,1040)"));
    }

    #[test]
    fn later_comment_changes_the_earlier_placement_through_the_renderer() {
        let tall = "long\n".repeat(10) + "long";
        let comments = [
            ("0,5,12,16777215", "x"),
            ("0.1,5,25,16777215", "xyz"),
            ("0.2,4,71,16777215", &tall),
        ];
        let options = RenderOptions::default();
        let source = xml(&comments);
        let older = render_ass_prefix_v2(&source, &options, comments.len()).unwrap();
        let without_future = render_ass(&xml(&comments[..2]), &options).unwrap();
        let with_future = render_ass(&source, &options).unwrap();
        let old_lines = dialogues(&older.ass);
        let new_lines = dialogues(&with_future.ass);

        assert_eq!(dialogue_y(dialogues(&without_future.ass)[1]), 59);
        assert_eq!(dialogue_y(old_lines[1]), 59);
        assert_eq!(dialogue_y(new_lines[1]), 40);
        assert_eq!(old_lines.len(), new_lines.len());
        for (old, new) in old_lines.iter().zip(new_lines) {
            let (old_start, old_end) = old.split_once("\\pos(").unwrap();
            let (new_start, new_end) = new.split_once("\\pos(").unwrap();
            assert_eq!(old_start, new_start);
            assert_eq!(
                old_end.split_once(')').unwrap().1,
                new_end.split_once(')').unwrap().1
            );
        }
    }

    #[test]
    fn v1_prefix_retains_a_byte_exact_legacy_fixture() {
        let source = xml(&[("1,1,25,16777215", "legacy")]);
        let rendered = render_ass_prefix_v1(&source, &RenderOptions::default(), 1).unwrap();
        let expected = concat!(
            "[Script Info]\n",
            "ScriptType: v4.00+\n",
            "WrapStyle: 2\n",
            "ScaledBorderAndShadow: yes\n",
            "PlayResX: 1920\n",
            "PlayResY: 1080\n\n",
            "[V4+ Styles]\n",
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n",
            "Style: Danmaku,Arial,42,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,0,7,20,20,20,1\n\n",
            "[Events]\n",
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
            "Dialogue: 0,0:00:01.00,0:00:09.00,Danmaku,,0,0,0,,{\\an7\\move(2001,40,-81,40)\\fs25\\c&HFFFFFF&\\alpha&H00&\\clip(0,0,1920,1080)}legacy\n",
        );
        assert_eq!(rendered.ass, expected);
    }

    #[test]
    fn v2_prefix_retains_a_byte_exact_greedy_fixture() {
        let source = xml(&[("1,1,25,16777215", "legacy")]);
        let rendered = render_ass_prefix_v2(&source, &RenderOptions::default(), 1).unwrap();
        let expected = concat!(
            "[Script Info]\n",
            "ScriptType: v4.00+\n",
            "WrapStyle: 2\n",
            "ScaledBorderAndShadow: yes\n",
            "PlayResX: 1920\n",
            "PlayResY: 1080\n\n",
            "[V4+ Styles]\n",
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n",
            "Style: Danmaku,Arial,42,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,0,7,20,20,20,1\n\n",
            "[Events]\n",
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
            "Dialogue: 0,0:00:01.00,0:00:09.00,Danmaku,,0,0,0,,{\\an7\\move(2001,40,-81,40)\\fs25\\c&HFFFFFF&\\alpha&H00&\\clip(0,0,1920,1080)}legacy\n",
        );
        assert_eq!(rendered.ass, expected);
    }

    #[test]
    fn prefix_is_selected_before_sorting_and_matches_its_original_render() {
        let old = xml(&[
            ("10,1,25,16711680", "first source"),
            ("12,5,30,65280", "second source"),
        ]);
        let appended = xml(&[
            ("10,1,25,16711680", "first source"),
            ("12,5,30,65280", "second source"),
            ("1,1,96,255", "earlier appended wide 漢字"),
        ]);
        let old_render = render_ass(&old, &RenderOptions::default()).unwrap();
        let prefix = render_ass_prefix(&appended, &RenderOptions::default(), 2).unwrap();
        assert_eq!(old_render, prefix);
        assert!(
            render_ass_prefix(&appended, &RenderOptions::default(), 3)
                .unwrap()
                .ass
                .contains("earlier appended wide 漢字")
        );
        assert!(render_ass_prefix(&appended, &RenderOptions::default(), 4).is_err());
    }

    #[test]
    fn entities_are_decoded_once_and_untrusted_ass_controls_are_escaped() {
        let source =
            "<i><d p=\"1,1,25,16777215\">&amp;lt; \\\\N {\\pos(1,2)} &lt;script&gt;</d></i>";
        let rendered = render_ass(source, &RenderOptions::default()).unwrap();
        let line = dialogues(&rendered.ass)[0];
        assert!(line.contains("&lt; ＼＼N ｛＼pos(1,2)｝ <script>"));
        assert!(!line.contains("{\\pos(1,2)}"));
        assert!(
            render_ass(
                "<i><d p='1,1,25,16777215'>bad</i>",
                &RenderOptions::default()
            )
            .is_err()
        );
    }

    #[test]
    fn only_direct_d_nodes_in_root_namespace_or_unqualified_are_considered() {
        let source = r#"<i xmlns="urn:root" xmlns:x="urn:other">
            <d p="1,1,25,16777215">root namespace</d>
            <x:d p="1,1,25,16777215">other namespace</x:d>
            <section><d p="1,1,25,16777215">nested</d></section>
            <plain xmlns=""><d p="2,1,25,16777215">unqualified</d></plain>
            <d xmlns="" p="3,1,25,16777215">reset namespace</d>
        </i>"#;
        let rendered = render_ass(source, &RenderOptions::default()).unwrap();
        let lines = dialogues(&rendered.ass);
        assert_eq!(rendered.source_comments, 2);
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with("root namespace"));
        assert!(lines[1].ends_with("reset namespace"));
    }

    #[test]
    fn options_and_input_limits_are_validated() {
        for options in [
            RenderOptions {
                font_scale: 0.0,
                ..RenderOptions::default()
            },
            RenderOptions {
                font_scale: f64::NAN,
                ..RenderOptions::default()
            },
            RenderOptions {
                opacity: 1.1,
                ..RenderOptions::default()
            },
            RenderOptions {
                display_area: 0.0,
                ..RenderOptions::default()
            },
            RenderOptions {
                merge_window_ms: 10_001,
                ..RenderOptions::default()
            },
        ] {
            assert!(render_ass("<i/>", &options).is_err());
        }
        let oversized = " ".repeat(MAX_XML_BYTES + 1);
        assert!(render_ass(&oversized, &RenderOptions::default()).is_err());
    }
}
