use std::fmt::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail, ensure};

use crate::config::BilibiliDanmakuConfig;
use crate::safe_fs::RootedFs;

const WIDTH: f64 = 1920.0;
const HEIGHT: f64 = 1080.0;
const READ_LIMIT: usize = 64 * 1024 * 1024;
const MARKER: &str = "; TelegramVideoDownloader Danmaku v1: ";
static TEMP_ID: AtomicU64 = AtomicU64::new(0);

/// Style only the ASS files reported by the pinned bbdown-core download, before publication.
pub(crate) fn style_file(
    root: &RootedFs,
    path: &Path,
    settings: &BilibiliDanmakuConfig,
) -> Result<()> {
    let entry = root.bind_entry(path, false)?;
    let identity = root
        .bound_entry_identity(&entry)?
        .context("downloaded ASS sidecar is missing")?;
    let source = root.open_bound_file_if_identity(&entry, identity)?;
    let bytes = source.read_limited(READ_LIMIT)?;
    let text = std::str::from_utf8(&bytes).context("downloaded ASS is not UTF-8")?;
    let Some(styled) = restyle_ass(text, settings)? else {
        return Ok(());
    };
    let temp = path.with_file_name(format!(
        ".tvd-danmaku-{}-{}.tmp",
        std::process::id(),
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    // The completed download has no concurrent writer. Reuse the filesystem layer's object
    // identity gate and atomic exchange; directory child-entry churn is not a file replacement.
    root.replace_bound_file_atomically_if_identity(
        &entry,
        identity,
        &temp,
        styled.as_bytes(),
        0o644,
    )?;
    Ok(())
}

struct Event<'a> {
    fields: Vec<&'a str>,
    text: &'a str,
    color: &'a str,
    font_size: f64,
    motion: Motion,
}

enum Motion {
    Fixed,
    Scroll { width: f64, reverse: bool },
}

/// This is an adapter for bbdown-core 0a94b071's generated ASS, not a general ASS editor.
/// Reject an unfamiliar layout instead of silently publishing partially styled comments.
fn restyle_ass(input: &str, settings: &BilibiliDanmakuConfig) -> Result<Option<String>> {
    settings.validate()?;
    let marker = format!(
        "{MARKER}font_scale={};opacity={};display_area={}",
        settings.font_scale, settings.opacity, settings.display_area
    );
    if let Some(existing) = input.lines().find(|line| line.starts_with(MARKER)) {
        ensure!(
            existing == marker,
            "ASS sidecar already has different danmaku settings; regenerate the sidecar"
        );
        return Ok(None);
    }
    ensure!(
        input.lines().any(|line| line == "PlayResX: 1920")
            && input.lines().any(|line| line == "PlayResY: 1080"),
        "unsupported bbdown-core ASS resolution"
    );
    let events = input
        .lines()
        .filter_map(|line| line.strip_prefix("Dialogue: "))
        .map(parse_event)
        .collect::<Result<Vec<_>>>()?;
    let max_height = events
        .iter()
        .map(|event| {
            let lines = event.text.matches("\\N").count() + 1;
            event.font_size * settings.font_scale * 1.25 * lines as f64 + 4.0
        })
        .fold(1.0, f64::max)
        .ceil();
    let region_height = (HEIGHT * settings.display_area).floor().max(1.0);
    let margin = 40.0_f64.min(region_height / 8.0).floor();
    let lanes = ((region_height - 2.0 * margin) / max_height)
        .floor()
        .max(1.0) as usize;
    let alpha = ((1.0 - settings.opacity) * 255.0).round() as u8;
    let mut output = String::with_capacity(input.len());
    let mut event_index = 0;
    let mut style_seen = false;
    for line in input.lines() {
        if line == "[Script Info]" {
            writeln!(output, "{line}\n{marker}")?;
        } else if let Some(style) = line.strip_prefix("Style: ") {
            let mut fields = style.split(',').map(str::to_string).collect::<Vec<_>>();
            ensure!(
                fields.len() == 23 && fields[0] == "Danmaku",
                "unsupported bbdown-core ASS style"
            );
            let font: f64 = fields[2].parse().context("invalid ASS style font size")?;
            ensure!(
                font.is_finite() && font > 0.0,
                "invalid ASS style font size"
            );
            fields[2] = format!("{:.0}", font * settings.font_scale);
            for color in &mut fields[3..7] {
                ensure!(
                    color.len() == 10
                        && color.starts_with("&H")
                        && color[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "invalid ASS style color"
                );
                *color = format!("&H{alpha:02X}{}", &color[4..]);
            }
            writeln!(output, "Style: {}", fields.join(","))?;
            style_seen = true;
        } else if line.starts_with("Dialogue: ") {
            let event = &events[event_index];
            let y = margin + (event_index % lanes) as f64 * max_height;
            let font = (event.font_size * settings.font_scale).round().max(1.0);
            let position = match event.motion {
                Motion::Fixed => format!("\\an8\\pos(960,{y:.0})"),
                Motion::Scroll { width, reverse } => {
                    let width = width * settings.font_scale;
                    let (start, end) = if reverse {
                        (-width, WIDTH + width)
                    } else {
                        (WIDTH + width, -width)
                    };
                    format!("\\an7\\move({start:.0},{y:.0},{end:.0},{y:.0})")
                }
            };
            writeln!(
                output,
                "Dialogue: {},{{{position}\\fs{font:.0}\\c{}\\alpha&H{alpha:02X}&\\clip(0,0,1920,{region_height:.0})}}{}",
                event.fields[..9].join(","),
                event.color,
                event.text
            )?;
            event_index += 1;
        } else {
            writeln!(output, "{line}")?;
        }
        ensure!(
            output.len() <= READ_LIMIT * 2,
            "styled ASS exceeds its size limit"
        );
    }
    ensure!(style_seen, "bbdown-core ASS style is missing");
    Ok(Some(output))
}

fn parse_event(line: &str) -> Result<Event<'_>> {
    let fields = line.splitn(10, ',').collect::<Vec<_>>();
    ensure!(
        fields.len() == 10 && fields[3] == "Danmaku",
        "unsupported ASS event"
    );
    let (tags, text) = fields[9]
        .strip_prefix('{')
        .and_then(|text| text.split_once('}'))
        .context("ASS event override block is missing")?;
    let (position, style) = tags
        .split_once("\\fs")
        .context("ASS event font is missing")?;
    let (font, color) = style
        .split_once("\\c")
        .context("ASS event color is missing")?;
    let font_size: f64 = font.parse().context("invalid ASS event font size")?;
    ensure!(
        font_size.is_finite() && (12.0..=96.0).contains(&font_size),
        "unsupported ASS event font size"
    );
    ensure!(
        color.len() == 9
            && color.starts_with("&H")
            && color.ends_with('&')
            && color[2..8].bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid ASS event color"
    );
    let motion = if let Some(coordinates) = position
        .strip_prefix("\\move(")
        .and_then(|text| text.strip_suffix(')'))
    {
        let coordinates = coordinates
            .split(',')
            .map(str::parse::<f64>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            coordinates.len() == 4 && coordinates.iter().all(|value| value.is_finite()),
            "unsupported ASS movement"
        );
        let start = coordinates[0];
        let end = coordinates[2];
        let width = -start.min(end);
        ensure!(
            width > 0.0 && (start.max(end) - WIDTH - width).abs() < 1.0,
            "unsupported ASS movement endpoints"
        );
        Motion::Scroll {
            width,
            reverse: start < end,
        }
    } else if ["\\an2\\pos(960,", "\\an8\\pos(960,"]
        .iter()
        .any(|prefix| position.starts_with(prefix) && position.ends_with(')'))
    {
        Motion::Fixed
    } else {
        bail!("unsupported ASS event positioning");
    };
    Ok(Event {
        fields,
        text,
        color,
        font_size,
        motion,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(events: &str) -> String {
        format!(
            "[Script Info]\nScriptType: v4.00+\nPlayResX: 1920\nPlayResY: 1080\n\n[V4+ Styles]\nStyle: Danmaku,Arial,42,&H00FFFFFF,&H000000FF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,2,0,7,20,20,20,1\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n{events}"
        )
    }

    fn dialogue(tags: &str, text: &str) -> String {
        format!(
            "Dialogue: 0,0:00:01.00,0:00:09.00,Danmaku,,0,0,0,,{{{tags}\\fs25\\c&H112233&}}{text}\n"
        )
    }

    #[test]
    fn default_profile_scales_fonts_motion_and_all_alpha_channels() {
        let input = fixture(&dialogue("\\move(2020,40,-100,40)", "中文, \\{text\\}"));
        let styled = restyle_ass(&input, &BilibiliDanmakuConfig::default())
            .unwrap()
            .unwrap();
        assert!(
            styled.contains("Style: Danmaku,Arial,126,&H4DFFFFFF,&H4D0000FF,&H4D000000,&H4D000000")
        );
        assert!(styled.contains(
            "\\an7\\move(2220,40,-300,40)\\fs75\\c&H112233&\\alpha&H4D&\\clip(0,0,1920,540)"
        ));
        assert!(styled.contains("中文, \\{text\\}"));
        assert!(styled.contains("0:00:01.00,0:00:09.00"));
    }

    #[test]
    fn both_scroll_directions_and_fixed_modes_use_only_upper_lanes() {
        let tags = [
            "\\move(2020,768,-100,768)",
            "\\move(-100,820,2020,820)",
            "\\an2\\pos(960,1040)",
            "\\an8\\pos(960,404)",
        ];
        let events = tags
            .iter()
            .cycle()
            .take(12)
            .map(|tags| dialogue(tags, "text"))
            .collect::<String>();
        let styled = restyle_ass(&fixture(&events), &BilibiliDanmakuConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(styled.matches("\\clip(0,0,1920,540)").count(), 12);
        assert!(styled.contains("\\move(-300,138,2220,138)"));
        assert!(styled.contains("\\an8\\pos(960,236)"));
        assert!(!styled.contains("\\an2"));
        assert!(styled.contains("\\an8\\pos(960,334)"));
        assert!(!styled.contains(",1040)"));
    }

    #[test]
    fn restyling_is_idempotent_and_different_settings_fail_without_rescaling() {
        let settings = BilibiliDanmakuConfig::default();
        let input = fixture(&dialogue("\\move(2020,40,-100,40)", "text"));
        let styled = restyle_ass(&input, &settings).unwrap().unwrap();
        assert!(restyle_ass(&styled, &settings).unwrap().is_none());
        let changed = BilibiliDanmakuConfig {
            font_scale: 2.0,
            ..settings
        };
        assert!(
            restyle_ass(&styled, &changed)
                .unwrap_err()
                .to_string()
                .contains("regenerate")
        );
    }

    #[test]
    fn custom_profile_and_large_multiline_comments_keep_clip_boundary() {
        let settings = BilibiliDanmakuConfig {
            font_scale: 1.0,
            opacity: 1.0,
            display_area: 1.0,
            ..Default::default()
        };
        let input = fixture(
            &dialogue("\\an2\\pos(960,1040)", "first\\Nsecond").replace("\\fs25", "\\fs96"),
        );
        let styled = restyle_ass(&input, &settings).unwrap().unwrap();
        assert!(styled.contains("\\fs96\\c&H112233&\\alpha&H00&\\clip(0,0,1920,1080)"));
        let styled = restyle_ass(&input, &BilibiliDanmakuConfig::default())
            .unwrap()
            .unwrap();
        assert!(styled.contains("\\fs288"));
        assert!(styled.contains("\\pos(960,40)"));
        assert!(styled.contains("\\clip(0,0,1920,540)"));
    }

    #[test]
    fn unfamiliar_upstream_layout_is_rejected() {
        let input = fixture(&dialogue("\\pos(50,60)", "text"));
        assert!(restyle_ass(&input, &BilibiliDanmakuConfig::default()).is_err());
        assert!(
            restyle_ass(
                &input.replace("PlayResX: 1920", "PlayResX: 1280"),
                &BilibiliDanmakuConfig::default()
            )
            .is_err()
        );
    }

    #[test]
    #[ignore = "requires FFmpeg with ASS support or TVD_ASS_RENDERER producing one RGB24 frame"]
    fn libass_render_keeps_the_lower_half_black_and_applies_opacity() {
        let directory = std::env::temp_dir().join(format!(
            "tvd-danmaku-render-{}-{}",
            std::process::id(),
            TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let input = fixture(&dialogue(
            "\\an2\\pos(960,1040)",
            "FIRST\\NSECOND\\NTHIRD\\NFOURTH\\NFIFTH\\NSIXTH\\NSEVENTH\\NEIGHTH\\NNINTH\\NTENTH\\NELEVENTH\\NTWELFTH",
        )).replace("0:00:01.00", "0:00:00.00").replace("&H112233&", "&HFFFFFF&");
        let render = |settings: &BilibiliDanmakuConfig, name: &str| {
            let path = directory.join(name);
            std::fs::write(&path, restyle_ass(&input, settings).unwrap().unwrap()).unwrap();
            let frame = if let Some(renderer) = std::env::var_os("TVD_ASS_RENDERER") {
                std::process::Command::new(renderer).arg(&path).output()
            } else {
                std::process::Command::new("ffmpeg")
                    .args([
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-f",
                        "lavfi",
                        "-i",
                        "color=black:s=1920x1080:r=1:d=1",
                        "-vf",
                    ])
                    .arg(format!("ass={}", path.display()))
                    .args([
                        "-frames:v",
                        "1",
                        "-pix_fmt",
                        "rgb24",
                        "-f",
                        "rawvideo",
                        "pipe:1",
                    ])
                    .output()
            }
            .expect("an ASS renderer must be installed for this opt-in render test");
            assert!(
                frame.status.success(),
                "{}",
                String::from_utf8_lossy(&frame.stderr)
            );
            assert_eq!(frame.stdout.len(), 1920 * 1080 * 3);
            frame.stdout
        };
        let settings = BilibiliDanmakuConfig::default();
        let clipped = render(&settings, "clipped.ass");
        let half = 1920 * 540 * 3;
        assert!(clipped[..half].iter().any(|pixel| *pixel > 100));
        assert!(clipped[half..].iter().all(|pixel| *pixel == 0));
        let brightest = *clipped.iter().max().unwrap();
        assert!(
            (170..=185).contains(&brightest),
            "70% white should render near 178, got {brightest}"
        );
        let full = render(
            &BilibiliDanmakuConfig {
                opacity: 1.0,
                display_area: 1.0,
                ..settings
            },
            "full.ass",
        );
        assert!(full[half..].iter().any(|pixel| *pixel > 100));
        assert_eq!(*full.iter().max().unwrap(), 255);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
