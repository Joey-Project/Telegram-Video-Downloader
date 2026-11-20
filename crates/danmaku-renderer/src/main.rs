use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use danmaku_renderer::{MAX_XML_BYTES, RenderOptions, render_ass};

const USAGE: &str = "Usage: danmaku-renderer [OPTIONS] <input.xml|->

Convert Bilibili-style XML danmaku to ASS subtitles. Use '-' to read XML from stdin;
ASS is written to stdout.

Options:
  --font-scale VALUE       Font size multiplier, greater than 0 and at most 10 (default: 1.0)
  --opacity VALUE          Text opacity, from 0.0 to 1.0 (default: 1.0)
  --display-area VALUE     Screen-height fraction, greater than 0 and at most 1 (default: 1.0)
  --merge-window-ms VALUE  Group window from 0 to 10000 milliseconds (default: 1500, 0 disables)
  -h, --help               Show this help";

struct Args {
    input: String,
    options: RenderOptions,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("danmaku-renderer: {error:#}");
        std::process::exit(2);
    }
}

fn run() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let xml = read_bounded(&args.input)?;
    let xml = std::str::from_utf8(&xml).context("input XML is not valid UTF-8")?;
    let rendered = render_ass(xml, &args.options).context("failed to render XML danmaku")?;
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(rendered.ass.as_bytes())
        .context("failed to write ASS output")?;
    stdout.flush().context("failed to flush ASS output")?;
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args> {
    let mut input = None;
    let mut options = RenderOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "--font-scale" => {
                options.font_scale = parse_value("--font-scale", args.next())?;
            }
            "--opacity" => {
                options.opacity = parse_value("--opacity", args.next())?;
            }
            "--display-area" => {
                options.display_area = parse_value("--display-area", args.next())?;
            }
            "--merge-window-ms" => {
                options.merge_window_ms = parse_value("--merge-window-ms", args.next())?;
            }
            value if value.starts_with('-') && value != "-" => {
                bail!("unknown option '{value}'\n\n{USAGE}");
            }
            value => {
                if input.replace(value.to_owned()).is_some() {
                    bail!("expected exactly one XML input\n\n{USAGE}");
                }
            }
        }
    }

    let input = input.ok_or_else(|| anyhow::anyhow!("missing XML input\n\n{USAGE}"))?;
    Ok(Args { input, options })
}

fn parse_value<T: std::str::FromStr>(name: &str, raw: Option<String>) -> Result<T> {
    raw.ok_or_else(|| anyhow::anyhow!("missing value for {name}"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid value for {name}"))
}

fn read_bounded(input: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(64 * 1024);
    let read_limit = (MAX_XML_BYTES + 1) as u64;
    if input == "-" {
        io::stdin()
            .lock()
            .take(read_limit)
            .read_to_end(&mut bytes)
            .context("failed to read XML from stdin")?;
    } else {
        let path = Path::new(input);
        let file = File::open(path).context("failed to open XML input")?;
        file.take(read_limit)
            .read_to_end(&mut bytes)
            .context("failed to read XML input")?;
    }
    if bytes.len() > MAX_XML_BYTES {
        bail!("input XML exceeds the 16 MiB limit");
    }
    Ok(bytes)
}
