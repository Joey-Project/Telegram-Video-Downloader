#![cfg(all(target_os = "macos", feature = "native"))]

use flac_alac_converter::{ConvertRequest, ConverterClient, Event};
use mp4ameta::{Data, Fourcc, FreeformIdent, Tag};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const DRIVER_TIMEOUT: Duration = Duration::from_secs(60);
const TOOL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_STDOUT: usize = 512 * 1024;
const MAX_STDERR: usize = 64 * 1024;
const AUDIT_MEAN: &str = "org.joey.flac-original";
const AUDIT_RAW_MEAN: &str = "org.joey.flac-original.raw";
const WAV_AUDIT_MEAN: &str = "org.joey.wav-original";
const WAV_AUDIT_RAW_MEAN: &str = "org.joey.wav-original.raw";
const BINARY: &str = env!("CARGO_BIN_EXE_flac-alac-converter");

struct ProcessOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct ChildGuard {
    child: Child,
    process_group: i32,
    finished: bool,
}

impl ChildGuard {
    fn terminate_and_reap(&mut self) {
        if self.finished {
            return;
        }
        unsafe {
            libc::kill(-self.process_group, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    self.finished = true;
                    return;
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                _ => break,
            }
        }
        unsafe {
            libc::kill(-self.process_group, libc::SIGKILL);
        }
        let _ = self.child.wait();
        self.finished = true;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.terminate_and_reap();
    }
}

fn capture_stream<R: Read + Send + 'static>(
    mut stream: R,
    limit: usize,
    exceeded: Arc<AtomicBool>,
) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 8192];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    if bytes.len() + count > limit {
                        exceeded.store(true, Ordering::Release);
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
            }
        }
        bytes
    })
}

fn execute(
    command: &mut Command,
    stdin: &[u8],
    timeout: Duration,
) -> Result<ProcessOutput, &'static str> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let child = command
        .spawn()
        .map_err(|_| "test subprocess could not start")?;
    let process_group = child.id() as i32;
    let mut guard = ChildGuard {
        child,
        process_group,
        finished: false,
    };
    let stdout = guard.child.stdout.take().ok_or("stdout pipe missing")?;
    let stderr = guard.child.stderr.take().ok_or("stderr pipe missing")?;
    let stdout_overflow = Arc::new(AtomicBool::new(false));
    let stderr_overflow = Arc::new(AtomicBool::new(false));
    let stdout_reader = capture_stream(stdout, MAX_STDOUT, Arc::clone(&stdout_overflow));
    let stderr_reader = capture_stream(stderr, MAX_STDERR, Arc::clone(&stderr_overflow));

    if let Some(mut pipe) = guard.child.stdin.take() {
        pipe.write_all(stdin)
            .map_err(|_| "subprocess input write failed")?;
    }

    let deadline = Instant::now() + timeout;
    let status = loop {
        if stdout_overflow.load(Ordering::Acquire) || stderr_overflow.load(Ordering::Acquire) {
            guard.terminate_and_reap();
            return Err("test subprocess exceeded its output limit");
        }
        match guard.child.try_wait() {
            Ok(Some(status)) => {
                guard.finished = true;
                break status;
            }
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => {
                guard.terminate_and_reap();
                return Err("test subprocess exceeded its deadline");
            }
            Err(_) => {
                guard.terminate_and_reap();
                return Err("test subprocess status could not be read");
            }
        }
    };
    let stdout = stdout_reader.join().map_err(|_| "stdout reader failed")?;
    let stderr = stderr_reader.join().map_err(|_| "stderr reader failed")?;
    if stdout_overflow.load(Ordering::Acquire) || stderr_overflow.load(Ordering::Acquire) {
        return Err("test subprocess exceeded its output limit");
    }
    Ok(ProcessOutput {
        status,
        stdout,
        stderr,
    })
}

fn run_tool(program: &str, args: &[String]) -> ProcessOutput {
    let mut command = Command::new(program);
    command.args(args);
    execute(&mut command, &[], TOOL_TIMEOUT).unwrap_or_else(|reason| panic!("{reason}"))
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_pcm32(pcm: &[u8], bits: u32) -> Vec<u8> {
    assert!(matches!(bits, 8 | 16 | 24 | 32));
    let width = (bits / 8) as usize;
    assert_eq!(pcm.len() % width, 0);
    let shift = 32 - bits;
    let mut canonical = Vec::with_capacity((pcm.len() / width) * 4);
    for bytes in pcm.chunks_exact(width) {
        let mut raw = 0_u32;
        for (index, byte) in bytes.iter().enumerate() {
            raw |= u32::from(*byte) << (index * 8);
        }
        let sample = ((raw << shift) as i32) >> shift;
        canonical.extend_from_slice(&sample.wrapping_shl(shift).to_le_bytes());
    }
    canonical
}

#[derive(Clone, Copy)]
struct Spec {
    bits: u32,
    rate: u32,
    channels: u32,
    frames: u32,
}

struct Fixture {
    directory: TempDir,
    input: PathBuf,
    pcm: Vec<u8>,
    pcm_sha256: String,
    original_comments: Vec<(String, String)>,
}

fn encode_signed_sample(sample: i64, bits: u32, pcm: &mut Vec<u8>) {
    let width = (bits / 8) as usize;
    let masked = (sample as u64) & ((1_u64 << bits) - 1);
    pcm.extend_from_slice(&masked.to_le_bytes()[..width]);
}

fn synthetic_pcm(spec: Spec) -> Vec<u8> {
    let min = -(1_i64 << (spec.bits - 1));
    let max = (1_i64 << (spec.bits - 1)) - 1;
    let edge = [min, max, -1, 1, 0, -257, 257, -255, 255, min + 1, max - 1];
    let mut pcm = Vec::with_capacity(
        spec.frames as usize * spec.channels as usize * (spec.bits / 8) as usize,
    );
    let mask = (1_u64 << spec.bits) - 1;
    let sign = 1_u64 << (spec.bits - 1);
    let mut state = 0xA341_316C_u32 ^ spec.bits ^ spec.rate;
    for index in 0..(spec.frames * spec.channels) as usize {
        let sample = if index < edge.len() {
            edge[index]
        } else {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            let mut raw = u64::from(state) & mask;
            if spec.bits == 32 {
                let low = ((index * 73 + 29) % 255 + 1) as u64;
                raw = (raw & !0xFF) | low;
            }
            if raw & sign != 0 {
                (raw as i64) - (1_i64 << spec.bits)
            } else {
                raw as i64
            }
        };
        encode_signed_sample(sample, spec.bits, &mut pcm);
    }
    pcm
}

fn write_wave(path: &Path, spec: Spec, pcm: &[u8]) {
    let width = spec.bits / 8;
    let block_align = spec.channels * width;
    let fmt = if spec.channels > 2 {
        assert_eq!(spec.channels, 6, "only the 5.1 test layout is generated");
        let mut fmt = Vec::with_capacity(40);
        fmt.extend_from_slice(&0xFFFE_u16.to_le_bytes());
        fmt.extend_from_slice(&(spec.channels as u16).to_le_bytes());
        fmt.extend_from_slice(&spec.rate.to_le_bytes());
        fmt.extend_from_slice(&(spec.rate * block_align).to_le_bytes());
        fmt.extend_from_slice(&(block_align as u16).to_le_bytes());
        fmt.extend_from_slice(&(spec.bits as u16).to_le_bytes());
        fmt.extend_from_slice(&22_u16.to_le_bytes());
        fmt.extend_from_slice(&(spec.bits as u16).to_le_bytes());
        fmt.extend_from_slice(&0x3F_u32.to_le_bytes());
        fmt.extend_from_slice(b"\x01\x00\x00\x00\x00\x00\x10\x00\x80\x00\x00\xaa\x00\x38\x9b\x71");
        fmt
    } else {
        let mut fmt = Vec::with_capacity(16);
        fmt.extend_from_slice(&1_u16.to_le_bytes());
        fmt.extend_from_slice(&(spec.channels as u16).to_le_bytes());
        fmt.extend_from_slice(&spec.rate.to_le_bytes());
        fmt.extend_from_slice(&(spec.rate * block_align).to_le_bytes());
        fmt.extend_from_slice(&(block_align as u16).to_le_bytes());
        fmt.extend_from_slice(&(spec.bits as u16).to_le_bytes());
        fmt
    };
    // WAVE PCM uses unsigned 8-bit samples, while Fixture::pcm stays in
    // canonical signed-sample representation for comparisons.
    let wave_pcm = if spec.bits == 8 {
        pcm.iter().map(|byte| byte ^ 0x80).collect::<Vec<_>>()
    } else {
        pcm.to_vec()
    };
    let riff_size = 4 + 8 + fmt.len() as u32 + 8 + wave_pcm.len() as u32;
    let mut bytes = Vec::with_capacity(44 + wave_pcm.len());
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&riff_size.to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&fmt);
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&(wave_pcm.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&wave_pcm);
    fs::write(path, bytes).expect("synthetic WAV should be written");
}

fn append_wave_chunk(path: &Path, id: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    let mut bytes = fs::read(path).expect("synthetic WAV should be readable");
    assert_eq!(&bytes[..4], b"RIFF");
    let length = u32::try_from(payload.len()).expect("WAVE test chunk should fit");
    let mut record = Vec::with_capacity(payload.len() + 9);
    record.extend_from_slice(id);
    record.extend_from_slice(&length.to_le_bytes());
    record.extend_from_slice(payload);
    if payload.len() & 1 != 0 {
        record.push(0);
    }
    bytes.extend_from_slice(&record);
    let riff_size = u32::try_from(bytes.len() - 8).expect("synthetic WAVE should fit RIFF");
    bytes[4..8].copy_from_slice(&riff_size.to_le_bytes());
    fs::write(path, bytes).expect("WAVE metadata chunk should be appended");
    record
}

fn info_subchunk(id: &[u8; 4], value: &str, output: &mut Vec<u8>) {
    let length = u32::try_from(value.len() + 1).expect("INFO test value should fit");
    output.extend_from_slice(id);
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    output.push(0);
    if length & 1 != 0 {
        output.push(0);
    }
}

fn tags() -> Vec<(String, String)> {
    vec![
        ("TITLE".into(), "标题・日本語の歌 🎵".into()),
        ("ARTIST".into(), "한국어 아티스트".into()),
        ("ARTIST".into(), "русский исполнитель".into()),
        ("ALBUM".into(), "Français café / cafe\u{301}".into()),
        ("GENRE".into(), "Italiano 🎼".into()),
        ("COMMENT".into(), "Commentaire e\u{301} 🎧".into()),
        (
            "DESCRIPTION".into(),
            "Mixed: 中文, 日本語, 한국어, Русский".into(),
        ),
        ("CUSTOM_FIELD".into(), "e\u{301} composed? / é".into()),
        ("CUSTOM_FIELD".into(), "emoji 👩🏽‍🎤".into()),
    ]
}

struct RawFixtureData {
    picture_png: Vec<u8>,
    picture_block_index: usize,
    invalid_comment: Vec<u8>,
    invalid_comment_name: String,
    opaque_payload: Vec<u8>,
    opaque_block_name: String,
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn png_chunk(output: &mut Vec<u8>, kind: &[u8; 4], payload: &[u8]) {
    output.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    output.extend_from_slice(kind);
    output.extend_from_slice(payload);
    let start = output.len() - payload.len() - 4;
    output.extend_from_slice(&crc32(&output[start..]).to_be_bytes());
}

fn tiny_png() -> Vec<u8> {
    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&1_u32.to_be_bytes());
    ihdr.extend_from_slice(&1_u32.to_be_bytes());
    ihdr.extend_from_slice(&[8, 6, 0, 0, 0]);
    png_chunk(&mut png, b"IHDR", &ihdr);

    let scanline = [0, 0x20, 0x80, 0xE0, 0xFF];
    let mut zlib = vec![0x78, 0x01, 0x01, 5, 0, 0xFA, 0xFF];
    zlib.extend_from_slice(&scanline);
    let mut a = 1_u32;
    let mut b = 0_u32;
    for byte in scanline {
        a = (a + u32::from(byte)) % 65_521;
        b = (b + a) % 65_521;
    }
    zlib.extend_from_slice(&((b << 16) | a).to_be_bytes());
    png_chunk(&mut png, b"IDAT", &zlib);
    png_chunk(&mut png, b"IEND", &[]);
    png
}

fn append_flac_picture(png: &[u8]) -> Vec<u8> {
    let mut picture = Vec::new();
    picture.extend_from_slice(&3_u32.to_be_bytes());
    picture.extend_from_slice(&("image/png".len() as u32).to_be_bytes());
    picture.extend_from_slice(b"image/png");
    let description = "synthetic cover 🖼".as_bytes();
    picture.extend_from_slice(&(description.len() as u32).to_be_bytes());
    picture.extend_from_slice(description);
    picture.extend_from_slice(&1_u32.to_be_bytes());
    picture.extend_from_slice(&1_u32.to_be_bytes());
    picture.extend_from_slice(&32_u32.to_be_bytes());
    picture.extend_from_slice(&0_u32.to_be_bytes());
    picture.extend_from_slice(&(png.len() as u32).to_be_bytes());
    picture.extend_from_slice(png);
    picture
}

fn read_le_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("four-byte field"),
    )
}

fn rewrite_fixture_metadata(path: &Path) -> RawFixtureData {
    let source = fs::read(path).expect("FLAC source should be readable");
    assert!(source.starts_with(b"fLaC"));
    let mut blocks = Vec::<(u8, Vec<u8>)>::new();
    let mut cursor = 4_usize;
    loop {
        assert!(cursor + 4 <= source.len());
        let header = source[cursor];
        let last = header & 0x80 != 0;
        let kind = header & 0x7F;
        let length = (usize::from(source[cursor + 1]) << 16)
            | (usize::from(source[cursor + 2]) << 8)
            | usize::from(source[cursor + 3]);
        let start = cursor + 4;
        let end = start.checked_add(length).expect("block size fits");
        assert!(end <= source.len());
        blocks.push((kind, source[start..end].to_vec()));
        cursor = end;
        if last {
            break;
        }
    }
    let audio = source[cursor..].to_vec();

    let invalid_comment = b"BINARY_VALUE=\xff\xfe".to_vec();
    let comment_index = blocks
        .iter()
        .position(|(kind, _)| *kind == 4)
        .expect("FLAC fixture has a comment block");
    let comment_payload = &blocks[comment_index].1;
    let vendor_length = read_le_u32(comment_payload, 0) as usize;
    let count_offset = 4 + vendor_length;
    let existing_count = read_le_u32(comment_payload, count_offset) as usize;
    let entries_offset = count_offset + 4;
    let mut updated_comment = Vec::with_capacity(comment_payload.len() + invalid_comment.len() + 8);
    updated_comment.extend_from_slice(&comment_payload[..count_offset]);
    updated_comment.extend_from_slice(&((existing_count as u32 + 1).to_le_bytes()));
    updated_comment.extend_from_slice(&comment_payload[entries_offset..]);
    updated_comment.extend_from_slice(&(invalid_comment.len() as u32).to_le_bytes());
    updated_comment.extend_from_slice(&invalid_comment);
    blocks[comment_index].1 = updated_comment;

    let picture_png = tiny_png();
    let picture_block_index = blocks.len();
    blocks.push((6, append_flac_picture(&picture_png)));
    let opaque_payload = b"TEST\x00opaque metadata test payload\xff".to_vec();
    let opaque_block_index = blocks.len();
    blocks.push((2, opaque_payload.clone()));

    let mut rewritten = b"fLaC".to_vec();
    for (index, (kind, payload)) in blocks.iter().enumerate() {
        assert!(payload.len() <= 0xFF_FFFF);
        rewritten.push(*kind | if index + 1 == blocks.len() { 0x80 } else { 0 });
        let length = payload.len() as u32;
        rewritten.extend_from_slice(&[(length >> 16) as u8, (length >> 8) as u8, length as u8]);
        rewritten.extend_from_slice(payload);
    }
    rewritten.extend_from_slice(&audio);
    fs::write(path, rewritten).expect("FLAC metadata fixture should be rewritten");

    let metadata = flac_alac_converter::metadata::read(path)
        .expect("invalid comments and reserved metadata should be retained");
    assert_eq!(metadata.invalid_comments.len(), 1);
    assert_eq!(metadata.invalid_comments[0].raw, invalid_comment);
    assert!(metadata.opaque_blocks.iter().any(|block| {
        block.block_index == opaque_block_index
            && block.block_type == 2
            && block.payload == opaque_payload
    }));
    RawFixtureData {
        picture_png,
        picture_block_index,
        invalid_comment,
        invalid_comment_name: format!("FLAC_RAW_COMMENT_{comment_index:06}_{existing_count:06}"),
        opaque_payload,
        opaque_block_name: format!("FLAC_RAW_BLOCK_{opaque_block_index:06}_TYPE_02"),
    }
}

fn create_fixture(spec: Spec, with_tags: bool) -> Fixture {
    let directory = tempfile::tempdir().expect("temporary fixture directory should be created");
    let wave = directory
        .path()
        .join("synthetic source 日本語 café e\u{301} 🎵.wav");
    let input = directory
        .path()
        .join("synthetic FLAC 日本語 한국어 русский café 🎵.flac");
    let pcm = synthetic_pcm(spec);
    if spec.bits == 32 {
        let sample_count = spec.frames as usize * spec.channels as usize;
        let nonzero_low_bytes = pcm
            .as_chunks::<4>()
            .0
            .iter()
            .filter(|sample| sample[0] != 0)
            .count();
        assert!(nonzero_low_bytes > sample_count - 32);
    }
    let pcm_sha256 = sha256(&canonical_pcm32(&pcm, spec.bits));
    write_wave(&wave, spec, &pcm);

    let encoded = run_tool(
        "flac",
        &[
            "--silent".into(),
            "--verify".into(),
            "--output-name".into(),
            input.to_string_lossy().into_owned(),
            wave.to_string_lossy().into_owned(),
        ],
    );
    assert!(
        encoded.status.success(),
        "FLAC fixture encoding failed: {}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    let original_comments = if with_tags {
        let remove = run_tool(
            "metaflac",
            &[
                "--remove-all-tags".into(),
                input.to_string_lossy().into_owned(),
            ],
        );
        assert!(
            remove.status.success(),
            "FLAC tag reset failed: {}",
            String::from_utf8_lossy(&remove.stderr)
        );
        let expected_comments = tags();
        let mut args = expected_comments
            .iter()
            .map(|(key, value)| format!("--set-tag={key}={value}"))
            .collect::<Vec<_>>();
        args.push(input.to_string_lossy().into_owned());
        let set = run_tool("metaflac", &args);
        assert!(
            set.status.success(),
            "FLAC tags could not be written: {}",
            String::from_utf8_lossy(&set.stderr)
        );
        let tag_export = directory.path().join("original comments.txt");
        let export = run_tool(
            "metaflac",
            &[
                format!("--export-tags-to={}", tag_export.to_string_lossy()),
                input.to_string_lossy().into_owned(),
            ],
        );
        assert!(
            export.status.success(),
            "FLAC comments could not be exported: {}",
            String::from_utf8_lossy(&export.stderr)
        );
        let original_comments: Vec<(String, String)> = fs::read_to_string(tag_export)
            .expect("FLAC comments should be UTF-8")
            .lines()
            .map(|line| {
                let (key, value) = line
                    .split_once('=')
                    .expect("FLAC comment should have a key");
                (key.to_owned(), value.to_owned())
            })
            .collect();
        assert!(original_comments.iter().any(|(key, _)| key == "TITLE"));
        assert_eq!(
            original_comments
                .iter()
                .filter(|(key, _)| key == "ARTIST")
                .count(),
            2
        );
        assert!(
            original_comments
                .iter()
                .any(|(_, value)| value.contains("e\u{301}"))
        );
        assert!(
            original_comments
                .iter()
                .any(|(_, value)| value.contains('é'))
        );
        original_comments
    } else {
        Vec::new()
    };

    Fixture {
        directory,
        input,
        pcm,
        pcm_sha256,
        original_comments,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DecodedWave {
    bits: u32,
    rate: u32,
    channels: u32,
    frames: u64,
    pcm: Vec<u8>,
}

fn read_decoded_wave(path: &Path, target_bits: u32) -> DecodedWave {
    let bytes = fs::read(path).expect("decoded WAVE should be readable");
    assert!(
        bytes.len() <= 32 * 1024 * 1024,
        "decoded WAVE exceeded test bound"
    );
    assert!(bytes.len() >= 44 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE");
    let mut fmt = None;
    let mut pcm = None;
    let mut offset = 12;
    while offset + 8 <= bytes.len() {
        let kind = &bytes[offset..offset + 4];
        let size = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let start = offset + 8;
        let end = start
            .checked_add(size)
            .expect("WAVE chunk length should fit");
        assert!(end <= bytes.len(), "WAVE chunk exceeds file");
        if kind == b"fmt " {
            fmt = Some(&bytes[start..end]);
        } else if kind == b"data" {
            pcm = Some(&bytes[start..end]);
        }
        offset = end + (size & 1);
    }
    let fmt = fmt.expect("WAVE fmt chunk should exist");
    let pcm = pcm.expect("WAVE data chunk should exist");
    assert!(fmt.len() >= 16);
    let tag = u16::from_le_bytes(fmt[0..2].try_into().unwrap());
    let channels = u16::from_le_bytes(fmt[2..4].try_into().unwrap()) as u32;
    let rate = u32::from_le_bytes(fmt[4..8].try_into().unwrap());
    let block_align = u16::from_le_bytes(fmt[12..14].try_into().unwrap()) as usize;
    let container_bits = u16::from_le_bytes(fmt[14..16].try_into().unwrap()) as u32;
    let valid_bits = if tag == 0xFFFE {
        assert!(fmt.len() >= 40);
        assert_eq!(
            &fmt[24..40],
            b"\x01\x00\x00\x00\x00\x00\x10\x00\x80\x00\x00\xaa\x00\x38\x9b\x71"
        );
        let valid = u16::from_le_bytes(fmt[18..20].try_into().unwrap()) as u32;
        if valid == 0 { container_bits } else { valid }
    } else {
        assert_eq!(tag, 1, "decoded WAVE must be integer PCM");
        container_bits
    };
    assert_eq!(valid_bits, target_bits);
    assert!(container_bits >= target_bits);
    let width = (container_bits / 8) as usize;
    assert_eq!(block_align, channels as usize * width);
    assert_eq!(pcm.len() % block_align, 0);
    let frames = (pcm.len() / block_align) as u64;
    let canonical = if container_bits == target_bits {
        pcm.to_vec()
    } else {
        let output_width = (target_bits / 8) as usize;
        let shift = container_bits - target_bits;
        let mask = (1_u64 << target_bits) - 1;
        let mut converted = Vec::with_capacity(frames as usize * channels as usize * output_width);
        for sample_bytes in pcm.chunks_exact(width) {
            let mut unsigned = 0_u64;
            for (byte_index, byte) in sample_bytes.iter().enumerate() {
                unsigned |= u64::from(*byte) << (byte_index * 8);
            }
            let sample = if unsigned & (1_u64 << (container_bits - 1)) != 0 {
                unsigned as i64 - (1_i64 << container_bits)
            } else {
                unsigned as i64
            };
            let sample = sample >> shift;
            let value = (sample as u64) & mask;
            converted.extend_from_slice(&value.to_le_bytes()[..output_width]);
        }
        converted
    };
    DecodedWave {
        bits: target_bits,
        rate,
        channels,
        frames,
        pcm: canonical,
    }
}

fn event_lines(output: &ProcessOutput) -> Vec<Value> {
    let text = String::from_utf8(output.stdout.clone()).expect("converter stdout should be UTF-8");
    text.lines()
        .map(|line| serde_json::from_str(line).expect("converter stdout should be NDJSON"))
        .collect()
}

fn convert_request(
    input: &Path,
    output: &Path,
    version: u32,
    timeout_seconds: u64,
    cancel: bool,
) -> ProcessOutput {
    convert_request_with_compatibility(input, output, version, timeout_seconds, true, cancel)
}

fn convert_request_with_compatibility(
    input: &Path,
    output: &Path,
    version: u32,
    timeout_seconds: u64,
    allow_compatibility: bool,
    cancel: bool,
) -> ProcessOutput {
    let request = json!({
        "command": "convert",
        "version": version,
        "input": input,
        "output": output,
        "allow_compatibility": allow_compatibility,
        "timeout_seconds": timeout_seconds,
    });
    let mut stdin = serde_json::to_vec(&request).expect("convert command should serialize");
    stdin.push(b'\n');
    if cancel {
        // Root's protocol processes a buffered cancel with the convert before starting work.
        stdin.extend_from_slice(b"{\"command\":\"cancel\"}\n");
    }
    let mut command = Command::new(BINARY);
    execute(&mut command, &stdin, DRIVER_TIMEOUT).unwrap_or_else(|reason| panic!("{reason}"))
}

fn assert_failed_without_final(output: &ProcessOutput, final_path: &Path) -> Vec<Value> {
    let events = event_lines(output);
    assert!(
        events.iter().any(|event| event["event"] == "failed"),
        "expected failed event: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!events.iter().any(|event| event["event"] == "completed"));
    assert!(
        !final_path.exists(),
        "failed conversion must not publish a final file"
    );
    for event in &events {
        if let Some(directory) = event.get("staging_directory").and_then(Value::as_str) {
            assert!(
                Path::new(directory).is_dir(),
                "reported staging must be retained"
            );
        }
        if let Some(partial) = event.get("partial_output").and_then(Value::as_str) {
            assert!(
                Path::new(partial).exists(),
                "reported partial output must be retained"
            );
        }
    }
    events
}

#[test]
fn eight_bit_flac_promotes_losslessly_only_in_compatibility_mode() {
    let spec = Spec {
        bits: 8,
        rate: 48_000,
        channels: 2,
        frames: 960,
    };
    let fixture = create_fixture(spec, false);
    let source_before = fs::read(&fixture.input).expect("8-bit source FLAC should be readable");
    let output = fixture.directory.path().join("promoted to 16-bit ALAC.m4a");
    let process = convert_request(&fixture.input, &output, 1, 1_800, false);
    assert!(
        process.status.success(),
        "compatibility conversion failed: stdout={} stderr={}",
        String::from_utf8_lossy(&process.stdout),
        String::from_utf8_lossy(&process.stderr)
    );
    let events = event_lines(&process);
    let completed = events
        .iter()
        .find(|event| event["event"] == "completed")
        .expect("8-bit compatibility conversion should complete");
    let report = &completed["report"];
    assert_eq!(report["source"]["bits_per_sample"], 8);
    assert_eq!(report["output"]["bits_per_sample"], 16);
    assert_eq!(report["audio_bit_exact"], true);
    assert_eq!(report["pcm_sha256"], fixture.pcm_sha256);
    assert!(
        report["warnings"]
            .as_array()
            .expect("conversion warnings should be an array")
            .iter()
            .any(|warning| warning["code"] == "precision_promoted")
    );

    let decoded_path = fixture
        .directory
        .path()
        .join("independent promoted decode.wav");
    let decode = run_tool(
        "/usr/bin/afconvert",
        &[
            "-f".into(),
            "WAVE".into(),
            "-d".into(),
            "LEI16".into(),
            output.to_string_lossy().into_owned(),
            decoded_path.to_string_lossy().into_owned(),
        ],
    );
    assert!(
        decode.status.success(),
        "independent promoted ALAC decode failed: {}",
        String::from_utf8_lossy(&decode.stderr)
    );
    let decoded = read_decoded_wave(&decoded_path, 16);
    assert_eq!(decoded.bits, 16);
    assert_eq!(decoded.rate, spec.rate);
    assert_eq!(decoded.channels, spec.channels);
    assert_eq!(decoded.frames, u64::from(spec.frames));

    let mut expected_promoted = Vec::with_capacity(fixture.pcm.len() * 2);
    for sample in &fixture.pcm {
        let promoted = i16::from(*sample as i8) * 256;
        expected_promoted.extend_from_slice(&promoted.to_le_bytes());
    }
    assert_eq!(decoded.pcm, expected_promoted);
    assert_eq!(
        sha256(&canonical_pcm32(&decoded.pcm, 16)),
        fixture.pcm_sha256
    );
    assert_eq!(
        fs::read(&fixture.input).expect("source remains readable after conversion"),
        source_before,
        "compatibility conversion must preserve the unsigned-WAVE-derived FLAC"
    );

    let strict_output = fixture.directory.path().join("strict mode must reject.m4a");
    let strict =
        convert_request_with_compatibility(&fixture.input, &strict_output, 1, 1_800, false, false);
    assert_failed_without_final(&strict, &strict_output);
    assert_eq!(
        fs::read(&fixture.input).expect("source remains readable after strict rejection"),
        source_before,
        "strict-mode rejection must preserve source bytes"
    );
}

#[test]
fn native_wav_pcm_widths_encode_bit_exactly_and_preserve_info_and_unknown_chunks() {
    for bits in [8, 16, 24, 32] {
        let spec = Spec {
            bits,
            rate: 48_000,
            channels: 2,
            frames: 960,
        };
        let directory = TempDir::new().expect("WAVE fixture directory should be created");
        let input = directory.path().join("source 日本語 русский café.wav");
        let output = directory.path().join("converted Italian 🎵.m4a");
        let decoded_path = directory.path().join("independent decode.wav");
        let pcm = synthetic_pcm(spec);
        let expected_pcm_sha256 = sha256(&canonical_pcm32(&pcm, bits));
        write_wave(&input, spec, &pcm);

        let mut info = b"INFO".to_vec();
        info_subchunk(b"INAM", "标题 日本語 한국어 café français", &mut info);
        info_subchunk(b"IART", "русский 한국어 исполнитель", &mut info);
        info_subchunk(b"IPRD", "Italiano", &mut info);
        let list_record = append_wave_chunk(&input, b"LIST", &info);
        let junk_record = append_wave_chunk(&input, b"JUNK", b"opaque");
        let source_before = fs::read(&input).expect("WAVE source should be readable");

        let process = convert_request(&input, &output, 1, 1_800, false);
        assert!(
            process.status.success(),
            "WAVE conversion failed for {bits}-bit PCM: stdout={} stderr={}",
            String::from_utf8_lossy(&process.stdout),
            String::from_utf8_lossy(&process.stderr)
        );
        let events = event_lines(&process);
        let report = &events
            .iter()
            .find(|event| event["event"] == "completed")
            .expect("WAVE conversion should complete")["report"];
        let output_bits = if bits == 8 { 16 } else { bits };
        assert_eq!(report["source"]["bits_per_sample"], bits);
        assert_eq!(report["output"]["bits_per_sample"], output_bits);
        assert_eq!(report["source"]["frames"], spec.frames);
        assert_eq!(report["audio_bit_exact"], true);
        assert_eq!(report["pcm_sha256"], expected_pcm_sha256);
        let warnings = report["warnings"]
            .as_array()
            .expect("warnings should be listed");
        assert_eq!(
            warnings
                .iter()
                .any(|warning| warning["code"] == "precision_promoted"),
            bits == 8
        );
        assert_eq!(
            fs::read(&input).expect("source remains readable after conversion"),
            source_before,
            "WAVE source must remain byte-for-byte unchanged"
        );

        let decode = run_tool(
            "/usr/bin/afconvert",
            &[
                "-f".into(),
                "WAVE".into(),
                "-d".into(),
                format!("LEI{output_bits}"),
                output.to_string_lossy().into_owned(),
                decoded_path.to_string_lossy().into_owned(),
            ],
        );
        assert!(
            decode.status.success(),
            "independent WAVE-to-ALAC decode failed: {}",
            String::from_utf8_lossy(&decode.stderr)
        );
        let decoded = read_decoded_wave(&decoded_path, output_bits);
        assert_eq!(decoded.rate, spec.rate);
        assert_eq!(decoded.channels, spec.channels);
        assert_eq!(decoded.frames, u64::from(spec.frames));
        let expected_encoded_pcm = if bits == 8 {
            pcm.iter()
                .map(|byte| (i16::from(*byte as i8) * 256).to_le_bytes())
                .flat_map(|sample| sample.into_iter())
                .collect::<Vec<_>>()
        } else {
            pcm.clone()
        };
        assert_eq!(decoded.pcm, expected_encoded_pcm);
        assert_eq!(
            sha256(&canonical_pcm32(&decoded.pcm, output_bits)),
            expected_pcm_sha256,
            "AudioToolbox's independent decode must match the WAVE source PCM"
        );

        let tag = Tag::read_from_path(&output).expect("converted M4A metadata should be readable");
        for (atom, expected) in [
            (Fourcc(*b"\xa9nam"), "标题 日本語 한국어 café français"),
            (Fourcc(*b"\xa9ART"), "русский 한국어 исполнитель"),
            (Fourcc(*b"\xa9alb"), "Italiano"),
        ] {
            assert_eq!(
                tag.strings_of(&atom).map(str::to_owned).collect::<Vec<_>>(),
                vec![expected.to_owned()]
            );
        }
        let title_audit = FreeformIdent::new_borrowed(WAV_AUDIT_MEAN, "INFO_INAM");
        assert_eq!(
            tag.data_of(&title_audit).cloned().collect::<Vec<_>>(),
            vec![Data::Utf8("标题 日本語 한국어 café français".to_owned())]
        );
        let list_audit =
            FreeformIdent::new_borrowed(WAV_AUDIT_RAW_MEAN, "WAV_CHUNK_000002_4C495354");
        let junk_audit =
            FreeformIdent::new_borrowed(WAV_AUDIT_RAW_MEAN, "WAV_CHUNK_000003_4A554E4B");
        assert_eq!(
            tag.data_of(&list_audit).cloned().collect::<Vec<_>>(),
            vec![Data::Reserved(list_record)]
        );
        assert_eq!(
            tag.data_of(&junk_audit).cloned().collect::<Vec<_>>(),
            vec![Data::Reserved(junk_record)]
        );
        let preserved = report["metadata"]["preserved"]
            .as_array()
            .expect("preserved metadata should be listed");
        assert!(preserved.contains(&json!("WAV INFO INAM")));
        assert!(preserved.contains(&json!("WAV chunk 2 LIST")));
        assert!(preserved.contains(&json!("WAV chunk 3 JUNK")));
        assert!(
            report["metadata"]["unmapped"]
                .as_array()
                .unwrap()
                .contains(&json!("WAV chunk 3 JUNK"))
        );
    }
}

#[test]
fn native_worker_rejects_float_wav_and_cancels_pcm_wav_without_publication() {
    let failed_directory = TempDir::new().expect("failure fixture directory should be created");
    let failed_input = failed_directory.path().join("float input.wav");
    let failed_output = failed_directory.path().join("float output.m4a");
    let float_spec = Spec {
        bits: 32,
        rate: 48_000,
        channels: 2,
        frames: 32,
    };
    write_wave(&failed_input, float_spec, &synthetic_pcm(float_spec));
    let mut float_bytes = fs::read(&failed_input).expect("generated WAV should be readable");
    float_bytes[20..22].copy_from_slice(&3_u16.to_le_bytes());
    fs::write(&failed_input, float_bytes).expect("WAV format tag should be changed for fixture");
    let failed_before = fs::read(&failed_input).expect("float WAV should be readable");
    let failure = ConverterClient::new(BINARY)
        .convert(
            &ConvertRequest::new(&failed_input, &failed_output),
            &AtomicBool::new(false),
            &mut |_| Ok(()),
        )
        .unwrap_err();
    let failure_message = format!("{failure:#}");
    assert!(
        failure_message.contains("floating-point"),
        "expected the worker's WAVE float rejection, got {failure_message}"
    );
    assert!(
        !failed_output.exists(),
        "unsupported float WAVE must not publish"
    );
    assert_eq!(
        fs::read(&failed_input).expect("float source remains readable"),
        failed_before,
        "failed WAVE conversion must leave its source unchanged"
    );

    let cancel_directory =
        TempDir::new().expect("cancellation fixture directory should be created");
    let cancel_input = cancel_directory.path().join("large PCM source.wav");
    let cancel_output = cancel_directory.path().join("cancelled output.m4a");
    let cancel_spec = Spec {
        bits: 16,
        rate: 48_000,
        channels: 2,
        frames: 3_000_000,
    };
    write_wave(&cancel_input, cancel_spec, &synthetic_pcm(cancel_spec));
    let cancel_before = fs::read(&cancel_input).expect("large WAVE source should be readable");
    let mut request = ConvertRequest::new(&cancel_input, &cancel_output);
    request.timeout_seconds = 60;
    let cancelled = AtomicBool::new(false);
    let saw_progress = AtomicBool::new(false);
    let error = ConverterClient::new(BINARY)
        .convert(&request, &cancelled, &mut |event| {
            if matches!(event, Event::Progress { frames, .. } if *frames > 0) {
                saw_progress.store(true, Ordering::Release);
                cancelled.store(true, Ordering::Release);
            }
            Ok(())
        })
        .unwrap_err();
    assert!(
        saw_progress.load(Ordering::Acquire),
        "WAVE encoding must report progress before cancellation"
    );
    let cancellation_message = format!("{error:#}");
    assert!(
        cancellation_message.contains("cancel"),
        "expected cancellation, got {cancellation_message}"
    );
    assert!(
        !cancel_output.exists(),
        "cancelled WAVE conversion must not publish"
    );
    assert_eq!(
        fs::read(&cancel_input).expect("cancelled WAVE source remains readable"),
        cancel_before,
        "cancelled WAVE conversion must leave its source unchanged"
    );
}

#[test]
fn cooperative_cancellation_during_native_frames_retains_staging_and_original() {
    let fixture = create_fixture(
        Spec {
            bits: 24,
            rate: 48_000,
            channels: 2,
            frames: 192_000,
        },
        false,
    );
    let before = fs::read(&fixture.input).unwrap();
    let output = fixture
        .directory
        .path()
        .join("cancelled-during-encoding.m4a");
    let cancelled = AtomicBool::new(false);
    let failure = flac_alac_converter::engine::convert(
        &ConvertRequest::new(&fixture.input, &output),
        &cancelled,
        &mut |event| {
            if matches!(event, Event::Progress { frames, .. } if frames > 0) {
                cancelled.store(true, Ordering::Release);
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert!(failure.cancelled);
    assert!(failure.staging_directory.unwrap().is_dir());
    assert!(failure.partial_output.unwrap().is_file());
    assert!(!output.exists());
    assert_eq!(fs::read(&fixture.input).unwrap(), before);
}

#[test]
fn original_metadata_changes_are_benign_but_content_changes_prevent_publication() {
    use std::io::{Seek, SeekFrom};
    use std::os::unix::fs::PermissionsExt;
    for change_content in [false, true] {
        let fixture = create_fixture(
            Spec {
                bits: 16,
                rate: 48_000,
                channels: 2,
                frames: 1_920,
            },
            false,
        );
        let before = fs::read(&fixture.input).unwrap();
        let output = fixture.directory.path().join("source-stability.m4a");
        let result = flac_alac_converter::engine::convert(
            &ConvertRequest::new(&fixture.input, &output),
            &AtomicBool::new(false),
            &mut |event| {
                if matches!(event, Event::Started { .. }) {
                    if change_content {
                        let mut file = fs::OpenOptions::new().write(true).open(&fixture.input)?;
                        file.seek(SeekFrom::End(-1))?;
                        file.write_all(&[before.last().unwrap() ^ 1])?;
                        file.sync_all()?;
                    } else {
                        fs::set_permissions(&fixture.input, fs::Permissions::from_mode(0o400))?;
                    }
                }
                Ok(())
            },
        );
        if change_content {
            let failure = result.unwrap_err();
            assert!(failure.message.contains("original input changed"));
            assert!(failure.staging_directory.unwrap().is_dir());
            assert!(!output.exists());
            let mut expected = before;
            *expected.last_mut().unwrap() ^= 1;
            assert_eq!(fs::read(&fixture.input).unwrap(), expected);
        } else {
            assert!(result.unwrap().audio_bit_exact);
            assert_eq!(fs::read(&fixture.input).unwrap(), before);
        }
    }
}

#[test]
fn real_flac_to_alac_matrix_preserves_integer_pcm_and_original_comments() {
    let version = run_tool("flac", &["--version".into()]);
    assert!(version.status.success());
    assert!(String::from_utf8_lossy(&version.stdout).starts_with("flac "));

    let cases = [
        Spec {
            bits: 16,
            rate: 48_000,
            channels: 2,
            frames: 1_920,
        },
        Spec {
            bits: 24,
            rate: 96_000,
            channels: 1,
            frames: 3_840,
        },
        Spec {
            bits: 32,
            rate: 44_100,
            channels: 2,
            frames: 1_764,
        },
    ];
    for (case_index, spec) in cases.into_iter().enumerate() {
        let fixture = create_fixture(spec, true);
        let raw_fixture = if case_index == 0 {
            Some(rewrite_fixture_metadata(&fixture.input))
        } else {
            None
        };
        let source_before = fs::read(&fixture.input).expect("source FLAC should be readable");
        let output = fixture
            .directory
            .path()
            .join("converted Apple Lossless 日本語 русский café e\u{301} 🎵.m4a");
        let (events, report) = if case_index == 1 {
            let mut request = ConvertRequest::new(&fixture.input, &output);
            request.timeout_seconds = 30;
            let mut observed = Vec::<Event>::new();
            let typed_report = ConverterClient::new(BINARY)
                .convert(&request, &AtomicBool::new(false), &mut |event| {
                    observed.push(event.clone());
                    Ok(())
                })
                .expect("typed client conversion should complete");
            let events = observed
                .iter()
                .map(|event| serde_json::to_value(event).expect("event should serialize"))
                .collect::<Vec<_>>();
            (
                events,
                serde_json::to_value(typed_report).expect("report should serialize"),
            )
        } else {
            let process = convert_request(&fixture.input, &output, 1, 1_800, false);
            assert!(
                process.status.success(),
                "converter failed: stdout={} stderr={}",
                String::from_utf8_lossy(&process.stdout),
                String::from_utf8_lossy(&process.stderr)
            );
            let events = event_lines(&process);
            let completed = events
                .iter()
                .find(|event| event["event"] == "completed")
                .expect("conversion should complete");
            let report = completed["report"].clone();
            (events, report)
        };
        assert!(events.iter().any(|event| event["event"] == "started"));
        let expected_spec = json!({
            "sample_rate": spec.rate,
            "channels": spec.channels,
            "bits_per_sample": spec.bits,
            "frames": spec.frames,
        });
        assert_eq!(report["source"], expected_spec);
        assert_eq!(report["output"], expected_spec);
        assert_eq!(report["audio_bit_exact"], true);
        assert_eq!(report["pcm_sha256"], fixture.pcm_sha256);
        assert!(report["metadata"]["mapped"].is_array());
        assert!(report["metadata"]["preserved"].is_array());
        assert!(report["metadata"]["unmapped"].is_array());
        assert!(report["metadata"]["warnings"].is_array());
        assert!(output.is_file(), "final ALAC output should exist");

        let decoded = fixture
            .directory
            .path()
            .join("independent afconvert decode.wav");
        let decode = run_tool(
            "/usr/bin/afconvert",
            &[
                "-f".into(),
                "WAVE".into(),
                "-d".into(),
                format!("LEI{}", spec.bits),
                output.to_string_lossy().into_owned(),
                decoded.to_string_lossy().into_owned(),
            ],
        );
        assert!(
            decode.status.success(),
            "independent ALAC decode failed: {}",
            String::from_utf8_lossy(&decode.stderr)
        );
        let decoded = read_decoded_wave(&decoded, spec.bits);
        assert_eq!(decoded.bits, spec.bits);
        assert_eq!(decoded.rate, spec.rate);
        assert_eq!(decoded.channels, spec.channels);
        assert_eq!(decoded.frames, u64::from(spec.frames));
        assert_eq!(
            sha256(&canonical_pcm32(&decoded.pcm, spec.bits)),
            fixture.pcm_sha256
        );
        assert_eq!(decoded.pcm, fixture.pcm);

        let tag = Tag::read_from_path(&output).expect("M4A metadata should be readable");
        for (atom, key) in [
            (Fourcc(*b"\xa9nam"), "TITLE"),
            (Fourcc(*b"\xa9ART"), "ARTIST"),
            (Fourcc(*b"\xa9alb"), "ALBUM"),
            (Fourcc(*b"\xa9gen"), "GENRE"),
        ] {
            let expected = fixture
                .original_comments
                .iter()
                .filter(|(source_key, _)| source_key == key)
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>();
            let actual = tag.strings_of(&atom).map(str::to_owned).collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "standard {key} values must retain exact UTF-8"
            );
        }
        let expected_comments = fixture
            .original_comments
            .iter()
            .filter(|(key, _)| key == "COMMENT" || key == "DESCRIPTION")
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            tag.strings_of(&Fourcc(*b"\xa9cmt"))
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            expected_comments,
            "standard comment values must retain exact UTF-8"
        );

        let audited = freeform_comments(&output);
        let expected_audit = fixture
            .original_comments
            .iter()
            .map(|(name, value)| (AUDIT_MEAN.to_owned(), name.clone(), value.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            audited, expected_audit,
            "original freeform atoms retain order"
        );

        let mut seen_names = Vec::new();
        for (name, _) in &fixture.original_comments {
            if seen_names.contains(name) {
                continue;
            }
            seen_names.push(name.clone());
            let ident = FreeformIdent::new_borrowed(AUDIT_MEAN, name);
            let expected = fixture
                .original_comments
                .iter()
                .filter(|(key, _)| key == name)
                .map(|(_, value)| Data::Utf8(value.clone()))
                .collect::<Vec<_>>();
            assert_eq!(
                tag.data_of(&ident).cloned().collect::<Vec<_>>(),
                expected,
                "all original {name} values must be retained exactly and in order"
            );
        }
        assert!(
            report["metadata"]["unmapped"]
                .as_array()
                .unwrap()
                .contains(&json!("CUSTOM_FIELD"))
        );

        if let Some(raw) = raw_fixture {
            let covr = tag.data_of(&Fourcc(*b"covr")).cloned().collect::<Vec<_>>();
            assert_eq!(covr, vec![Data::Png(raw.picture_png.clone())]);
            let picture_name = format!("FLAC_PICTURE_BLOCK_{:06}", raw.picture_block_index);
            let picture_ident = FreeformIdent::new_borrowed(AUDIT_RAW_MEAN, &picture_name);
            assert_eq!(
                tag.data_of(&picture_ident).cloned().collect::<Vec<_>>(),
                vec![Data::Reserved(append_flac_picture(&raw.picture_png))]
            );
            let invalid_ident =
                FreeformIdent::new_borrowed(AUDIT_RAW_MEAN, &raw.invalid_comment_name);
            assert_eq!(
                tag.data_of(&invalid_ident).cloned().collect::<Vec<_>>(),
                vec![Data::Reserved(raw.invalid_comment.clone())]
            );
            let opaque_ident = FreeformIdent::new_borrowed(AUDIT_RAW_MEAN, &raw.opaque_block_name);
            assert_eq!(
                tag.data_of(&opaque_ident).cloned().collect::<Vec<_>>(),
                vec![Data::Reserved(raw.opaque_payload.clone())]
            );
            let warnings = report["metadata"]["warnings"].as_array().unwrap();
            assert!(
                warnings
                    .iter()
                    .any(|warning| warning["code"] == "invalid_vorbis_comment_preserved")
            );
            assert!(
                warnings
                    .iter()
                    .any(|warning| warning["code"] == "opaque_flac_block_preserved")
            );
        }
        assert_eq!(
            fs::read(&fixture.input).expect("source remains readable"),
            source_before
        );
    }
}

#[test]
fn protocol_failures_cancellation_and_existing_destinations_never_publish_or_delete_partials() {
    let spec = Spec {
        bits: 16,
        rate: 48_000,
        channels: 2,
        frames: 960,
    };
    let fixture = create_fixture(spec, false);
    let source_before = fs::read(&fixture.input).expect("source FLAC should be readable");

    let bad_version_output = fixture.directory.path().join("unsupported protocol.m4a");
    let bad_version = convert_request(&fixture.input, &bad_version_output, 9, 1_800, false);
    assert_failed_without_final(&bad_version, &bad_version_output);

    let invalid_deadline_output = fixture.directory.path().join("invalid deadline.m4a");
    let invalid_deadline = convert_request(&fixture.input, &invalid_deadline_output, 1, 0, false);
    assert_failed_without_final(&invalid_deadline, &invalid_deadline_output);

    let cancelled_output = fixture.directory.path().join("cancelled - 日本語.m4a");
    let cancelled = convert_request(&fixture.input, &cancelled_output, 1, 1_800, true);
    let cancelled_events = event_lines(&cancelled);
    assert!(
        cancelled_events
            .iter()
            .any(|event| event["event"] == "cancelled"),
        "buffered cancel should be honored"
    );
    assert!(
        !cancelled_events
            .iter()
            .any(|event| event["event"] == "completed")
    );
    assert!(!cancelled_output.exists());
    for event in &cancelled_events {
        if let Some(partial) = event.get("partial_output").and_then(Value::as_str) {
            assert!(
                Path::new(partial).exists(),
                "cancelled partial output must be retained"
            );
        }
    }

    let existing_output = fixture.directory.path().join("existing destination.m4a");
    let sentinel = b"pre-existing destination must remain byte-for-byte unchanged";
    fs::write(&existing_output, sentinel).expect("sentinel output should be created");
    let existing = convert_request(&fixture.input, &existing_output, 1, 1_800, false);
    let existing_events = event_lines(&existing);
    assert!(
        existing_events
            .iter()
            .any(|event| event["event"] == "failed")
    );
    assert_eq!(
        fs::read(&existing_output).expect("existing output remains"),
        sentinel
    );

    let corrupt = fixture.directory.path().join("corrupt source.flac");
    fs::write(&corrupt, b"not an FLAC stream\0\xff")
        .expect("corrupt source fixture should be written");
    let corrupt_before = fs::read(&corrupt).expect("corrupt fixture should be readable");
    let corrupt_output = fixture.directory.path().join("corrupt result.m4a");
    let corrupt_process = convert_request(&corrupt, &corrupt_output, 1, 1_800, false);
    assert_failed_without_final(&corrupt_process, &corrupt_output);
    assert_eq!(
        fs::read(&corrupt).expect("corrupt source remains"),
        corrupt_before
    );
    assert_eq!(
        fs::read(&fixture.input).expect("source remains readable"),
        source_before
    );
}

#[test]
fn six_channel_input_is_rejected_without_downmixing_or_changing_source() {
    let spec = Spec {
        bits: 24,
        rate: 48_000,
        channels: 6,
        frames: 960,
    };
    let fixture = create_fixture(spec, false);
    let source_before = fs::read(&fixture.input).expect("6-channel source remains readable");
    let width = (spec.bits / 8) as usize;
    let first_frame = fixture.pcm.chunks_exact(width).take(6).collect::<Vec<_>>();
    for left in 0..first_frame.len() {
        for right in left + 1..first_frame.len() {
            assert_ne!(
                first_frame[left], first_frame[right],
                "each speaker starts with a distinct sample"
            );
        }
    }
    let source = flac_alac_converter::metadata::read(&fixture.input)
        .expect("6-channel STREAMINFO should be valid");
    assert_eq!(source.audio.channels, 6);

    let output = fixture.directory.path().join("must not downmix.m4a");
    let process = convert_request(&fixture.input, &output, 1, 1_800, false);
    assert_failed_without_final(&process, &output);
    assert_eq!(
        fs::read(&fixture.input).expect("source remains readable"),
        source_before,
        "explicit multichannel rejection must preserve source bytes"
    );
}

fn freeform_comments(path: &Path) -> Vec<(String, String, String)> {
    let bytes = fs::read(path).expect("ALAC file should be readable for metadata verification");
    assert!(
        bytes.len() <= 32 * 1024 * 1024,
        "ALAC output exceeded metadata test bound"
    );
    let mut comments = Vec::new();
    walk_mp4_atoms(&bytes, 0, &mut comments);
    comments
}

fn walk_mp4_atoms(bytes: &[u8], depth: usize, comments: &mut Vec<(String, String, String)>) {
    assert!(depth < 12, "MP4 atom nesting exceeded bound");
    let mut offset = 0_usize;
    while offset + 8 <= bytes.len() {
        let size32 = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let kind = &bytes[offset + 4..offset + 8];
        let (size, header) = if size32 == 1 {
            assert!(offset + 16 <= bytes.len());
            (
                u64::from_be_bytes(bytes[offset + 8..offset + 16].try_into().unwrap()) as usize,
                16,
            )
        } else if size32 == 0 {
            (bytes.len() - offset, 8)
        } else {
            (size32 as usize, 8)
        };
        assert!(
            size >= header && offset + size <= bytes.len(),
            "MP4 atom bounds are valid"
        );
        let payload = &bytes[offset + header..offset + size];
        if kind == b"----" {
            comments.extend(parse_freeform(payload));
        } else if matches!(
            kind,
            b"moov" | b"udta" | b"meta" | b"ilst" | b"trak" | b"mdia" | b"minf" | b"stbl"
        ) {
            let child_payload = if kind == b"meta" {
                assert!(payload.len() >= 4);
                &payload[4..]
            } else {
                payload
            };
            walk_mp4_atoms(child_payload, depth + 1, comments);
        }
        offset += size;
        if size32 == 0 {
            break;
        }
    }
    assert_eq!(
        offset,
        bytes.len(),
        "MP4 atom list should have no trailing fragment"
    );
}

fn parse_freeform(payload: &[u8]) -> Vec<(String, String, String)> {
    let mut offset = 0_usize;
    let mut mean = None;
    let mut name = None;
    let mut values = Vec::new();
    while offset + 8 <= payload.len() {
        let size = u32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        let kind = &payload[offset + 4..offset + 8];
        assert!(
            size >= 8 && offset + size <= payload.len(),
            "freeform atom bounds are valid"
        );
        let content = &payload[offset + 8..offset + size];
        match kind {
            b"mean" => {
                assert!(content.len() >= 4);
                mean =
                    Some(String::from_utf8(content[4..].to_vec()).expect("freeform mean is UTF-8"));
            }
            b"name" => {
                assert!(content.len() >= 4);
                name =
                    Some(String::from_utf8(content[4..].to_vec()).expect("freeform name is UTF-8"));
            }
            b"data" => {
                assert!(content.len() >= 8);
                values.push((
                    u32::from_be_bytes(content[..4].try_into().unwrap()),
                    &content[8..],
                ));
            }
            _ => {}
        }
        offset += size;
    }
    let (Some(mean), Some(name)) = (mean, name) else {
        return Vec::new();
    };
    if mean != AUDIT_MEAN {
        return Vec::new();
    }
    values
        .into_iter()
        .map(|(kind, value)| {
            assert_eq!(kind, 1, "original comment audit data must be UTF-8");
            (
                mean.clone(),
                name.clone(),
                String::from_utf8(value.to_vec()).expect("original comment is UTF-8"),
            )
        })
        .collect()
}
