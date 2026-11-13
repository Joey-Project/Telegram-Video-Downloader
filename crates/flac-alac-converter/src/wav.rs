//! Bounded RIFF/WAVE PCM inspection for the native ALAC worker.
//!
//! Only little-endian RIFF with integer PCM is accepted. Audio samples remain
//! in their source chunk ranges; this parser reads only bounded headers and
//! metadata chunks.

use crate::metadata::{RawMetadataChunk, SourceComment, SourceKind, SourceMetadata};
use crate::types::{AudioSpec, Warning};
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const RIFF_HEADER_BYTES: u64 = 12;
const MAX_CHUNKS: usize = 1024;
const MAX_METADATA_BYTES: usize = 32 * 1024 * 1024;
const MAX_METADATA_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const MAX_INFO_COMMENTS: usize = 8192;
const MAX_INFO_COMMENT_BYTES: usize = 1024 * 1024;
const MAX_INFO_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_WARNINGS: usize = 128;
const PCM_SUBFORMAT_GUID: &[u8; 16] =
    b"\x01\x00\x00\x00\x00\x00\x10\x00\x80\x00\x00\xaa\x00\x38\x9b\x71";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataChunk {
    pub offset: u64,
    pub bytes: u64,
}

/// Parsed audio layout plus the shared metadata representation used by FLAC.
#[derive(Debug, Clone)]
pub struct WavSource {
    pub metadata: SourceMetadata,
    pub data_chunks: Vec<DataChunk>,
    pub container_bits: u32,
    pub block_align: u32,
}

/// Detect RIFF-family files so malformed or unsupported WAVE headers reach the
/// WAVE parser and receive a specific error instead of a generic FLAC error.
pub fn is_riff_file(path: &Path) -> Result<bool> {
    let mut file =
        File::open(path).with_context(|| format!("open audio source {}", path.display()))?;
    let mut signature = [0; 4];
    match file.read_exact(&mut signature) {
        Ok(()) => Ok(matches!(&signature, b"RIFF" | b"RIFX" | b"RF64" | b"BW64")),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error).context("read audio source signature"),
    }
}

pub fn read(path: &Path) -> Result<WavSource> {
    let mut file =
        File::open(path).with_context(|| format!("open WAVE source {}", path.display()))?;
    let file_metadata = file.metadata().context("stat WAVE source")?;
    ensure!(file_metadata.is_file(), "WAVE source is not a regular file");
    let file_length = file_metadata.len();
    ensure!(
        file_length >= RIFF_HEADER_BYTES,
        "truncated WAVE RIFF header"
    );

    let mut header = [0; 12];
    file.read_exact(&mut header)
        .context("read WAVE RIFF header")?;
    ensure!(&header[8..12] == b"WAVE", "RIFF file is not WAVE audio");
    ensure!(
        &header[..4] == b"RIFF",
        "unsupported WAVE container; only little-endian RIFF is supported"
    );
    let riff_size = u64::from(u32::from_le_bytes(header[4..8].try_into().unwrap()));
    ensure!(riff_size >= 4, "WAVE RIFF size is too small");
    let riff_end = 8_u64
        .checked_add(riff_size)
        .context("WAVE RIFF size overflow")?;
    ensure!(
        riff_end == file_length,
        "WAVE RIFF size does not match file length"
    );

    let mut offset = RIFF_HEADER_BYTES;
    let mut chunk_index = 0usize;
    let mut format = None;
    let mut data_chunks = Vec::new();
    let mut metadata_bytes = 0usize;
    let mut comments = Vec::new();
    let mut raw_chunks = Vec::new();
    let mut warnings = Vec::new();
    let mut comment_bytes = 0usize;
    let mut total_data_bytes = 0_u64;

    while offset < riff_end {
        ensure!(chunk_index < MAX_CHUNKS, "WAVE chunk count exceeds limit");
        ensure!(
            riff_end - offset >= 8,
            "truncated WAVE chunk header at offset {offset}"
        );
        file.seek(SeekFrom::Start(offset))
            .context("seek to WAVE chunk header")?;
        let mut chunk_header = [0; 8];
        file.read_exact(&mut chunk_header)
            .context("read WAVE chunk header")?;
        let id: [u8; 4] = chunk_header[..4].try_into().unwrap();
        let chunk_length = u64::from(u32::from_le_bytes(chunk_header[4..8].try_into().unwrap()));
        let payload_start = offset
            .checked_add(8)
            .context("WAVE chunk offset overflow")?;
        let payload_end = payload_start
            .checked_add(chunk_length)
            .context("WAVE chunk length overflow")?;
        ensure!(
            payload_end <= riff_end,
            "WAVE chunk extends beyond RIFF bounds"
        );
        let padded_end = payload_end
            .checked_add(chunk_length & 1)
            .context("WAVE chunk padding offset overflow")?;
        ensure!(padded_end <= riff_end, "truncated WAVE chunk padding");

        match &id {
            b"fmt " => {
                ensure!(format.is_none(), "WAVE contains duplicate fmt chunks");
                ensure!(chunk_length <= 64 * 1024, "WAVE fmt chunk exceeds limit");
                let payload = read_chunk_payload(&mut file, payload_start, chunk_length)?;
                let parsed = parse_format(&payload)?;
                format = Some(parsed);
                if chunk_length != 16 {
                    let fmt_tag = u16::from_le_bytes(payload[0..2].try_into().unwrap());
                    let record_length =
                        usize::try_from(padded_end.saturating_sub(offset)).unwrap_or(usize::MAX);
                    if record_length > MAX_METADATA_CHUNK_BYTES
                        || metadata_bytes
                            .checked_add(record_length)
                            .is_none_or(|total| total > MAX_METADATA_BYTES)
                    {
                        push_warning(
                            &mut warnings,
                            "wav_chunk_not_preserved",
                            format!(
                                "WAVE fmt extension ({} bytes) exceeds the bounded metadata preservation budget and was not copied to the M4A.",
                                chunk_length
                            ),
                        );
                    } else {
                        let name = format!("WAV_CHUNK_{chunk_index:06}_{}", hex_id(&id));
                        let label = format!("WAV chunk {chunk_index} {}", display_id(&id));
                        let record =
                            read_chunk_record(&mut file, offset, padded_end, chunk_length)?;
                        metadata_bytes = metadata_bytes
                            .checked_add(record.len())
                            .context("WAVE metadata byte count overflow")?;
                        raw_chunks.push(RawMetadataChunk {
                            name,
                            label,
                            bytes: record,
                            unmapped: fmt_tag != 0xfffe && payload.len() > 18,
                        });
                    }
                }
            }
            b"data" => {
                total_data_bytes = total_data_bytes
                    .checked_add(chunk_length)
                    .context("WAVE data size overflow")?;
                data_chunks.push(DataChunk {
                    offset: payload_start,
                    bytes: chunk_length,
                });
            }
            _ => {
                let payload_size = usize::try_from(chunk_length).ok();
                let can_retain = payload_size.is_some_and(|size| {
                    size <= MAX_METADATA_CHUNK_BYTES
                        && metadata_bytes
                            .checked_add(size.saturating_add(9))
                            .is_some_and(|total| total <= MAX_METADATA_BYTES)
                });
                if can_retain {
                    let name = format!("WAV_CHUNK_{chunk_index:06}_{}", hex_id(&id));
                    let label = format!("WAV chunk {chunk_index} {}", display_id(&id));
                    let record = read_chunk_record(&mut file, offset, padded_end, chunk_length)?;
                    metadata_bytes = metadata_bytes
                        .checked_add(record.len())
                        .context("WAVE metadata byte count overflow")?;
                    let body_end = 8_usize
                        .checked_add(payload_size.expect("retained chunk size was checked"))
                        .context("WAVE metadata body offset overflow")?;
                    let body = &record[8..body_end];
                    let mut unmapped = true;
                    if id == *b"LIST" {
                        match parse_info_list(
                            body,
                            chunk_index,
                            &mut comments,
                            &mut comment_bytes,
                            &mut warnings,
                        ) {
                            Ok(true) => unmapped = false,
                            Ok(false) => {}
                            Err(reason) => push_warning(
                                &mut warnings,
                                "wav_info_not_parsed",
                                format!(
                                    "{label} was preserved, but its INFO subchunks could not be parsed: {reason}."
                                ),
                            ),
                        }
                    }
                    raw_chunks.push(RawMetadataChunk {
                        name,
                        label,
                        bytes: record,
                        unmapped,
                    });
                } else {
                    push_warning(
                        &mut warnings,
                        "wav_chunk_not_preserved",
                        format!(
                            "WAVE chunk {} ({} bytes) exceeds the bounded metadata preservation budget and was not copied to the M4A.",
                            display_id(&id),
                            chunk_length
                        ),
                    );
                }
            }
        }
        offset = padded_end;
        chunk_index += 1;
    }
    ensure!(offset == riff_end, "WAVE chunk walk did not reach RIFF end");

    let parsed_format = format.context("WAVE source has no fmt chunk")?;
    ensure!(!data_chunks.is_empty(), "WAVE source has no data chunk");
    for (index, chunk) in data_chunks.iter().enumerate() {
        ensure!(
            chunk
                .bytes
                .is_multiple_of(u64::from(parsed_format.block_align)),
            "WAVE data chunk {index} is not an exact number of sample frames"
        );
    }
    ensure!(total_data_bytes > 0, "WAVE source has no audio frames");
    ensure!(
        total_data_bytes.is_multiple_of(u64::from(parsed_format.block_align)),
        "WAVE data size is not an exact number of sample frames"
    );
    let frames = total_data_bytes / u64::from(parsed_format.block_align);
    ensure!(frames > 0, "WAVE source has no audio frames");
    let audio = AudioSpec {
        sample_rate: parsed_format.sample_rate,
        channels: parsed_format.channels,
        bits_per_sample: parsed_format.valid_bits,
        frames,
    };

    Ok(WavSource {
        metadata: SourceMetadata {
            kind: SourceKind::Wav,
            audio,
            comments,
            comment_blocks: Vec::new(),
            invalid_comments: Vec::new(),
            pictures: Vec::new(),
            opaque_blocks: Vec::new(),
            id3v2_prefix: None,
            streaminfo: Vec::new(),
            raw_chunks,
            warnings,
        },
        data_chunks,
        container_bits: parsed_format.container_bits,
        block_align: parsed_format.block_align,
    })
}

#[derive(Debug, Clone, Copy)]
struct ParsedFormat {
    sample_rate: u32,
    channels: u32,
    container_bits: u32,
    valid_bits: u32,
    block_align: u32,
}

fn parse_format(bytes: &[u8]) -> Result<ParsedFormat> {
    ensure!(bytes.len() >= 16, "WAVE fmt chunk is shorter than 16 bytes");
    let tag = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
    let channels = u32::from(u16::from_le_bytes(bytes[2..4].try_into().unwrap()));
    let sample_rate = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let byte_rate = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let block_align = u32::from(u16::from_le_bytes(bytes[12..14].try_into().unwrap()));
    let container_bits = u32::from(u16::from_le_bytes(bytes[14..16].try_into().unwrap()));
    let valid_bits = match tag {
        1 => container_bits,
        3 => anyhow::bail!(
            "unsupported WAVE IEEE floating-point samples; conversion requires integer PCM to guarantee bit-exact ALAC"
        ),
        0xfffe => {
            ensure!(bytes.len() >= 40, "WAVE extensible fmt chunk is truncated");
            let extension_size = u16::from_le_bytes(bytes[16..18].try_into().unwrap());
            ensure!(
                extension_size >= 22,
                "WAVE extensible fmt data is too short"
            );
            ensure!(
                usize::from(extension_size) <= bytes.len() - 18,
                "WAVE extensible fmt extension exceeds its chunk"
            );
            ensure!(
                &bytes[24..40] == PCM_SUBFORMAT_GUID,
                "unsupported WAVE extensible subtype; only integer PCM is accepted"
            );
            let bits = u32::from(u16::from_le_bytes(bytes[18..20].try_into().unwrap()));
            let channel_mask = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
            if channel_mask != 0 && !matches!((channels, channel_mask), (1, 0x4) | (2, 0x3)) {
                anyhow::bail!(
                    "unsupported WAVE extensible channel mask 0x{channel_mask:08x}; this version supports mono center or stereo left/right only"
                );
            }
            bits
        }
        other => anyhow::bail!(
            "unsupported WAVE encoding tag 0x{other:04x}; only integer PCM is supported"
        ),
    };

    if tag == 1 && bytes.len() > 16 {
        ensure!(
            bytes.len() >= 18,
            "WAVE PCM fmt extension size is truncated"
        );
        let extension_size = usize::from(u16::from_le_bytes(bytes[16..18].try_into().unwrap()));
        ensure!(
            extension_size <= bytes.len() - 18,
            "WAVE PCM fmt extension exceeds its chunk"
        );
    }

    ensure!(
        (1..=2).contains(&channels),
        "unsupported WAVE channel count {channels}; this version supports mono and stereo only"
    );
    ensure!(sample_rate > 0, "WAVE sample rate must be nonzero");
    ensure!(
        matches!(container_bits, 8 | 16 | 24 | 32),
        "unsupported WAVE PCM container precision {container_bits}; expected 8, 16, 24, or 32 bits"
    );
    ensure!(
        matches!(valid_bits, 8 | 16 | 24 | 32),
        "unsupported WAVE valid sample precision {valid_bits}; expected 8, 16, 24, or 32 bits"
    );
    ensure!(
        valid_bits == container_bits,
        "WAVE valid precision {valid_bits} differs from its {container_bits}-bit container; padded samples are not accepted"
    );
    let expected_block_align = channels
        .checked_mul(container_bits / 8)
        .context("WAVE block alignment overflow")?;
    ensure!(
        block_align == expected_block_align,
        "WAVE block alignment {block_align} does not match channels and PCM width ({expected_block_align})"
    );
    let expected_byte_rate = sample_rate
        .checked_mul(block_align)
        .context("WAVE byte rate overflow")?;
    ensure!(
        byte_rate == expected_byte_rate,
        "WAVE byte rate {byte_rate} does not match sample rate and block alignment ({expected_byte_rate})"
    );
    Ok(ParsedFormat {
        sample_rate,
        channels,
        container_bits,
        valid_bits,
        block_align,
    })
}

/// Returns `true` for a parsed INFO LIST and appends recognized or opaque text
/// fields. The raw LIST chunk remains independently retained for exact audit.
fn parse_info_list(
    bytes: &[u8],
    chunk_index: usize,
    comments: &mut Vec<SourceComment>,
    total_comment_bytes: &mut usize,
    warnings: &mut Vec<Warning>,
) -> std::result::Result<bool, String> {
    if bytes.len() < 4 || &bytes[..4] != b"INFO" {
        return Ok(false);
    }
    let mut offset = 4usize;
    let mut entry_index = 0usize;
    let mut fully_interpreted = true;
    while offset < bytes.len() {
        if bytes.len() - offset < 8 {
            return Err("truncated INFO subchunk header".to_owned());
        }
        let id: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
        let size = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + 8;
        let payload_end = payload_start
            .checked_add(size)
            .ok_or_else(|| "INFO subchunk size overflow".to_owned())?;
        if payload_end > bytes.len() {
            return Err("INFO subchunk extends beyond LIST bounds".to_owned());
        }
        let padded_end = payload_end
            .checked_add(size & 1)
            .ok_or_else(|| "INFO subchunk padding overflow".to_owned())?;
        if padded_end > bytes.len() {
            return Err("truncated INFO subchunk padding".to_owned());
        }
        let code = display_id(&id);
        let payload = &bytes[payload_start..payload_end];
        let text_end = payload
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(payload.len());
        if payload[text_end..].iter().any(|byte| *byte != 0) {
            fully_interpreted = false;
            push_warning(
                warnings,
                "wav_info_trailing_bytes",
                format!(
                    "WAVE INFO field {code} contains nonzero bytes after its text terminator; the complete LIST chunk remains preserved."
                ),
            );
        }
        let value_bytes = &payload[..text_end];
        if value_bytes.len() > MAX_INFO_COMMENT_BYTES {
            fully_interpreted = false;
            push_warning(
                warnings,
                "wav_info_value_too_large",
                format!(
                    "WAVE INFO field {code} exceeds the text mapping limit and remains in the raw LIST chunk."
                ),
            );
        } else if let Ok(value) = std::str::from_utf8(value_bytes) {
            let next_total = total_comment_bytes.saturating_add(value_bytes.len());
            if comments.len() >= MAX_INFO_COMMENTS || next_total > MAX_INFO_TOTAL_BYTES {
                fully_interpreted = false;
                push_warning(
                    warnings,
                    "wav_info_mapping_limit",
                    "WAVE INFO text mapping reached its bounded field or byte limit; remaining fields stay in the raw LIST chunk.".to_owned(),
                );
            } else {
                *total_comment_bytes = next_total;
                let (key, known) = info_key(&id);
                fully_interpreted &= known;
                comments.push(SourceComment {
                    block_index: chunk_index,
                    entry_index,
                    key,
                    value: value.to_owned(),
                    audit_name: Some(format!("INFO_{code}")),
                });
                if !known {
                    push_warning(
                        warnings,
                        "wav_info_field_preserved_opaque",
                        format!(
                            "WAVE INFO field {code} was retained but is not mapped to a standard Music field."
                        ),
                    );
                }
            }
        } else {
            fully_interpreted = false;
            push_warning(
                warnings,
                "wav_info_invalid_utf8",
                format!(
                    "WAVE INFO field {code} is not valid UTF-8; its original bytes remain preserved in the raw LIST chunk."
                ),
            );
        }
        offset = padded_end;
        entry_index += 1;
    }
    Ok(fully_interpreted)
}

fn info_key(id: &[u8; 4]) -> (String, bool) {
    let key = match id {
        b"INAM" => "TITLE",
        b"IART" => "ARTIST",
        b"IPRD" => "ALBUM",
        b"ICRD" => "DATE",
        b"IGNR" => "GENRE",
        b"ICMT" | b"ICMS" => "COMMENT",
        b"ITRK" => "TRACKNUMBER",
        _ => return (display_id(id), false),
    };
    (key.to_owned(), true)
}

fn read_chunk_payload(file: &mut File, offset: u64, length: u64) -> Result<Vec<u8>> {
    let length =
        usize::try_from(length).context("WAVE metadata chunk exceeds addressable memory")?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .context("reserve bounded WAVE metadata chunk")?;
    bytes.resize(length, 0);
    file.seek(SeekFrom::Start(offset))
        .context("seek to WAVE metadata payload")?;
    file.read_exact(&mut bytes)
        .context("read WAVE metadata payload")?;
    Ok(bytes)
}

fn read_chunk_record(
    file: &mut File,
    offset: u64,
    padded_end: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let size = padded_end
        .checked_sub(offset)
        .context("WAVE metadata record size underflow")?;
    let size_usize = usize::try_from(size).context("WAVE metadata record too large")?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size_usize)
        .context("reserve bounded WAVE metadata record")?;
    bytes.resize(size_usize, 0);
    file.seek(SeekFrom::Start(offset))
        .context("seek to WAVE metadata record")?;
    file.read_exact(&mut bytes)
        .context("read WAVE metadata record")?;
    ensure!(
        bytes.len() == usize::try_from(length).unwrap_or(usize::MAX) + 8 + (length as usize & 1),
        "WAVE metadata record length changed"
    );
    Ok(bytes)
}

fn display_id(id: &[u8; 4]) -> String {
    id.iter()
        .map(|byte| match byte {
            0x20..=0x7e => char::from(*byte).to_string(),
            _ => format!("\\x{byte:02X}"),
        })
        .collect()
}

fn hex_id(id: &[u8; 4]) -> String {
    id.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn push_warning(warnings: &mut Vec<Warning>, code: &str, message: String) {
    if warnings.len() < MAX_WARNINGS - 1 {
        warnings.push(Warning::new(code, message));
    } else if !warnings
        .iter()
        .any(|warning| warning.code == "additional_warnings_omitted")
    {
        warnings.push(Warning::new(
            "additional_warnings_omitted",
            "Additional WAVE metadata warnings were omitted after reaching the bounded warning limit.",
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn wave(format_tag: u16, bits: u16, payload: &[u8]) -> Vec<u8> {
        let channels = 1_u16;
        let block_align = channels * (bits / 8);
        let rate = 48_000_u32;
        let mut fmt = Vec::new();
        fmt.extend_from_slice(&format_tag.to_le_bytes());
        fmt.extend_from_slice(&channels.to_le_bytes());
        fmt.extend_from_slice(&rate.to_le_bytes());
        fmt.extend_from_slice(&(rate * u32::from(block_align)).to_le_bytes());
        fmt.extend_from_slice(&block_align.to_le_bytes());
        fmt.extend_from_slice(&bits.to_le_bytes());
        let riff_size =
            4 + 8 + fmt.len() as u32 + 8 + payload.len() as u32 + (payload.len() as u32 & 1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&riff_size.to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&fmt);
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
        if payload.len() & 1 != 0 {
            bytes.push(0);
        }
        bytes
    }

    fn extensible_wave(container_bits: u16, valid_bits: u16, channels: u16, mask: u32) -> Vec<u8> {
        let block_align = channels * (container_bits / 8);
        let rate = 48_000_u32;
        let mut fmt = Vec::new();
        fmt.extend_from_slice(&0xfffe_u16.to_le_bytes());
        fmt.extend_from_slice(&channels.to_le_bytes());
        fmt.extend_from_slice(&rate.to_le_bytes());
        fmt.extend_from_slice(&(rate * u32::from(block_align)).to_le_bytes());
        fmt.extend_from_slice(&block_align.to_le_bytes());
        fmt.extend_from_slice(&container_bits.to_le_bytes());
        fmt.extend_from_slice(&22_u16.to_le_bytes());
        fmt.extend_from_slice(&valid_bits.to_le_bytes());
        fmt.extend_from_slice(&mask.to_le_bytes());
        fmt.extend_from_slice(PCM_SUBFORMAT_GUID);
        let payload = vec![0; usize::from(block_align)];
        let riff_size = 4 + 8 + fmt.len() as u32 + 8 + payload.len() as u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&riff_size.to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&fmt);
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes
    }

    fn riff_with_chunks(fmt: &[u8], data_chunks: &[&[u8]]) -> Vec<u8> {
        let mut chunks = Vec::new();
        chunks.extend_from_slice(b"fmt ");
        chunks.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
        chunks.extend_from_slice(fmt);
        if fmt.len() & 1 != 0 {
            chunks.push(0);
        }
        for payload in data_chunks {
            chunks.extend_from_slice(b"data");
            chunks.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            chunks.extend_from_slice(payload);
            if payload.len() & 1 != 0 {
                chunks.push(0);
            }
        }
        let riff_size = 4 + chunks.len() as u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&riff_size.to_le_bytes());
        bytes.extend_from_slice(b"WAVE");
        bytes.extend_from_slice(&chunks);
        bytes
    }

    #[test]
    fn parses_supported_integer_pcm_widths() {
        for (bits, payload) in [
            (8, vec![0, 128, 255]),
            (16, vec![0, 0, 0, 128]),
            (24, vec![0, 0, 0, 0, 0, 128]),
            (32, vec![0, 0, 0, 128]),
        ] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("source.wav");
            fs::write(&path, wave(1, bits, &payload)).unwrap();
            let parsed = read(&path).unwrap();
            assert_eq!(parsed.metadata.audio.bits_per_sample, u32::from(bits));
            assert_eq!(
                parsed.metadata.audio.frames,
                (payload.len() / (usize::from(bits) / 8)) as u64
            );
            assert_eq!(parsed.container_bits, u32::from(bits));
        }
    }

    #[test]
    fn rejects_float_and_truncated_riff_explicitly() {
        let directory = tempdir().unwrap();
        let float_path = directory.path().join("float.wav");
        fs::write(&float_path, wave(3, 32, &[0; 4])).unwrap();
        assert!(
            read(&float_path)
                .unwrap_err()
                .to_string()
                .contains("floating-point")
        );

        let truncated_path = directory.path().join("truncated.wav");
        let mut truncated = wave(1, 16, &[0, 0]);
        truncated.pop();
        fs::write(&truncated_path, truncated).unwrap();
        assert!(read(&truncated_path).is_err());
    }

    #[test]
    fn validates_extensible_pcm_mask_and_valid_container_precision() {
        let directory = tempdir().unwrap();
        let valid_path = directory.path().join("extensible.wav");
        fs::write(&valid_path, extensible_wave(16, 16, 2, 0x3)).unwrap();
        let parsed = read(&valid_path).unwrap();
        assert_eq!(parsed.metadata.audio.channels, 2);
        assert_eq!(parsed.metadata.audio.bits_per_sample, 16);

        let invalid_mask_path = directory.path().join("invalid-mask.wav");
        fs::write(&invalid_mask_path, extensible_wave(16, 16, 2, 0x4)).unwrap();
        assert!(
            read(&invalid_mask_path)
                .unwrap_err()
                .to_string()
                .contains("channel mask")
        );

        let mismatched_bits_path = directory.path().join("padded.wav");
        fs::write(&mismatched_bits_path, extensible_wave(32, 24, 2, 0x3)).unwrap();
        assert!(
            read(&mismatched_bits_path)
                .unwrap_err()
                .to_string()
                .contains("differs from its")
        );
    }

    #[test]
    fn rejects_incomplete_data_frames_and_chunk_bounds() {
        let directory = tempdir().unwrap();
        let mut fmt = Vec::new();
        fmt.extend_from_slice(&1_u16.to_le_bytes());
        fmt.extend_from_slice(&2_u16.to_le_bytes());
        fmt.extend_from_slice(&48_000_u32.to_le_bytes());
        fmt.extend_from_slice(&192_000_u32.to_le_bytes());
        fmt.extend_from_slice(&4_u16.to_le_bytes());
        fmt.extend_from_slice(&16_u16.to_le_bytes());
        let split_frame_path = directory.path().join("split-frame.wav");
        fs::write(
            &split_frame_path,
            riff_with_chunks(&fmt, &[&[0, 0], &[0, 0]]),
        )
        .unwrap();
        assert!(
            read(&split_frame_path)
                .unwrap_err()
                .to_string()
                .contains("data chunk 0")
        );

        let out_of_bounds_path = directory.path().join("chunk-out-of-bounds.wav");
        let mut out_of_bounds = wave(1, 16, &[0, 0]);
        out_of_bounds[40..44].copy_from_slice(&4_u32.to_le_bytes());
        fs::write(&out_of_bounds_path, out_of_bounds).unwrap();
        assert!(
            read(&out_of_bounds_path)
                .unwrap_err()
                .to_string()
                .contains("extends beyond RIFF bounds")
        );
    }
}
