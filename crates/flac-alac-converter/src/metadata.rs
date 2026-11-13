//! Bounded FLAC metadata extraction and M4A metadata migration.
//!
//! Audio samples are never read here. The source reader stops after the final
//! FLAC metadata block, and the writer only edits metadata in the already
//! encoded M4A produced by the native codec.

use crate::types::{AudioSpec, Warning};
use anyhow::{Context, Result, ensure};
use mp4ameta::{Data, DataIdent, Fourcc, FreeformIdent, Ident, Tag};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub use crate::types::MetadataReport;

const MAX_ID3V2_TAG_BYTES: u64 = 16 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 32 * 1024 * 1024;
const MAX_METADATA_BLOCKS: usize = 1024;
const MAX_MP4_TOP_LEVEL_ATOMS: usize = 4096;
const MAX_MP4_MOOV_BYTES: u64 = 64 * 1024 * 1024;
const MAX_MP4_OTHER_ATOM_BYTES: u64 = 64 * 1024 * 1024;
const MAX_MP4_TRAILING_NON_MEDIA_BYTES: u64 = 64 * 1024 * 1024;
const MAX_COMMENT_BLOCKS: usize = 64;
const MAX_COMMENT_COUNT: usize = 8192;
const MAX_COMMENT_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_COMMENT_BYTES: usize = 1024 * 1024;
const MAX_COMMENT_KEY_BYTES: usize = 255;
const MAX_PICTURES: usize = 64;
const MAX_PICTURE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PICTURE_TOTAL_BYTES: usize = 24 * 1024 * 1024;
const MAX_WARNINGS: usize = 128;

const AUDIT_MEAN: &str = "org.joey.flac-original";
const AUDIT_RAW_MEAN: &str = "org.joey.flac-original.raw";
const WAV_AUDIT_MEAN: &str = "org.joey.wav-original";
const WAV_AUDIT_RAW_MEAN: &str = "org.joey.wav-original.raw";
const AUDIT_RAW_BLOCK_PREFIX: &str = "FLAC_RAW_BLOCK";
const AUDIT_RAW_COMMENT_PREFIX: &str = "FLAC_RAW_COMMENT";
const AUDIT_RAW_VENDOR_PREFIX: &str = "FLAC_RAW_VENDOR";
const AUDIT_STREAMINFO_NAME: &str = "FLAC_STREAMINFO_RAW";

/// Fully owned source metadata needed for migration and exact read-back checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub kind: SourceKind,
    pub audio: AudioSpec,
    pub comments: Vec<SourceComment>,
    pub comment_blocks: Vec<SourceCommentBlock>,
    pub invalid_comments: Vec<InvalidComment>,
    pub pictures: Vec<FlacPicture>,
    pub opaque_blocks: Vec<OpaqueMetadataBlock>,
    pub id3v2_prefix: Option<Vec<u8>>,
    pub streaminfo: Vec<u8>,
    pub raw_chunks: Vec<RawMetadataChunk>,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceKind {
    Flac,
    Wav,
}

/// A bounded non-audio WAVE chunk retained byte-for-byte in the M4A audit area.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawMetadataChunk {
    pub name: String,
    pub label: String,
    pub bytes: Vec<u8>,
    pub unmapped: bool,
}

/// One valid UTF-8 source comment, preserving its original key spelling/order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceComment {
    pub block_index: usize,
    pub entry_index: usize,
    pub key: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_name: Option<String>,
}

/// Vendor data from one Vorbis Comment block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCommentBlock {
    pub block_index: usize,
    pub vendor: Option<String>,
    pub invalid_vendor: Option<Vec<u8>>,
}

/// A comment entry that could not safely be represented as Unicode metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidComment {
    pub block_index: usize,
    pub entry_index: usize,
    pub raw: Vec<u8>,
    pub reason: String,
}

/// A FLAC picture block, including its original descriptor fields and bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlacPicture {
    pub block_index: usize,
    pub picture_type: u32,
    pub mime_type: String,
    pub description: String,
    pub width: u32,
    pub height: u32,
    pub depth: u32,
    pub colors: u32,
    pub data: Vec<u8>,
}

/// A metadata block whose format is not decoded by this reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpaqueMetadataBlock {
    pub block_index: usize,
    pub block_type: u8,
    pub payload: Vec<u8>,
}

/// Reads native FLAC STREAMINFO, Vorbis comments, pictures, and bounded opaque
/// blocks without invoking a media process or reading audio frames.
pub fn read(path: &Path) -> Result<SourceMetadata> {
    let mut file = File::open(path)
        .with_context(|| format!("open FLAC metadata source {}", path.display()))?;
    let file_metadata = file.metadata().context("stat FLAC metadata source")?;
    ensure!(
        file_metadata.is_file(),
        "FLAC metadata source is not a regular file"
    );
    let file_length = file_metadata.len();

    let id3v2_prefix = seek_to_flac_marker(&mut file, file_length)?;

    let mut total_bytes = 0usize;
    let mut block_index = 0usize;
    let mut saw_streaminfo = false;
    let mut streaminfo = Vec::new();
    let mut audio = None;
    let mut comments = Vec::new();
    let mut comment_blocks = Vec::new();
    let mut invalid_comments = Vec::new();
    let mut pictures = Vec::new();
    let mut opaque_blocks = Vec::new();
    let mut comment_bytes = 0usize;
    let mut picture_bytes = 0usize;
    let mut warnings = Vec::new();
    let mut saw_last = false;

    while !saw_last {
        ensure!(
            block_index < MAX_METADATA_BLOCKS,
            "FLAC metadata block count exceeds limit"
        );
        let mut header = [0u8; 4];
        file.read_exact(&mut header)
            .with_context(|| format!("read FLAC metadata block header {block_index}"))?;
        saw_last = (header[0] & 0x80) != 0;
        let block_type = header[0] & 0x7f;
        let length =
            (usize::from(header[1]) << 16) | (usize::from(header[2]) << 8) | usize::from(header[3]);
        total_bytes = total_bytes
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("FLAC metadata byte count overflow"))?;
        ensure!(
            total_bytes <= MAX_METADATA_BYTES,
            "FLAC metadata exceeds the 32 MiB limit"
        );
        let mut payload = vec![0u8; length];
        file.read_exact(&mut payload)
            .with_context(|| format!("read FLAC metadata block {block_index}"))?;

        match block_type {
            0 => {
                ensure!(
                    !saw_streaminfo && block_index == 0,
                    "FLAC STREAMINFO must be the first and only STREAMINFO block"
                );
                ensure!(
                    payload.len() == 34,
                    "FLAC STREAMINFO block must be exactly 34 bytes"
                );
                let parsed = parse_streaminfo(&payload)?;
                audio = Some(parsed);
                streaminfo = payload;
                saw_streaminfo = true;
            }
            4 => {
                ensure!(
                    comment_blocks.len() < MAX_COMMENT_BLOCKS,
                    "FLAC Vorbis Comment block count exceeds limit"
                );
                match parse_vorbis_comment(
                    &payload,
                    block_index,
                    comments.len().saturating_add(invalid_comments.len()),
                    comment_bytes,
                ) {
                    Ok(parsed) => {
                        comment_bytes = comment_bytes.saturating_add(parsed.text_bytes);
                        comments.extend(parsed.comments);
                        invalid_comments.extend(parsed.invalid_comments);
                        comment_blocks.push(parsed.block);
                    }
                    Err(reason) => {
                        push_warning(
                            &mut warnings,
                            "vorbis_comment_block_opaque",
                            format!(
                                "Vorbis Comment block {block_index} was malformed or exceeded a safe parser limit ({reason}); its bytes are retained as an opaque block."
                            ),
                        );
                        opaque_blocks.push(OpaqueMetadataBlock {
                            block_index,
                            block_type,
                            payload,
                        });
                    }
                }
            }
            6 => {
                ensure!(
                    pictures.len() < MAX_PICTURES,
                    "FLAC picture count exceeds limit"
                );
                match parse_picture(&payload, block_index) {
                    Ok(picture) => {
                        let next_picture_bytes = picture_bytes
                            .checked_add(picture.data.len())
                            .ok_or_else(|| anyhow::anyhow!("FLAC picture byte count overflow"))?;
                        if next_picture_bytes <= MAX_PICTURE_TOTAL_BYTES {
                            picture_bytes = next_picture_bytes;
                            pictures.push(picture);
                        } else {
                            push_warning(
                                &mut warnings,
                                "picture_total_limit_opaque",
                                format!(
                                    "FLAC picture block {block_index} exceeded the 24 MiB decoded-picture budget; its original block is retained opaquely."
                                ),
                            );
                            opaque_blocks.push(OpaqueMetadataBlock {
                                block_index,
                                block_type,
                                payload,
                            });
                        }
                    }
                    Err(reason) => {
                        push_warning(
                            &mut warnings,
                            "picture_block_opaque",
                            format!(
                                "FLAC picture block {block_index} could not be decoded safely ({reason}); its bytes are retained as an opaque block."
                            ),
                        );
                        opaque_blocks.push(OpaqueMetadataBlock {
                            block_index,
                            block_type,
                            payload,
                        });
                    }
                }
            }
            _ => opaque_blocks.push(OpaqueMetadataBlock {
                block_index,
                block_type,
                payload,
            }),
        }
        block_index += 1;
    }

    ensure!(saw_streaminfo, "FLAC source has no STREAMINFO block");
    let audio = audio.context("FLAC source has no parsed STREAMINFO")?;
    Ok(SourceMetadata {
        kind: SourceKind::Flac,
        audio,
        comments,
        comment_blocks,
        invalid_comments,
        pictures,
        opaque_blocks,
        id3v2_prefix,
        streaminfo,
        raw_chunks: Vec::new(),
        warnings,
    })
}

/// Migrates FLAC or WAVE source metadata to an existing encoded M4A and verifies every
/// migrated value by reopening the result through mp4ameta.
pub fn write_and_verify(source: &SourceMetadata, output: &Path) -> Result<MetadataReport> {
    prepare_moov_for_bounded_write(output)?;
    let mut tag = Tag::read_from_path(output)
        .with_context(|| format!("read encoded M4A metadata {}", output.display()))?;
    let mut report = MetadataReport {
        warnings: source.warnings.clone(),
        ..MetadataReport::default()
    };

    clear_our_audit_items(&mut tag);
    clear_standard_items(&mut tag);

    let mut expected_audit = Vec::<(DataIdent, Data)>::new();
    let mut preserved_names = BTreeSet::new();
    let mut unknown_keys = BTreeSet::new();

    if source.kind == SourceKind::Flac {
        for block in &source.comment_blocks {
            let name = format!("FLAC_VORBIS_VENDOR_{:06}", block.block_index);
            if let Some(vendor) = &block.vendor {
                add_audit_data(
                    &mut tag,
                    &mut expected_audit,
                    &name,
                    Data::Utf8(vendor.clone()),
                );
                preserved_names.insert(format!("Vorbis vendor block {}", block.block_index));
            }
            if let Some(raw) = &block.invalid_vendor {
                let raw_name = format!("{AUDIT_RAW_VENDOR_PREFIX}_{:06}", block.block_index);
                add_audit_data(
                    &mut tag,
                    &mut expected_audit,
                    &raw_name,
                    Data::Reserved(raw.clone()),
                );
                preserved_names.insert(format!("raw Vorbis vendor block {}", block.block_index));
                report
                    .unmapped
                    .push(format!("Vorbis vendor block {}", block.block_index));
                report.warnings.push(Warning::new(
                "invalid_vorbis_vendor_preserved",
                format!("Vorbis Comment block {} had a non-UTF-8 vendor string; its original bytes were retained.", block.block_index),
            ));
            }
        }

        if let Some(raw) = &source.id3v2_prefix {
            add_audit_data(
                &mut tag,
                &mut expected_audit,
                "FLAC_ID3V2_PREFIX_RAW",
                Data::Reserved(raw.clone()),
            );
            preserved_names.insert("ID3v2 prefix".to_owned());
            report.unmapped.push("ID3v2 prefix".to_owned());
            report.warnings.push(Warning::new(
            "id3v2_prefix_preserved_opaque",
            "An ID3v2 prefix before the FLAC stream was retained as opaque bytes; its frames were not interpreted or mapped.",
        ));
        }
    }

    for comment in &source.comments {
        let audit_name = comment
            .audit_name
            .as_deref()
            .unwrap_or(comment.key.as_str());
        let ident = source_comment_ident(source.kind, audit_name);
        let data = Data::Utf8(comment.value.clone());
        tag.add_data(ident.clone(), data.clone());
        expected_audit.push((ident, data));
        preserved_names.insert(match source.kind {
            SourceKind::Flac => format!("FLAC comment {}", comment.key),
            SourceKind::Wav => format!("WAV INFO {}", audit_name.trim_start_matches("INFO_")),
        });
        if !is_mapped_key(&comment.key) {
            unknown_keys.insert(match source.kind {
                SourceKind::Flac => comment.key.clone(),
                SourceKind::Wav => format!("WAV INFO {}", audit_name.trim_start_matches("INFO_")),
            });
        }
    }

    if source.kind == SourceKind::Flac {
        for invalid in &source.invalid_comments {
            let name = format!(
                "{AUDIT_RAW_COMMENT_PREFIX}_{:06}_{:06}",
                invalid.block_index, invalid.entry_index
            );
            add_audit_data(
                &mut tag,
                &mut expected_audit,
                &name,
                Data::Reserved(invalid.raw.clone()),
            );
            preserved_names.insert(format!(
                "raw FLAC comment block {} entry {}",
                invalid.block_index, invalid.entry_index
            ));
            report.unmapped.push(format!(
                "FLAC raw comment block {} entry {}",
                invalid.block_index, invalid.entry_index
            ));
            report.warnings.push(Warning::new(
            "invalid_vorbis_comment_preserved",
            format!(
                "FLAC comment block {} entry {} was retained as opaque bytes ({}) and was not mapped to Music fields.",
                invalid.block_index, invalid.entry_index, invalid.reason
            ),
        ));
        }

        add_audit_data(
            &mut tag,
            &mut expected_audit,
            AUDIT_STREAMINFO_NAME,
            Data::Reserved(source.streaminfo.clone()),
        );
        preserved_names.insert("FLAC STREAMINFO".to_owned());

        for block in &source.opaque_blocks {
            let name = format!(
                "{AUDIT_RAW_BLOCK_PREFIX}_{:06}_TYPE_{:02X}",
                block.block_index, block.block_type
            );
            add_audit_data(
                &mut tag,
                &mut expected_audit,
                &name,
                Data::Reserved(block.payload.clone()),
            );
            preserved_names.insert(format!(
                "FLAC metadata block {} type {}",
                block.block_index, block.block_type
            ));
            report.unmapped.push(format!(
                "FLAC metadata block {} type {}",
                block.block_index, block.block_type
            ));
            report.warnings.push(Warning::new(
            "opaque_flac_block_preserved",
            format!(
                "FLAC metadata block {} (type {}) was retained in a namespaced M4A freeform atom.",
                block.block_index, block.block_type
            ),
        ));
        }
    } else {
        for chunk in &source.raw_chunks {
            let ident = raw_audit_ident_for(source.kind, &chunk.name);
            let data = Data::Reserved(chunk.bytes.clone());
            tag.add_data(ident.clone(), data.clone());
            expected_audit.push((ident, data));
            preserved_names.insert(chunk.label.clone());
            if chunk.unmapped {
                report.unmapped.push(chunk.label.clone());
                report.warnings.push(Warning::new(
                    "wav_chunk_preserved_opaque",
                    format!("{} was retained byte-for-byte in a namespaced M4A audit atom; its contents were not mapped to Music fields.", chunk.label),
                ));
            }
        }
    }

    let mut expected_text_atoms = Vec::<(Fourcc, Vec<String>)>::new();
    apply_text_mapping(source, &mut tag, &mut expected_text_atoms, &mut report);
    apply_number_mapping(source, &mut tag, &mut report);
    apply_picture_mapping(
        source,
        &mut tag,
        &mut report,
        &mut preserved_names,
        &mut expected_audit,
    )?;

    report.preserved.extend(preserved_names);
    report.unmapped.extend(unknown_keys);
    report.preserved.sort();
    report.unmapped.sort();
    report.mapped.sort();
    report.mapped.dedup();
    report
        .warnings
        .sort_by(|a, b| a.code.cmp(&b.code).then(a.message.cmp(&b.message)));
    cap_warnings(&mut report.warnings);

    tag.write_to_path(output)
        .with_context(|| format!("write migrated M4A metadata {}", output.display()))?;

    let verified = Tag::read_from_path(output)
        .with_context(|| format!("read back migrated M4A metadata {}", output.display()))?;
    verify_audit_data(&verified, &expected_audit)?;
    verify_text_atoms(&verified, &expected_text_atoms)?;
    verify_number_mapping(source, &verified)?;
    verify_picture_mapping(source, &verified)?;

    Ok(report)
}

#[derive(Clone, Copy, Debug)]
struct Mp4Atom {
    kind: [u8; 4],
    start: u64,
    end: u64,
    header_size: u64,
    size_to_eof: bool,
}

impl Mp4Atom {
    fn len(self) -> u64 {
        self.end - self.start
    }
}

#[derive(Debug)]
struct Mp4Layout {
    file_length: u64,
    mdat: Mp4Atom,
    moov: Mp4Atom,
    trailing_non_media_bytes: u64,
}

/// Relocates a bounded `moov` atom to EOF before mp4ameta rewrites metadata.
/// AudioToolbox commonly writes `moov` before `mdat`; mp4ameta buffers bytes
/// after changed atoms through EOF, so leaving that layout would buffer audio.
fn prepare_moov_for_bounded_write(path: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open encoded M4A for metadata rewrite {}", path.display()))?;
    let metadata = file.metadata().context("stat encoded M4A before rewrite")?;
    ensure!(metadata.is_file(), "encoded M4A is not a regular file");
    let layout = scan_mp4_layout(&mut file, metadata.len())?;

    if layout.moov.end == layout.file_length && layout.mdat.end <= layout.moov.start {
        return Ok(());
    }

    let moov_len = layout.moov.len();
    ensure!(
        layout
            .trailing_non_media_bytes
            .checked_add(moov_len)
            .is_some_and(|total| total <= MAX_MP4_TRAILING_NON_MEDIA_BYTES),
        "encoded M4A has too much non-media data after mdat to relocate moov safely"
    );
    let explicit_mdat_size = if layout.mdat.size_to_eof {
        ensure!(
            layout.mdat.len() <= u64::from(u32::MAX),
            "zero-sized mdat exceeds the 32-bit size limit and cannot be relocated safely"
        );
        Some(
            u32::try_from(layout.mdat.len())
                .context("zero-sized M4A mdat exceeds a 32-bit atom size")?,
        )
    } else {
        None
    };

    // The only atom copied into memory is the moov, capped by scan_mp4_layout.
    let moov_capacity =
        usize::try_from(moov_len).context("encoded M4A moov size exceeds addressable memory")?;
    let mut moov_bytes = Vec::new();
    moov_bytes
        .try_reserve_exact(moov_capacity)
        .context("reserve bounded buffer for encoded M4A moov")?;
    moov_bytes.resize(moov_capacity, 0);
    file.seek(SeekFrom::Start(layout.moov.start))
        .context("seek to encoded M4A moov")?;
    file.read_exact(&mut moov_bytes)
        .context("read bounded encoded M4A moov")?;

    ensure!(
        file.seek(SeekFrom::End(0))
            .context("seek to encoded M4A end")?
            == layout.file_length,
        "encoded M4A length changed during metadata preparation"
    );
    file.write_all(&moov_bytes)
        .context("append bounded M4A moov for metadata rewrite")?;
    file.flush().context("flush relocated M4A moov")?;

    if let Some(mdat_size) = explicit_mdat_size {
        file.seek(SeekFrom::Start(layout.mdat.start))
            .context("seek to zero-sized M4A mdat header")?;
        file.write_all(&mdat_size.to_be_bytes())
            .context("set explicit size on relocated M4A mdat")?;
    }
    let old_moov_type = layout
        .moov
        .start
        .checked_add(4)
        .ok_or_else(|| anyhow::anyhow!("original M4A moov type offset overflow"))?;
    file.seek(SeekFrom::Start(old_moov_type))
        .context("seek to original M4A moov type")?;
    file.write_all(b"free")
        .context("replace original M4A moov with equal-sized free atom")?;
    file.flush().context("flush M4A metadata preparation")?;
    file.sync_all().context("sync M4A metadata preparation")?;

    let new_file_length = layout
        .file_length
        .checked_add(moov_len)
        .ok_or_else(|| anyhow::anyhow!("relocated M4A size overflow"))?;
    let relocated = scan_mp4_layout(&mut file, new_file_length)?;
    ensure!(
        relocated.moov.end == new_file_length && relocated.mdat.end <= relocated.moov.start,
        "relocated M4A does not have mdat before a tail moov"
    );
    Ok(())
}

fn scan_mp4_layout(reader: &mut (impl Read + Seek), file_length: u64) -> Result<Mp4Layout> {
    let mut atoms = Vec::new();
    let mut offset = 0u64;
    while offset < file_length {
        ensure!(
            atoms.len() < MAX_MP4_TOP_LEVEL_ATOMS,
            "encoded M4A has too many top-level atoms"
        );
        let atom = read_mp4_atom(reader, offset, file_length, true)?;
        if atoms.is_empty() {
            ensure!(
                atom.kind == *b"ftyp",
                "encoded M4A does not start with ftyp"
            );
        }
        ensure!(
            atom.kind != *b"moof"
                && atom.kind != *b"mfra"
                && atom.kind != *b"sidx"
                && atom.kind != *b"ssix",
            "fragmented M4A layouts are unsupported for metadata migration"
        );
        if atom.kind == *b"moov" {
            ensure!(
                atom.len() <= MAX_MP4_MOOV_BYTES,
                "encoded M4A moov exceeds the 64 MiB limit"
            );
            validate_moov_is_unfragmented(reader, atom)?;
        } else if atom.kind != *b"mdat" {
            ensure!(
                atom.len() <= MAX_MP4_OTHER_ATOM_BYTES,
                "encoded M4A has an oversized non-media top-level atom"
            );
        }
        offset = atom.end;
        atoms.push(atom);
    }
    ensure!(
        offset == file_length,
        "encoded M4A atom walk did not reach EOF"
    );

    let mut mdat = None;
    let mut moov = None;
    for atom in &atoms {
        if atom.kind == *b"mdat" {
            ensure!(
                mdat.replace(*atom).is_none(),
                "multiple M4A mdat atoms are unsupported"
            );
        } else if atom.kind == *b"moov" {
            ensure!(
                moov.replace(*atom).is_none(),
                "multiple M4A moov atoms are unsupported"
            );
        }
    }
    let mdat = mdat.context("encoded M4A is missing its mdat atom")?;
    let moov = moov.context("encoded M4A is missing its moov atom")?;
    ensure!(
        mdat.end <= moov.start || moov.end <= mdat.start,
        "encoded M4A mdat and moov atoms overlap"
    );

    let mut trailing_non_media_bytes = 0u64;
    for atom in atoms.iter().filter(|atom| atom.start >= mdat.end) {
        if atom.kind != *b"mdat" {
            trailing_non_media_bytes = trailing_non_media_bytes
                .checked_add(atom.len())
                .ok_or_else(|| anyhow::anyhow!("encoded M4A trailing atom size overflow"))?;
        }
    }
    ensure!(
        trailing_non_media_bytes <= MAX_MP4_TRAILING_NON_MEDIA_BYTES,
        "encoded M4A has too much non-media data after mdat"
    );

    Ok(Mp4Layout {
        file_length,
        mdat,
        moov,
        trailing_non_media_bytes,
    })
}

fn read_mp4_atom(
    reader: &mut (impl Read + Seek),
    start: u64,
    parent_end: u64,
    allow_size_to_eof: bool,
) -> Result<Mp4Atom> {
    ensure!(start < parent_end, "empty encoded M4A atom range");
    ensure!(
        parent_end - start >= 8,
        "encoded M4A atom is shorter than its header"
    );
    reader
        .seek(SeekFrom::Start(start))
        .context("seek to encoded M4A atom header")?;
    let mut header = [0u8; 8];
    reader
        .read_exact(&mut header)
        .context("read encoded M4A atom header")?;
    let size32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    let kind = [header[4], header[5], header[6], header[7]];
    let (atom_len, header_size, size_to_eof) = match size32 {
        0 => {
            ensure!(
                allow_size_to_eof && kind == *b"mdat",
                "zero-sized M4A atoms are only supported for top-level mdat"
            );
            (parent_end - start, 8, true)
        }
        1 => {
            ensure!(
                parent_end - start >= 16,
                "extended-size M4A atom is shorter than its header"
            );
            let mut extended = [0u8; 8];
            reader
                .read_exact(&mut extended)
                .context("read extended M4A atom size")?;
            (u64::from_be_bytes(extended), 16, false)
        }
        size => (u64::from(size), 8, false),
    };
    ensure!(
        atom_len >= header_size,
        "M4A atom is smaller than its header"
    );
    let end = start
        .checked_add(atom_len)
        .ok_or_else(|| anyhow::anyhow!("encoded M4A atom offset overflow"))?;
    ensure!(
        end <= parent_end,
        "encoded M4A atom extends beyond its parent"
    );
    Ok(Mp4Atom {
        kind,
        start,
        end,
        header_size,
        size_to_eof,
    })
}

fn validate_moov_is_unfragmented(reader: &mut (impl Read + Seek), moov: Mp4Atom) -> Result<()> {
    let mut offset = moov
        .start
        .checked_add(moov.header_size)
        .ok_or_else(|| anyhow::anyhow!("M4A moov child offset overflow"))?;
    while offset < moov.end {
        let child = read_mp4_atom(reader, offset, moov.end, false)?;
        ensure!(child.kind != *b"mvex", "fragmented M4A moov contains mvex");
        offset = child.end;
    }
    ensure!(
        offset == moov.end,
        "M4A moov child walk did not reach its end"
    );
    Ok(())
}

fn seek_to_flac_marker(file: &mut File, file_length: u64) -> Result<Option<Vec<u8>>> {
    file.seek(SeekFrom::Start(0))
        .context("seek to FLAC source start")?;
    let mut first = [0u8; 3];
    file.read_exact(&mut first)
        .context("read FLAC source prefix")?;
    let id3v2_prefix = if &first == b"ID3" {
        let mut rest = [0u8; 7];
        file.read_exact(&mut rest)
            .context("read ID3v2 header before FLAC stream")?;
        let version = rest[0];
        let flags = rest[2];
        let has_footer = flags & 0x10 != 0;
        ensure!(
            !has_footer || version == 4,
            "ID3v2 footer flag is only valid for version 2.4"
        );
        let size_bytes = &rest[3..7];
        ensure!(
            size_bytes.iter().all(|b| b & 0x80 == 0),
            "ID3v2 size uses invalid synchsafe bytes"
        );
        let tag_size = (u64::from(size_bytes[0]) << 21)
            | (u64::from(size_bytes[1]) << 14)
            | (u64::from(size_bytes[2]) << 7)
            | u64::from(size_bytes[3]);
        ensure!(
            tag_size <= MAX_ID3V2_TAG_BYTES,
            "ID3v2 prefix exceeds the 16 MiB limit"
        );
        // ID3v2.4's synchsafe size excludes both the ten-byte header and
        // optional ten-byte footer, so include the footer in the FLAC offset.
        let footer_len = if has_footer { 10u64 } else { 0 };
        let tag_end = 10u64
            .checked_add(tag_size)
            .ok_or_else(|| anyhow::anyhow!("ID3v2 prefix offset overflow"))?;
        let offset = tag_end
            .checked_add(footer_len)
            .ok_or_else(|| anyhow::anyhow!("ID3v2 footer offset overflow"))?;
        ensure!(
            offset.checked_add(4).is_some_and(|end| end <= file_length),
            "ID3v2 prefix extends beyond the file"
        );
        let total_len =
            usize::try_from(offset).context("ID3v2 prefix size exceeds addressable memory")?;
        let mut raw = Vec::with_capacity(total_len);
        raw.extend_from_slice(&first);
        raw.extend_from_slice(&rest);
        raw.resize(total_len, 0);
        file.read_exact(&mut raw[10..])
            .context("read bounded ID3v2 prefix")?;
        if has_footer {
            let footer_start = usize::try_from(tag_end)
                .context("ID3v2 footer offset exceeds addressable memory")?;
            let footer_end = footer_start
                .checked_add(10)
                .ok_or_else(|| anyhow::anyhow!("ID3v2 footer range overflow"))?;
            let footer = raw
                .get(footer_start..footer_end)
                .context("ID3v2 footer is outside the declared tag")?;
            ensure!(
                &footer[..3] == b"3DI" && footer[3..] == raw[3..10],
                "ID3v2.4 footer does not match its header"
            );
        }
        file.seek(SeekFrom::Start(offset))
            .context("skip ID3v2 prefix")?;
        Some(raw)
    } else {
        file.seek(SeekFrom::Start(0))
            .context("rewind FLAC source")?;
        None
    };

    let mut marker = [0u8; 4];
    file.read_exact(&mut marker)
        .context("read FLAC stream marker")?;
    ensure!(&marker == b"fLaC", "source is not a native FLAC stream");
    Ok(id3v2_prefix)
}

fn parse_streaminfo(payload: &[u8]) -> Result<AudioSpec> {
    ensure!(
        payload.len() == 34,
        "FLAC STREAMINFO payload must be 34 bytes"
    );
    let packed = u64::from_be_bytes(payload[10..18].try_into().expect("fixed STREAMINFO field"));
    let sample_rate = ((packed >> 44) & 0x000f_ffff) as u32;
    let channels = (((packed >> 41) & 0x7) + 1) as u32;
    let bits_per_sample = (((packed >> 36) & 0x1f) + 1) as u32;
    let frames = packed & 0x0f_ffff_ffff;
    ensure!(sample_rate > 0, "FLAC STREAMINFO sample rate is zero");
    ensure!(
        (1..=8).contains(&channels),
        "FLAC STREAMINFO channel count is outside 1..=8"
    );
    ensure!(
        (4..=32).contains(&bits_per_sample),
        "FLAC STREAMINFO bit depth is outside the supported 4..=32 range"
    );
    Ok(AudioSpec {
        sample_rate,
        channels,
        bits_per_sample,
        frames,
    })
}

struct ParsedCommentBlock {
    block: SourceCommentBlock,
    comments: Vec<SourceComment>,
    invalid_comments: Vec<InvalidComment>,
    text_bytes: usize,
}

fn parse_vorbis_comment(
    payload: &[u8],
    block_index: usize,
    existing_count: usize,
    existing_bytes: usize,
) -> std::result::Result<ParsedCommentBlock, &'static str> {
    let mut cursor = SliceCursor::new(payload);
    let vendor_len = cursor.read_u32_le().ok_or("missing vendor length")? as usize;
    if vendor_len > MAX_COMMENT_BYTES {
        return Err("vendor string exceeds 1 MiB");
    }
    let vendor_bytes = cursor.take(vendor_len).ok_or("truncated vendor string")?;
    let (vendor, invalid_vendor) = match std::str::from_utf8(vendor_bytes) {
        Ok(value) => (Some(value.to_owned()), None),
        Err(_) => (None, Some(vendor_bytes.to_vec())),
    };
    let count = cursor.read_u32_le().ok_or("missing comment count")? as usize;
    if count > MAX_COMMENT_COUNT || existing_count.saturating_add(count) > MAX_COMMENT_COUNT {
        return Err("comment count exceeds limit");
    }
    let mut comments = Vec::with_capacity(count.min(1024));
    let mut invalid_comments = Vec::new();
    let mut text_bytes = vendor_len;
    for entry_index in 0..count {
        let entry_len = cursor.read_u32_le().ok_or("missing comment entry length")? as usize;
        if entry_len > MAX_COMMENT_BYTES {
            return Err("comment entry exceeds 1 MiB");
        }
        text_bytes = text_bytes
            .checked_add(entry_len)
            .ok_or("comment byte count overflow")?;
        if existing_bytes.saturating_add(text_bytes) > MAX_COMMENT_TOTAL_BYTES {
            return Err("total comment bytes exceed 8 MiB");
        }
        let raw = cursor.take(entry_len).ok_or("truncated comment entry")?;
        let Some(eq_index) = raw.iter().position(|byte| *byte == b'=') else {
            invalid_comments.push(InvalidComment {
                block_index,
                entry_index,
                raw: raw.to_vec(),
                reason: "entry has no key/value separator".to_owned(),
            });
            continue;
        };
        let key_bytes = &raw[..eq_index];
        let value_bytes = &raw[eq_index + 1..];
        if key_bytes.is_empty()
            || key_bytes.len() > MAX_COMMENT_KEY_BYTES
            || !key_bytes
                .iter()
                .all(|byte| (0x20..=0x7d).contains(byte) && *byte != b'=')
        {
            invalid_comments.push(InvalidComment {
                block_index,
                entry_index,
                raw: raw.to_vec(),
                reason: "key is not a bounded Vorbis ASCII key".to_owned(),
            });
            continue;
        }
        let key = std::str::from_utf8(key_bytes)
            .expect("validated Vorbis keys are ASCII")
            .to_owned();
        match std::str::from_utf8(value_bytes) {
            Ok(value) => comments.push(SourceComment {
                block_index,
                entry_index,
                key,
                value: value.to_owned(),
                audit_name: None,
            }),
            Err(_) => invalid_comments.push(InvalidComment {
                block_index,
                entry_index,
                raw: raw.to_vec(),
                reason: "value is not valid UTF-8".to_owned(),
            }),
        }
    }
    if !cursor.is_empty() {
        return Err("trailing bytes after comment entries");
    }
    Ok(ParsedCommentBlock {
        block: SourceCommentBlock {
            block_index,
            vendor,
            invalid_vendor,
        },
        comments,
        invalid_comments,
        text_bytes,
    })
}

fn parse_picture(
    payload: &[u8],
    block_index: usize,
) -> std::result::Result<FlacPicture, &'static str> {
    let mut cursor = SliceCursor::new(payload);
    let picture_type = cursor.read_u32_be().ok_or("missing picture type")?;
    let mime =
        read_be_len_bytes(&mut cursor, MAX_COMMENT_BYTES).ok_or("invalid picture MIME type")?;
    let mime_type = std::str::from_utf8(mime)
        .map_err(|_| "picture MIME type is not UTF-8")?
        .to_owned();
    let description =
        read_be_len_bytes(&mut cursor, MAX_COMMENT_BYTES).ok_or("invalid picture description")?;
    let description = std::str::from_utf8(description)
        .map_err(|_| "picture description is not UTF-8")?
        .to_owned();
    let width = cursor.read_u32_be().ok_or("missing picture width")?;
    let height = cursor.read_u32_be().ok_or("missing picture height")?;
    let depth = cursor.read_u32_be().ok_or("missing picture depth")?;
    let colors = cursor.read_u32_be().ok_or("missing picture color count")?;
    let data =
        read_be_len_bytes(&mut cursor, MAX_PICTURE_BYTES).ok_or("invalid picture data length")?;
    if !cursor.is_empty() {
        return Err("trailing bytes after picture data");
    }
    Ok(FlacPicture {
        block_index,
        picture_type,
        mime_type,
        description,
        width,
        height,
        depth,
        colors,
        data: data.to_vec(),
    })
}

fn read_be_len_bytes<'a>(cursor: &mut SliceCursor<'a>, max: usize) -> Option<&'a [u8]> {
    let length = cursor.read_u32_be()? as usize;
    if length > max {
        return None;
    }
    cursor.take(length)
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.position.checked_add(length)?;
        let value = self.bytes.get(self.position..end)?;
        self.position = end;
        Some(value)
    }
    fn read_u32_le(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn read_u32_be(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn add_audit_data(tag: &mut Tag, expected: &mut Vec<(DataIdent, Data)>, name: &str, data: Data) {
    let ident = raw_audit_ident(name);
    tag.add_data(ident.clone(), data.clone());
    expected.push((ident, data));
}

fn source_comment_ident(kind: SourceKind, key: &str) -> DataIdent {
    let mean = match kind {
        SourceKind::Flac => AUDIT_MEAN,
        SourceKind::Wav => WAV_AUDIT_MEAN,
    };
    FreeformIdent::new_borrowed(mean, key).into()
}

fn raw_audit_ident(name: &str) -> DataIdent {
    raw_audit_ident_for(SourceKind::Flac, name)
}

fn raw_audit_ident_for(kind: SourceKind, name: &str) -> DataIdent {
    let mean = match kind {
        SourceKind::Flac => AUDIT_RAW_MEAN,
        SourceKind::Wav => WAV_AUDIT_RAW_MEAN,
    };
    DataIdent::freeform(mean, name.to_owned())
}

fn clear_our_audit_items(tag: &mut Tag) {
    let mut stale = Vec::<DataIdent>::new();
    for (ident, _) in tag.data() {
        if ident.freeform().is_some_and(|freeform| {
            freeform.mean == AUDIT_MEAN
                || freeform.mean == AUDIT_RAW_MEAN
                || freeform.mean == WAV_AUDIT_MEAN
                || freeform.mean == WAV_AUDIT_RAW_MEAN
        }) && !stale.iter().any(|known| known == ident)
        {
            stale.push(ident.clone());
        }
    }
    for ident in stale {
        tag.remove_data_of(&ident);
    }
}

fn clear_standard_items(tag: &mut Tag) {
    const IDS: [[u8; 4]; 17] = [
        *b"\xa9nam",
        *b"\xa9ART",
        *b"aART",
        *b"\xa9alb",
        *b"\xa9wrt",
        *b"\xa9lyr",
        *b"\xa9day",
        *b"\xa9gen",
        *b"\xa9cmt",
        *b"\xa9grp",
        *b"trkn",
        *b"disk",
        *b"soar",
        *b"soaa",
        *b"soco",
        *b"sonm",
        *b"soal",
    ];
    for bytes in IDS {
        let ident = Fourcc(bytes);
        tag.remove_data_of(&ident);
    }
}

fn apply_text_mapping(
    source: &SourceMetadata,
    tag: &mut Tag,
    expected: &mut Vec<(Fourcc, Vec<String>)>,
    report: &mut MetadataReport,
) {
    map_text(
        source,
        tag,
        expected,
        report,
        "title",
        &["TITLE"],
        Fourcc(*b"\xa9nam"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "artist",
        &["ARTIST", "ARTISTS"],
        Fourcc(*b"\xa9ART"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "album artist",
        &["ALBUMARTIST", "ALBUM ARTIST", "ALBUM_ARTIST"],
        Fourcc(*b"aART"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "album",
        &["ALBUM"],
        Fourcc(*b"\xa9alb"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "composer",
        &["COMPOSER", "COMPOSERS"],
        Fourcc(*b"\xa9wrt"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "lyrics",
        &["LYRICS", "UNSYNCEDLYRICS", "UNSYNCED LYRICS"],
        Fourcc(*b"\xa9lyr"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "date",
        &["DATE", "YEAR"],
        Fourcc(*b"\xa9day"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "genre",
        &["GENRE", "GENRES"],
        Fourcc(*b"\xa9gen"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "comment",
        &["COMMENT", "DESCRIPTION"],
        Fourcc(*b"\xa9cmt"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "grouping",
        &["GROUPING", "CONTENTGROUP"],
        Fourcc(*b"\xa9grp"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "title sort order",
        &[
            "TITLESORT",
            "TITLESORTORDER",
            "TITLE_SORT",
            "TITLE_SORT_ORDER",
        ],
        Fourcc(*b"sonm"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "artist sort order",
        &[
            "ARTISTSORT",
            "ARTISTSORTORDER",
            "ARTIST_SORT",
            "ARTIST_SORT_ORDER",
        ],
        Fourcc(*b"soar"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "album artist sort order",
        &[
            "ALBUMARTISTSORT",
            "ALBUMARTISTSORTORDER",
            "ALBUMARTIST_SORT",
            "ALBUM_ARTIST_SORT",
            "ALBUMARTIST_SORT_ORDER",
            "ALBUM_ARTIST_SORT_ORDER",
        ],
        Fourcc(*b"soaa"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "composer sort order",
        &[
            "COMPOSERSORT",
            "COMPOSERSORTORDER",
            "COMPOSER_SORT",
            "COMPOSER_SORT_ORDER",
        ],
        Fourcc(*b"soco"),
    );
    map_text(
        source,
        tag,
        expected,
        report,
        "album sort order",
        &[
            "ALBUMSORT",
            "ALBUMSORTORDER",
            "ALBUM_SORT",
            "ALBUM_SORT_ORDER",
        ],
        Fourcc(*b"soal"),
    );
}

fn map_text(
    source: &SourceMetadata,
    tag: &mut Tag,
    expected: &mut Vec<(Fourcc, Vec<String>)>,
    report: &mut MetadataReport,
    output_name: &str,
    aliases: &[&str],
    ident: Fourcc,
) {
    let values = comment_values(source, aliases);
    if values.is_empty() {
        return;
    }
    let mut written = Vec::with_capacity(values.len());
    for value in values {
        let data = Data::Utf8(value.clone());
        tag.add_data(ident, data);
        written.push(value);
    }
    expected.push((ident, written));
    report.mapped.push(output_name.to_owned());
}

fn apply_number_mapping(source: &SourceMetadata, tag: &mut Tag, report: &mut MetadataReport) {
    let track_values = comment_values(source, &["TRACKNUMBER", "TRACK"]);
    let total_track_values = comment_values(source, &["TRACKTOTAL", "TOTALTRACKS", "TOTALTRACK"]);
    let (track_number, embedded_total) = first_number_pair(&track_values);
    let total_tracks = first_number(&total_track_values).or(embedded_total);
    if let Some(number) = track_number {
        if let Some(total) = total_tracks {
            tag.set_track(number, total);
        } else {
            tag.set_track_number(number);
        }
        report.mapped.push("track number".to_owned());
        if total_tracks.is_some() {
            report.mapped.push("total tracks".to_owned());
        }
    } else if !track_values.is_empty() {
        push_warning(&mut report.warnings, "track_number_not_numeric", "A FLAC track number could not be represented in the M4A track atom; original values remain in the audit atoms.".to_owned());
    }
    if track_number.is_none()
        && let Some(total) = first_number(&total_track_values)
    {
        tag.set_total_tracks(total);
        report.mapped.push("total tracks".to_owned());
    }

    let disc_values = comment_values(source, &["DISCNUMBER", "DISC"]);
    let total_disc_values = comment_values(source, &["DISCTOTAL", "TOTALDISCS", "TOTALDISC"]);
    let (disc_number, embedded_total) = first_number_pair(&disc_values);
    let total_discs = first_number(&total_disc_values).or(embedded_total);
    if let Some(number) = disc_number {
        if let Some(total) = total_discs {
            tag.set_disc(number, total);
        } else {
            tag.set_disc_number(number);
        }
        report.mapped.push("disc number".to_owned());
        if total_discs.is_some() {
            report.mapped.push("total discs".to_owned());
        }
    } else if !disc_values.is_empty() {
        push_warning(&mut report.warnings, "disc_number_not_numeric", "A FLAC disc number could not be represented in the M4A disc atom; original values remain in the audit atoms.".to_owned());
    }
    if disc_number.is_none()
        && let Some(total) = first_number(&total_disc_values)
    {
        tag.set_total_discs(total);
        report.mapped.push("total discs".to_owned());
    }
}

fn apply_picture_mapping(
    source: &SourceMetadata,
    tag: &mut Tag,
    report: &mut MetadataReport,
    preserved: &mut BTreeSet<String>,
    expected_audit: &mut Vec<(DataIdent, Data)>,
) -> Result<()> {
    let cover = Fourcc(*b"covr");
    for picture in &source.pictures {
        let raw = encode_picture(picture)?;
        let name = format!("FLAC_PICTURE_BLOCK_{:06}", picture.block_index);
        let ident = raw_audit_ident(&name);
        let raw_data = Data::Reserved(raw);
        tag.add_data(ident.clone(), raw_data.clone());
        expected_audit.push((ident, raw_data));
        preserved.insert(format!("FLAC picture block {}", picture.block_index));

        let is_front_cover = picture.picture_type == 3;
        let supported_data = supported_picture_data(picture);
        if is_front_cover {
            if let Some(data) = supported_data {
                let atom_data = match picture.mime_type.to_ascii_lowercase().as_str() {
                    "image/jpeg" => Data::Jpeg(data.to_vec()),
                    "image/png" => Data::Png(data.to_vec()),
                    _ => unreachable!("supported_picture_data checks the MIME type"),
                };
                tag.add_data(cover, atom_data);
                report.mapped.push("front cover artwork".to_owned());
            } else {
                report
                    .unmapped
                    .push(format!("FLAC picture block {}", picture.block_index));
                push_warning(
                    &mut report.warnings,
                    "picture_not_mapped_to_covr",
                    format!(
                        "FLAC picture block {} is not a validated JPEG or PNG front cover; its complete block remains in the audit atom.",
                        picture.block_index
                    ),
                );
            }
        } else {
            report
                .unmapped
                .push(format!("FLAC picture block {}", picture.block_index));
        }
    }
    Ok(())
}

fn supported_picture_data(picture: &FlacPicture) -> Option<&[u8]> {
    if picture.picture_type != 3 {
        return None;
    }
    match picture.mime_type.to_ascii_lowercase().as_str() {
        "image/jpeg" if picture.data.starts_with(&[0xff, 0xd8, 0xff]) => Some(&picture.data),
        "image/png" if picture.data.starts_with(b"\x89PNG\r\n\x1a\n") => Some(&picture.data),
        _ => None,
    }
}

fn encode_picture(picture: &FlacPicture) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(
        picture.data.len() + picture.mime_type.len() + picture.description.len() + 32,
    );
    payload.extend_from_slice(&picture.picture_type.to_be_bytes());
    append_be_len_bytes(&mut payload, picture.mime_type.as_bytes())?;
    append_be_len_bytes(&mut payload, picture.description.as_bytes())?;
    payload.extend_from_slice(&picture.width.to_be_bytes());
    payload.extend_from_slice(&picture.height.to_be_bytes());
    payload.extend_from_slice(&picture.depth.to_be_bytes());
    payload.extend_from_slice(&picture.colors.to_be_bytes());
    append_be_len_bytes(&mut payload, &picture.data)?;
    Ok(payload)
}

fn append_be_len_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let length = u32::try_from(bytes.len()).context("FLAC metadata field is too long to encode")?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn comment_values(source: &SourceMetadata, aliases: &[&str]) -> Vec<String> {
    source
        .comments
        .iter()
        .filter(|comment| {
            aliases
                .iter()
                .any(|alias| comment.key.eq_ignore_ascii_case(alias))
        })
        .map(|comment| comment.value.clone())
        .collect()
}

fn is_mapped_key(key: &str) -> bool {
    const ALIASES: &[&str] = &[
        "TITLE",
        "ARTIST",
        "ARTISTS",
        "ALBUMARTIST",
        "ALBUM ARTIST",
        "ALBUM_ARTIST",
        "ALBUM",
        "COMPOSER",
        "COMPOSERS",
        "LYRICS",
        "UNSYNCEDLYRICS",
        "UNSYNCED LYRICS",
        "DATE",
        "YEAR",
        "GENRE",
        "GENRES",
        "COMMENT",
        "DESCRIPTION",
        "GROUPING",
        "CONTENTGROUP",
        "TITLESORT",
        "TITLESORTORDER",
        "TITLE_SORT",
        "TITLE_SORT_ORDER",
        "ARTISTSORT",
        "ARTISTSORTORDER",
        "ARTIST_SORT",
        "ARTIST_SORT_ORDER",
        "ALBUMARTISTSORT",
        "ALBUMARTISTSORTORDER",
        "ALBUMARTIST_SORT",
        "ALBUM_ARTIST_SORT",
        "ALBUMARTIST_SORT_ORDER",
        "ALBUM_ARTIST_SORT_ORDER",
        "COMPOSERSORT",
        "COMPOSERSORTORDER",
        "COMPOSER_SORT",
        "COMPOSER_SORT_ORDER",
        "ALBUMSORT",
        "ALBUMSORTORDER",
        "ALBUM_SORT",
        "ALBUM_SORT_ORDER",
        "TRACKNUMBER",
        "TRACK",
        "TRACKTOTAL",
        "TOTALTRACKS",
        "TOTALTRACK",
        "DISCNUMBER",
        "DISC",
        "DISCTOTAL",
        "TOTALDISCS",
        "TOTALDISC",
    ];
    ALIASES.iter().any(|alias| key.eq_ignore_ascii_case(alias))
}

fn first_number_pair(values: &[String]) -> (Option<u16>, Option<u16>) {
    for value in values {
        let (number, total) = match value.split_once('/') {
            Some((number, total)) => (parse_u16(number), parse_u16(total)),
            None => (parse_u16(value), None),
        };
        if let Some(number) = number {
            return (Some(number), total);
        }
    }
    (None, None)
}

fn first_number(values: &[String]) -> Option<u16> {
    values.iter().find_map(|value| parse_u16(value))
}

fn parse_u16(value: &str) -> Option<u16> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn verify_audit_data(tag: &Tag, expected: &[(DataIdent, Data)]) -> Result<()> {
    let mut grouped = Vec::<(DataIdent, Vec<Data>)>::new();
    for (ident, data) in expected {
        if let Some((_, values)) = grouped.iter_mut().find(|(existing, _)| existing == ident) {
            values.push(data.clone());
        } else {
            grouped.push((ident.clone(), vec![data.clone()]));
        }
    }
    for (ident, values) in grouped {
        let actual = tag.data_of(&ident).cloned().collect::<Vec<_>>();
        ensure!(
            actual == values,
            "M4A freeform audit read-back mismatch for {ident}"
        );
    }
    Ok(())
}

fn verify_text_atoms(tag: &Tag, expected: &[(Fourcc, Vec<String>)]) -> Result<()> {
    for (ident, values) in expected {
        let actual = tag.strings_of(ident).map(str::to_owned).collect::<Vec<_>>();
        ensure!(
            actual.as_slice() == values.as_slice(),
            "M4A standard text metadata read-back mismatch for {ident:?}"
        );
    }
    Ok(())
}

fn verify_number_mapping(source: &SourceMetadata, tag: &Tag) -> Result<()> {
    let track_values = comment_values(source, &["TRACKNUMBER", "TRACK"]);
    let total_track_values = comment_values(source, &["TRACKTOTAL", "TOTALTRACKS", "TOTALTRACK"]);
    let (track, embedded_track_total) = first_number_pair(&track_values);
    let total_tracks = first_number(&total_track_values).or(embedded_track_total);
    if let Some(track) = track {
        ensure!(
            tag.track_number() == Some(track),
            "M4A track number read-back mismatch"
        );
        if let Some(total) = total_tracks {
            ensure!(
                tag.total_tracks() == Some(total),
                "M4A total track count read-back mismatch"
            );
        }
    } else if let Some(total) = first_number(&total_track_values) {
        ensure!(
            tag.total_tracks() == Some(total),
            "M4A total track count read-back mismatch"
        );
    }

    let disc_values = comment_values(source, &["DISCNUMBER", "DISC"]);
    let total_disc_values = comment_values(source, &["DISCTOTAL", "TOTALDISCS", "TOTALDISC"]);
    let (disc, embedded_disc_total) = first_number_pair(&disc_values);
    let total_discs = first_number(&total_disc_values).or(embedded_disc_total);
    if let Some(disc) = disc {
        ensure!(
            tag.disc_number() == Some(disc),
            "M4A disc number read-back mismatch"
        );
        if let Some(total) = total_discs {
            ensure!(
                tag.total_discs() == Some(total),
                "M4A total disc count read-back mismatch"
            );
        }
    } else if let Some(total) = first_number(&total_disc_values) {
        ensure!(
            tag.total_discs() == Some(total),
            "M4A total disc count read-back mismatch"
        );
    }
    Ok(())
}

fn verify_picture_mapping(source: &SourceMetadata, tag: &Tag) -> Result<()> {
    let expected = source
        .pictures
        .iter()
        .filter_map(|picture| supported_picture_data(picture).map(|data| (picture, data.to_vec())))
        .collect::<Vec<_>>();
    let cover = Fourcc(*b"covr");
    let actual = tag.data_of(&cover).cloned().collect::<Vec<_>>();
    let expected_data = expected
        .iter()
        .map(
            |(picture, data)| match picture.mime_type.to_ascii_lowercase().as_str() {
                "image/jpeg" => Data::Jpeg(data.clone()),
                "image/png" => Data::Png(data.clone()),
                _ => unreachable!("supported_picture_data filters to JPEG and PNG"),
            },
        )
        .collect::<Vec<_>>();
    ensure!(
        actual == expected_data,
        "M4A cover artwork read-back mismatch"
    );
    Ok(())
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
            "Additional metadata warnings were omitted after reaching the bounded warning limit.",
        ));
    }
}

fn cap_warnings(warnings: &mut Vec<Warning>) {
    if warnings.len() > MAX_WARNINGS {
        warnings.truncate(MAX_WARNINGS - 1);
        warnings.push(Warning::new(
            "additional_warnings_omitted",
            "Additional metadata warnings were omitted after reaching the bounded warning limit.",
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AUDIT_MEAN, Mp4Layout, SourceKind, prepare_moov_for_bounded_write, raw_audit_ident,
        scan_mp4_layout, seek_to_flac_marker, source_comment_ident,
    };
    use mp4ameta::DataIdent;
    use std::fs::{self, File, OpenOptions};
    use std::io::{Cursor, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn source_comment_keys_cannot_collide_with_internal_audit_names() {
        for name in [
            "FLAC_STREAMINFO_RAW",
            "FLAC_VORBIS_VENDOR_000001",
            "FLAC_RAW_BLOCK_000002_TYPE_02",
        ] {
            assert_eq!(
                source_comment_ident(SourceKind::Flac, name),
                DataIdent::freeform(AUDIT_MEAN, name.to_owned())
            );
            assert_ne!(
                source_comment_ident(SourceKind::Flac, name),
                raw_audit_ident(name)
            );
        }
    }

    #[test]
    fn relocates_moov_without_moving_mdat_bytes() {
        let ftyp = test_atom(b"ftyp", b"M4A \0\0\0\0");
        let moov = test_atom(b"moov", &test_atom(b"mvhd", &[]));
        let free = test_atom(b"free", &[0; 4]);
        let mdat = test_atom(b"mdat", b"synthetic-audio-payload");
        let mdat_start = ftyp.len() + moov.len() + free.len();
        let mut original = Vec::new();
        original.extend_from_slice(&ftyp);
        original.extend_from_slice(&moov);
        original.extend_from_slice(&free);
        original.extend_from_slice(&mdat);
        let path = write_temp_mp4(&original);

        prepare_moov_for_bounded_write(&path).unwrap();

        let rewritten = fs::read(&path).unwrap();
        assert_eq!(
            &rewritten[mdat_start..mdat_start + mdat.len()],
            mdat.as_slice()
        );
        assert_eq!(&rewritten[ftyp.len() + 4..ftyp.len() + 8], b"free");
        assert_eq!(&rewritten[rewritten.len() - moov.len()..], moov.as_slice());
        let layout = scan_temp_layout(&rewritten);
        assert_eq!(layout.mdat.start, u64::try_from(mdat_start).unwrap());
        assert_eq!(layout.moov.end, u64::try_from(rewritten.len()).unwrap());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn converts_small_zero_sized_mdat_before_appending_moov() {
        let ftyp = test_atom(b"ftyp", b"M4A \0\0\0\0");
        let moov = test_atom(b"moov", &test_atom(b"mvhd", &[]));
        let mut mdat = Vec::from([0u8, 0, 0, 0]);
        mdat.extend_from_slice(b"mdat");
        mdat.extend_from_slice(b"synthetic-audio");
        let mdat_start = ftyp.len() + moov.len();
        let mdat_size = u32::try_from(mdat.len()).unwrap();
        let mut original = Vec::new();
        original.extend_from_slice(&ftyp);
        original.extend_from_slice(&moov);
        original.extend_from_slice(&mdat);
        let path = write_temp_mp4(&original);

        prepare_moov_for_bounded_write(&path).unwrap();

        let rewritten = fs::read(&path).unwrap();
        assert_eq!(
            &rewritten[mdat_start..mdat_start + 4],
            &mdat_size.to_be_bytes()
        );
        assert_eq!(
            &rewritten[mdat_start + 8..mdat_start + mdat.len()],
            &mdat[8..]
        );
        assert_eq!(&rewritten[rewritten.len() - moov.len()..], moov.as_slice());
        let layout = scan_temp_layout(&rewritten);
        assert_eq!(layout.mdat.end, layout.moov.start);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_fragmented_moov_before_rewrite() {
        let ftyp = test_atom(b"ftyp", b"M4A \0\0\0\0");
        let moov = test_atom(b"moov", &test_atom(b"mvex", &[]));
        let mdat = test_atom(b"mdat", b"synthetic-audio");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ftyp);
        bytes.extend_from_slice(&moov);
        bytes.extend_from_slice(&mdat);
        let mut cursor = Cursor::new(bytes.clone());

        assert!(scan_mp4_layout(&mut cursor, u64::try_from(bytes.len()).unwrap()).is_err());
    }

    #[test]
    fn captures_id3v24_footer_before_flac_marker() {
        let mut bytes = Vec::from(*b"ID3\x04\x00\x10\x00\x00\x00\x03");
        bytes.extend_from_slice(b"tag");
        bytes.extend_from_slice(b"3DI\x04\x00\x10\x00\x00\x00\x03");
        bytes.extend_from_slice(b"fLaC");
        let path = write_temp_mp4(&bytes);
        let mut file = File::open(&path).unwrap();
        let prefix = seek_to_flac_marker(&mut file, u64::try_from(bytes.len()).unwrap())
            .unwrap()
            .unwrap();

        assert_eq!(prefix, bytes[..23]);
        fs::remove_file(path).unwrap();
    }

    fn scan_temp_layout(bytes: &[u8]) -> Mp4Layout {
        let mut cursor = Cursor::new(bytes);
        scan_mp4_layout(&mut cursor, u64::try_from(bytes.len()).unwrap()).unwrap()
    }

    fn test_atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let size = u32::try_from(payload.len() + 8).unwrap();
        let mut atom = Vec::with_capacity(size as usize);
        atom.extend_from_slice(&size.to_be_bytes());
        atom.extend_from_slice(kind);
        atom.extend_from_slice(payload);
        atom
    }

    fn write_temp_mp4(bytes: &[u8]) -> PathBuf {
        let serial = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "flac-alac-metadata-layout-{}-{serial}.m4a",
            std::process::id()
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(bytes).unwrap();
        path
    }
}
