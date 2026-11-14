//! Bounded validation and identity extraction for uploaded BitTorrent metainfo.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, PathBuf};

use anyhow::{Context, Result, anyhow, bail, ensure};
use sha1::{Digest, Sha1};
use sha2::Sha256;

use crate::torrent_types::{
    MAX_TORRENT_COMPONENT_BYTES, MAX_TORRENT_ENTRIES, MAX_TORRENT_NAME_BYTES, MAX_TORRENT_NESTING,
    MAX_TORRENT_PATH_BYTES, MetainfoFile, TorrentIdentity, TorrentMetainfo,
};

const MAX_INTEGER_BYTES: usize = 32;
const PRIVATE_COMPONENT_PREFIX: &str = ".telegram-video-downloader-";

#[derive(Debug)]
struct Node {
    start: usize,
    end: usize,
    value: Value,
}

#[derive(Debug)]
enum Value {
    Bytes(Vec<u8>),
    Int(i64),
    List(Vec<Node>),
    Dict(BTreeMap<Vec<u8>, Node>),
}

struct Parser<'a> {
    input: &'a [u8],
    cursor: usize,
    entries: usize,
    max_entries: usize,
}

/// Parse a bounded `.torrent` payload, preserving the exact encoded `info` dictionary bytes for
/// v1 SHA-1 and v2 SHA-256 identifiers. Tracker URLs and other private fields are never copied
/// into error messages.
pub fn parse_metainfo(bytes: &[u8], max_bytes: usize, max_files: usize) -> Result<TorrentMetainfo> {
    ensure!(max_bytes > 0, "torrent byte limit must be positive");
    ensure!(max_files > 0, "torrent file limit must be positive");
    ensure!(
        bytes.len() <= max_bytes,
        "torrent metainfo exceeds the configured byte limit"
    );
    ensure!(!bytes.is_empty(), "torrent metainfo is empty");

    let mut parser = Parser {
        input: bytes,
        cursor: 0,
        entries: 0,
        max_entries: MAX_TORRENT_ENTRIES,
    };
    let root = parser.node(0).context("invalid torrent metainfo")?;
    ensure!(
        parser.cursor == bytes.len(),
        "torrent metainfo has trailing bytes"
    );
    let root = as_dict(&root, "torrent metainfo root must be a dictionary")?;
    let info =
        get(root, b"info").ok_or_else(|| anyhow!("torrent metainfo has no info dictionary"))?;
    let info_dict = as_dict(info, "torrent info field must be a dictionary")?;
    let raw_info = bytes
        .get(info.start..info.end)
        .ok_or_else(|| anyhow!("torrent info dictionary bounds are invalid"))?;

    let name = text_field(info_dict, b"name.utf-8", b"name")?;
    validate_display_name(&name)?;

    let v1_files = parse_v1_files(info_dict, &name, max_files)?;
    let v2_files = parse_v2_files(info_dict, &name, max_files)?;
    let has_v1 = get(info_dict, b"pieces").is_some();
    let has_v2 =
        get(info_dict, b"meta version").is_some() || get(info_dict, b"file tree").is_some();

    ensure!(
        has_v1 || has_v2,
        "torrent info dictionary has no supported file layout"
    );
    if has_v1 {
        validate_v1_hash_fields(info_dict)?;
        ensure!(!v1_files.is_empty(), "torrent contains no files");
    } else {
        ensure!(
            v1_files.is_empty(),
            "torrent has v1 files but no pieces field"
        );
    }
    if has_v2 {
        validate_v2_hash_fields(info_dict)?;
        ensure!(!v2_files.is_empty(), "torrent contains no v2 files");
    } else {
        ensure!(
            v2_files.is_empty(),
            "torrent has a v2 file tree without meta version"
        );
    }

    let files = match (has_v1, has_v2) {
        (true, true) => {
            let piece_length = u64::try_from(int_field(info_dict, b"piece length")?)
                .context("hybrid torrent piece length is invalid")?;
            validate_v1_padding(&v1_files, piece_length)?;
            ensure!(
                same_file_set(&v1_files, &v2_files),
                "hybrid torrent layouts disagree"
            );
            v1_files
        }
        (true, false) => v1_files,
        (false, true) => v2_files,
        (false, false) => unreachable!("supported layout was checked above"),
    };
    ensure!(files.len() <= max_files, "torrent contains too many files");
    validate_file_paths(&files)?;
    let total_size = files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.size)
            .ok_or_else(|| anyhow!("torrent file sizes overflow"))
    })?;
    let files = files
        .into_iter()
        .enumerate()
        .map(|(index, mut file)| {
            file.index =
                u32::try_from(index).context("torrent file index exceeds the supported range")?;
            Ok(file)
        })
        .collect::<Result<Vec<_>>>()?;

    let v1 = if has_v1 {
        Some(hex(&Sha1::digest(raw_info)))
    } else {
        None
    };
    let v2 = if has_v2 {
        Some(hex(&Sha256::digest(raw_info)))
    } else {
        None
    };
    let private = match get(info_dict, b"private") {
        Some(node) => match &node.value {
            Value::Int(0) => false,
            Value::Int(1) => true,
            _ => bail!("torrent private flag is invalid"),
        },
        None => false,
    };

    Ok(TorrentMetainfo {
        identity: TorrentIdentity { v1, v2 },
        raw_sha256: hex(&Sha256::digest(bytes)),
        name,
        files,
        total_size,
        private,
    })
}

impl Parser<'_> {
    fn node(&mut self, depth: usize) -> Result<Node> {
        ensure!(
            depth <= MAX_TORRENT_NESTING,
            "torrent metainfo nesting is too deep"
        );
        let start = self.cursor;
        let tag = *self
            .input
            .get(self.cursor)
            .ok_or_else(|| anyhow!("torrent metainfo ended unexpectedly"))?;
        let value = match tag {
            b'0'..=b'9' => Value::Bytes(self.byte_string()?),
            b'i' => Value::Int(self.integer()?),
            b'l' => {
                self.cursor += 1;
                let mut values = Vec::new();
                loop {
                    match self.input.get(self.cursor) {
                        Some(b'e') => {
                            self.cursor += 1;
                            break;
                        }
                        Some(_) => {
                            self.bump_entry()?;
                            values.push(self.node(depth + 1)?);
                        }
                        None => bail!("torrent metainfo list ended unexpectedly"),
                    }
                }
                Value::List(values)
            }
            b'd' => {
                self.cursor += 1;
                let mut values = BTreeMap::new();
                let mut previous_key: Option<Vec<u8>> = None;
                loop {
                    match self.input.get(self.cursor) {
                        Some(b'e') => {
                            self.cursor += 1;
                            break;
                        }
                        Some(b'0'..=b'9') => {
                            self.bump_entry()?;
                            let key = self.byte_string()?;
                            if let Some(previous) = previous_key.as_ref() {
                                ensure!(
                                    previous < &key,
                                    "torrent metainfo dictionary keys are duplicated or unsorted"
                                );
                            }
                            previous_key = Some(key.clone());
                            let value = self.node(depth + 1)?;
                            values.insert(key, value);
                        }
                        Some(_) => bail!("torrent metainfo dictionary key is invalid"),
                        None => bail!("torrent metainfo dictionary ended unexpectedly"),
                    }
                }
                Value::Dict(values)
            }
            _ => bail!("torrent metainfo contains an invalid bencode token"),
        };
        Ok(Node {
            start,
            end: self.cursor,
            value,
        })
    }

    fn byte_string(&mut self) -> Result<Vec<u8>> {
        let length_start = self.cursor;
        let colon = self.input[self.cursor..]
            .iter()
            .position(|byte| *byte == b':')
            .map(|offset| self.cursor + offset)
            .ok_or_else(|| anyhow!("torrent byte string has no length delimiter"))?;
        let digits = &self.input[length_start..colon];
        ensure!(!digits.is_empty(), "torrent byte string length is empty");
        ensure!(
            digits.len() <= MAX_INTEGER_BYTES,
            "torrent byte string length is too large"
        );
        ensure!(
            digits.iter().all(u8::is_ascii_digit),
            "torrent byte string length is invalid"
        );
        ensure!(
            digits.len() == 1 || digits[0] != b'0',
            "torrent byte string length is not canonical"
        );
        let length = std::str::from_utf8(digits)
            .context("torrent byte string length is invalid")?
            .parse::<usize>()
            .context("torrent byte string length is too large")?;
        self.cursor = colon + 1;
        let end = self
            .cursor
            .checked_add(length)
            .ok_or_else(|| anyhow!("torrent byte string length overflows"))?;
        let bytes = self
            .input
            .get(self.cursor..end)
            .ok_or_else(|| anyhow!("torrent byte string exceeds the input"))?
            .to_vec();
        self.cursor = end;
        Ok(bytes)
    }

    fn integer(&mut self) -> Result<i64> {
        self.cursor += 1;
        let start = self.cursor;
        let end = self.input[self.cursor..]
            .iter()
            .position(|byte| *byte == b'e')
            .map(|offset| self.cursor + offset)
            .ok_or_else(|| anyhow!("torrent integer has no terminator"))?;
        let digits = &self.input[start..end];
        ensure!(!digits.is_empty(), "torrent integer is empty");
        ensure!(
            digits.len() <= MAX_INTEGER_BYTES,
            "torrent integer is too large"
        );
        let number = std::str::from_utf8(digits)
            .context("torrent integer is invalid")?
            .parse::<i64>()
            .context("torrent integer is out of range")?;
        ensure!(
            digits == number.to_string().as_bytes(),
            "torrent integer is not canonical"
        );
        self.cursor = end + 1;
        Ok(number)
    }

    fn bump_entry(&mut self) -> Result<()> {
        self.entries = self.entries.saturating_add(1);
        ensure!(
            self.entries <= self.max_entries,
            "torrent metainfo has too many entries"
        );
        Ok(())
    }
}

fn as_dict<'a>(node: &'a Node, error: &'static str) -> Result<&'a BTreeMap<Vec<u8>, Node>> {
    match &node.value {
        Value::Dict(values) => Ok(values),
        _ => bail!("{error}"),
    }
}

fn get<'a>(dict: &'a BTreeMap<Vec<u8>, Node>, key: &[u8]) -> Option<&'a Node> {
    dict.get(key)
}

fn int_field(dict: &BTreeMap<Vec<u8>, Node>, key: &[u8]) -> Result<i64> {
    match get(dict, key).map(|node| &node.value) {
        Some(Value::Int(value)) => Ok(*value),
        _ => bail!("torrent integer field is missing or invalid"),
    }
}

fn bytes_field<'a>(dict: &'a BTreeMap<Vec<u8>, Node>, key: &[u8]) -> Result<&'a [u8]> {
    match get(dict, key).map(|node| &node.value) {
        Some(Value::Bytes(value)) => Ok(value),
        _ => bail!("torrent byte field is missing or invalid"),
    }
}

fn text_field(dict: &BTreeMap<Vec<u8>, Node>, preferred: &[u8], fallback: &[u8]) -> Result<String> {
    let bytes = match get(dict, preferred) {
        Some(node) => match &node.value {
            Value::Bytes(bytes) => bytes.as_slice(),
            _ => bail!("torrent name field is invalid"),
        },
        None => bytes_field(dict, fallback)?,
    };
    ensure!(
        bytes.len() <= MAX_TORRENT_NAME_BYTES,
        "torrent name is too long"
    );
    String::from_utf8(bytes.to_vec()).context("torrent name is not valid UTF-8")
}

fn validate_display_name(name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "torrent name is empty");
    ensure!(
        !name.chars().any(char::is_control),
        "torrent name contains control characters"
    );
    validate_component(name)
}

fn validate_v1_hash_fields(info: &BTreeMap<Vec<u8>, Node>) -> Result<()> {
    let pieces = bytes_field(info, b"pieces")?;
    ensure!(
        pieces.len() % 20 == 0,
        "torrent piece hash data is malformed"
    );
    let piece_length = int_field(info, b"piece length")?;
    ensure!(piece_length > 0, "torrent piece length is invalid");
    Ok(())
}

fn validate_v2_hash_fields(info: &BTreeMap<Vec<u8>, Node>) -> Result<()> {
    ensure!(
        int_field(info, b"meta version")? == 2,
        "torrent meta version is unsupported"
    );
    ensure!(
        get(info, b"file tree").is_some(),
        "torrent v2 file tree is missing"
    );
    let piece_length = int_field(info, b"piece length")?;
    ensure!(
        piece_length > 0 && (piece_length as u64).is_power_of_two(),
        "torrent v2 piece length is invalid"
    );
    Ok(())
}

fn parse_v1_files(
    info: &BTreeMap<Vec<u8>, Node>,
    name: &str,
    max_files: usize,
) -> Result<Vec<MetainfoFile>> {
    match (get(info, b"files"), get(info, b"length")) {
        (Some(_), Some(_)) => bail!("torrent has both single-file and multi-file layouts"),
        (Some(files_node), None) => {
            let entries = match &files_node.value {
                Value::List(entries) => entries,
                _ => bail!("torrent file list is invalid"),
            };
            ensure!(
                entries.len() <= max_files,
                "torrent contains too many files"
            );
            let mut files = Vec::with_capacity(entries.len());
            for entry in entries {
                let dict = as_dict(entry, "torrent file entry is invalid")?;
                let size = nonnegative_size(int_field(dict, b"length")?)?;
                reject_symlink(dict)?;
                let path_node = get(dict, b"path.utf-8").or_else(|| get(dict, b"path"));
                let components = path_components(
                    path_node.ok_or_else(|| anyhow!("torrent file path is missing"))?,
                )?;
                let relative_path = safe_relative_path(&components)?;
                files.push(MetainfoFile {
                    index: 0,
                    relative_path,
                    size,
                    is_padding: has_attr(dict, b'p')?,
                });
            }
            Ok(files)
        }
        (None, Some(_)) => {
            let size = nonnegative_size(int_field(info, b"length")?)?;
            reject_symlink(info)?;
            Ok(vec![MetainfoFile {
                index: 0,
                relative_path: safe_relative_path(&[name.to_string()])?,
                size,
                is_padding: has_attr(info, b'p')?,
            }])
        }
        (None, None) => Ok(Vec::new()),
    }
}

fn parse_v2_files(
    info: &BTreeMap<Vec<u8>, Node>,
    name: &str,
    max_files: usize,
) -> Result<Vec<MetainfoFile>> {
    let has_version = get(info, b"meta version").is_some();
    let has_tree = get(info, b"file tree").is_some();
    ensure!(has_version == has_tree, "torrent v2 metadata is incomplete");
    if !has_version {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    let mut components = Vec::new();
    let tree = get(info, b"file tree").ok_or_else(|| anyhow!("torrent v2 file tree is missing"))?;
    walk_v2_tree(tree, &mut components, &mut files, max_files)?;
    if files.len() == 1 && files[0].relative_path.as_os_str().is_empty() {
        files[0].relative_path = safe_relative_path(&[name.to_string()])?;
    }
    Ok(files)
}

fn walk_v2_tree(
    node: &Node,
    components: &mut Vec<String>,
    files: &mut Vec<MetainfoFile>,
    max_files: usize,
) -> Result<()> {
    let tree = as_dict(node, "torrent v2 file tree node is invalid")?;
    if let Some((_, leaf)) = tree.iter().find(|(key, _)| key.is_empty()) {
        ensure!(
            tree.len() == 1,
            "torrent v2 file node also contains child entries"
        );
        let file = as_dict(leaf, "torrent v2 file leaf is invalid")?;
        reject_symlink(file)?;
        let size = nonnegative_size(int_field(file, b"length")?)?;
        let relative_path = if components.is_empty() {
            PathBuf::new()
        } else {
            safe_relative_path(components)?
        };
        files.push(MetainfoFile {
            index: 0,
            relative_path,
            size,
            is_padding: has_attr(file, b'p')?,
        });
        ensure!(files.len() <= max_files, "torrent contains too many files");
        return Ok(());
    }
    ensure!(
        !tree.is_empty(),
        "torrent v2 file tree contains an empty directory"
    );
    for (component, child) in tree {
        let component =
            String::from_utf8(component.clone()).context("torrent v2 path is not UTF-8")?;
        validate_component(&component)?;
        components.push(component);
        walk_v2_tree(child, components, files, max_files)?;
        components.pop();
    }
    Ok(())
}

fn path_components(node: &Node) -> Result<Vec<String>> {
    let entries = match &node.value {
        Value::List(entries) => entries,
        _ => bail!("torrent file path is invalid"),
    };
    ensure!(!entries.is_empty(), "torrent file path is empty");
    let mut components = Vec::with_capacity(entries.len());
    for component in entries {
        let value = match &component.value {
            Value::Bytes(value) => value,
            _ => bail!("torrent path component is invalid"),
        };
        let component = String::from_utf8(value.clone()).context("torrent path is not UTF-8")?;
        validate_component(&component)?;
        components.push(component);
    }
    Ok(components)
}

fn safe_relative_path(components: &[String]) -> Result<PathBuf> {
    ensure!(!components.is_empty(), "torrent file path is empty");
    let mut output = PathBuf::new();
    let mut bytes = 0_usize;
    for component in components {
        validate_component(component)?;
        bytes = bytes
            .checked_add(component.len() + 1)
            .ok_or_else(|| anyhow!("torrent file path is too long"))?;
        ensure!(
            bytes <= MAX_TORRENT_PATH_BYTES,
            "torrent file path is too long"
        );
        output.push(component);
    }
    ensure!(output.is_relative(), "torrent file path is absolute");
    ensure!(
        output
            .components()
            .all(|part| matches!(part, Component::Normal(_))),
        "torrent file path contains traversal"
    );
    Ok(output)
}

fn validate_component(component: &str) -> Result<()> {
    ensure!(!component.is_empty(), "torrent path component is empty");
    ensure!(
        component != "." && component != "..",
        "torrent path contains traversal"
    );
    ensure!(
        component.len() <= MAX_TORRENT_COMPONENT_BYTES,
        "torrent path component is too long"
    );
    ensure!(
        !component
            .get(..PRIVATE_COMPONENT_PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PRIVATE_COMPONENT_PREFIX)),
        "torrent path component uses a reserved private name"
    );
    ensure!(
        !component.chars().any(|character| character.is_control() || matches!(character, '/' | '\\' | ':' | '\0')),
        "torrent path component contains an unsafe character"
    );
    Ok(())
}

fn nonnegative_size(value: i64) -> Result<u64> {
    u64::try_from(value).context("torrent file size is negative or out of range")
}

fn reject_symlink(dict: &BTreeMap<Vec<u8>, Node>) -> Result<()> {
    ensure!(
        get(dict, b"symlink path").is_none(),
        "torrent symlink metadata is unsupported"
    );
    ensure!(
        !has_attr(dict, b'l')?,
        "torrent symlink metadata is unsupported"
    );
    Ok(())
}

fn has_attr(dict: &BTreeMap<Vec<u8>, Node>, attr: u8) -> Result<bool> {
    match get(dict, b"attr").map(|node| &node.value) {
        Some(Value::Bytes(value)) => Ok(value.contains(&attr)),
        Some(_) => bail!("torrent file attributes are invalid"),
        None => Ok(false),
    }
}

fn validate_file_paths(files: &[MetainfoFile]) -> Result<()> {
    let mut paths = BTreeSet::new();
    for file in files {
        let path = safe_relative_path(
            &file
                .relative_path
                .components()
                .map(|component| match component {
                    Component::Normal(value) => value
                        .to_str()
                        .map(str::to_string)
                        .ok_or_else(|| anyhow!("torrent path is not valid UTF-8")),
                    _ => bail!("torrent file path contains traversal"),
                })
                .collect::<Result<Vec<_>>>()?,
        )?;
        ensure!(
            paths.insert(path.clone()),
            "torrent has duplicate file paths"
        );
    }
    for path in &paths {
        let mut parent = path.parent();
        while let Some(candidate) = parent {
            ensure!(
                !paths.contains(candidate),
                "torrent path is both a file and a directory"
            );
            parent = candidate.parent();
        }
    }
    Ok(())
}

fn same_file_set(left: &[MetainfoFile], right: &[MetainfoFile]) -> bool {
    let payload_files: Vec<_> = left.iter().filter(|file| !file.is_padding).collect();
    if payload_files.len() != right.len() || right.iter().any(|file| file.is_padding) {
        return false;
    }
    payload_files
        .into_iter()
        .zip(right)
        .all(|(left, right)| left.relative_path == right.relative_path && left.size == right.size)
}

fn validate_v1_padding(files: &[MetainfoFile], piece_length: u64) -> Result<()> {
    ensure!(piece_length > 0, "hybrid torrent piece length is invalid");
    let mut offset = 0_u64;
    for file in files {
        if file.is_padding {
            let remainder = offset % piece_length;
            ensure!(
                remainder != 0,
                "hybrid torrent contains unnecessary padding"
            );
            let expected = piece_length - remainder;
            ensure!(
                file.size == expected,
                "hybrid torrent padding length does not align the file layout"
            );
        } else if file.size > 0 {
            ensure!(
                offset.is_multiple_of(piece_length),
                "hybrid torrent payload file is not piece-aligned"
            );
        }
        offset = offset
            .checked_add(file.size)
            .ok_or_else(|| anyhow!("hybrid torrent file sizes overflow"))?;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bencode_bytes(bytes: &[u8]) -> Vec<u8> {
        let mut output = format!("{}:", bytes.len()).into_bytes();
        output.extend_from_slice(bytes);
        output
    }

    fn dict(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut output = vec![b'd'];
        for (key, value) in entries {
            output.extend(bencode_bytes(&key));
            output.extend(value);
        }
        output.push(b'e');
        output
    }

    fn integer(value: i64) -> Vec<u8> {
        format!("i{value}e").into_bytes()
    }

    fn list(values: Vec<Vec<u8>>) -> Vec<u8> {
        let mut output = vec![b'l'];
        for value in values {
            output.extend(value);
        }
        output.push(b'e');
        output
    }

    fn text(value: &str) -> Vec<u8> {
        bencode_bytes(value.as_bytes())
    }

    fn valid_v1() -> (Vec<u8>, Vec<u8>) {
        let info = dict(vec![
            (b"length".to_vec(), integer(3)),
            (b"name".to_vec(), text("clip.mp4")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
            (b"private".to_vec(), integer(1)),
        ]);
        let root = dict(vec![
            (
                b"announce".to_vec(),
                text("https://secret.invalid/announce?pass=hidden"),
            ),
            (b"info".to_vec(), info.clone()),
        ]);
        (root, info)
    }

    fn v1_single_name(name_key: &[u8], name: &str) -> Vec<u8> {
        let mut fields = vec![
            (b"length".to_vec(), integer(3)),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
            (name_key.to_vec(), text(name)),
        ];
        if name_key == b"name.utf-8" {
            fields.push((b"name".to_vec(), text("ordinary-name")));
        }
        dict(vec![(b"info".to_vec(), dict(fields))])
    }

    fn v1_multi_path(path_key: &[u8], components: &[&str]) -> Vec<u8> {
        let mut file_fields = vec![
            (b"length".to_vec(), integer(3)),
            (
                path_key.to_vec(),
                list(components.iter().map(|component| text(component)).collect()),
            ),
        ];
        if path_key == b"path.utf-8" {
            file_fields.push((b"path".to_vec(), list(vec![text("ordinary.mp3")])));
        }
        let info = dict(vec![
            (b"files".to_vec(), list(vec![dict(file_fields)])),
            (b"name".to_vec(), text("ordinary-root")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
        ]);
        dict(vec![(b"info".to_vec(), info)])
    }

    #[test]
    fn v1_hashes_exact_info_and_keeps_private_flag() {
        let (bytes, info) = valid_v1();
        let parsed = parse_metainfo(&bytes, 1024, 10).unwrap();
        assert_eq!(
            parsed.identity.v1.as_deref(),
            Some(hex(&Sha1::digest(&info)).as_str())
        );
        assert_eq!(parsed.identity.v2, None);
        assert_eq!(parsed.raw_sha256, hex(&Sha256::digest(&bytes)));
        assert!(parsed.private);
        assert_eq!(parsed.files[0].relative_path, PathBuf::from("clip.mp4"));
        assert_eq!(parsed.files[0].size, 3);
    }

    #[test]
    fn v2_tree_is_walked_in_sorted_file_order() {
        let file = dict(vec![
            (b"length".to_vec(), integer(1)),
            (b"pieces root".to_vec(), bencode_bytes(&[9; 32])),
        ]);
        let tree = dict(vec![
            (b"b.txt".to_vec(), dict(vec![(b"".to_vec(), file.clone())])),
            (
                b"dir".to_vec(),
                dict(vec![(b"a.txt".to_vec(), dict(vec![(b"".to_vec(), file)]))]),
            ),
        ]);
        let info = dict(vec![
            (b"file tree".to_vec(), tree),
            (b"meta version".to_vec(), integer(2)),
            (b"name".to_vec(), text("collection")),
            (b"piece length".to_vec(), integer(16_384)),
        ]);
        let bytes = dict(vec![(b"info".to_vec(), info)]);
        let parsed = parse_metainfo(&bytes, 4096, 10).unwrap();
        assert!(parsed.identity.v1.is_none());
        assert_eq!(parsed.identity.v2.as_ref().unwrap().len(), 64);
        assert_eq!(parsed.files[0].relative_path, PathBuf::from("b.txt"));
        assert_eq!(parsed.files[1].relative_path, PathBuf::from("dir/a.txt"));
    }

    #[test]
    fn hybrid_requires_matching_file_layout_and_keeps_both_hashes() {
        let pieces = bencode_bytes(&[4; 20]);
        let path = list(vec![text("folder"), text("a.bin")]);
        let files = list(vec![dict(vec![
            (b"length".to_vec(), integer(2)),
            (b"path".to_vec(), path),
        ])]);
        let v2_file = dict(vec![
            (b"length".to_vec(), integer(2)),
            (b"pieces root".to_vec(), bencode_bytes(&[5; 32])),
        ]);
        let tree = dict(vec![(
            b"folder".to_vec(),
            dict(vec![(
                b"a.bin".to_vec(),
                dict(vec![(b"".to_vec(), v2_file)]),
            )]),
        )]);
        let info = dict(vec![
            (b"file tree".to_vec(), tree),
            (b"files".to_vec(), files),
            (b"meta version".to_vec(), integer(2)),
            (b"name".to_vec(), text("folder")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), pieces),
        ]);
        let bytes = dict(vec![(b"info".to_vec(), info)]);
        let parsed = parse_metainfo(&bytes, 4096, 10).unwrap();
        assert!(parsed.identity.v1.is_some());
        assert!(parsed.identity.v2.is_some());
        assert_eq!(parsed.files.len(), 1);
    }

    fn hybrid_with_padding(padding_size: i64, second_v2_size: i64) -> Vec<u8> {
        let v1_files = list(vec![
            dict(vec![
                (b"length".to_vec(), integer(2)),
                (b"path".to_vec(), list(vec![text("folder"), text("a.bin")])),
            ]),
            dict(vec![
                (b"attr".to_vec(), text("p")),
                (b"length".to_vec(), integer(padding_size)),
                (
                    b"path".to_vec(),
                    list(vec![text(".pad"), text(&padding_size.to_string())]),
                ),
            ]),
            dict(vec![
                (b"length".to_vec(), integer(3)),
                (b"path".to_vec(), list(vec![text("folder"), text("b.bin")])),
            ]),
        ]);
        let v2_leaf = |size| {
            dict(vec![
                (b"length".to_vec(), integer(size)),
                (b"pieces root".to_vec(), bencode_bytes(&[5; 32])),
            ])
        };
        let tree = dict(vec![(
            b"folder".to_vec(),
            dict(vec![
                (b"a.bin".to_vec(), dict(vec![(b"".to_vec(), v2_leaf(2))])),
                (
                    b"b.bin".to_vec(),
                    dict(vec![(b"".to_vec(), v2_leaf(second_v2_size))]),
                ),
            ]),
        )]);
        let info = dict(vec![
            (b"file tree".to_vec(), tree),
            (b"files".to_vec(), v1_files),
            (b"meta version".to_vec(), integer(2)),
            (b"name".to_vec(), text("folder")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[4; 40])),
        ]);
        dict(vec![(b"info".to_vec(), info)])
    }

    #[test]
    fn hybrid_padding_is_omitted_from_v2_comparison_but_keeps_v1_indexes() {
        let parsed = parse_metainfo(&hybrid_with_padding(16_382, 3), 4096, 10)
            .expect("valid hybrid padding aligns the following v2 payload");

        assert_eq!(parsed.files.len(), 3);
        assert_eq!(parsed.files[0].index, 0);
        assert_eq!(parsed.files[0].relative_path, PathBuf::from("folder/a.bin"));
        assert!(!parsed.files[0].is_padding);
        assert_eq!(parsed.files[1].index, 1);
        assert!(parsed.files[1].is_padding);
        assert_eq!(parsed.files[1].size, 16_382);
        assert_eq!(parsed.files[2].index, 2);
        assert_eq!(parsed.files[2].relative_path, PathBuf::from("folder/b.bin"));
        assert!(!parsed.files[2].is_padding);
    }

    #[test]
    fn hybrid_rejects_malformed_padding_and_payload_mismatch() {
        assert!(
            parse_metainfo(&hybrid_with_padding(16_381, 3), 4096, 10).is_err(),
            "padding must fill exactly to the next piece boundary"
        );
        assert!(
            parse_metainfo(&hybrid_with_padding(16_382, 4), 4096, 10).is_err(),
            "hybrid payload sizes must match between v1 and v2 layouts"
        );
    }

    fn hybrid_with_trailing_padding(padding_size: i64) -> Vec<u8> {
        let v1_files = list(vec![
            dict(vec![
                (b"length".to_vec(), integer(2)),
                (b"path".to_vec(), list(vec![text("folder"), text("a.bin")])),
            ]),
            dict(vec![
                (b"attr".to_vec(), text("p")),
                (b"length".to_vec(), integer(padding_size)),
                (
                    b"path".to_vec(),
                    list(vec![text(".pad"), text(&padding_size.to_string())]),
                ),
            ]),
        ]);
        let v2_file = dict(vec![
            (b"length".to_vec(), integer(2)),
            (b"pieces root".to_vec(), bencode_bytes(&[5; 32])),
        ]);
        let tree = dict(vec![(
            b"folder".to_vec(),
            dict(vec![(
                b"a.bin".to_vec(),
                dict(vec![(b"".to_vec(), v2_file)]),
            )]),
        )]);
        let info = dict(vec![
            (b"file tree".to_vec(), tree),
            (b"files".to_vec(), v1_files),
            (b"meta version".to_vec(), integer(2)),
            (b"name".to_vec(), text("folder")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[4; 20])),
        ]);
        dict(vec![(b"info".to_vec(), info)])
    }

    #[test]
    fn hybrid_allows_exact_trailing_alignment_padding_only() {
        let parsed = parse_metainfo(&hybrid_with_trailing_padding(16_382), 4096, 10)
            .expect("a final padding entry may align the v1 layout");
        assert_eq!(parsed.files.len(), 2);
        assert_eq!(parsed.files[0].index, 0);
        assert_eq!(parsed.files[1].index, 1);
        assert!(parsed.files[1].is_padding);
        assert_eq!(parsed.files[1].size, 16_382);

        assert!(
            parse_metainfo(&hybrid_with_trailing_padding(16_381), 4096, 10).is_err(),
            "a trailing pad must still fill exactly to the piece boundary"
        );
    }

    #[test]
    fn rejects_traversal_symlinks_duplicate_paths_and_oversized_inputs() {
        let path = list(vec![text(".."), text("escape.txt")]);
        let multi = dict(vec![
            (
                b"files".to_vec(),
                list(vec![dict(vec![
                    (b"length".to_vec(), integer(1)),
                    (b"path".to_vec(), path),
                ])]),
            ),
            (b"name".to_vec(), text("unsafe")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[1; 20])),
        ]);
        assert!(parse_metainfo(&dict(vec![(b"info".to_vec(), multi)]), 4096, 10).is_err());

        let symlink_file = dict(vec![
            (b"attr".to_vec(), text("l")),
            (b"length".to_vec(), integer(1)),
            (b"path".to_vec(), list(vec![text("link")])),
            (b"symlink path".to_vec(), list(vec![text("target")])),
        ]);
        let info = dict(vec![
            (b"files".to_vec(), list(vec![symlink_file])),
            (b"name".to_vec(), text("unsafe")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[1; 20])),
        ]);
        assert!(parse_metainfo(&dict(vec![(b"info".to_vec(), info)]), 4096, 10).is_err());
        let (bytes, _) = valid_v1();
        assert!(parse_metainfo(&bytes, bytes.len() - 1, 10).is_err());
    }

    #[test]
    fn rejects_reserved_private_names_and_paths_but_allows_other_hidden_directories() {
        for (name_key, private_name) in [
            (b"name".as_slice(), ".telegram-video-downloader-state"),
            (b"name.utf-8".as_slice(), ".TeLeGrAm-ViDeO-DoWnLoAdEr-state"),
        ] {
            assert!(
                parse_metainfo(&v1_single_name(name_key, private_name), 4096, 10).is_err(),
                "reserved torrent root name should be rejected"
            );
        }

        assert!(
            parse_metainfo(
                &v1_multi_path(
                    b"path",
                    &["nested", ".telegram-video-downloader-state", "file.mp3"],
                ),
                4096,
                10,
            )
            .is_err()
        );
        assert!(
            parse_metainfo(
                &v1_multi_path(
                    b"path.utf-8",
                    &["nested", ".TELEGRAM-VIDEO-DOWNLOADER-state", "file.mp3"],
                ),
                4096,
                10,
            )
            .is_err()
        );

        let v2_file = dict(vec![
            (b"length".to_vec(), integer(1)),
            (b"pieces root".to_vec(), bencode_bytes(&[9; 32])),
        ]);
        let v2_tree = dict(vec![(
            b"nested".to_vec(),
            dict(vec![(
                b".Telegram-Video-Downloader-cache".to_vec(),
                dict(vec![(
                    b"file.mp3".to_vec(),
                    dict(vec![(b"".to_vec(), v2_file)]),
                )]),
            )]),
        )]);
        let v2_info = dict(vec![
            (b"file tree".to_vec(), v2_tree),
            (b"meta version".to_vec(), integer(2)),
            (b"name".to_vec(), text("ordinary-root")),
            (b"piece length".to_vec(), integer(16_384)),
        ]);
        assert!(parse_metainfo(&dict(vec![(b"info".to_vec(), v2_info)]), 4096, 10,).is_err());

        let hidden_directory = v1_multi_path(b"path", &[".pad", "song.mp3"]);
        let parsed = parse_metainfo(&hidden_directory, 4096, 10)
            .expect("ordinary hidden torrent directories remain valid");
        assert_eq!(
            parsed.files[0].relative_path,
            PathBuf::from(".pad/song.mp3")
        );
    }

    #[test]
    fn rejects_noncanonical_bencode_and_too_many_files() {
        assert!(
            parse_metainfo(
                b"d4:infod4:name1:a6:lengthi01e12:piece lengthi1e6:pieces20:xxxxxxxxxxxxxxxxxxxxee",
                1024,
                10
            )
            .is_err()
        );
        let entries = (0..2)
            .map(|index| {
                dict(vec![
                    (b"length".to_vec(), integer(1)),
                    (b"path".to_vec(), list(vec![text(&format!("f{index}"))])),
                ])
            })
            .collect();
        let info = dict(vec![
            (b"files".to_vec(), list(entries)),
            (b"name".to_vec(), text("many")),
            (b"piece length".to_vec(), integer(16_384)),
            (b"pieces".to_vec(), bencode_bytes(&[1; 40])),
        ]);
        assert!(parse_metainfo(&dict(vec![(b"info".to_vec(), info)]), 4096, 1).is_err());
    }
}
