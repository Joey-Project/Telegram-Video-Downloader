//! Bounded, privacy-conscious parsing of BitTorrent magnet links.

use std::fmt;

use anyhow::{Result, anyhow, ensure};
use url::Url;

use crate::torrent_types::{MAX_TORRENT_NAME_BYTES, TorrentIdentity};

pub(crate) const MAX_MAGNET_URI_BYTES: usize = 8 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ParsedMagnet {
    pub(crate) identity: TorrentIdentity,
    /// Submission form with `so` selection parameters removed so qBittorrent downloads all files.
    /// This can still contain private tracker data and must not be logged or formatted.
    pub(crate) normalized_uri: String,
    pub(crate) display_name: Option<String>,
}

impl fmt::Debug for ParsedMagnet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedMagnet")
            .field("identity", &self.identity)
            .field("normalized_uri", &"[redacted]")
            .field("display_name", &self.display_name)
            .finish()
    }
}

/// Parse and normalize a supported v1, v2, or hybrid magnet URI.
///
/// Error messages are deliberately generic because magnet URIs can contain private tracker
/// passkeys. The URI itself is bounded before parsing and never included in diagnostics.
pub(crate) fn parse_magnet_uri(uri: &str) -> Result<ParsedMagnet> {
    ensure!(
        !uri.is_empty() && uri.len() <= MAX_MAGNET_URI_BYTES,
        "magnet URI is empty or too long"
    );
    let mut parsed = Url::parse(uri).map_err(|_| anyhow!("magnet URI is invalid"))?;
    ensure!(
        parsed.scheme() == "magnet",
        "URI must use the magnet scheme"
    );
    ensure!(
        parsed.host().is_none() && parsed.fragment().is_none(),
        "magnet URI has unsupported components"
    );

    let mut v1 = None;
    let mut v2 = None;
    let mut display_name = None;
    let mut kept_pairs = Vec::new();
    for (key, value) in parsed.query_pairs() {
        match key.as_ref() {
            "xt" => parse_exact_topic(&value, &mut v1, &mut v2)?,
            "dn" => {
                ensure!(
                    value.len() <= MAX_TORRENT_NAME_BYTES && !value.chars().any(char::is_control),
                    "magnet display name is invalid"
                );
                if display_name.is_none() && !value.is_empty() {
                    display_name = Some(value.to_string());
                }
            }
            _ => {}
        }
        if !key.eq_ignore_ascii_case("so") {
            kept_pairs.push((key.into_owned(), value.into_owned()));
        }
    }
    ensure!(
        v1.is_some() || v2.is_some(),
        "magnet URI has no supported exact topic"
    );
    parsed.query_pairs_mut().clear().extend_pairs(kept_pairs);
    let normalized_uri = parsed.to_string();
    ensure!(
        normalized_uri.len() <= MAX_MAGNET_URI_BYTES,
        "normalized magnet URI is too long"
    );
    Ok(ParsedMagnet {
        identity: TorrentIdentity { v1, v2 },
        normalized_uri,
        display_name,
    })
}

fn parse_exact_topic(topic: &str, v1: &mut Option<String>, v2: &mut Option<String>) -> Result<()> {
    let topic_lower = topic.to_ascii_lowercase();
    if let Some(hash) = topic_lower.strip_prefix("urn:btih:") {
        let normalized = match hash.len() {
            40 if hash.bytes().all(|byte| byte.is_ascii_hexdigit()) => hash.to_string(),
            32 => base32_hash_to_hex(hash)?,
            _ => return Err(anyhow!("magnet v1 identity is malformed")),
        };
        merge_identity(v1, normalized, "v1")
    } else if let Some(hash) = topic_lower.strip_prefix("urn:btmh:") {
        let hash = hash
            .strip_prefix("1220")
            .ok_or_else(|| anyhow!("magnet v2 identity is unsupported"))?;
        ensure!(
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "magnet v2 identity is malformed"
        );
        merge_identity(v2, hash.to_string(), "v2")
    } else {
        Ok(())
    }
}

fn merge_identity(slot: &mut Option<String>, value: String, kind: &str) -> Result<()> {
    if let Some(previous) = slot {
        ensure!(
            previous == &value,
            "magnet URI contains conflicting {kind} identities"
        );
    } else {
        *slot = Some(value);
    }
    Ok(())
}

fn base32_hash_to_hex(encoded: &str) -> Result<String> {
    let mut buffer = 0_u32;
    let mut bits = 0_u32;
    let mut bytes = Vec::with_capacity(20);
    for byte in encoded.bytes() {
        let value = match byte {
            b'a'..=b'z' => u32::from(byte - b'a'),
            b'2'..=b'7' => u32::from(byte - b'2' + 26),
            _ => return Err(anyhow!("magnet v1 identity is malformed")),
        };
        buffer = (buffer << 5) | value;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            bytes.push(((buffer >> bits) & 0xff) as u8);
            buffer &= (1_u32 << bits) - 1;
        }
    }
    ensure!(
        bytes.len() == 20 && bits == 0,
        "magnet v1 identity is malformed"
    );
    Ok(hex(&bytes))
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
    use crate::torrent_types::TorrentMagnet;

    #[test]
    fn parses_hex_base32_v2_and_hybrid_topics_and_removes_selection() {
        let v1_hex = "0123456789abcdef0123456789abcdef01234567";
        let v2_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let base32_zero = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let parsed = parse_magnet_uri(&format!(
            "magnet:?xt=urn:btih:{v1_hex}&xt=urn:btmh:1220{v2_hex}&dn=album+name&so=1-3&tr=udp%3A%2F%2Ftracker.invalid%3A80%2Fannounce"
        ))
        .expect("parse hybrid magnet");
        assert_eq!(parsed.identity.v1.as_deref(), Some(v1_hex));
        assert_eq!(parsed.identity.v2.as_deref(), Some(v2_hex));
        assert_eq!(parsed.display_name.as_deref(), Some("album name"));
        let normalized = Url::parse(&parsed.normalized_uri).unwrap();
        let pairs = normalized.query_pairs().collect::<Vec<_>>();
        assert!(pairs.iter().all(|(key, _)| key != "so"));
        assert!(pairs.iter().any(|(key, _)| key == "tr"));

        let parsed = parse_magnet_uri(&format!("magnet:?xt=urn:btih:{base32_zero}"))
            .expect("decode base32 v1 hash");
        assert_eq!(parsed.identity.v1.unwrap(), "00".repeat(20));
    }

    #[test]
    fn rejects_conflicting_or_malformed_identities_and_unbounded_names() {
        assert!(parse_magnet_uri(
            "magnet:?xt=urn:btih:0000000000000000000000000000000000000000&xt=urn:btih:1111111111111111111111111111111111111111"
        )
        .is_err());
        assert!(parse_magnet_uri("magnet:?xt=urn:btih:invalid").is_err());
        assert!(parse_magnet_uri(&format!("magnet:?xt=urn:btmh:1220{}", "0".repeat(64))).is_ok());
        assert!(
            parse_magnet_uri(&format!(
                "magnet:?xt=urn:btih:{}&dn={}",
                "0".repeat(40),
                "x".repeat(MAX_TORRENT_NAME_BYTES + 1)
            ))
            .is_err()
        );
    }

    #[test]
    fn debug_redacts_tracker_data_and_display_name_is_plain_text() {
        let uri = "magnet:?xt=urn:btih:0000000000000000000000000000000000000000&dn=public+name&tr=udp%3A%2F%2Ftracker.invalid%2Fannounce";
        let parsed = parse_magnet_uri(uri).unwrap();
        let input = TorrentMagnet {
            update_id: 1,
            chat_id: 2,
            message_id: 3,
            user_id: 4,
            uri: uri.to_string(),
        };
        assert!(!format!("{parsed:?}").contains("tracker.invalid"));
        assert!(!format!("{input:?}").contains("tracker.invalid"));
        assert_eq!(parsed.display_name.as_deref(), Some("public name"));
    }
}
