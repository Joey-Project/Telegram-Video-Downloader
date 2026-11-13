use anyhow::{Result, bail, ensure};
use std::io::{BufRead, Write};

pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

pub fn read_line(reader: &mut impl BufRead, maximum: usize) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let end = available.iter().position(|byte| *byte == b'\n');
        let count = end.map_or(available.len(), |position| position + 1);
        ensure!(line.len() + count <= maximum, "protocol line exceeds limit");
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if end.is_some() {
            return Ok(Some(line));
        }
    }
}

pub fn write_line(writer: &mut impl Write, value: &impl serde::Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_EVENT_BYTES {
        bail!("protocol event exceeds limit");
    }
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_reader_accepts_unicode_and_final_eof() {
        let input = "中文 日本語 한국어 Русский Français Italiano e\u{301} 🎵";
        assert_eq!(
            read_line(&mut input.as_bytes(), 1024).unwrap().unwrap(),
            input.as_bytes()
        );
        assert!(read_line(&mut &b"12345\n"[..], 5).is_err());
    }
}
