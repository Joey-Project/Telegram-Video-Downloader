//! Direct FLAC-to-ALAC conversion through libFLAC and Apple's AudioToolbox.
//!
//! The native implementation is deliberately available only on macOS with the
//! `native` feature. Other targets keep the same API and return an explicit
//! unsupported error.

use crate::types::{AudioSpec, Warning};
use crate::wav::WavSource;
use anyhow::{Result, bail};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Controls<'a> {
    pub cancelled: &'a AtomicBool,
    pub progress: &'a mut dyn FnMut(u64) -> Result<()>,
    pub allow_compatibility: bool,
}

#[derive(Debug, Clone)]
pub struct Encoding {
    pub output: AudioSpec,
    pub input_pcm_sha256: String,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Clone)]
pub struct Verification {
    pub output: AudioSpec,
    pub output_pcm_sha256: String,
    pub frames: u64,
    pub bit_exact: bool,
}

#[cfg(all(feature = "native", target_os = "macos"))]
#[path = "native/macos.rs"]
mod macos;

pub fn encode(
    input: &Path,
    output: &Path,
    spec: &AudioSpec,
    controls: &mut Controls<'_>,
) -> Result<Encoding> {
    check_cancelled(controls)?;
    validate_spec(spec)?;
    let (output_bits, warnings) =
        plan_precision(spec.bits_per_sample, controls.allow_compatibility)?;
    #[cfg(all(feature = "native", target_os = "macos"))]
    {
        macos::encode(input, output, spec, output_bits, warnings, controls)
    }
    #[cfg(not(all(feature = "native", target_os = "macos")))]
    {
        let _ = (input, output, output_bits, warnings);
        bail!("direct FLAC-to-ALAC conversion is supported only on macOS with the `native` feature")
    }
}

/// Encode a validated WAVE PCM layout through the same AudioToolbox ALAC path
/// used by FLAC. The original WAVE file remains read-only.
pub fn encode_wav(
    input: &Path,
    output: &Path,
    source: &WavSource,
    controls: &mut Controls<'_>,
) -> Result<Encoding> {
    check_cancelled(controls)?;
    let spec = &source.metadata.audio;
    if source.metadata.kind != crate::metadata::SourceKind::Wav {
        bail!("WAVE encoder received non-WAVE source metadata");
    }
    validate_spec(spec)?;
    if !matches!(spec.bits_per_sample, 8 | 16 | 24 | 32)
        || source.container_bits != spec.bits_per_sample
    {
        bail!(
            "unsupported WAVE PCM precision {} bits in a {}-bit container",
            spec.bits_per_sample,
            source.container_bits
        );
    }
    let (output_bits, warnings) =
        plan_wav_precision(spec.bits_per_sample, controls.allow_compatibility)?;
    #[cfg(all(feature = "native", target_os = "macos"))]
    {
        macos::encode_wav(input, output, source, output_bits, warnings, controls)
    }
    #[cfg(not(all(feature = "native", target_os = "macos")))]
    {
        let _ = (input, output, source, output_bits, warnings);
        bail!("direct WAVE-to-ALAC conversion is supported only on macOS with the `native` feature")
    }
}

pub fn verify(
    output: &Path,
    encoding: &Encoding,
    controls: &mut Controls<'_>,
) -> Result<Verification> {
    check_cancelled(controls)?;
    validate_spec(&encoding.output)?;
    #[cfg(all(feature = "native", target_os = "macos"))]
    {
        macos::verify(output, encoding, controls)
    }
    #[cfg(not(all(feature = "native", target_os = "macos")))]
    {
        let _ = (output, encoding);
        bail!("direct ALAC verification is supported only on macOS with the `native` feature")
    }
}

fn validate_spec(spec: &AudioSpec) -> Result<()> {
    if !(1..=2).contains(&spec.channels) {
        bail!(
            "unsupported FLAC channel layout with {} channels; this version supports mono and stereo only and will not downmix multichannel audio because AudioSpec has no speaker-layout field",
            spec.channels
        );
    }
    if spec.sample_rate == 0 {
        bail!("FLAC sample rate must be nonzero");
    }
    if !(4..=32).contains(&spec.bits_per_sample) {
        bail!(
            "unsupported FLAC sample precision {}; expected 4 through 32 bits",
            spec.bits_per_sample
        );
    }
    if spec.frames == 0 {
        bail!(
            "FLAC STREAMINFO total sample count must be known and nonzero; unknown-length streams are unsupported"
        );
    }
    Ok(())
}

fn plan_precision(input_bits: u32, allow_compatibility: bool) -> Result<(u32, Vec<Warning>)> {
    let output_bits = alac_source_bits(input_bits)?;
    if output_bits == input_bits {
        return Ok((output_bits, Vec::new()));
    }
    if !allow_compatibility {
        bail!(
            "FLAC precision {input_bits} is not a direct ALAC source depth; lossless promotion to {output_bits} bits requires compatibility mode"
        );
    }
    Ok((
        output_bits,
        vec![Warning::new(
            "precision_promoted",
            format!(
                "Promoted {input_bits}-bit FLAC samples to {output_bits}-bit ALAC source precision without changing sample values."
            ),
        )],
    ))
}

fn plan_wav_precision(input_bits: u32, allow_compatibility: bool) -> Result<(u32, Vec<Warning>)> {
    let output_bits = alac_source_bits(input_bits)?;
    if output_bits == input_bits {
        return Ok((output_bits, Vec::new()));
    }
    if !allow_compatibility {
        bail!(
            "{input_bits}-bit WAVE PCM is not a direct ALAC source depth; lossless promotion to {output_bits} bits requires compatibility mode"
        );
    }
    Ok((
        output_bits,
        vec![Warning::new(
            "precision_promoted",
            format!(
                "Promoted {input_bits}-bit WAVE PCM samples to {output_bits}-bit ALAC source precision without changing sample values."
            ),
        )],
    ))
}

fn check_cancelled(controls: &Controls<'_>) -> Result<()> {
    if controls.cancelled.load(Ordering::Relaxed) {
        bail!("conversion cancelled");
    }
    Ok(())
}

pub(super) fn full_scale_sample(sample: i32, bits: u32) -> Option<i32> {
    if !(4..=32).contains(&bits) {
        return None;
    }
    let wide = i64::from(sample);
    let min = -(1_i64 << (bits - 1));
    let max = (1_i64 << (bits - 1)) - 1;
    if wide < min || wide > max {
        return None;
    }
    Some(sample.wrapping_shl(32 - bits))
}

fn alac_source_bits(input_bits: u32) -> Result<u32> {
    match input_bits {
        4..=16 => Ok(16),
        17..=20 => Ok(20),
        21..=24 => Ok(24),
        25..=32 => Ok(32),
        _ => bail!("unsupported FLAC precision {input_bits}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    fn spec() -> AudioSpec {
        AudioSpec {
            sample_rate: 48_000,
            channels: 2,
            bits_per_sample: 24,
            frames: 1,
        }
    }

    #[test]
    fn alac_precision_plan_promotes_only_to_supported_lossless_depths() {
        for (source, target) in [
            (4, 16),
            (15, 16),
            (16, 16),
            (17, 20),
            (20, 20),
            (21, 24),
            (24, 24),
            (25, 32),
            (32, 32),
        ] {
            let (output, warnings) = plan_precision(source, true).unwrap();
            assert_eq!(output, target);
            assert_eq!(warnings.is_empty(), source == target);
        }
    }

    #[test]
    fn strict_mode_rejects_non_native_alac_precision_before_file_access() {
        let mut source = spec();
        source.bits_per_sample = 17;
        let cancelled = AtomicBool::new(false);
        let mut progress = |_| Ok(());
        let mut controls = Controls {
            cancelled: &cancelled,
            progress: &mut progress,
            allow_compatibility: false,
        };
        let error = encode(
            Path::new("missing.flac"),
            Path::new("missing.m4a"),
            &source,
            &mut controls,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("requires compatibility mode"));
    }

    #[test]
    fn rejects_multichannel_and_unknown_length_specs_without_downmixing() {
        let cancelled = AtomicBool::new(false);
        let mut source = spec();
        source.channels = 6;
        let mut progress = |_| Ok(());
        let mut controls = Controls {
            cancelled: &cancelled,
            progress: &mut progress,
            allow_compatibility: true,
        };
        let error = encode(
            Path::new("missing.flac"),
            Path::new("missing.m4a"),
            &source,
            &mut controls,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("mono and stereo only"));
        assert!(error.contains("will not downmix"));

        let mut source = spec();
        source.frames = 0;
        let mut progress = |_| Ok(());
        let mut controls = Controls {
            cancelled: &cancelled,
            progress: &mut progress,
            allow_compatibility: true,
        };
        let error = encode(
            Path::new("missing.flac"),
            Path::new("missing.m4a"),
            &source,
            &mut controls,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("total sample count must be known"));
    }

    #[test]
    fn cancellation_is_checked_before_opening_inputs_or_outputs() {
        let cancelled = AtomicBool::new(true);
        let mut progress = |_| Ok(());
        let mut controls = Controls {
            cancelled: &cancelled,
            progress: &mut progress,
            allow_compatibility: true,
        };
        let error = encode(
            Path::new("missing.flac"),
            Path::new("missing.m4a"),
            &spec(),
            &mut controls,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("conversion cancelled"));
    }

    #[test]
    fn full_scale_pcm_conversion_preserves_32_bit_values_and_high_alignment() {
        assert_eq!(full_scale_sample(i32::MIN, 32), Some(i32::MIN));
        assert_eq!(full_scale_sample(i32::MAX, 32), Some(i32::MAX));
        assert_eq!(full_scale_sample(-8_388_608, 24), Some(i32::MIN));
        assert_eq!(full_scale_sample(8_388_607, 24), Some(2_147_483_392));
        assert_eq!(full_scale_sample(32_767, 16), Some(2_147_418_112));
        assert_eq!(full_scale_sample(32_768, 16), None);
    }
}
