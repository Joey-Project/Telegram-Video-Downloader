//! Isolated lossless-source-to-ALAC process adapter for Music preparation.

use anyhow::{Context, Result, bail, ensure};
use flac_alac_converter::client::ConverterClient;
use flac_alac_converter::types::{
    AudioSpec, ConversionReport, ConvertRequest, Event, PROTOCOL_VERSION,
};
use serde::Serialize;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::music_types::{AudioBytes, MusicCancellation, MusicSettings};

const MAX_REPORT_BYTES: usize = 4 * 1024 * 1024;
const MAX_FAILURE_RECORD_BYTES: usize = 64 * 1024;
const MAX_FAILURE_TEXT_BYTES: usize = 16 * 1024;
const MAX_RETAINED_STAGING_PATHS: usize = 32;

static OUTPUT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Default)]
pub(crate) struct ConversionTrace {
    pub output_path: Option<PathBuf>,
    pub partial_output: Option<PathBuf>,
    pub staging_directory: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct ConversionOutput {
    pub path: PathBuf,
    pub report: ConversionReport,
}

trait ConverterProcess {
    fn convert(
        &self,
        request: &ConvertRequest,
        cancelled: &AtomicBool,
        observe: &mut dyn FnMut(&Event) -> Result<()>,
    ) -> Result<ConversionReport>;
}

impl ConverterProcess for ConverterClient {
    fn convert(
        &self,
        request: &ConvertRequest,
        cancelled: &AtomicBool,
        observe: &mut dyn FnMut(&Event) -> Result<()>,
    ) -> Result<ConversionReport> {
        ConverterClient::convert(self, request, cancelled, observe)
    }
}

pub(crate) fn convert_lossless(
    source: &AudioBytes,
    working_dir: &Path,
    settings: &MusicSettings,
    cancellation: &MusicCancellation,
    trace: &mut ConversionTrace,
) -> Result<ConversionOutput> {
    let client = ConverterClient::new(settings.converter.clone());
    convert_lossless_with(
        &client,
        source,
        working_dir,
        settings.conversion_timeout_seconds,
        cancellation,
        trace,
    )
}

fn convert_lossless_with(
    client: &dyn ConverterProcess,
    source: &AudioBytes,
    working_dir: &Path,
    timeout_seconds: u64,
    cancellation: &MusicCancellation,
    trace: &mut ConversionTrace,
) -> Result<ConversionOutput> {
    ensure!(
        (1..=86_400).contains(&timeout_seconds),
        "Music conversion timeout must be between 1 and 86400 seconds"
    );
    ensure!(
        source.sha256.len() == 64 && source.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "lossless source SHA-256 is invalid"
    );
    let canonical_working_dir = fs::canonicalize(working_dir).with_context(|| {
        format!(
            "resolve private prepare-attempt directory {}",
            working_dir.display()
        )
    })?;
    let output = unique_output_path(&canonical_working_dir, &source.sha256)?;
    trace.output_path = Some(output.clone());

    let mut request = ConvertRequest::new(&source.path, &output);
    request.allow_compatibility = true;
    request.timeout_seconds = timeout_seconds;
    let result = client.convert(&request, cancellation.atomic(), &mut |event| {
        match event {
            Event::Failed {
                partial_output,
                staging_directory,
                ..
            }
            | Event::Cancelled {
                partial_output,
                staging_directory,
                ..
            } => {
                trace.partial_output = partial_output.clone();
                trace.staging_directory = staging_directory.clone();
            }
            _ => {}
        }
        Ok(())
    });

    let report = result.map_err(|error| {
        if let Some(path) = trace.staging_directory.as_ref() {
            error.context(format!(
            "lossless audio conversion failed; retained converter staging is at {}",
                path.display()
            ))
        } else {
            error.context("lossless audio conversion failed; inspect the private prepare-attempt directory for retained staging")
        }
    })?;
    let canonical_output = fs::canonicalize(&output).with_context(|| {
        format!(
            "resolve completed lossless conversion output {}",
            output.display()
        )
    })?;
    validate_conversion_report(&report, &source.sha256, &canonical_output)?;
    trace.output_path = Some(canonical_output.clone());
    Ok(ConversionOutput {
        path: canonical_output,
        report,
    })
}

pub(crate) fn validate_conversion_report(
    report: &ConversionReport,
    source_sha256: &str,
    output_path: &Path,
) -> Result<()> {
    ensure!(
        report.version == PROTOCOL_VERSION,
        "converter report protocol version is unsupported"
    );
    ensure!(
        report.audio_bit_exact,
        "converter did not verify bit-exact audio; lossy or altered output is rejected"
    );
    ensure!(
        same_sha256(&report.input_sha256, source_sha256),
        "converter report input SHA-256 does not match the original audio"
    );
    ensure!(
        is_sha256(&report.output_sha256),
        "converter output SHA-256 is invalid"
    );
    ensure!(
        is_sha256(&report.pcm_sha256),
        "converter PCM SHA-256 is invalid"
    );
    ensure!(
        report.output_path == output_path,
        "converter report names a different output path"
    );
    ensure!(
        valid_audio_spec(&report.source) && valid_audio_spec(&report.output),
        "converter report audio properties are invalid"
    );
    ensure!(
        report.source.sample_rate == report.output.sample_rate
            && report.source.channels == report.output.channels
            && report.source.frames == report.output.frames,
        "converter changed sample rate, channel count, or frame count"
    );
    ensure!(
        matches!(report.output.bits_per_sample, 16 | 20 | 24 | 32),
        "converter output bit depth is not supported by ALAC"
    );
    Ok(())
}

pub(crate) fn persist_success_report(working_dir: &Path, report: &ConversionReport) -> Result<()> {
    let bytes =
        serde_json::to_vec_pretty(report).context("serialize lossless conversion report")?;
    ensure!(
        bytes.len() <= MAX_REPORT_BYTES,
        "lossless conversion report exceeds the private persistence limit"
    );
    create_private_record(working_dir.join("conversion-report.json"), &bytes)
        .context("persist complete lossless conversion report without replacement")
}

pub(crate) fn persist_failure_record(
    working_dir: &Path,
    trace: &ConversionTrace,
    error: &anyhow::Error,
) -> Result<()> {
    let record = FailureRecord {
        version: 1,
        error: bounded_text(&format!("{error:#}"), MAX_FAILURE_TEXT_BYTES),
        output_path: trace.output_path.clone(),
        partial_output: trace.partial_output.clone(),
        staging_directory: trace.staging_directory.clone(),
        retained_staging_directories: find_retained_staging_directories(working_dir)?,
    };
    let bytes =
        serde_json::to_vec_pretty(&record).context("serialize lossless conversion failure")?;
    ensure!(
        bytes.len() <= MAX_FAILURE_RECORD_BYTES,
        "lossless conversion failure record exceeds its persistence limit"
    );
    create_private_record(working_dir.join("conversion-failure.json"), &bytes)
        .context("persist bounded lossless conversion failure without replacement")
}

#[derive(Serialize)]
struct FailureRecord {
    version: u32,
    error: String,
    output_path: Option<PathBuf>,
    partial_output: Option<PathBuf>,
    staging_directory: Option<PathBuf>,
    retained_staging_directories: Vec<PathBuf>,
}

fn create_private_record(path: PathBuf, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .with_context(|| format!("create private report {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write private report {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync private report {}", path.display()))?;
    Ok(())
}

fn find_retained_staging_directories(working_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = fs::read_dir(working_dir).with_context(|| {
        format!(
            "inspect prepare-attempt directory {}",
            working_dir.display()
        )
    })?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.context("read prepare-attempt directory entry")?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(".flac-alac-")
        {
            continue;
        }
        let file_type = entry
            .file_type()
            .context("inspect retained converter staging entry")?;
        if file_type.is_dir() {
            paths.push(entry.path());
            if paths.len() >= MAX_RETAINED_STAGING_PATHS {
                break;
            }
        }
    }
    Ok(paths)
}

fn unique_output_path(working_dir: &Path, sha256: &str) -> Result<PathBuf> {
    ensure!(
        working_dir.is_absolute(),
        "lossless prepare directory must be absolute"
    );
    for _ in 0..128 {
        let sequence = OUTPUT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let candidate = working_dir.join(format!(
            "lossless-alac-{}-{}-{}-{}.m4a",
            &sha256[..16],
            std::process::id(),
            sequence,
            stamp
        ));
        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
            Ok(_) => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect reserved lossless conversion path {}",
                        candidate.display()
                    )
                });
            }
        }
    }
    bail!("could not reserve a unique lossless-to-ALAC output path")
}

fn valid_audio_spec(spec: &AudioSpec) -> bool {
    (1..=2).contains(&spec.channels)
        && spec.sample_rate > 0
        && (4..=32).contains(&spec.bits_per_sample)
        && spec.frames > 0
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn same_sha256(left: &str, right: &str) -> bool {
    is_sha256(left) && is_sha256(right) && left.eq_ignore_ascii_case(right)
}

fn bounded_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flac_alac_converter::types::{MetadataReport, Warning};
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let root =
                fs::canonicalize(std::env::temp_dir()).expect("resolve system temporary directory");
            for sequence in 0..128_u64 {
                let stamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after Unix epoch")
                    .as_nanos();
                let path = root.join(format!(
                    "music-conversion-test-{}-{sequence}-{stamp}",
                    std::process::id()
                ));
                match fs::create_dir(&path) {
                    Ok(()) => {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                                .expect("make private test directory");
                        }
                        return Self(path);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("create test directory: {error}"),
                }
            }
            panic!("could not create a unique test directory")
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct MockConverter {
        source_sha256: String,
        output_contents: Vec<u8>,
        expect_cancelled: bool,
        warning: Option<Warning>,
        fail_with_stage: Option<PathBuf>,
    }

    impl ConverterProcess for MockConverter {
        fn convert(
            &self,
            request: &ConvertRequest,
            cancelled: &AtomicBool,
            observe: &mut dyn FnMut(&Event) -> Result<()>,
        ) -> Result<ConversionReport> {
            assert_eq!(request.timeout_seconds, 37);
            assert!(request.allow_compatibility);
            assert_eq!(cancelled.load(Ordering::Acquire), self.expect_cancelled);
            if let Some(staging_directory) = &self.fail_with_stage {
                observe(&Event::Failed {
                    version: PROTOCOL_VERSION,
                    code: "fixture_failure".to_owned(),
                    message: "mock worker failure".to_owned(),
                    partial_output: None,
                    staging_directory: Some(staging_directory.clone()),
                })?;
                bail!("mock conversion failed");
            }
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&request.output)
                .expect("mock converter publishes without replacement");
            output
                .write_all(&self.output_contents)
                .expect("write mock ALAC output");
            output.sync_all().expect("sync mock ALAC output");
            let output_path = fs::canonicalize(&request.output).expect("canonical mock output");
            if let Some(warning) = &self.warning {
                observe(&Event::Warning {
                    version: PROTOCOL_VERSION,
                    warning: warning.clone(),
                })?;
            }
            Ok(ConversionReport {
                version: PROTOCOL_VERSION,
                source: AudioSpec {
                    sample_rate: 44_100,
                    channels: 2,
                    bits_per_sample: 16,
                    frames: 100,
                },
                output: AudioSpec {
                    sample_rate: 44_100,
                    channels: 2,
                    bits_per_sample: 16,
                    frames: 100,
                },
                output_path,
                input_sha256: self.source_sha256.clone(),
                output_sha256: sha256(&self.output_contents),
                pcm_sha256: "b".repeat(64),
                audio_bit_exact: true,
                metadata: MetadataReport::default(),
                warnings: self.warning.iter().cloned().collect(),
            })
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn source(directory: &TestDirectory) -> AudioBytes {
        let path = directory.path().join("source.flac");
        fs::write(&path, b"test flac bytes").expect("write source fixture");
        AudioBytes {
            path,
            sha256: sha256(b"test flac bytes"),
            size: 15,
        }
    }

    #[test]
    fn process_adapter_uses_compatibility_mode_and_never_overwrites_outputs() {
        let directory = TestDirectory::new();
        let source = source(&directory);
        #[cfg(unix)]
        let work_dir = {
            let alias = directory.path().join("prepare-work-alias");
            std::os::unix::fs::symlink(directory.path(), &alias)
                .expect("create prepare directory alias");
            alias
        };
        #[cfg(not(unix))]
        let work_dir = directory.path().to_path_buf();
        let warning = Warning::new("precision_promoted", "source precision was promoted");
        let converter = MockConverter {
            source_sha256: source.sha256.clone(),
            output_contents: b"verified alac test bytes".to_vec(),
            expect_cancelled: false,
            warning: Some(warning.clone()),
            fail_with_stage: None,
        };
        let settings = MusicSettings {
            converter: PathBuf::from("mock-converter"),
            conversion_timeout_seconds: 37,
            ..MusicSettings::default()
        };
        let cancellation = MusicCancellation::default();
        let mut first_trace = ConversionTrace::default();
        let first = convert_lossless_with(
            &converter,
            &source,
            &work_dir,
            settings.conversion_timeout_seconds,
            &cancellation,
            &mut first_trace,
        )
        .expect("convert with mock isolated process");
        assert_eq!(first.report.warnings, vec![warning]);
        assert_eq!(fs::read(&first.path).unwrap(), converter.output_contents);
        assert_eq!(first.path.parent(), Some(directory.path()));

        let mut second_trace = ConversionTrace::default();
        let second = convert_lossless_with(
            &converter,
            &source,
            &work_dir,
            settings.conversion_timeout_seconds,
            &cancellation,
            &mut second_trace,
        )
        .expect("retry conversion without replacing the first result");
        assert_ne!(first.path, second.path);
        assert_eq!(fs::read(&first.path).unwrap(), converter.output_contents);
    }

    #[test]
    fn wav_conversion_report_binds_original_and_alac_hashes() {
        let directory = TestDirectory::new();
        let input_bytes = b"synthetic WAV source bytes";
        let output_bytes = b"verified ALAC import bytes";
        let path = directory.path().join("source.WAV");
        fs::write(&path, input_bytes).expect("write WAV source fixture");
        let source = AudioBytes {
            path,
            sha256: sha256(input_bytes),
            size: input_bytes.len() as u64,
        };
        let converter = MockConverter {
            source_sha256: source.sha256.clone(),
            output_contents: output_bytes.to_vec(),
            expect_cancelled: false,
            warning: None,
            fail_with_stage: None,
        };
        let mut trace = ConversionTrace::default();

        let converted = convert_lossless_with(
            &converter,
            &source,
            directory.path(),
            37,
            &MusicCancellation::default(),
            &mut trace,
        )
        .expect("WAV source is sent through the existing conversion protocol");

        assert_ne!(source.sha256, converted.report.output_sha256);
        assert_eq!(converted.report.input_sha256, source.sha256);
        assert_eq!(converted.report.output_sha256, sha256(output_bytes));
        validate_conversion_report(&converted.report, &source.sha256, &converted.path)
            .expect("conversion report remains bound to the source and ALAC output");
        persist_success_report(directory.path(), &converted.report)
            .expect("persist the complete WAV conversion report");
        let saved: ConversionReport = serde_json::from_slice(
            &fs::read(directory.path().join("conversion-report.json")).unwrap(),
        )
        .expect("parse the saved WAV conversion report");
        assert_eq!(saved.input_sha256, source.sha256);
        assert_eq!(saved.output_sha256, sha256(output_bytes));
    }

    #[test]
    fn cancellation_flag_is_passed_to_the_converter_client() {
        let directory = TestDirectory::new();
        let source = source(&directory);
        let converter = MockConverter {
            source_sha256: source.sha256.clone(),
            output_contents: b"verified alac test bytes".to_vec(),
            expect_cancelled: true,
            warning: None,
            fail_with_stage: None,
        };
        let cancellation = MusicCancellation::default();
        cancellation.cancel();
        let mut trace = ConversionTrace::default();
        let output = convert_lossless_with(
            &converter,
            &source,
            directory.path(),
            37,
            &cancellation,
            &mut trace,
        )
        .expect("mock observes the same cancellation flag");
        assert!(output.path.is_file());
    }

    #[test]
    fn conversion_report_rejects_pcm_changes_and_resampling_or_downmixing() {
        let path = PathBuf::from("/tmp/result.m4a");
        let mut report = ConversionReport {
            version: PROTOCOL_VERSION,
            source: AudioSpec {
                sample_rate: 48_000,
                channels: 2,
                bits_per_sample: 24,
                frames: 200,
            },
            output: AudioSpec {
                sample_rate: 48_000,
                channels: 2,
                bits_per_sample: 24,
                frames: 200,
            },
            output_path: path.clone(),
            input_sha256: "a".repeat(64),
            output_sha256: "b".repeat(64),
            pcm_sha256: "c".repeat(64),
            audio_bit_exact: true,
            metadata: MetadataReport::default(),
            warnings: Vec::new(),
        };
        validate_conversion_report(&report, &"a".repeat(64), &path)
            .expect("bit-exact same-rate stereo conversion is accepted");

        report.audio_bit_exact = false;
        assert!(validate_conversion_report(&report, &"a".repeat(64), &path).is_err());
        report.audio_bit_exact = true;
        report.output.sample_rate = 44_100;
        assert!(validate_conversion_report(&report, &"a".repeat(64), &path).is_err());
        report.output.sample_rate = 48_000;
        report.output.channels = 1;
        assert!(validate_conversion_report(&report, &"a".repeat(64), &path).is_err());
    }

    #[test]
    fn failed_process_records_include_the_retained_staging_path() {
        let directory = TestDirectory::new();
        let source = source(&directory);
        let stage = directory.path().join(".flac-alac-failed-stage");
        fs::create_dir(&stage).expect("create retained worker staging fixture");
        let converter = MockConverter {
            source_sha256: source.sha256.clone(),
            output_contents: Vec::new(),
            expect_cancelled: false,
            warning: None,
            fail_with_stage: Some(stage.clone()),
        };
        let cancellation = MusicCancellation::default();
        let mut trace = ConversionTrace::default();
        let error = convert_lossless_with(
            &converter,
            &source,
            directory.path(),
            37,
            &cancellation,
            &mut trace,
        )
        .expect_err("mock worker should fail");
        assert!(format!("{error:#}").contains(stage.to_str().unwrap()));
        assert_eq!(trace.staging_directory, Some(stage.clone()));
        persist_failure_record(directory.path(), &trace, &error)
            .expect("persist bounded private failure record");
        let value: serde_json::Value = serde_json::from_slice(
            &fs::read(directory.path().join("conversion-failure.json")).unwrap(),
        )
        .expect("parse failure record");
        assert_eq!(value["staging_directory"], stage.to_str().unwrap());
        assert!(
            !value["error"]
                .as_str()
                .unwrap()
                .contains("mock worker failure")
        );
    }

    #[test]
    fn conversion_report_is_persisted_create_only_with_private_permissions() {
        let directory = TestDirectory::new();
        let path = directory.path().join("result.m4a");
        fs::write(&path, b"output").expect("create report output fixture");
        let report = ConversionReport {
            version: PROTOCOL_VERSION,
            source: AudioSpec {
                sample_rate: 44_100,
                channels: 2,
                bits_per_sample: 16,
                frames: 100,
            },
            output: AudioSpec {
                sample_rate: 44_100,
                channels: 2,
                bits_per_sample: 16,
                frames: 100,
            },
            output_path: path,
            input_sha256: "a".repeat(64),
            output_sha256: "b".repeat(64),
            pcm_sha256: "c".repeat(64),
            audio_bit_exact: true,
            metadata: MetadataReport::default(),
            warnings: vec![Warning::new("metadata", "metadata retained")],
        };
        persist_success_report(directory.path(), &report).expect("persist report");
        let record = directory.path().join("conversion-report.json");
        let saved: ConversionReport =
            serde_json::from_slice(&fs::read(&record).unwrap()).expect("parse saved report");
        assert_eq!(saved.output_sha256, report.output_sha256);
        assert!(persist_success_report(directory.path(), &report).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(record).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn failure_record_is_bounded_and_identifies_retained_staging() {
        let directory = TestDirectory::new();
        let stage = directory.path().join(".flac-alac-test-stage");
        fs::create_dir(&stage).expect("create retained staging fixture");
        let trace = ConversionTrace {
            output_path: Some(directory.path().join("result.m4a")),
            partial_output: None,
            staging_directory: Some(stage.clone()),
            // The filesystem scan also finds staging left behind without an event.
        };
        let error = anyhow::anyhow!("worker failed: {}", "x".repeat(MAX_FAILURE_TEXT_BYTES * 2));
        persist_failure_record(directory.path(), &trace, &error)
            .expect("persist bounded failure record");
        let value: serde_json::Value = serde_json::from_slice(
            &fs::read(directory.path().join("conversion-failure.json")).unwrap(),
        )
        .expect("parse failure record");
        assert_eq!(
            value["staging_directory"].as_str(),
            Some(stage.to_str().unwrap())
        );
        assert!(value["error"].as_str().unwrap().len() <= MAX_FAILURE_TEXT_BYTES);
        assert_eq!(
            value["retained_staging_directories"][0],
            stage.to_str().unwrap()
        );
        assert!(persist_failure_record(directory.path(), &trace, &error).is_err());
    }
}
