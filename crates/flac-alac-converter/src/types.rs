use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u32,
    pub bits_per_sample: u32,
    pub frames: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Warning {
    pub code: String,
    pub message: String,
}

impl Warning {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataReport {
    pub mapped: Vec<String>,
    pub preserved: Vec<String>,
    pub unmapped: Vec<String>,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvertRequest {
    pub version: u32,
    pub input: PathBuf,
    pub output: PathBuf,
    #[serde(default = "allow_compatibility")]
    pub allow_compatibility: bool,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn allow_compatibility() -> bool {
    true
}
fn default_timeout_seconds() -> u64 {
    1800
}

impl ConvertRequest {
    pub fn new(input: impl Into<PathBuf>, output: impl Into<PathBuf>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            input: input.into(),
            output: output.into(),
            allow_compatibility: true,
            timeout_seconds: default_timeout_seconds(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    Convert(ConvertRequest),
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionReport {
    pub version: u32,
    pub source: AudioSpec,
    pub output: AudioSpec,
    pub output_path: PathBuf,
    pub input_sha256: String,
    pub output_sha256: String,
    pub pcm_sha256: String,
    pub audio_bit_exact: bool,
    pub metadata: MetadataReport,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Started {
        version: u32,
        source: AudioSpec,
    },
    Progress {
        version: u32,
        frames: u64,
        total_frames: u64,
    },
    Warning {
        version: u32,
        warning: Warning,
    },
    Completed {
        version: u32,
        report: ConversionReport,
    },
    Failed {
        version: u32,
        code: String,
        message: String,
        partial_output: Option<PathBuf>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        staging_directory: Option<PathBuf>,
    },
    Cancelled {
        version: u32,
        partial_output: Option<PathBuf>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        staging_directory: Option<PathBuf>,
    },
}

impl Event {
    pub fn version(&self) -> u32 {
        match self {
            Self::Started { version, .. }
            | Self::Progress { version, .. }
            | Self::Warning { version, .. }
            | Self::Completed { version, .. }
            | Self::Failed { version, .. }
            | Self::Cancelled { version, .. } => *version,
        }
    }
}
