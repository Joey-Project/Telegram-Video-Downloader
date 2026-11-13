//! File-only native conversion, protocol types and an isolated-process client.
pub mod client;
#[cfg(feature = "native")]
pub mod engine;
#[cfg(feature = "native")]
pub mod metadata;
#[cfg(feature = "native")]
pub mod native;
pub mod protocol;
pub mod types;

pub use client::ConverterClient;
pub use types::{ConversionReport, ConvertRequest, Event};
