//! The `xml2arrow` crate provides functionality for parsing XML data into Apache Arrow record batches.
//!
//! This crate allows you to convert structured XML data into Arrow record batches,
//! which are a columnar data format widely used for data processing and analytics.
//! This can be particularly useful for working with XML data in Rust-based data pipelines.
pub mod config;

pub mod errors;
pub use errors::{Error, Result};

pub mod lint;
pub use lint::Lint;

mod path_registry;
mod xml_parser;
// The two free functions are deprecated (see their notes); re-exporting them
// is not itself a use worth warning about.
#[allow(deprecated)]
pub use xml_parser::{
    BatchOptions, BatchStream, EventSource, Parser, ReaderSource, SingleTableReader, SliceSource,
    TableBatch, parse_xml, parse_xml_slice,
};

pub use config::{
    Config, ConfigBuilder, DType, FieldConfig, FieldConfigBuilder, ParserOptions, TableConfig,
    TableConfigBuilder,
};
