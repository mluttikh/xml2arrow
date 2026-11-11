//! The `xml2arrow` crate provides functionality for parsing XML data into Apache Arrow record batches.
//!
//! This crate allows you to convert structured XML data into Arrow record batches,
//! which are a columnar data format widely used for data processing and analytics.
//! This can be particularly useful for working with XML data in Rust-based data pipelines.
//!
//! # Where to start
//!
//! - [`Parser`] is the entry point: build one from a [`Config`], then call
//!   [`Parser::parse`] (any `BufRead`), [`Parser::parse_slice`] (zero-copy
//!   over bytes), or [`Parser::parse_batches`] to stream batches instead of
//!   collecting them.
//! - The [`config`] module documents the mapping from XML paths to tables and
//!   fields.
//! - [`Config::lint`] and [`Parser::warnings`] report configurations that are
//!   valid but commonly surprising. The library never prints; hosts log them.
//! - [`Error`] is the failure surface. Value-level errors carry the row and
//!   byte offset where the problem was found.

// Every public item carries documentation, and this keeps it that way: the
// config keys and error variants *are* the interface, so an undocumented one
// is a gap in the contract rather than a missing nicety. `warn` rather than
// `deny` so a work-in-progress build is not blocked; CI runs clippy with
// `-D warnings`, which promotes it.
#![warn(missing_docs)]

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
