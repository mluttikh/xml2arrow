//! The `xml2arrow` crate provides functionality for parsing XML data into Apache Arrow record batches.
//!
//! This crate allows you to convert structured XML data into Arrow record batches,
//! which are a columnar data format widely used for data processing and analytics.
//! This can be particularly useful for working with XML data in Rust-based data pipelines.
pub mod config;

mod errors;
pub use errors::{Error, Result};

mod xml_parser;
pub use xml_parser::parse_xml;
mod xml_path;

pub use config::Config;
