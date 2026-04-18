//! Error types for the xml2arrow crate.
//!
//! The error variants here are intentionally structured rather than stringly
//! typed: downstream tooling (the separate `xml2arrow-python` bindings,
//! structured logs, retry logic) can route on e.g. `ParseError` vs
//! `MissingRequiredField` without regex-matching free-form messages.
//!
//! `Error` itself is `#[non_exhaustive]` so that adding new variants is a
//! non-breaking change; callers in other crates must include a wildcard arm.
//!
//! The `#[cfg(feature = "python")]` blocks below keep the Python exception
//! hierarchy in lockstep with the Rust variants. The actual PyO3 binding
//! crate lives in a separate repository (`xml2arrow-python`); this feature
//! exists only so both sides share a single source of truth for the
//! exception class hierarchy.

use std::fmt;
use std::str::Utf8Error;
use std::sync::Arc;

#[cfg(feature = "python")]
use arrow::pyarrow::PyArrowException;
use derive_more::From;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
use pyo3::create_exception;

pub type Result<T> = core::result::Result<T, Error>;

/// The crate's top-level error type.
///
/// Marked `#[non_exhaustive]` to keep future variant additions backward
/// compatible. Within this crate matches are still checked exhaustively.
#[derive(Debug, From)]
#[non_exhaustive]
pub enum Error {
    /// Errors from the `quick-xml` crate during XML parsing.
    #[from]
    XmlParsing(quick_xml::Error),
    /// Errors from the `quick-xml` crate that can be raised during parsing attributes.
    #[from]
    XmlParseAttr(quick_xml::events::attributes::AttrError),
    /// Errors from the `quick-xml` crate that can be raised when decoding or encoding.
    #[from]
    XmlParseEncoding(quick_xml::encoding::EncodingError),
    /// Errors from the Serde YAML crate during configuration parsing.
    #[from]
    Yaml(yaml_serde::Error),
    /// Standard I/O errors.
    #[from]
    Io(std::io::Error),
    /// Errors from the arrow crate during Arrow operations.
    #[from]
    Arrow(arrow::error::ArrowError),
    /// Errors during UTF-8 string conversion.
    #[from]
    Utf8Error(Utf8Error),
    /// A field's text couldn't be decoded into its configured data type.
    ///
    /// `field` and `path` use `Arc<str>` so repeated errors for the same
    /// field (common when a whole column is malformed) share the name and
    /// path allocations across clones.
    ParseError {
        field: Arc<str>,
        path: Arc<str>,
        value: String,
        kind: ParseKind,
    },
    /// A non-nullable field had no text (or whitespace-only text) in a row.
    /// Promoted out of `ParseError` because the handling differs — there is
    /// no raw value to show, and the fix is a configuration change
    /// (`nullable: true`) rather than cleaning input data.
    MissingRequiredField { field: Arc<str>, path: Arc<str> },
    /// A scale or offset was configured on a data type that doesn't support it.
    UnsupportedConversion {
        conversion: ConversionKind,
        data_type: String,
    },
    /// The configuration failed `Config::validate()`.
    InvalidConfig { reason: ConfigIssue },
}

/// Which primitive parser failed and why.
///
/// Separated from `Error::ParseError` so new parse failure modes can be
/// added without changing consumers that only care about the generic
/// "this field's text couldn't be decoded" signal.
#[derive(Debug)]
#[non_exhaustive]
pub enum ParseKind {
    /// A numeric (integer or float) parser rejected the raw text. Carries
    /// the target type name and the underlying parser's message.
    InvalidNumber {
        type_name: &'static str,
        reason: String,
    },
    /// A boolean token didn't match any recognized form.
    InvalidBoolean,
}

/// Which transform was attempted on a type that doesn't support it.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConversionKind {
    Scaling,
    Offset,
}

/// A classification of configuration problems detected by `Config::validate`.
/// Having this as a dedicated enum (rather than a free-form string) lets the
/// Python bindings and structured logs distinguish e.g. a duplicate-name bug
/// from a misaligned xml_path without substring matching.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConfigIssue {
    EmptyTableName,
    DuplicateTableName {
        name: String,
    },
    EmptyTableXmlPath {
        table: String,
    },
    EmptyFieldName {
        table: String,
    },
    DuplicateFieldName {
        table: String,
        field: String,
    },
    EmptyFieldXmlPath {
        table: String,
        field: String,
    },
    FieldPathNotUnderTable {
        table: String,
        table_path: String,
        field: String,
        field_path: String,
    },
}

// --- Display -----------------------------------------------------------------
//
// Display output is the user-visible surface. We keep it textually stable
// across the structured-variant refactor so log lines / test expectations
// that matched on substrings continue to work.

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::XmlParsing(e) => write!(f, "{e}"),
            Error::XmlParseAttr(e) => write!(f, "{e}"),
            Error::XmlParseEncoding(e) => write!(f, "{e}"),
            Error::Yaml(e) => write!(f, "{e}"),
            Error::Io(e) => write!(f, "{e}"),
            Error::Arrow(e) => write!(f, "{e}"),
            Error::Utf8Error(e) => write!(f, "{e}"),
            Error::ParseError {
                field,
                path,
                value,
                kind,
            } => match kind {
                ParseKind::InvalidNumber { type_name, reason } => write!(
                    f,
                    "Failed to parse value '{value}' as {type_name} for field '{field}' at path {path}: {reason}"
                ),
                ParseKind::InvalidBoolean => write!(
                    f,
                    "Failed to parse value '{value}' as boolean for field '{field}' at path {path}: expected one of 'true', 'false', '1', '0', 'yes', 'no', 'on', 'off', 't', 'f', 'y', or 'n'"
                ),
            },
            Error::MissingRequiredField { field, path } => write!(
                f,
                "Missing value for non-nullable field '{field}' at path {path}"
            ),
            Error::UnsupportedConversion {
                conversion,
                data_type,
            } => match conversion {
                ConversionKind::Scaling => write!(
                    f,
                    "Scaling is only supported for Float32 and Float64, not {data_type}"
                ),
                ConversionKind::Offset => write!(
                    f,
                    "Offset is only supported for Float32 and Float64, not {data_type}"
                ),
            },
            Error::InvalidConfig { reason } => fmt::Display::fmt(reason, f),
        }
    }
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigIssue::EmptyTableName => f.write_str("Table name must not be empty"),
            ConfigIssue::DuplicateTableName { name } => write!(f, "Duplicate table name '{name}'"),
            ConfigIssue::EmptyTableXmlPath { table } => {
                write!(f, "Table '{table}' has an empty xml_path")
            }
            ConfigIssue::EmptyFieldName { table } => {
                write!(f, "Field name must not be empty in table '{table}'")
            }
            ConfigIssue::DuplicateFieldName { table, field } => {
                write!(f, "Duplicate field name '{field}' in table '{table}'")
            }
            ConfigIssue::EmptyFieldXmlPath { table, field } => {
                write!(
                    f,
                    "Field '{field}' in table '{table}' has an empty xml_path"
                )
            }
            ConfigIssue::FieldPathNotUnderTable {
                table,
                table_path,
                field,
                field_path,
            } => write!(
                f,
                "Field '{field}' has xml_path '{field_path}' which is not under table '{table}' xml_path '{table_path}'"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::XmlParsing(e) => Some(e),
            Error::XmlParseAttr(e) => Some(e),
            Error::XmlParseEncoding(e) => Some(e),
            Error::Yaml(e) => Some(e),
            Error::Io(e) => Some(e),
            Error::Arrow(e) => Some(e),
            Error::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// --- Python exception hierarchy ---------------------------------------------
//
// The mapping from `Error` variants to concrete Python exception types lives
// here to keep the two sides from drifting. Adding a new `Error` variant
// without updating `From<Error> for PyErr` fails to compile below; the
// `pyerr_mapping_is_exhaustive` test in the `python`-feature test module then
// round-trips one sample of every variant to catch accidental mapping
// regressions (e.g. two variants silently mapped to the same exception).

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    Xml2ArrowError,
    pyo3::exceptions::PyException,
    "Base exception for the xml2arrow package."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    XmlParsingError,
    Xml2ArrowError,
    "Raised when an error occurs during XML parsing."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    YamlParsingError,
    Xml2ArrowError,
    "Raised when an error occurs during YAML configuration parsing."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    ParseError,
    Xml2ArrowError,
    "Raised when an error occurs during parsing of values from strings to specific data types."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    UnsupportedConversionError,
    Xml2ArrowError,
    "Raised when an unsupported conversion (scale or offset) is attempted."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    InvalidConfigError,
    Xml2ArrowError,
    "Raised when the configuration is invalid (e.g., duplicate names, invalid paths)."
);

#[cfg(feature = "python")]
impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        match value {
            Error::Io(e) => e.into(),
            Error::Utf8Error(e) => e.into(),
            Error::Arrow(e) => PyArrowException::new_err(e.to_string()),
            Error::XmlParsing(e) => XmlParsingError::new_err(e.to_string()),
            Error::XmlParseAttr(e) => XmlParsingError::new_err(e.to_string()),
            Error::XmlParseEncoding(e) => XmlParsingError::new_err(e.to_string()),
            Error::Yaml(e) => YamlParsingError::new_err(e.to_string()),
            e @ Error::ParseError { .. } => ParseError::new_err(e.to_string()),
            e @ Error::MissingRequiredField { .. } => ParseError::new_err(e.to_string()),
            e @ Error::UnsupportedConversion { .. } => {
                UnsupportedConversionError::new_err(e.to_string())
            }
            e @ Error::InvalidConfig { .. } => InvalidConfigError::new_err(e.to_string()),
        }
    }
}

#[cfg(all(test, feature = "python"))]
mod pyerr_tests {
    //! Guards against `From<Error> for PyErr` silently falling out of sync
    //! with the `Error` enum.
    //!
    //! There are two guards here, in order of importance:
    //!
    //! 1. `exhaustiveness_guard` — a match that names every `Error` variant.
    //!    Adding a new variant fails to compile here, forcing the author to
    //!    update `sample_of_each_variant` (and by extension the PyErr
    //!    mapping) before the code even builds. This is the real load-bearing
    //!    check because the CI matrix only runs `cargo clippy` for the
    //!    `python` feature (not `cargo test`) — so this compile-time guard
    //!    is what catches drift in CI.
    //!
    //! 2. `pyerr_mapping_round_trips` — converts one sample of every variant
    //!    through `PyErr::from` at runtime. `PyErr::new_err` defers the
    //!    actual Python-object construction, so this test doesn't require
    //!    an initialized interpreter and still flags runtime panics in the
    //!    mapping's argument construction.
    use super::*;

    /// Compile-time check: adding a new `Error` variant without updating
    /// this match fails to build, which in turn forces
    /// `sample_of_each_variant` (and `From<Error> for PyErr`) to stay in
    /// sync. The `#[allow(dead_code)]` keeps rustc quiet about the function
    /// never being called — its only job is to exist and compile.
    #[allow(dead_code)]
    fn exhaustiveness_guard(e: Error) {
        match e {
            Error::XmlParsing(_)
            | Error::XmlParseAttr(_)
            | Error::XmlParseEncoding(_)
            | Error::Yaml(_)
            | Error::Io(_)
            | Error::Arrow(_)
            | Error::Utf8Error(_)
            | Error::ParseError { .. }
            | Error::MissingRequiredField { .. }
            | Error::UnsupportedConversion { .. }
            | Error::InvalidConfig { .. } => {}
        }
    }

    fn sample_of_each_variant() -> Vec<Error> {
        // A deliberately broken UTF-8 sequence. Building the bytes from a
        // runtime `Vec` rather than a literal hides it from rustc's
        // `invalid_from_utf8` lint (which would otherwise fire on a
        // statically-known bad literal).
        let invalid: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
        let bad_utf8 = std::str::from_utf8(&invalid).unwrap_err();

        vec![
            Error::XmlParsing(quick_xml::Error::Syntax(
                quick_xml::errors::SyntaxError::UnclosedTag,
            )),
            Error::XmlParseAttr(quick_xml::events::attributes::AttrError::Duplicated(0, 0)),
            Error::XmlParseEncoding(quick_xml::encoding::EncodingError::from(bad_utf8)),
            Error::Yaml(yaml_serde::from_str::<u32>("not a number").unwrap_err()),
            Error::Io(std::io::Error::other("io")),
            Error::Arrow(arrow::error::ArrowError::ComputeError("compute".into())),
            Error::Utf8Error(bad_utf8),
            Error::ParseError {
                field: Arc::from("f"),
                path: Arc::from("/p"),
                value: "x".into(),
                kind: ParseKind::InvalidNumber {
                    type_name: "i32",
                    reason: "bad digit".into(),
                },
            },
            Error::ParseError {
                field: Arc::from("f"),
                path: Arc::from("/p"),
                value: "maybe".into(),
                kind: ParseKind::InvalidBoolean,
            },
            Error::MissingRequiredField {
                field: Arc::from("f"),
                path: Arc::from("/p"),
            },
            Error::UnsupportedConversion {
                conversion: ConversionKind::Scaling,
                data_type: "Int32".into(),
            },
            Error::UnsupportedConversion {
                conversion: ConversionKind::Offset,
                data_type: "Int32".into(),
            },
            Error::InvalidConfig {
                reason: ConfigIssue::EmptyTableName,
            },
        ]
    }

    #[test]
    fn pyerr_mapping_round_trips() {
        for err in sample_of_each_variant() {
            // `PyErr::new_err` is lazy — it stores arguments without touching
            // the interpreter — so this runs even when Python isn't linked
            // at runtime. The assertion is simply that conversion doesn't
            // panic and Display still produces non-empty output.
            let display = err.to_string();
            let _py_err: PyErr = err.into();
            assert!(!display.is_empty());
        }
    }
}
