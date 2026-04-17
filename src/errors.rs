use std::str::Utf8Error;

#[cfg(feature = "python")]
use arrow::pyarrow::PyArrowException;
use derive_more::From;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
use pyo3::create_exception;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    /// Errors from the `quick-xml` crate during XML parsing.
    #[from]
    XmlParsing(quick_xml::Error),
    /// Errors from the `quick-xml` crate that can be raised during parsing attributes.
    #[from]
    XmlParseAttr(quick_xml::events::attributes::AttrError),
    // Errors from the QuickXML crate that can be raised when decoding or encoding.
    #[from]
    XmlParseEncoding(quick_xml::encoding::EncodingError),
    /// Errors from the Serde YAML crate during configuration parsing
    #[from]
    Yaml(yaml_serde::Error),
    /// Standard I/O errors
    #[from]
    Io(std::io::Error),
    /// Errors from the arrow crate during Arrow operations
    #[from]
    Arrow(arrow::error::ArrowError),
    /// Errors during UTF-8 string conversion
    #[from]
    Utf8Error(Utf8Error),
    /// Errors that occur during parsing of values from strings to specific data types.
    ParseError(String),
    /// Error when applying a scaling or an offset is attempted on unsupported data types.
    UnsupportedConversion(String),
    /// Error indicating that the configuration is invalid (e.g., duplicate names, invalid paths).
    InvalidConfig(String),
}

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
            Error::ParseError(e) => ParseError::new_err(e.clone()),
            Error::UnsupportedConversion(e) => UnsupportedConversionError::new_err(e.clone()),
            Error::InvalidConfig(e) => InvalidConfigError::new_err(e.clone()),
        }
    }
}
