use std::str::Utf8Error;

#[cfg(feature = "python")]
use arrow::pyarrow::PyArrowException;
use derive_more::From;
#[cfg(feature = "python")]
use pyo3::create_exception;
#[cfg(feature = "python")]
use pyo3::PyErr;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    /// Errors from the QuickXML crate during XML parsing
    #[from]
    XmlParsing(quick_xml::Error),
    /// Errors from the QuickXML crate that can be raised during parsing attributes.
    #[from]
    XmlParseAttr(quick_xml::events::attributes::AttrError),
    /// Errors from the Serde YAML crate during configuration parsing
    #[from]
    Yaml(serde_yaml::Error),
    /// Standard I/O errors
    #[from]
    Io(std::io::Error),
    /// Errors from the arrow crate during Arrow operations
    #[from]
    Arrow(arrow::error::ArrowError),
    /// Errors during UTF-8 string conversion
    #[from]
    Utf8Error(Utf8Error),
    /// Custom error for unsupported data types, holding a descriptive message
    UnsupportedDataType(String),
    /// Errors that occur during parsing of values from strings to specific data types.
    ParseError(String),
    /// Error indicating that a table specified in the configuration was not found in the XML data.
    /// Contains the XML path of the missing table.
    TableNotFound(String),
    /// Error indicating that there is no table on the stack.
    NoTableOnStack,
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
    UnsupportedDataTypeError,
    Xml2ArrowError,
    "Raised when an unsupported data type is encountered."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    TableNotFoundError,
    Xml2ArrowError,
    "Raised when a table specified in the configuration is not found in the XML data."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    NoTableOnStackError,
    Xml2ArrowError,
    "Raised when an operation is performed that requires a table to be on the stack, but none is present."
);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    ParseError,
    Xml2ArrowError,
    "Raised when an error occurs during parsing of values from strings to specific data types."
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
            Error::Yaml(e) => YamlParsingError::new_err(e.to_string()),
            Error::UnsupportedDataType(e) => UnsupportedDataTypeError::new_err(e.to_string()),
            Error::TableNotFound(e) => TableNotFoundError::new_err(e.to_string()),
            Error::NoTableOnStack => {
                NoTableOnStackError::new_err("There is no table on the stack".to_string())
            }
            Error::ParseError(e) => ParseError::new_err(e.to_string()),
        }
    }
}
