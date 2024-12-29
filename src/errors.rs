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
    XMLParsing(quick_xml::Error),
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
    /// Error indicating that a table specified in the configuration was not found in the XML data.
    /// Contains the XML path of the missing table.
    TableNotFound(String),
    /// Error indicating that there is no table on the stack.
    NoTableOnStack,
}

#[cfg(feature = "python")]
create_exception!(xml2arrow, XmlParsingError, pyo3::exceptions::PyException);

#[cfg(feature = "python")]
create_exception!(xml2arrow, YamlParsingError, pyo3::exceptions::PyException);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    UnsupportedDataTypeError,
    pyo3::exceptions::PyException
);

#[cfg(feature = "python")]
create_exception!(xml2arrow, TableNotFoundError, pyo3::exceptions::PyException);

#[cfg(feature = "python")]
create_exception!(
    xml2arrow,
    NoTableOnStackError,
    pyo3::exceptions::PyException
);

#[cfg(feature = "python")]
impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        match value {
            Error::Io(e) => e.into(),
            Error::Utf8Error(e) => e.into(),
            Error::Arrow(e) => PyArrowException::new_err(e.to_string()),
            Error::XMLParsing(e) => XmlParsingError::new_err(e.to_string()),
            Error::Yaml(e) => YamlParsingError::new_err(e.to_string()),
            Error::UnsupportedDataType(e) => UnsupportedDataTypeError::new_err(e.to_string()),
            Error::TableNotFound(e) => TableNotFoundError::new_err(e.to_string()),
            Error::NoTableOnStack => {
                NoTableOnStackError::new_err("There is no table on the stack".to_string())
            }
        }
    }
}
