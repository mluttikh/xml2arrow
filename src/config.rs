use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use crate::errors::{Error, Result};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

/// Top-level configuration for XML to Arrow conversion.
///
/// This struct holds a collection of `TableConfig` structs, each defining how a specific
/// part of the XML document should be parsed into an Arrow table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Config {
    /// A vector of `TableConfig` structs, each defining a table to be extracted from the XML.
    pub tables: Vec<TableConfig>,
}

impl Config {
    /// Creates a `Config` struct from a YAML configuration file.
    ///
    /// This function reads a YAML file at the given path and deserializes it into a `Config` struct.
    ///
    /// # Arguments
    ///
    /// *   `path`: The path to the YAML configuration file.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    ///
    /// *   `Ok(Config)`: The deserialized `Config` struct.
    /// *   `Err(Error)`: An `Error` value if the file cannot be opened, read, or parsed as YAML.
    ///
    /// # Errors
    ///
    /// This function may return the following errors:
    ///
    /// *   `Error::Io`: If an I/O error occurs while opening or reading the file.
    /// *   `Error::Yaml`: If there is an error parsing the YAML data.
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        serde_yaml::from_reader(reader).map_err(Error::Yaml)
    }

    /// Writes the `Config` struct to a YAML file.
    ///
    /// This function serializes the `Config` struct to YAML format and writes it to a file at the given path.
    ///
    /// # Arguments
    ///
    /// *   `path`: The path to the output YAML file.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    ///
    /// *   `Ok(())`: If the `Config` was successfully written to the file.
    /// *   `Err(Error)`: An `Error` value if the file cannot be created or the `Config` cannot be serialized to YAML.
    ///
    /// # Errors
    ///
    /// This function may return the following errors:
    ///
    /// *   `Error::Io`: If an I/O error occurs while creating or writing to the file.
    /// *   `Error::Yaml`: If there is an error serializing the `Config` to YAML.
    pub fn to_yaml_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_yaml::to_writer(writer, self).map_err(Error::Yaml)
    }
}

/// Configuration for an XML table to be parsed into an Arrow record batch.
///
/// This struct defines how an XML structure should be interpreted as a table, including
/// the path to the table elements, the element representing a row, and the configuration
/// of the fields (columns) within the table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct TableConfig {
    /// The name of the table.
    pub name: String,
    /// The XML path to the table elements. For example `/data/dataset/table`.
    pub xml_path: String,
    /// The name of the XML element that represents a single row in the table.
    pub row_element: String,
    /// The levels of nesting for this table. This is used to create the indices for nested tables.
    /// For example if the xml_path is `/data/dataset/table/item/properties` and the row element is `property`
    /// the levels should be `["table", "properties"]`.
    pub levels: Vec<String>,
    /// A vector of `FieldConfig` structs, each defining a field (column) in the table.
    pub fields: Vec<FieldConfig>,
}

/// Configuration for a single field within an XML table.
///
/// This struct defines how a specific XML element or attribute should be extracted and
/// converted into an Arrow column.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct FieldConfig {
    /// The name of the field (and the name of the resulting Arrow column).
    pub name: String,
    /// The XML path to the element or attribute.
    pub xml_path: String,
    /// The data type of the field. This determines the Arrow data type of the resulting column.
    pub data_type: DType,
    /// Whether the field is nullable (can contain null values). Defaults to false.
    #[serde(default)]
    pub nullable: bool,
    /// Scale for decimal types.
    pub scale: Option<f64>,
    /// Offset for decimal types.
    pub offset: Option<f64>,
}

/// Represents the data type of a field.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DType {
    Boolean,
    Float32,
    Float64,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Utf8,
}

impl DType {
    pub(crate) fn as_arrow_type(&self) -> DataType {
        match self {
            DType::Boolean => DataType::Boolean,
            DType::Float32 => DataType::Float32,
            DType::Float64 => DataType::Float64,
            DType::Utf8 => DataType::Utf8,
            DType::UInt16 => DataType::UInt16,
            DType::Int16 => DataType::Int16,
            DType::Int32 => DataType::Int32,
            &DType::UInt32 => DataType::UInt32,
            DType::Int64 => DataType::Int64,
            DType::UInt64 => DataType::UInt64,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_yaml_rw(
        #[values(
            Config {
                tables: vec![TableConfig {
                    name: "table1".to_string(),
                    xml_path: "/path/to".to_string(),
                    row_element: "root".to_string(),
                    levels: vec![],
                    fields: vec![
                        FieldConfig {
                            name: "string_field".to_string(),
                            xml_path: "string_field".to_string(),
                            data_type: DType::Utf8,
                            nullable: true,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "int32_field".to_string(),
                            xml_path: "int32_field".to_string(),
                            data_type: DType::Int32,
                            nullable: false,
                            scale: None,
                            offset: None,
                        },
                        FieldConfig {
                            name: "float64_field".to_string(),
                            xml_path: "float64_field".to_string(),
                            data_type: DType::Float64,
                            nullable: true,
                            scale: Some(1.0e-9),
                            offset: Some(1.0e-3),
                        },
                    ],
                }],
            },
            Config { tables: vec![] }
        )]
        config: Config,
    ) {
        // Write to a temporary file
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        config.to_yaml_file(&path).unwrap();

        // Read from the same file
        let read_config = Config::from_yaml_file(&path).unwrap();

        // Check if the read config is the same as the original
        assert_eq!(config, read_config);
    }

    #[test]
    fn test_from_yaml_file_invalid() {
        let invalid_yaml = "tables:\n  - name: table1\n    row_element: /path\n    fields:\n      - name: field1\n        xml_path: path\n        type: InvalidType\n        nullable: true";
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        std::fs::write(&path, invalid_yaml).unwrap();
        let result = Config::from_yaml_file(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Yaml(_)));
    }

    #[test]
    fn test_from_yaml_file_not_existing() {
        let result = Config::from_yaml_file(PathBuf::from("not_existing.yaml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Io(_)));
    }

    #[test]
    fn test_to_yaml_file_not_existing_path() {
        let config = Config { tables: vec![] };
        let result = config.to_yaml_file(PathBuf::from("/not/existing/path/config.yaml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Io(_)));
    }

    #[test]
    fn test_field_config_default_nullable_from_yaml() {
        let yaml_string = r#"
    name: test_field
    xml_path: /path/to/field
    data_type: Utf8
    "#;

        let field_config: FieldConfig = serde_yaml::from_str(yaml_string).unwrap();
        assert!(!field_config.nullable);
    }
}
