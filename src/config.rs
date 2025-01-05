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

    /// Checks if the configuration contains any fields that require attribute parsing.
    ///
    /// This method iterates through all tables and their fields in the configuration and returns
    /// `true` if any field's XML path contains the "@" symbol, indicating that it targets an attribute.
    ///
    /// # Returns
    ///
    /// `true` if the configuration contains at least one attribute to parse, `false` otherwise.
    pub fn requires_attribute_parsing(&self) -> bool {
        for table in &self.tables {
            for field in &table.fields {
                if field.xml_path.contains("@") {
                    return true;
                }
            }
        }
        false
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
    /// The levels of nesting for this table. This is used to create the indices for nested tables.
    /// For example if the xml_path is `/data/dataset/table/item/properties` the levels should
    /// be `["table", "properties"]`.
    pub levels: Vec<String>,
    /// A vector of `FieldConfig` structs, each defining a field (column) in the table.
    pub fields: Vec<FieldConfig>,
}

impl TableConfig {
    pub fn new(name: &str, xml_path: &str, levels: Vec<String>, fields: Vec<FieldConfig>) -> Self {
        Self {
            name: name.to_string(),
            xml_path: xml_path.to_string(),
            levels,
            fields,
        }
    }
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

/// A builder for configuring a `FieldConfig` struct.
///
/// This builder allows you to set the various properties of a field
/// definition within a table configuration for parsing XML data.
#[derive(Default)]
pub struct FieldConfigBuilder {
    name: String,
    xml_path: String,
    data_type: DType,
    nullable: bool,
    scale: Option<f64>,
    offset: Option<f64>,
}

impl FieldConfigBuilder {
    /// Creates a new `FieldConfigBuilder` with the provided name, XML path, and data type.
    ///
    /// This is the starting point for building a `FieldConfig`.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field.
    /// * `xml_path` - The XML path that points to the location of the field data in the XML document.
    /// * `data_type` - The data type of the field.
    ///
    /// # Returns
    ///
    /// A new `FieldConfigBuilder` instance with the provided properties.
    pub fn new(name: &str, xml_path: &str, data_type: DType) -> Self {
        Self {
            name: name.to_string(),
            xml_path: xml_path.to_string(),
            data_type,
            ..Default::default()
        }
    }

    /// Sets the `nullable` flag for the field configuration being built.
    ///
    /// This method allows you to specify whether the field can be null (missing data) in the XML document.
    ///
    /// # Arguments
    ///
    /// * `nullable` - A boolean value indicating whether the field is nullable.
    ///
    /// # Returns
    ///
    /// The builder instance itself, allowing for method chaining.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Sets the `scale` factor for the field configuration being built.
    ///
    /// This method is typically used with float data types to specify the scale factor.
    ///
    /// # Arguments
    ///
    /// * `scale` - The scale factor as an f64 value.
    ///
    /// # Returns
    ///
    /// The builder instance itself, allowing for method chaining.
    pub fn scale(mut self, scale: f64) -> Self {
        self.scale = Some(scale);
        self
    }

    /// Sets the `offset` value for the field configuration being built.
    ///
    /// This method can be used with float data types to specify an offset value.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset value as an f64 value.
    ///
    /// # Returns
    ///
    /// The builder instance itself, allowing for method chaining.
    pub fn offset(mut self, offset: f64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Consumes the builder and builds the final `FieldConfig` struct.
    ///
    /// This method takes the configuration set on the builder and returns a new `FieldConfig` instance.
    ///
    /// # Returns
    ///
    /// A `FieldConfig` struct with the configured properties
    pub fn build(self) -> FieldConfig {
        FieldConfig {
            name: self.name,
            xml_path: self.xml_path,
            data_type: self.data_type,
            nullable: self.nullable,
            scale: self.scale,
            offset: self.offset,
        }
    }
}

/// Represents the data type of a field.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DType {
    Boolean,
    Float32,
    Float64,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    #[default]
    Utf8,
}

impl DType {
    pub(crate) fn as_arrow_type(&self) -> DataType {
        match self {
            DType::Boolean => DataType::Boolean,
            DType::Float32 => DataType::Float32,
            DType::Float64 => DataType::Float64,
            DType::Utf8 => DataType::Utf8,
            DType::Int8 => DataType::Int8,
            DType::UInt8 => DataType::UInt8,
            DType::Int16 => DataType::Int16,
            DType::UInt16 => DataType::UInt16,
            DType::Int32 => DataType::Int32,
            &DType::UInt32 => DataType::UInt32,
            DType::Int64 => DataType::Int64,
            DType::UInt64 => DataType::UInt64,
        }
    }
}

/// Creates a `Config` struct from a YAML string at compile time.
///
/// This macro takes a YAML string literal as input and parses it into a `Config` struct at compile time.
/// It panics if the YAML is invalid.
#[macro_export]
macro_rules! config_from_yaml {
    ($yaml:expr) => {{
        match serde_yaml::from_str($yaml) {
            Ok(config) => config,
            Err(e) => panic!("Invalid YAML configuration: {}", e),
        }
    }};
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
                tables: vec![
                    TableConfig::new("table1", "/path/to", vec![], vec![
                        FieldConfigBuilder::new("string_field", "/path/to/string_field", DType::Utf8).nullable(true).build(),
                        FieldConfigBuilder::new("int32_field", "/path/to/int32_field", DType::Int32).build(),
                        FieldConfigBuilder::new("float64_field", "/path/to/float64_field", DType::Float64).nullable(true).scale(1.0e-9).offset(1.0e-3).build(),
                        ]
                    ),
                ],
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
