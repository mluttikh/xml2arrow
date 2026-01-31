use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use crate::errors::{Error, Result};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

/// Configuration for the XML parser.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ParserOptions {
    /// Whether to trim whitespace from text nodes. Defaults to false.
    #[serde(default)]
    pub trim_text: bool,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self { trim_text: false }
    }
}

/// Top-level configuration for XML to Arrow conversion.
///
/// This struct holds a collection of `TableConfig` structs, each defining how a specific
/// part of the XML document should be parsed into an Arrow table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Config {
    /// A vector of `TableConfig` structs, each defining a table to be extracted from the XML.
    pub tables: Vec<TableConfig>,
    /// Parser options.
    #[serde(default)]
    pub parser_options: ParserOptions,
}

impl Config {
    /// Validates the configuration by checking all field configurations.
    ///
    /// Returns an error if any field uses an unsupported combination (e.g., scale/offset on non-float types).
    pub fn validate(&self) -> Result<()> {
        for table in &self.tables {
            for field in &table.fields {
                field.validate()?;
            }
        }
        Ok(())
    }

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
        let config: Config = serde_yaml::from_reader(reader).map_err(Error::Yaml)?;
        config.validate()?;
        Ok(config)
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

impl FieldConfig {
    /// Validates that scale/offset are only used with floating point data types.
    pub fn validate(&self) -> Result<()> {
        match self.data_type {
            DType::Float32 | DType::Float64 => Ok(()),
            _ => {
                if self.scale.is_some() {
                    return Err(Error::UnsupportedConversion(format!(
                        "Scaling is only supported for Float32 and Float64, not {:?}",
                        self.data_type
                    )));
                }
                if self.offset.is_some() {
                    return Err(Error::UnsupportedConversion(format!(
                        "Offset is only supported for Float32 and Float64, not {:?}",
                        self.data_type
                    )));
                }
                Ok(())
            }
        }
    }
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
    pub fn build(self) -> Result<FieldConfig> {
        let cfg = FieldConfig {
            name: self.name,
            xml_path: self.xml_path,
            data_type: self.data_type,
            nullable: self.nullable,
            scale: self.scale,
            offset: self.offset,
        };
        cfg.validate()?;
        Ok(cfg)
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
        match serde_yaml::from_str::<$crate::config::Config>($yaml) {
            Ok(config) => {
                if let Err(e) = config.validate() {
                    panic!("Invalid configuration: {:?}", e);
                }
                config
            }
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
    fn test_yaml_config_roundtrip(
        #[values(
            Config {
                parser_options: Default::default(),
                tables: vec![
                    TableConfig::new("table1", "/path/to", vec![], vec![
                        match FieldConfigBuilder::new("string_field", "/path/to/string_field", DType::Utf8)
                            .nullable(true)
                            .build()
                        {
                            Ok(f) => f,
                            Err(e) => panic!("Failed to build field config: {:?}", e),
                        },
                        match FieldConfigBuilder::new("int32_field", "/path/to/int32_field", DType::Int32)
                            .build()
                        {
                            Ok(f) => f,
                            Err(e) => panic!("Failed to build field config: {:?}", e),
                        },
                        match FieldConfigBuilder::new("float64_field", "/path/to/float64_field", DType::Float64)
                            .nullable(true)
                            .scale(1.0e-9)
                            .offset(1.0e-3)
                            .build()
                        {
                            Ok(f) => f,
                            Err(e) => panic!("Failed to build field config: {:?}", e),
                        },
                        ]
                    ),
                ],
            },
            Config {
                parser_options: Default::default(),
                tables: vec![]
            }
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
    fn test_yaml_from_file_invalid_content() {
        let invalid_yaml = "tables:\n  - name: table1\n    row_element: /path\n    fields:\n      - name: field1\n        xml_path: path\n        type: InvalidType\n        nullable: true";
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        std::fs::write(&path, invalid_yaml).unwrap();
        let result = Config::from_yaml_file(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Yaml(_)));
    }

    #[test]
    fn test_yaml_from_file_not_found() {
        let result = Config::from_yaml_file(PathBuf::from("not_existing.yaml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Io(_)));
    }

    #[test]
    fn test_yaml_to_file_invalid_path() {
        let config = Config {
            tables: vec![],
            parser_options: Default::default(),
        };
        let result = config.to_yaml_file(PathBuf::from("/not/existing/path/config.yaml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Io(_)));
    }

    #[test]
    fn test_yaml_field_nullable_default() {
        let yaml_string = r#"
            name: test_field
            xml_path: /path/to/field
            data_type: Utf8
            "#;

        let field_config: FieldConfig = serde_yaml::from_str(yaml_string).unwrap();
        assert!(!field_config.nullable);
    }

    #[test]
    fn test_yaml_parser_options_trim_text_default() {
        let yaml_string = r#"
            tables:
              - name: test_table
                xml_path: /root
                levels: []
                fields:
                  - name: bool_field
                    xml_path: /root/value
                    data_type: Boolean
                    nullable: true
            "#;

        let config: Config = serde_yaml::from_str(yaml_string).unwrap();
        assert!(
            !config.parser_options.trim_text,
            "trim_text should default to false"
        );
    }

    #[test]
    fn test_yaml_parser_options_trim_text_explicit() {
        let yaml_string = r#"
            parser_options:
              trim_text: true
            tables: []
            "#;

        let config: Config = serde_yaml::from_str(yaml_string).unwrap();
        assert!(
            config.parser_options.trim_text,
            "trim_text should be true when explicitly set"
        );
    }

    #[test]
    fn test_yaml_parser_options_empty_section() {
        let yaml_string = r#"
            parser_options: {}
            tables: []
            "#;

        let config: Config = serde_yaml::from_str(yaml_string).unwrap();
        assert!(
            !config.parser_options.trim_text,
            "trim_text should default to false when parser_options is empty"
        );
    }

    #[test]
    fn test_config_requires_attr_parsing_with_attributes() {
        let config = Config {
            tables: vec![TableConfig::new(
                "test",
                "/root",
                vec![],
                vec![
                    match FieldConfigBuilder::new("id", "/root/item/@id", DType::Int32).build() {
                        Ok(f) => f,
                        Err(e) => panic!("Failed to build field config: {:?}", e),
                    },
                ],
            )],
            parser_options: Default::default(),
        };

        assert!(config.requires_attribute_parsing());
    }

    #[test]
    fn test_config_requires_attr_parsing_without_attributes() {
        let config = Config {
            tables: vec![TableConfig::new(
                "test",
                "/root",
                vec![],
                vec![
                    match FieldConfigBuilder::new("id", "/root/item/id", DType::Int32).build() {
                        Ok(f) => f,
                        Err(e) => panic!("Failed to build field config: {:?}", e),
                    },
                ],
            )],
            parser_options: Default::default(),
        };

        assert!(!config.requires_attribute_parsing());
    }

    #[test]
    fn test_config_requires_attr_parsing_mixed() {
        let config = Config {
            tables: vec![TableConfig::new(
                "test",
                "/root",
                vec![],
                vec![
                    match FieldConfigBuilder::new("id", "/root/item/id", DType::Int32).build() {
                        Ok(f) => f,
                        Err(e) => panic!("Failed to build field config: {:?}", e),
                    },
                    match FieldConfigBuilder::new("type", "/root/item/@type", DType::Utf8).build() {
                        Ok(f) => f,
                        Err(e) => panic!("Failed to build field config: {:?}", e),
                    },
                ],
            )],
            parser_options: Default::default(),
        };

        assert!(config.requires_attribute_parsing());
    }

    #[test]
    fn test_dtype_as_arrow_type_all_variants() {
        use arrow::datatypes::DataType as ArrowDataType;

        assert_eq!(DType::Boolean.as_arrow_type(), ArrowDataType::Boolean);
        assert_eq!(DType::Float32.as_arrow_type(), ArrowDataType::Float32);
        assert_eq!(DType::Float64.as_arrow_type(), ArrowDataType::Float64);
        assert_eq!(DType::Utf8.as_arrow_type(), ArrowDataType::Utf8);
        assert_eq!(DType::Int8.as_arrow_type(), ArrowDataType::Int8);
        assert_eq!(DType::UInt8.as_arrow_type(), ArrowDataType::UInt8);
        assert_eq!(DType::Int16.as_arrow_type(), ArrowDataType::Int16);
        assert_eq!(DType::UInt16.as_arrow_type(), ArrowDataType::UInt16);
        assert_eq!(DType::Int32.as_arrow_type(), ArrowDataType::Int32);
        assert_eq!(DType::UInt32.as_arrow_type(), ArrowDataType::UInt32);
        assert_eq!(DType::Int64.as_arrow_type(), ArrowDataType::Int64);
        assert_eq!(DType::UInt64.as_arrow_type(), ArrowDataType::UInt64);
    }

    #[test]
    fn test_builder_field_config_chaining() {
        let field = FieldConfigBuilder::new("test_field", "/path/to/field", DType::Float64)
            .nullable(true)
            .scale(0.001)
            .offset(100.0)
            .build()
            .unwrap();

        assert_eq!(field.name, "test_field");
        assert_eq!(field.xml_path, "/path/to/field");
        assert_eq!(field.data_type, DType::Float64);
        assert!(field.nullable);
        assert_eq!(field.scale, Some(0.001));
        assert_eq!(field.offset, Some(100.0));
    }

    #[test]
    fn test_builder_field_config_scale_only() {
        let field = FieldConfigBuilder::new("test", "/path", DType::Float32)
            .scale(0.5)
            .build()
            .unwrap();

        assert_eq!(field.scale, Some(0.5));
        assert_eq!(field.offset, None);
    }

    #[test]
    fn test_builder_field_config_offset_only() {
        let field = FieldConfigBuilder::new("test", "/path", DType::Float64)
            .offset(5.0)
            .build()
            .unwrap();

        assert_eq!(field.scale, None);
        assert_eq!(field.offset, Some(5.0));
    }
}
