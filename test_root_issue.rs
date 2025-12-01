use xml2arrow::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_yaml = r#"
tables:
  - name: report
    xml_path: /
    levels: []
    fields:
      - name: title
        xml_path: /report/header/title
        data_type: Utf8
        nullable: false
"#;
    
    let xml = r#"<report><header><title>Test</title></header></report>"#;
    
    let config: Config = serde_yaml::from_str(config_yaml)?;
    let batches = parse_xml(xml.as_bytes(), &config)?;
    
    println!("Tables found: {:?}", batches.keys().collect::<Vec<_>>());
    for (name, batch) in &batches {
        println!("Table '{}': {} rows", name, batch.num_rows());
    }
    
    Ok(())
}
