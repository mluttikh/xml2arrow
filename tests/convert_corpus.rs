//! Converting to configuration format version 2 changes no output.
//!
//! `Config::to_version_2` promises that every document parses to the same
//! tables, columns, values and errors under a fully converted config as under
//! the original. The frozen corpus holds a version 1 config and a document for
//! every behavior worth freezing, so each of its version 1 cases is converted
//! here, written out as YAML and read back the way a user would, and parsed
//! under both configs. The two results must be identical.
//!
//! A case that does not convert fully still declares version 2, and must not
//! load: the parts left over are what the author has to decide.

use std::fs;
use std::path::Path;

use xml2arrow::{Config, Parser};

#[test]
fn converting_a_corpus_config_changes_no_output() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let mut cases: Vec<_> = fs::read_dir(&root)
        .expect("corpus directory")
        .map(|entry| entry.expect("corpus entry").path())
        .filter(|path| path.join("config.yaml").is_file())
        .collect();
    cases.sort();

    let mut failures = Vec::new();
    let (mut converted, mut version_1) = (0, 0);
    for case in &cases {
        let name = case.file_name().unwrap().to_string_lossy().into_owned();
        let yaml = fs::read_to_string(case.join("config.yaml")).unwrap();
        let xml = fs::read(case.join("input.xml")).unwrap();
        let original = Config::from_yaml_str(&yaml).unwrap();
        if original.version == Some(2) {
            continue;
        }
        version_1 += 1;

        match check_case(&original, &xml) {
            Ok(true) => converted += 1,
            Ok(false) => {}
            Err(message) => failures.push(format!("--- {name} ---\n{message}")),
        }
    }

    assert!(version_1 > 0, "no version 1 corpus cases found");
    assert!(
        failures.is_empty(),
        "{} of {version_1} converted corpus case(s) changed output:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    // Not an assertion: a case may legitimately need a decision. The count is
    // printed so a change in how much converts shows up in the test log.
    println!("{converted} of {version_1} version 1 corpus configs converted fully");
}

/// Whether the case converted fully, or why it broke the promise.
fn check_case(original: &Config, xml: &[u8]) -> Result<bool, String> {
    let conversion = original
        .to_version_2()
        .map_err(|e| format!("conversion failed: {e}"))?;
    if conversion.config.version != Some(2) {
        return Err(format!(
            "declares version {:?} rather than 2",
            conversion.config.version
        ));
    }
    if !conversion.unconverted.is_empty() {
        // Checked on the config itself rather than a written copy, which
        // drops unknown keys and so could load without them being resolved.
        return match conversion.config.validate() {
            Err(_) => Ok(false),
            Ok(()) => Err(format!(
                "loads, although parts are unconverted: {:?}",
                conversion.unconverted
            )),
        };
    }

    // The config the promise is about is the one a user reads back from disk.
    let written = yaml_serde::to_string(&conversion.config).map_err(|e| e.to_string())?;
    let converted = Config::from_yaml_str(&written)
        .map_err(|e| format!("converted config does not load: {e}\n{written}"))?;
    if converted != conversion.config {
        return Err(format!("converted config does not round-trip:\n{written}"));
    }

    let before = Parser::new(original).and_then(|p| p.parse_slice(xml));
    let after = Parser::new(&converted).and_then(|p| p.parse_slice(xml));
    match (before, after) {
        (Ok(before), Ok(after)) if before == after => Ok(true),
        (Err(before), Err(after)) if before.to_string() == after.to_string() => Ok(true),
        (before, after) => Err(format!(
            "original:  {before:?}\nconverted: {after:?}\nconverted config:\n{written}"
        )),
    }
}
