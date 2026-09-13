//! `convert_config` — convert a configuration to format version 2.
//!
//! ```text
//! cargo run --example convert_config -- config.yaml > config-v2.yaml
//! ```
//!
//! Writes the converted config to stdout, and to stderr one line for each part
//! left for you to decide. Exits 0 when the config is fully converted and
//! declares `version: 2`, 1 when parts are left, and 2 on an error.
//!
//! The conversion changes no output by construction; `config_diff` shows that
//! on your own documents. The written file is fresh: the original's comments
//! and layout are not kept.
//!
//! A dev-only example rather than a shipped binary, like `config_diff`: it
//! costs users nothing, and the conversion itself is the library's
//! `Config::to_version_2`.

use std::process::ExitCode;

use xml2arrow::Config;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [path] = args.as_slice() else {
        eprintln!("usage: convert_config <config.yaml>");
        return ExitCode::from(2);
    };

    let conversion = match Config::from_yaml_file(path).and_then(|c| c.to_version_2()) {
        Ok(conversion) => conversion,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    match yaml_serde::to_string(&conversion.config) {
        Ok(yaml) => print!("{yaml}"),
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    }

    if conversion.unconverted.is_empty() {
        eprintln!("Converted: the config declares version: 2.");
        return ExitCode::SUCCESS;
    }
    for part in &conversion.unconverted {
        eprintln!("left for you: {part}");
    }
    eprintln!(
        "{} part(s) left: the config keeps its version until they are resolved. Resolve them, \
         then convert again.",
        conversion.unconverted.len()
    );
    ExitCode::FAILURE
}
