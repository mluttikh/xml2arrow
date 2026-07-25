//! Adversarial and malformed-input tests.
//!
//! These pin the crate's **trust model** — what an untrusted document can and
//! cannot make the parser do — as documented in the README's "Security & trust
//! model" section. Each test names the guarantee it defends, so a change that
//! weakens one fails loudly here rather than silently in a user's pipeline.
//!
//! Every case runs through all three entry points (`parse_xml_slice`,
//! `parse_xml` over a `BufRead`, and `Parser::parse_batches_slice`). That is
//! deliberate: the checks live in the shared `handle_event`, and this matrix is
//! what proves the three event pumps have not drifted apart.

use std::io::Write;

use arrow::array::Array;
use rstest::rstest;
use tempfile::NamedTempFile;
use xml2arrow::{
    BatchOptions, Config, Error, Parser, errors::ParseKind, parse_xml, parse_xml_slice,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A config over `/r/v`, deliberately minimal so the tests exercise the parser
/// rather than the configuration surface.
fn config(extra_parser_options: &str) -> Config {
    let yaml = format!(
        r#"
parser_options:
{extra_parser_options}
tables:
  - name: rows
    xml_path: /r
    levels: [rows]
    fields:
      - name: v
        xml_path: /r/v
        data_type: Utf8
        nullable: true
"#
    );
    yaml_serde::from_str(&yaml).unwrap_or_else(|e| panic!("invalid YAML config: {e}"))
}

fn default_config() -> Config {
    config("  {}")
}

/// The three public ways to drive a parse. Each returns the concatenated `v`
/// column so tests can assert on *content*, not merely on `is_err()` — an
/// error raised for the wrong reason must not pass a security assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entry {
    Slice,
    Buffered,
    Batches,
}

impl Entry {
    fn run(self, xml: &str, config: &Config) -> Result<Vec<String>, Error> {
        match self {
            Entry::Slice => {
                let batches = parse_xml_slice(xml.as_bytes(), config)?;
                Ok(collect_values(batches.get("rows")))
            }
            Entry::Buffered => {
                // Through a real file, so the buffered reader genuinely refills
                // its buffer rather than seeing the whole document at once.
                let mut file = NamedTempFile::new().expect("temp file");
                file.write_all(xml.as_bytes()).expect("write");
                file.flush().expect("flush");
                let handle = std::fs::File::open(file.path()).expect("open");
                let batches = parse_xml(std::io::BufReader::new(handle), config)?;
                Ok(collect_values(batches.get("rows")))
            }
            Entry::Batches => {
                let parser = Parser::new(config)?;
                // A tiny row threshold forces multiple flushes, so a truncation
                // or depth error has to survive being raised *after* batches
                // have already been yielded.
                let opts = BatchOptions::default().with_max_rows_per_batch(2);
                let mut values = Vec::new();
                for batch in parser.parse_batches_slice(xml.as_bytes(), opts) {
                    let batch = batch?;
                    if &*batch.table == "rows" {
                        values.extend(collect_values(Some(&batch.batch)));
                    }
                }
                Ok(values)
            }
        }
    }
}

fn collect_values(batch: Option<&arrow::record_batch::RecordBatch>) -> Vec<String> {
    let Some(batch) = batch else {
        return Vec::new();
    };
    let Some(column) = batch.column_by_name("v") else {
        return Vec::new();
    };
    let array = column
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("column 'v' is Utf8");
    (0..array.len())
        .map(|i| {
            if arrow::array::Array::is_null(array, i) {
                String::new()
            } else {
                array.value(i).to_string()
            }
        })
        .collect()
}

const ALL_ENTRIES: [Entry; 3] = [Entry::Slice, Entry::Buffered, Entry::Batches];

// ---------------------------------------------------------------------------
// XXE — external entities are never fetched
// ---------------------------------------------------------------------------

#[test]
fn xxe_system_file_entity_is_not_fetched() {
    // The canary lives in a real file on disk that the document points at. The
    // assertion is on *content*: no column may contain the canary, which is a
    // strictly stronger claim than "the parse errored".
    let mut canary = NamedTempFile::new().expect("temp file");
    canary.write_all(b"XXECANARY_9F3A2B").expect("write canary");
    canary.flush().expect("flush");
    let path = canary.path().display().to_string();

    let xml = format!(
        r#"<?xml version="1.0"?><!DOCTYPE r [<!ENTITY xxe SYSTEM "file://{path}">]><r><v>&xxe;</v></r>"#
    );

    for entry in ALL_ENTRIES {
        let result = entry.run(&xml, &default_config());
        match result {
            Err(Error::ParseError {
                kind: ParseKind::UnresolvedEntity,
                ..
            }) => {}
            Err(other) => panic!("{entry:?}: expected UnresolvedEntity, got {other}"),
            Ok(values) => panic!("{entry:?}: expected an error, parsed {values:?}"),
        }
        // Belt and braces: even the error's own rendering must not carry the
        // file's contents.
        let rendered = format!("{:?}", entry.run(&xml, &default_config()));
        assert!(
            !rendered.contains("XXECANARY"),
            "{entry:?}: canary leaked into output: {rendered}"
        );
    }
}

#[test]
fn xxe_http_entity_is_not_fetched() {
    // No network path exists at all; this documents that as a guarantee rather
    // than an accident of the test environment being offline.
    let xml = r#"<?xml version="1.0"?><!DOCTYPE r [<!ENTITY xxe SYSTEM "http://127.0.0.1:1/x">]><r><v>&xxe;</v></r>"#;
    for entry in ALL_ENTRIES {
        assert!(
            matches!(
                entry.run(xml, &default_config()),
                Err(Error::ParseError {
                    kind: ParseKind::UnresolvedEntity,
                    ..
                })
            ),
            "{entry:?}: http entity should be unresolvable, not fetched"
        );
    }
}

#[test]
fn parameter_entity_in_dtd_is_ignored() {
    // The DTD's internal subset is inert: a parameter entity referencing an
    // external resource neither resolves nor prevents the document's own data
    // from parsing.
    let xml = r#"<?xml version="1.0"?><!DOCTYPE r [<!ENTITY % pe SYSTEM "file:///etc/passwd"> %pe;]><r><v>ok</v></r>"#;
    for entry in ALL_ENTRIES {
        let values = entry
            .run(xml, &default_config())
            .unwrap_or_else(|e| panic!("{entry:?}: {e}"));
        assert_eq!(values, vec!["ok".to_string()], "{entry:?}");
    }
}

#[test]
fn billion_laughs_does_not_expand() {
    // Entity expansion is not merely bounded — it does not happen. The bomb
    // fails at its first reference, so there is nothing to amplify. Asserting
    // the *kind* matters: a timeout or an OOM would also "not return data".
    let xml = r#"<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;"><!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">]><r><v>&lol3;</v></r>"#;
    for entry in ALL_ENTRIES {
        assert!(
            matches!(
                entry.run(xml, &default_config()),
                Err(Error::ParseError {
                    kind: ParseKind::UnresolvedEntity,
                    ..
                })
            ),
            "{entry:?}: entity bomb should fail at the first reference"
        );
    }
}

#[test]
fn internal_dtd_entity_is_rejected() {
    // Pins a documented *limitation*, not a bug: entities declared in the
    // document's own DTD are not resolved either. This is the trade-off that
    // makes the guarantees above unconditional. If we ever add internal-entity
    // support, this test tells us we changed a published contract.
    let xml =
        r#"<?xml version="1.0"?><!DOCTYPE r [<!ENTITY greeting "hello">]><r><v>&greeting;</v></r>"#;
    for entry in ALL_ENTRIES {
        assert!(
            matches!(
                entry.run(xml, &default_config()),
                Err(Error::ParseError {
                    kind: ParseKind::UnresolvedEntity,
                    ..
                })
            ),
            "{entry:?}: internal DTD entities are documented as unresolved"
        );
    }
}

#[test]
fn predefined_entities_and_char_refs_still_resolve() {
    // Positive control: the guarantees above must not be achieved by refusing
    // all entity syntax.
    let xml = r#"<r><v>a&amp;b&#66;c&#x41;d</v></r>"#;
    for entry in ALL_ENTRIES {
        let values = entry
            .run(xml, &default_config())
            .unwrap_or_else(|e| panic!("{entry:?}: {e}"));
        assert_eq!(values, vec!["a&bBcAd".to_string()], "{entry:?}");
    }
}

// ---------------------------------------------------------------------------
// Truncation — a document that ends mid-element is not silently accepted
// ---------------------------------------------------------------------------

/// Cut points: mid-text, mid-element (after a complete row), and after a
/// complete row but before the root closes. All three previously returned
/// `Ok` with a short batch.
#[rstest]
#[case::mid_text("<r><v>a</v><v2>b</v2><v>c")]
#[case::mid_element("<r><v>a</v><nested><deep>")]
#[case::root_not_closed("<r><v>a</v>")]
fn truncated_input_is_rejected(#[case] xml: &str) {
    for entry in ALL_ENTRIES {
        match entry.run(xml, &default_config()) {
            Err(Error::TruncatedInput { open_elements }) => {
                assert!(
                    open_elements > 0,
                    "{entry:?}: open_elements should be positive"
                );
            }
            Err(other) => panic!("{entry:?}: expected TruncatedInput, got {other}"),
            Ok(values) => panic!(
                "{entry:?}: truncated input silently returned {} row(s): {values:?}",
                values.len()
            ),
        }
    }
}

#[test]
fn complete_document_is_not_reported_as_truncated() {
    // The other side of the check: a well-formed document must still parse.
    let xml = "<r><v>a</v></r>";
    for entry in ALL_ENTRIES {
        let values = entry
            .run(xml, &default_config())
            .unwrap_or_else(|e| panic!("{entry:?}: {e}"));
        assert_eq!(values, vec!["a".to_string()], "{entry:?}");
    }
}

#[test]
fn truncated_input_is_accepted_when_opted_in() {
    // The recovery escape hatch returns the rows parsed before the cut.
    let config = config("  allow_truncated_input: true");
    let xml = "<r><v>a</v><v2>x</v2><v>b";
    for entry in ALL_ENTRIES {
        let values = entry
            .run(xml, &config)
            .unwrap_or_else(|e| panic!("{entry:?}: {e}"));
        assert_eq!(values, vec!["a".to_string()], "{entry:?}");
    }
}

#[test]
fn stop_at_paths_is_not_truncation() {
    // Regression guard for the *placement* of the check. `stop_at_paths` exits
    // through `close_element`'s Break and never reaches the Eof arm, so a
    // deliberate early stop on a complete document must still succeed. A naive
    // "depth > 0 when the loop exits" implementation placed in the event pumps
    // rather than in the Eof arm would fail here.
    let config = config("  stop_at_paths: ['/r/v']");
    let xml = "<r><v>a</v><v>b</v></r>";
    for entry in ALL_ENTRIES {
        let values = entry
            .run(xml, &config)
            .unwrap_or_else(|e| panic!("{entry:?}: {e}"));
        assert_eq!(values, vec!["a".to_string()], "{entry:?}");
    }
}

#[test]
fn empty_input_is_not_reported_as_truncated() {
    // Zero elements opened means depth 0 at EOF — an empty document is valid
    // (and already covered by an integration test), not truncated.
    for entry in ALL_ENTRIES {
        let values = entry
            .run("", &default_config())
            .unwrap_or_else(|e| panic!("{entry:?}: {e}"));
        assert!(values.is_empty(), "{entry:?}: got {values:?}");
    }
}

// ---------------------------------------------------------------------------
// Other malformed input
// ---------------------------------------------------------------------------

#[test]
fn mismatched_end_tag_is_rejected() {
    // Pre-existing quick-xml behaviour (`validate_closing_tags`), pinned here
    // so the trust-model section can cite it.
    let xml = "<r><v>a</b></r>";
    for entry in ALL_ENTRIES {
        assert!(
            entry.run(xml, &default_config()).is_err(),
            "{entry:?}: mismatched end tag should be rejected"
        );
    }
}
