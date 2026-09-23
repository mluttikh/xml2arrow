//! Configuration lints: warnings about configs that are *valid* but whose
//! behavior is likely to surprise.
//!
//! [`Config::validate`] rejects configs that cannot work. This module reports
//! the next tier: configs that parse and run, but whose row semantics or value
//! handling depend on rules users routinely get wrong — chiefly that **row
//! boundaries are inferred rather than declared**: a row of a table ends when
//! any *configured* direct child of its `xml_path` closes, so a table with two
//! such children yields two half-filled rows per container, and adding a field
//! can change a table's row count.
//!
//! Two properties are deliberate:
//!
//! - **The library never prints.** Lints are data. Hosts decide whether to log
//!   them, fail CI on them, or ignore them.
//! - **Lints never change behavior.** They are advisory in every release they
//!   appear in; nothing here can turn a working parse into a failing one.
//!
//! Lints are computed on demand ([`Config::lint`], [`Parser::warnings`]) rather
//! than in `Parser::new`, because `Parser::new`'s fixed cost dominates parses of
//! small documents and every lint here is a pure function of the config that a
//! caller can ask for exactly once.
//!
//! [`Parser::warnings`]: crate::Parser::warnings

use std::fmt;

use std::borrow::Cow;

use crate::config::{
    Config, DType, path_is_strictly_under, path_is_under, path_segments, resolve_field_path,
};
use crate::errors::ConfigIssue;

/// An advisory finding about a configuration.
///
/// `#[non_exhaustive]`: new lints are added in minor releases, and matches must
/// carry a wildcard arm. Use the [`fmt::Display`] rendering for messages —
/// the wording is not a stability surface, but the variant set is.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lint {
    /// The document sets a key the configuration does not define, so the key
    /// is ignored. Usually a misspelling, which leaves the setting it was meant
    /// to be at its default: `scal: 0.1` leaves the column unscaled.
    ///
    /// Reported for a version 1 config, which loads today; version 2 rejects
    /// it. Only a config read from a document can have one, and
    /// `Config::to_yaml_file` does not write it back.
    UnknownKey {
        /// Where the key is, as a reader would look for it: `field 'value' of
        /// table 'readings'`, `parser_options`, `the top level`.
        location: String,
        /// The unknown key, as written.
        key: String,
    },
    /// A path has a `.` or `..` segment, which no element can match, since an
    /// XML name cannot be `.` or `..`. The table has no rows, the field is never
    /// filled, or the stop path never stops the parse.
    ///
    /// Reported for a table's `xml_path`, a field's `xml_path` and
    /// `stop_at_paths` in a version 1 config, which loads today; version 2
    /// rejects every path with one.
    DotSegmentInPath {
        /// Where the path is: `the xml_path of table 'stations'`, `field 'id'
        /// of table 'stations'`, `` `parser_options.stop_at_paths` ``.
        location: String,
        /// The path, as written.
        path: String,
    },
    /// A table's row boundaries are inferred from **more than one** distinct
    /// child element, so it yields one partially-filled row per configured
    /// child element rather than one row per container element.
    ///
    /// This is the single most common source of surprising row counts: the
    /// config never states which element is a row, so the parser deduces it
    /// from which fields happen to be configured, and adding a *field* can
    /// change the *row count*.
    InferredRowBoundary {
        /// The table whose rows are inferred.
        table: String,
        /// Its `xml_path` — the container whose children delimit rows.
        xml_path: String,
        /// Every configured direct child element of `xml_path`, in first-seen
        /// order. Each one finalizes a row of this table when it closes.
        child_elements: Vec<String>,
    },
    /// A field lies inside the `xml_path` of another table nested within its
    /// own, so it never receives a value: whenever a value there arrives, the
    /// nested table is the innermost open table, and values go to its fields
    /// only.
    ///
    /// The column is all null, all `""`, or a `MissingRequiredField` error,
    /// however the document is written. Reported for a version 1 config, which
    /// loads today; version 2 rejects it.
    FieldInsideNestedTable {
        /// The table the field is declared on.
        table: String,
        /// The field that never receives a value.
        field: String,
        /// The field's resolved path.
        field_path: String,
        /// The nested table that captures values there.
        nested_table: String,
        /// That table's `xml_path`, which contains `field_path`.
        nested_table_path: String,
    },
    /// A table has fields but **no** configured child element, so no element
    /// close can ever finalize one of its rows: the table silently produces
    /// zero rows.
    ///
    /// Reachable when every field of a table maps to an attribute of the table
    /// element itself (`/a/@id` on a table at `/a`) — the attribute
    /// pseudo-nodes are entered and left without going through the
    /// row-finalizing close path.
    NeverFinalizesRows {
        /// The table that can never finalize a row.
        table: String,
        /// Its `xml_path`, which has no configured child element.
        xml_path: String,
    },
    /// A table has no column: no fields, no key and no link. It is excluded
    /// from the output entirely and exists only to feed its row counter to
    /// descendant tables' `levels` columns and `index_of:` links.
    ///
    /// A table without fields that declares a key or a link has a column, so
    /// it is output and not reported.
    StructuralTable {
        /// The table without columns.
        table: String,
    },
    /// A table declares more `levels` than it has enclosing table scopes (its
    /// non-root ancestor tables plus itself).
    ///
    /// Each level labels one parent-link index column, and the runtime can
    /// only supply one index per enclosing scope, so the surplus columns never
    /// receive a value — the parse fails later with an opaque column-length
    /// mismatch, or silently produces nothing if the table stays empty.
    /// Reported here rather than rejected outright: a config in which the
    /// affected table never yields a row works today, and rejecting it would
    /// break that config.
    ExcessLevels {
        /// The table declaring the surplus levels.
        table: String,
        /// How many `levels` entries it declares.
        declared: usize,
        /// How many enclosing table scopes actually exist to fill them.
        available: usize,
    },
    /// Non-nullable `Utf8` fields yield an empty string when the value is
    /// missing, where every other data type raises `MissingRequiredField`.
    ///
    /// A long-standing asymmetry, listed here so configs that *rely* on it are
    /// visible: set `nullable: true` on these fields to distinguish "absent"
    /// from "present but empty".
    ///
    /// Reported for version 1 only. Version 2 makes a missing non-nullable
    /// value an error whatever the type, unless the field sets `on_missing`.
    ImplicitEmptyString {
        /// The table containing the fields.
        table: String,
        /// The non-nullable `Utf8` fields that yield `""` when absent.
        fields: Vec<String>,
    },
    /// The configuration uses configuration format version 1, which is
    /// deprecated: it declares no `version:`, or `version: 1`.
    ///
    /// Version 1 keeps working, and this lint changes nothing about how a
    /// document parses. Its job is to be the one place a version 1 user hears
    /// about the deprecation without rereading the documentation, and to say
    /// what is left: `issues` holds the errors declaring `version: 2` would
    /// raise, in the order they appear in the YAML.
    ///
    /// Three kinds are left out, because each has its own lint that says what
    /// it does to the output today, and reporting them twice would be noise:
    /// [`Lint::UnknownKey`], [`Lint::DotSegmentInPath`] and
    /// [`Lint::FieldInsideNestedTable`]. Version 2 rejects those as well.
    ///
    /// Every table of a version 1 config has at least one issue, since none can
    /// declare `row:`, so `issues` is empty only for a config without tables.
    /// The notice also says that declaring `version: 2` switches two defaults
    /// (see [`Config::version`](crate::Config::version)).
    ConfigVersion1 {
        /// The errors declaring `version: 2` would raise, apart from the kinds
        /// reported as lints of their own.
        issues: Vec<ConfigIssue>,
    },
}

impl fmt::Display for Lint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lint::UnknownKey { location, key } => write!(
                f,
                "Unknown key '{key}' in {location} is ignored; check its spelling, or remove it"
            ),
            Lint::DotSegmentInPath { location, path } => write!(
                f,
                "The path '{path}' in {location} has a '.' or '..' segment, so it never matches \
                 an element; write it without one"
            ),
            Lint::InferredRowBoundary {
                table,
                xml_path,
                child_elements,
            } => write!(
                f,
                "Table '{table}' (xml_path {xml_path}) has {} distinct configured child \
                 elements ({}); row boundaries are inferred, so this table produces {} \
                 partially-filled rows per <{}> rather than one. Configuration format \
                 version 2 fixes it by declaring the row: `row: \".\"` for one row per <{}>, \
                 or `row: <element>` to name the repeating element",
                child_elements.len(),
                child_elements.join(", "),
                child_elements.len(),
                path_segments(xml_path).next_back().unwrap_or("/"),
                path_segments(xml_path).next_back().unwrap_or("/"),
            ),
            Lint::FieldInsideNestedTable {
                table,
                field,
                field_path,
                nested_table,
                nested_table_path,
            } => write!(
                f,
                "Field '{field}' of table '{table}' (path '{field_path}') lies inside table \
                 '{nested_table}' (xml_path {nested_table_path}), which captures every value \
                 there, so '{field}' never receives one. Declare the field on '{nested_table}', \
                 or remove it"
            ),
            Lint::NeverFinalizesRows { table, xml_path } => write!(
                f,
                "Table '{table}' (xml_path {xml_path}) has no configured child element, so no \
                 element close can finalize a row and the table will be empty. Attributes of \
                 the table element itself do not delimit rows — move the table's xml_path up \
                 one level so that <{}> becomes its row element",
                path_segments(xml_path).next_back().unwrap_or("/"),
            ),
            Lint::ExcessLevels {
                table,
                declared,
                available,
            } => write!(
                f,
                "Table '{table}' declares {declared} level(s) but only {available} enclosing \
                 table scope(s) exist (its ancestor tables plus itself); each level labels one \
                 index column, so the extra column(s) never receive a value and assembling the \
                 batch will fail once the table produces a row"
            ),
            Lint::StructuralTable { table } => write!(
                f,
                "Table '{table}' has no fields, key or link, so it is excluded from the output; \
                 it exists only to count its rows for nested tables' position columns"
            ),
            Lint::ImplicitEmptyString { table, fields } => write!(
                f,
                "Table '{table}' has {} non-nullable Utf8 field(s) ({}): a missing or empty \
                 value yields \"\" rather than an error, unlike every other data type. Set \
                 nullable: true to distinguish missing from empty",
                fields.len(),
                fields.join(", "),
            ),
            Lint::ConfigVersion1 { issues } => write_config_version_1(f, issues),
        }
    }
}

/// Renders [`Lint::ConfigVersion1`] as one paragraph: what is left, grouped by
/// kind, each with a count and at most three names.
///
/// Grouped rather than listed because the configs most likely to be far from
/// version 2 are the largest ones — a config generated from a schema can have
/// hundreds of fields, every one spelled `xml_path:`, and one line per field
/// would bury the other kinds of change under a wall of identical ones. The
/// full list is in `issues` for anything that wants to act on it.
fn write_config_version_1(f: &mut fmt::Formatter<'_>, issues: &[ConfigIssue]) -> fmt::Result {
    let (mut scopes, mut rows, mut levels, mut fields, mut links, mut other) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for issue in issues {
        match issue {
            ConfigIssue::ReplacedKey {
                table,
                field: None,
                key: "xml_path",
                ..
            } => scopes.push(table.clone()),
            ConfigIssue::MissingRow { table } => rows.push(table.clone()),
            ConfigIssue::ReplacedKey {
                table,
                key: "levels",
                ..
            } => levels.push(table.clone()),
            ConfigIssue::ReplacedKey {
                table,
                field: Some(field),
                ..
            } => fields.push(format!("{table}.{field}")),
            ConfigIssue::NestedTableWithoutLinks {
                table,
                enclosing_table,
            } => links.push(format!("{table} inside {enclosing_table}")),
            // Anything else is stated as the error itself.
            other_issue => other.push(other_issue.to_string()),
        }
    }

    let mut left = Vec::new();
    if !scopes.is_empty() {
        left.push(format!(
            "{} must rename the `xml_path:` key to `scope:` ({})",
            counted(scopes.len(), "table", "tables"),
            first_few(&scopes),
        ));
    }
    if !rows.is_empty() {
        left.push(format!(
            "{} must declare `row:` rather than infer {} rows ({})",
            counted(rows.len(), "table", "tables"),
            if rows.len() == 1 { "its" } else { "their" },
            first_few(&rows),
        ));
    }
    if !levels.is_empty() {
        left.push(format!(
            "{} must replace `levels:` with `links:` ({})",
            counted(levels.len(), "table", "tables"),
            first_few(&levels),
        ));
    }
    if !fields.is_empty() {
        left.push(format!(
            "{} must rename the `xml_path:` key to `path:` ({})",
            counted(fields.len(), "field", "fields"),
            first_few(&fields),
        ));
    }
    if !links.is_empty() {
        left.push(format!(
            "{} must declare `links:`, or `links: []` for none ({})",
            counted(links.len(), "nested table", "nested tables"),
            first_few(&links),
        ));
    }
    left.extend(other);

    write!(
        f,
        "This config uses configuration format version 1, which is deprecated. "
    )?;
    if left.is_empty() {
        write!(
            f,
            "Nothing in it needs to change; add `version: 2` to finish. "
        )?;
    } else {
        write!(
            f,
            "Before it can declare `version: 2`: {}. ",
            left.join("; ")
        )?;
    }
    write!(
        f,
        "Declaring `version: 2` also changes two defaults: `Utf8` values are trimmed, and a \
         missing non-nullable `Utf8` value is an error rather than \"\""
    )
}

/// `1 table`, `3 tables`.
fn counted(n: usize, singular: &str, plural: &str) -> String {
    format!("{n} {}", if n == 1 { singular } else { plural })
}

/// Up to three names, then an ellipsis — enough to find the first ones, short
/// enough that one kind of change cannot crowd out the others.
fn first_few(names: &[String]) -> String {
    const SHOWN: usize = 3;
    let mut out = names
        .iter()
        .take(SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if names.len() > SHOWN {
        out.push_str(", …");
    }
    out
}

impl Config {
    /// Returns advisory findings about this configuration, in a deterministic
    /// order (tables in configuration order, lints in a fixed order per table).
    ///
    /// An empty result means nothing looked suspicious — it is *not* a
    /// correctness guarantee. See [`Lint`] for what is checked, and
    /// [`Config::validate`] for the checks that are hard errors instead.
    ///
    /// ```rust
    /// # use xml2arrow::{Config, config_from_yaml};
    /// let config = config_from_yaml!(r#"
    /// tables:
    ///   - name: header
    ///     xml_path: /report/header
    ///     levels: []
    ///     fields:
    ///       - {name: title, xml_path: /report/header/title, data_type: Utf8}
    ///       - {name: created, xml_path: /report/header/created, data_type: Utf8}
    /// "#);
    /// for lint in config.lint() {
    ///     println!("{lint}");   // "Table 'header' ... has 2 configured child elements ..."
    /// }
    /// ```
    #[must_use]
    pub fn lint(&self) -> Vec<Lint> {
        // Unknown keys first: a misspelled key is the likeliest explanation for
        // the findings after it.
        let mut lints: Vec<Lint> = self
            .unknown_keys()
            .map(|(location, key)| Lint::UnknownKey {
                location,
                key: key.to_string(),
            })
            .collect();
        // Then paths no element can match, for the same reason.
        lints.extend(
            self.dot_segment_paths()
                .map(|(location, path)| Lint::DotSegmentInPath {
                    location,
                    path: path.to_string(),
                }),
        );
        for table in &self.tables {
            if table.fields.is_empty() {
                // Only a table with no column at all is left out of the output:
                // a key or a link column puts it in. Either way, the
                // row-boundary lints below would be noise for a table without
                // fields.
                if table.key_column_name().is_none()
                    && table.links.as_ref().is_none_or(Vec::is_empty)
                {
                    lints.push(Lint::StructuralTable {
                        table: table.name.clone(),
                    });
                }
                continue;
            }

            // The row-boundary lints describe the *inferred* rule, which only
            // version 1 has. A version 2 table declares `row:`.
            if table.row.is_none() {
                let children = self.row_delimiting_children(table.path());
                match children.len() {
                    0 => lints.push(Lint::NeverFinalizesRows {
                        table: table.name.clone(),
                        xml_path: table.path().to_string(),
                    }),
                    1 => {} // Unambiguous: exactly the element a `row:` would name.
                    _ => lints.push(Lint::InferredRowBoundary {
                        table: table.name.clone(),
                        xml_path: table.path().to_string(),
                        child_elements: children,
                    }),
                }
            }

            // Values are routed by which table's `xml_path` is open, not by which
            // row is, so a nested table captures the field however rows end.
            for field in &table.fields {
                if let Some((field_path, nested)) = self.nested_table_capturing(table, field) {
                    lints.push(Lint::FieldInsideNestedTable {
                        table: table.name.clone(),
                        field: field.name.clone(),
                        field_path,
                        nested_table: nested.name.clone(),
                        nested_table_path: nested.path().to_string(),
                    });
                }
            }

            let available = self.enclosing_table_scopes(table.path());
            if table.levels.len() > available {
                lints.push(Lint::ExcessLevels {
                    table: table.name.clone(),
                    declared: table.levels.len(),
                    available,
                });
            }

            // Version 1 only: under version 2 a missing non-nullable value is an
            // error whatever the type, unless the field says otherwise, so no
            // version 2 config relies on the asymmetry this lint points out.
            let implicit_empty: Vec<String> = table
                .fields
                .iter()
                .filter(|f| self.is_version_1() && f.data_type == DType::Utf8 && !f.nullable)
                .map(|f| f.name.clone())
                .collect();
            if !implicit_empty.is_empty() {
                lints.push(Lint::ImplicitEmptyString {
                    table: table.name.clone(),
                    fields: implicit_empty,
                });
            }
        }

        // Last, after the per-table findings: those describe what a document
        // does *today*, which is more urgent than a deprecation. Any version
        // other than 1 or 2 is a validation error, not a version 1 config.
        if self.is_version_1() {
            // Without the kinds that have a lint of their own, above: each of
            // those says what the problem does to the output today, which
            // matters more than that version 2 rejects it too.
            let issues = self
                .version_2_issues()
                .into_iter()
                .filter(|issue| {
                    !matches!(
                        issue,
                        ConfigIssue::UnknownKey { .. }
                            | ConfigIssue::DotSegmentInPath { .. }
                            | ConfigIssue::FieldInsideNestedTable { .. }
                    )
                })
                .collect();
            lints.push(Lint::ConfigVersion1 { issues });
        }
        lints
    }

    /// How many index values a row of the table at `table_path` receives: one
    /// per enclosing non-root table scope, plus one for the table itself unless
    /// *it* is the root table.
    ///
    /// The root table (`xml_path: /`) is excluded on both counts, because
    /// `end_current_row` skips it when collecting parent indices — it
    /// represents the document rather than a repeating scope, and so supplies
    /// no index to its own rows either. Counting it would let the one config
    /// this lint exists to catch through: a root table declaring any level at
    /// all has an index column that can never be filled.
    fn enclosing_table_scopes(&self, table_path: &str) -> usize {
        let is_root = path_segments(table_path).next().is_none();
        usize::from(!is_root)
            + self
                .tables
                .iter()
                .filter(|ancestor| {
                    path_segments(ancestor.path()).next().is_some()
                        && path_is_strictly_under(table_path, ancestor.path())
                })
                .count()
    }

    /// [`Config::lint`] without the format notice.
    ///
    /// Every version 1 config now carries [`Lint::ConfigVersion1`], and nearly
    /// every test config is version 1 — deliberately, since the lints that
    /// describe inferred rows only arise there. A test about one of those
    /// lints asserts the behavioural findings; the format notice has its own
    /// tests.
    #[cfg(test)]
    pub(crate) fn lint_excluding_deprecation(&self) -> Vec<Lint> {
        self.lint()
            .into_iter()
            .filter(|lint| !matches!(lint, Lint::ConfigVersion1 { .. }))
            .collect()
    }

    /// Every configured direct child *element* of `table_path`, in first-seen
    /// order — the exact set whose closing tags finalize a row of the table at
    /// that path (`close_element` in `xml_parser.rs`).
    ///
    /// Mirrors the registry's view rather than the config's: intermediate
    /// segments of a deep field path (`/a/b/c` contributes `b` to a table at
    /// `/a`) are trie nodes too, and a nested *table's* path counts as well,
    /// because closing its element also ends the parent's row. So does a
    /// `stop_at_paths` entry: registering it creates its node, and closing that
    /// element ends a row even though no field reads it. Attribute pseudo-nodes
    /// are excluded: `parse_attributes` enters and leaves them without going
    /// through the row-finalizing close path.
    pub(crate) fn row_delimiting_children(&self, table_path: &str) -> Vec<String> {
        let depth = path_segments(table_path).count();
        let all_paths = self
            .tables
            .iter()
            .flat_map(|t| {
                std::iter::once(Cow::Borrowed(t.path()))
                    .chain(t.fields.iter().filter_map(|f| resolve_field_path(t, f)))
            })
            .chain(
                self.parser_options
                    .stop_at_paths
                    .iter()
                    .map(|path| Cow::Borrowed(path.as_str())),
            );

        let mut children: Vec<String> = Vec::new();
        for path in all_paths {
            let path = path.as_ref();
            if !path_is_under(path, table_path) {
                continue;
            }
            // `None` when the path *is* the table path; `@`-prefixed when it
            // is an attribute of the table element.
            let Some(child) = path_segments(path).nth(depth) else {
                continue;
            };
            // An attribute never ends a row. Nor does a `.` or `..` segment: no
            // element has that name, so counting one would report rows that
            // cannot occur.
            if child.starts_with('@') || child == "." || child == ".." {
                continue;
            }
            if !children.iter().any(|existing| existing == child) {
                children.push(child.to_string());
            }
        }
        children
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_from_yaml;

    /// The deprecation notice for configuration format version 1.
    mod config_version_1 {
        use super::*;
        use crate::errors::Error;

        fn issues(config: &Config) -> Option<Vec<ConfigIssue>> {
            config.lint().into_iter().find_map(|lint| match lint {
                Lint::ConfigVersion1 { issues } => Some(issues),
                _ => None,
            })
        }

        fn notice(config: &Config) -> String {
            config
                .lint()
                .into_iter()
                .find(|l| matches!(l, Lint::ConfigVersion1 { .. }))
                .unwrap()
                .to_string()
        }

        fn table_key(table: &str, key: &'static str, replacement: &'static str) -> ConfigIssue {
            ConfigIssue::ReplacedKey {
                table: table.into(),
                field: None,
                key,
                replacement,
            }
        }

        fn field_key(table: &str, field: &str) -> ConfigIssue {
            ConfigIssue::ReplacedKey {
                table: table.into(),
                field: Some(field.into()),
                key: "xml_path",
                replacement: "path",
            }
        }

        fn missing_row(table: &str) -> ConfigIssue {
            ConfigIssue::MissingRow {
                table: table.into(),
            }
        }

        /// A version 1 config with one of every kind of change, in the order
        /// they appear.
        fn legacy() -> Config {
            config_from_yaml!(
                r#"
tables:
  - name: stations
    xml_path: /report/stations
    levels: []
    fields:
      - {name: id, xml_path: /report/stations/station/@id, data_type: Utf8}
  - name: measurements
    xml_path: /report/stations/station/ms
    levels: [station]
    fields:
      - {name: v, xml_path: /report/stations/station/ms/m/v, data_type: Int32}
  - name: notes
    xml_path: /report/stations/station/notes
    fields:
      - {name: text, xml_path: /report/stations/station/notes/note/text, data_type: Utf8}
"#
            )
        }

        #[test]
        fn a_version_1_config_is_told_everything_that_is_left_in_order() {
            assert_eq!(
                issues(&legacy()).unwrap(),
                vec![
                    table_key("stations", "xml_path", "scope"),
                    missing_row("stations"),
                    field_key("stations", "id"),
                    table_key("measurements", "xml_path", "scope"),
                    missing_row("measurements"),
                    table_key("measurements", "levels", "links"),
                    field_key("measurements", "v"),
                    table_key("notes", "xml_path", "scope"),
                    missing_row("notes"),
                    field_key("notes", "text"),
                    ConfigIssue::NestedTableWithoutLinks {
                        table: "notes".into(),
                        enclosing_table: "stations".into(),
                    },
                ]
            );
        }

        /// `measurements` uses `levels:` and has no `links:`, but is told
        /// only to replace its levels — doing so is the linking step, and
        /// reporting both would count one change twice.
        #[test]
        fn a_table_with_levels_is_not_also_told_to_add_links() {
            let issues = issues(&legacy()).unwrap();
            assert!(!issues.iter().any(|issue| matches!(
                issue,
                ConfigIssue::NestedTableWithoutLinks { table, .. } if table == "measurements"
            )));
        }

        #[rstest::rstest]
        #[case::absent("")]
        #[case::explicit("version: 1\n")]
        fn absent_and_version_1_are_the_same_format(#[case] header: &str) {
            let yaml = format!(
                "{header}tables:\n  - {{name: t, xml_path: /r, fields: [{{name: v, xml_path: /r/i/v, data_type: Int32}}]}}\n"
            );
            let config = Config::from_yaml_str(&yaml).unwrap();
            assert_eq!(
                issues(&config),
                Some(vec![
                    table_key("t", "xml_path", "scope"),
                    missing_row("t"),
                    field_key("t", "v"),
                ])
            );
        }

        #[test]
        fn a_config_that_declares_version_2_has_no_notice() {
            let config = config_from_yaml!(
                r#"
version: 2
tables:
  - {name: t, scope: /r, row: i, fields: [{name: v, path: v, data_type: Int32}]}
"#
            );
            assert_eq!(issues(&config), None);
        }

        /// Only a config without tables has nothing left, and the `version: 2`
        /// line can still change values, so the message says so.
        #[test]
        fn a_config_with_nothing_left_is_told_the_line_changes_defaults() {
            let config = config_from_yaml!("tables: []\n");
            assert_eq!(issues(&config), Some(vec![]));
            let message = notice(&config);
            assert!(
                message.contains("Nothing in it needs to change"),
                "{message}"
            );
            assert!(message.contains("trimmed"), "{message}");
        }

        /// Misspells `scale` on a field.
        fn unknown_key() -> Config {
            config_from_yaml!(
                r#"
tables:
  - {name: t, xml_path: /r, fields: [{name: v, xml_path: /r/i/v, data_type: Float64, scal: 0.1}]}
"#
            )
        }

        /// `outer.count` lies inside `inner`, which captures its value.
        fn field_inside_nested_table() -> Config {
            config_from_yaml!(
                r#"
tables:
  - {name: outer, xml_path: /r, fields: [{name: count, xml_path: /r/a/bs/@count, data_type: Int32, nullable: true}]}
  - {name: inner, xml_path: /r/a/bs, fields: [{name: v, xml_path: /r/a/bs/b/v, data_type: Int32}]}
"#
            )
        }

        /// The stop path has a `.` segment.
        fn dot_segment() -> Config {
            config_from_yaml!(
                r#"
parser_options: {stop_at_paths: [/r/./end]}
tables:
  - {name: t, xml_path: /r, fields: [{name: v, xml_path: /r/i/v, data_type: Utf8}]}
"#
            )
        }

        /// Version 2 rejects an unknown key, a dot-segment path and a field a
        /// nested table captures, but each already has a lint that says what
        /// it does to the output today. It is reported there once, and not
        /// again in the notice.
        #[rstest::rstest]
        #[case::unknown_key(unknown_key())]
        #[case::dot_segment(dot_segment())]
        #[case::field_inside_nested_table(field_inside_nested_table())]
        fn a_problem_with_its_own_lint_is_reported_once(#[case] config: Config) {
            let lints = config.lint();
            let own_lints = lints
                .iter()
                .filter(|lint| {
                    matches!(
                        lint,
                        Lint::UnknownKey { .. }
                            | Lint::DotSegmentInPath { .. }
                            | Lint::FieldInsideNestedTable { .. }
                    )
                })
                .count();
            assert_eq!(own_lints, 1, "{lints:?}");
            assert!(
                !issues(&config).unwrap().iter().any(|issue| matches!(
                    issue,
                    ConfigIssue::UnknownKey { .. }
                        | ConfigIssue::DotSegmentInPath { .. }
                        | ConfigIssue::FieldInsideNestedTable { .. }
                )),
                "{lints:?}"
            );
        }

        /// The property one list of issues exists to guarantee: declaring
        /// `version: 2` fails with exactly the first issue the list names, and
        /// every issue on it reaches the user, in the notice or as a lint of
        /// its own. A second copy of the rules for either side could drift;
        /// one copy cannot.
        #[test]
        fn the_notice_and_version_2_validation_agree() {
            let configs = [
                legacy(),
                config_from_yaml!("tables: []\n"),
                config_from_yaml!(
                    r#"
tables:
  - {name: doc, xml_path: /, fields: [{name: title, xml_path: /report/title, data_type: Utf8}]}
  - {name: s, xml_path: /report/ss, fields: [{name: v, xml_path: /report/ss/s/v, data_type: Int32}]}
"#
                ),
                field_inside_nested_table(),
                unknown_key(),
                dot_segment(),
            ];
            for config in configs {
                let all = config.version_2_issues();
                let mut v2 = config.clone();
                v2.version = Some(2);
                match v2.validate() {
                    Ok(()) => assert!(all.is_empty(), "validates, yet {all:?} are left"),
                    Err(Error::InvalidConfig { reason }) => {
                        assert_eq!(Some(&reason), all.first(), "{all:?}");
                    }
                    Err(other) => panic!("unexpected error: {other}"),
                }
                let in_notice = issues(&config).expect("every case is version 1");
                let lints = config.lint();
                for issue in &all {
                    let reported = in_notice.contains(issue)
                        || lints.iter().any(|lint| {
                            matches!(
                                (issue, lint),
                                (ConfigIssue::UnknownKey { .. }, Lint::UnknownKey { .. })
                                    | (
                                        ConfigIssue::DotSegmentInPath { .. },
                                        Lint::DotSegmentInPath { .. }
                                    )
                                    | (
                                        ConfigIssue::FieldInsideNestedTable { .. },
                                        Lint::FieldInsideNestedTable { .. }
                                    )
                            )
                        });
                    assert!(reported, "{issue:?} reaches the user nowhere: {lints:?}");
                }
            }
        }

        /// Grouped by kind with a count and the first three names, so a config
        /// generated from a schema — hundreds of `xml_path:` fields — gets a
        /// paragraph rather than a page, and the other kinds stay visible.
        #[test]
        fn the_message_groups_changes_and_shows_a_few_names() {
            let fields: String = (0..40)
                .map(|i| format!("      - {{name: f{i}, xml_path: /r/i/f{i}, data_type: Int32}}\n"))
                .collect();
            let yaml = format!("tables:\n  - name: t\n    xml_path: /r\n    fields:\n{fields}");
            let message = notice(&Config::from_yaml_str(&yaml).unwrap());
            assert!(message.contains("40 fields must rename"), "{message}");
            assert!(message.contains("t.f0, t.f1, t.f2, …"), "{message}");
            assert!(!message.contains("t.f3"), "{message}");
        }

        #[test]
        fn one_of_a_kind_reads_in_the_singular() {
            let config = config_from_yaml!(
                r#"
tables:
  - {name: t, xml_path: /r, fields: [{name: v, xml_path: /r/i/v, data_type: Int32}]}
"#
            );
            let message = notice(&config);
            assert!(
                message.contains(
                    "1 table must declare `row:` rather than infer its rows (t); 1 field must \
                     rename the `xml_path:` key to `path:` (t.v)"
                ),
                "{message}"
            );
        }
    }

    #[test]
    fn single_child_element_is_not_linted() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: value, xml_path: /data/item/value, data_type: Int32}
      - {name: unit, xml_path: /data/item/unit, data_type: Int32}
"#
        );
        assert_eq!(config.lint_excluding_deprecation(), vec![]);
    }

    #[test]
    fn multiple_child_elements_report_inferred_rows() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: header
    xml_path: /report/header
    levels: []
    fields:
      - {name: title, xml_path: /report/header/title, data_type: Int32}
      - {name: created, xml_path: /report/header/created, data_type: Int32}
"#
        );
        assert_eq!(
            config.lint_excluding_deprecation(),
            vec![Lint::InferredRowBoundary {
                table: "header".to_string(),
                xml_path: "/report/header".to_string(),
                child_elements: vec!["title".to_string(), "created".to_string()],
            }]
        );
    }

    #[test]
    fn nested_table_path_counts_as_a_row_delimiting_child() {
        // The `station` element closing ends a row of `stations` even though
        // no field of `stations` sits under it — the registry marks it as a
        // configured child either way.
        let config = config_from_yaml!(
            r#"
tables:
  - name: stations
    xml_path: /report/stations
    levels: []
    fields:
      - {name: count, xml_path: /report/stations/count, data_type: Int32}
  - name: measurements
    xml_path: /report/stations/station
    levels: []
    fields:
      - {name: value, xml_path: /report/stations/station/m/value, data_type: Int32}
"#
        );
        let lints = config.lint_excluding_deprecation();
        assert_eq!(
            lints[0],
            Lint::InferredRowBoundary {
                table: "stations".to_string(),
                xml_path: "/report/stations".to_string(),
                child_elements: vec!["count".to_string(), "station".to_string()],
            }
        );
    }

    #[test]
    fn deep_field_paths_contribute_their_first_segment_only() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: a, xml_path: /data/item/deep/a, data_type: Int32}
      - {name: b, xml_path: /data/item/deep/b, data_type: Int32}
"#
        );
        assert_eq!(config.lint_excluding_deprecation(), vec![]);
    }

    #[test]
    fn attribute_only_table_never_finalizes_rows() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: doc
    xml_path: /data
    levels: []
    fields:
      - {name: id, xml_path: /data/@id, data_type: Int32}
"#
        );
        assert_eq!(
            config.lint_excluding_deprecation(),
            vec![Lint::NeverFinalizesRows {
                table: "doc".to_string(),
                xml_path: "/data".to_string(),
            }]
        );
    }

    #[test]
    fn structural_table_is_reported_once_and_skips_other_lints() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: scope
    xml_path: /data
    levels: []
    fields: []
  - name: items
    xml_path: /data/items
    levels: [scope]
    fields:
      - {name: value, xml_path: /data/items/item/value, data_type: Int32}
"#
        );
        assert_eq!(
            config.lint_excluding_deprecation(),
            vec![Lint::StructuralTable {
                table: "scope".to_string()
            }]
        );
    }

    /// A table without fields that declares a key has a column, so it is
    /// output, and there is nothing structural to report.
    #[test]
    fn a_table_without_fields_but_with_a_key_is_not_structural() {
        let config = config_from_yaml!(
            r#"
version: 2
tables:
  - {name: stations, scope: /r/ss, row: s, row_id: true, fields: []}
  - name: readings
    scope: /r/ss/s/rs
    row: r
    links: [{parent: stations}]
    fields:
      - {name: v, path: v, data_type: Int32}
"#
        );
        assert_eq!(config.lint(), vec![]);
    }

    #[test]
    fn excess_levels_are_reported_against_enclosing_scopes() {
        // `items` has one ancestor table (`scope`) plus itself = 2 scopes, so
        // a third level can never receive a value.
        let config = config_from_yaml!(
            r#"
tables:
  - name: scope
    xml_path: /data
    levels: []
    fields: []
  - name: items
    xml_path: /data/items
    levels: [scope, item, surplus]
    fields:
      - {name: v, xml_path: /data/items/item/v, data_type: Int32}
"#
        );
        assert!(
            config
                .lint_excluding_deprecation()
                .contains(&Lint::ExcessLevels {
                    table: "items".to_string(),
                    declared: 3,
                    available: 2,
                })
        );
    }

    #[test]
    fn levels_matching_the_scope_depth_are_not_reported() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: scope
    xml_path: /data
    levels: []
    fields: []
  - name: items
    xml_path: /data/items
    levels: [scope, item]
    fields:
      - {name: v, xml_path: /data/items/item/v, data_type: Int32}
"#
        );
        assert!(
            !config
                .lint()
                .iter()
                .any(|lint| matches!(lint, Lint::ExcessLevels { .. }))
        );
    }

    #[test]
    fn a_root_table_declaring_any_level_is_reported() {
        // The root table supplies no index even to its own rows, so a single
        // level is already one more than can ever be filled. Without this the
        // config parses and then dies at batch assembly with "all columns in a
        // record batch must have the same length" — the opaque failure this
        // lint exists to pre-empt.
        let config = config_from_yaml!(
            r#"
tables:
  - name: doc
    xml_path: /
    levels: [report]
    fields:
      - {name: title, xml_path: /report/title, data_type: Int32}
"#
        );
        assert!(
            config
                .lint_excluding_deprecation()
                .contains(&Lint::ExcessLevels {
                    table: "doc".to_string(),
                    declared: 1,
                    available: 0,
                })
        );
    }

    #[test]
    fn a_root_table_without_levels_is_not_reported() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: doc
    xml_path: /
    levels: []
    fields:
      - {name: title, xml_path: /report/title, data_type: Int32}
"#
        );
        assert!(
            !config
                .lint()
                .iter()
                .any(|lint| matches!(lint, Lint::ExcessLevels { .. }))
        );
    }

    #[test]
    fn root_table_does_not_count_as_an_enclosing_scope() {
        // The root table is skipped when parent indices are collected, so a
        // table under it has only itself.
        let config = config_from_yaml!(
            r#"
tables:
  - name: doc
    xml_path: /
    levels: []
    fields:
      - {name: title, xml_path: /report/title, data_type: Utf8}
  - name: items
    xml_path: /report/items
    levels: [a, b]
    fields:
      - {name: v, xml_path: /report/items/item/v, data_type: Int32}
"#
        );
        assert!(
            config
                .lint_excluding_deprecation()
                .contains(&Lint::ExcessLevels {
                    table: "items".to_string(),
                    declared: 2,
                    available: 1,
                })
        );
    }

    #[test]
    fn non_nullable_utf8_fields_are_reported_together() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: name, xml_path: /data/item/name, data_type: Utf8}
      - {name: label, xml_path: /data/item/label, data_type: Utf8}
      - {name: note, xml_path: /data/item/note, data_type: Utf8, nullable: true}
      - {name: count, xml_path: /data/item/count, data_type: Int32}
"#
        );
        assert_eq!(
            config.lint_excluding_deprecation(),
            vec![Lint::ImplicitEmptyString {
                table: "items".to_string(),
                fields: vec!["name".to_string(), "label".to_string()],
            }]
        );
    }

    #[test]
    fn version_2_has_no_implicit_empty_strings_to_report() {
        // Under `version: 2` a missing non-nullable `Utf8` value is an error,
        // so a warning that it yields "" would be false.
        let config = config_from_yaml!(
            r#"
version: 2
tables:
  - name: items
    scope: /data
    row: item
    fields:
      - {name: name, path: name, data_type: Utf8}
"#
        );
        assert_eq!(config.lint(), vec![]);
    }

    /// A stop path under a table is a trie node like any configured child, so
    /// its closing tag ends a row too, and it counts toward inferred rows.
    #[test]
    fn a_stop_path_under_a_table_counts_as_a_row_ending_child() {
        let config = config_from_yaml!(
            r#"
parser_options:
  stop_at_paths: [/data/marker]
tables:
  - name: items
    xml_path: /data
    levels: []
    fields:
      - {name: v, xml_path: /data/item/v, data_type: Int32, nullable: true}
"#
        );
        assert_eq!(
            config.lint_excluding_deprecation(),
            vec![Lint::InferredRowBoundary {
                table: "items".to_string(),
                xml_path: "/data".to_string(),
                child_elements: vec!["item".to_string(), "marker".to_string()],
            }]
        );
    }

    #[test]
    fn root_table_children_are_resolved_from_the_document_element() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: doc
    xml_path: /
    levels: []
    fields:
      - {name: title, xml_path: /report/title, data_type: Int32}
"#
        );
        assert_eq!(config.lint_excluding_deprecation(), vec![]);
    }

    #[test]
    fn messages_name_the_table_and_the_offending_elements() {
        let lint = Lint::InferredRowBoundary {
            table: "header".to_string(),
            xml_path: "/report/header".to_string(),
            child_elements: vec!["title".to_string(), "created".to_string()],
        };
        let message = lint.to_string();
        assert!(message.contains("'header'"));
        assert!(message.contains("title, created"));
        assert!(message.contains("<header>"));
        // The lint carries the fix, not just the diagnosis.
        assert!(message.contains("row:"));
    }

    // --- Declared rows silence the inferred-boundary lints --------------------

    /// The lint describes a rule the table no longer uses. Reporting it after a
    /// user has taken the lint's own advice would be the worst kind of noise.
    #[test]
    fn declaring_a_row_silences_the_inferred_boundary_lint() {
        let inferred = config_from_yaml!(
            r#"
            tables:
              - name: header
                xml_path: /report/header
                levels: []
                fields:
                  - {name: title, xml_path: /report/header/title, data_type: Utf8}
                  - {name: created, xml_path: /report/header/created, data_type: Utf8}
            "#
        );
        assert!(
            inferred
                .lint()
                .iter()
                .any(|l| matches!(l, Lint::InferredRowBoundary { .. }))
        );

        let declared = config_from_yaml!(
            r#"
            version: 2
            tables:
              - name: header
                scope: /report/header
                row: "."
                fields:
                  - {name: title, path: title, data_type: Utf8}
                  - {name: created, path: created, data_type: Utf8}
            "#
        );
        assert!(
            !declared
                .lint()
                .iter()
                .any(|l| matches!(l, Lint::InferredRowBoundary { .. }))
        );
    }

    /// `NeverFinalizesRows` is likewise a statement about inference. A declared
    /// row is the fix for it, so it must not survive the fix.
    #[test]
    fn declaring_a_row_silences_the_never_finalizes_lint() {
        let config = config_from_yaml!(
            r#"
            version: 2
            tables:
              - name: t
                scope: /a
                row: "."
                fields:
                  - {name: id, path: "@id", data_type: Utf8}
            "#
        );
        assert!(
            !config
                .lint()
                .iter()
                .any(|l| matches!(l, Lint::NeverFinalizesRows { .. }))
        );
    }

    /// No element is named `.` or `..`, so a version 1 path with such a
    /// segment matches nothing. It is reported, and not counted as a child
    /// element that ends rows: `.` here would otherwise make the table look as
    /// if its rows were split.
    #[test]
    fn a_path_with_a_dot_segment_is_reported_and_ends_no_rows() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: t
                xml_path: /r
                levels: []
                fields:
                  - {name: v, xml_path: /r/item/v, data_type: Utf8, nullable: true}
                  - {name: w, xml_path: /r/./item/w, data_type: Utf8, nullable: true}
            "#
        );
        let lints = config.lint_excluding_deprecation();
        assert_eq!(
            lints,
            vec![Lint::DotSegmentInPath {
                location: "field 'w' of table 't'".into(),
                path: "/r/./item/w".into(),
            }]
        );
        assert_eq!(
            config.row_delimiting_children("/r"),
            vec!["item".to_string()]
        );
    }

    /// A misspelled key is ignored by version 1, which leaves the setting it
    /// was meant to be at its default. Reported first, where it was written.
    #[test]
    fn an_unknown_key_is_reported_where_it_was_written() {
        let config = config_from_yaml!(
            r#"
            parser_options: {trim_txt: true}
            tables:
              - name: t
                xml_path: /r
                fields:
                  - {name: v, xml_path: /r/i/v, data_type: Float64, scal: 0.1}
            "#
        );
        let lints = config.lint_excluding_deprecation();
        assert_eq!(
            lints[..2],
            [
                Lint::UnknownKey {
                    location: "`parser_options`".into(),
                    key: "trim_txt".into(),
                },
                Lint::UnknownKey {
                    location: "field 'v' of table 't'".into(),
                    key: "scal".into(),
                },
            ],
            "{lints:?}"
        );
        assert_eq!(
            lints[1].to_string(),
            "Unknown key 'scal' in field 'v' of table 't' is ignored; check its spelling, or \
             remove it"
        );
    }

    /// The shape the frozen corpus pins as `self_closing_table_element`: `n`
    /// lies inside `inner`, so `outer.n` is null in every row even though the
    /// document holds a value. `name` sits beside `inner` and is not reported.
    #[test]
    fn a_field_inside_a_nested_table_is_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: outer
                xml_path: /data
                levels: []
                fields:
                  - {name: n, xml_path: /data/group/n, data_type: Int32, nullable: true}
                  - {name: name, xml_path: /data/name, data_type: Utf8, nullable: true}
              - name: inner
                xml_path: /data/group
                levels: [group]
                fields:
                  - {name: v, xml_path: /data/group/item/v, data_type: Int32, nullable: true}
            "#
        );
        let lints = config.lint_excluding_deprecation();
        let captured: Vec<&Lint> = lints
            .iter()
            .filter(|l| matches!(l, Lint::FieldInsideNestedTable { .. }))
            .collect();
        assert_eq!(
            captured,
            vec![&Lint::FieldInsideNestedTable {
                table: "outer".into(),
                field: "n".into(),
                field_path: "/data/group/n".into(),
                nested_table: "inner".into(),
                nested_table_path: "/data/group".into(),
            }],
            "got {lints:?}"
        );
        let message = captured[0].to_string();
        assert!(
            message.contains("'n'") && message.contains("'inner'"),
            "{message}"
        );
    }
}
