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

/// One change a configuration still needs before it can declare `version: 2`.
///
/// Reported, all together, by [`Lint::ConfigVersion1`]. The same list drives
/// `Config::validate` for a config that *does* declare `version: 2` — the first
/// entry is the error — so what the lint says is left and what validation
/// rejects cannot disagree.
///
/// `#[non_exhaustive]`: configuration format version 2 may tighten further
/// before 1.0, and each new requirement arrives as a variant.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MigrationStep {
    /// The table leaves its row boundaries inferred; it must declare `row:`.
    DeclareRow {
        /// The table without a `row:`.
        table: String,
    },
    /// The table uses `levels:`; it must declare `links:` instead.
    ///
    /// Not always value-preserving: an `index_of:` link reproduces a level
    /// column that counts an enclosing table's rows, but nothing reproduces
    /// the one counting the table's own rows.
    ReplaceLevels {
        /// The table using `levels:`.
        table: String,
    },
    /// The field is spelled with `xml_path:`; the key must be renamed to
    /// `path:`. The value needs no change.
    RenameFieldXmlPath {
        /// The field's table.
        table: String,
        /// The field using `xml_path:`.
        field: String,
    },
    /// A table nested inside another omits `links:`, so nothing in the config
    /// says whether its rows relate to the enclosing table's. It must declare
    /// the key — `links: []` if the table deliberately has no link.
    LinkNestedTable {
        /// The nested table.
        table: String,
        /// The nearest table enclosing it.
        enclosing_table: String,
    },
}

impl MigrationStep {
    /// The validation error for a config that declares `version: 2` while this
    /// step is still outstanding.
    pub(crate) fn into_config_issue(self) -> ConfigIssue {
        match self {
            MigrationStep::DeclareRow { table } => ConfigIssue::InferredRowInVersion2 { table },
            MigrationStep::ReplaceLevels { table } => ConfigIssue::LevelsInVersion2 { table },
            MigrationStep::RenameFieldXmlPath { table, field } => {
                ConfigIssue::FieldXmlPathInVersion2 { table, field }
            }
            MigrationStep::LinkNestedTable {
                table,
                enclosing_table,
            } => ConfigIssue::NestedTableWithoutLinksInVersion2 {
                table,
                enclosing_table,
            },
        }
    }
}

impl fmt::Display for MigrationStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationStep::DeclareRow { table } => {
                write!(f, "table '{table}': declare `row:`; its rows are inferred")
            }
            MigrationStep::ReplaceLevels { table } => {
                write!(f, "table '{table}': replace `levels:` with `links:`")
            }
            MigrationStep::RenameFieldXmlPath { table, field } => write!(
                f,
                "field '{field}' of table '{table}': rename the `xml_path:` key to `path:`; \
                 the value is unchanged"
            ),
            MigrationStep::LinkNestedTable {
                table,
                enclosing_table,
            } => write!(
                f,
                "table '{table}': declare `links:` relating it to '{enclosing_table}', which \
                 encloses it — or `links: []` if it deliberately has no link"
            ),
        }
    }
}

/// An advisory finding about a configuration.
///
/// `#[non_exhaustive]`: new lints are added in minor releases, and matches must
/// carry a wildcard arm. Use the [`fmt::Display`] rendering for messages —
/// the wording is not a stability surface, but the variant set is.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lint {
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
    /// A table declares `row:`, but one of its fields sits outside that row's
    /// subtree, so the field cannot be part of the row it is configured on.
    ///
    /// Advisory rather than an error because the field still behaves exactly
    /// as it did before the `row:` line was added — its value attaches to
    /// whichever row finalizes next. That is rarely what the author meant, but
    /// it is not a new failure, and Phase C does not break working configs.
    FieldOutsideRow {
        /// The table declaring the row.
        table: String,
        /// The field that sits outside it.
        field: String,
        /// The field's resolved path.
        field_path: String,
        /// The row element's resolved path, which does not contain it.
        row_path: String,
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
    /// A table declares no fields. It is excluded from the output entirely and
    /// exists only to feed its row counter to descendant tables' `levels`
    /// index columns.
    StructuralTable {
        /// The table that declares no fields.
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
    /// affected table never yields a row works today, and this phase does not
    /// break working configs.
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
    /// Only fields left on that default are reported. A field whose
    /// `on_missing` is stated, on the field or under `defaults:`, has chosen
    /// its behavior; and under `version: 2` the default is an error.
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
    /// exactly what is left: `steps` is everything `version: 2` would still
    /// reject, in the order it appears in the YAML.
    ///
    /// An empty `steps` means the config already uses only version 2 keys and
    /// needs nothing but the `version: 2` line — which is the step that can
    /// change values, since it switches two defaults (see
    /// [`Config::version`](crate::Config::version)).
    ConfigVersion1 {
        /// What must change before the config can declare `version: 2`.
        steps: Vec<MigrationStep>,
    },
}

impl fmt::Display for Lint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lint::InferredRowBoundary {
                table,
                xml_path,
                child_elements,
            } => write!(
                f,
                "Table '{table}' (xml_path {xml_path}) has {} distinct configured child \
                 elements ({}); row boundaries are inferred, so this table produces {} \
                 partially-filled rows per <{}> rather than one. Declare `row:` to fix it: \
                 `row: \".\"` for one row per <{}>, or `row: <element>` to name the \
                 repeating element",
                child_elements.len(),
                child_elements.join(", "),
                child_elements.len(),
                path_segments(xml_path).next_back().unwrap_or("/"),
                path_segments(xml_path).next_back().unwrap_or("/"),
            ),
            Lint::FieldOutsideRow {
                table,
                field,
                field_path,
                row_path,
            } => write!(
                f,
                "Table '{table}' declares its row at '{row_path}', but field '{field}' \
                 (xml_path '{field_path}') is outside that subtree, so its value attaches to \
                 whichever row finalizes next rather than to a row of its own element"
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
                "Table '{table}' declares no fields: it is excluded from the output and exists \
                 only to supply index values to nested tables' `levels` columns"
            ),
            Lint::ImplicitEmptyString { table, fields } => write!(
                f,
                "Table '{table}' has {} non-nullable Utf8 field(s) ({}): a missing or empty \
                 value yields \"\" rather than an error, unlike every other data type. Set \
                 nullable: true to distinguish missing from empty",
                fields.len(),
                fields.join(", "),
            ),
            Lint::ConfigVersion1 { steps } => write_config_version_1(f, steps),
        }
    }
}

/// Renders [`Lint::ConfigVersion1`] as one paragraph: what is left, grouped by
/// kind, each with a count and at most three names.
///
/// Grouped rather than listed because the configs most likely to be far from
/// version 2 are the largest ones — a config generated from a schema can have
/// hundreds of fields, every one spelled `xml_path:`, and one line per field
/// would bury the other kinds of step under a wall of identical ones. The full
/// list is in `steps` for anything that wants to act on it.
fn write_config_version_1(f: &mut fmt::Formatter<'_>, steps: &[MigrationStep]) -> fmt::Result {
    let (mut rows, mut levels, mut fields, mut links) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for step in steps {
        match step {
            MigrationStep::DeclareRow { table } => rows.push(table.clone()),
            MigrationStep::ReplaceLevels { table } => levels.push(table.clone()),
            MigrationStep::RenameFieldXmlPath { table, field } => {
                fields.push(format!("{table}.{field}"))
            }
            MigrationStep::LinkNestedTable {
                table,
                enclosing_table,
            } => links.push(format!("{table} inside {enclosing_table}")),
        }
    }

    let mut left = Vec::new();
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

    write!(
        f,
        "This config uses configuration format version 1, which is deprecated. "
    )?;
    if left.is_empty() {
        write!(
            f,
            "It already uses only version 2 keys; add `version: 2` to finish. "
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
/// enough that one kind of step cannot crowd out the others.
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
        let mut lints = Vec::new();
        for table in &self.tables {
            if table.fields.is_empty() {
                // A structural table's row counter is all that matters, so the
                // row-boundary lints below would be noise for it.
                lints.push(Lint::StructuralTable {
                    table: table.name.clone(),
                });
                continue;
            }

            // The row-boundary lints describe the *inferred* rule. A table that
            // declares `row:` has opted out of it, so reporting inference
            // there would be advice to do what it already did.
            match table.row_path() {
                Some(row_path) => {
                    for field in &table.fields {
                        // A relative `path` resolves *into* the row subtree by
                        // construction, so only an absolute spelling can fall
                        // outside it. An unresolvable field is a validation
                        // error, not a lint, so skip it here.
                        let Some(field_path) = resolve_field_path(table, field) else {
                            continue;
                        };
                        if !path_is_under(&field_path, &row_path) {
                            lints.push(Lint::FieldOutsideRow {
                                table: table.name.clone(),
                                field: field.name.clone(),
                                field_path: field_path.into_owned(),
                                row_path: row_path.clone(),
                            });
                        }
                    }
                }
                None => {
                    let children = self.row_delimiting_children(&table.xml_path);
                    match children.len() {
                        0 => lints.push(Lint::NeverFinalizesRows {
                            table: table.name.clone(),
                            xml_path: table.xml_path.clone(),
                        }),
                        1 => {} // Unambiguous: exactly the element a `row:` would name.
                        _ => lints.push(Lint::InferredRowBoundary {
                            table: table.name.clone(),
                            xml_path: table.xml_path.clone(),
                            child_elements: children,
                        }),
                    }
                }
            }

            let available = self.enclosing_table_scopes(&table.xml_path);
            if table.levels.len() > available {
                lints.push(Lint::ExcessLevels {
                    table: table.name.clone(),
                    declared: table.levels.len(),
                    available,
                });
            }

            // Only where the "" is the type-dependent default nobody asked for.
            // Under `version: 2` that default is an error, and a field (or
            // `defaults:`) stating `on_missing` has chosen its behavior —
            // neither relies on the asymmetry this lint points out.
            let implicit_empty: Vec<String> = table
                .fields
                .iter()
                .filter(|f| f.data_type == DType::Utf8 && !f.nullable)
                .filter(|f| {
                    self.version != Some(2)
                        && f.policies.on_missing.is_none()
                        && self
                            .defaults
                            .as_ref()
                            .is_none_or(|d| d.on_missing.is_none())
                })
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
        // does *today*, which is more urgent than a deprecation. Absent and
        // `1` are the same format; any other value is a validation error, not
        // a version 1 config.
        if matches!(self.version, None | Some(1)) {
            lints.push(Lint::ConfigVersion1 {
                steps: self.version_2_steps(),
            });
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
                    path_segments(&ancestor.xml_path).next().is_some()
                        && path_is_strictly_under(table_path, &ancestor.xml_path)
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
    /// because closing its element also ends the parent's row. Attribute
    /// pseudo-nodes are excluded: `parse_attributes` enters and leaves them
    /// without going through the row-finalizing close path.
    fn row_delimiting_children(&self, table_path: &str) -> Vec<String> {
        let depth = path_segments(table_path).count();
        let all_paths = self.tables.iter().flat_map(|t| {
            std::iter::once(Cow::Borrowed(t.xml_path.as_str()))
                .chain(t.fields.iter().filter_map(|f| resolve_field_path(t, f)))
        });

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
            if child.starts_with('@') {
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

        fn steps(config: &Config) -> Option<Vec<MigrationStep>> {
            config.lint().into_iter().find_map(|lint| match lint {
                Lint::ConfigVersion1 { steps } => Some(steps),
                _ => None,
            })
        }

        /// A version 1 config with one of every step, in the order they appear.
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
    row: note
    fields:
      - {name: text, path: text, data_type: Utf8}
"#
            )
        }

        #[test]
        fn a_version_1_config_is_told_everything_that_is_left_in_order() {
            assert_eq!(
                steps(&legacy()).unwrap(),
                vec![
                    MigrationStep::DeclareRow {
                        table: "stations".into()
                    },
                    MigrationStep::RenameFieldXmlPath {
                        table: "stations".into(),
                        field: "id".into(),
                    },
                    MigrationStep::DeclareRow {
                        table: "measurements".into()
                    },
                    MigrationStep::ReplaceLevels {
                        table: "measurements".into()
                    },
                    MigrationStep::RenameFieldXmlPath {
                        table: "measurements".into(),
                        field: "v".into(),
                    },
                    MigrationStep::LinkNestedTable {
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
            let steps = steps(&legacy()).unwrap();
            assert!(!steps.iter().any(|s| matches!(
                s,
                MigrationStep::LinkNestedTable { table, .. } if table == "measurements"
            )));
        }

        /// `links: []` is a declaration — "deliberately unlinked" — so a
        /// table carrying it has nothing left to do on that front.
        #[test]
        fn a_table_declaring_links_empty_is_not_told_to_add_links() {
            let config = config_from_yaml!(
                r#"
tables:
  - {name: outer, xml_path: /r/os, row: o, fields: [{name: a, path: a, data_type: Int32}]}
  - {name: inner, xml_path: /r/os/o/is, row: i, links: [], fields: [{name: b, path: b, data_type: Int32}]}
"#
            );
            assert_eq!(steps(&config), Some(vec![]));
        }

        #[rstest::rstest]
        #[case::absent("")]
        #[case::explicit("version: 1\n")]
        fn absent_and_version_1_are_the_same_format(#[case] header: &str) {
            let yaml = format!(
                "{header}tables:\n  - {{name: t, xml_path: /r, row: i, fields: [{{name: v, path: v, data_type: Int32}}]}}\n"
            );
            let config: Config = yaml_serde::from_str(&yaml).unwrap();
            assert_eq!(steps(&config), Some(vec![]));
        }

        #[test]
        fn a_config_that_declares_version_2_has_no_notice() {
            let config = config_from_yaml!(
                r#"
version: 2
tables:
  - {name: t, xml_path: /r, row: i, fields: [{name: v, path: v, data_type: Int32}]}
"#
            );
            assert_eq!(steps(&config), None);
        }

        /// Only the `version: 2` line is left — and that line is the one step
        /// that can change values, so the message must still say so.
        #[test]
        fn a_config_using_only_version_2_keys_is_told_the_last_step_changes_defaults() {
            let config = config_from_yaml!(
                r#"
tables:
  - {name: t, xml_path: /r, row: i, fields: [{name: v, path: v, data_type: Utf8}]}
"#
            );
            assert_eq!(steps(&config), Some(vec![]));
            let message = config
                .lint()
                .into_iter()
                .find(|l| matches!(l, Lint::ConfigVersion1 { .. }))
                .unwrap()
                .to_string();
            assert!(
                message.contains("already uses only version 2 keys"),
                "{message}"
            );
            assert!(message.contains("trimmed"), "{message}");
        }

        /// The property `version_2_steps` exists to guarantee: the lint lists
        /// nothing exactly when declaring `version: 2` would validate, and
        /// otherwise the validation error is the lint's first step. A second
        /// copy of the rules for either side could drift; one copy cannot.
        #[test]
        fn the_notice_and_version_2_validation_agree() {
            let with_version = |config: &Config| {
                let mut v2 = config.clone();
                v2.version = Some(2);
                v2
            };
            let configs = [
                legacy(),
                config_from_yaml!(
                    r#"
tables:
  - {name: t, xml_path: /r, row: i, fields: [{name: v, path: v, data_type: Int32}]}
"#
                ),
                // A root metadata table: the table below it is nested, and must
                // say so — the lint and validation must agree on that too.
                config_from_yaml!(
                    r#"
tables:
  - {name: doc, xml_path: /, row: report, fields: [{name: title, path: title, data_type: Utf8}]}
  - {name: s, xml_path: /report/ss, row: s, fields: [{name: v, path: v, data_type: Int32}]}
"#
                ),
            ];
            for config in configs {
                let steps = steps(&config).expect("every case is version 1");
                match with_version(&config).validate() {
                    Ok(()) => assert!(steps.is_empty(), "validates, yet the lint lists {steps:?}"),
                    Err(Error::InvalidConfig { reason }) => {
                        let first = steps.first().expect("rejected, yet the lint lists nothing");
                        assert_eq!(
                            format!("{reason}"),
                            format!("{}", first.clone().into_config_issue())
                        );
                    }
                    Err(other) => panic!("unexpected error: {other}"),
                }
            }
        }

        /// Grouped by kind with a count and the first three names, so a config
        /// generated from a schema — hundreds of `xml_path:` fields — gets a
        /// paragraph rather than a page, and the other kinds stay visible.
        #[test]
        fn the_message_groups_steps_and_shows_a_few_names() {
            let fields: String = (0..40)
                .map(|i| format!("      - {{name: f{i}, xml_path: /r/i/f{i}, data_type: Int32}}\n"))
                .collect();
            let yaml = format!(
                "tables:\n  - name: t\n    xml_path: /r\n    row: i\n    fields:\n{fields}"
            );
            let config: Config = yaml_serde::from_str(&yaml).unwrap();
            let message = config
                .lint()
                .into_iter()
                .find(|l| matches!(l, Lint::ConfigVersion1 { .. }))
                .unwrap()
                .to_string();
            assert!(message.contains("40 fields must rename"), "{message}");
            assert!(message.contains("t.f0, t.f1, t.f2, …"), "{message}");
            assert!(!message.contains("t.f3"), "{message}");
        }

        #[test]
        fn a_single_step_reads_in_the_singular() {
            let config = config_from_yaml!(
                r#"
tables:
  - {name: t, xml_path: /r, fields: [{name: v, path: /r/i/v, data_type: Int32}]}
"#
            );
            let message = config
                .lint()
                .into_iter()
                .find(|l| matches!(l, Lint::ConfigVersion1 { .. }))
                .unwrap()
                .to_string();
            assert!(
                message.contains("1 table must declare `row:` rather than infer its rows (t)"),
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
    fn non_nullable_utf8_fields_that_state_on_missing_are_not_reported() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: name, path: name, data_type: Utf8, on_missing: error}
      - {name: label, path: label, data_type: Utf8, on_missing: empty}
      - {name: note, path: note, data_type: Utf8}
"#
        );
        assert_eq!(
            config.lint_excluding_deprecation(),
            vec![Lint::ImplicitEmptyString {
                table: "items".to_string(),
                fields: vec!["note".to_string()],
            }]
        );
    }

    #[test]
    fn an_on_missing_default_covers_every_field() {
        let config = config_from_yaml!(
            r#"
defaults:
  on_missing: error
tables:
  - name: items
    xml_path: /data
    row: item
    fields:
      - {name: name, path: name, data_type: Utf8}
"#
        );
        assert_eq!(config.lint_excluding_deprecation(), vec![]);
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
    xml_path: /data
    row: item
    fields:
      - {name: name, path: name, data_type: Utf8}
"#
        );
        assert_eq!(config.lint(), vec![]);
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
        // The lint's job in Phase C is to carry the fix, not just the diagnosis.
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
            tables:
              - name: header
                xml_path: /report/header
                row: "."
                levels: []
                fields:
                  - {name: title, xml_path: /report/header/title, data_type: Utf8}
                  - {name: created, xml_path: /report/header/created, data_type: Utf8}
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
            tables:
              - name: t
                xml_path: /a
                row: "."
                levels: []
                fields:
                  - {name: id, xml_path: /a/@id, data_type: Utf8}
            "#
        );
        assert!(
            !config
                .lint()
                .iter()
                .any(|l| matches!(l, Lint::NeverFinalizesRows { .. }))
        );
    }

    #[test]
    fn a_field_outside_the_declared_row_is_reported() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: t
                xml_path: /report/data
                row: item
                levels: []
                fields:
                  - {name: v, xml_path: /report/data/item/v, data_type: Int32}
                  - {name: stray, xml_path: /report/data/summary, data_type: Utf8}
            "#
        );
        let lints = config.lint_excluding_deprecation();
        let outside: Vec<&Lint> = lints
            .iter()
            .filter(|l| matches!(l, Lint::FieldOutsideRow { .. }))
            .collect();
        assert_eq!(outside.len(), 1, "got {lints:?}");
        let Lint::FieldOutsideRow {
            field, row_path, ..
        } = outside[0]
        else {
            unreachable!()
        };
        assert_eq!(field, "stray");
        assert_eq!(row_path, "/report/data/item");
        assert!(outside[0].to_string().contains("'stray'"));
    }

    /// An attribute of the row element is inside the row subtree, so declaring
    /// a row must not flag it.
    #[test]
    fn an_attribute_of_the_row_element_is_inside_the_row() {
        let config = config_from_yaml!(
            r#"
            tables:
              - name: t
                xml_path: /report/data
                row: item
                levels: []
                fields:
                  - {name: id, xml_path: /report/data/item/@id, data_type: Utf8}
            "#
        );
        assert!(
            !config
                .lint()
                .iter()
                .any(|l| matches!(l, Lint::FieldOutsideRow { .. })),
            "got {:?}",
            config.lint_excluding_deprecation()
        );
    }
}
