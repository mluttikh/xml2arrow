//! Configuration lints: warnings about configs that are *valid* but whose
//! behavior is likely to surprise.
//!
//! [`Config::validate`] rejects configs that cannot work. This module reports
//! the next tier: configs that load and run, but in which a table can never
//! produce a row, a column can never be filled, or a value is handled by a rule
//! users routinely get wrong.
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

use crate::config::{
    Config, DType, FieldConfig, TableConfig, has_dot_segment, path_is_strictly_under,
    path_is_under, path_segments,
};

/// An advisory finding about a configuration.
///
/// `#[non_exhaustive]`: new lints are added in minor releases, and matches must
/// carry a wildcard arm. Use the [`fmt::Display`] rendering for messages; the
/// wording is not a stability surface, but the variant set is.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lint {
    /// A path has a `.` or `..` segment, which no element can match, since an
    /// XML name cannot be `.` or `..`. The table has no rows, the field is never
    /// filled, or the stop path never stops the parse.
    DotSegmentInPath {
        /// Where the path is: `the xml_path of table 'stations'`, `field 'id'
        /// of table 'stations'`, `` `parser_options.stop_at_paths` ``.
        location: String,
        /// The path, as written.
        path: String,
    },
    /// A field lies inside the `xml_path` of another table nested within its
    /// own, so it never receives a value: whenever a value there arrives, the
    /// nested table is the innermost open table, and values go to its fields
    /// only.
    ///
    /// The column is all null, all `""`, or a `MissingRequiredField` error,
    /// however the document is written.
    FieldInsideNestedTable {
        /// The table the field is declared on.
        table: String,
        /// The field that never receives a value.
        field: String,
        /// The field's `xml_path`.
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
    /// element itself (`/a/@id` on a table at `/a`): the attribute
    /// pseudo-nodes are entered and left without going through the
    /// row-finalizing close path.
    NeverFinalizesRows {
        /// The table that can never finalize a row.
        table: String,
        /// Its `xml_path`, which has no configured child element.
        xml_path: String,
    },
    /// A table has no fields. It is excluded from the output entirely and
    /// exists only to feed its row counter to descendant tables' `levels`
    /// columns.
    StructuralTable {
        /// The table without fields.
        table: String,
    },
    /// A table declares more `levels` than it has enclosing table scopes (its
    /// non-root ancestor tables plus itself).
    ///
    /// Each level labels one parent-link index column, and the runtime can
    /// only supply one index per enclosing scope, so the surplus columns never
    /// receive a value: the parse fails later with an opaque column-length
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
    ImplicitEmptyString {
        /// The table containing the fields.
        table: String,
        /// The non-nullable `Utf8` fields that yield `""` when absent.
        fields: Vec<String>,
    },
}

impl fmt::Display for Lint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lint::DotSegmentInPath { location, path } => write!(
                f,
                "The path '{path}' in {location} has a '.' or '..' segment, so it never matches \
                 an element; write it without one"
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
                 the table element itself do not delimit rows; move the table's xml_path up \
                 one level so that <{}> becomes its row element",
                path_segments(xml_path).next_back().unwrap_or("/"),
            ),
            Lint::StructuralTable { table } => write!(
                f,
                "Table '{table}' has no fields, so it is excluded from the output; it exists \
                 only to count its rows for nested tables' <level> columns"
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
            Lint::ImplicitEmptyString { table, fields } => write!(
                f,
                "Table '{table}' has {} non-nullable Utf8 field(s) ({}): a missing or empty \
                 value yields \"\" rather than an error, unlike every other data type. Set \
                 nullable: true to distinguish missing from empty",
                fields.len(),
                fields.join(", "),
            ),
        }
    }
}

impl Config {
    /// Returns advisory findings about this configuration, in a deterministic
    /// order: paths no element can match first, then tables in configuration
    /// order, with lints in a fixed order per table.
    ///
    /// An empty result means nothing looked suspicious. It is *not* a
    /// correctness guarantee. See [`Lint`] for what is checked, and
    /// [`Config::validate`] for the checks that are hard errors instead.
    ///
    /// ```rust
    /// # use xml2arrow::{Config, config_from_yaml};
    /// let config = config_from_yaml!(r#"
    /// tables:
    ///   - name: stations
    ///     xml_path: /report/station
    ///     levels: []
    ///     fields:
    ///       - {name: id, xml_path: /report/station/@id, data_type: Utf8, nullable: true}
    /// "#);
    /// for lint in config.lint() {
    ///     println!("{lint}");   // "Table 'stations' ... has no configured child element ..."
    /// }
    /// ```
    #[must_use]
    pub fn lint(&self) -> Vec<Lint> {
        // Paths no element can match come first: such a path is the likeliest
        // explanation for the findings after it.
        let mut lints: Vec<Lint> = self
            .dot_segment_paths()
            .map(|(location, path)| Lint::DotSegmentInPath {
                location,
                path: path.to_string(),
            })
            .collect();

        for table in &self.tables {
            if table.fields.is_empty() {
                // The lints below describe rows and columns, which a table
                // without fields does not have.
                lints.push(Lint::StructuralTable {
                    table: table.name.clone(),
                });
                continue;
            }

            if self.row_delimiting_children(&table.xml_path).is_empty() {
                lints.push(Lint::NeverFinalizesRows {
                    table: table.name.clone(),
                    xml_path: table.xml_path.clone(),
                });
            }

            // Values are routed by which table's `xml_path` is open, so a
            // nested table captures the field however rows end.
            for field in &table.fields {
                if let Some(nested) = self.nested_table_capturing(table, field) {
                    lints.push(Lint::FieldInsideNestedTable {
                        table: table.name.clone(),
                        field: field.name.clone(),
                        field_path: field.xml_path.clone(),
                        nested_table: nested.name.clone(),
                        nested_table_path: nested.xml_path.clone(),
                    });
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

            let implicit_empty: Vec<String> = table
                .fields
                .iter()
                .filter(|f| f.data_type == DType::Utf8 && !f.nullable)
                .map(|f| f.name.clone())
                .collect();
            if !implicit_empty.is_empty() {
                lints.push(Lint::ImplicitEmptyString {
                    table: table.name.clone(),
                    fields: implicit_empty,
                });
            }
        }
        lints
    }

    /// Every path with a `.` or `..` segment, as `(where, path)`, in document
    /// order: `stop_at_paths`, each table's `xml_path`, and each field's.
    fn dot_segment_paths(&self) -> impl Iterator<Item = (String, &str)> + '_ {
        let stop_paths = self
            .parser_options
            .stop_at_paths
            .iter()
            .filter(|path| has_dot_segment(path))
            .map(|path| ("`parser_options.stop_at_paths`".to_string(), path.as_str()));
        let tables = self.tables.iter().flat_map(|table| {
            let table_path = has_dot_segment(&table.xml_path).then(|| {
                (
                    format!("the xml_path of table '{}'", table.name),
                    table.xml_path.as_str(),
                )
            });
            let fields = table
                .fields
                .iter()
                .filter(|field| has_dot_segment(&field.xml_path))
                .map(move |field| {
                    (
                        format!("field '{}' of table '{}'", field.name, table.name),
                        field.xml_path.as_str(),
                    )
                });
            table_path.into_iter().chain(fields)
        });
        stop_paths.chain(tables)
    }

    /// The table that captures `field`'s values instead of `table`, or `None`
    /// when `table` receives them.
    ///
    /// A value is delivered to the fields of the innermost *open* table only,
    /// and a table is open for as long as its element is. So when another
    /// table's element lies inside `table`'s and encloses the field's
    /// location, that table is open whenever a value there arrives, and the
    /// field never receives one. The innermost such table is the one that
    /// captures the value, so it is the one named.
    ///
    /// An attribute of the nested table's own element is inside it too: the
    /// table opens before its element's attributes are read.
    fn nested_table_capturing(
        &self,
        table: &TableConfig,
        field: &FieldConfig,
    ) -> Option<&TableConfig> {
        // A path no element can match is reported for that alone.
        if has_dot_segment(&field.xml_path) {
            return None;
        }
        self.tables
            .iter()
            .filter(|other| path_is_strictly_under(&other.xml_path, &table.xml_path))
            .filter(|other| path_is_under(&field.xml_path, &other.xml_path))
            .max_by_key(|other| path_segments(&other.xml_path).count())
    }

    /// How many index values a row of the table at `table_path` receives: one
    /// per enclosing non-root table scope, plus one for the table itself unless
    /// *it* is the root table.
    ///
    /// The root table (`xml_path: /`) is excluded on both counts, because
    /// `end_current_row` skips it when collecting parent indices: it
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

    /// Every configured direct child *element* of `table_path`, in first-seen
    /// order: the exact set whose closing tags finalize a row of the table at
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
    fn row_delimiting_children(&self, table_path: &str) -> Vec<&str> {
        let depth = path_segments(table_path).count();
        let all_paths = self
            .tables
            .iter()
            .flat_map(|t| {
                std::iter::once(t.xml_path.as_str())
                    .chain(t.fields.iter().map(|f| f.xml_path.as_str()))
            })
            .chain(self.parser_options.stop_at_paths.iter().map(String::as_str));

        let mut children: Vec<&str> = Vec::new();
        for path in all_paths {
            if !path_is_under(path, table_path) {
                continue;
            }
            // `None` when the path *is* the table path; `@`-prefixed when it
            // is an attribute of the table element.
            let Some(child) = path_segments(path).nth(depth) else {
                continue;
            };
            // An attribute never ends a row. Nor does a `.` or `..` segment: no
            // element has that name.
            if child.starts_with('@') || child == "." || child == ".." {
                continue;
            }
            if !children.contains(&child) {
                children.push(child);
            }
        }
        children
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_from_yaml;

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
            config.lint(),
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
            config.lint(),
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
        assert!(config.lint().contains(&Lint::ExcessLevels {
            table: "items".to_string(),
            declared: 3,
            available: 2,
        }));
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
        assert!(config.lint().contains(&Lint::ExcessLevels {
            table: "doc".to_string(),
            declared: 1,
            available: 0,
        }));
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
        assert!(config.lint().contains(&Lint::ExcessLevels {
            table: "items".to_string(),
            declared: 2,
            available: 1,
        }));
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
            config.lint(),
            vec![Lint::ImplicitEmptyString {
                table: "items".to_string(),
                fields: vec!["name".to_string(), "label".to_string()],
            }]
        );
    }

    /// A path with a `.` segment matches nothing. It is reported, and not
    /// counted as a child element that ends rows.
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
        let lints = config.lint();
        assert_eq!(
            lints,
            vec![Lint::DotSegmentInPath {
                location: "field 'w' of table 't'".into(),
                path: "/r/./item/w".into(),
            }]
        );
        assert_eq!(config.row_delimiting_children("/r"), vec!["item"]);
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
        let lints = config.lint();
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

    #[test]
    fn a_config_with_nothing_to_flag_has_no_lints() {
        let config = config_from_yaml!(
            r#"
tables:
  - name: items
    xml_path: /data
    levels: [item]
    fields:
      - {name: id, xml_path: /data/item/@id, data_type: Int32}
      - {name: note, xml_path: /data/item/note, data_type: Utf8, nullable: true}
"#
        );
        assert_eq!(config.lint(), vec![]);
    }

    /// A stop path under a table is a trie node like any configured child, so
    /// its closing tag ends a row, and a table whose only other paths are
    /// attributes of its own element does finalize rows.
    #[test]
    fn a_stop_path_under_a_table_counts_as_a_row_ending_child() {
        let config = config_from_yaml!(
            r#"
parser_options:
  stop_at_paths: [/data/end]
tables:
  - name: doc
    xml_path: /data
    levels: []
    fields:
      - {name: id, xml_path: /data/@id, data_type: Int32}
"#
        );
        assert_eq!(config.row_delimiting_children("/data"), vec!["end"]);
        assert_eq!(config.lint(), vec![]);
    }

    #[test]
    fn dot_segments_are_reported_for_tables_and_stop_paths_too() {
        let config = config_from_yaml!(
            r#"
parser_options:
  stop_at_paths: [/r/../end]
tables:
  - name: t
    xml_path: /r/./items
    levels: []
    fields:
      - {name: v, xml_path: /r/./items/item/v, data_type: Utf8, nullable: true}
"#
        );
        let dotted: Vec<String> = config
            .lint()
            .into_iter()
            .filter_map(|lint| match lint {
                Lint::DotSegmentInPath { location, .. } => Some(location),
                _ => None,
            })
            .collect();
        assert_eq!(
            dotted,
            vec![
                "`parser_options.stop_at_paths`",
                "the xml_path of table 't'",
                "field 'v' of table 't'",
            ]
        );
    }

    #[test]
    fn every_message_names_what_it_is_about() {
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
      - {name: label, xml_path: /data/items/item/label, data_type: Utf8}
"#
        );
        for lint in config.lint() {
            let message = lint.to_string();
            let subject = match &lint {
                Lint::StructuralTable { table } => table,
                Lint::ExcessLevels { table, .. } => table,
                Lint::ImplicitEmptyString { table, .. } => table,
                other => panic!("unexpected lint: {other:?}"),
            };
            assert!(message.contains(&format!("'{subject}'")), "{message}");
        }
        assert_eq!(config.lint().len(), 3);
    }
}
