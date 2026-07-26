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

use crate::config::{Config, DType, path_is_under, path_segments, paths_equal};

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
        table: String,
        xml_path: String,
        /// Every configured direct child element of `xml_path`, in first-seen
        /// order. Each one finalizes a row of this table when it closes.
        child_elements: Vec<String>,
    },
    /// A table has fields but **no** configured child element, so no element
    /// close can ever finalize one of its rows: the table silently produces
    /// zero rows.
    ///
    /// Reachable when every field of a table maps to an attribute of the table
    /// element itself (`/a/@id` on a table at `/a`) — the attribute
    /// pseudo-nodes are entered and left without going through the
    /// row-finalizing close path.
    NeverFinalizesRows { table: String, xml_path: String },
    /// A table declares no fields. It is excluded from the output entirely and
    /// exists only to feed its row counter to descendant tables' `levels`
    /// index columns.
    StructuralTable { table: String },
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
        table: String,
        declared: usize,
        available: usize,
    },
    /// Non-nullable `Utf8` fields yield an empty string when the value is
    /// missing, where every other data type raises `MissingRequiredField`.
    ///
    /// A long-standing asymmetry, listed here so configs that *rely* on it are
    /// visible: set `nullable: true` on these fields to distinguish "absent"
    /// from "present but empty".
    ImplicitEmptyString { table: String, fields: Vec<String> },
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
                "Table '{table}' (xml_path {xml_path}) has {} configured child elements ({}): \
                 each one finalizes a row when it closes, so this table yields one \
                 partially-filled row per child element rather than one row per <{}>. \
                 If you expected a single row, split the table or configure fields under a \
                 single child element",
                child_elements.len(),
                child_elements.join(", "),
                path_segments(xml_path).next_back().unwrap_or("/"),
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
        }
    }
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
                        && !paths_equal(&ancestor.xml_path, table_path)
                        && path_is_under(table_path, &ancestor.xml_path)
                })
                .count()
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
            std::iter::once(t.xml_path.as_str()).chain(t.fields.iter().map(|f| f.xml_path.as_str()))
        });

        let mut children: Vec<String> = Vec::new();
        for path in all_paths {
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
        assert_eq!(config.lint(), vec![]);
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
            config.lint(),
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
        let lints = config.lint();
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
        assert_eq!(config.lint(), vec![]);
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
        assert_eq!(config.lint(), vec![]);
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
    }
}
